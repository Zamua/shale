// Package membership wraps hashicorp/memberlist + exposes a small,
// shale-shaped API to the rest of the system: a snapshot of current
// Members + a channel of join/leave Events. It is deliberately I/O
// free at the type layer (Member is a plain value object) and does
// NOT own routing or the hash ring - the cluster layer subscribes to
// Events + rebuilds its ring in response.
//
// Each node advertises its gRPC listen address via memberlist's
// per-node Meta bytes; peers decode that on receipt so they know
// where to forward requests.
//
// Meta wire format (backward-shaped): a node that is NOT draining
// publishes exactly its gRPC address as a plain UTF-8 string - byte
// identical to the pre-draining format, so an old peer (or an old
// decode path) reading it sees the address unchanged. A draining node
// appends a single NUL separator + the marker byte 'D' (addr+"\x00D").
// decodeMeta splits on the first NUL: the head is always the address;
// a trailing "D" segment means Draining=true. Absence of any NUL means
// not draining. So the common (non-draining) case is the legacy format,
// and the draining bit rides alongside the address in the SAME Meta
// payload that the graceful-leave path uses. Two further trailing
// segments carry the standing declared unit count ("U<count>") and the
// node's STABLE identity + boot epoch ("I<nodeID>" and "E<epoch>") - the
// last two decouple the memberlist node NAME (which is per-process unique,
// so a restart is a clean new-node join) from the stable shale node id
// (which rides in Meta and survives restarts). All segments are
// order-independent and any decoder ignores a prefix it does not know
// (forward-compatible), so an OLD peer still parses addr/draining/count.
package membership

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
)

// leaveTimeout bounds the wait for the graceful-leave broadcast in
// Close. memberlist.Leave(0) waits forever for the leave broadcast to
// be gossiped out, which blocks indefinitely when a peer is still
// alive but the broadcast does not complete promptly (slow or
// unreachable peer). A bounded timeout lets Close fall through to
// Shutdown; the remaining peers still observe the departure via
// failure detection even if the graceful broadcast did not land.
const leaveTimeout = 5 * time.Second

// gossipConfig returns the memberlist preset used for every new node.
// It defaults to DefaultLANConfig, which is correct for the real
// cross-host VPS cluster (conservative probe/suspicion timings tuned for
// a real network). Tests call UseLocalGossipForTests to swap in the much
// tighter loopback preset so in-process clusters converge in
// milliseconds instead of seconds.
var gossipConfig = memberlist.DefaultLANConfig

// UseLocalGossipForTests switches new memberlist instances to the
// loopback (DefaultLocalConfig) preset, whose probe/gossip/suspicion
// timings are an order of magnitude tighter than LAN. TEST-ONLY: call it
// once from a test package's init() so that package's in-process
// clusters converge fast. It is never called in production, so prod
// nodes keep the LAN preset.
func UseLocalGossipForTests() { gossipConfig = memberlist.DefaultLocalConfig }

// EventType distinguishes the kinds of membership change Events
// subscribers can observe.
type EventType int

const (
	// EventJoin is delivered when memberlist detects a new live node
	// (including the local node on Open).
	EventJoin EventType = iota
	// EventLeave is delivered when memberlist detects a node has
	// departed (graceful Leave or failure detection).
	EventLeave
)

// Member is the value object describing one cluster participant.
// Addr is the gRPC host:port the node listens on - decoded from the
// memberlist Meta payload.
type Member struct {
	// ID is the STABLE shale node identity (Config.NodeID). It is decoded
	// from the peer's Meta payload (the "I<id>" segment), falling back to the
	// memberlist node name for a legacy peer that does not publish it (whose
	// memberlist name still IS its stable id). NOT the memberlist node name,
	// which is per-process-unique (NodeID#bootEpoch) so a restart never
	// collides with its own prior incarnation; see Open + the SPEC subsection
	// "Stable node identity vs memberlist node name".
	ID string
	// Addr is the node's gRPC host:port, as broadcast via Config.GRPCAddr.
	Addr string
	// Draining is true when the node has entered the graceful-leave
	// (scale-down) DRAINING state: it stays a full, alive, addressable
	// member (Addr is still valid + serving) but yields OWNERSHIP - the
	// ring-from-membership reconcile drops a draining member from the
	// consistent-hash ownership ring so its positions redistribute to
	// non-draining members. Decoded from the peer's Meta payload.
	Draining bool
	// Joining is true when the node is WARMING UP into the ring: it booted
	// with one or more DEFERRED positions (owned-but-not-yet-mounted, because a
	// peer's serving marker was present so the node did not fence the live peer)
	// and has not yet mounted every position it owns. It is the entry-side mirror
	// of Draining: while a node is Joining, every node's per-op current/pending
	// split EXCLUDES it from the CURRENT set (subject to a quorum floor), so the
	// still-mounted DISPLACED owner stays the stable holder and the newcomer
	// acquires the position via the background pending-owner path (serving-marking
	// it, which gates the displaced owner's drain). Cleared once the node has
	// mounted every position it owns. Decoded from the peer's Meta payload. A
	// cold start (no serving markers anywhere) defers nothing and never sets it,
	// so first-cluster convergence is unchanged.
	Joining bool
	// DeclaredUnitCount is the node's STANDING declared shard (unit) count -
	// the value the operator configured for it (SHALE_UNIT_COUNT). It is
	// gossiped in the Meta payload so the cluster can detect cluster-wide
	// AGREEMENT on a desired count and drive a declarative reshard toward it
	// (see the cluster's observeDeclaredReshardTarget). 0 means UNKNOWN /
	// absent (a legacy node that does not gossip it, or a non-multi-backend
	// node); an unknown count breaks unanimity and defers any auto-reshard,
	// the fail-safe. Static for the node's lifetime.
	DeclaredUnitCount uint32

	// epoch is the UNEXPORTED per-process boot token (time.Now().UnixNano()
	// captured at Open) decoded from the Meta "E<epoch>" segment, 0 when a
	// peer's Meta carries none (a legacy node). It exists ONLY to break the
	// dedup tie when two memberlist names momentarily share one stable ID
	// during a restart (old process name A, new process name B, both id S):
	// Members()/Snapshot() project the per-name cache to one Member per ID,
	// keeping the HIGHEST epoch (the newest process). It is NOT part of the
	// public surface the ring or cluster consumes.
	epoch uint64
}

// metaSep separates the address head from the optional draining marker
// in the Meta wire format. A NUL never appears in a host:port address,
// so it is an unambiguous delimiter and keeps the non-draining payload
// byte-identical to the bare-address legacy format.
const metaSep = '\x00'

// metaDrainingMarker is the trailing segment that flags a draining node.
const metaDrainingMarker = "D"

// metaJoiningMarker is the trailing segment that flags a JOINING (warming) node.
// It is the entry-side mirror of metaDrainingMarker: its own NUL-delimited "J"
// segment, order-independent, ignored by any decoder that does not understand it
// (forward-compatible), so an old peer still parses addr/draining/count.
const metaJoiningMarker = "J"

// metaUnitCountPrefix tags the trailing segment carrying a node's standing
// DECLARED unit count, e.g. "U16". It is its own NUL-delimited segment after
// the optional draining marker, so it composes with draining and is ignored
// by any decoder that does not understand it (forward-compatible). A NUL
// never appears in the decimal count, so the segment is unambiguous.
const metaUnitCountPrefix = "U"

// metaStableIDPrefix tags the trailing segment carrying the node's STABLE
// shale identity (Config.NodeID), e.g. "Ishaled-homog-0". It decouples the
// stable id from the per-process-unique memberlist node NAME: peers recover
// the stable id from Meta rather than from the name, so a restart (which gets
// a fresh name) keeps the same logical id. Its own NUL-delimited segment, so
// it composes with draining + count in any order and is ignored by a decoder
// that does not understand it (forward-compatible). A NodeID may hold any
// bytes except NUL (the segment separator, which never appears in a pod name),
// so the segment is unambiguous.
const metaStableIDPrefix = "I"

// metaEpochPrefix tags the trailing segment carrying the node's per-PROCESS
// boot epoch (a monotonic token captured at Open), e.g. "E1718000000000000000".
// It is the dedup tie-break when two memberlist names momentarily share one
// stable id during a restart (highest epoch wins = newest process). Its own
// NUL-delimited segment, decimal uint64, ignored by an unaware decoder
// (forward-compatible).
const metaEpochPrefix = "E"

// encodeMeta builds the Meta payload for the local node. The head is the
// address (legacy-identical bare-address form). A draining node appends
// metaSep + the draining marker; a node with a known declared unit count (> 0)
// appends metaSep + "U<count>"; a node with a stable id appends metaSep +
// "I<id>" and metaSep + "E<epoch>". Segments are independent + order does not
// matter to the decoder, so the payload may carry any subset. The stable-id +
// epoch segments are always emitted by a node that knows them (epoch is the
// per-process Open token, always nonzero in production).
func encodeMeta(addr string, draining, joining bool, declaredUnitCount uint32, stableID string, epoch uint64) []byte {
	s := addr
	if draining {
		s += string(metaSep) + metaDrainingMarker
	}
	if joining {
		s += string(metaSep) + metaJoiningMarker
	}
	if declaredUnitCount > 0 {
		s += string(metaSep) + metaUnitCountPrefix + strconv.FormatUint(uint64(declaredUnitCount), 10)
	}
	if stableID != "" {
		s += string(metaSep) + metaStableIDPrefix + stableID
	}
	if epoch > 0 {
		s += string(metaSep) + metaEpochPrefix + strconv.FormatUint(epoch, 10)
	}
	return []byte(s)
}

// decodeMeta parses a Meta payload back into the address, draining bit,
// declared unit count, stable id, and boot epoch. The head (up to the first
// metaSep, or the whole payload if none) is always the address; a trailing
// metaDrainingMarker segment sets draining, "U<count>" sets the declared unit
// count, "I<id>" sets the stable id, "E<epoch>" sets the boot epoch. Unknown /
// unparseable trailing segments are ignored (forward-compatible); an absent
// count yields 0 (UNKNOWN), an absent stable id yields "" (the caller falls
// back to the memberlist name), an absent epoch yields 0.
func decodeMeta(meta []byte) (addr string, draining, joining bool, declaredUnitCount uint32, stableID string, epoch uint64) {
	s := string(meta)
	head, rest, found := strings.Cut(s, string(metaSep))
	if !found {
		return s, false, false, 0, "", 0
	}
	for _, seg := range strings.Split(rest, string(metaSep)) {
		switch {
		case seg == metaDrainingMarker:
			draining = true
		case seg == metaJoiningMarker:
			joining = true
		case strings.HasPrefix(seg, metaUnitCountPrefix):
			if n, err := strconv.ParseUint(seg[len(metaUnitCountPrefix):], 10, 32); err == nil {
				declaredUnitCount = uint32(n)
			}
		case strings.HasPrefix(seg, metaStableIDPrefix):
			stableID = seg[len(metaStableIDPrefix):]
		case strings.HasPrefix(seg, metaEpochPrefix):
			if n, err := strconv.ParseUint(seg[len(metaEpochPrefix):], 10, 64); err == nil {
				epoch = n
			}
		}
	}
	return head, draining, joining, declaredUnitCount, stableID, epoch
}

// Event is one membership change notification.
type Event struct {
	Type   EventType
	Member Member
}

// Config configures a Membership instance.
type Config struct {
	// NodeID is this node's stable, unique identity within the cluster.
	NodeID string
	// BindAddr is the memberlist UDP+TCP listen address ("host:port"
	// or ":port" for all interfaces). This is what seeds dial to
	// reach this node.
	BindAddr string
	// Seeds is the list of peer memberlist addresses to contact at
	// startup. Empty means "I am the first node". If non-empty and
	// Open cannot reach any seed, Open returns an error.
	Seeds []string
	// GRPCAddr is this node's gRPC service address. It is broadcast
	// to peers as the node's Meta payload so they can forward
	// requests here. Format: "host:port".
	GRPCAddr string
	// DeclaredUnitCount is the node's standing declared shard (unit) count,
	// broadcast in the Meta payload (the declarative-reshard signal; see
	// Member.DeclaredUnitCount). 0 (the default) means "do not advertise a
	// count" - the node is non-multi-backend or pre-feature, and peers see it
	// as UNKNOWN. The cluster passes its configured SHALE_UNIT_COUNT here in
	// multi-backend mode.
	DeclaredUnitCount uint32
	// Joining seeds the INITIAL value of this node's gossiped Joining bit, so it
	// is present in the VERY FIRST Meta payload memberlist broadcasts (atomically
	// with the node's ring presence). This closes a race the entry-side
	// write-transparency needs: if a newcomer advertised Joining only AFTER its
	// address had already gossiped, a peer could learn the newcomer is a member
	// (and clean-cut RELEASE the position the newcomer displaces) BEFORE it learns
	// the newcomer is Joining (which would have told it to HOLD + drain instead).
	// Because the bit rides in the SAME Meta payload as the address, seeding it
	// true here makes presence and Joining atomic. The cluster sets it true for a
	// node JOINING an existing replicated cluster (has seeds, R>1); the reconcile
	// clears it once the node has mounted every position it owns. A founder (no
	// seeds) leaves it false, so cold first-cluster formation is unchanged.
	Joining bool
	// LogOutput is where memberlist's internal logger writes. nil
	// means stderr (memberlist's default). Tests pass io.Discard to
	// stay quiet.
	LogOutput io.Writer
	// RejoinInterval is how often a background goroutine re-contacts
	// Seeds (via memberlist.Join) to re-bridge a gossip split. memberlist's
	// own PushPull anti-entropy only syncs with members ALREADY in the
	// local member list, so once a mass rolling restart (every pod gets a
	// NEW address; old addresses are reaped while new ones join in disjoint
	// waves) fragments the ring into two groups that have pruned each other
	// out, neither group can reach the other and the seed (the only stable
	// bridge) is never re-dialed - the startup split becomes PERMANENT.
	// A periodic re-Join heals it: Join is idempotent (a no-op when the seed
	// is already a known live member, a MERGE when split). The loop runs
	// only when Seeds is non-empty, stops on Close, and does NOT rejoin once
	// the node has called Leave (a departing node must not re-advertise
	// itself). Zero DISABLES the loop (the default for in-process tests that
	// do not churn addresses); production sets a non-zero interval.
	RejoinInterval time.Duration

	// MetaRefreshInterval is how often a background goroutine re-broadcasts
	// THIS node's own Meta payload (gRPC address + draining bit + declared
	// unit count) via memberlist.UpdateNode. It is a DISTINCT concern from
	// RejoinInterval: rejoin re-bridges a gossip SPLIT (re-Join the seeds);
	// meta-refresh re-publishes the local node's metadata to NON-split-but-
	// STALE peers. After a STAGGERED rolling restart a restarted pod keeps
	// its stable node NAME but RESETS its memberlist incarnation to 0, so a
	// peer that still remembers the OLD process's higher incarnation REJECTS
	// the new pod's fresh Meta as stale (memberlist's aliveNode incarnation
	// guard) - leaving peers with a per-peer-divergent view of one member's
	// declared count that does NOT self-heal, which turns any unanimity gate
	// (the declarative-reshard driver) into a RACE. UpdateNode re-reads
	// NodeMeta and broadcasts with a BUMPED (process-local) incarnation each
	// call, so repeated ticks monotonically climb the local incarnation and
	// within a bounded number of rounds overtake the stale remembered value,
	// clearing the rejection. The loop runs only on a non-zero interval,
	// stops on Close, and SKIPS once the node is leaving (a departing node
	// must not re-advertise itself - identical guard to the rejoin loop).
	// Zero DISABLES the loop (the default for in-process tests that do not
	// churn addresses); production sets a non-zero interval. NB the
	// incarnation bump alone is insufficient when a peer holds the old entry
	// as StateDead with the old IP - the address-reclaim gate rejects the
	// new IP first; DeadNodeReclaimTime (set on the gossip config in Open)
	// opens that window so the re-broadcast can land.
	MetaRefreshInterval time.Duration

	// AllowSoloStart controls what happens when Seeds is non-empty but NONE of
	// them is reachable at Open. By default (false) that is a hard error: a
	// joiner with dead seeds must not silently fork a new cluster. When true,
	// Open instead comes up SOLO (a 1-node memberlist) and returns success, and
	// the CALLER owns the form-vs-join decision (e.g. a durable CAS form-lock).
	// This is what lets a homogeneous fleet - every pod configured with the SAME
	// seed list (a headless Service), no dedicated seed pod - boot from cold:
	// the first pod up reaches no peer, starts solo, and contends to form;
	// later pods reach it and join.
	AllowSoloStart bool

	// TestingStableMemberlistName forces the memberlist node NAME to be the bare
	// stable NodeID (the PRE-#473 behavior) instead of the per-process-unique
	// NodeID#bootEpoch. TEST-ONLY: it exists purely so a test can demonstrate the
	// dead-node-reclaim / conflicting-address failure that #473 fixed - a restart
	// that reuses its name presents a same-name/new-address alive message, which
	// memberlist's aliveNode gates behind DeadNodeReclaimTime. Production must
	// leave this false (the default), so a restart is always a fresh unique-named
	// node and the reclaim gate is never reached. Named Testing* per convention.
	TestingStableMemberlistName bool
}

// DefaultRejoinInterval is the production re-Join cadence used when a
// caller wires periodic seed anti-entropy without an explicit interval.
// 30s matches memberlist's DefaultLANConfig PushPull cadence: frequent
// enough that a mass-restart gossip split heals within a couple of
// gossip rounds, infrequent enough that the idempotent no-op Join on a
// healthy cluster is negligible overhead.
const DefaultRejoinInterval = 30 * time.Second

// DefaultMetaRefreshInterval is the production cadence for the local-Meta
// re-broadcast loop. 10s is small enough that a peer holding a STALE Meta
// view of this node (the reset-incarnation case after a staggered rolling
// restart) self-heals within a couple of refresh rounds - bounding the
// window the declarative-reshard unanimity gate can be stuck racing - yet
// large enough that the re-broadcast (a single small UpdateNode alive
// message on an unchanged Meta) adds negligible gossip load.
const DefaultMetaRefreshInterval = 10 * time.Second

// deadNodeReclaimTime is the bounded window after which a peer may reclaim a
// DEAD node's NAME for a NEW address. memberlist defaults DeadNodeReclaimTime
// to 0, which means a StateDead entry's name can NEVER be reclaimed by a
// different IP - so a restarted pod (same stable NodeID, new pod IP) that a
// peer reaped as dead (a hard kill where the graceful Leave did not reach
// that peer) has its fresh Meta rejected at the conflicting-address gate
// FOREVER, before the incarnation is even examined. A bounded reclaim window
// lets that peer accept the new IP once the old entry has been dead this long.
//
// NO LONGER LOAD-BEARING FOR RESTARTS as of the identity decouple (#473): the
// memberlist node name is now per-process-unique (NodeID#bootEpoch), so a
// restarted pod is a brand-new memberlist node that never shares a name with
// its dead prior incarnation - the conflicting-address gate this window
// mitigated is simply never reached for a restart. Retained (harmless) as a
// belt-and-suspenders reclaim window for any residual same-name/new-address
// case and to avoid a behavior change; see the SPEC subsection "Stable node
// identity vs memberlist node name".
const deadNodeReclaimTime = 30 * time.Second

// metaUpdateTimeout bounds how long a single meta-refresh tick waits inside
// memberlist.UpdateNode for the queued alive broadcast to flush. It must be
// NON-ZERO: memberlist treats a 0 timeout as "wait forever" (not "do not
// wait"), and with a live peer UpdateNode blocks on the broadcast's notify
// channel - which never fires if the next tick's re-broadcast SUPERSEDES this
// one before it transmits (memberlist's TransmitLimitedQueue invalidates the
// older same-node broadcast, and an invalidated broadcast does not signal its
// notify). A 0 timeout therefore wedges the loop's goroutine permanently on the
// first superseded tick. A small bounded timeout makes a tick that cannot flush
// promptly return; the next tick retries with a still-higher incarnation. Kept
// well under DefaultMetaRefreshInterval so a slow tick never overruns into the
// next one.
const metaUpdateTimeout = 2 * time.Second

// Membership is the running gossip layer for one node. All exported
// methods are safe for concurrent use.
type Membership struct {
	cfg Config

	ml     *memberlist.Memberlist
	events *eventDelegate
	meta   *metaDelegate

	mu     sync.Mutex
	closed bool
	// leaving is set by Leave(): once the node has gracefully announced
	// its departure, the periodic rejoin loop must NOT re-Join the seeds
	// (that would re-advertise a draining / departing node back into the
	// cluster it is leaving). It is a distinct flag from closed because
	// Leave() does NOT mark the Membership closed (the transport stays up
	// so Members / Snapshot keep returning the live view).
	leaving bool

	// rejoinDone is closed by Close to stop the periodic rejoin goroutine.
	// nil when no rejoin loop is running (no seeds or RejoinInterval == 0).
	rejoinDone chan struct{}
	// rejoinWG tracks the rejoin goroutine so Close can wait for it to
	// exit cleanly before returning.
	rejoinWG sync.WaitGroup
	// rejoinAttempts counts how many times the loop actually called
	// ml.Join (it was alive + not leaving). rejoinSkips counts ticks the
	// loop skipped because the node was leaving or closing. Both are
	// observability hooks, surfaced for tests + debugging.
	rejoinAttempts atomic.Uint64
	rejoinSkips    atomic.Uint64

	// metaRefreshDone is closed by Close to stop the periodic Meta
	// re-broadcast goroutine. nil when no refresh loop is running
	// (MetaRefreshInterval == 0). It is a SEPARATE lifecycle from the
	// rejoin loop: it heals a stale-Meta peer view, not a gossip split.
	metaRefreshDone chan struct{}
	// metaRefreshWG tracks the meta-refresh goroutine so Close can wait for
	// it to exit cleanly before tearing the transport down.
	metaRefreshWG sync.WaitGroup
	// metaRefreshAttempts counts how many times the loop actually called
	// ml.UpdateNode (alive + not leaving). metaRefreshSkips counts ticks the
	// loop skipped because the node was leaving or closing. Observability
	// hooks, surfaced for tests + debugging.
	metaRefreshAttempts atomic.Uint64
	metaRefreshSkips    atomic.Uint64

	// mlName is THIS node's unique memberlist node name (Config.NodeID +
	// "#" + bootEpoch), the cache key for the local node's own entry. It is
	// distinct from cfg.NodeID (the stable id): the local self-seed and the
	// SetDraining self-update both key the cache by mlName, while the projected
	// Member.ID stays the stable id. Set once in Open before any goroutine can
	// observe Membership, then read-only.
	mlName string

	// cache holds the authoritative Member snapshot keyed by the UNIQUE
	// memberlist node name (NOT the stable Member.ID). Reads (Members,
	// Snapshot) consult this map under cacheMu - then PROJECT it to one Member
	// per stable id (highest epoch wins) - instead of dereferencing
	// memberlist.Node pointers, whose Name + Meta fields memberlist mutates
	// from its own goroutines without a per-Node lock. The cache is populated
	// from inside memberlist's NotifyJoin / NotifyLeave / NotifyUpdate
	// callbacks (which run serialized vs the alive/dead transitions, so reading
	// Node fields THERE is safe), plus a single seeding pass at Open time before
	// any external goroutine can observe Membership. Keying by name keeps a
	// restarting node's old + new entries distinct (see upsertCache).
	cacheMu sync.RWMutex
	cache   map[string]Member
}

// metaDelegate is the minimal memberlist.Delegate that publishes our
// GRPCAddr (and draining bit) as the node's Meta payload. The other
// Delegate methods are no-ops; we do not use memberlist's user-message
// channel.
//
// addr is fixed for the node's lifetime; draining flips when
// SetDraining is called. NodeMeta is invoked by memberlist from its own
// goroutines (on gossip + on UpdateNode), so the fields are guarded by
// mu. SetDraining mutates draining + then asks memberlist to re-read
// NodeMeta via UpdateNode, gossiping the new payload out.
type metaDelegate struct {
	mu       sync.Mutex
	addr     string
	draining bool
	// joining flips true while the node is WARMING (booted with deferred
	// positions) and false once it has mounted every position it owns. Read
	// under mu in NodeMeta alongside draining; mirrors the draining lifecycle.
	joining bool
	// declaredUnitCount is the node's standing declared shard count. Unlike
	// draining it is fixed for the node's lifetime (set at construction from
	// Config.DeclaredUnitCount); it is read under mu only to share the lock
	// with the draining read in NodeMeta.
	declaredUnitCount uint32
	// stableID is the node's stable shale identity (Config.NodeID), published
	// in Meta so peers recover it independently of the per-process memberlist
	// name. epoch is the per-process boot token captured at Open. Both are
	// fixed for the node's lifetime; they are read under mu only to share the
	// lock with the draining read in NodeMeta.
	stableID string
	epoch    uint64
}

func (d *metaDelegate) NodeMeta(limit int) []byte {
	d.mu.Lock()
	meta := encodeMeta(d.addr, d.draining, d.joining, d.declaredUnitCount, d.stableID, d.epoch)
	d.mu.Unlock()
	if len(meta) > limit {
		return meta[:limit]
	}
	return meta
}

// setDraining updates the local draining flag. Returns true if the
// value changed (so the caller knows whether a gossip refresh is worth
// triggering).
func (d *metaDelegate) setDraining(draining bool) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.draining == draining {
		return false
	}
	d.draining = draining
	return true
}

// setJoining updates the local joining flag. Returns true if the value changed
// (so the caller knows whether a gossip refresh is worth triggering). Mirror of
// setDraining.
func (d *metaDelegate) setJoining(joining bool) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.joining == joining {
		return false
	}
	d.joining = joining
	return true
}

func (d *metaDelegate) NotifyMsg([]byte)                {}
func (d *metaDelegate) GetBroadcasts(int, int) [][]byte { return nil }
func (d *metaDelegate) LocalState(bool) []byte          { return nil }
func (d *metaDelegate) MergeRemoteState([]byte, bool)   {}

// eventDelegate forwards memberlist's join/leave/update callbacks
// into the Membership's events channel as Event values. It uses a
// non-blocking send: if no consumer is reading, the event is dropped.
// Callers who care about every transition must keep up with Events()
// OR call Snapshot()/Members() periodically to reconcile against the
// authoritative current state. The dropCount counter (read via
// Membership.DropCount) makes drops observable so reconcilers can
// log + alert on backpressure.
//
// The mutex coordinates with Close: after Close has acquired the
// write lock + set closed=true, send is guaranteed to skip the send
// rather than panic. memberlist's background goroutines may continue
// calling these callbacks for a brief window after Shutdown returns,
// so this guard is load-bearing - without it, a stray late callback
// will panic on send-to-closed-channel.
type eventDelegate struct {
	mu        sync.RWMutex
	out       chan Event
	closed    bool
	dropCount atomic.Uint64

	// parent is the Membership whose cache the callbacks update. Set
	// once during Open before memberlist.Create can fire any callback,
	// then read-only for the lifetime of the delegate. Stays nil only
	// in the TestDropCountObservable case where the delegate is poked
	// directly without a parent (the test bypasses the public path).
	parent *Membership
}

// nodeToMember derives our Member value object from a memberlist.Node,
// returning BOTH the unique memberlist node name (the cache key) and the
// Member (whose ID is the STABLE shale id). Member.ID is the stable id
// decoded from the node's Meta, FALLING BACK to the memberlist name when the
// Meta carries no stable-id segment - the backward-compat path for a legacy
// peer whose memberlist name still IS its stable id during a rolling upgrade.
// Member.epoch is the decoded boot epoch (0 when absent), used only for the
// per-id dedup tie-break in Members()/Snapshot().
//
// MUST ONLY be called either (a) at Open() during the single-writer
// seeding window before goroutines spawn, or (b) inside a memberlist
// event callback (NotifyJoin/Leave/Update), where memberlist serializes
// the call against its own alive/dead transitions and Node-field
// mutation. Calling this from a reconcile / Members / Snapshot path
// races with memberlist's internal aliveNode writes; the cache exists
// precisely to avoid that.
func nodeToMember(n *memberlist.Node) (name string, m Member) {
	name = strings.Clone(n.Name)
	meta := append([]byte(nil), n.Meta...)
	addr, draining, joining, declaredUnitCount, stableID, epoch := decodeMeta(meta)
	id := stableID
	if id == "" {
		// Legacy peer (no stable-id segment): its memberlist name IS its
		// stable id, so fall back to the name.
		id = name
	}
	return name, Member{
		ID:                id,
		Addr:              addr,
		Draining:          draining,
		Joining:           joining,
		DeclaredUnitCount: declaredUnitCount,
		epoch:             epoch,
	}
}

func (e *eventDelegate) NotifyJoin(n *memberlist.Node) {
	name, m := nodeToMember(n)
	e.upsertCache(name, m)
	e.send(EventJoin, m)
}

func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	name, m := nodeToMember(n)
	e.removeCache(name)
	e.send(EventLeave, m)
}

func (e *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	// Treat metadata updates as a join refresh: the addressing info
	// for the node may have changed (e.g. GRPCAddr migrated).
	name, m := nodeToMember(n)
	e.upsertCache(name, m)
	e.send(EventJoin, m)
}

// upsertCache writes the given Member into the parent Membership's cache,
// keyed by the UNIQUE memberlist node name (NOT the stable Member.ID). Keying
// by name is load-bearing for the restart leave-hazard: during a restart both
// the old process (name A, stable id S) and the new process (name B, stable id
// S) are briefly present; keying by name keeps them as distinct entries so the
// old NotifyLeave(A) drops only A. Members()/Snapshot() project per-name
// entries down to one Member per stable id. No-op if no parent is attached
// (the synthetic-delegate test path). Late stray callbacks after Close are
// harmless: Members() already returns nil once the parent's closed flag is set,
// so the post-Close cache state is unobservable.
func (e *eventDelegate) upsertCache(name string, m Member) {
	if e.parent == nil {
		return
	}
	e.parent.cacheMu.Lock()
	if e.parent.cache == nil {
		e.parent.cache = make(map[string]Member, 4)
	}
	e.parent.cache[name] = m
	e.parent.cacheMu.Unlock()
}

// removeCache deletes the given memberlist NAME from the parent Membership's
// cache. No-op if no parent is attached. See upsertCache for the keyed-by-name
// rationale (it keeps a restarting node's old + new entries distinct) and the
// post-Close rationale.
func (e *eventDelegate) removeCache(name string) {
	if e.parent == nil {
		return
	}
	e.parent.cacheMu.Lock()
	delete(e.parent.cache, name)
	e.parent.cacheMu.Unlock()
}

func (e *eventDelegate) send(t EventType, m Member) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return
	}
	ev := Event{Type: t, Member: m}
	select {
	case e.out <- ev:
	default:
		// Drop on backpressure. Subscribers can always reconcile via
		// Snapshot()/Members() if they fall behind; the dropCount
		// counter (Membership.DropCount) lets observers detect this.
		e.dropCount.Add(1)
	}
}

// shutdown marks the delegate closed + closes the underlying channel
// so subscribers ranging over it observe completion. Subsequent send
// calls become no-ops.
func (e *eventDelegate) shutdown() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.closed = true
	close(e.out)
}

// Open brings the local memberlist up + (if seeds are non-empty)
// joins an existing cluster. On error, any partially-initialized
// resources are released before returning.
func Open(cfg Config) (*Membership, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("membership: NodeID is required")
	}
	if cfg.BindAddr == "" {
		return nil, errors.New("membership: BindAddr is required")
	}

	bindHost, bindPort, err := splitHostPort(cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("membership: invalid BindAddr %q: %w", cfg.BindAddr, err)
	}

	events := &eventDelegate{out: make(chan Event, 64)}

	// Construct the Membership shell first so events.parent points at
	// a real cache before memberlist.Create can fire NotifyJoin for
	// the local node. cache is non-nil so the first callback never
	// has to allocate under cacheMu.
	m := &Membership{
		cfg:    cfg,
		events: events,
		cache:  make(map[string]Member, 4),
	}
	events.parent = m

	// Each PROCESS gets a UNIQUE memberlist node name = stable id + "#" +
	// bootEpoch, so a restart is a brand-new memberlist node: the old process
	// dies a normal death (different name, no same-name/new-address conflict to
	// reject) and the new one joins cleanly. The stable id rides in Meta (see
	// metaStableIDPrefix) so peers recover the logical identity independently of
	// this per-process name. bootEpoch is a per-process monotonic token; using
	// time.Now in production code is fine (only the workflow SCRIPT layer cannot).
	bootEpoch := uint64(time.Now().UnixNano())
	mlName := fmt.Sprintf("%s#%d", cfg.NodeID, bootEpoch)
	if cfg.TestingStableMemberlistName {
		// PRE-#473 behavior (test-only): reuse the bare stable id as the memberlist
		// name, so a restart collides with its own dead prior incarnation and hits
		// the conflicting-address / DeadNodeReclaimTime gate. The stable id still
		// rides in Meta + the boot epoch still climbs, so only the NAME reuse (the
		// exact pre-fix condition) is reintroduced.
		mlName = cfg.NodeID
	}
	m.mlName = mlName

	mlCfg := gossipConfig()
	mlCfg.Name = mlName
	if bindHost != "" {
		mlCfg.BindAddr = bindHost
		mlCfg.AdvertiseAddr = bindHost
	}
	mlCfg.BindPort = bindPort
	mlCfg.AdvertisePort = bindPort
	// Bounded dead-node address reclaim. memberlist defaults this to 0,
	// which makes a StateDead entry's NAME un-reclaimable by a NEW IP - so a
	// restarted pod (same NodeID, new pod IP) that a peer reaped as dead has
	// its fresh Meta rejected at the conflicting-address gate forever. A
	// bounded window lets that peer accept the new IP after the old entry has
	// been dead this long, the companion the periodic Meta re-broadcast needs
	// (the incarnation bump alone cannot clear the address gate). See
	// deadNodeReclaimTime.
	mlCfg.DeadNodeReclaimTime = deadNodeReclaimTime
	meta := &metaDelegate{addr: cfg.GRPCAddr, draining: false, joining: cfg.Joining, declaredUnitCount: cfg.DeclaredUnitCount, stableID: cfg.NodeID, epoch: bootEpoch}
	m.meta = meta
	mlCfg.Delegate = meta
	mlCfg.Events = events
	if cfg.LogOutput != nil {
		mlCfg.LogOutput = cfg.LogOutput
	}

	ml, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("membership: memberlist.Create: %w", err)
	}

	if len(cfg.Seeds) > 0 {
		contacted, err := ml.Join(cfg.Seeds)
		// AllowSoloStart (homogeneous bootstrap): unreachable seeds are EXPECTED.
		// The headless Service lists peers that may be down, and the first pod up
		// reaches none - memberlist.Join then returns contacted==0 AND a dial err.
		// Under the flag both are non-fatal: contacted==0 means "start solo" (the
		// caller contends to form via a durable CAS form-lock), contacted>0 means
		// "joined". Without the flag, either is a hard failure (a joiner with dead
		// seeds must not silently fork a new cluster).
		if !cfg.AllowSoloStart {
			if err != nil {
				_ = ml.Shutdown()
				return nil, fmt.Errorf("membership: join: %w", err)
			}
			if contacted == 0 {
				_ = ml.Shutdown()
				return nil, fmt.Errorf("membership: join: no seeds reachable (seeds=%v)", cfg.Seeds)
			}
		}
	}
	m.ml = ml

	// Belt-and-suspenders cache seed for the LOCAL node only, built from our
	// OWN config (id + GRPCAddr; not draining at Open) - never read from a
	// live memberlist Node. NotifyJoin already upserts the local node (and
	// every peer) synchronously during Create/Join, so this only guarantees
	// self is present the instant Open returns. We must NOT iterate
	// ml.Members() and nodeToMember() it here: that reads Node.Meta outside
	// any memberlist lock and RACES memberlist's gossip goroutines
	// (packetHandler -> aliveNode concurrently mutates Node.Meta - confirmed
	// by -race, see nodeToMember's contract). Peers are reconciled solely via
	// the memberlist-serialized NotifyJoin/Leave/Update callbacks, the only
	// safe place to read a Node's fields.
	// Keyed by the UNIQUE memberlist name (mlName), not the stable id, so it
	// matches the eventDelegate's keying; the projected Member carries the
	// stable id (cfg.NodeID) and this process's boot epoch.
	self := Member{ID: cfg.NodeID, Addr: cfg.GRPCAddr, Joining: cfg.Joining, DeclaredUnitCount: cfg.DeclaredUnitCount, epoch: bootEpoch}
	m.cacheMu.Lock()
	if _, ok := m.cache[mlName]; !ok {
		m.cache[mlName] = self
	}
	m.cacheMu.Unlock()

	// Start the periodic seed re-Join (anti-entropy that heals a
	// post-startup gossip split, e.g. after a mass rolling restart). Only
	// runs when there is a seed to re-bridge to AND a non-zero interval was
	// configured; tests that do not churn addresses leave RejoinInterval 0
	// to disable it.
	if len(cfg.Seeds) > 0 && cfg.RejoinInterval > 0 {
		m.rejoinDone = make(chan struct{})
		m.rejoinWG.Add(1)
		go m.rejoinLoop(cfg.Seeds, cfg.RejoinInterval)
	}

	// Start the periodic local-Meta re-broadcast (heals a stale-Meta peer
	// view after a staggered rolling restart - the reset-incarnation case).
	// Unlike rejoin it does NOT require seeds (a node re-publishes its OWN
	// metadata to whatever peers it is already connected to), only a non-zero
	// interval; in-process tests that do not churn addresses leave it 0.
	if cfg.MetaRefreshInterval > 0 {
		m.metaRefreshDone = make(chan struct{})
		m.metaRefreshWG.Add(1)
		go m.metaRefreshLoop(cfg.MetaRefreshInterval)
	}

	return m, nil
}

// rejoinLoop periodically re-contacts the seeds via memberlist.Join to
// re-bridge a gossip split. Join is idempotent: a no-op when the seed is
// already a known live member, a MERGE (PushPull reconciles the two member
// lists) when the ring has fragmented. The loop exits when rejoinDone is
// closed (by Close) and SKIPS the Join while the node is leaving or closed
// (a departing node must not re-advertise itself back into the cluster).
func (m *Membership) rejoinLoop(seeds []string, interval time.Duration) {
	defer m.rejoinWG.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-m.rejoinDone:
			return
		case <-t.C:
			m.mu.Lock()
			skip := m.closed || m.leaving
			m.mu.Unlock()
			if skip {
				m.rejoinSkips.Add(1)
				continue
			}
			before := len(m.ml.Members())
			if _, err := m.ml.Join(seeds); err != nil {
				// Best-effort: a transient seed-unreachable error is
				// expected during churn; the next tick retries.
				continue
			}
			m.rejoinAttempts.Add(1)
			if after := len(m.ml.Members()); after > before {
				// Only log when the re-Join actually merged new members
				// (a split was healed); the healthy-cluster no-op stays
				// quiet. memberlist's logger is the package's only sink.
				if m.cfg.LogOutput != nil {
					_, _ = fmt.Fprintf(m.cfg.LogOutput, "[DEBUG] membership: rejoin merged members (%d -> %d) node=%s\n", before, after, m.cfg.NodeID)
				}
			}
		}
	}
}

// RejoinAttempts returns how many times the periodic rejoin loop has
// actually called memberlist.Join (the node was alive + not leaving).
// Observability hook for tests + debugging; 0 when no rejoin loop runs.
func (m *Membership) RejoinAttempts() uint64 { return m.rejoinAttempts.Load() }

// RejoinSkips returns how many rejoin ticks were skipped because the node
// was leaving or closing. Observability hook for tests + debugging.
func (m *Membership) RejoinSkips() uint64 { return m.rejoinSkips.Load() }

// metaRefreshLoop periodically re-broadcasts THIS node's own Meta payload via
// memberlist.UpdateNode, so a peer holding a STALE view of this node's Meta
// (the reset-incarnation case after a staggered rolling restart) self-heals on
// a bounded interval. UpdateNode re-reads the local NodeMeta and broadcasts a
// fresh alive message with a BUMPED process-local incarnation each call, so
// repeated ticks monotonically climb the incarnation and within a bounded
// number of rounds overtake whatever stale value a peer remembered for this
// name - clearing memberlist's incarnation-staleness rejection. The local
// node's own cache is already authoritative for its Meta, so this changes
// nothing locally; it only re-publishes. The loop exits when metaRefreshDone
// is closed (by Close) and SKIPS the broadcast while the node is leaving or
// closed (a departing node must not re-advertise itself - the same guard the
// rejoin loop applies).
func (m *Membership) metaRefreshLoop(interval time.Duration) {
	defer m.metaRefreshWG.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-m.metaRefreshDone:
			return
		case <-t.C:
			m.mu.Lock()
			skip := m.closed || m.leaving
			m.mu.Unlock()
			if skip {
				m.metaRefreshSkips.Add(1)
				continue
			}
			// UpdateNode re-reads NodeMeta (unchanged on a steady node) and
			// re-broadcasts it with a bumped incarnation. The timeout is BOUNDED
			// and non-zero on purpose: memberlist reads 0 as "wait forever" (NOT
			// "do not wait"), and with a live peer UpdateNode blocks on the
			// broadcast's notify channel, which never fires if the next tick
			// supersedes this broadcast before it transmits - a 0 timeout would
			// wedge this goroutine permanently. metaUpdateTimeout caps the wait;
			// a tick that times out (or hits a racing-shutdown error) still
			// re-broadcast via normal gossip and is a best-effort no-op, so it
			// counts as an attempt and the next tick retries with a higher
			// incarnation. See metaUpdateTimeout.
			_ = m.ml.UpdateNode(metaUpdateTimeout)
			m.metaRefreshAttempts.Add(1)
		}
	}
}

// MetaRefreshAttempts returns how many times the periodic meta-refresh loop
// has actually called memberlist.UpdateNode (the node was alive + not
// leaving). Observability hook for tests + debugging; 0 when no refresh loop
// runs.
func (m *Membership) MetaRefreshAttempts() uint64 { return m.metaRefreshAttempts.Load() }

// MetaRefreshSkips returns how many meta-refresh ticks were skipped because
// the node was leaving or closing. Observability hook for tests + debugging.
func (m *Membership) MetaRefreshSkips() uint64 { return m.metaRefreshSkips.Load() }

// Members returns a snapshot of currently known cluster members,
// sorted by ID for deterministic iteration. Returns nil if the
// Membership has been Closed (defensive: callers may still hold a
// reference; nil is a safe sentinel they can already handle).
//
// Reads from the internal cache populated by NotifyJoin / NotifyLeave
// / NotifyUpdate, NOT from memberlist.Members(). memberlist's
// *Node fields mutate from its own goroutines without a per-Node
// lock, so dereferencing them concurrently is a data race the race
// detector catches. The event callbacks are serialized vs those
// internal mutations, so the cache is consistent with the
// authoritative view memberlist publishes via events.
//
// The cache is keyed by the unique memberlist node NAME; this PROJECTS it to
// one Member per STABLE id so the ring (which keys on Member.ID) sees exactly
// one entry per logical node. When two memberlist names momentarily share a
// stable id (the restart window: old process + new process), the projection
// keeps the entry with the HIGHEST epoch (the newest process), so consumers
// observe the restarted node's fresh address + declared count rather than the
// dead instance's stale view.
func (m *Membership) Members() []Member {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	m.cacheMu.RLock()
	byID := make(map[string]Member, len(m.cache))
	for _, mem := range m.cache {
		if cur, ok := byID[mem.ID]; !ok || mem.epoch > cur.epoch {
			byID[mem.ID] = mem
		}
	}
	m.cacheMu.RUnlock()
	out := make([]Member, 0, len(byID))
	for _, mem := range byID {
		out = append(out, mem)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Snapshot returns the authoritative current member list, identical
// to Members(). Exposed as a distinct name so reconciliation call
// sites (which use it to recover from event-channel drops) read
// clearly at the call site: "fetch the truth, ignore what events
// said." Same closed-aware semantics as Members.
func (m *Membership) Snapshot() []Member { return m.Members() }

// DropCount returns the cumulative number of join/leave events the
// event channel has dropped under backpressure since Open. Useful for
// observability + for tests verifying the reconciliation path covers
// for drops.
func (m *Membership) DropCount() uint64 {
	return m.events.dropCount.Load()
}

// Events returns the channel join/leave events are published on. The
// channel is closed when Membership is Closed; subscribers should
// range over it. Events may be dropped under backpressure - use
// Members() to reconcile if a consumer falls behind.
func (m *Membership) Events() <-chan Event {
	return m.events.out
}

// SetDraining flips the LOCAL node's draining bit and gossips the change
// to peers. A draining node STAYS a full, alive, addressable member - it
// keeps serving gRPC and keeps appearing in every node's Snapshot with
// its Addr intact - but advertises that it is YIELDING OWNERSHIP, so the
// ring-from-membership reconcile on every node drops it from the
// consistent-hash ownership ring and redistributes its positions to
// non-draining members. This is the foundation of the graceful-leave
// (scale-down) DRAINING node-state; it is NOT Leave() (the transport
// stays up and the node is not marked closed).
//
// The flag rides in the same Meta payload that already carries the gRPC
// address. SetDraining updates the local metaDelegate then calls
// memberlist.UpdateNode so memberlist re-reads NodeMeta + gossips the new
// payload. If the value is unchanged, it is a no-op (no gossip churn).
// No-op (returns nil) once the Membership is Closed.
func (m *Membership) SetDraining(draining bool) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	if !m.meta.setDraining(draining) {
		return nil
	}
	// Update THIS node's OWN cache entry directly so the local Snapshot
	// reflects the new draining bit immediately. memberlist's UpdateNode
	// broadcasts the new Meta to peers (who learn it via their NotifyUpdate),
	// but it does NOT reliably fire a local NotifyUpdate for the node itself -
	// so without this the local node's own cache (and therefore its ring
	// reconcile) would never observe its own draining state and would keep
	// owning the positions it is trying to yield.
	m.cacheMu.Lock()
	if m.cache != nil {
		// Keyed by THIS node's unique memberlist name (mlName), matching the
		// self-seed + eventDelegate keying; the entry's Member.ID stays the
		// stable id.
		if cur, ok := m.cache[m.mlName]; ok {
			cur.Draining = draining
			m.cache[m.mlName] = cur
		}
	}
	m.cacheMu.Unlock()
	// UpdateNode re-reads NodeMeta (now reflecting the flag) and gossips it,
	// BOUNDED by metaUpdateTimeout. The bound is load-bearing: memberlist treats
	// a 0 timeout as "wait forever" (NOT "do not wait"), and with any remembered
	// alive peer UpdateNode blocks on the broadcast's notify channel, which never
	// fires if a concurrent same-node broadcast (the meta-refresh loop) SUPERSEDES
	// this one before it transmits, or if the peers are unreachable mid-teardown -
	// wedging the calling goroutine permanently (the exact hazard metaUpdateTimeout
	// documents for the refresh loop). A timed-out update is best-effort, not an
	// error: the new Meta is already queued and still propagates via normal gossip.
	return m.ml.UpdateNode(metaUpdateTimeout)
}

// SetJoining flips the LOCAL node's joining bit and gossips the change to peers.
// A joining node is WARMING: it stays a full, alive, addressable member and a
// real (pending) OWNER of the positions it is mounting, but advertises that it
// has not yet mounted every position it owns, so every node's per-op current /
// pending split EXCLUDES it from the CURRENT set (subject to a quorum floor) -
// the still-mounted displaced owner stays the stable holder and this node
// acquires its positions via the background pending-owner path. It is the
// entry-side mirror of SetDraining; like SetDraining it does NOT leave the
// cluster (the transport stays up and the node is not marked closed).
//
// The flag rides in the same Meta payload that already carries the gRPC address
// and the draining bit. SetJoining updates the local metaDelegate then calls
// memberlist.UpdateNode so memberlist re-reads NodeMeta + gossips the new
// payload. If the value is unchanged, it is a no-op (no gossip churn). No-op
// (returns nil) once the Membership is Closed.
func (m *Membership) SetJoining(joining bool) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	if !m.meta.setJoining(joining) {
		return nil
	}
	// Update THIS node's OWN cache entry directly so the local Snapshot reflects
	// the new joining bit immediately (the same reason SetDraining does: memberlist
	// does not reliably fire a local NotifyUpdate for the node itself, so without
	// this the node's own ring reconcile would never observe its own joining state).
	m.cacheMu.Lock()
	if m.cache != nil {
		if cur, ok := m.cache[m.mlName]; ok {
			cur.Joining = joining
			m.cache[m.mlName] = cur
		}
	}
	m.cacheMu.Unlock()
	// BOUNDED UpdateNode (see SetDraining for the full rationale): a 0 timeout is
	// memberlist's "wait forever", and the Joining-bit CLEAR runs on the reconcile
	// cadence where a meta-refresh re-broadcast can supersede it (its notify never
	// fires) or the cluster can be mid-teardown (peers unreachable) - either way
	// the reconcile goroutine that called this would wedge in UpdateNode forever
	// (caught by goleak as a leaked settle-timer reconcile blocked 1+ min in
	// UpdateNode during the integration suite's teardown). The queued broadcast
	// still propagates via normal gossip after a timeout, so bounding is safe.
	return m.ml.UpdateNode(metaUpdateTimeout)
}

// Leave broadcasts the graceful departure to peers WITHOUT shutting the
// local transport down. memberlist exposes Leave (gossip the clean leave
// so peers record a graceful departure and re-own this node's units)
// DISTINCT from Shutdown (tear down the transport). Splitting them lets a
// graceful-leave drain broadcast the departure (so survivors begin
// re-owning + forwarding to this node) while THIS node keeps serving gRPC
// and keeps being forwarded-to throughout the drain; the transport is torn
// down later by Close.
//
// Idempotent + safe to call before Close: Close's own Leave is a no-op
// once memberlist has already left. Leave is best-effort (bounded by
// leaveTimeout); peers still observe the departure via failure detection
// even if the graceful broadcast did not land. It does NOT mark the
// Membership closed (the transport stays up), so Members / Snapshot keep
// returning the live view.
func (m *Membership) Leave() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	// Mark leaving so the periodic rejoin loop stops re-Joining: a node
	// that has gracefully announced its departure must not re-advertise
	// itself back into the cluster it is draining out of.
	m.leaving = true
	m.mu.Unlock()
	return m.ml.Leave(leaveTimeout)
}

// Close performs a graceful Leave (best-effort broadcast to peers)
// followed by Shutdown of the local memberlist. Idempotent.
//
// Order matters: Leave + Shutdown both trigger eventDelegate
// callbacks (memberlist re-fires NotifyLeave for the local node on
// graceful exit), so we must drive the channel shutdown LAST,
// after all callback-firing memberlist activity has wound down.
//
// Leave here is idempotent with a prior Leave() call (the graceful-leave
// drain may have already broadcast the departure): memberlist's second
// Leave is a no-op, so Close still runs Shutdown to tear the transport
// down.
func (m *Membership) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	rejoinDone := m.rejoinDone
	metaRefreshDone := m.metaRefreshDone
	m.mu.Unlock()

	// Stop the periodic background goroutines and wait for them to exit
	// BEFORE tearing the transport down, so neither can call ml.Join /
	// ml.UpdateNode on a shut-down memberlist. closed=true (set above)
	// already makes any in-flight tick skip; closing the channel wakes a
	// blocked select.
	if rejoinDone != nil {
		close(rejoinDone)
		m.rejoinWG.Wait()
	}
	if metaRefreshDone != nil {
		close(metaRefreshDone)
		m.metaRefreshWG.Wait()
	}

	// Best-effort graceful leave; ignore errors so Shutdown still runs.
	// A bounded timeout (not 0) is load-bearing: Leave(0) blocks forever
	// waiting for the leave broadcast when a peer is still alive but the
	// broadcast does not complete promptly. See leaveTimeout.
	_ = m.ml.Leave(leaveTimeout)
	err := m.ml.Shutdown()
	m.events.shutdown()
	return err
}

// splitHostPort parses a "host:port" or ":port" string into its
// pieces; host may be empty meaning "all interfaces".
func splitHostPort(s string) (host string, port int, err error) {
	h, p, err := net.SplitHostPort(s)
	if err != nil {
		return "", 0, err
	}
	pn, err := strconv.Atoi(p)
	if err != nil {
		return "", 0, fmt.Errorf("port %q: %w", p, err)
	}
	return h, pn, nil
}
