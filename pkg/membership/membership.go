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
// payload that the graceful-leave path uses.
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
	// ID is the unique node identifier (matches Config.NodeID).
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
	// DeclaredUnitCount is the node's STANDING declared shard (unit) count -
	// the value the operator configured for it (SHALE_UNIT_COUNT). It is
	// gossiped in the Meta payload so the cluster can detect cluster-wide
	// AGREEMENT on a desired count and drive a declarative reshard toward it
	// (see the cluster's observeDeclaredReshardTarget). 0 means UNKNOWN /
	// absent (a legacy node that does not gossip it, or a non-multi-backend
	// node); an unknown count breaks unanimity and defers any auto-reshard,
	// the fail-safe. Static for the node's lifetime.
	DeclaredUnitCount uint32
}

// metaSep separates the address head from the optional draining marker
// in the Meta wire format. A NUL never appears in a host:port address,
// so it is an unambiguous delimiter and keeps the non-draining payload
// byte-identical to the bare-address legacy format.
const metaSep = '\x00'

// metaDrainingMarker is the trailing segment that flags a draining node.
const metaDrainingMarker = "D"

// metaUnitCountPrefix tags the trailing segment carrying a node's standing
// DECLARED unit count, e.g. "U16". It is its own NUL-delimited segment after
// the optional draining marker, so it composes with draining and is ignored
// by any decoder that does not understand it (forward-compatible). A NUL
// never appears in the decimal count, so the segment is unambiguous.
const metaUnitCountPrefix = "U"

// encodeMeta builds the Meta payload for the local node. The head is the
// address (legacy-identical bare-address form when there is nothing else to
// carry). A draining node appends metaSep + the draining marker; a node with
// a known declared unit count (> 0) appends metaSep + "U<count>". Segments are
// independent, so the payload may carry neither, either, or both.
func encodeMeta(addr string, draining bool, declaredUnitCount uint32) []byte {
	s := addr
	if draining {
		s += string(metaSep) + metaDrainingMarker
	}
	if declaredUnitCount > 0 {
		s += string(metaSep) + metaUnitCountPrefix + strconv.FormatUint(uint64(declaredUnitCount), 10)
	}
	return []byte(s)
}

// decodeMeta parses a Meta payload back into the address, draining bit, and
// declared unit count. The head (up to the first metaSep, or the whole payload
// if none) is always the address; a trailing metaDrainingMarker segment sets
// draining, a trailing "U<count>" segment sets the declared unit count.
// Unknown / unparseable trailing segments are ignored (forward-compatible);
// an absent count segment yields 0 (UNKNOWN).
func decodeMeta(meta []byte) (addr string, draining bool, declaredUnitCount uint32) {
	s := string(meta)
	head, rest, found := strings.Cut(s, string(metaSep))
	if !found {
		return s, false, 0
	}
	for _, seg := range strings.Split(rest, string(metaSep)) {
		switch {
		case seg == metaDrainingMarker:
			draining = true
		case strings.HasPrefix(seg, metaUnitCountPrefix):
			if n, err := strconv.ParseUint(seg[len(metaUnitCountPrefix):], 10, 32); err == nil {
				declaredUnitCount = uint32(n)
			}
		}
	}
	return head, draining, declaredUnitCount
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
}

// DefaultRejoinInterval is the production re-Join cadence used when a
// caller wires periodic seed anti-entropy without an explicit interval.
// 30s matches memberlist's DefaultLANConfig PushPull cadence: frequent
// enough that a mass-restart gossip split heals within a couple of
// gossip rounds, infrequent enough that the idempotent no-op Join on a
// healthy cluster is negligible overhead.
const DefaultRejoinInterval = 30 * time.Second

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

	// cache holds the authoritative Member snapshot keyed by node ID.
	// Reads (Members, Snapshot) consult this map under cacheMu instead
	// of dereferencing memberlist.Node pointers, whose Name + Meta
	// fields memberlist mutates from its own goroutines without a
	// per-Node lock. The cache is populated from inside memberlist's
	// NotifyJoin / NotifyLeave / NotifyUpdate callbacks (which run
	// serialized vs the alive/dead transitions, so reading Node fields
	// THERE is safe), plus a single seeding pass at Open time before
	// any external goroutine can observe Membership.
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
	// declaredUnitCount is the node's standing declared shard count. Unlike
	// draining it is fixed for the node's lifetime (set at construction from
	// Config.DeclaredUnitCount); it is read under mu only to share the lock
	// with the draining read in NodeMeta.
	declaredUnitCount uint32
}

func (d *metaDelegate) NodeMeta(limit int) []byte {
	d.mu.Lock()
	meta := encodeMeta(d.addr, d.draining, d.declaredUnitCount)
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

// nodeToMember derives our Member value object from a memberlist.Node.
// MUST ONLY be called either (a) at Open() during the single-writer
// seeding window before goroutines spawn, or (b) inside a memberlist
// event callback (NotifyJoin/Leave/Update), where memberlist serializes
// the call against its own alive/dead transitions and Node-field
// mutation. Calling this from a reconcile / Members / Snapshot path
// races with memberlist's internal aliveNode writes; the cache exists
// precisely to avoid that.
func nodeToMember(n *memberlist.Node) Member {
	name := strings.Clone(n.Name)
	meta := append([]byte(nil), n.Meta...)
	addr, draining, declaredUnitCount := decodeMeta(meta)
	return Member{
		ID:                name,
		Addr:              addr,
		Draining:          draining,
		DeclaredUnitCount: declaredUnitCount,
	}
}

func (e *eventDelegate) NotifyJoin(n *memberlist.Node) {
	m := nodeToMember(n)
	e.upsertCache(m)
	e.send(EventJoin, m)
}

func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	m := nodeToMember(n)
	e.removeCache(m.ID)
	e.send(EventLeave, m)
}

func (e *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	// Treat metadata updates as a join refresh: the addressing info
	// for the node may have changed (e.g. GRPCAddr migrated).
	m := nodeToMember(n)
	e.upsertCache(m)
	e.send(EventJoin, m)
}

// upsertCache writes the given Member into the parent Membership's
// cache. No-op if no parent is attached (the synthetic-delegate test
// path). Late stray callbacks after Close are harmless: Members()
// already returns nil once the parent's closed flag is set, so the
// post-Close cache state is unobservable.
func (e *eventDelegate) upsertCache(m Member) {
	if e.parent == nil {
		return
	}
	e.parent.cacheMu.Lock()
	if e.parent.cache == nil {
		e.parent.cache = make(map[string]Member, 4)
	}
	e.parent.cache[m.ID] = m
	e.parent.cacheMu.Unlock()
}

// removeCache deletes the given ID from the parent Membership's cache.
// No-op if no parent is attached. See upsertCache for the post-Close
// rationale.
func (e *eventDelegate) removeCache(id string) {
	if e.parent == nil {
		return
	}
	e.parent.cacheMu.Lock()
	delete(e.parent.cache, id)
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

	mlCfg := gossipConfig()
	mlCfg.Name = cfg.NodeID
	if bindHost != "" {
		mlCfg.BindAddr = bindHost
		mlCfg.AdvertiseAddr = bindHost
	}
	mlCfg.BindPort = bindPort
	mlCfg.AdvertisePort = bindPort
	meta := &metaDelegate{addr: cfg.GRPCAddr, declaredUnitCount: cfg.DeclaredUnitCount}
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
	self := Member{ID: cfg.NodeID, Addr: cfg.GRPCAddr, DeclaredUnitCount: cfg.DeclaredUnitCount}
	m.cacheMu.Lock()
	if _, ok := m.cache[self.ID]; !ok {
		m.cache[self.ID] = self
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
func (m *Membership) Members() []Member {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	m.cacheMu.RLock()
	out := make([]Member, 0, len(m.cache))
	for _, mem := range m.cache {
		out = append(out, mem)
	}
	m.cacheMu.RUnlock()
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
		if cur, ok := m.cache[m.cfg.NodeID]; ok {
			cur.Draining = draining
			m.cache[m.cfg.NodeID] = cur
		}
	}
	m.cacheMu.Unlock()
	// UpdateNode re-reads NodeMeta (now reflecting the flag) and gossips
	// it. The 0 timeout means "do not block waiting for the broadcast to
	// flush"; the change still propagates via the normal gossip loop.
	return m.ml.UpdateNode(0)
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
	m.mu.Unlock()

	// Stop the periodic rejoin goroutine and wait for it to exit BEFORE
	// tearing the transport down, so it can never call ml.Join on a
	// shut-down memberlist. closed=true (set above) already makes any
	// in-flight tick skip; closing the channel wakes a blocked select.
	if rejoinDone != nil {
		close(rejoinDone)
		m.rejoinWG.Wait()
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
