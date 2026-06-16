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
}

// metaSep separates the address head from the optional draining marker
// in the Meta wire format. A NUL never appears in a host:port address,
// so it is an unambiguous delimiter and keeps the non-draining payload
// byte-identical to the bare-address legacy format.
const metaSep = '\x00'

// metaDrainingMarker is the trailing segment that flags a draining node.
const metaDrainingMarker = "D"

// encodeMeta builds the Meta payload for the local node. A non-draining
// node encodes exactly its address (legacy-identical). A draining node
// appends metaSep + the draining marker.
func encodeMeta(addr string, draining bool) []byte {
	if !draining {
		return []byte(addr)
	}
	return []byte(addr + string(metaSep) + metaDrainingMarker)
}

// decodeMeta parses a Meta payload back into the address + draining bit.
// The head (up to the first metaSep, or the whole payload if none) is
// always the address; a trailing metaDrainingMarker segment sets
// draining. Unknown trailing segments are ignored (forward-compatible).
func decodeMeta(meta []byte) (addr string, draining bool) {
	s := string(meta)
	head, rest, found := strings.Cut(s, string(metaSep))
	if !found {
		return s, false
	}
	for _, seg := range strings.Split(rest, string(metaSep)) {
		if seg == metaDrainingMarker {
			draining = true
		}
	}
	return head, draining
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
	// LogOutput is where memberlist's internal logger writes. nil
	// means stderr (memberlist's default). Tests pass io.Discard to
	// stay quiet.
	LogOutput io.Writer
}

// Membership is the running gossip layer for one node. All exported
// methods are safe for concurrent use.
type Membership struct {
	cfg Config

	ml     *memberlist.Memberlist
	events *eventDelegate
	meta   *metaDelegate

	mu     sync.Mutex
	closed bool

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
}

func (d *metaDelegate) NodeMeta(limit int) []byte {
	d.mu.Lock()
	meta := encodeMeta(d.addr, d.draining)
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
	addr, draining := decodeMeta(meta)
	return Member{
		ID:       name,
		Addr:     addr,
		Draining: draining,
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
	meta := &metaDelegate{addr: cfg.GRPCAddr}
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
		if err != nil {
			_ = ml.Shutdown()
			return nil, fmt.Errorf("membership: join: %w", err)
		}
		if contacted == 0 {
			_ = ml.Shutdown()
			return nil, fmt.Errorf("membership: join: no seeds reachable (seeds=%v)", cfg.Seeds)
		}
	}
	m.ml = ml

	// Belt-and-suspenders cache seed. The NotifyJoin path SHOULD have
	// inserted every currently-known node already (memberlist fires
	// it during Create + Join for the local node and each peer), but
	// pulling from memberlist.Members() once here covers the
	// theoretical window between Join returning and the final
	// NotifyJoin draining through. This is the ONE call site outside
	// the event callbacks that reads Node fields directly: it is
	// safe only because no other Membership consumer has a handle
	// yet (Open has not returned), so memberlist's gossip goroutines
	// are the only writers and their NotifyJoin/Leave is what we are
	// reconciling against.
	for _, n := range ml.Members() {
		mem := nodeToMember(n)
		m.cacheMu.Lock()
		if _, ok := m.cache[mem.ID]; !ok {
			m.cache[mem.ID] = mem
		}
		m.cacheMu.Unlock()
	}

	return m, nil
}

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
	m.mu.Unlock()

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
