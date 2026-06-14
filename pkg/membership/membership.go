// Package membership wraps hashicorp/memberlist + exposes a small,
// shale-shaped API to the rest of the system: a snapshot of current
// Members + a channel of join/leave Events. It is deliberately I/O
// free at the type layer (Member is a plain value object) and does
// NOT own routing or the hash ring - the cluster layer subscribes to
// Events + rebuilds its ring in response.
//
// Each node advertises its gRPC listen address via memberlist's
// per-node Meta bytes; peers decode that on receipt so they know
// where to forward requests. The format is a plain UTF-8 string.
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
// GRPCAddr as the node's Meta payload. The other Delegate methods are
// no-ops; we do not use memberlist's user-message channel.
type metaDelegate struct {
	meta []byte
}

func (d *metaDelegate) NodeMeta(limit int) []byte {
	if len(d.meta) > limit {
		return d.meta[:limit]
	}
	return d.meta
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
	return Member{
		ID:   name,
		Addr: string(meta),
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

	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.Name = cfg.NodeID
	if bindHost != "" {
		mlCfg.BindAddr = bindHost
		mlCfg.AdvertiseAddr = bindHost
	}
	mlCfg.BindPort = bindPort
	mlCfg.AdvertisePort = bindPort
	mlCfg.Delegate = &metaDelegate{meta: []byte(cfg.GRPCAddr)}
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

// Close performs a graceful Leave (best-effort broadcast to peers)
// followed by Shutdown of the local memberlist. Idempotent.
//
// Order matters: Leave + Shutdown both trigger eventDelegate
// callbacks (memberlist re-fires NotifyLeave for the local node on
// graceful exit), so we must drive the channel shutdown LAST,
// after all callback-firing memberlist activity has wound down.
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
