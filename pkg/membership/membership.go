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
	"sync"
	"sync/atomic"

	"github.com/hashicorp/memberlist"
)

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
}

func (e *eventDelegate) NotifyJoin(n *memberlist.Node) {
	e.send(EventJoin, n)
}

func (e *eventDelegate) NotifyLeave(n *memberlist.Node) {
	e.send(EventLeave, n)
}

func (e *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	// Treat metadata updates as a join refresh: the addressing info
	// for the node may have changed (e.g. GRPCAddr migrated).
	e.send(EventJoin, n)
}

func (e *eventDelegate) send(t EventType, n *memberlist.Node) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return
	}
	ev := Event{Type: t, Member: nodeToMember(n)}
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

	return &Membership{
		cfg:    cfg,
		ml:     ml,
		events: events,
	}, nil
}

// Members returns a snapshot of currently known cluster members,
// sorted by ID for deterministic iteration. Returns nil if the
// Membership has been Closed (defensive: the underlying memberlist
// would race with shutdown; nil is a safe sentinel callers can
// already handle).
func (m *Membership) Members() []Member {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	nodes := m.ml.Members()
	out := make([]Member, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, nodeToMember(n))
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
	_ = m.ml.Leave(0)
	err := m.ml.Shutdown()
	m.events.shutdown()
	return err
}

// nodeToMember converts memberlist's Node into our value object,
// decoding the Meta payload as the gRPC address.
func nodeToMember(n *memberlist.Node) Member {
	return Member{
		ID:   n.Name,
		Addr: string(n.Meta),
	}
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
