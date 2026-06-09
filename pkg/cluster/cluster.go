// Package cluster is the public API surface of shale. Apps import
// this package + call cluster.Open(cfg) to get a Cluster handle that
// routes KV operations across nodes.
//
// Two modes:
//
//   - single-node: cfg.BindAddr empty. Cluster delegates every op
//     directly to the local Backend. Useful for tests, embedded apps
//     that don't yet want gossip, and the v0.1 baseline.
//   - multi-node:  cfg.BindAddr non-empty. Cluster brings up a
//     membership.Membership on BindAddr (joining via cfg.Seeds) + a
//     ring.Ring populated from membership events. Single-key ops
//     hash the key through the ring; if the owner is the local node
//     they hit the local Backend, otherwise they are forwarded to
//     the owner's gRPC service via a cached rpc.Client.
//
// v0.2 static topology: membership join/leave events update the ring
// but no key data moves between nodes. A node returning to an already-
// populated cluster sees only newly-written keys. Rebalancing lands
// in v0.3.
//
// See docs/SPEC.md for the full model.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/membership"
	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/ring"
)

// reconcileInterval is how often the cluster polls membership for a
// full snapshot and reapplies any add/remove the event-channel layer
// may have missed under backpressure. Small enough that a dropped
// event recovers within seconds; large enough to be free in steady
// state. Exposed as a var (not const) so tests can shrink it.
var reconcileInterval = 5 * time.Second

// ErrEmptyValue is returned by Put when value is nil or zero-length.
// Use Delete to remove a key; the envelope's empty-payload shape is
// reserved for Delete tombstones and storing one via Put would surface
// as NotFound on subsequent Get calls (R>1) or empty bytes (R=1),
// silently splitting Put-with-empty into two different semantics by
// replication factor.
var ErrEmptyValue = errors.New("cluster: Put with empty value; use Delete to remove a key")

// WriteConsistency picks how many replica acks a Put / Delete waits
// for before returning. iota+1 so the zero value is "unset" + Open
// normalizes to the v0.4 default (WriteQuorum).
type WriteConsistency int

const (
	// WriteOne returns success as soon as the primary acks. Loosest
	// setting + lowest write latency; tolerates the most replica
	// failures but offers the weakest durability.
	WriteOne WriteConsistency = iota + 1
	// WriteQuorum waits for floor(R/2)+1 acks. The v0.4 default:
	// survives the loss of a minority of replicas without losing the
	// write.
	WriteQuorum
	// WriteAll waits for every replica to ack. Any down replica fails
	// the write.
	WriteAll
)

// ReadConsistency picks how many replicas a Get reads from. iota+1
// so the zero value is "unset" + Open normalizes to the v0.4 default
// (ReadNearest).
type ReadConsistency int

const (
	// ReadNearest reads from the primary only (one hop in the common
	// case; matches v0.3 R=1 behavior). No read-repair fires on
	// ReadNearest since there is nothing to compare against.
	ReadNearest ReadConsistency = iota + 1
	// ReadQuorum reads from floor(R/2)+1 replicas + returns the LWW
	// winner. Triggers async read-repair on lagging replicas.
	ReadQuorum
	// ReadAll reads from every replica. Strongest read freshness;
	// triggers read-repair on any disagreement.
	ReadAll
)

// Config configures a Cluster. NodeID + Backend are always required.
// The peer-discovery fields (BindAddr, Seeds, GRPCAddr) are required
// only for multi-node mode; in single-node mode they may be empty.
type Config struct {
	// NodeID is this node's stable identity. Used in membership +
	// ring placement. MUST be unique within the cluster.
	NodeID string

	// Backend is the local KV engine this node owns. Required.
	Backend backend.Backend

	// BindAddr is the host:port memberlist listens on (UDP + TCP).
	// Non-empty enables multi-node mode. Format: "host:port" (host
	// may be empty for "all interfaces").
	BindAddr string

	// GRPCAddr is this node's gRPC service address, broadcast to
	// peers as their forwarding target. Required in multi-node mode.
	// Format: "host:port" (host should be a routable address; an
	// empty host works for loopback tests but won't reach peers on
	// other machines).
	GRPCAddr string

	// Seeds are addresses of already-running cluster nodes (their
	// BindAddr). Bootstrap gossip contacts these to discover the
	// rest of the membership. Empty means this node is the seed.
	Seeds []string

	// ShardKeyFn lets the app extract a shard key from a full key.
	// Default = hashTagged identity (full key, honoring `{tag}`
	// hash tags). Override for custom shard key shapes.
	ShardKeyFn func(key []byte) []byte

	// LogOutput is where membership's internal logger writes. nil
	// means memberlist's default (stderr). Tests pass io.Discard.
	LogOutput io.Writer

	// RebalanceSettleDelay is how long the cluster waits after a
	// membership event before kicking off Evaluate. Each subsequent
	// event in the window resets the timer (debounce). Zero falls
	// back to the package default (5s; matches docs/SPEC.md).
	RebalanceSettleDelay time.Duration

	// RebalanceRetryAfterMs is the hint returned in the
	// FailedPrecondition error when a Put / Delete lands on a key
	// whose partition is migrating out. Clients SHOULD wait this
	// many ms before retrying. Zero falls back to 50ms (matches
	// rebalance.DefaultOptions).
	RebalanceRetryAfterMs int

	// RebalanceGraceDuration is how long a HandedOff range stays on
	// the source before the sweep deletes its now-foreign keys. Zero
	// falls back to rebalance.DefaultOptions (30s, matching
	// docs/SPEC.md "Cutover" T_drain). Integration tests + faster-
	// feedback fixtures pass small values.
	RebalanceGraceDuration time.Duration

	// RebalanceHandoffTimeout bounds how long the source's runSend
	// waits for the destination to ack the gRPC stream. Past the
	// timeout the partition transitions Done-with-error (the next
	// Evaluate retries). Zero falls back to the package default
	// (rebalance.DefaultOptions, 5 minutes). Integration tests
	// shrink it so a "destination never asks" scenario fails fast.
	RebalanceHandoffTimeout time.Duration

	// ReplicationFactor is the number of nodes that hold a copy of
	// each key. Zero is normalized to 1 by Open (v0.3 behavior:
	// single owner per key, no replicas, no LWW envelope cost on the
	// read path). Values > 1 are clamped at fan-out time to the
	// number of distinct members in the ring (see ring.LocateKeyN).
	// HA + LWW conflict resolution is opt-in via ReplicationFactor > 1.
	ReplicationFactor int

	// WriteConsistency picks how many replica acks a Put / Delete
	// waits for. Zero is normalized to WriteQuorum by Open (the v0.4
	// default). See WriteConsistency for the per-value semantics.
	WriteConsistency WriteConsistency

	// ReadConsistency picks how many replicas a Get reads from. Zero
	// is normalized to ReadNearest by Open (the v0.4 default; matches
	// v0.3 single-replica read behavior). See ReadConsistency for the
	// per-value semantics.
	ReadConsistency ReadConsistency

	// WriteTimeout bounds the wall-clock budget for a Put / Delete
	// fanout. Each replica dispatch inherits this deadline through
	// context.WithTimeout, so a blackholed peer (hung gRPC, half-open
	// TCP) cancels at deadline rather than blocking forever. The
	// fanout still returns deterministically when the deadline fires:
	// any replica that hasn't replied is counted as a failure for the
	// purposes of the success/failure budget. Zero falls back to 5s.
	WriteTimeout time.Duration

	// ReadTimeout bounds the wall-clock budget for a Get fanout.
	// Same shape as WriteTimeout: per-dispatch deadline so a hung
	// peer doesn't block the call beyond ReadTimeout, even if the
	// configured consistency would otherwise wait for that replica.
	// Zero falls back to 5s.
	ReadTimeout time.Duration
}

// normalizeConfig fills in v0.4 default values for any zero-valued
// fields that have a defined default. Called once at the top of Open
// so the rest of the package can rely on the normalized values
// without re-checking zero everywhere. Mutates cfg in place via the
// pointer; the caller's local Config value is left as supplied (Open
// works against the normalized copy held in c.cfg).
func normalizeConfig(cfg *Config) {
	if cfg.ReplicationFactor == 0 {
		cfg.ReplicationFactor = 1
	}
	if cfg.WriteConsistency == 0 {
		cfg.WriteConsistency = WriteQuorum
	}
	if cfg.ReadConsistency == 0 {
		cfg.ReadConsistency = ReadNearest
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
}

// Cluster is the public handle apps use. All operations are
// goroutine safe.
type Cluster struct {
	cfg     Config
	backend backend.Backend

	// Populated in multi-node mode; nil in single-node mode.
	ring       *ring.Ring
	membership *membership.Membership

	clientsMu sync.RWMutex
	clients   map[string]*peerClient // peer gRPC addr -> client

	// closeCh is closed by Close exactly once to signal the events
	// loop (and any other lifecycle goroutines) to exit. closeOnce
	// guards the close so concurrent / repeated Close calls don't
	// panic on close-of-closed. closed mirrors the open/closed state
	// for fast no-lock checks from the KV path.
	closeCh   chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	loopWG    sync.WaitGroup

	// Rebalance state (multi-node only; rebalance is empty in
	// single-node mode). rebalance is the per-range Coordinator, held
	// behind an atomic.Pointer so the events / reconcile loops can
	// observe a coherent value without serializing against the rare
	// replaceCoordinator path (--cancel). ringGen is the monotonic
	// generation counter bumped on every membership-driven ring
	// change (events loop + reconcile loop both call bumpRingGen).
	// lastEvalRing is the ring snapshot from the most recent
	// Evaluate, paired with the live ring on the next tick to compute
	// the delta plan. settleMu + settleTimer drive the debounce:
	// every ring change (re)arms the timer; when it fires,
	// runEvaluate computes the plan. rebalanceCtx / rebalanceCancel
	// drive the background sweep loop.
	rebalance       atomic.Pointer[rebalance.Coordinator]
	ringGen         atomic.Uint64
	settleMu        sync.Mutex
	settleTimer     *time.Timer
	lastEvalRing    *ring.Ring
	rebalanceCtx    context.Context
	rebalanceCancel context.CancelFunc

	// repairCtx / repairCancel govern the lifetime of async read-
	// repair goroutines. Close cancels repairCtx so any in-flight
	// repair sees a canceled gRPC context + bails out; repairWG
	// blocks Close until every spawned repair has exited so a Close-
	// concurrent repair cannot try to dial through a torn-down
	// peerClient after Close has cleared c.clients. Initialized
	// unconditionally (single-node mode is a degenerate case with
	// no peers to repair against, so the goroutine count stays at 0
	// but the context still exists for safe Close coordination).
	repairCtx    context.Context
	repairCancel context.CancelFunc
	repairWG     sync.WaitGroup
}

// Open initializes a Cluster from cfg. In single-node mode it just
// records the cfg + returns the wrapper. In multi-node mode (when
// cfg.BindAddr is non-empty) it additionally brings up memberlist,
// joins seeds, seeds the ring with the local node + any peers already
// seen, and starts a goroutine that mirrors membership events into the
// ring.
func Open(cfg Config) (*Cluster, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("cluster: NodeID is required")
	}
	if len(cfg.NodeID) > MaxNodeIDLen {
		return nil, fmt.Errorf("cluster: NodeID length %d exceeds MaxNodeIDLen %d; the envelope's uint16 length prefix would silently truncate",
			len(cfg.NodeID), MaxNodeIDLen)
	}
	if cfg.Backend == nil {
		return nil, errors.New("cluster: Backend is required")
	}
	normalizeConfig(&cfg)

	c := &Cluster{
		cfg:     cfg,
		backend: cfg.Backend,
		clients: make(map[string]*peerClient),
		closeCh: make(chan struct{}),
	}
	c.repairCtx, c.repairCancel = context.WithCancel(context.Background())

	if cfg.BindAddr == "" {
		// Single-node mode: no membership, no ring, every op is local.
		return c, nil
	}

	if cfg.GRPCAddr == "" {
		return nil, errors.New("cluster: GRPCAddr is required in multi-node mode")
	}

	mem, err := membership.Open(membership.Config{
		NodeID:    cfg.NodeID,
		BindAddr:  cfg.BindAddr,
		GRPCAddr:  cfg.GRPCAddr,
		Seeds:     cfg.Seeds,
		LogOutput: cfg.LogOutput,
	})
	if err != nil {
		return nil, fmt.Errorf("cluster: membership: %w", err)
	}

	c.membership = mem
	c.ring = ring.New()

	// Seed the ring with whatever membership knows about right now
	// (always at least the local node; possibly peers if Join already
	// returned a populated snapshot).
	for _, m := range mem.Members() {
		c.ring.Add(ring.Member{ID: m.ID, Addr: m.Addr})
	}

	// Rebalance machinery: Coordinator + settle-timer + sweep.
	// initRebalance MUST run BEFORE the events / reconcile goroutines
	// spawn. Both of those call bumpRingGen -> scheduleEvaluate, which
	// reads c.rebalance + (under settleMu) c.lastEvalRing. Initializing
	// after the spawn races on those fields the very first time a
	// membership event arrives. initRebalance only touches local fields
	// from the calling goroutine, so doing it here is the safest
	// publish point.
	c.initRebalance()

	// Run the events loop so future joins / leaves keep the ring in
	// sync with membership. Also run a slower reconciliation loop
	// that re-syncs the ring against the authoritative membership
	// snapshot on a fixed cadence - belt + suspenders against the
	// rare event-channel drop (membership uses a non-blocking send
	// to preserve no-deadlock guarantees; if a consumer ever falls
	// behind a join/leave could be lost, leaving the ring stale
	// forever without this loop).
	c.loopWG.Add(2)
	go c.runEventsLoop()
	go c.runReconcileLoop()

	return c, nil
}

// runEventsLoop mirrors membership join/leave events into the ring.
// It exits when the membership events channel is closed (Close drives
// that) or closeCh is signalled. The two-channel select keeps Close
// from blocking on a slow producer.
func (c *Cluster) runEventsLoop() {
	defer c.loopWG.Done()
	events := c.membership.Events()
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return
			}
			switch ev.Type {
			case membership.EventJoin:
				// If the addr changed (NotifyUpdate path - same ID,
				// different Meta payload), evict the cached client
				// for the OLD addr so the next dial picks up the
				// new endpoint. Look up the prior addr via the ring
				// snapshot before Add overwrites it.
				oldAddr := c.priorAddrForID(ev.Member.ID)
				c.ring.Add(ring.Member{ID: ev.Member.ID, Addr: ev.Member.Addr})
				if oldAddr != "" && oldAddr != ev.Member.Addr {
					c.evictClient(oldAddr)
				}
				c.bumpRingGen()
			case membership.EventLeave:
				c.ring.Remove(ev.Member.ID)
				// Drop any cached client for this departed peer so
				// the next dial picks up a fresh connection if the
				// same node ever returns on a different address.
				c.evictClient(ev.Member.Addr)
				c.bumpRingGen()
			}
		case <-c.closeCh:
			return
		}
	}
}

// runReconcileLoop re-syncs the ring against membership.Members() on
// a fixed cadence. The event-channel layer in membership drops events
// on backpressure (intentional - keeps memberlist's callback goroutine
// unblocked); without this loop, a single dropped join would leave the
// ring permanently missing that node. With it, ring divergence
// auto-heals within one tick.
func (c *Cluster) runReconcileLoop() {
	defer c.loopWG.Done()
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-t.C:
			c.reconcileRingFromMembership()
		}
	}
}

// reconcileRingFromMembership reads the authoritative membership snapshot
// and applies any missed adds/removes to the ring. Idempotent: an
// already-present member's Add is a no-op (ring.Add overwrites the
// same Member with itself) and a non-member's absence is the desired
// state.
func (c *Cluster) reconcileRingFromMembership() {
	if c.membership == nil || c.ring == nil {
		return
	}
	snap := c.membership.Snapshot()
	want := make(map[string]ring.Member, len(snap))
	for _, m := range snap {
		want[m.ID] = ring.Member{ID: m.ID, Addr: m.Addr}
	}
	changed := false
	// Add anyone missing from the ring (or with a stale Addr).
	for id, m := range want {
		oldAddr := c.priorAddrForID(id)
		if oldAddr != m.Addr {
			c.ring.Add(m)
			if oldAddr != "" {
				c.evictClient(oldAddr)
			}
			changed = true
		}
	}
	// Remove anyone the ring still has but membership has dropped.
	for _, m := range c.ring.Members() {
		if _, ok := want[m.ID]; !ok {
			c.ring.Remove(m.ID)
			c.evictClient(m.Addr)
			changed = true
		}
	}
	if changed {
		// A reconcile-driven ring change is no different from an
		// event-driven one as far as the rebalance protocol cares;
		// arm the settle timer so the missed delta gets folded into
		// the next Evaluate.
		c.bumpRingGen()
	}
}

// priorAddrForID returns the Addr currently recorded in the ring for
// the given ID, or "" if absent. Used by the reconciliation path to
// evict a now-stale cached client when a peer's Addr changes.
func (c *Cluster) priorAddrForID(id string) string {
	if c.ring == nil {
		return ""
	}
	for _, m := range c.ring.Members() {
		if m.ID == id {
			return m.Addr
		}
	}
	return ""
}

// NodeID returns this node's stable identity, as supplied in Config.
func (c *Cluster) NodeID() string { return c.cfg.NodeID }

// Members returns a snapshot of the cluster's current ring membership.
// In single-node mode this is a single-element slice containing the
// local node with Addr=cfg.GRPCAddr (which can be empty if the caller
// didn't set GRPCAddr; no gRPC peer routing is set up in single-node
// mode regardless).
func (c *Cluster) Members() []ring.Member {
	if c.ring == nil {
		return []ring.Member{{ID: c.cfg.NodeID, Addr: c.cfg.GRPCAddr}}
	}
	return c.ring.Members()
}

// OwnsKey reports whether the local node is the ring owner of key.
// Used by the gRPC server's forwarding-loop guard: a request that
// arrives with forwarded=true but does NOT belong here is refused
// rather than re-forwarded (which would loop A->B->A on diverged
// rings).
func (c *Cluster) OwnsKey(key []byte) bool {
	_, local := c.ownerOf(key)
	return local
}

// LocalScanPrefix returns an iterator over the LOCAL backend's keys
// with the given prefix, bypassing ring routing entirely. Use this
// for admin-style operations (peer snapshotting, per-node counters)
// where the receiver explicitly wants to see what's physically on
// this node's storage, not "what the ring says belongs here".
func (c *Cluster) LocalScanPrefix(prefix []byte) (backend.Iterator, error) {
	if c.closed.Load() || c.backend == nil {
		return nil, backend.ErrClosed
	}
	return c.backend.ScanPrefix(prefix)
}

// LocalGet returns the value for key directly from the local backend,
// bypassing the cluster's owner routing. Used by the gRPC Get
// handler in the v0.3 receive-window read forwarder: when the
// destination forwards a read to us (the source) because we still
// hold the authoritative copy mid-migration, we want to serve from
// our local backend even though our own ring has moved the
// partition off us. Returns backend.ErrNotFound if the key is
// absent locally.
func (c *Cluster) LocalGet(key []byte) ([]byte, error) {
	if c.closed.Load() || c.backend == nil {
		return nil, backend.ErrClosed
	}
	return c.backend.Get(key)
}

// Close releases all cluster resources. After Close, no other method
// may be called. Idempotent + safe to call concurrently with Put/Get/
// Delete/ScanPrefix - in-flight KV ops still race-finish against the
// pre-Close backend, but ops STARTING after closed=true return
// backend.ErrClosed instead of panicking on a torn-down backend.
func (c *Cluster) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		// Another Close already ran (or is running). Wait for the
		// background loops to wind down before returning so callers
		// see a fully-stopped cluster on Close return regardless of
		// which call won the CAS.
		c.loopWG.Wait()
		return nil
	}

	var firstErr error

	// Stop the settle timer so it can't fire a fresh Evaluate after
	// the membership is torn down. Held in settleMu to coordinate
	// with scheduleEvaluate.
	c.settleMu.Lock()
	if c.settleTimer != nil {
		c.settleTimer.Stop()
		c.settleTimer = nil
	}
	c.settleMu.Unlock()

	// Stop the Coordinator + cancel the sweep context. Coordinator.Stop
	// closes its internal stopCh which terminates in-flight FetchRange
	// goroutines; cancel of rebalanceCtx terminates the sweep loop. Both
	// are idempotent.
	if rb := c.rebalance.Load(); rb != nil {
		rb.Stop()
	}
	if c.rebalanceCancel != nil {
		c.rebalanceCancel()
	}

	// Cancel any in-flight read-repair goroutines + wait for them to
	// exit BEFORE tearing down the peer-client cache. A repair that's
	// mid-PutForwarded against a cached peerClient would deadlock /
	// race / segfault when we close the conn below; canceling the
	// repair context first lets the gRPC call return cleanly, and the
	// short wait drains them with a budget so a wedged peer doesn't
	// stall Close indefinitely. 5s is enough for any in-flight repair
	// to honor ctx.Done; past that we give up and proceed, accepting
	// that the goroutine may log a "use of closed connection" error
	// (which is swallowed by the repair path anyway).
	if c.repairCancel != nil {
		c.repairCancel()
	}
	repairDone := make(chan struct{})
	go func() {
		c.repairWG.Wait()
		close(repairDone)
	}()
	select {
	case <-repairDone:
	case <-time.After(5 * time.Second):
	}

	// Signal the events + reconcile loops to exit + wait for them
	// before tearing down membership so we don't race with a late
	// ring.Add on closed ring. The sweep goroutine also drains via
	// loopWG (initRebalance adds 1 to the wait group for it).
	c.closeOnce.Do(func() { close(c.closeCh) })
	c.loopWG.Wait()

	if c.membership != nil {
		if err := c.membership.Close(); err != nil {
			firstErr = err
		}
	}

	// Tear down all cached peer clients.
	c.clientsMu.Lock()
	for addr, cli := range c.clients {
		_ = cli.Close()
		delete(c.clients, addr)
	}
	c.clientsMu.Unlock()

	// Close the backend but DO NOT nil it out: a Put racing with
	// Close could have already loaded c.backend before this point,
	// and nil-ing it under their feet would either panic or trip
	// the race detector. The backend's own Close marks it closed
	// internally + subsequent ops return backend.ErrClosed cleanly.
	// c.closed.Load() is the primary guard; this is the safety net.
	if c.backend != nil {
		if err := c.backend.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// shardKey extracts the shard key for routing per cfg.ShardKeyFn, or
// the default (ring.ShardKey, honoring `{hash-tag}`) if unset.
func (c *Cluster) shardKey(key []byte) []byte {
	if c.cfg.ShardKeyFn != nil {
		return c.cfg.ShardKeyFn(key)
	}
	return ring.ShardKey(key)
}

// ownerOf returns the ring member that owns key + a bool indicating
// whether the local node is that owner. In single-node mode (no ring)
// it reports local-ownership unconditionally.
func (c *Cluster) ownerOf(key []byte) (owner ring.Member, isLocal bool) {
	if c.ring == nil || c.ring.Empty() {
		return ring.Member{ID: c.cfg.NodeID, Addr: c.cfg.GRPCAddr}, true
	}
	// LocateKey applies ShardKey internally; passing the raw key here
	// would double-extract for ShardKeyFn callers. Use the resolved
	// shard key as the hashing input.
	owner = c.ring.LocateKey(c.shardKey(key))
	return owner, owner.ID == c.cfg.NodeID
}

// clientFor returns a cached peerClient for addr, dialing on miss.
func (c *Cluster) clientFor(addr string) (*peerClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("cluster: peer has empty gRPC address")
	}
	c.clientsMu.RLock()
	cli, ok := c.clients[addr]
	c.clientsMu.RUnlock()
	if ok {
		return cli, nil
	}
	c.clientsMu.Lock()
	defer c.clientsMu.Unlock()
	if cli, ok := c.clients[addr]; ok {
		return cli, nil
	}
	cli, err := newPeerClient(addr)
	if err != nil {
		return nil, fmt.Errorf("cluster: dial %s: %w", addr, err)
	}
	c.clients[addr] = cli
	return cli, nil
}

// TestingDropAllPeerClients force-closes every cached peer-gRPC
// client without touching membership / the Coordinator / the local
// gRPC server. Used by the destination-crash failure tests to
// SEVER outgoing connections (the dest-initiated MigrateRange
// stream is a client connection; tearing down the client cancels
// the stream + the source-side handler returns the
// "transport-closed" error, which the wired-handoff path then
// routes to Done-with-error). Membership stays intact so the
// source's plan is not torn down before the rejection lands.
//
// Test-only; not exported in the public API surface. Name is
// camelCase with the Testing prefix because Go's "Testing*"
// convention earmarks white-box hooks that survive a Test-only
// build constraint without the constraint itself (the cluster
// package's tests live in the same package; integration tests
// import + call this directly).
func (c *Cluster) TestingDropAllPeerClients() {
	c.clientsMu.Lock()
	for addr, cli := range c.clients {
		_ = cli.Close()
		delete(c.clients, addr)
	}
	c.clientsMu.Unlock()
}

// evictClient closes + drops the cached client for addr, if present.
// Called when a peer leaves the ring so a returning node on the same
// address gets a fresh connection.
func (c *Cluster) evictClient(addr string) {
	if addr == "" {
		return
	}
	c.clientsMu.Lock()
	cli, ok := c.clients[addr]
	if ok {
		delete(c.clients, addr)
	}
	c.clientsMu.Unlock()
	if ok {
		_ = cli.Close()
	}
}

// -- KV surface (mirrors backend.Backend) ----------------------------------

// Put stores value under key, routing to the owning node.
//
// v0.3/v0.4 rebalancing: if the key's partition is mid-migration on
// this node (either streaming out OR being received), the local Put
// is rejected with codes.ResourceExhausted + a retry-after hint (per
// docs/SPEC.md "Cutover"). Clients should retry with backoff per the
// hint; the SDK wraps this transparently with a bounded retry budget.
// codes.FailedPrecondition is reserved for the forwarding loop-guard
// (docs/SPEC.md "Failure handling"); codes.Unavailable is reserved
// for genuine peer-down failures (so the fanout's failure budget can
// short-circuit on dead nodes). The three codes have different retry
// semantics so they must not be conflated.
//
// v0.4 replication: when ReplicationFactor > 1 the originator stamps
// the payload (time.Now().UnixNano() + NodeID) once, wraps it in an
// LWW envelope, and fans out to R replicas. The call returns once W
// acks land per WriteConsistency. Migration-guard rejections from
// individual replicas are treated as transient (don't count toward
// either ack or failure budget) so a single mid-handoff replica
// doesn't fail an otherwise-quorum write. See docs/SPEC.md "Fan-out
// + ack accounting".
//
// Empty-value rejection: Put refuses nil + zero-length value with
// ErrEmptyValue. The envelope's empty-payload shape is reserved for
// Delete tombstones; allowing Put(key, nil) would silently store a
// tombstone at R>1 (looking like a Delete on subsequent reads) while
// at R=1 the same call would store an empty value (looking like a
// successful Put). The asymmetry is a foot-gun. Apps that want to
// remove a key must call Delete explicitly.
func (c *Cluster) Put(key, value []byte) error {
	if c.closed.Load() || c.backend == nil {
		return backend.ErrClosed
	}
	if len(value) == 0 {
		return ErrEmptyValue
	}
	if c.replicationFactor() > 1 && c.ring != nil && !c.ring.Empty() {
		return c.putReplicated(key, value)
	}
	owner, local := c.ownerOf(key)
	if local {
		if rb := c.rebalance.Load(); rb != nil && (rb.IsMigrating(key) || rb.IsReceiving(key)) {
			return migrationGuardError(c.retryAfterMs())
		}
		return c.backend.Put(key, value)
	}
	cli, err := c.clientFor(owner.Addr)
	if err != nil {
		return err
	}
	return cli.PutForwarded(context.Background(), key, value)
}

// Get returns the value for key, routing to the owning node.
//
// v0.3 rebalancing: if the key's partition is being received into
// this node (StateReceiving), the local data is not yet authoritative
// + the source still owns reads. Per docs/SPEC.md "Cutover" the
// destination transparently forwards the read back to the source's
// gRPC; the source still serves the key from its local copy until
// the destination ack flips it HandedOff. Callers see a normal
// successful Get rather than a transient error. Source-side
// IsMigrating (StateSending / StateHandedOff) is fine: we still
// have the data locally + serve the read normally.
//
// v0.4 replication: when ReplicationFactor > 1 the Get reads from N
// replicas per ReadConsistency (1 / quorum / R), picks the LWW winner
// across returned envelopes, and (on Quorum / All) asynchronously
// pushes the winner back to any lagging replica. Tombstones (empty
// payload) surface as backend.ErrNotFound. See docs/SPEC.md "Read path".
func (c *Cluster) Get(key []byte) ([]byte, error) {
	if c.closed.Load() || c.backend == nil {
		return nil, backend.ErrClosed
	}
	if c.replicationFactor() > 1 && c.ring != nil && !c.ring.Empty() {
		return c.getReplicated(key)
	}
	owner, local := c.ownerOf(key)
	if local {
		if rb := c.rebalance.Load(); rb != nil {
			if mv, ok := rb.ReceivingMove(key); ok && mv.From.Addr != "" {
				return c.forwardGet(mv.From.Addr, key)
			}
		}
		return c.backend.Get(key)
	}
	return c.forwardGet(owner.Addr, key)
}

// forwardGet dials addr (a peer's gRPC address) + issues a Get with
// the cluster-internal forwarded=true marker. Used by the routed-Get
// path AND by the v0.3 receiving-window read forwarder: a read that
// lands on the destination during its StateReceiving window is
// transparently forwarded back to the source so the caller sees a
// successful read rather than a transient error.
func (c *Cluster) forwardGet(addr string, key []byte) ([]byte, error) {
	cli, err := c.clientFor(addr)
	if err != nil {
		return nil, err
	}
	val, found, err := cli.GetForwarded(context.Background(), key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, backend.ErrNotFound
	}
	return val, nil
}

// Delete removes key, routing to the owning node.
//
// v0.3/v0.4 rebalancing: same write-guard semantics as Put. Mid-
// migration keys are rejected with ResourceExhausted + retry-after;
// the client retries once the range hands off cleanly.
//
// v0.4 replication: Delete writes a tombstone (empty-payload
// envelope) carrying the current Stamp + fans out to R replicas with
// the same Put accounting. The tombstone participates in LWW like
// any other write, so a Delete that races with a concurrent Put
// resolves by timestamp.
func (c *Cluster) Delete(key []byte) error {
	if c.closed.Load() || c.backend == nil {
		return backend.ErrClosed
	}
	if c.replicationFactor() > 1 && c.ring != nil && !c.ring.Empty() {
		return c.putReplicated(key, nil)
	}
	owner, local := c.ownerOf(key)
	if local {
		if rb := c.rebalance.Load(); rb != nil && (rb.IsMigrating(key) || rb.IsReceiving(key)) {
			return migrationGuardError(c.retryAfterMs())
		}
		return c.backend.Delete(key)
	}
	cli, err := c.clientFor(owner.Addr)
	if err != nil {
		return err
	}
	return cli.DeleteForwarded(context.Background(), key)
}

// ScanPrefix returns an iterator over keys with the given prefix on
// the owning shard. The prefix is treated as a shard key + routed to
// the owning node; the scan runs entirely on that node's backend. For
// cross-shard scans, use Aggregate.
func (c *Cluster) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if c.closed.Load() || c.backend == nil {
		return nil, backend.ErrClosed
	}
	owner, local := c.ownerOf(prefix)
	if local {
		return c.backend.ScanPrefix(prefix)
	}
	cli, err := c.clientFor(owner.Addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := cli.ScanPrefixForwarded(ctx, prefix)
	if err != nil {
		cancel()
		return nil, err
	}
	return &remoteIterator{stream: stream, cancel: cancel}, nil
}

// Begin starts a transaction on the shard that owns the FIRST key
// touched. The returned Transaction is lazy: the underlying Backend
// transaction is opened on the first Put / Get / Delete / ScanPrefix,
// pinned to that key's owning shard. Subsequent operations whose key
// hashes to a different shard return backend.ErrCrossShard. If the
// owning shard is remote, gRPC transaction proxying lands in a future
// version; for v0.2 a remote-pinned transaction returns ErrCrossShard
// on first use, with a descriptive wrap, so callers see the limitation
// at the call site rather than silently running against the wrong
// backend.
func (c *Cluster) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	if c.closed.Load() || c.backend == nil {
		return nil, backend.ErrClosed
	}
	if c.ring == nil {
		// Single-node mode: every key belongs to us, so delegate
		// straight through.
		return c.backend.Begin(level)
	}
	return &clusterTx{c: c, level: level}, nil
}

// AggregateResult is one entry in the slice returned by Aggregate:
// either Value (whatever fn returned for that peer) or Err (a
// transport / snapshotting failure that prevented fn from running for
// that peer). Exactly one of the two is meaningful per entry; the
// other is the zero value. Splitting them keeps Err distinct from a
// peer that legitimately returned an error VALUE (fn might return
// errors as part of its normal API).
type AggregateResult struct {
	// Value is what fn returned for this peer. Nil iff Err is set.
	Value any
	// Err is the cross-shard failure that prevented fn from running
	// (snapshot transport failed, peer unreachable, etc.). Nil on
	// success.
	Err error
}

// Aggregate runs fn locally on each node's Backend in parallel +
// collects the per-node results. Use for cross-shard operations
// (admin lists, full-table scans, computed aggregates). NOT for
// hot-path queries: use shard-aware key design for those.
//
// Each entry in the returned slice is an AggregateResult: Err is set
// if shale couldn't even run fn for that peer (snapshot transport
// failure, dial failure), otherwise Value holds whatever fn returned.
// Order of results is unspecified.
//
// v0.2: peer fan-out walks the ring's member list and, for each peer
// other than the local node, streams the peer's LOCAL backend over
// gRPC (via the admin-only LocalScan RPC, which bypasses ring
// routing) into a transient in-memory backend snapshot that fn sees.
// The local node runs fn against its own backend directly.
func (c *Cluster) Aggregate(fn func(b backend.Backend) any) []AggregateResult {
	if c.closed.Load() || c.backend == nil {
		return nil
	}
	members := c.Members()
	results := make([]AggregateResult, len(members))
	var wg sync.WaitGroup
	for i, m := range members {
		i, m := i, m
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m.ID == c.cfg.NodeID {
				results[i] = AggregateResult{Value: fn(c.backend)}
				return
			}
			snap, err := c.snapshotPeer(m.Addr)
			if err != nil {
				results[i] = AggregateResult{Err: err}
				return
			}
			results[i] = AggregateResult{Value: fn(snap)}
		}()
	}
	wg.Wait()
	return results
}

// snapshotPeer pulls the full keyspace from a peer's LOCAL backend
// into a transient in-process backend. Used by Aggregate. The
// snapshot is local to one fn invocation; the backend is not exposed
// beyond it.
//
// Uses LocalScan (admin path) rather than ScanPrefix so the receiving
// node hands us its own keys directly. ScanPrefix would route the
// empty prefix through ownerOf - hashing nil to whichever shard owns
// it - and we'd get a single shard back N times instead of each
// shard's slice once.
func (c *Cluster) snapshotPeer(addr string) (backend.Backend, error) {
	cli, err := c.clientFor(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := cli.LocalScan(ctx, nil)
	if err != nil {
		return nil, err
	}
	snap := newSnapshotBackend()
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		_ = snap.Put(msg.GetKey(), msg.GetValue())
	}
	return snap, nil
}
