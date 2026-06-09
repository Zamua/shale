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
	"github.com/Zamua/shale/pkg/ring"
)

// reconcileInterval is how often the cluster polls membership for a
// full snapshot and reapplies any add/remove the event-channel layer
// may have missed under backpressure. Small enough that a dropped
// event recovers within seconds; large enough to be free in steady
// state. Exposed as a var (not const) so tests can shrink it.
var reconcileInterval = 5 * time.Second

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
	if cfg.Backend == nil {
		return nil, errors.New("cluster: Backend is required")
	}

	c := &Cluster{
		cfg:     cfg,
		backend: cfg.Backend,
		clients: make(map[string]*peerClient),
		closeCh: make(chan struct{}),
	}

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
			case membership.EventLeave:
				c.ring.Remove(ev.Member.ID)
				// Drop any cached client for this departed peer so
				// the next dial picks up a fresh connection if the
				// same node ever returns on a different address.
				c.evictClient(ev.Member.Addr)
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
	// Add anyone missing from the ring (or with a stale Addr).
	for id, m := range want {
		oldAddr := c.priorAddrForID(id)
		if oldAddr != m.Addr {
			c.ring.Add(m)
			if oldAddr != "" {
				c.evictClient(oldAddr)
			}
		}
	}
	// Remove anyone the ring still has but membership has dropped.
	for _, m := range c.ring.Members() {
		if _, ok := want[m.ID]; !ok {
			c.ring.Remove(m.ID)
			c.evictClient(m.Addr)
		}
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
// local node with an empty Addr (no gRPC peer routing is set up).
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

	// Signal the events + reconcile loops to exit + wait for them
	// before tearing down membership so we don't race with a late
	// ring.Add on closed ring.
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
func (c *Cluster) Put(key, value []byte) error {
	if c.closed.Load() || c.backend == nil {
		return backend.ErrClosed
	}
	owner, local := c.ownerOf(key)
	if local {
		return c.backend.Put(key, value)
	}
	cli, err := c.clientFor(owner.Addr)
	if err != nil {
		return err
	}
	return cli.PutForwarded(context.Background(), key, value)
}

// Get returns the value for key, routing to the owning node.
func (c *Cluster) Get(key []byte) ([]byte, error) {
	if c.closed.Load() || c.backend == nil {
		return nil, backend.ErrClosed
	}
	owner, local := c.ownerOf(key)
	if local {
		return c.backend.Get(key)
	}
	cli, err := c.clientFor(owner.Addr)
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
func (c *Cluster) Delete(key []byte) error {
	if c.closed.Load() || c.backend == nil {
		return backend.ErrClosed
	}
	owner, local := c.ownerOf(key)
	if local {
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
