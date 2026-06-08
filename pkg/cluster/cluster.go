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

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/membership"
	"github.com/Zamua/shale/pkg/ring"
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

	// closeCh is closed by Close to signal the events loop to exit.
	closeCh chan struct{}
	loopWG  sync.WaitGroup
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
	// sync with membership.
	c.loopWG.Add(1)
	go c.runEventsLoop()

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
				c.ring.Add(ring.Member{ID: ev.Member.ID, Addr: ev.Member.Addr})
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

// Close releases all cluster resources. After Close, no other method
// may be called. Idempotent.
func (c *Cluster) Close() error {
	var firstErr error

	// Signal the events loop to exit + wait for it before tearing down
	// membership so we don't race with a late ring.Add on closed ring.
	select {
	case <-c.closeCh:
		// already closed
	default:
		close(c.closeCh)
	}
	c.loopWG.Wait()

	if c.membership != nil {
		if err := c.membership.Close(); err != nil {
			firstErr = err
		}
		c.membership = nil
	}

	// Tear down all cached peer clients.
	c.clientsMu.Lock()
	for addr, cli := range c.clients {
		_ = cli.Close()
		delete(c.clients, addr)
	}
	c.clientsMu.Unlock()

	if c.backend != nil {
		if err := c.backend.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.backend = nil
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
	if c.backend == nil {
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
	return cli.Put(context.Background(), key, value)
}

// Get returns the value for key, routing to the owning node.
func (c *Cluster) Get(key []byte) ([]byte, error) {
	if c.backend == nil {
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
	val, found, err := cli.Get(context.Background(), key)
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
	if c.backend == nil {
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
	return cli.Delete(context.Background(), key)
}

// ScanPrefix returns an iterator over keys with the given prefix on
// the owning shard. The prefix is treated as a shard key + routed to
// the owning node; the scan runs entirely on that node's backend. For
// cross-shard scans, use Aggregate.
func (c *Cluster) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if c.backend == nil {
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
	stream, err := cli.ScanPrefix(ctx, prefix)
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
	if c.backend == nil {
		return nil, backend.ErrClosed
	}
	if c.ring == nil {
		// Single-node mode: every key belongs to us, so delegate
		// straight through.
		return c.backend.Begin(level)
	}
	return &clusterTx{c: c, level: level}, nil
}

// Aggregate runs fn locally on each node's Backend in parallel +
// collects the per-node results. Use for cross-shard operations
// (admin lists, full-table scans, computed aggregates). NOT for
// hot-path queries: use shard-aware key design for those.
//
// v0.2: peer fan-out walks the ring's member list and, for each peer
// other than the local node, streams ScanPrefix(nil) over gRPC into a
// local in-memory backend snapshot that fn sees. The local node runs
// fn against its own backend directly. Order of results is unspecified.
func (c *Cluster) Aggregate(fn func(b backend.Backend) any) []any {
	if c.backend == nil {
		return nil
	}
	members := c.Members()
	results := make([]any, len(members))
	var wg sync.WaitGroup
	for i, m := range members {
		i, m := i, m
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m.ID == c.cfg.NodeID {
				results[i] = fn(c.backend)
				return
			}
			snap, err := c.snapshotPeer(m.Addr)
			if err != nil {
				results[i] = err
				return
			}
			results[i] = fn(snap)
		}()
	}
	wg.Wait()
	return results
}

// snapshotPeer pulls the full keyspace from a peer into a transient
// in-process backend. Used by Aggregate. The snapshot is local to one
// fn invocation; the backend is not exposed beyond it.
func (c *Cluster) snapshotPeer(addr string) (backend.Backend, error) {
	cli, err := c.clientFor(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := cli.ScanPrefix(ctx, nil)
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
