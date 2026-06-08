// Package cluster is the public API surface of shale. Apps import
// this package + call cluster.Open(cfg) to get a Cluster handle that
// routes KV operations across nodes.
//
// v0.1 (current): single-node mode only — Cluster wraps one Backend
// directly. Calling cluster.Open without any peer config gives you a
// functioning KV API that just delegates to the local Backend. This
// is the API lockup release; useful for apps that may scale-out
// later but want the same import + interface today.
//
// v0.2 (planned): multi-node mode — Cluster uses memberlist for
// membership discovery + buraksezer/consistent for the hash ring +
// gRPC for inter-node forwarding. Peer config in Config enables it.
//
// See docs/SPEC.md for the full model.
package cluster

import (
	"errors"

	"github.com/Zamua/shale/pkg/backend"
)

// Config configures a Cluster. NodeID + Backend are always required.
// The peer-discovery fields (BindAddr, Seeds) are required only when
// running multi-node (v0.2+); ignored in single-node mode.
type Config struct {
	// NodeID is this node's stable identity. Used in membership +
	// ring placement. MUST be unique within the cluster.
	NodeID string

	// Backend is the local KV engine this node owns. Required.
	Backend backend.Backend

	// BindAddr is the host:port memberlist + the shale gRPC service
	// listen on. Format: "host:port" (host may be empty for "all
	// interfaces"). Required for multi-node; ignored single-node.
	BindAddr string

	// Seeds are addresses of already-running cluster nodes. Bootstrap
	// gossip contacts these to discover the rest of the membership.
	// Empty in single-node mode.
	Seeds []string

	// ShardKeyFn lets the app extract a shard key from a full key.
	// Default = hashTagged identity (full key, honoring `{tag}`
	// hash tags). Override for custom shard key shapes.
	ShardKeyFn func(key []byte) []byte
}

// Cluster is the public handle apps use. All operations are go-routine safe.
type Cluster struct {
	cfg     Config
	backend backend.Backend
}

// Open initializes a Cluster from cfg. In v0.1 with single-node
// mode, this just records the cfg + returns the Cluster wrapper.
// Multi-node mode (v0.2+) will additionally bring up memberlist + gRPC.
func Open(cfg Config) (*Cluster, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("cluster: NodeID is required")
	}
	if cfg.Backend == nil {
		return nil, errors.New("cluster: Backend is required")
	}
	return &Cluster{cfg: cfg, backend: cfg.Backend}, nil
}

// Close releases all cluster resources. After Close, no other method
// may be called. Idempotent.
func (c *Cluster) Close() error {
	if c.backend == nil {
		return nil
	}
	err := c.backend.Close()
	c.backend = nil
	return err
}

// -- KV surface (mirrors backend.Backend) ----------------------------------

// Put stores value under key, routing to the owning node.
func (c *Cluster) Put(key, value []byte) error {
	if c.backend == nil {
		return backend.ErrClosed
	}
	// v0.1: single-node — delegate. v0.2+: ring.LocateKey + maybe gRPC.
	return c.backend.Put(key, value)
}

// Get returns the value for key, routing to the owning node.
func (c *Cluster) Get(key []byte) ([]byte, error) {
	if c.backend == nil {
		return nil, backend.ErrClosed
	}
	return c.backend.Get(key)
}

// Delete removes key, routing to the owning node.
func (c *Cluster) Delete(key []byte) error {
	if c.backend == nil {
		return backend.ErrClosed
	}
	return c.backend.Delete(key)
}

// ScanPrefix returns an iterator over keys with the given prefix on
// the owning shard. In multi-node mode this is single-shard only —
// the prefix is treated as a shard key + routed to that shard. For
// cross-shard scans, use Aggregate.
func (c *Cluster) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if c.backend == nil {
		return nil, backend.ErrClosed
	}
	return c.backend.ScanPrefix(prefix)
}

// Begin starts a transaction on the shard that owns the FIRST key
// touched in the transaction. Subsequent operations on other shards
// return backend.ErrCrossShard.
func (c *Cluster) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	if c.backend == nil {
		return nil, backend.ErrClosed
	}
	return c.backend.Begin(level)
}

// Aggregate runs fn locally on each node's Backend in parallel +
// collects the per-node results. Use for cross-shard operations
// (admin lists, full-table scans, computed aggregates). NOT for
// hot-path queries — use shard-aware key design for those.
//
// In v0.1 single-node mode, fn runs once locally + the result is a
// 1-element slice.
func (c *Cluster) Aggregate(fn func(b backend.Backend) any) []any {
	if c.backend == nil {
		return nil
	}
	return []any{fn(c.backend)}
}
