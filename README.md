# shale

Horizontal-scale layer for embedded KV stores. Each instance you import is a cluster node; consistent hashing distributes keys across nodes; durability is whatever backend you plug in (SlateDB on object storage by default).

```go
import "github.com/Zamua/shale/pkg/cluster"

c, _ := cluster.Open(cluster.Config{
    NodeID:   "node-1",
    BindAddr: ":7946",
    Seeds:    []string{"node-2:7946", "node-3:7946"},
    Backend:  slate.New(slate.Config{Bucket: "my-data", ...}),
})
defer c.Close()

c.Put([]byte("user:alice"), []byte("data"))
val, _ := c.Get([]byte("user:alice"))
```

The cluster layer routes `user:alice` to whichever node owns that hash range, forwards over gRPC, and writes through to that node's local backend. Add a node → keys rebalance with minimal data movement. Remove a node → ownership shifts.

## Why it exists

Sharded KV stores either:
- Run as heavyweight services (Cassandra, ScyllaDB, TiKV) — operational overhead, separate process
- Bake clustering into the data engine (Redis Cluster) — locks you to that engine
- Don't exist in the embedded-library + pluggable-backend shape

shale is the third option: thin Go library, BYO backend, scales by adding processes that import it.

## Status

Pre-v0.1. Designs landing first; implementation incremental:

| Version | Scope | Status |
| --- | --- | --- |
| v0.1 | Single-node `Cluster` wrapping one Backend (API lockup) | not started |
| v0.2 | Multi-node hash ring + gRPC forwarding (static topology) | not started |
| v0.3 | Shard rebalancing on membership changes | not started |
| v0.4 | Replication (R replicas per shard, quorum reads/writes) | not started |
| v0.5 | Observability + benchmarks | not started |

Don't use in production yet.

## Backends shipped

- `pkg/backend/memory` — in-memory; for tests + dev
- `pkg/backend/slate` — SlateDB-on-object-storage (planned)
- BYO: implement `pkg/backend.Backend` for anything that's KV-shaped (BadgerDB, Pebble, RocksDB via cgo, etc.)

## License

Apache 2.0. See `LICENSE`.
