# shale

Horizontal-scale layer for embedded KV stores. Each instance you import is a cluster node; consistent hashing distributes keys across nodes; durability is whatever backend you plug in.

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
