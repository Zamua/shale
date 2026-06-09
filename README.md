# shale

Horizontal-scale layer for embedded KV stores. Each instance you import is a cluster node; consistent hashing distributes keys across nodes; durability is whatever backend you plug in.

## Why it exists

Sharded KV stores either:
- Run as heavyweight services (Cassandra, ScyllaDB, TiKV) - operational overhead, separate process
- Bake clustering into the data engine (Redis Cluster) - locks you to that engine
- Don't exist in the embedded-library + pluggable-backend shape

shale is the third option: thin Go library, BYO backend, scales by adding processes that import it.

## Roadmap

- [x] v0.1: single-node Cluster, gRPC service, `shaled` standalone binary, `shale` CLI (put/get/delete/scan/topology/stats/ping). API lockup.
- [x] v0.2: multi-node hash ring + gRPC forwarding (static topology); hash-tag co-location; topology RPC + CLI subcommand. (v0.2.1, v0.2.2 followed as bug-fix rolls.)
- [x] Pebble backend (pure Go, durable across restart); slots into the Backend interface alongside memory + slate.
- [x] `shale bench` subcommand (originally roadmapped for v0.5; pulled forward to help measure v0.3 rebalancing impact).
- [x] v0.3: shard rebalancing on membership changes (settle-delay, streaming MigrateRange RPC, write rejection during cutover, in-flight read forwarding, cleanup sweep); `shale rebalance` CLI. (v0.3.1 followed with 7 fixes including a P0 data-loss path on real backends.)
- [ ] v0.4 (in progress): replication factor R + tunable read/write consistency (Nearest / Quorum / All) + LWW conflict resolution + read-repair.
- [ ] v0.5: observability (Prometheus metrics, tracing hooks) + real benchmarks vs single-node backends.
- [ ] v0.6: hostthis migration — swap raw SlateDB for shale-with-SlateDB-backend on production-shape data.

Don't use in production yet.

## License

Apache 2.0. See `LICENSE`.
