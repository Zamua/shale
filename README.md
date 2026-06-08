# shale

Horizontal-scale layer for embedded KV stores. Each instance you import is a cluster node; consistent hashing distributes keys across nodes; durability is whatever backend you plug in.

## Why it exists

Sharded KV stores either:
- Run as heavyweight services (Cassandra, ScyllaDB, TiKV) - operational overhead, separate process
- Bake clustering into the data engine (Redis Cluster) - locks you to that engine
- Don't exist in the embedded-library + pluggable-backend shape

shale is the third option: thin Go library, BYO backend, scales by adding processes that import it.

## Roadmap

- [ ] v0.1: single-node Cluster, gRPC service, `shaled` standalone binary, `shale` CLI (put/get/delete/scan/topology/stats/ping). API lockup.
- [ ] v0.2: multi-node hash ring + gRPC forwarding (static topology)
- [ ] v0.3: shard rebalancing on membership changes; `shale migrate-from` for backend handoff
- [ ] v0.4: replication (R replicas per shard, quorum reads/writes)
- [ ] v0.5: observability + benchmarks; `shale bench`

Don't use in production yet.

## License

Apache 2.0. See `LICENSE`.
