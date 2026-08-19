# shale

[![test](https://github.com/Zamua/shale/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/Zamua/shale/actions/workflows/test.yml)
[![lint](https://github.com/Zamua/shale/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/Zamua/shale/actions/workflows/lint.yml)

Horizontal-scale layer for embedded KV stores. Each instance you import is a cluster node; consistent hashing distributes keys across nodes; durability is whatever backend you plug in.

## Why it exists

Sharded KV stores either:
- Run as heavyweight services (Cassandra, ScyllaDB, TiKV) - operational overhead, separate process
- Bake clustering into the data engine (Redis Cluster) - locks you to that engine
- Don't exist in the embedded-library + pluggable-backend shape

shale is the third option: thin Go library, BYO backend, scales by adding processes that import it.

## What works today

- Consistent-hash sharding across N embedded nodes: single-key ops route in one hop, cross-shard ops fan out explicitly.
- Replication factor R with tunable read/write consistency (Nearest / Quorum / All), LWW conflict resolution, and read-repair.
- Online resharding with no acked-write loss: decentralized split/merge, zero-copy unit handoff over shared object storage, driven declaratively by a target unit count every node advertises through the coordinator.
- Value-separation: large values stream to a blob plane out-of-band, transactionally bound to the metadata write.
- Homogeneous bootstrap: every node runs the same image and try-joins-else-forms via an object-storage CAS marker, so there is no dedicated seed.
- Pluggable backends, each its own Go module: SlateDB-on-object-storage (default), Pebble (local LSM), in-memory.

**Don't use in production yet.**

## Milestones

- [x] v0.1: single-node Cluster, gRPC service, `shale` CLI (put/get/delete/scan/topology/stats/ping).
- [x] v0.2: multi-node hash ring + gRPC forwarding; hash-tag co-location.
- [x] v0.3: shard rebalancing on membership changes; `shale rebalance` CLI.
- [x] v0.4: replication factor R + tunable consistency + LWW + read-repair + tombstone deletes.
- [x] v0.5: observability (pprof + a per-node `/debug/shale/state` endpoint) + real benchmarks.
- [x] v0.5.x: multi-module split (`backends/slate`, `backends/pebble`, each its own `go.mod`) + per-backend `Settings` pass-through.
- [x] v0.6: production-shape defaults - relaxed durability (`AwaitDurable=false`) paired with replication; replica-aware lossless rebalance at R>1.
- [x] v0.7: online multi-node resharding via a cluster-wide freeze barrier; zero-copy SlateDB unit handoff over shared object storage; chaos/soak-validated losslessness.
- [x] v0.8: value-separation (streaming blob values); decentralized online split/merge resharding; homogeneous bootstrap; degraded-boot + self-healing fenced-mount recovery.
- [x] v0.9: declarative resharding from the advertised unit count; membership/ring hardening under dense churn.

## Development

One-time setup after cloning, to enable the repo-shipped lint + format
hooks:

```
git config core.hooksPath .githooks
```

This points git at `.githooks/` for hook lookup, so the `pre-commit`
script in that directory runs on every commit. It checks `gofmt`, runs
`go vet`, and (when `golangci-lint` is installed) lints the staged diff.
See `.githooks/README.md` for the install command and version pin.

## License

Apache 2.0. See `LICENSE`.
