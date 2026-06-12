# Real-cluster chaos validation results

This file captures a run of the `chaosreal` orchestrator (`TestRealClusterChaos`,
gated behind `//go:build chaosreal`) against a REAL N-node `shaled-slate` deploy:
separate OS processes, real gRPC + memberlist, a shared MinIO bucket as the
durable object-storage backing. It is the step-2 "prove it on a REAL cluster"
companion to the in-process chaos soak (`//go:build chaos`), which proves the
cluster coordination but cannot prove durability across a real process death or
the real network. See `chaos_real_test.go` for the full can/cannot-prove scope.

## Environment

- `shaled-slate` built with `-tags slatedb` (cgo, linked against
  `libslatedb_uniffi`), legacy per-node backend mode.
- MinIO as the S3-compatible endpoint; one fresh bucket per run (the shared
  durable backing).
- KV ops over the real `pkg/rpc` gRPC client; structural events are real
  `os/exec` launches and `SIGKILL`s.
- The oracle (`oracle.go`, build-tag-free) is unchanged - it consumes acks from
  the real client identically to the in-process one.

## Runs

| run | nodes | duration | seed | acked puts | acked deletes | reads verified | kill/join/leave/restart | dur survived | dur unavailable (legacy gap) | violations |
|-----|-------|----------|------|------------|---------------|----------------|-------------------------|--------------|------------------------------|------------|
| 1   | 2     | 25s      | 1        | 108 | 23 | 853  | 2/0/1/2 | 53  | 40 | none |
| 2   | 2     | 40s      | (seed B) | 338 | 76 | 1084 | 4/3/0/4 | 60  | 31 | none |
| 3   | 3     | 45s      | (seed C) | 210 | 40 | 1058 | 3/1/1/3 | 113 | 35 | none |

Wall times: ~55s / ~104s / ~99s respectively (the elapsed time exceeds the
workload `DURATION` because every structural event blocks on real topology
convergence + a per-event settle). All three runs: ZERO acked-write-loss and
ZERO wrong-value violations.

After each run the bucket held the durable slatedb data (103 / 350 / 218 objects)
- concrete evidence the acked writes reached object storage. The post-SIGKILL
durability probe read those values back correctly from the surviving founder.

## The durability assertion (the new thing this proves)

After a HARD `SIGKILL` of a non-founder node mid-workload, every founder-owned
acked write was still readable with the correct value from the surviving founder
(`dur survived`), across a real process death + a real object-store round-trip.
No survivor ever served a stale, corrupt, or resurrected value (the only class
the legacy deploy is held to, hard-gated in PHASE 1). The killed node was then
restarted and the cluster re-converged.

`dur unavailable` counts founder-NOT-owned keys whose owning node was the one
killed. Under legacy per-node mode each node owns a DISTINCT slatedb instance and
there is no per-unit lease handoff, so a survivor cannot re-mount a dead owner's
instance. Those keys are durable in the dead node's instance but not served by a
survivor. This is the documented mode limitation, counted as a metric, never a
failure. With N=3 the founder owns a larger share of the keyspace relative to the
single killed node, so survival is correspondingly higher (113 vs 35).

## What this deploy could NOT exercise (honest env blocker)

`reshard_supported=false` on every run. Two pieces are absent end to end:

1. **Multi-backend mode is not wired into any shaled binary.** The cluster core
   has both modes (`Backend` for legacy per-node; `BackendFactory` + `UnitCount`
   for multi-backend, where reshard and per-unit lease handoff live), but no
   shaled binary (`cmd/`, `backends/*/cmd/`) configures the factory path - there
   is no slatedb `BackendFactory` and no unit-count flag. Every real node runs
   legacy per-node mode.
2. **No operator-facing reshard surface.** The gRPC server exposes only the
   cluster-internal `ReshardControl` (coordinator-to-peer freeze barrier); there
   is no reshard RPC and no `shale reshard` CLI subcommand. Reshard is an
   in-process `Cluster.Reshard()` call no binary exposes.

Consequence: the reshard and the join-after-reshard generation-propagation path
(the v0.8 join-fix) CANNOT be driven on a real cluster today. They remain proven
only by the in-process chaos soak. The orchestrator records the gap honestly
(`reshard_supported=false`) rather than faking a reshard. Closing this gap
requires a slatedb `BackendFactory` plus an operator reshard surface; until then
the real-cluster validation covers the slice of the durability story legacy mode
is responsible for (single-owner durable-before-ack, real network, real death).

## Real-vs-in-process observations

- **Failover is far slower than in-process.** `retryable_retries` ran into the
  thousands to tens of thousands per run (4669 / 9476 / 12208) versus the
  in-process handoff. A real cross-process failover waits on memberlist
  fail-detection (several gossip intervals) plus a ring refresh, where the
  in-process handoff is a goroutine-local state flip. The faithful client rides
  this out by re-routing to a survivor; not one of those retries became a loss.
- **A few transient reads per run** (`transient_reads` 4-5) where the chosen
  entry node was mid-kill: the client re-selected a live entry and the read
  settled. Real connection-refused / transport-closing signals that the
  in-process path never produces were exercised and handled.
- **Durability is genuinely cross-process here.** The in-process "kill" is a
  goroutine stop with the data still in shared memory; this run's `SIGKILL`
  reaps a real OS process whose memory is gone, and the survivor serves the
  value back out of MinIO. That is the gap the in-process harness explicitly
  cannot close, closed for the legacy single-owner slice.

## How to reproduce

Build the binary (`-tags slatedb`, cgo), stand up MinIO with a fresh bucket, then:

```sh
SHALE_REAL_BINARY=<abs path to shaled-slate> \
SHALE_REAL_LIBDIR=<dir with libslatedb_uniffi> \
SHALE_REAL_ENDPOINT=http://127.0.0.1:9000 \
SHALE_REAL_BUCKET=<a fresh bucket> \
SHALE_REAL_ACCESS_KEY=<key> SHALE_REAL_SECRET_KEY=<secret> \
SHALE_REAL_USE_SSL=false \
SHALE_REAL_NODES=2 SHALE_REAL_DURATION=25s SHALE_REAL_SEED=1 \
  go test -tags chaosreal ./tests/chaos/ -run TestRealClusterChaos -v -timeout 8m
```

The test owns the child-process lifecycle end to end (a `defer top.CloseAll()`
guarantees no orphaned `shaled-slate` processes survive, even on a panic). It
skips with a precise reason if any required env piece is missing, rather than
faking a run.
