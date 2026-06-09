# shale v0.5 benchmark results

Generated: 2026-06-09 16:45 UTC
Hardware: Apple Mac mini (M-series), single process, loopback memberlist + loopback gRPC.
Workload: 10000 ops per phase (write then read), 1024-byte random values, 4 concurrent client goroutines.
Harness: `cmd/shale-bench` (in-process spin-up via `tests/integration` patterns).
Re-run: `make bench-v0.5 > docs/BENCH-v0.5.md`.

## Table

| Scenario | Backend | Nodes | R | Put p50 (ms) | Put p99 (ms) | Get p50 (ms) | Get p99 (ms) | Put ops/s | Get ops/s |
|---|---|---|---|---:|---:|---:|---:|---:|---:|
| raw-pebble | pebble | 1 | 1 | 0.00 | 0.08 | 0.01 | 0.18 | 163,074 | 214,309 |
| cluster-pebble-n1-r1 | pebble | 1 | 1 | 0.00 | 0.08 | 0.01 | 0.17 | 143,215 | 171,034 |
| cluster-pebble-n3-r1 | pebble | 3 | 1 | 0.06 | 0.24 | 0.06 | 0.20 | 46,406 | 68,137 |
| cluster-pebble-n3-r3 | pebble | 3 | 3 | 0.09 | 0.41 | 0.22 | 0.57 | 25,209 | 17,661 |
| raw-memory | memory | 1 | 1 | 0.00 | 0.07 | 0.00 | 0.00 | 1,202,712 | 3,729,141 |
| cluster-memory-n1-r1 | memory | 1 | 1 | 0.00 | 0.19 | 0.00 | 0.01 | 448,319 | 2,318,236 |
| cluster-memory-n3-r1 | memory | 3 | 1 | 0.04 | 0.27 | 0.04 | 0.11 | 79,359 | 118,505 |
| cluster-memory-n3-r3 | memory | 3 | 3 | 0.09 | 0.31 | 0.19 | 0.47 | 40,696 | 20,628 |

Notes on the configuration:

- R=1 scenarios use `WriteOne` (no quorum to wait for) + `ReadNearest` (primary only).
- R=3 scenarios use `WriteQuorum` (2-of-3 acks) + `ReadQuorum` (2-of-3 reads + LWW winner). `ReadNearest` is unsafe at R>1 under bench-shape workloads because the Put returns after the quorum acks, so a read landing on the laggard replica milliseconds later can miss; see "Surprise" below.

## Where the cost goes

**Baseline (`raw-*`).** Pebble at ~163K writes/s and memory at ~1.2M writes/s sets the floor: no shale code involved, just `backend.Backend.Put` calls in-process. Memory reads are essentially "find pointer, return slice" so 3.7M ops/s reflects the cost of the bench loop itself (sync.WaitGroup + atomic counter + the per-op time.Now) more than the backend.

**Cluster overhead at R=1, 1 node (`cluster-*-n1-r1`).** Pebble: 143K writes/s vs 163K raw, a ~12% drop. This is the cost of the cluster layer's "is this key local?" check, the ring lookup, the migration-guard check, and the value-envelope encode/decode (Stamp + Payload framing). All of those are pure CPU on local data; no gRPC hop fires because there's nothing to forward to. Memory shows a sharper drop (1.2M -> 448K writes/s) because the per-op envelope encoding cost is a larger fraction of the work when the backend itself is free.

**Sharding cost: R=1, 3 nodes (`cluster-*-n3-r1`).** Drops to 46K writes/s (pebble) / 79K writes/s (memory). Most keys now route to a peer, so each Put pays one loopback gRPC RTT. The gap between memory (79K) and pebble (46K) at the same shape isolates the gRPC + ring cost (~ memory at 79K) from the gRPC + ring + pebble durable-write cost (~ pebble at 46K). Reads at 68K (pebble) / 118K (memory) reflect the same hop plus a cache hit on the gRPC client connection pool.

**Replication cost: R=3, 3 nodes (`cluster-*-n3-r3`).** The headline number. Writes drop to 25K/s (pebble) and 41K/s (memory) - roughly half of the R=1 / 3-node numbers, which is the expected cost of fanning each Put out to 3 replicas in parallel and waiting for 2 quorum acks. Reads drop sharper: 18K/s (pebble) / 21K/s (memory), about a 3.5x slowdown vs R=1, because ReadQuorum fans out to 2 replicas + waits for both + runs an LWW comparison before returning. The asymmetry (reads cost MORE than writes at R=3) is a real surprise; see below.

## Surprise worth flagging for v0.5.1

**Reads at R=3 are slower than writes at R=3.** Writes p50 = 0.09ms, reads p50 = 0.22ms (pebble) / 0.19ms (memory). The ratio is consistent across both backends so it's a cluster-layer effect, not storage.

The candidate explanations:

1. ReadQuorum fans out + waits for floor(R/2)+1 = 2 replica responses, like WriteQuorum. The wait should be symmetric.
2. ReadQuorum kicks off async read-repair if replicas disagree (writes don't). Even when replicas agree at steady state, the comparison + the goroutine spawn cost something.
3. Reads have to decode the value envelope (Stamp + Payload) once per replica response THEN run LWW, while writes encode once + dispatch.

The 2-3x ratio is bigger than I'd expect from envelope decode alone. Worth a focused profile in v0.5.1: a CPU profile of `cluster-pebble-n3-r3` read phase will show whether time is dominated by gRPC unmarshaling, LWW comparison, or the read-repair goroutine spawn even when no repair is actually needed.

**Open question:** is `ReadQuorum` the right default for R=3? At R=1 we default to `ReadNearest`; at R>1 the spec leaves the choice to the operator. Operators picking R=3 for HA probably want `ReadQuorum` to avoid the read-your-writes hole - which is what the bench measures here. But a future "ReadNearest with primary-lag guard" mode (e.g. wait for the primary's own ack before letting Get return) could give back most of the latency at the cost of one knob more.

**Setup-time race fixed in the harness itself.** First implementation of the harness called `WaitForRebalanceIdle` immediately after the nodes joined, which is BEFORE the Coordinator's settle-delay debounce had fired. WaitForIdle then returned "idle" (nothing pending), the bench started writing, the bootstrap Evaluate fired mid-workload, and writes started failing with `ResourceExhausted` migration-guards that the cluster's replicate path classifies as transient + counts out of the ack budget - resulting in "needed 2 acks, got 1 (0 failures)". The fix is the same one `tests/integration/helpers_test.go` uses: sleep past the settle window FIRST, then call WaitForIdle, then a small post-idle drain. Worth lifting that gate into the public Cluster API as a `WaitForBootstrapSettled(ctx)` helper in v0.5.1 so other harnesses don't repeat the trap.

## Reproducing

```bash
make bench-v0.5                          # canonical (10k ops, 1KB values, 4 clients)
SHALE_BENCH_OPS=100000 make bench-v0.5   # more samples, tighter tails
SHALE_BENCH_DIR=/tmp/bench-v0.5 make bench-v0.5  # keep per-scenario JSON
```

Per-scenario knobs (drive `shale-bench` directly):

```bash
go build -o shale-bench ./cmd/shale-bench/
./shale-bench --mode cluster --backend pebble --nodes 3 --rf 3 \
              --ops 100000 --value-size 4096 --concurrency 16 --json
```

## What this bench does NOT measure

- **Cross-host network cost.** Loopback gRPC + loopback memberlist have no real RTT. Real-world R=3 numbers will be worse (~ +1 inter-host RTT per fan-out) but multi-host setups may also recover throughput because each pebble backend gets its own disk.
- **Slate backend.** Excluded from the v0.5 suite because it requires MinIO + the slatedb build tag. Slate-specific benches can plug in once the MinIO CI fixture (added in v0.5) is reusable.
- **Topology change cost.** Membership churn (node add/leave/rebalance) is its own measurement; the v0.5 suite assumes a stable cluster.
- **Large values.** 1KB is small; gRPC overhead dominates. A 64KB/256KB variant would tell a different story (backend cost rises, gRPC cost stays roughly fixed per call).
- **Long-tail percentiles beyond p99.** p99.9 / max would surface GC pauses + scheduler stalls on the in-process model. Worth a follow-up.
