# shale v0.5 benchmark results

Generated: 2026-06-09 16:45 UTC
Hardware: Apple Mac mini (M-series), single process, loopback membership + loopback gRPC. Membership ran on the gossip adapter that shipped at the time; it was removed in v0.19.0.
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

## Slate scenarios (run separately via `make bench-v0.5-slate`)

Slate is gated behind its own target because the harness has to bring up a MinIO container, the binary needs `-tags slatedb` + the SlateDB cdylib + cgo, and every strict-mode Put round-trips to object storage (so op counts are smaller to keep each scenario under 60s).

Eight scenarios run: the four shapes (raw + cluster at n1/r1, n3/r1, n3/r3) crossed with the two durability modes the slate backend now supports. The "strict" set uses `AwaitDurable=true` (slatedb's own default, byte-exact with the pre-WriteOptions slate path). The "relaxed" set passes `slate.Config{WriteOptions: &slatedb.WriteOptions{AwaitDurable: false}}` so each Put acks at memtable insert; durability comes from the background WAL flush. The contract for what `Backend.Put` acks means is the backend's choice; see `docs/SPEC.md` "Backend durability is a backend concern" for shale's stance.

### AwaitDurable=true (strict; current shale default)

| Scenario | Backend | Nodes | R | Put p50 (ms) | Put p99 (ms) | Get p50 (ms) | Get p99 (ms) | Put ops/s | Get ops/s |
|---|---|---|---|---:|---:|---:|---:|---:|---:|
| raw-slate | slate | 1 | 1 | 103 | 109 | 0.02 | 1.16 | 77.2 | 119,251 |
| cluster-slate-n1-r1 | slate | 1 | 1 | 102 | 106 | 0.01 | 0.23 | 78.4 | 345,612 |
| cluster-slate-n3-r1 | slate | 3 | 1 | 100 | 107 | 0.11 | 0.65 | 97.7 | 64,431 |
| cluster-slate-n3-r3 | slate | 3 | 3 | 103 | 108 | 0.47 | 2.34 | 77.4 | 11,876 |

### AwaitDurable=false (relaxed; pair with R>=2 in production)

| Scenario | Backend | Nodes | R | Put p50 (ms) | Put p99 (ms) | Get p50 (ms) | Get p99 (ms) | Put ops/s | Get ops/s |
|---|---|---|---|---:|---:|---:|---:|---:|---:|
| raw-slate-relaxed | slate | 1 | 1 | 0.04 | 0.94 | 0.02 | 0.23 | 118,411 | 259,791 |
| cluster-slate-n1-r1-relaxed | slate | 1 | 1 | 0.11 | 0.73 | 0.02 | 0.37 | 49,234 | 192,231 |
| cluster-slate-n3-r1-relaxed | slate | 3 | 1 | 0.26 | 2.33 | 0.13 | 0.67 | 19,037 | 59,159 |
| cluster-slate-n3-r3-relaxed | slate | 3 | 3 | 0.39 | 3.79 | 0.40 | 1.27 | 11,661 | 15,115 |

The two R=1 relaxed rows (`raw-slate-relaxed`, `cluster-slate-n1-r1-relaxed`) are informational only: relaxed durability at R=1 is unsafe (a single replica crash inside the WAL-flush window loses the un-flushed writes), and per spec a fast-ack backend must be paired with `ReplicationFactor >= 2` in production. The numbers are included so the shape of the speedup is visible across the suite, NOT as a recommended deployment target. `cluster-slate-n3-r3-relaxed` is the production-shaped relaxed scenario.

Workload: 500 ops per phase, 1024-byte values, 8 concurrent clients. MinIO running locally in colima (single-node, no replication, plaintext HTTP on the loopback). SlateDB v0.13.1.

### Strict vs relaxed put throughput (same shape, same MinIO)

| Shape | Strict ops/s | Relaxed ops/s | Speedup |
|---|---:|---:|---:|
| raw-slate | 77.2 | 118,411 | 1533x |
| cluster-slate-n1-r1 | 78.4 | 49,234 | 628x |
| cluster-slate-n3-r1 | 97.7 | 19,037 | 195x |
| cluster-slate-n3-r3 | 77.4 | 11,661 | 151x |

### Where the cost goes (slate)

**Strict mode is S3-RTT-bound, full stop.** Every strict shape pins at ~77-100 ops/s and p50 ~100ms. That's the SlateDB commit waiting on the object-store sync against local-loopback MinIO. None of the shale-layer cost (envelope encode, ring lookup, gRPC fan-out) is visible against that backdrop: the cluster's `n3-r3` write is no slower than the raw single-store write because all three replicas' SlateDB commits overlap and the slowest one still finishes in ~100ms. Throughput scales with concurrency rather than fan-out, because each concurrent client is mostly blocked on S3 PUT latency that other clients can overlap with.

**Relaxed mode unmasks the shale-layer cost.** With AwaitDurable=false the SlateDB ack is microsecond-scale (memtable insert), so the S3-RTT floor disappears and the remaining cost is the bench loop + envelope encode + (in the cluster shapes) the ring lookup + gRPC fan-out. The headline 1533x speedup at `raw-slate` is the naked WAL-flush savings (118K ops/s relaxed vs 77 ops/s strict); the cluster shapes give back successive chunks of that to the cluster layer's own cost. `cluster-slate-n1-r1-relaxed` (628x) loses ~58% to the in-process cluster layer (49K vs raw's 118K), `cluster-slate-n3-r1-relaxed` (195x) further loses ~61% to the gRPC fan-out (19K vs 49K), and the production-shaped `cluster-slate-n3-r3-relaxed` (151x) is what an operator running with R=3 + fast-ack should expect. The relaxed cluster numbers now look a lot like pebble's: n3-r3 puts at ~12K/s for both backends, which is the expected outcome when the backend Put is no longer the bottleneck.

**Was the speedup what we predicted?** Roughly yes for the magnitudes (slatedb's per-Put cost falling from ~13ms typical to "microseconds + background flush" matches "two-to-three orders of magnitude" rule-of-thumb), but bigger than expected at the raw shape. The 1533x at `raw-slate` is partly an MinIO-on-colima artifact: virtualized disk fsync inside colima is unusually slow, so the strict baseline is depressed relative to real-S3 or even bare-metal MinIO. On real AWS S3 the strict floor would be 50-150ms instead of 100ms, but the relaxed ceiling would also rise (the bench's relaxed CPU cost would no longer be the bottleneck on a faster path either). The cluster-shaped speedups (628x / 195x / 151x) are the more meaningful numbers because they include the in-process shale cost.

**Reads are essentially unchanged across modes.** WriteOptions only affects the durability path on Put/Delete/Commit; reads serve from the SlateDB blockcache either way. The strict and relaxed read columns are within noise of each other for the same shape.

**Multi-writer per store is fenced; the harness gives each node its own DbName.** SlateDB enforces single-writer-per-store via writer-epoch. Naively wiring N cluster nodes to the same SlateDB instance would have them fence each other within milliseconds of cluster startup. The harness gives each node its own DbName (= node id) inside the shared bucket, so the 3-node scenarios run as 3 independent SlateDB stores. That's not how a real shale-on-slate deploy would be shaped (in production, ONE SlateDB per cluster, with shale's replication providing HA on top), but it's the only multi-node story consistent with SlateDB's single-writer guarantee in a single-process bench.

**Caveat: this is loopback MinIO with no replication, no TLS, no S3 throttling.** Real S3 / R2 / GCS RTTs from a US-East EC2 instance to the same-region bucket are typically 5-15ms one-way; cross-region or cross-cloud is 50-200ms. A real-world strict slate write at ~3x that RTT puts the floor at ~15-45ms p50 in the best case and ~150-600ms in the worst. The 103ms we see here is dominated by MinIO's own commit fsync inside colima (virtualized disk on Apple Silicon).

### Reproducing the slate run

```bash
make bench-v0.5-slate                                    # canonical (500 ops, 1KB, conc 8; all 8 scenarios)
SHALE_BENCH_OPS=2000 make bench-v0.5-slate               # more samples; ~30-60s/scenario
SHALE_BENCH_CONC=32 make bench-v0.5-slate                # squeeze peak throughput
SLATEDB_LIB_DIR=/path/to/lib make bench-v0.5-slate       # custom SlateDB cdylib
```

To drive one scenario directly (e.g. just the production-shaped relaxed case), build the harness with the slatedb tag and pass `--slate-await-durable=false`:

```bash
CGO_ENABLED=1 \
  CGO_LDFLAGS="-L/path/to/slatedb/target/release" \
  DYLD_LIBRARY_PATH=/path/to/slatedb/target/release \
  go build -tags slatedb -o shale-bench ./cmd/shale-bench/
SHALE_BENCH_S3_ENDPOINT=http://127.0.0.1:9000 \
  SHALE_BENCH_S3_BUCKET=shale-bench \
  SHALE_BENCH_S3_ACCESS_KEY=... \
  SHALE_BENCH_S3_SECRET_KEY=... \
  ./shale-bench --mode cluster --backend slate --nodes 3 --rf 3 \
                --slate-await-durable=false \
                --ops 500 --value-size 1024 --concurrency 8 --json
```

Requirements:

- Docker reachable (colima or Docker Desktop on macOS; native docker in CI).
- The SlateDB cdylib (`libslatedb_uniffi.{dylib,so}`). Default lookup is `/private/tmp/slatedb-src/target/release`; override via `SLATEDB_LIB_DIR`.
- cgo toolchain (`CGO_ENABLED=1`), same setup as `make test-slate`.

The script brings up `shale-bench-minio` on host port 19000, creates the `shale-bench` bucket, runs the 4 scenarios (re-creating the bucket between each so the previous run's writer epochs don't fence the next open), prints the markdown table, and tears MinIO down on exit. To reuse an existing MinIO instead, export `SHALE_BENCH_S3_ENDPOINT` + `SHALE_BENCH_S3_BUCKET` + `SHALE_BENCH_S3_ACCESS_KEY` + `SHALE_BENCH_S3_SECRET_KEY` before invoking; the script will skip the docker dance.

## What this bench does NOT measure

- **Cross-host network cost.** Loopback gRPC + loopback membership have no real RTT. Real-world R=3 numbers will be worse (~ +1 inter-host RTT per fan-out) but multi-host setups may also recover throughput because each pebble backend gets its own disk.
- **Cross-region S3 RTT for the slate suite.** Local-MinIO numbers are a floor; real AWS S3 RTTs push slate write p50 into the 50-200ms range depending on placement.
- **Topology change cost.** Membership churn (node add/leave/rebalance) is its own measurement; the v0.5 suite assumes a stable cluster.
- **Large values.** 1KB is small; gRPC overhead dominates. A 64KB/256KB variant would tell a different story (backend cost rises, gRPC cost stays roughly fixed per call).
- **Long-tail percentiles beyond p99.** p99.9 / max would surface GC pauses + scheduler stalls on the in-process model. Worth a follow-up.
- **Slate reads against a cold blockcache.** The current slate bench writes then immediately reads the same keys, so every Get hits SlateDB's in-process blockcache and never round-trips to S3. A workload larger than the cache (or a fresh-process read phase) would surface object-storage read RTT in the Get column too.
