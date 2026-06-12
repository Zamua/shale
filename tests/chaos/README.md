# Chaos / soak harness for multi-node shale (v0.8)

This is the "prove it" step for v0.8 before a real N=2 production deploy. The
unit tests and the `lossless_*_gate_test.go` gates already pass; they pin
specific, hand-constructed scenarios. This harness is the sustained,
adversarial, end-to-end exercise: writes and reads under load while nodes fail,
recover, join, leave, and the cluster reshards (singly and in combination), all
under one strict invariant:

> **NO ACKED WRITE MAY BE LOST.**

"Acked" means the client `Put` / `Delete` / `Transact` returned success. A
retryable error (`codes.Unavailable` during a handoff/acquiring window, or
`codes.FailedPrecondition` during a reshard cutover, or the cluster-wide freeze
refusal) is **not** an ack: the harness retries it, and only a `nil` return is
recorded as acked. This mirrors the faithful client-retry behavior the whole
v0.8 safety story rests on.

The harness runs **in-process** today (N nodes via goroutines + ephemeral ports,
reusing the `tests/integration` fixtures: `startSharedNode`, the
`sharedfactory` shared backing, `openClusterRetryBind`, `waitForMembersAll`).
The design keeps the workload driver and the oracle behind a small `Cluster`
adapter interface so the SAME driver + oracle can later be pointed at a real
N-node deploy (gRPC clients to live `shaled` processes) without touching the
core logic. See "Extending to a real N-node deploy" below.

## What the harness found, and the fix it now gates

The harness did its job: it surfaced a real, reproducible v0.8 data-loss behavior
that the focused `lossless_*_gate_test.go` gates do NOT exercise, because none of
them adds a node to an already-resharded cluster. That finding has since been
FIXED (`feat(cluster): join-after-reshard generation propagation via GenState peer
RPC`); this harness is the regression gate that proves the fix and would catch a
regression.

**The finding (now fixed).**

> **A node that joins a cluster which has already resharded started at generation 0
> and did not learn the cluster's current generation.** It therefore routed and
> claimed ownership at the wrong generation, orphaning a set of units: a `Put` or
> `Get` for a key in an affected unit resolved to an "owner" that disclaimed it,
> and shale's forwarding loop-guard returned a permanent
> `FailedPrecondition: forwarding loop refused: this node does not own the key` /
> `this node is not a replica for the key`. The condition did NOT self-heal: it
> persisted through the workload, through reconcile, and through the post-chaos
> settle, so reads and writes for those keys stayed permanently unavailable.

Where it came from: `Cluster.initGenState()` unconditionally initialized a fresh
node at `gen: 0`, and there was no path for a joiner to learn the live generation
from its peers. The single-node and fixed-membership reshard paths were always
fine (the gates prove it); the gap was specifically **join-after-reshard**.

**The fix.** A multi-backend joiner (Open WITH seeds) now learns the cluster's
live `{generation, unit-count}` by a synchronous peer RPC (`GenState`) to a seed
BEFORE it derives or mounts any unit, then seeds its `genState` from the answer -
so it never routes / owns / serves a key at gen 0 after the cluster has resharded.
It fails closed if the seed is unreachable (Open fails, no gen-0 fallback). See
`docs/SPEC.md` -> "Generation propagation to a joining node".

**Before / after (the regression proof).** The previously-failing seeds reproduced
the loss with `SHALE_CHAOS_DURATION=8s SHALE_CHAOS_CHAOS_EVERY=400ms`:

| seed | before the fix | after the fix |
|------|----------------|---------------|
| 7  | ~1311 `LOST` violations, all `forwarding loop refused`; workload wedged (only ~25k acked puts); `retryable_retries` ~12k hammering the permanent error; `WaitSettled` timed out | ZERO `forwarding loop refused` loss; ran cleanly through reshard + join (~447k acked puts) |
| 11 | join-after-reshard loss of the same shape | zero loss across ~387k acked writes |

A seed that did NOT combine a reshard with a post-reshard join always passed clean
both before and after - which is what made the gap a genuine *combination*
finding, invisible to any single gate. The tuned default soak (below) now fires
the reshard-then-join path on EVERY seed via the coverage prologue, so the fix is
exercised by default rather than only by a lucky seed.

## How to run

The harness is a long-running soak test, gated behind the `chaos` build tag so
the normal `go test ./...` stays fast and never picks it up.

```sh
# Run the DEFAULT soak. This is a genuine soak: a deterministic COVERAGE PROLOGUE
# fires reshard, reshard-while-down, leave-mid-reshard, join, and kill+restart
# ONCE up front (so every run exercises the hard cases - including the
# join-after-reshard path - regardless of seed), then a seeded random schedule
# piles adversarial churn on top. The vacuous-check ENFORCES that the combinations
# fired, so a default run cannot pass without proving the hard cases ran.
go test -tags chaos ./tests/chaos/ -run TestChaosSoak -v -timeout 12m

# Fast SMOKE variant for the inner edit loop. It SKIPS the prologue and RELAXES
# the combination-coverage gate (loss detection stays fully active), so it just
# checks the harness builds + the happy path works. Use it to iterate; use the
# default (or a long DURATION) to actually prove the cluster.
SHALE_CHAOS_SMOKE=1 go test -tags chaos ./tests/chaos/ -run TestChaosSoak -v -timeout 8m

# Longer, more adversarial run with an explicit seed (reproducible):
SHALE_CHAOS_SEED=1234567 SHALE_CHAOS_DURATION=120s \
  go test -tags chaos ./tests/chaos/ -run TestChaosSoak -v -timeout 20m

# Knobs (all optional; env-driven so no flag plumbing through go test):
#   SHALE_CHAOS_SEED       int64   RNG seed (default 1; ALWAYS logged so a
#                                  failing run is reproducible by re-exporting it)
#   SHALE_CHAOS_DURATION   dur     wall-clock workload budget (default 30s;
#                                  6s under SHALE_CHAOS_SMOKE). An explicit value
#                                  overrides the smoke default.
#   SHALE_CHAOS_SMOKE      bool    fast, shallow inner-loop variant: skip the
#                                  coverage prologue + relax the combination gate
#   SHALE_CHAOS_WRITERS    int     concurrent writer goroutines (default 6)
#   SHALE_CHAOS_READERS    int     concurrent reader goroutines (default 4)
#   SHALE_CHAOS_NODES      int     initial node count (default 3)
#   SHALE_CHAOS_UNITS      int     initial unit count, power of two (default 8)
#   SHALE_CHAOS_CHAOS_EVERY dur    mean interval between random chaos events
#                                  (default 250ms; 300ms under SHALE_CHAOS_SMOKE)
```

Without the `-tags chaos` build tag, the test file is excluded from the build
entirely (`//go:build chaos`), so `go test ./...` is unaffected. (The pure oracle
in `oracle.go` + `oracle_test.go` carry NO build tag, so the model itself is still
unit-tested by the normal suite.)

Wall-clock note: the elapsed run time is meaningfully longer than
`SHALE_CHAOS_DURATION`. The duration bounds the WORKLOAD window (how long writers
and readers hammer the cluster); on top of it sit the structural chaos events
(each kill+restart, join, leave, and reshard blocks the scheduler on real
topology convergence - the per-event settle + convergence wait, NOT the
`CHAOS_EVERY` interval, is what dominates the event rate), then a final settle
phase that drives the cluster to a fully-converged, all-units-mounted state before
the verification sweep. The 30s default lands ~9-12 events and completes in 1-2
minutes of wall clock; budget the `-timeout` accordingly. The coverage prologue
runs to completion even if the duration timer fires mid-way, so a too-short
DURATION just means the random schedule gets little or no turn (the prologue still
proves the hard cases). Pick the workload duration for how much *additional*
random adversarial overlap you want on top of the guaranteed prologue.

The RNG is seeded **only** from `SHALE_CHAOS_SEED` (default 1). The harness never
reads wall-clock time or `crypto/rand` for any decision that affects which chaos
event fires or which key a writer touches; every such choice is drawn from the
seeded `*math/rand.Rand`. A given seed + knob set therefore replays the same
sequence of chaos events (modulo genuine scheduler nondeterminism in goroutine
interleaving, which the oracle is robust to: it only ever asserts on acked
writes, never on timing).

**Run it on a reasonably quiet box.** The N nodes are in-process goroutines, so
the whole cluster is CPU-bound on the host. The oracle's verdicts are about DATA
(values, not timing), but a few availability/settle assertions are bounded by wall
clock (the post-chaos `WaitSettled`, the final-sweep per-key budget, the
continuous-read `reverify` budget). Those budgets are generous on an idle box, but
a co-scheduled heavy job (notably another `go test -race` churning all cores) can
starve the cluster enough that convergence overruns a budget and a run reports a
spurious availability violation even though no acked write was actually lost. If a
seed fails once under load and passes cleanly when re-run on a quiet box, that was
host contention, not a cluster bug - re-export the logged seed and re-run it
isolated to confirm. (A genuine loss reproduces deterministically from its seed
regardless of host load; a contention artifact does not.)

## Architecture

Three pieces, organized DDD-style (the oracle is a pure domain model with no
I/O; the driver and scheduler are orchestration; the cluster is an adapter).

```
  +-------------------+        +------------------+        +------------------+
  |  M writer         |  put   |                  |  read  |  N reader        |
  |  goroutines       +------->|   live cluster   |<-------+  goroutines      |
  |  Put/Delete/      |  (R=1, |  (in-process     |        |  (verify latest  |
  |  Transact         |  dur-  |   N nodes,       |        |   acked value)   |
  |                   |  able- |   shared backing)|        |                  |
  +---------+---------+  before+--------+---------+        +---------+--------+
            | record ack         ^      | chaos                     | check
            v                    |      v                           v
  +-------------------------------------------------------------------------+
  |                    ORACLE  (pure model, mutex-guarded)                   |
  |   key -> {value, version, deleted}  for every ACKED write/delete        |
  |   continuous + final assertion: every acked key reads back its latest   |
  |   recorded value from EVERY node (via gRPC forwarding); a deleted key   |
  |   reads back not-found.                                                  |
  +-------------------------------------------------------------------------+
            ^
            | inject on a seeded schedule
  +---------+---------+
  |  CHAOS scheduler  |   kill+restart node | join node | leave node | reshard
  |  (seeded RNG)     |   + COMBINATIONS (reshard while a node is down;
  |                   |    node leaving mid-reshard -> clean ABORT)
  +-------------------+
```

### The oracle (the core)

`oracle.go` is a pure, I/O-free domain model. It holds, per key, the latest
**acked** state:

- `version`: a monotonically increasing per-key counter. Every acked write/delete
  bumps it. The writer encodes the version into the value (`v<version>|payload`)
  so a reader can detect a *stale* value (an older acked version surfacing after
  a newer one) distinctly from a *lost* value (not-found / wrong payload).
- `value`: the exact bytes the client last acked for the key, or a tombstone
  marker if the last acked op was a `Delete`.
- `deleted`: whether the latest acked op was a delete (so the expected read is
  `ErrNotFound`).

Concurrency rule: a key is only ever written by ONE writer goroutine (the key
space is partitioned by writer id), so "latest acked version" is unambiguous
without distributed coordination. This is the standard trick for a lossless
oracle under concurrency: avoid two writers racing the same key so the model has
a single source of truth per key. (Cross-key `Transact` is exercised separately,
on a disjoint counter-key space, with the read-modify-write invariant checked at
the end.)

The oracle exposes:

- `RecordPut(key, value, version)` / `RecordDelete(key, version)`: called by a
  writer ONLY after the cluster returned success (an ack). Takes the model lock,
  bumps the per-key version, stores the value/tombstone.
- `Expected(key) -> (value, deleted, version, known)`: what a reader should see.
- `Verify(...)`: given an observed read result for a key, decide PASS / STALE /
  LOST / CORRUPT against the recorded latest. A read of a *not-yet-acked-newer*
  version is fine (the reader raced the writer); a read of an *older-than-latest*
  acked version, or not-found for a live key, or wrong payload, is a violation.

The oracle never blocks on I/O and never knows about nodes, rings, or RPC; it is
testable in isolation (`oracle_test.go`, which runs under the normal suite - it
has no build tag - so the model itself is covered even when the soak is not run).

### The workload driver

`harness.go`:

- **M writer goroutines.** Each owns a disjoint key subspace (`w{id}-k{n}`). On
  each tick it picks an operation by the seeded RNG (weighted: ~70% Put, ~15%
  Delete of a previously-written key, ~15% Transact on a per-writer counter key),
  issues it through a *rotating* entry node (so writes route both locally and
  forwarded), retries the retryable signals up to a deadline, and on success
  calls the matching `oracle.Record*`. Anything that is not retryable and not a
  benign not-found-on-delete is a hard failure recorded as a violation.
- **N reader goroutines.** Each repeatedly picks a random known key (from the
  oracle's key set) and a random entry node, reads with the standard retry, and
  feeds the result to `oracle.Verify`. A LOST/STALE/CORRUPT verdict is recorded
  as a violation immediately (and the run can fail fast or accumulate, per knob).
  Readers run continuously, so the oracle is checked *during* chaos, not only at
  the end.
- A **final full sweep** after the workload stops and the cluster quiesces:
  every acked key is read from EVERY node and checked. This is the belt to the
  continuous readers' suspenders - it catches a loss that the random readers
  happened not to sample mid-run.

### The chaos scheduler

`chaos.go` injects events on a seeded schedule. Between events it sleeps a
randomized interval around `SHALE_CHAOS_CHAOS_EVERY`. Each event is drawn from
the seeded RNG over the enabled event types, subject to guards (e.g. never leave
the last node; never drive node count below 1; serialize a reshard against a
membership change so the two coordinators do not livelock). Event types:

1. **NODE KILL + RESTART** (`kill`). Force-stops a node's gRPC server
   (`grpc.Server.Stop`, no graceful drain) and shuts its cluster. Survivors
   re-acquire its units' leases via the reconcile; no acked write may be lost;
   writes resume on survivors. After a randomized down-interval, a fresh node is
   started (same backing, new id) to model the restart rejoining.
2. **NODE JOIN** (`join`). Starts a new `sharedNode` on the shared backing,
   seeded at an existing member. The ring redistributes units to it via the
   Phase 3 lease handoff (copy-free). No acked write lost across the handoff.
3. **NODE LEAVE** (`leave`). Gracefully closes a node (broadcasts the memberlist
   Leave). Survivors converge and re-acquire its units. No acked write lost.
4. **RESHARD** (`reshard`). Calls `Reshard()` on a coordinator node, doubling
   the unit count cluster-wide via the freeze barrier. No acked write lost across
   the reshard; the concurrent writers' freeze-window refusals are retried, not
   acked-then-lost.

**Combinations** (the high-value adversarial cases the single gates cannot
express):

- **Reshard while a node is down.** A `kill` leaves a node down; while it is
  down, a `reshard` runs on a survivor coordinator over the reduced membership.
  The reshard must complete (or cleanly abort) over the live members; the
  restarted node rejoins at the new generation and acquires its units. No acked
  write lost.
- **Node leaving mid-reshard -> clean ABORT.** A membership change concurrent
  with the freeze barrier must ABORT the reshard cleanly: every node unfreezes,
  discards half-built children, stays at gen g, and writes resume. The oracle
  then sees zero loss (the abort touched no live data) and the next reshard
  attempt can succeed. The scheduler arms this by firing a `leave`/`kill`
  shortly after kicking off a `reshard` (timing-randomized by the seed).

The scheduler serializes its own events behind a single mutex so two structural
changes never overlap *from the harness's point of view*; the interesting
concurrency (a membership change arriving while a reshard's barrier is in flight)
is produced deliberately by the combination events above, which start the
reshard in a goroutine and then fire the membership change while it is running.

## Invariants checked

| # | Invariant | Where checked |
|---|-----------|---------------|
| 1 | No acked write lost: every acked `Put` key reads back its latest acked value from at least one node, and no node returns a wrong value. | continuous readers + final sweep |
| 2 | No acked delete resurrected: a key whose latest acked op is `Delete` reads back not-found. | continuous readers + final sweep |
| 3 | No persisted stale value: a read that returns an older-than-latest acked value is re-verified with a bounded retry; if it never converges to the correct value it is a violation (a transient mid-cutover stale read that self-heals is counted, not failed). | `oracle.Verify` + reverify |
| 4 | No corruption: a read never returns a payload that was never acked for that key (decoded by version, so the in-flight write landing a hair early is not mistaken for corruption). | `oracle.Verify` |
| 5 | Transact RMW integrity: a per-writer counter incremented under `Transact` equals the number of acked increments (no lost update). The increment is IDEMPOTENT (monotonic-max toward the writer's acked-count+1), so a Transact whose commit was durable-but-unacked (entry node died after the commit landed) re-runs as a no-op instead of double-counting - this makes `stored == acked` an exact, falsifiable lossless check robust to Transact's at-least-once contract under a crash, rather than one confounded by benign retries. | counter sweep |
| 6 | Reads stay available: a reader running through chaos, WITH the standard retry, never hits a permanent failure or a permanently wrong value. | continuous readers + reverify |
| 7 | Liveness: the cluster keeps acking new writes throughout (the workload does not wedge); at least one ack per writer. | final accounting |
| 8 | Clean abort on mid-reshard membership change: an aborted reshard leaves the cluster at gen g, unfrozen, dataset intact. | combination event + final sweep |
| 9 | Cluster re-settles after chaos: before the final sweep, every unit is mounted on the node the ring assigns it to, at one uniform generation. A cluster that cannot re-converge to a clean routing state is itself a finding. | `WaitSettled` |

## Detection power: what this harness CAN and CANNOT catch

This is an **in-process** harness: N nodes are goroutines sharing ONE in-memory
backing (`sharedfactory`). That choice is what makes it cheap and fast, and it is
exactly right for what the harness is FOR. But it bounds what it can prove, and
the harness must not be over-trusted past that bound.

**What it PROVES (the cluster-coordination layer).** Every loss class that lives
in routing, ownership, and the protocol state machine: generation-qualified
routing, the consistent-hash ring, the Phase 3 lease handoff (mount / release /
epoch fencing), the multi-node reshard freeze barrier (freeze / bisect / flip /
resume / abort), the join-time generation propagation (the fix above), and the
`Transact` OCC commit re-resolving the owner across a cutover. A bug in any of
these - a key orphaned to the wrong owner, a write acked into a unit that FLIP
then retires, a stale-generation joiner, a lost RMW under a reshard - manifests as
a wrong / missing / resurrected value the oracle catches. This is the layer the
v0.8 multi-node story is built on, and it is the layer a real N=2 deploy most
needs proven before launch.

**What it CANNOT catch (storage durability).** Because every node shares ONE
in-memory backing, a write that the cluster logic "made durable" is instantly
visible to every node and never actually has to survive a process restart, an
fsync gap, or an object-store write that was acked-but-not-yet-persisted. The
whole **durability-window** loss class is therefore INVISIBLE here:

- A real `slatedb` / object-storage backend acks a write into an in-memory
  memtable + a WAL whose flush to object storage is asynchronous. A process crash
  in that window loses an "acked" write the harness's shared backing would have
  kept. The in-process `KillNode` drops the gRPC server + closes the cluster
  handle, but the data is still in the shared backing for the survivor to mount -
  it models a COORDINATION failover, not a STORAGE loss.
- Torn / partial object writes, fsync lying, S3 read-after-write consistency
  edge cases, and clock-skew effects on LWW stamps across real machines are all
  out of reach of an in-process model.

So a green soak here means **the cluster coordination is sound** (routing,
handoff, reshard, generation), NOT that **storage is durable** on a real backend.
The two are independent: this harness retires the coordination risk; a real
backend's durability has to be proven separately (a crash-consistency test against
`slatedb` on real object storage, fault injection at the WAL/flush boundary, etc.)
before trusting end-to-end durability. The "Extending to a real N-node deploy"
section below is the path to that second proof: the SAME oracle + driver, pointed
at real `shaled` processes over real network + real storage, is what would finally
close the durability-window class. Until then, do not read "the chaos soak is
green" as "no acked write can ever be lost on prod" - read it as "the cluster
never loses an acked write to a coordination bug," which is a narrower (true,
valuable) claim.

### Transient vs persisted: why reads are re-verified

shale's contract under a reshard/handoff cutover is *retryable-availability plus
eventual correctness*, not per-read linearizability across the cutover window: a
read that briefly routes to a stale generation/owner may return an older value or
get a "this node does not own the key" refusal while that node's ring catches up.
The harness treats a single non-PASS continuous read as a *candidate*: it
re-verifies the key with a bounded retry (`reverify`). If the key converges to its
correct value, it was the expected transient (counted as `transient_reads`); if it
stays wrong past the retry budget, it is a hard violation (a genuinely lost / stale
/ resurrected acked write). This mirrors the existing reshard gate's "read through
the whole window with retry, never a *permanently* wrong value" assertion. The
final sweep, run only after `WaitSettled` has driven the cluster to a clean routing
state, is strict: there is no cutover in flight, so any wrong/missing value there
is unconditionally a violation.

The final sweep reads each key from EVERY live node and passes the key iff at
least one node serves the correct latest value AND no node serves a wrong one. A
node that refuses to route a key it does not own (stale ring) is skipped, not
counted as loss - the value is still served by the owner; only a key that NO node
can serve correctly is a genuine availability loss.

## Metrics reported

At the end (and on `-v`, periodically) the harness logs a report:

- `acked_puts`, `acked_deletes`, `acked_transacts`: total acked writes by kind.
- `reads_verified`: total reads checked by the readers + final sweep.
- `transient_reads`: continuous reads that were non-PASS on first read but
  converged to the correct value on re-verify (expected mid-cutover artifacts, not
  violations). Quantifies how much cutover turbulence the readers actually saw.
- `chaos_events`: count by type (`kill`, `restart`, `join`, `leave`, `reshard`,
  `reshard_aborted`).
- `combination_events`: the high-value COMBINATIONS, tracked DISTINCTLY from the
  base counters so a passing report can PROVE the hard cases actually fired (the
  base counters alone cannot - a reshard-while-down bumps both `kill` and
  `reshard`, indistinguishable from a plain kill plus a plain reshard):
  - `reshard_while_down`: a reshard driven over the reduced membership while a node
    was down; `reshard_while_down_committed`: of those, how many committed the
    reshard (vs aborted because of the down node).
  - `leave_mid_reshard`: a membership change fired to land mid-freeze-barrier;
    `leave_mid_reshard_aborted`: of those, how many drove the reshard to a clean
    ABORT (the intended outcome) vs committed before the leave landed.
- `retryable_retries`: how many times a retryable error was retried (a proxy for
  how much cutover/handoff/freeze turbulence the run actually produced - a run
  with zero is suspiciously quiet and is flagged).
- `final_node_count`, `final_unit_count`, `final_generation`.
- `violations`: the list of oracle violations (LOST / STALE / CORRUPT /
  RESURRECTED). **The pass condition is `len(violations) == 0.`**

A run is reported *vacuous* and FAILS if it never stressed anything - a soak that
proves nothing is a failure, not a pass. A run is vacuous if it produced zero
acked writes, zero chaos events, or zero retryable retries; AND, for a real soak
(not `SHALE_CHAOS_SMOKE`), if it did not COMMIT at least one reshard and fire each
combination at least once. The deterministic coverage prologue makes a default run
satisfy the combination requirement by construction, so the vacuous-check is a
backstop that catches a misconfiguration (e.g. a too-short DURATION that cut the
prologue off, or a future change that broke an event handler) rather than letting
it pass green.

## Extending to a real N-node deploy later

The driver and oracle depend on a tiny `ClusterClient` adapter
(`Put/Get/Delete/Transact/Reshard`) and a `Topology` adapter
(`Members/AddNode/RemoveNode/KillNode/RestartNode`), both implemented in
`adapter_inproc.go` against the in-process `sharedNode` fixtures. To run the same
soak against a real N-node `shaled` cluster:

1. Implement `ClusterClient` with a real gRPC client (the `pkg/rpc` client
   already exists) dialing each node's address.
2. Implement `Topology` with operator actions - `AddNode` = launch a `shaled`
   process / pod; `KillNode` = `SIGKILL`; `RestartNode` = relaunch; `RemoveNode`
   = graceful stop / drain; `Reshard` = the admin RPC. On real infra these are
   orchestrator calls (systemd, k8s, the deploy script), not goroutines.
3. The oracle, the writer/reader loops, the chaos scheduler, and every invariant
   above are unchanged. The seed-driven reproducibility carries over; only the
   adapters change.

This is the whole reason the oracle is pure and the cluster is behind an
interface: the in-process soak is the cheap, fast confidence-builder, and the
exact same adversarial program graduates to the real deploy when you want to
prove it against process boundaries and real network faults.

## The real-cluster orchestrator (`//go:build chaosreal`)

The step-2 "prove it on a REAL cluster" validation. It reuses the SAME pure oracle
(`oracle.go`, no build tag) but drives a deploy of SEPARATE OS PROCESSES instead of
goroutines: real `shaled-slate` child processes, real gRPC + memberlist over the
network, a SHARED object-storage bucket (MinIO) as the durable backing. One
invocation owns the whole lifecycle (launch, kill, restart, teardown):

```sh
# Build shaled-slate (cgo + the slatedb cdylib on the loader path) from backends/slate:
CGO_ENABLED=1 CGO_LDFLAGS="-L$HOME/.local/lib" DYLD_LIBRARY_PATH="$HOME/.local/lib" \
  go build -tags slatedb -o /tmp/shaled-slate ./cmd/shaled-slate

# Stand up MinIO + a FRESH bucket (the shared durable backing), then run the orchestrator:
SHALE_REAL_BINARY=/tmp/shaled-slate \
SHALE_REAL_LIBDIR=$HOME/.local/lib \
SHALE_REAL_ENDPOINT=http://127.0.0.1:9000 \
SHALE_REAL_BUCKET=shale-real-chaos \
SHALE_REAL_ACCESS_KEY=admin SHALE_REAL_SECRET_KEY=supersecret \
  go test -tags chaosreal ./tests/chaos/ -run TestRealClusterChaos -v -timeout 10m
```

Gated behind `//go:build chaosreal`, so the normal suite AND the in-process chaos
soak (`//go:build chaos`) are untouched. With no env set, the test SKIPS with a
precise reason (which piece is missing), never a fake pass. The adapter lives in
`adapter_real.go` (the `Topology`: launch / SIGKILL / SIGTERM / Reshard-seam over
`os/exec`) + `realclient.go` (the `ClusterClient`: Put/Get/Delete over the real
`pkg/rpc` gRPC client); the orchestration + invariants live in `run_real.go` +
`chaos_real_test.go`.

### What it can and cannot prove (read before trusting a green run)

`shaled-slate` runs the LEGACY per-node backend mode: ONE `slate.Slate` per
process, the ring routing each key to a single owner. It is NOT wired for
multi-backend mode (`BackendFactory` + `UnitCount`), because no slatedb
`BackendFactory` exists and `shaled-slate` exposes no unit-count flag. Two
consequences bound the orchestrator, both reported honestly rather than faked:

- **Reshard is unavailable.** `Cluster.Reshard` refuses outside multi-backend
  mode, and there is no operator Reshard RPC on the gRPC surface. The Reshard seam
  is present and returns a typed "unsupported" error; the report shows
  `reshard_supported=false`.
- **Cross-process durability handoff is unavailable.** Each node owns a DISTINCT
  slatedb instance (a distinct DbName prefix in the shared bucket - two writers on
  one (bucket, dbName) fence each other). When a node is SIGKILLed, its OWNED keys
  are durable in ITS OWN instance, but a survivor owns a DIFFERENT instance and
  legacy mode has no per-unit lease handoff to re-mount the dead node's instance.
  So a survivor cannot serve a dead owner's keys. Multi-backend mode (one
  slatedb-per-unit keyed by `GenUnit`, re-mounted by the survivor on lease
  re-acquire) is what would close this; it is not yet wired for slate.

The run is therefore split into two phases:

- **PHASE 1 - stable-membership durability (HARD-gated).** Seed a dataset against
  stable membership, then SIGKILL a non-founder node mid-workload and probe the
  surviving founder for EVERY acked key. Each key is `SURVIVED` (founder serves the
  correct durable value across the hard process death - the real durable-before-ack
  win), `UNAVAILABLE` (founder cannot serve it: the owner-killed legacy gap,
  counted), or a `VIOLATION` (founder serves a WRONG value - stale/corrupt/
  resurrected). The HARD assertion is wrong-value-only: in a stable-membership kill
  a survivor either owns the key (correct value) or does not (unavailable), so it
  can NEVER legitimately serve a wrong value. A wrong value across real process
  death + real gRPC + a real object-store round-trip is a genuine bug and fails the
  run. The killed node is then restarted and must rejoin.
- **PHASE 2 - churn coverage (INFORMATIONAL).** Real joins / leaves / kill+restarts
  over the real network. Under legacy mode, ring churn with no inter-instance data
  movement makes resurrection/staleness inherent (a node re-acquiring ownership
  serves its own stale copy), so phase-2 wrong-value reads + un-acked writes are
  recorded as METRICS (`churn_wrong_values`, `write_failures`), NOT failures -
  gating on them would just re-assert the known mode limitation.

A green run means: the real network + real process death + real object storage do
not lose or corrupt a write the legacy single-owner model is responsible for, and a
quantified fraction of the acked set provably survives a hard SIGKILL. It does NOT
mean cross-process durable handoff or reshard works on slate - those need the
multi-backend slate factory, which is the next step before a prod 2-pod cutover.
The empirical evidence the durability gap is real (and not a harness artifact): a
2-node hand smoke writes 20 keys, SIGKILLs the owner of half, and the survivor then
serves only the keys it owned - the killed owner's keys read back not-found, their
bytes stranded in the dead node's DbName prefix.
