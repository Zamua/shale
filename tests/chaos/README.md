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

## What the harness found (read this before the prod deploy)

The harness does its job: it surfaced a real, reproducible v0.8 behavior that the
focused `lossless_*_gate_test.go` gates do NOT exercise, because none of them adds
a node to an already-resharded cluster.

> **A node that joins a cluster which has already resharded starts at generation 0
> and does not learn the cluster's current generation.** It therefore routes and
> claims ownership at the wrong generation, which orphans a set of units: a `Put`
> or `Get` for a key in an affected unit resolves to an "owner" that disclaims it,
> and shale's forwarding loop-guard returns a permanent
> `FailedPrecondition: forwarding loop refused: this node does not own the key` /
> `this node is not a replica for the key`. The condition does NOT self-heal: it
> persists through the workload, through reconcile, and through the post-chaos
> settle, so reads and writes for those keys stay permanently unavailable.

Where it comes from: `Cluster.initGenState()` unconditionally initializes a fresh
node at `gen: 0`, and there is no path for a joiner to learn the live generation
from its peers (via gossip metadata, a join-time sync, or a catch-up reshard). The
single-node and fixed-membership reshard paths are fine (the gates prove it); the
gap is specifically **join-after-reshard**.

How to reproduce: a seed whose event sequence fires a `reshard` and then a
node `join` / kill+`restart`. For example:

```sh
SHALE_CHAOS_SEED=7 SHALE_CHAOS_DURATION=8s SHALE_CHAOS_CHAOS_EVERY=400ms \
  go test -tags chaos ./tests/chaos/ -run TestChaosSoak -v -timeout 8m
```

surfaces ~1000+ `LOST` violations, all of the loop-refused shape above, plus a
very high `retryable_retries` count (the workload hammering the permanent error)
and a `WaitSettled` that times out (the cluster never re-reaches a clean
all-units-on-their-ring-owner state). A seed that happens NOT to combine a reshard
with a post-reshard join (e.g. the default `SHALE_CHAOS_SEED=1` short run) passes
clean with zero loss across hundreds of thousands of acked writes - which is what
makes the gap a genuine *combination* finding, invisible to any single gate.

This is exactly the "prove it before a real N=2 deploy" signal the harness exists
to produce: **do not turn on join-after-reshard in production until the joiner
learns the live generation.** The fix is a cluster change (propagate the live
generation to a joining node, e.g. via memberlist meta or a join-time reshard
catch-up) and belongs in its own spec-first commit; this harness is the regression
gate that will confirm it.

## How to run

The harness is a long-running soak test, gated behind the `chaos` build tag so
the normal `go test ./...` stays fast and never picks it up.

```sh
# Run the default soak (8s of workload; finishes in well under a minute of wall
# clock once topology convergence + the final settle/sweep are counted):
go test -tags chaos ./tests/chaos/ -run TestChaosSoak -v -timeout 4m

# Longer, more adversarial run with an explicit seed (reproducible):
SHALE_CHAOS_SEED=1234567 SHALE_CHAOS_DURATION=60s \
  go test -tags chaos ./tests/chaos/ -run TestChaosSoak -v -timeout 10m

# Knobs (all optional; env-driven so no flag plumbing through go test):
#   SHALE_CHAOS_SEED       int64   RNG seed (default 1; ALWAYS logged so a
#                                  failing run is reproducible by re-exporting it)
#   SHALE_CHAOS_DURATION   dur     wall-clock workload budget (default 8s)
#   SHALE_CHAOS_WRITERS    int     concurrent writer goroutines (default 6)
#   SHALE_CHAOS_READERS    int     concurrent reader goroutines (default 4)
#   SHALE_CHAOS_NODES      int     initial node count (default 3)
#   SHALE_CHAOS_UNITS      int     initial unit count, power of two (default 8)
#   SHALE_CHAOS_CHAOS_EVERY dur    mean interval between chaos events (default 600ms)
```

Without the `-tags chaos` build tag, the test file is excluded from the build
entirely (`//go:build chaos`), so `go test ./...` is unaffected.

Wall-clock note: the elapsed run time is meaningfully longer than
`SHALE_CHAOS_DURATION`. The duration bounds the WORKLOAD window (how long writers
and readers hammer the cluster); on top of it sit the structural chaos events
(each kill+restart, join, leave, and reshard blocks the scheduler on real
topology convergence), then a final settle phase that drives the cluster to a
fully-converged, all-units-mounted state before the verification sweep. An 8s
default workload typically lands a handful of events and completes in tens of
seconds of wall clock; budget the `-timeout` accordingly. Pick the workload
duration for how much adversarial overlap you want, not for total runtime.

The RNG is seeded **only** from `SHALE_CHAOS_SEED` (default 1). The harness never
reads wall-clock time or `crypto/rand` for any decision that affects which chaos
event fires or which key a writer touches; every such choice is drawn from the
seeded `*math/rand.Rand`. A given seed + knob set therefore replays the same
sequence of chaos events (modulo genuine scheduler nondeterminism in goroutine
interleaving, which the oracle is robust to: it only ever asserts on acked
writes, never on timing).

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
| 5 | Transact RMW integrity: a per-writer counter incremented under `Transact` equals the number of acked increments (no lost update). | counter sweep |
| 6 | Reads stay available: a reader running through chaos, WITH the standard retry, never hits a permanent failure or a permanently wrong value. | continuous readers + reverify |
| 7 | Liveness: the cluster keeps acking new writes throughout (the workload does not wedge); at least one ack per writer. | final accounting |
| 8 | Clean abort on mid-reshard membership change: an aborted reshard leaves the cluster at gen g, unfrozen, dataset intact. | combination event + final sweep |
| 9 | Cluster re-settles after chaos: before the final sweep, every unit is mounted on the node the ring assigns it to, at one uniform generation. A cluster that cannot re-converge to a clean routing state is itself a finding. | `WaitSettled` |

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
- `retryable_retries`: how many times a retryable error was retried (a proxy for
  how much cutover/handoff/freeze turbulence the run actually produced - a run
  with zero is suspiciously quiet and is flagged).
- `final_node_count`, `final_unit_count`, `final_generation`.
- `violations`: the list of oracle violations (LOST / STALE / CORRUPT /
  RESURRECTED). **The pass condition is `len(violations) == 0.`**

A run that produces zero acked writes, or zero chaos events, or zero retryable
retries is reported as *vacuous* and fails: a soak that never stressed anything
proves nothing.

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
