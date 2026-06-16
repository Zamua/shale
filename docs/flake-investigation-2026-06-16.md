# Race-flake investigation: pkg/cluster (2026-06-16)

Two intermittent `-race` flakes characterized. Both live in
`pkg/cluster/cluster_internal_test.go`. Neither is a detector-flagged data
race (0 "DATA RACE" / "WARNING: DATA RACE" in any captured failure); both are
logic/timing races between the test body and the Cluster's background loops,
exposed by `-race`'s altered goroutine scheduling. Both pass reliably without
`-race`. No fix applied (characterize-only task).

Run shape (one `-race` process at a time, anchored, RAM-checked before each):

```
GOWORK=off go test ./pkg/cluster -run '^<TestName>$' -race -count=10 -v -p 1 -parallel 1 -timeout 300s
```

## Flake 1: TestReconcileRingFromMembership_RestoresMissingLocal

- Observed failure rate: **1/20** (0/10 first invocation, 1/10 second).
- Failing assertion: `cluster_internal_test.go:100`
  `post-Remove ring should be empty, got [solo]`
- Elapsed on the failing iteration: 0.00s (early assertion, not a timeout).

Captured failure output:

```
=== RUN   TestReconcileRingFromMembership_RestoresMissingLocal
    cluster_internal_test.go:100: post-Remove ring should be empty, got [solo]
--- FAIL: TestReconcileRingFromMembership_RestoresMissingLocal (0.00s)
```

Mechanism: the test waits for the local node to land in the ring
(`waitForLocalInRing`, line 91), then does `c.ring.Remove("solo")` (line 98)
and immediately asserts the ring is empty (line 99-100). But the background
`runEventsLoop` / `runReconcileLoop` are live and can re-insert "solo"
concurrently:
- `runEventsLoop` (cluster.go:737) calls `c.ring.Add(...)` on the local node's
  own `EventJoin`. If that join event is still in flight (or redelivered)
  between the test's `Remove` and its `Members()` read, "solo" reappears.
- `runReconcileLoop` (cluster.go:771) periodically calls
  `reconcileRingFromMembership`, which re-Adds every member membership still
  knows about (cluster.go:816) - including the just-removed local node.

The test's intent is to drive divergence manually then call
`reconcileRingFromMembership()` itself (line 103); the background reconcile /
event Add racing the manual `Remove` defeats the "post-Remove ring is empty"
precondition. Note `gossip_fast_test.go` shortens `SetSweepInterval` /
`defaultSettleDelay` AND the reconcile cadence is on the fast path, so the
window for a background re-Add is small but nonzero.

## Flake 2: TestWaitForRebalanceIdle_BlocksWhileDebouncePending

- Observed failure rate: **1/10** (caught on the first invocation).
- Failing assertion: `cluster_internal_test.go:486`
  `expected settlePending==0 at rest, got 1`
- Elapsed on the failing iteration: 0.01s (early sanity assertion; passing
  iterations take ~0.31s because they exercise the full 300ms block window).

Captured failure output:

```
=== RUN   TestWaitForRebalanceIdle_BlocksWhileDebouncePending
    cluster_internal_test.go:486: expected settlePending==0 at rest, got 1
--- FAIL: TestWaitForRebalanceIdle_BlocksWhileDebouncePending (0.01s)
```

Mechanism: after `waitForLocalInRing` (line 479) the test asserts the node is
quiescent: `c.settlePending.Load() == 0` (line 485). But the local node's own
join still drives the events loop, and `runEventsLoop` -> `c.bumpRingGen()`
(cluster.go:741) -> `scheduleEvaluate()` (rebalance.go:210/215) does a fresh
arm that `settlePending.Add(1)` (rebalance.go:233). If that background
`bumpRingGen` from the initial join lands between `waitForLocalInRing`
returning and the line-485 read, `settlePending` is already 1, and the "at
rest" precondition fails. The `RebalanceSettleDelay: time.Hour` only guarantees
the timer will not FIRE on its own; it does not prevent the timer being ARMED
(and the pending counter incremented) by a background ring-change event. So the
`time.Hour` immunity claim covers the manual-drive portion of the test but NOT
this initial-quiescence assertion.

## Belief check: pre-existing vs caused by the 2026-06-16 speed work

Refined the handoff belief. Both flakes are pre-existing in NATURE (the
test-vs-background-loop race is structural, not introduced by shortening
intervals), but the speed work plausibly changed their RATE:
- The shortened `SetSweepInterval(200ms)` / `defaultSettleDelay` / reconcile
  cadence make background re-Add / bumpRingGen ticks fire sooner relative to
  the test body, which can WIDEN the window in which a background tick collides
  with the early assertion. So "immune because settle is time.Hour" is correct
  for the timer FIRING but does not make the test immune to a background
  bumpRingGen ARMING the pending counter, nor to a background reconcile/event
  re-Add. The flakes would exist on the pre-speed-work cadence too, likely at a
  lower rate.

## Root cause (shared)

Both tests assert a "clean/at-rest" precondition (empty ring / zero pending)
immediately after `waitForLocalInRing`, while the Cluster's `runEventsLoop`
and `runReconcileLoop` are running and can mutate the ring / arm the settle
timer in response to the local node's own membership. The assertions race
those background loops. `-race` perturbs scheduling enough to surface the
collision a few percent of the time.

Fix direction (NOT applied here): make the tests deterministic w.r.t. the
background loops - e.g. quiesce / pause the reconcile + events loops (or set
`reconcileInterval = time.Hour` as `TestEventsLoop_EvictsClientOnAddrChange`
already does at line 131-133) around the manual setup, and drain `settlePending`
to a known state before the at-rest assertions, rather than assuming the loops
are idle. This is a test-correctness fix, not a production-code bug: the
production loops re-adding the local member / arming a debounce is exactly the
auto-heal behavior the system wants.
