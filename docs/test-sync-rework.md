# Test synchronization rework

Design doc (read-only investigation phase; no code changed yet). Goal: kill the
fixed unconditional settle-sleeps in the integration and pkg/cluster test suites
by closing the one structural hole that forced them, namely that a debounce-
scheduled rebalance evaluation is invisible to `WaitForRebalanceIdle`.

## 1. Root cause: the pending-but-unfired debounce window

The rebalance pipeline on each node is:

```
membership event (NotifyJoin/Leave)
  -> Cluster.bumpRingGen()            (cluster.go via rebalance.go)
       -> Cluster.scheduleEvaluate()  (legacy mode)  -- arms c.settleTimer = AfterFunc(settleDelay, runEvaluate)
       -> Cluster.scheduleReconcile() (multi mode)   -- arms c.settleTimer = AfterFunc(settleDelay, runReconcile)
  ...settleDelay elapses...
  -> Cluster.runEvaluate()            (rebalance.go)
       -> Coordinator.Evaluate(old, current, gen)    -- registers ranges, launches runSend/runReceive goroutines
            -> ranges flip Sending/Receiving -> HandedOff -> Done (sweep)
```

`WaitForRebalanceIdle` is a thin pass-through:

```
Cluster.WaitForRebalanceIdle(ctx)   (rebalance.go:310)
  -> Coordinator.WaitForIdle(ctx)   (state.go:680)
       -> polls Coordinator.idle()  (state.go:695): true iff every entry in c.ranges is terminal (StateDone)
```

The defect: the debounce timer (`c.settleTimer`) lives on the **Cluster**, not the
Coordinator. Between the membership event and the moment `runEvaluate` actually
calls `Coordinator.Evaluate`, the Coordinator's `c.ranges` map is still empty (or
holds only previously-terminal entries). So:

- Test joins node, ring converges (sub-second under SWIM on loopback).
- Test calls `WaitForRebalanceIdle` BEFORE `settleDelay` (500ms in the fixture) elapses.
- Coordinator has no non-terminal ranges -> `idle()` returns true -> the wait returns immediately.
- The settle timer fires mid-test, `Evaluate` registers Sending/Receiving ranges, and
  those race the test's assertions (the classic "key is migrating out" ResourceExhausted flake).

The 700ms sleep in `waitForClusterReady` (helpers_test.go:511) exists only to step
PAST the 500ms `RebalanceSettleDelay` so that by the time `WaitForRebalanceIdle`
is called, `Evaluate` has already run and the in-flight ranges exist for `idle()`
to observe. It is a fixed-duration patch over a missing happens-before edge.

The same hole produces the post-idle settle sleeps (cluster_test.go:804/917,
founder_grows_shardkey_test.go:170): the periodic `reconcileTick` (sweep loop) or a
late settle-timer re-arm can register a NEW range AFTER `WaitForIdle` already
returned, so the test sleeps to "cover" any late re-arm. The pending counter below
closes the re-arm half of this too, because the re-arm increments pending before
`WaitForIdle` can observe an empty range table as idle.

## 2. The fix: count a pending (debounce-scheduled) evaluation as not-idle

Make `WaitForRebalanceIdle` block while EITHER an evaluation is scheduled-but-not-
yet-run OR an evaluation is in flight (the existing non-terminal-range check). The
debounce is a Cluster concern (the timer is Cluster state and is shared by both the
legacy `runEvaluate` and the multi-mode `runReconcile` paths), so the pending
counter lives on the **Cluster**, and `WaitForRebalanceIdle` becomes pending-aware
at the Cluster layer rather than delegating the whole decision to the Coordinator.

### Mechanism: an armed/pending counter on Cluster

Add to `Cluster` (cluster.go, in the settle block near `settleTimer`):

```go
settlePending atomic.Int64   // # of debounce-scheduled evaluations not yet completed
```

Lifecycle (all increments/decrements pair exactly once per armed timer):

- `scheduleEvaluate` / `scheduleReconcile` (rebalance.go:215, multibackend_rebalance.go:53):
  when arming a FRESH timer, increment `settlePending`. When RE-arming (Stop()ing an
  existing live timer that has not fired), do NOT double-count: the re-arm replaces a
  pending evaluation with another pending evaluation, so the counter stays put. The
  clean way to express this: increment only when there was no live timer
  (`c.settleTimer == nil`), and on re-arm leave the count alone. Stopping a timer
  that already fired (Stop returns false) is the runEvaluate path's own concern, see
  below.
- `runEvaluate` (rebalance.go:234) and `runReconcile` (multibackend_rebalance.go:79):
  these run when the timer fires. They must transfer the "pending" obligation into an
  "in-flight" obligation atomically with respect to `WaitForRebalanceIdle`. Simplest
  correct shape: keep `settlePending` incremented across the whole `runEvaluate` body
  (the AfterFunc callback), and decrement it in a `defer` at the END of `runEvaluate`,
  AFTER `Coordinator.Evaluate` has returned. Because `Coordinator.Evaluate` is
  synchronous in registering ranges (it returns only once every move is in
  `c.ranges` in a non-terminal state, per its doc contract at state.go:297-305), by
  the time `runEvaluate` decrements `settlePending` the Coordinator's range table is
  already populated. So there is no gap: pending>0 covers "timer armed, not yet run",
  and the Coordinator's non-terminal ranges cover "running -> draining". The handoff
  is seamless.
- `runEvaluateNow` (rebalance.go:277, the --apply path) and the teardown timer-stop
  in `Cluster.Close` (cluster.go:921) must keep the counter balanced: `runEvaluateNow`
  stops the pending timer then calls `runEvaluate` directly. It should claim the
  pending obligation (so a concurrent WaitForRebalanceIdle still blocks through the
  immediate evaluate) and the `runEvaluate` defer releases it. Close's timer-stop
  must zero out a pending count for a timer that will now never fire.

### WaitForRebalanceIdle becomes pending-aware

`Cluster.WaitForRebalanceIdle` (rebalance.go:310) changes from a pure pass-through to:

```go
func (c *Cluster) WaitForRebalanceIdle(ctx context.Context) error {
    rb := c.rebalance.Load()
    if rb == nil {
        return nil // single-node mode, but still honor multi-mode pending below
    }
    ticker := time.NewTicker(/* small, e.g. 10ms */)
    defer ticker.Stop()
    for {
        if c.settlePending.Load() == 0 && c.coordinatorIdle() {
            return nil
        }
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-ticker.C:
        }
    }
}
```

where `coordinatorIdle()` wraps the existing `Coordinator.WaitForIdle`-style check
(reuse `rb.WaitForIdle` with an already-done fast path, or expose a non-blocking
`Coordinator.Idle() bool` and call it here). The cleanest split:

- Add `Coordinator.Idle() bool` to pkg/rebalance/state.go (exported wrapper over the
  existing private `idle()`), so the Cluster can poll Coordinator quiescence WITHOUT
  the Coordinator needing to know about the Cluster's debounce.
- The Cluster's loop ANDs `settlePending == 0` with `rb.Idle()`. This keeps the
  bounded-context boundary clean: the Coordinator owns range quiescence, the Cluster
  owns debounce quiescence, and `WaitForRebalanceIdle` is the Cluster-level
  conjunction of the two.

Multi mode: there is no Coordinator (`c.rebalance` is nil) but `scheduleReconcile`
still increments `settlePending` and `runReconcile` still decrements it, so
`WaitForRebalanceIdle` blocks correctly through a pending unit-reconcile even though
`rb == nil`. The early `return nil` for `rb == nil` must therefore be removed /
guarded so the pending check still runs in multi mode.

### Precise list of types/functions to change

- `pkg/cluster/cluster.go`: add field `settlePending atomic.Int64` to the `Cluster`
  struct (settle block); decrement in `Close`'s timer-stop path.
- `pkg/cluster/rebalance.go`: `scheduleEvaluate` (increment on fresh arm),
  `runEvaluate` (defer decrement after `Coordinator.Evaluate` returns),
  `runEvaluateNow` (claim/balance the obligation), `WaitForRebalanceIdle` (pending-
  aware loop).
- `pkg/cluster/multibackend_rebalance.go`: `scheduleReconcile` (increment on fresh
  arm), `runReconcile` (defer decrement).
- `pkg/rebalance/state.go`: add exported `func (c *Coordinator) Idle() bool` wrapping
  the existing private `idle()` (state.go:695). No change to `WaitForIdle` itself; it
  remains usable directly by callers that only care about Coordinator-range
  quiescence (none of the test sleeps need that distinction once the Cluster wrapper
  is fixed).

### Concurrency notes

- `settlePending` is `atomic.Int64` so increments/decrements from the AfterFunc
  goroutine, the membership-events goroutine (scheduleEvaluate), and the
  WaitForRebalanceIdle poller need no extra lock. The timer-swap itself is already
  serialized under `settleMu`; do the "increment only if no live timer" decision
  inside that same `settleMu` critical section so the "was there a live timer"
  read and the increment are atomic together. The decrement at end of runEvaluate is
  a plain atomic add (no settleMu needed).
- A re-arm that Stop()s a timer whose AfterFunc has ALREADY started running but not
  yet reached its decrement: this is the one race to get right. Guard it by having
  `runEvaluate` capture-and-clear `c.settleTimer = nil` under `settleMu` at its top
  (it already takes settleMu to read `lastEvalRing`), so a concurrent
  `scheduleEvaluate` sees `settleTimer == nil` and treats itself as a FRESH arm
  (its own increment), while the in-flight `runEvaluate` still owns its earlier
  increment and decrements it on exit. Net: two pending obligations briefly
  coexist (the running one and the freshly-armed one), which is correct: there
  genuinely are two evaluations the waiter must see through.

## 3. Per-sleep disposition table

Scope per the task: the fixed unconditional SETTLE-sleeps. The ~83 `time.Sleep`
sites are dominated by poll-backoff sleeps inside poll-until-ready loops (e.g.
`waitForMembersAll` 50ms, `waitForWriteReady` 50ms, the `lease_handoff` 20ms poll
bodies, the many `for { ... time.Sleep(small); }` retry loops). Those are FINE and
must be KEPT: they are the backoff inside a real condition wait, not an
unconditional settle. The table below lists every site that is (or might be) a fixed
unconditional settle and its disposition.

| site (file:line, duration) | what it waits for | disposition |
| --- | --- | --- |
| tests/integration/helpers_test.go:511 (700ms) | step past 500ms RebalanceSettleDelay so WaitForRebalanceIdle sees in-flight ranges | REMOVE. The fixed WaitForRebalanceIdle now blocks on settlePending>0, so the immediately-following WaitForRebalanceIdle loop already waits the debounce out deterministically. |
| tests/integration/helpers_test.go:528 (50ms) | post-idle drain (grace sweep ticks, ring-rebuild fanouts, client cache invalidation) | REMOVE if the per-test assertions are already gated on the fixed WaitForRebalanceIdle; otherwise REPLACE-WITH a second WaitForRebalanceIdle call (cheap, returns immediately when truly idle). Keep only if a specific downstream assertion needs cache-invalidation drained and no condition exposes it. |
| tests/integration/helpers_test.go:270 (50ms) | poll backoff inside waitForMembersAll | KEEP (poll-backoff). |
| tests/integration/helpers_test.go:447 (50ms) | poll backoff inside waitForWriteReady | KEEP (poll-backoff). |
| pkg/cluster/cluster_test.go:804 (500ms) | one extra sweep cycle so HandedOff source ranges flip Done before placement assert | REMOVE. HandedOff is non-terminal (state.go IsTerminal), so the fixed WaitForRebalanceIdle (already called at :793) blocks through the sweep flip to Done. The separate late-reconcileTick re-arm is now covered by settlePending. Belt-and-suspenders: re-call WaitForRebalanceIdle after the sleep site, or poll physical placement directly. |
| pkg/cluster/cluster_test.go:917 (1500ms) | a late reconcile tick + source sweep dropping stale copies after WaitForRebalanceIdle returned | REPLACE-WITH a poll-until-placement-correct loop (poll the ring-owner backends until every key resides on exactly its owner, bounded). The pending counter closes the late-re-arm race; the residual is sweep-timing, best expressed as a condition poll on physical placement rather than a fixed sleep. |
| pkg/cluster/founder_grows_shardkey_test.go:170 (1500ms) | identical to 917: late reconcile tick + sweep dropping stale copies | REPLACE-WITH the same poll-until-placement-correct loop (poll until each subject's keys are co-located on its ring owner and absent elsewhere, bounded). |
| pkg/cluster/cluster_test.go:700 (100ms) | (verify in impl) settle after an op before an assertion | INVESTIGATE-IN-IMPL: if unconditional settle, REPLACE-WITH WaitForRebalanceIdle or a condition poll; if it is a poll-loop body, KEEP. Not in the core settle set named by the task. |
| pkg/cluster/cluster_test.go:292/324 (50ms) | poll backoff inside ring-size / membership wait loops | KEEP (poll-backoff). |
| pkg/cluster/cluster_test.go:998 (50ms) | poll backoff inside a wait loop | KEEP (poll-backoff). |

All remaining sites enumerated in section 4 are poll-backoff bodies or
deliberate timing constructs (e.g. "age past the grace" in the reshard barrier
tests, concurrency-window sleeps in race repros) that are NOT settle-sleeps and are
out of scope: KEEP.

### Sites confirmed KEEP (poll-backoff or deliberate timing), not settle-sleeps

These are listed so the implementer does not "clean them up" by mistake:

- Poll-backoff bodies inside condition loops: helpers_test.go:270/447;
  cluster_test.go:292/324/998; lease_handoff_test.go:141/338/355;
  rebalance_helpers_test.go:130/178; reshard_test.go:268;
  rf2_shrink_retention_test.go:263/291; replicate_consistency_knobs_test.go:239;
  cas_replicate_*_test.go poll bodies; replicate_read_test.go poll bodies;
  replicate_test.go:233; fanout_internal_test.go:123; cluster_internal_test.go poll
  bodies.
- Deliberate timing (grace-aging, race-window, concurrency interleave): the reshard
  barrier "age past the grace" sleeps (multibackend_reshard_barrier_internal_test.go
  :228/255), the 2ms/3ms interleave sleeps in rebalance_write_rejection /
  rebalance_read_forward / lossless_multinode_reshard concurrent-probe loops, and the
  900ms reshard-generation settle waits in the reshard-gen tests (those wait on
  object-store-scale reshard barriers, a different subsystem than the rebalance
  Coordinator debounce; out of scope here, revisit separately if they flake).

The reshard-family fixed sleeps (join_after_reshard_gen, lossless_*_reshard_gate,
multinode_reshard_review_gaps, reshard_test.go:101/123/189/376/430,
rebalance_failure_test.go:64/99/278) are NOT rebalance-Coordinator debounce sleeps;
they wait on reshard barriers / handoff timeouts / grace windows. They are out of
scope for THIS rework (which targets the WaitForRebalanceIdle debounce hole) and
should be addressed in a follow-up once the Coordinator-debounce fix lands and is
proven.

## 4. Does docs/SPEC.md need an edit?

Yes, a small one. SPEC.md "Trigger" (line 588-594) documents the settle timer
(`T_settle`, re-arm on each event) but says nothing about `WaitForRebalanceIdle`'s
observable contract. This rework CHANGES that contract: "idle" now means "no
evaluation is scheduled-but-unrun AND no range is in flight", i.e. a node with a
pending debounce timer is explicitly NOT idle. Per the spec-first rule, add one
paragraph (near the settle-timer description) stating:

> A node is considered *rebalance-idle* only when no settle-timer evaluation is
> pending (scheduled but not yet fired) and every tracked migration has reached a
> terminal state. `WaitForRebalanceIdle` blocks until both hold, so a caller that
> observes idle immediately after a membership change is guaranteed the debounced
> evaluation has already run and drained, not merely that nothing has been scheduled
> yet.

This is a behavior change to a public Cluster method, so it belongs in the same
commit as the code per the spec-first discipline. No other SPEC section changes (the
migration mechanics, reconcile, and convergence invariant are unchanged; only the
observability contract of "idle" is tightened).
