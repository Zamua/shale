# Overlap handoff (Option B) for R>1 multi-backend membership change

Status: DESIGN (amended). This is the design companion to the SPEC section
"v0.8 Phase 2e: overlap handoff (Option B)". The SPEC is the source of
truth for behavior; this doc carries the state diagram, the handoff
sequence, and the planned package/file layout for the future
implementation. No code has landed yet.

This revision closes the holes found by the adversarial review in
`docs/design/overlap-handoff-review.md` (kept as history). The operator
chose the architectural forks; the resolutions are baked in below. The
one-line summary of each is in the SPEC's "Resolved review findings"
note.

## The one-sentence design

Replace the clean-cut RELEASE-then-ACQUIRE of a moving `ReplicaUnit` with
an ACQUIRE-then-RELEASE driven by a single per-`ReplicaUnit`
ownership-transition state machine, where: (1) the NEW owner, while it is
still mounting, FORWARDS routed ops to the predecessor (the OLD owner) so
the ring rotation does not strand the position; and (2) the OLD owner
releases exactly when the NEW owner becomes READY (proven by the
`ReplicaHandoffReady` RPC, re-verified before any release the durable
epoch alone triggers). Availability stops depending on mount time because
the old owner keeps serving (via the new owner's forward) until the new
owner is provably serving locally.

## Two flips, named once

The review's central insight: there are TWO distinct events the original
design conflated under the word "flip". This doc uses these names
consistently.

- **Ring rotation**: membership converges and the ring's replica set for a
  position changes (the OLD owner drops out of the set, the NEW owner
  enters). This happens at convergence, BEFORE any mount completes. The
  write/read fan-out (`replicasForKey -> unitReplicas -> LocateKeyN`) is
  re-derived per op from the LIVE ring, so the ring rotation is what
  actually steers which nodes are contacted.
- **Mount flip**: the NEW owner finishes `OpenReplicaUnit` and inserts its
  `mountMap` entry, so a routed op now resolves locally on the new owner
  instead of being forwarded to the predecessor. This is the correctness
  cut (it happens strictly after the durable fence).

Option B's correctness argument is about the mount flip. Availability is
governed by the ring rotation: once the ring rotates, the fan-out targets
the NEW owner, which is still `Acquiring` and has no local mount. The
design must therefore make the new owner usable DURING `Acquiring`, which
is what the predecessor-forwarding (below) does.

## Why a state machine, not flags

The current code expresses a handoff as two independent halves on two
nodes: the losing node drops the unit from its desired set and RELEASEs;
the gaining node adds it and ACQUIREs. Nothing sequences them, so there is
a window where neither serves. Option B adds sequencing, and the clean way
to express "this position is mid-transition, and these are the only legal
next steps" is one explicit phase per side, keyed by `ReplicaUnit`, not a
scatter of booleans (isDraining, hasFlipped, pendingRelease, ...).

Note the complete state is NOT "phase value + mountMap presence" alone any
more (the original design's claim, now falsified): the NEW owner's
`Acquiring` phase ALSO carries a forwarding behavior (it proxies to the
predecessor), and the maps move to `ReplicaUnit` keying. The phase plus
the mount entry plus the recorded predecessor address is the state; every
transition is still a single guarded edge.

## Roles and the shared truth

- A `ReplicaUnit` = `(GenUnit, replica-position)` is the unit of ownership.
  It is node-independent: the same durable prefix `dbNameReplica(ru)` is
  what the OLD owner has open and what the NEW owner opens. That
  node-independence is what makes overlap possible. The durable identity is
  ALREADY per-`ReplicaUnit` (`dbNameReplica(ru)`); the in-memory maps must
  match it (see "Keying by ReplicaUnit" below).
- The ring is the shared truth. Both nodes compute the same
  `desiredReplicaUnits()` from the same converged ring and independently
  conclude "position rP of unit K moves from node OLD to node NEW." The
  membership delta (the ring shape just before convergence vs after) is
  also where the NEW owner learns the predecessor's address.
- Two cross-node signals, with DISTINCT roles (the review's P1-1 fix):
  - The `ReplicaHandoffReady(ru, E)` RPC is the AUTHORITATIVE readiness
    signal. It is sent only AFTER the new owner's mount flip (so it proves
    a live owner is serving the position). The old owner releases on it.
  - The DURABLE slatedb manifest writer-epoch (`DurableEpochReplica(ru)`)
    is a LIVENESS HINT only. Seeing it cross proves SOMEONE fenced, NOT
    that a live writer is serving (a new owner that fences and then crashes
    mid-mount advances the epoch without ever serving). On seeing the epoch
    cross, the old owner RE-VERIFIES a live owner is serving (a readiness
    probe to the new replica) before releasing; it NEVER releases on a bare
    epoch advance.

## Keying by ReplicaUnit (the model fix, review P0-2 / P2-2)

`mountMap` (`map[GenUnit]Backend`) and `replicaPos` (`map[GenUnit]uint8`)
today hold ONE backend + ONE position per unit per node. Overlap requires
a single node to hold the OLD position (draining) AND the NEW position
(acquiring/ready) of the SAME unit at once: this happens whenever a ring
change shuffles this node's index within a unit's replica list, which the
reconcile diff already produces (`reconcileReplicaUnits`: "RELEASE units
... replicated at a DIFFERENT position ... re-acquired below at the new
position"). The per-unit maps cannot represent both positions.

Resolution: re-key by `ReplicaUnit`.

- `mountMap`  becomes `map[ReplicaUnit]backend.Backend`.
- `replicaPos` is SUBSUMED by the key (the position is `ru.Replica`); it is
  removed as a separate map. (Where a `GenUnit -> position` view is still
  convenient, derive it from the `ReplicaUnit` keys.)
- the new `handoffPhase` map is `map[ReplicaUnit]HandoffState`.
- the pure `HandoffState` value object keys by `ReplicaUnit` too (this also
  fixes P2-2: a node CAN be `Draining` position p and `Acquiring` position
  q of one unit simultaneously, so the pure type must distinguish them).

Ripple (every `mountMap`/`replicaPos` reader must move to ReplicaUnit):

- `localBackendForKey(key)`: today resolves `mountMap[gu]` with NO position
  check. It must resolve the position THIS node holds for the key's unit.
  Because a node can hold at most one LOCAL replica position per unit at
  steady state (a node appears at most once in a unit's replica set), the
  resolver derives the position from the live ring (this node's index in
  `unitReplicas(gu)`) and looks up `mountMap[ReplicaUnit{gu, pos}]`. During
  a same-unit position shuffle, BOTH the draining old position and the
  acquiring new position can be present; the resolver picks the one the
  ring currently assigns this node (the new position once the ring rotates,
  the old position before). The draining old-position entry is reachable
  only via the predecessor-forward path (below), never via this node's own
  ring index after rotation.
- `localWriteBackendForKey(key)`: same change; the reshard write-pause keying
  is unaffected (it keys by old-count unit id, orthogonal to replica position).
- `evictStaleMount(gu, failed)`: becomes `evictStaleMount(ru, failed)` and
  compare-deletes `mountMap[ru]` (the CAS-by-pointer guard is unchanged).
- `mountedBackends()`: iterates `ReplicaUnit` keys; the admin scan still
  reports the union of every mounted position (ordering extends from
  `(gen, unit)` to `(gen, unit, position)`).
- `reconcileReplicaUnits` RELEASE/ACQUIRE diff: keyed by `ReplicaUnit`, so a
  position shuffle becomes "acquire new position (overlap), then drain old
  position", with both entries coexisting until the drain releases.

This is the honest model and a real (mechanical) refactor; it is called out
so the implementer does NOT bolt on a parallel "draining map" (the exact
scattered-flags spaghetti the design forbids).

## Predecessor forwarding (the availability fix, review P0-1)

The OLD owner keeps the position mounted during the overlap, but once the
ring rotates the fan-out no longer targets the OLD owner: it targets the
NEW owner, which is `Acquiring` and has no local mount. The original design
assumed routing follows the mount map; it does not (routing is
ring-derived). So the still-mounted OLD owner is unreachable, and
availability bottoms out on Option A's retry. This is the central hole.

Resolution (operator decision: NEW-OWNER FORWARDING, the review's option
(b), NOT the pending-ranges union (a)): while a `ReplicaUnit` is in
`PhaseAcquiring` on the NEW owner, a routed op for that position is NOT
answered with `errUnitAcquiring`. Instead the new owner FORWARDS the op to
the predecessor (the OLD owner) over RPC and returns the predecessor's
result, until its own mount completes (`PhaseReady`), after which it serves
locally and stops forwarding.

Why this fork: minimal blast radius. The hot quorum / fan-out path and the
W math are UNTOUCHED (the fan-out still targets the new replica set; W is
still `requiredWriteAcks` against R). The whole overlap is confined to the
NEW owner's `Acquiring` state, where the state machine does the work. No
expanded-set resolver, no pre-change/post-change union to thread through
`replicasForKey`, no per-op W recomputation. This matches the "no spaghetti
/ simpler shape wins" goal.

Mechanics:

- **Learning the predecessor address.** The new owner records the
  predecessor when it enters `Acquiring`. The predecessor is the node that
  held this exact `ReplicaUnit` (same position rP of unit K) in the
  PRE-change replica set, derived from the membership/ring delta (the
  replica set computed against the ring shape just before convergence). The
  cluster already observes topology change events; the pre-change replica
  set for a position is `unitReplicas(gu)` evaluated against the prior ring
  generation/membership snapshot. The recorded predecessor address travels
  in the `HandoffState` (an `Acquiring` entry carries `Predecessor NodeAddr`).
- **Transparent for reads AND writes.** Forwarding reuses the existing
  `clientFor` + forwarded-op plumbing (`PutForwarded` for Put/Delete-as-
  tombstone, `forwardGet` for Get, the CAS-apply forward for commits) -
  the SAME forward path the cluster already uses for a non-local replica
  leg, just AIMED at the predecessor's address instead of the ring-resolved
  owner. The predecessor's RPC handler lands in its normal local-apply /
  local-read path (it is still mounted and serving the position), so the
  op is applied/served against the authoritative durable database exactly
  as a direct op would be. The forwarded write is still `AwaitDurable`,
  so a forwarded ack means the write is durable at `dbNameReplica(ru)` and
  the new owner will read it on open.
- **Predecessor unreachable.** If the forward RPC fails (predecessor down,
  dial error, timeout), the new owner FALLS THROUGH to the existing
  Option-A belt: it returns `errUnitAcquiring` for the position, which the
  fan-out tolerates as a transient and the `WriteTimeout`-bounded retry
  loops on. So a predecessor crash mid-overlap degrades exactly to the
  Option-A window for that position (see crash case 2), never worse.
- **Stop condition.** Forwarding stops the instant the new owner reaches
  `PhaseReady` (its own mount entry is inserted). The same event that
  stops forwarding is the event that makes the new owner serve locally and
  the event the old owner releases on (see the dovetail below). One event,
  three effects.

The fan-out and W math are NOT in the list of "what does not change" any
more: the new owner gains an `Acquiring`-state forwarding behavior. That is
the only routing-surface change, and it is local to one node's one state.

## State diagram

Two halves of ONE transition, each a small phase machine, keyed by
`ReplicaUnit`. The phase map holds only IN-FLIGHT positions; absence means
steady-state (Owned if mounted, Absent if not).

NEW owner (the node GAINING the position):

```
  Absent
    | ring rotation assigns rP here, not yet mounted
    | record predecessor addr (from the pre-change replica set)
    v
  Acquiring  ---- OpenReplicaUnit running (slow MinIO mount + fence) ----.
    |  routing now targets THIS node (ring rotated), but no local mount.  |
    |  a routed op is FORWARDED to the predecessor (OLD owner), which is  |
    |  still serving. THIS IS THE OVERLAP WINDOW. read AND write          |
    |  transparent. If the predecessor is unreachable, fall through to    |
    |  errUnitAcquiring (the Option-A belt).                              |
    |  OpenReplicaUnit returns (mounted + durably fenced at E)            |
    v                                                                     |
  Ready  --- mountMap[ru]=b inserted under mountMu ---------------------- '
    |    THE MOUNT FLIP. The instant this entry is visible, this node
    |    serves routed ops LOCALLY and STOPS forwarding.
    |    send ReplicaHandoffReady(ru, E) to the predecessor (authoritative
    |    release signal; sent only here, after the flip).
    v
  Owned   (steady state; phase entry dropped)
```

OLD owner (the node LOSING the position):

```
  Owned   (mounted + serving)
    | reconcile computes rP no longer desired here, but DO NOT release yet
    v
  Draining  --- keep serving (directly AND for the new owner's forwards) --.
    |   release-check fires when the NEW owner is READY, proven by EITHER  |
    |     (authoritative) a ReplicaHandoffReady(ru, E) arrives, OR         |
    |     (hint -> verify) DurableEpochReplica(ru) > myOpenEpoch AND a     |
    |        readiness probe to the new replica confirms it is serving.    |
    |   A BARE epoch advance NEVER releases. Until release, OLD keeps      |
    |   answering routed ops (its own + forwarded).                        |
    v                                                                      |
  Releasing  --- compare-and-delete mountMap[ru], CloseReplicaUnit(ru) --- '
    |    (flush is a no-op for acked writes: durable-before-ack;
    |     CloseReplicaUnit still seals to be safe). EXACTLY ONCE.
    v
  Absent  (phase entry dropped)
```

The cut (the fence point) is a single instant on the durable manifest: the
epoch E at which the NEW owner opened. Below it, the OLD owner is the
authoritative writer; at or above it, the NEW owner is. The OLD owner's own
writes are fenced the moment it tries one past that epoch (slatedb
`CloseReasonFenced`), so even a buggy late write on the old side cannot
corrupt the position. The mount flip happens strictly AFTER the fence, so
there is never an instant where both nodes serve locally AND both are
unfenced. Forwarding does not violate this: a forwarded write lands on the
OLD owner BELOW E (it is the authoritative writer there until E), and the
new owner does not serve locally until after E.

## The dovetail: release-on-Ready == stop-forwarding

The old owner releases exactly when the new owner becomes `Ready`, which is
ALSO exactly when the new owner stops forwarding. One event
(`ReplicaHandoffReady` after the mount flip) drives both: the new owner
flips to local serving (stops forwarding to the predecessor) AND signals
the old owner to release. There is no window where the old owner has
released but the new owner is still forwarding to it (the new owner only
forwards while `Acquiring`, and it sends the release signal only after
leaving `Acquiring`). This is the clean single-instant handoff the
state machine buys.

## Sequence of one handoff (happy path)

```
ring change: position rP of unit K  OLD --> NEW

  t0  both nodes observe converged ring; both schedule reconcile.
  t1  NEW reconcile: rP desired, not mounted -> phase Acquiring, record
        predecessor = OLD (from the pre-change replica set).
        NEW: OpenReplicaUnit(ru, intended) starts the slow mount.
  t1  OLD reconcile: rP no longer desired -> phase Draining (NOT released).
        OLD: keeps serving rP; arms release-check (RPC-wake primary, poll +
        re-verify as the liveness fallback).
  t1..t2  OVERLAP: the ring has rotated, so writes route to NEW. NEW is
        Acquiring (no local mount) and FORWARDS each op to OLD, which is
        still mounted and applies it durably, acking back through NEW.
        Mount time here is irrelevant to availability.
  t2  NEW: OpenReplicaUnit returns at openedEpoch E (E > durable, fences
        any lower writer). NEW inserts mountMap[ru] -> phase Ready. NEW now
        serves rP LOCALLY and STOPS forwarding. THE MOUNT FLIP.
        NEW: ReplicaHandoffReady(ru, E) -> OLD (authoritative release).
  t3  OLD release-check: ReplicaHandoffReady arrived (or the epoch crossed
        AND a readiness probe confirmed NEW serving). OLD: compare-and-delete
        its mountMap[ru] entry, CloseReplicaUnit(ru) once -> Releasing ->
        Absent.
  done. No instant between t1 and t3 had the position unserved.
```

During the whole interval at least one of {OLD, NEW} serves the position
(OLD directly or via forward through t2, NEW locally after t2), and only
one is ever the authoritative (unfenced) writer.

## Crash handling (the four cases)

1. NEW crashes mid-acquire (between t1 and t2): the new owner stops
   forwarding (it is gone), but it never sent `ReplicaHandoffReady` and
   never advanced past fence-then-serve, so OLD's release-check never fires
   (a bare epoch advance from NEW's pre-crash fence does NOT release: the
   readiness re-verify probe to NEW fails). OLD stays in `Draining` and
   KEEPS SERVING. No data loss, no unavailability of the position from the
   OLD owner's side; the only effect is that ops in-flight TO the crashed
   new owner fail and retry. The next reconcile re-derives the ring; if NEW
   is gone the position reassigns (possibly back to OLD, which drops the
   `Draining` phase and returns to Owned, or to a third node that starts
   its own `Acquiring` with OLD as predecessor). This is the crash case the
   release-on-Ready fix protects: under the OLD design's bare-epoch
   release, NEW's pre-crash fence would have triggered OLD to release,
   stranding the position. It no longer does.

2. OLD crashes while Draining (before NEW is Ready): the predecessor the
   new owner forwards to is gone, so the new owner's forward RPC fails and
   it falls through to `errUnitAcquiring` for the position. This degrades
   to the Option-A window for THIS one position only, bounded by NEW's
   mount, and only if the membership change ALSO killed the old owner
   mid-flight. R/W availability during this window depends on the config:
   see the R/W matrix below (at R=2/W=2 the position is write-unavailable
   until NEW mounts; at quorum configs the other replicas cover W). Every
   acked write is already durable at `dbNameReplica(ru)` (durable-before-
   ack), so NEW sees them all once it mounts; no acked write is lost.

3. NEW crashes AFTER Ready but before OLD released (between t2 and t3): the
   `ReplicaHandoffReady` may or may not have reached OLD. If it did, OLD has
   already released; the position is owned-by-NEW per the ring but NEW is
   gone, so the next reconcile reassigns it (to OLD or a third node) and
   re-acquires from the durable state. If the RPC did not reach OLD, OLD is
   still mounted but fenced (its writes fail); the epoch-cross hint fires,
   the readiness re-verify probe to NEW now FAILS (NEW is gone), so OLD does
   NOT release on the bare epoch and stays `Draining`-serving until the next
   reconcile reassigns the position. Either way no acked write is lost (all
   durable), and any stale OLD mount is torn down by `evictStaleMount` on
   its next fenced write.

4. Both nodes survive but the `ReplicaHandoffReady` RPC is lost: the old
   owner's release-check falls back to the durable-epoch hint, sees the
   epoch cross, probes the new replica's readiness, gets a positive
   confirmation (NEW is serving), and releases. Slightly higher latency
   than the RPC fast-path; correctness unaffected.

## The slate manifest seal (consistent mount under overlap)

The NEW owner must mount a view that contains every write the OLD owner
acked (whether a direct write to OLD before the ring rotated, or a write
forwarded through NEW to OLD during the overlap; both land on OLD's
authoritative database below epoch E). Three facts make this hold without a
new flush protocol:

- **Durable-before-ack.** The slate replica opens with `AwaitDurable=true`,
  so every acked write is in the durable WAL/SST before its ack returns.
  This holds identically for a forwarded write: the predecessor applies it
  with `AwaitDurable=true` before the forwarded ack returns.
- **WAL recovery over the full durable tail (review P1-2, see the explicit
  assumption below).** The NEW owner's `OpenReplicaUnit` must recover the
  FULL durable WAL tail, not merely load the manifest snapshot it read at
  fence time. An OLD-owner write whose durable-before-ack completes at
  `T_write < T_fence` is durable in the WAL but may NOT yet be reflected in
  the manifest the new owner read at `T_fence` (slatedb's WAL is durable
  independently of manifest checkpoints; a freshly-durable WAL entry is
  recovered on open via WAL replay, not via the manifest snapshot). For the
  seal to hold, NEW's open must replay that WAL tail.
- **The fence is on the durable manifest writer-epoch.** `OpenReplicaUnit`
  reads the manifest, opens at `max(intended, durable+1)`, and that open
  itself writes the bumped epoch to the manifest. The OLD owner's
  subsequent `CloseReplicaUnit` seals (`Db.Shutdown` flushes) but it is
  belt-and-suspenders for acked writes: those are already durable.

### EXPLICIT ASSUMPTION (implementation-must-verify, release-gating)

The slatedb-go binding (`slatedb.io/slatedb-go v0.13.1`) is a uniffi FFI
over the Rust core; the Go source does not expose the open/recovery path,
so we CANNOT confirm from the module source that `Db` open replays the full
durable WAL tail. The binding does expose an independent WAL concept (a
`WalReader` over WAL files that exist separately from the manifest), which
is consistent with WAL-replay-on-open but is not proof of it.

Therefore this is an EXPLICIT ASSUMPTION the implementation MUST PIN with a
test before Phase 2e ships: a write acked on the OLD owner JUST BEFORE the
fence (durable in the WAL but possibly not yet manifest-checkpointed) must
be readable on the NEW owner AFTER the handoff. If slatedb open does NOT
replay the durable WAL tail, this is a P0 lost-write and Phase 2e is blocked
until the gap is closed (e.g. forcing an OLD-side `Db.Flush`/checkpoint into
the manifest before the new owner opens, or another seal protocol). The
acceptance gate's loss oracle (every acked key readable everywhere) covers
this in aggregate, but a targeted test that writes-then-immediately-fences
is the honest pin.

### Fence-detection chain (why no acked-past-fence write exists)

The design asserts "a write the old owner acks AFTER the new owner has
fenced is impossible." The chain, spelled out rather than asserted:

1. slatedb detects a fence on the OLD writer's next manifest/WAL-touching
   write (its epoch is now below the manifest's writer-epoch E). The fence
   is observed AT the durable write, not asynchronously.
2. `AwaitDurable=true` forces that durable write to complete BEFORE the ack
   returns to the client.
3. Therefore a write that WOULD be fenced (its durable write touches the
   post-E manifest and is rejected, `CloseReasonFenced`) CANNOT have acked:
   the rejection happens at the durable-write step, which is strictly before
   the ack. So either the write completed its durable write before E (acked,
   durable, visible to NEW) or it is fenced (never acked). There is no
   "acked on old, invisible to new" gap. A write started before `T_fence`
   that would durably-commit after `T_fence` is fenced at its durable-write
   step and never acks.

This chain holds identically for a forwarded write (the predecessor is the
one doing the durable write; the forward only changes who initiates the RPC,
not where the durable write + fence detection happen).

## Planned package / file layout

Domain (pure, no I/O) in `pkg/storageunit`:

- `handoff.go` (new): the pure transition types, keyed by `ReplicaUnit`.
  - `HandoffPhase` enum: `PhaseAcquiring`, `PhaseReady`, `PhaseDraining`,
    `PhaseReleasing` (the four in-flight phases; steady states Owned /
    Absent are represented by mountMap presence + phase absence, not enum
    values).
  - `HandoffState` value object: `{Phase HandoffPhase, OpenEpoch Epoch,
    Predecessor NodeAddr}` (the predecessor is meaningful only in
    `Acquiring`). The cluster keys the in-flight record by `ReplicaUnit`
    (the position is the key, not a field).
  - Pure transition functions / guards: `NextOnReady`, `NextOnDrain`,
    `NextOnRelease`, returning the next phase or an error for an illegal
    edge; the guards make explicit WHICH phases are legal on WHICH side (a
    node is `Acquiring/Ready` on the gainer side, `Draining/Releasing` on
    the loser side, and CAN be in both for DIFFERENT positions of one unit,
    which is why the key is `ReplicaUnit`). A `Releasable(state, ready bool)`
    predicate encodes the release rule: release requires a positive
    readiness, NEVER a bare epoch compare (the epoch is only a hint that
    triggers the re-verify, handled in the controller). These are
    table-driven and trivially unit-testable with no ring or factory.

- `factory.go` (edit): extend `ReplicaBackendFactory` with
  `DurableEpochReplica(ru ReplicaUnit) (Epoch, error)` so the cluster can
  read the cross-node durable LIVENESS HINT through the domain seam (the
  slate `Backing` already implements it; this lifts it onto the interface).
  The test `sharedfactory` gains the same method over its shared epoch
  registry, plus the SLOW-mount injection the acceptance gate needs.

Controller (the wiring) in `pkg/cluster`:

- `cluster.go` (edit): re-key `mountMap` to `map[ReplicaUnit]backend.Backend`,
  remove the separate `replicaPos` map (subsumed by the key), add
  `handoffPhase map[ReplicaUnit]HandoffState` guarded by `mountMu`. Update
  every reader (`localBackendForKey`, `localWriteBackendForKey`,
  `evictStaleMount`, `mountedBackends`, the reconcile diff) per "Keying by
  ReplicaUnit" above.

- `multibackend_overlap.go` (new): the Option B controller, replacing the
  position-change branch of `reconcileReplicaUnits`.
  - `reconcileReplicaUnitsOverlap()`: the new diff. For a position MOVING
    AWAY (this node is the old owner, the position is still in some other
    node's new replica set): set `Draining` instead of releasing. For a
    position MOVING IN (this node is the new owner): set `Acquiring` (record
    the predecessor from the pre-change replica set) + start
    `acquireReplicaUnitOverlap`. A pure NEW mount (initial convergence, no
    predecessor) stays the existing acquire path. A node simply DROPPING OUT
    of a unit's replica set entirely (no specific successor takes ITS exact
    position; some other already-mounted replica covers W) stays a PLAIN
    clean-cut release (`releaseReplicaUnit`), because nobody forwards to it
    and nobody routes to it after the ring rotation - see "Overlap-move vs
    plain-release" below.
  - `acquireReplicaUnitOverlap(ru)`: OpenReplicaUnit; on success insert the
    `mountMap[ru]` entry (THE MOUNT FLIP) under mountMu, set `Ready -> Owned`,
    STOP forwarding, fire `ReplicaHandoffReady(ru, E)` to the predecessor.
  - the `Acquiring`-state forward: when `localBackendForKey` / the routed-op
    path finds a `ReplicaUnit` in `PhaseAcquiring` on this node, it forwards
    the op to the recorded predecessor via `clientFor` + the existing
    forwarded-op RPCs, returning the predecessor's result; on forward
    failure it returns `errUnitAcquiring` (the Option-A belt).
  - `drainCheck(ru)`: the old-owner release-check. The
    `DurableEpochReplica(ru)` read AND the readiness probe (both MinIO /
    network I/O) run BEFORE entering the critical section. The phase
    compare-and-advance (`Draining -> Releasing`) and the `mountMap[ru]`
    compare-and-delete are ONE critical section under `mountMu` (exactly-
    once via the CAS-on-delete, reusing the `evictStaleMount` CAS shape),
    then `CloseReplicaUnit(ru)` once. Release fires ONLY on a positive
    readiness (the RPC, or the epoch-hint-plus-probe), never on a bare
    epoch. Armed as a bounded periodic re-check on the existing settle /
    self-heal cadence and woken early by the RPC. Lock discipline below.

- `multibackend_overlap_rpc.go` (new): the readiness signal + the forward.
  - `ReplicaHandoffReady(ru, epoch)` one-shot best-effort RPC (client +
    handler), reusing the existing `clientFor` / rpc server plumbing. The
    handler wakes `drainCheck` for that `ru` and is the AUTHORITATIVE
    release trigger (sent only after the mount flip).
  - a readiness probe RPC (or reuse of an existing health/ping op aimed at
    the new replica) used by the epoch-hint fallback to confirm a live owner
    is serving the position before releasing.
  - the predecessor-forward reuses the EXISTING forwarded-op RPCs
    (`PutForwarded`, `forwardGet`, the CAS-apply forward); no new wire type
    for the forward itself.

- `multibackend_handoff_retry.go` (kept): Option A's retry stays as the belt
  for the residual cases (predecessor unreachable, crash mid-handoff, a
  pure-new-mount initial convergence). Option B shrinks the window to near-
  zero; the retry is the safety net, not the primary mechanism.

RPC proto (`pkg/rpc/proto`): add the `ReplicaHandoffReady` message + rpc and
the readiness-probe rpc (or reuse an existing health rpc). The forward path
adds NO new proto. No correctness depends on the readiness RPC's delivery
(the durable-epoch hint plus the probe is the crash-proof fallback); the
readiness PROBE, by contrast, is on the safe path (it is what makes a bare
epoch advance non-releasing).

## Lock discipline (review P1-3)

- The `drainCheck` I/O (`DurableEpochReplica` read, the readiness probe) runs
  OUTSIDE any cluster lock. A slow MinIO read or a slow probe must NOT block
  routed ops' `mountMap` reads.
- The phase compare-and-advance (`Draining -> Releasing`) and the
  `mountMap[ru]` compare-and-delete are ONE critical section under `mountMu`.
  Two wakeups (the poll tick and the RPC) racing into the same edge are made
  exactly-once by the CAS-on-`mountMap`-delete (delete only if the entry
  still points at the same backend) performed under the same `mountMu` hold
  as the phase advance. The phase machine ALSO rejects a second `Releasing`
  edge, but the `mountMap` CAS is the real exactly-once guard.
- Lock order: `drainCheck` (whether woken by the poll cadence or by the RPC
  handler goroutine) touches ONLY `mountMu` + the `handoffPhase` map. It does
  NOT take `reconcileMu` or `reshardMu`, so it cannot invert the
  `reshardMu -> reconcileMu` order the self-heal / settle path uses. The
  `CloseReplicaUnit` call happens AFTER the `mountMu` critical section (the
  entry is already removed), so a slow close does not hold `mountMu`.

## Overlap-move vs plain-release (review P2-3)

The overlap state machine applies ONLY when a position MOVES from a specific
OLD owner to a specific NEW owner (the new owner forwards to that
predecessor; the old owner drains until that new owner is Ready). A node
that simply DROPS OUT of a unit's replica set entirely - where some OTHER
already-mounted replica covers W and no node takes over THIS node's exact
position - stays a PLAIN clean-cut `releaseReplicaUnit`: nobody forwards to
it and nobody routes to it after the ring rotation, so there is nothing to
overlap. The reconcile diff distinguishes them by whether the departing
position has a NEW owner acquiring it (overlap) or simply vanishes from the
set with the remaining replicas still covering W (plain release). No stray
"isDraining" flag for the plain case: it takes the existing eager-delete
path unchanged.

## R/W availability matrix during a crash (review P1-4)

The happy path is mount-time-independent for all R/W configs (the old owner
serves via the new owner's forward throughout `Acquiring`). A crash DURING a
handoff degrades the AFFECTED position's availability, and the degradation
depends on the config:

| Config                         | OLD crashes mid-Draining            | NEW crashes mid-Acquiring      |
| ------------------------------ | ----------------------------------- | ------------------------------ |
| R>=3, W=majority (quorum)      | available (other replicas cover W)  | available (OLD still serving)  |
| R=2, W=1                       | available (the other replica is W)  | available (OLD still serving)  |
| R=2, W=2 (write-all)           | mount-bounded: writes to this       | available (OLD still serving)  |
|                                | position cannot reach W=2 until NEW |                                |
|                                | mounts (only 1 live replica)        |                                |

The honest statement: overlap makes the HAPPY path mount-time-independent,
but a crash during a handoff degrades the affected position to mount-bounded
for the configs where the surviving replica count cannot satisfy W. For R=2
write-all this is ANY single OLD-owner crash mid-handoff (not a rare double-
fault). Quorum configs (R>=3, W=majority) stay available under a single
crash. Do NOT state crash safety as "always fully available."

## What does NOT change

- The replicated apply-if-newer, LWW envelope stamping, quorum/fan-out
  selection of WHICH ring nodes are contacted, and the W math
  (`requiredWriteAcks` against the stable R): untouched. Option B changes
  only WHEN the old owner releases and ADDS a new-owner `Acquiring`-state
  forward to the predecessor; it does NOT change how a write is replicated
  across the ring's replica set or how W is computed.
- The legacy per-node path and the R=1 multi-backend lease handoff
  (Phase 3): untouched. Option B lives behind `multiReplicated()`.
- Epoch fencing semantics: unchanged. Option B reuses the exact same
  per-replica durable-manifest fence; it reads the durable epoch as a
  LIVENESS HINT (re-verified before release), and the open-time fence is
  the same acquire fence as before.

## What DOES change (explicit, since the original "nothing routing-side
changes" claim is now false)

- `mountMap` / `replicaPos` re-keyed to `ReplicaUnit` (and `replicaPos`
  folded into the key), rippling through every mount reader.
- The NEW owner gains an `Acquiring`-state behavior: forward routed ops to
  the recorded predecessor (read AND write), stop on `Ready`.
- The OLD owner's release trigger is "new owner Ready" (the RPC, or the
  epoch-hint-plus-readiness-probe), NOT a bare durable-epoch advance.
- A new `handoffPhase` map + the `ReplicaHandoffReady` and readiness-probe
  RPCs.
