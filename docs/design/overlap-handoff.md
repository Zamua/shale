# Overlap handoff (Option B) for R>1 multi-backend membership change

Status: DESIGN. This is the design companion to the SPEC section
"v0.8 Phase 2e: overlap handoff (Option B)". The SPEC is the source of
truth for behavior; this doc carries the state diagram, the handoff
sequence, and the planned package/file layout for the future
implementation. No code has landed yet.

## The one-sentence design

Replace the clean-cut RELEASE-then-ACQUIRE of a moving (unit, replica)
with an ACQUIRE-then-RELEASE driven by a single per-(unit, replica)
ownership-transition state machine, where the cut between "old owner
serves" and "old owner releases" is the moment the NEW owner's durable
manifest writer-epoch becomes visible ABOVE the old owner's open epoch.
Availability stops depending on mount time because the old owner never
stops serving until the new owner is provably ready.

## Why a state machine, not flags

The current code expresses a handoff as two independent halves on two
nodes: the losing node drops the unit from its desired set and RELEASEs;
the gaining node adds it and ACQUIREs. Nothing sequences them, so there
is a window where neither serves. Option B adds sequencing, and the clean
way to express "this position is mid-transition, and these are the only
legal next steps" is one explicit phase per side, not a scatter of
booleans (isDraining, hasFlipped, pendingRelease, ...). The phase value
plus the mountMap presence is the complete state; every transition is a
single guarded edge.

## Roles and the shared truth

- A (unit, replica) = (GenUnit, replica-position) is the unit of
  ownership. It is node-independent: the same durable prefix
  `dbNameReplica(ru)` is what the OLD owner has open and what the NEW
  owner opens. That node-independence is what makes overlap possible.
- The ring is the shared truth. Both nodes compute the same
  `desiredReplicaUnits()` from the same converged ring and independently
  conclude "position rP of unit K moves from node OLD to node NEW."
- The cross-node signal is the DURABLE slatedb manifest writer-epoch for
  ru, readable WITHOUT opening via `DurableEpochReplica(ru)`. The new
  owner's `OpenReplicaUnit` bumps that durable epoch as a side effect of
  fencing; the old owner polls it and sees it cross above its own open
  epoch. No new wire type is required for correctness.

## State diagram

Two halves of ONE transition, each a small phase machine. The phase map
holds only IN-FLIGHT positions; absence means steady-state (Owned if
mounted, Absent if not).

NEW owner (the node GAINING the position):

```
  Absent
    | ring assigns rP here, not yet mounted
    v
  Acquiring  ---- OpenReplicaUnit running (slow MinIO mount + fence) ----.
    |  routing does NOT resolve this node yet (mountMap has no entry);    |
    |  the OLD owner is still serving. THIS IS THE OVERLAP WINDOW.        |
    |  OpenReplicaUnit returns (mounted + durably fenced)                 |
    v                                                                     |
  Ready  --- mountMap[gu]=b + replicaPos[gu]=rP inserted under mountMu ---'
    |    THE INSTANT THIS ENTRY IS VISIBLE, this node answers routed
    |    ops for the position. THIS INSERT IS THE FLIP.
    |    (best-effort) send ReplicaHandoffReady(ru, openedEpoch) to OLD
    v
  Owned   (steady state; phase entry dropped)
```

OLD owner (the node LOSING the position):

```
  Owned   (mounted + serving)
    | reconcile computes rP no longer desired here, but DO NOT release yet
    v
  Draining  --- keep serving; arm a release-check ------------------.
    |   release-check fires when EITHER                              |
    |     (a) DurableEpochReplica(ru) > myOpenEpoch   (durable, primary), OR
    |     (b) a ReplicaHandoffReady(ru, e) arrives with e > myOpenEpoch (fast-path)
    |   Until then the old owner KEEPS ANSWERING routed ops.         |
    v                                                                |
  Releasing  --- compare-and-delete mountMap entry, CloseReplicaUnit ('
    |    (flush is a no-op for acked writes: durable-before-ack;
    |     CloseReplicaUnit still seals to be safe). EXACTLY ONCE.
    v
  Absent  (phase entry dropped)
```

The cut (the fence point) is a single instant on the durable manifest:
the epoch at which the NEW owner opened. Below it, the OLD owner is the
authoritative writer; at or above it, the NEW owner is. The OLD owner's
own writes are fenced the moment it tries one past that epoch (slatedb
CloseReasonFenced), so even a buggy late write on the old side cannot
corrupt the position. Routing flips at the NEW owner's mountMap insert,
which happens strictly AFTER the fence, so there is never an instant
where both nodes' mountMap resolves AND both are unfenced.

## Sequence of one handoff (happy path)

```
ring change: position rP of unit K  OLD --> NEW

  t0  both nodes observe converged ring; both schedule reconcile.
  t1  NEW reconcile: rP desired, not mounted -> phase Acquiring.
        NEW: OpenReplicaUnit(ru, intended) starts the slow mount.
  t1  OLD reconcile: rP no longer desired -> phase Draining (NOT released).
        OLD: keeps serving rP; arms release-check (poll + RPC-wake).
  t1..t2  OVERLAP: writes routed to rP land on OLD (still mounted), get
        acked normally. NEW is invisible to routing. Mount time here is
        irrelevant to availability.
  t2  NEW: OpenReplicaUnit returns at openedEpoch E (E > durable, fences
        any lower writer). NEW inserts mountMap entry -> phase Ready ->
        Owned. NEW now answers routed ops. THE FLIP.
        NEW: best-effort ReplicaHandoffReady(ru, E) -> OLD.
  t3  OLD release-check observes DurableEpochReplica(ru) == E > myOpenEpoch
        (or the RPC arrived first). OLD: compare-and-delete its mountMap
        entry, CloseReplicaUnit(ru) once -> phase Releasing -> Absent.
  done. No instant between t1 and t3 had the position unserved.
```

During the whole interval at least one of {OLD, NEW} serves the position,
and only one is ever the authoritative (unfenced) writer.

## Crash handling (the four cases)

1. NEW crashes mid-acquire (between t1 and t2): the durable epoch never
   advances, OLD's release-check never fires, OLD stays in Draining and
   KEEPS SERVING. No data loss, no unavailability, just no progress. The
   next reconcile re-derives the ring; if NEW is gone the ring reassigns
   rP (possibly back to OLD, which simply drops the Draining phase and
   returns to Owned, or to a third node which starts its own Acquiring).

2. OLD crashes while Draining (before NEW is Ready): the position is
   momentarily down on the routing-resolved side, BUT (a) at R>1 the
   other replicas of unit K still serve reads/writes toward W, and (b)
   NEW is already acquiring from the SAME durable state and completes the
   flip; durable-before-ack means every acked write is already in
   `dbNameReplica(ru)`, so NEW sees them. This degrades to the Option-A
   window for THIS one position only, bounded by NEW's mount, and only if
   the membership change ALSO killed the old owner mid-flight.

3. NEW crashes AFTER Ready but before OLD released (between t2 and t3):
   OLD is still mounted but fenced (its writes fail), and NEW is gone.
   The position is owned by NEW per the ring but unmounted. This is the
   ordinary acquiring window: the next reconcile reassigns rP (NEW is
   gone) and OLD, still holding the durable state, re-acquires at a
   higher epoch or hands to a third node. No acked write lost (all
   durable). OLD's stale mount is torn down by stale-mount eviction
   (`evictStaleMount`) the moment it tries a fenced write.

4. Both nodes survive but the ReplicaHandoffReady RPC is lost: pure
   latency. OLD's release-check falls back to the durable-epoch poll and
   releases on the next tick. Correctness is unaffected; the RPC only
   shortens the drain.

## The slate manifest seal (consistent mount)

The NEW owner must mount a view that contains every write the OLD owner
acked. Two facts make this hold without a new flush protocol:

- Durable-before-ack: the slate replica opens with AwaitDurable=true, so
  every acked write is in the durable WAL/SST before its ack returns. The
  NEW owner opening `dbNameReplica(ru)` reads that durable state.
- The fence is on the durable manifest writer-epoch. `OpenReplicaUnit`
  reads the manifest, opens at max(intended, durable+1), and that open
  itself writes the bumped epoch to the manifest. The OLD owner's
  subsequent CloseReplicaUnit seals (Db.Shutdown flushes) but it is
  belt-and-suspenders for acked writes: those are already durable.

The one subtlety Option B adds over Option A: in Option A the old owner
RELEASED (sealed) before the new owner ACQUIRED, so the new owner always
read a sealed manifest. In Option B the new owner acquires WHILE the old
owner is still writing. That is safe because the new owner does not need
the old owner's UN-acked, in-flight writes (the client never got success
for those and will retry); it needs every ACKED write, and those are
durable by AwaitDurable=true at the instant they were acked, which is
before the fence. A write the old owner acks AFTER the new owner has
fenced is impossible: that write would be on the old owner's now-fenced
handle and fail (CloseReasonFenced), so it is never acked. There is no
"acked on old, invisible to new" gap.

## Planned package / file layout

Domain (pure, no I/O) in `pkg/storageunit`:

- `handoff.go` (new): the pure transition types.
  - `HandoffPhase` enum: `PhaseAcquiring`, `PhaseReady`, `PhaseDraining`,
    `PhaseReleasing` (the four in-flight phases; steady states Owned /
    Absent are represented by mountMap presence + phase absence, not enum
    values).
  - `HandoffState{Phase HandoffPhase, Pos uint8, OpenEpoch Epoch}` value
    object: the per-(unit) in-flight record the cluster keys by GenUnit.
  - Pure transition functions / guards: `CanRelease(open, durable Epoch)
    bool` (= durable > open), `NextOnReady`, `NextOnDrain`, returning the
    next phase or an error for an illegal edge. These are table-driven and
    trivially unit-testable with no ring or factory.

- `factory.go` (edit): extend `ReplicaBackendFactory` with
  `DurableEpochReplica(ru ReplicaUnit) (Epoch, error)` so the cluster can
  read the cross-node durable signal through the domain seam (the slate
  Backing already implements it; this lifts it onto the interface). The
  test `sharedfactory` gains the same method over its shared epoch
  registry.

Controller (the wiring) in `pkg/cluster`:

- `multibackend_overlap.go` (new): the Option B controller, replacing the
  position-change branch of `reconcileReplicaUnits`.
  - `handoffPhase map[storageunit.GenUnit]storageunit.HandoffState` field
    on Cluster, guarded by mountMu (sibling of replicaPos).
  - `reconcileReplicaUnitsOverlap()`: the new diff. For a position MOVING
    AWAY (this node is the old owner): set Draining instead of releasing.
    For a position MOVING IN (this node is the new owner): set Acquiring +
    start `acquireReplicaUnitOverlap`. A pure NEW mount (initial
    convergence, no old owner) stays the existing acquire path.
  - `acquireReplicaUnitOverlap(ru)`: OpenReplicaUnit; on success insert the
    mountMap entry (THE FLIP) under mountMu, set Ready->Owned, fire the
    best-effort ReplicaHandoffReady RPC.
  - `drainCheck(gu)`: the old-owner release-check. Reads
    `DurableEpochReplica(ru)`; if it exceeds the old owner's open epoch,
    compare-and-delete the mountMap entry (only if it still points at the
    same backend, reusing the evictStaleMount CAS shape) and
    CloseReplicaUnit exactly once. Armed as a bounded periodic re-check on
    the existing settle/self-heal cadence and woken early by the RPC.

- `multibackend_overlap_rpc.go` (new): the fast-path wake.
  - `ReplicaHandoffReady(ru, epoch)` one-shot best-effort RPC (client +
    handler), reusing the existing `clientFor` / rpc server plumbing. The
    handler just wakes `drainCheck` for that gu. Losing it costs latency,
    never correctness.

- `multibackend_handoff_retry.go` (kept): Option A's retry stays as the
  belt for the residual cases (crash case 2/3, a pure-new-mount initial
  convergence). Option B shrinks the window to near-zero; the retry is the
  safety net for the rare unserved instant, not the primary mechanism.

RPC proto (`pkg/rpc/proto`): add the `ReplicaHandoffReady` message + rpc.
Fast-path only; no correctness depends on it.

Tests:

- `pkg/storageunit/handoff_test.go`: pure transition-table tests
  (every legal edge, every illegal edge rejected, `CanRelease` boundary).
- `tests/integration/overlap_handoff_test.go`: the acceptance gate. Stand
  up a multi-backend R=2 cluster on a sharedfactory whose
  `OpenReplicaUnit` can be made ARBITRARILY SLOW (a 30s mount injection),
  run a continuous writer through a 3->N membership change, and assert
  (a) the loss oracle (every acked key readable everywhere, zero loss) AND
  (b) ack rate ~100% EVEN with the slow mount (the Option A gate would
  drop to ~50% here). Kept honest by a break demo: disable the overlap
  (force clean-cut) and show the ack rate collapses, proving overlap is
  what holds availability.

## What does NOT change

- The write/read fan-out, apply-if-newer, quorum math, LWW envelope
  stamping: untouched. Option B changes only WHEN the old owner releases,
  not how a write is replicated or applied.
- The legacy per-node path and the R=1 multi-backend lease handoff
  (Phase 3): untouched. Option B lives behind `multiReplicated()`.
- Epoch fencing semantics: unchanged. Option B reuses the exact same
  per-replica durable-manifest fence; it only reads the durable epoch as
  a release signal in addition to using it as the acquire fence.
