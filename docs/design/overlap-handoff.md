# Overlap handoff (Option B) for R>1 multi-backend membership change

Status: DESIGN (amended). This is the design companion to the SPEC section
"v0.8 Phase 2e: overlap handoff (Option B)". The SPEC is the source of
truth for behavior; this doc carries the state diagram, the handoff
sequence, and the planned package/file layout for the future
implementation. No code has landed yet.

This revision closes the holes found by the adversarial reviews in
`docs/design/overlap-handoff-review.md` (first pass) and
`docs/design/overlap-handoff-rereview.md` (re-review of the amended
forwarding design), both kept as history. The operator chose the
architectural forks; the resolutions are baked in below. The one-line
summary of each is in the SPEC's "Resolved review findings" note.

The re-review's NEW findings (the holes the forwarding approach itself
introduced) are closed by five operator decisions folded in here:
position-addressed forward (NEW-P0), a retained prior-desired-replica-set
snapshot for predecessor identification (NEW-P1-1), single-hop scope with
an Option-A fallback for ambiguous predecessors (NEW-P1-2), the honest
case-3 residual (NEW-P1-3), and a pinned intra-open fence<=recovery
ordering (NEW-P1-4).

## The one-sentence design

Replace the clean-cut RELEASE-then-ACQUIRE of a moving `ReplicaUnit` with
an ACQUIRE-then-RELEASE driven by a single per-`ReplicaUnit`
ownership-transition state machine, where: (1) the NEW owner, while it is
still mounting, FORWARDS routed ops to the predecessor (the OLD owner) so
the ring rotation does not strand the position; and (2) the OLD owner
releases exactly when the NEW owner becomes READY (proven by a DURABLE,
POLL-OBSERVABLE serving marker the new owner writes once after its mount
flip; release is POLL-ONLY, never push, and a bare durable FENCE-epoch
advance alone NEVER triggers it). Availability stops depending on mount
time because the old owner keeps serving (via the new owner's forward)
until the new owner is provably serving locally.

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
  conclude "position rP of unit K moves from node OLD to node NEW." The NEW
  owner learns the predecessor's address by diffing a RETAINED
  prior-desired-replica-set snapshot against the live desired set (the
  multi analogue of the legacy `lastEvalRing`; see "Predecessor
  identification" below). The snapshot is consulted ONLY to identify the
  predecessor, NEVER for routing.
- Two durable signals over shared storage, with DISTINCT roles (the
  review's P1-1 fix). Release is POLL-ONLY; there is NO push RPC:
  - The SERVING MARKER (`WriteServingMarker(ru, E)` /
    `ReadServingMarker(ru)`) is the durable, poll-observable release signal.
    The new owner writes it EXACTLY ONCE, AFTER its mount flip (so it proves
    a live owner is actually SERVING the position), keyed by the
    `ReplicaUnit` and carrying its open epoch E. The old owner's
    `drainCheck` POLLS it on the periodic settle / self-heal cadence and
    releases only on observing a marker at an epoch STRICTLY ABOVE (`>`) its
    own open epoch. The boundary is STRICT, not `>=`, to reject a node's OWN
    stale gain-marker: a node that GAINED `ru` at epoch E wrote
    `WriteServingMarker(ru, E)`; if the ring later moves `ru` OFF that node,
    `beginDrain` sets `OpenEpoch = DurableEpochReplica(ru) = E` (unchanged
    until the NEW gainer opens). Under a `>=` gate the node would read its OWN
    stale marker E (`E >= E` true) and release while the real successor is
    still mid-mount, degrading any TWICE-churning position to clean-cut. A
    genuine successor always opens at `durable+1 >= E+1` and writes a marker
    STRICTLY above E, so `>` still releases on a real successor while
    rejecting the node's own gain-marker (exactly E). This matches
    `CanRelease`'s already-strict `durable > open` boundary.
  - The DURABLE slatedb manifest writer-epoch (`DurableEpochReplica(ru)`)
    is a LIVENESS HINT only, and it is STRICTLY WEAKER than the serving
    marker (it bumps at open-START, before the mount completes). Seeing it
    cross proves SOMEONE fenced, NOT that a live writer is serving (a new
    owner that fences and then crashes mid-mount advances the epoch without
    ever serving and without ever writing the marker). A BARE fence-epoch
    advance therefore NEVER releases the old owner; only the serving marker
    does. The fence epoch is kept here purely as a contrast: it is what the
    serving marker improves upon, not a release trigger.

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
  check. For a NORMAL (ring-routed) local op it must resolve the position
  THIS node holds for the key's unit. Because a node can hold at most one
  LOCAL replica position per unit at steady state (a node appears at most
  once in a unit's replica set), the resolver derives the position from the
  live ring (this node's index in `unitReplicas(gu)`) and looks up
  `mountMap[ReplicaUnit{gu, pos}]`. During a same-unit position shuffle,
  BOTH the draining old position and the acquiring new position can be
  present; the resolver picks the one the ring currently assigns this node
  (the new position once the ring rotates, the old position before).
- The DRAINING old-position entry is NOT reachable via this node's own ring
  index after rotation (the live ring no longer lists this node at that
  position - that is the whole premise of the move). It is reachable ONLY
  via the POSITION-ADDRESSED forward (the new-owner forward below): the
  forwarded op carries the EXPLICIT `ReplicaUnit`, and the predecessor's
  forwarded-op handler resolves `mountMap[ru]` DIRECTLY by that ru
  (explicitly willing to serve a `Draining`-phase entry), NOT by
  re-deriving the position from its own live ring index. This is the
  NEW-P0 fix; it is a real WIRE change (the forwarded-op messages gain a
  `ReplicaUnit` field), spelled out under "Predecessor forwarding" below.
  The draining-served path goes through the SAME `handoffPhase` /
  `mountMap[ru]` structures - there is NO parallel "draining backend" map.
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

- **Learning the predecessor address (NEW-P1-1).** The new owner records the
  predecessor when it enters `Acquiring`. The predecessor is the node that
  held this exact `ReplicaUnit` (same position rP of unit K) in the
  PRIOR desired replica set and does not in the LIVE set. The multi
  reconcile keeps NO prior-ring snapshot today (only the legacy path has
  `lastEvalRing`), so Phase 2e ADDS a retained prior-desired-replica-set
  snapshot (see "Predecessor identification" below for where it lives and
  when it is captured). The recorded predecessor address travels in the
  `HandoffState` (an `Acquiring` entry carries `Predecessor NodeAddr`).
- **Position-addressed forward (NEW-P0). This is a real WIRE change.** The
  forward is NOT the plain key-only forwarded op the cluster uses for a
  ring-resolved non-local replica leg. The plain forwarded ops
  (`PutRequest{forwarded}`, `GetRequest{forwarded}`,
  `DeleteRequest{forwarded}`) carry ONLY the key, and the receiver
  re-resolves the unit+position from the key against its OWN live ring
  index - which on the predecessor no longer yields the draining position
  (the position moved away). So the predecessor-forward carries the
  EXPLICIT `ReplicaUnit` (the moving gen-unit + position), and the
  predecessor's forwarded-op handler resolves `mountMap[ru]` DIRECTLY by
  that ru - explicitly willing to serve a `Draining`-phase entry - rather
  than re-deriving the position from its own ring index. Concretely: add a
  `ReplicaUnit` field (gen, unit, replica) to the forwarded-op path - either
  a new position-addressed forwarded variant or an added field on the
  existing `PutRequest` / `GetRequest` / `DeleteRequest` / CAS-apply-forward
  - so the predecessor can serve-by-ReplicaUnit. When that field is present
  the predecessor's handler bypasses the ring-index resolver and looks up
  `mountMap[ru]` directly, serving a `Draining` mount. This goes through the
  SAME `handoffPhase` / `mountMap[ru]` structures (no parallel draining map).
- **Transparent for reads AND writes.** The forward otherwise reuses the
  existing `clientFor` plumbing and the predecessor's normal local-apply /
  local-read code, just AIMED at the predecessor's address and ADDRESSED by
  the explicit `ReplicaUnit`. The predecessor lands in its local-apply /
  local-read path against the `mountMap[ru]` draining entry (it is still
  mounted and serving the position), so the op is applied/served against the
  authoritative durable database exactly as a direct op would be. The
  forwarded write is still `AwaitDurable`, so a forwarded ack means the
  write is durable at `dbNameReplica(ru)` and the new owner will read it on
  open.
- **Predecessor unreachable OR ambiguous (NEW-P1-2: single-hop scope with
  Option-A fallback).** If the forward RPC fails (predecessor down, dial
  error, timeout), OR the predecessor derived from the prior snapshot is
  AMBIGUOUS or STALE (the prior holder is already gone / has already
  released because the position coalesced more than one move into one
  reconcile - a multi-hop OLD -> MID -> NEW within one debounce window),
  the new owner does NOT guess. It FALLS THROUGH to the existing Option-A
  belt: it returns `errUnitAcquiring` for the position, which the fan-out
  tolerates as a transient and the `WriteTimeout`-bounded retry loops on.
  Overlap is therefore BEST-EFFORT and SINGLE-HOP: it forwards only when a
  single, unambiguous predecessor is identifiable from the prior snapshot;
  a big-churn coalesced (double-move) position degrades to today's Option-A
  behavior rather than forwarding to a stale node. A predecessor crash
  mid-overlap (crash case 2) is the same fallback. The blast radius stays
  small and the honest minimal fix is "degrade to Option A when the
  predecessor is ambiguous," never "forward blindly."
- **Stop condition.** Forwarding stops the instant the new owner reaches
  `PhaseReady` (its own mount entry is inserted). The same event that
  stops forwarding is the event that makes the new owner serve locally and
  the event the old owner releases on (see the dovetail below). One event,
  three effects.

The fan-out and W math are NOT in the list of "what does not change" any
more: the new owner gains an `Acquiring`-state forwarding behavior, AND the
forwarded-op wire gains a `ReplicaUnit` field so the predecessor serves the
draining position by explicit ru (the NEW-P0 fix). The W math against the
stable R is still untouched; the routing-surface change is the new owner's
`Acquiring`-state forward (local to one node's one state) plus the
position-addressed forward field. This is NOT a "no new proto" change.

## State diagram

Two halves of ONE transition, each a small phase machine, keyed by
`ReplicaUnit`. The phase map holds only IN-FLIGHT positions; absence means
steady-state (Owned if mounted, Absent if not).

NEW owner (the node GAINING the position):

```
  Absent
    | ring rotation assigns rP here, not yet mounted
    | record predecessor addr = prior-snapshot holder \ live holder of rP
    |   (if ambiguous/multi-hop: no predecessor -> Option-A fallback)
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
    |    WriteServingMarker(ru, E) to shared storage EXACTLY ONCE (the
    |    durable, poll-observable release signal; written only here, after
    |    the flip). No RPC is sent.
    v
  Owned   (steady state; phase entry dropped)
```

OLD owner (the node LOSING the position):

```
  Owned   (mounted + serving)
    | reconcile computes rP no longer desired here, but DO NOT release yet
    v
  Draining  --- keep serving (directly AND for the new owner's forwards) --.
    |   release-check (drainCheck) POLLS the serving marker on the         |
    |     periodic settle / self-heal cadence (no RPC wake). It fires when |
    |     ReadServingMarker(ru) returns a marker at epoch > myOpenEpoch    |
    |     (STRICT >: positive proof a live SUCCESSOR is SERVING; rejects   |
    |      my own stale gain-marker at exactly myOpenEpoch).               |
    |   A BARE DurableEpochReplica(ru) (fence) advance NEVER releases (it  |
    |     bumps at open-START, before the mount; only the marker proves    |
    |     serving). Until release, OLD keeps answering routed ops (its own |
    |     + forwarded).                                                    |
    v                                                                      |
  Releasing  --- compare-and-delete mountMap[ru], CloseReplicaUnit(ru) --- '
    |    (flush is a no-op for acked writes: durable-before-ack;
    |     CloseReplicaUnit still seals to be safe). EXACTLY ONCE.
    v
  Absent  (phase entry dropped)
```

## Predecessor identification (review NEW-P1-1, NEW-P1-2)

The forwarding fix needs the new owner to know WHICH node held the moving
position before. The multi-backend reconcile does not retain that today: the
LEGACY per-node path keeps `lastEvalRing` and diffs `(old, current)` in
`runEvaluate` (`rebalance.go:282-283`), but `reconcileReplicaUnits`
(`multibackend_replicated.go:224-254`) computes ONLY
`desiredReplicaUnits()` against the LIVE ring - it has no prior argument and
never reads `lastEvalRing`. So "evaluate the pre-change replica set" has no
source in the multi path.

Phase 2e ADDS a retained snapshot, the multi analogue of `lastEvalRing`:

- **What it holds.** The DESIRED replica sets as of the ring the LAST
  reconcile used - i.e. `desiredReplicaUnits()` captured at the END of each
  reconcile (a `map[GenUnit][]NodeID` of position -> holder, or the
  equivalent `[]ReplicaUnit`-to-owner view). NOT "the live ring minus this
  event"; the snapshot the previous reconcile actually acted on.
- **Where it lives.** A new `Cluster` field (e.g. `priorDesiredReplicas`)
  guarded by the same lock that guards `replicaPos` today (the mountMu /
  reconcile critical section), captured at the END of each
  `reconcileReplicaUnits` run, BEFORE the next run can read it.
- **How the predecessor is derived.** For a position rP of unit K that the
  live set assigns to THIS node but the prior snapshot did not, the
  predecessor is `prior-holder(K, rP) \ live-holder(K, rP)`: the node that
  held position rP of K in the prior snapshot and does not hold it now.
- **Consulted ONLY to identify the predecessor, NEVER for routing.** Routing
  is always live-ring-derived; the snapshot only answers "who do I forward
  to while I acquire."

**Single-hop scope (NEW-P1-2).** The snapshot is point-to-point: it captures
the ring as of the LAST reconcile, not every intermediate state. The settle
timer debounces a BURST of membership events into ONE reconcile, so a
position can move OLD -> MID -> NEW across two events that land in one
debounce window. When that happens the prior-snapshot holder (OLD) may
already be gone or have already released, and forwarding to it would fail.
Overlap is therefore scoped to SINGLE-HOP: if the predecessor derived from
the prior snapshot is ambiguous or stale (the prior holder no longer holds
the position, or more than one move coalesced for this position), the new
owner records NO predecessor and the `Acquiring` op falls through to the
Option-A clean-cut belt (`errUnitAcquiring` + bounded retry). A big-churn
coalesced move is thus CORRECT (degrades to today's behavior) rather than
forwarding to a stale node. Overlap is best-effort; it shrinks the
common-case window to near zero and leaves the rare multi-hop case to
Option A.

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
ALSO exactly when the new owner stops forwarding. One event (the mount flip)
drives both: the new owner flips to local serving (stops forwarding to the
predecessor) AND writes the serving marker the old owner polls to release on.
There is no window where the old owner has released but the new owner is
still forwarding to it (the new owner only forwards while `Acquiring`, it
writes the marker only after leaving `Acquiring`, and the old owner releases
only after observing that marker). The release is POLL-driven: the old owner
sees the marker on its next `drainCheck` tick rather than on a push, so the
single-instant handoff is observed at poll latency, not RPC latency.

## Sequence of one handoff (happy path)

```
ring change: position rP of unit K  OLD --> NEW

  t0  both nodes observe converged ring; both schedule reconcile.
  t1  NEW reconcile: rP desired, not mounted -> phase Acquiring, record
        predecessor = OLD (prior-snapshot holder \ live holder of rP). If
        that is ambiguous/multi-hop, record NO predecessor and let the
        Acquiring op fall through to Option-A (errUnitAcquiring + retry).
        NEW: OpenReplicaUnit(ru, intended) starts the slow mount.
  t1  OLD reconcile: rP no longer desired -> phase Draining (NOT released).
        OLD: keeps serving rP; arms release-check (drainCheck POLLS the
        serving marker on the settle / self-heal cadence; no RPC wake).
  t1..t2  OVERLAP: the ring has rotated, so writes route to NEW. NEW is
        Acquiring (no local mount) and FORWARDS each op to OLD ADDRESSED BY
        THE EXPLICIT ru (position-addressed forward). OLD's handler resolves
        mountMap[ru] directly (serving the Draining entry, not via its live
        ring index), applies durably, and acks back through NEW.
        Mount time here is irrelevant to availability.
  t2  NEW: OpenReplicaUnit returns at openedEpoch E (E > durable, fences
        any lower writer). NEW inserts mountMap[ru] -> phase Ready. NEW now
        serves rP LOCALLY and STOPS forwarding. THE MOUNT FLIP.
        NEW: WriteServingMarker(ru, E) to shared storage, exactly once.
  t3  OLD release-check (next drainCheck poll tick): ReadServingMarker(ru)
        returns a marker at epoch E STRICTLY ABOVE (>) OLD's open epoch
        (NEW opened at E > durable >= OLD's open epoch, so E > OLD's open
        epoch strictly; the strict gate also rejects OLD's own stale
        gain-marker if OLD had earlier gained rP at its own open epoch). OLD:
        compare-and-delete its mountMap[ru] entry, CloseReplicaUnit(ru) once
        -> Releasing -> Absent.
  done. No instant between t1 and t3 had the position unserved.
```

During the whole interval at least one of {OLD, NEW} serves the position
(OLD directly or via forward through t2, NEW locally after t2), and only
one is ever the authoritative (unfenced) writer.

## Crash handling (the four cases)

1. NEW crashes mid-acquire (between t1 and t2): the new owner stops
   forwarding (it is gone), but it never wrote the serving marker and never
   advanced past fence-then-serve, so OLD's release-check never fires (a
   bare fence-epoch advance from NEW's pre-crash fence does NOT release: no
   serving marker strictly above OLD's epoch exists). OLD stays in `Draining` and
   KEEPS SERVING. No data loss, no unavailability of the position from the
   OLD owner's side; the only effect is that ops in-flight TO the crashed
   new owner fail and retry. The next reconcile re-derives the ring; if NEW
   is gone the position reassigns (possibly back to OLD, which drops the
   `Draining` phase and returns to Owned, or to a third node that starts
   its own `Acquiring` with OLD as predecessor). This is the crash case the
   serving-marker release protects: under the OLD design's bare-epoch
   release, NEW's pre-crash fence would have triggered OLD to release,
   stranding the position. It no longer does, because the fence is not the
   release signal - the serving marker is, and it was never written.

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
   serving marker was written at the flip (it is durable in shared storage),
   so OLD reads it on its next `drainCheck` poll and releases; the position
   is owned-by-NEW per the ring but NEW is gone, so the next reconcile
   reassigns it (to OLD or a third node) and re-acquires from the durable
   state. If OLD has not yet polled when NEW crashes, OLD stays
   `Draining`-serving and either reads the (still-durable) marker on a later
   poll and releases, or the next reconcile reassigns the position first;
   either way the position is never stranded. No acked write is lost (all
   durable), and any stale OLD mount is torn down by `evictStaleMount` on
   its next fenced write.

   **Residual (NEW-P1-3, honesty fix - the marker read is point-in-time).**
   The serving-marker read is a point-in-time liveness OBSERVATION, NOT a
   lease or latch. There is a window: OLD reads the marker (NEW is serving,
   at read time), OLD proceeds into the release critical section, and NEW
   crashes CONCURRENTLY with that release (in the read-to-release gap). OLD
   already has its positive observation and does not re-read inside the lock
   (it could not without holding I/O under the lock, which the lock
   discipline forbids). So OLD releases AND NEW is gone: the position is
   UNSERVED until the NEXT RECONCILE reassigns it. This is benign - there is
   NO acked-write loss (durable-before-ack holds; every acked write is
   durable below E and recovered by whoever next acquires) - and the
   recovery is the same next-reconcile path as the rest of case 3. So the
   serving marker makes OLD safe against a NEW that crashed BEFORE becoming
   Ready (crash case 1, where no marker was ever written), NOT against a NEW
   that crashes in the release gap. Do not read the marker as making case 3
   fully crash-safe; it closes the bare-fence-epoch hole, not the
   point-in-time-liveness gap, and the gap's recovery is the benign next
   reconcile.

4. Both nodes survive but OLD is slow to observe the marker: because release
   is POLL-ONLY, OLD always converges to release on its next `drainCheck`
   tick once the durable marker is visible. There is no "lost signal" case
   to recover from (the marker is durable in shared storage, not a
   best-effort push); the only variable is poll latency, bounded by the
   settle / self-heal cadence. Correctness unaffected.

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
- **Intra-open ordering: the fence is effective NO LATER THAN the
  WAL-recovery cutoff (review NEW-P1-4).** Forwarding continues until the
  mount flip, which is strictly AFTER `OpenReplicaUnit` completes. So a
  forwarded write can be acked on OLD AFTER NEW's recovery snapshot but
  still BELOW E (OLD is authoritative below E until the fence is effective).
  Unless the fence becomes effective at or before NEW takes its recovery
  snapshot, such a write is durable below E yet outside NEW's recovered view
  - a lost read-after-write. The requirement: within a single
  `OpenReplicaUnit`, make the manifest-epoch FENCE effective FIRST, THEN
  take the WAL-recovery snapshot (or re-scan the WAL tail after the fence).
  Then every write OLD can still ack is inside NEW's recovered tail: a write
  past the fence is rejected on OLD and never acks; a write before the fence
  is in the recovered tail. `backends/slate/factory.go`'s `OpenReplicaUnit`
  TODAY orders `fenceEpochReplica` (writes the bumped manifest epoch =
  fence) BEFORE `openSlateReplica` (opens the db, where the Rust core's WAL
  recovery runs), which is the correct fence-then-recover order at the Go
  level. Whether the Rust core's recovery snapshot is taken at-or-after that
  bumped epoch is FFI-opaque (same opacity as the WAL-replay assumption
  below); flag it for implementation and PIN it with the widened test. If
  the slate open is found to recover-then-fence internally, the slate
  `OpenReplicaUnit` ordering must be pinned/adjusted (a re-scan of the WAL
  tail after the fence). Do not write that code here; flag it.

### EXPLICIT ASSUMPTION (implementation-must-verify, release-gating)

The slatedb-go binding (`slatedb.io/slatedb-go v0.13.1`) is a uniffi FFI
over the Rust core; the Go source does not expose the open/recovery path,
so we CANNOT confirm from the module source that `Db` open replays the full
durable WAL tail. The binding does expose an independent WAL concept (a
`WalReader` over WAL files that exist separately from the manifest), which
is consistent with WAL-replay-on-open but is not proof of it.

Therefore this is an EXPLICIT ASSUMPTION the implementation MUST PIN with a
test before Phase 2e ships. The pin covers BOTH the P1-2 case AND the
NEW-P1-4 intra-open ordering:

1. (P1-2) a write acked on the OLD owner JUST BEFORE the fence (durable in
   the WAL but possibly not yet manifest-checkpointed) must be readable on
   the NEW owner AFTER the handoff.
2. (NEW-P1-4, the WIDENED case) a write driven THROUGH THE FORWARD PATH
   CONCURRENTLY with the flip (forwarded to OLD, acked below E, racing NEW's
   open/recovery) must be readable on NEW AFTER the handoff. This exercises
   the fence-effective-no-later-than-recovery-cutoff ordering, not only the
   write-just-before-fence ordering.

If slatedb open does NOT replay the durable WAL tail, OR the fence is not
effective at-or-before the recovery cutoff, this is a P0 lost-write and
Phase 2e is blocked until the gap is closed (e.g. forcing an OLD-side
`Db.Flush`/checkpoint into the manifest before the new owner opens, pinning
the slate `OpenReplicaUnit` to re-scan the WAL tail after the fence, or
another seal protocol). The acceptance gate's loss oracle (every acked key
readable everywhere) covers this in aggregate, but the two targeted tests
(write-then-immediately-fence, and write-through-forward-concurrent-with-
flip) are the honest pins.

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
    Predecessor NodeID}` (the predecessor is meaningful only in
    `Acquiring`). The cluster keys the in-flight record by `ReplicaUnit`
    (the position is the key, not a field). The predecessor field is a
    `storageunit.NodeID` (the pure domain's node identity), NOT a dial
    address: it is derived from the prior/live replica-set diff, which yields
    `NodeID`s, and the pure domain stays I/O-free. Resolving a `NodeID` to a
    transport address is a controller/membership concern done at forward time.
    (Earlier drafts of this doc named the field `NodeAddr`; the foundation
    implementation pinned it to `NodeID` to keep the type I/O-free.)
  - Pure transition functions / guards: `NextOnReady`, `NextOnDrain`,
    `NextOnRelease`, returning the next phase or an error for an illegal
    edge; the guards make explicit WHICH phases are legal on WHICH side (a
    node is `Acquiring/Ready` on the gainer side, `Draining/Releasing` on
    the loser side, and CAN be in both for DIFFERENT positions of one unit,
    which is why the key is `ReplicaUnit`). A `Releasable(state, ready bool)`
    predicate encodes the release rule: release requires a positive
    readiness (a serving marker at an epoch STRICTLY ABOVE (`>`) the old
    owner's open epoch, computed in the controller from `ReadServingMarker`),
    NEVER a bare fence-epoch compare. The strict `>` (not `>=`) rejects the
    old owner's OWN stale gain-marker at exactly its open epoch (a real
    successor opens at `durable+1` and writes a marker strictly higher). These are table-driven and trivially unit-testable
    with no ring or factory.

- `factory.go` (edit): extend `ReplicaBackendFactory` with
  `DurableEpochReplica(ru ReplicaUnit) (Epoch, error)` so the cluster can
  read the cross-node durable LIVENESS HINT (the bare fence epoch, kept as
  a contrast) through the domain seam (the slate `Backing` already
  implements it; this lifts it onto the interface), PLUS the SERVING-MARKER
  seam: `WriteServingMarker(ru ReplicaUnit, epoch Epoch) error` and
  `ReadServingMarker(ru ReplicaUnit) (Epoch, bool, error)` (the durable,
  poll-observable release signal). The test `sharedfactory` implements all
  three over an in-memory registry (the serving marker is a per-`ru`
  epoch entry in a shared map), plus the SLOW-mount injection the
  acceptance gate needs. The slate factory implements the serving marker as
  a small durable object keyed by `ru` (real I/O); it is exercised only in
  staging, not in the local acceptance gate.
  Implementation-must-verify (NEW-P1-4): the slate `OpenReplicaUnit`
  (`backends/slate/factory.go`) today orders `fenceEpochReplica` (writes the
  bumped manifest epoch) BEFORE `openSlateReplica` (opens the db, where the
  Rust core recovers), which is the correct fence-then-recover order at the
  Go level. If the Rust core's recovery snapshot is found to be taken before
  the bumped epoch is effective (FFI-opaque), the slate open's internal
  ordering must be pinned/adjusted (re-scan the WAL tail after the fence).
  Flag for implementation; do not write code here.

Controller (the wiring) in `pkg/cluster`:

- `cluster.go` (edit): re-key `mountMap` to `map[ReplicaUnit]backend.Backend`,
  remove the separate `replicaPos` map (subsumed by the key), add
  `handoffPhase map[ReplicaUnit]HandoffState` guarded by `mountMu`. ADD a
  `priorDesiredReplicas` snapshot (the prior desired replica sets, the multi
  analogue of legacy `lastEvalRing`) guarded like `replicaPos` is today,
  captured at the END of each reconcile and consulted ONLY to identify the
  predecessor (never for routing) - per "Predecessor identification" above.
  Update every reader (`localBackendForKey`, `localWriteBackendForKey`,
  `evictStaleMount`, `mountedBackends`, the reconcile diff) per "Keying by
  ReplicaUnit" above; `localBackendForKey` also gains a position-addressed
  path used by the forwarded-op handler (resolve `mountMap[ru]` directly by
  an explicit ru, serving a `Draining` entry).

- `multibackend_overlap.go` (new): the Option B controller, replacing the
  position-change branch of `reconcileReplicaUnits`.
  - `reconcileReplicaUnitsOverlap()`: the new diff. For a position MOVING
    AWAY (this node is the old owner, the position is still in some other
    node's new replica set): set `Draining` instead of releasing. For a
    position MOVING IN (this node is the new owner): derive the predecessor
    as `priorDesiredReplicas`-holder \ live-holder of the moving position;
    if a SINGLE unambiguous predecessor results, set `Acquiring` (record
    it) + start `acquireReplicaUnitOverlap`. If the predecessor is AMBIGUOUS or
    STALE (multi-hop coalesced move; prior holder gone/already released),
    record NO predecessor so the `Acquiring` op falls through to Option A -
    single-hop scope, NEW-P1-2. At the END of the reconcile, capture the new
    `priorDesiredReplicas` snapshot. A pure NEW mount (initial convergence,
    no prior holder) stays the existing acquire path. A node simply DROPPING
    OUT of a unit's replica set entirely (no specific successor takes ITS
    exact position; some other already-mounted replica covers W) stays a
    PLAIN clean-cut release (`releaseReplicaUnit`), because nobody forwards
    to it and nobody routes to it after the ring rotation - see
    "Overlap-move vs plain-release" below.
  - `acquireReplicaUnitOverlap(ru)`: OpenReplicaUnit; on success insert the
    `mountMap[ru]` entry (THE MOUNT FLIP) under mountMu, set `Ready -> Owned`,
    STOP forwarding, then `WriteServingMarker(ru, E)` to shared storage
    EXACTLY ONCE (the durable, poll-observable release signal). No RPC.
  - the `Acquiring`-state forward: when `localBackendForKey` / the routed-op
    path finds a `ReplicaUnit` in `PhaseAcquiring` on this node WITH a
    recorded predecessor, it forwards the op to that predecessor via
    `clientFor` + the POSITION-ADDRESSED forwarded-op path (the explicit ru
    rides on the wire so the predecessor resolves `mountMap[ru]` directly),
    returning the predecessor's result; on forward failure OR no recorded
    predecessor (the single-hop fallback) it returns `errUnitAcquiring` (the
    Option-A belt).
  - the predecessor's forwarded-op handler: when a forwarded op carries an
    explicit `ReplicaUnit`, resolve `mountMap[ru]` DIRECTLY by that ru
    (serving a `Draining`-phase entry), bypassing the live-ring-index
    resolver. Through the SAME `handoffPhase` / `mountMap[ru]` structures -
    no parallel draining map.
  - `drainCheck(ru)`: the old-owner release-check. The
    `ReadServingMarker(ru)` poll (MinIO / shared-storage I/O) runs BEFORE
    entering the critical section. The phase compare-and-advance
    (`Draining -> Releasing`) and the `mountMap[ru]` compare-and-delete are
    ONE critical section under `mountMu` (exactly-once via the
    CAS-on-delete, reusing the `evictStaleMount` CAS shape), then
    `CloseReplicaUnit(ru)` once. Release fires ONLY on a positive readiness
    (a serving marker at an epoch STRICTLY ABOVE (`>`) this node's open
    epoch), never on a bare fence-epoch advance. The strict `>` (not `>=`)
    rejects the node's own stale gain-marker at exactly its open epoch. Armed as a bounded periodic re-check on the
    existing settle / self-heal cadence; POLL-ONLY (no RPC wake). Lock
    discipline below.

- `multibackend_overlap_forward.go` (new): the position-addressed forward.
  - the predecessor-forward is POSITION-ADDRESSED: it reuses the existing
    forwarded-op operations (Put/Get/Delete-as-tombstone/CAS-apply) but adds
    a `ReplicaUnit` field to the wire so the predecessor serves the draining
    position by explicit ru. This is a real WIRE change (NEW-P0); it is NOT
    a "no new proto" reuse of the existing key-only forwarded messages. (No
    `ReplicaHandoffReady` RPC and no readiness-probe RPC exist: release is
    poll-only via the durable serving marker on the factory seam.)

- `multibackend_handoff_retry.go` (kept): Option A's retry stays as the belt
  for the residual cases (predecessor unreachable OR ambiguous/multi-hop,
  crash mid-handoff, a pure-new-mount initial convergence). Option B shrinks
  the common-case window to near-zero; the retry is the safety net, not the
  primary mechanism.

RPC proto (`proto/shale.proto`): the ONLY proto change is a `ReplicaUnit`
(gen, unit, replica) field added to the forwarded-op path - either a new
position-addressed forwarded variant or an added field on the existing
`PutRequest` / `GetRequest` / `DeleteRequest` / CAS-apply-forward messages
(today they carry only the key + a `forwarded` bool, which re-resolves to
the wrong/absent position on the predecessor). The forward path therefore
DOES add proto, contrary to the earlier draft's claim. There is NO
`ReplicaHandoffReady` message + rpc and NO readiness-probe rpc: release is
POLL-ONLY via the durable serving marker on the factory seam, so no
correctness or release behavior depends on any push RPC. The position-
addressed forward is the only cross-node wire change Phase 2e introduces.

## Lock discipline (review P1-3)

- The `drainCheck` I/O (the `ReadServingMarker` poll) runs OUTSIDE any
  cluster lock. A slow MinIO read must NOT block routed ops' `mountMap`
  reads.
- The phase compare-and-advance (`Draining -> Releasing`) and the
  `mountMap[ru]` compare-and-delete are ONE critical section under `mountMu`.
  Overlapping poll ticks racing into the same edge are made exactly-once by
  the CAS-on-`mountMap`-delete (delete only if the entry still points at the
  same backend) performed under the same `mountMu` hold as the phase
  advance. The phase machine ALSO rejects a second `Releasing` edge, but the
  `mountMap` CAS is the real exactly-once guard.
- Lock order: `drainCheck` (run on the poll cadence) touches ONLY
  `mountMu` + the `handoffPhase` map. It does
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

## Graceful leave (scale-down) - drain on shutdown

The overlap machine above is symmetric between scale-UP and scale-DOWN: a
JOIN makes a survivor `Acquiring` while the existing owner `Draining`-serves;
a deliberate REMOVAL makes the survivors `Acquiring` the leaving node's
positions while the LEAVING node `Draining`-serves them. So a graceful leave
is just an overlap drain seen from the LOSING side, for ALL of a node's
positions at once. The one missing piece is on the SHUTDOWN path, not the
reconcile path: the leaving node's `Close()` does not WAIT for that drain.

THE GAP. On SIGTERM the run loop (`pkg/shaled/runtime.go`) calls
`Cluster.Close()` (`pkg/cluster/cluster.go`). `Close()` broadcasts the
memberlist leave (`membership.Close()` -> `memberlist.Leave(0)` then
`Shutdown()`), so peers re-own the units and begin forwarding to the leaving
node - but it IMMEDIATELY tears down the reconcile loops + peer clients +
`closeMountedUnits()`, WITHOUT waiting for the survivors' slow `Acquiring` to
complete. Every position the leaving node was serving is UNSERVED from the
instant `Close()` closes its mounts until the survivors flip - the same
mount-time gap Phase 2e removed for scale-up, reopened for scale-down. The
serving + `drainCheck` + forward path that would have held availability are
torn down before they could run.

THE FIX (reuse the overlap machinery; add a drain-wait on shutdown). Three
pieces:

1. **Split membership `Leave()` from `Close()`.** memberlist has `Leave()`
   (broadcast the graceful departure) DISTINCT from `Shutdown()` (tear down
   the transport). The wrapper's `Close()` does both today. Add a membership
   `Leave()` that broadcasts WITHOUT shutting the transport down, so the node
   keeps serving gRPC and keeps being FORWARDED-TO during the drain;
   `Shutdown()`/`Close()` of the transport stays in the existing teardown.

2. **`Cluster.DrainForLeave(ctx)` (a.k.a. `GracefulLeave(timeout)`).**
   Broadcast the graceful leave via the new membership `Leave()` (peers
   re-own this node's units + start forwarding to it), then BLOCK until the
   `handoffPhase` map holds no `Draining` entry owned by this node (every
   position `drainCheck` released; `mountMap` carries no position this node
   still owns) OR `ctx` cancels / the timeout fires. The reconcile loop,
   serving, `drainCheck`, and the forward path STAY ALIVE during this wait.
   Each `Draining` position releases on the SAME rule as any overlap drain (a
   successor's serving marker at an epoch strictly above this node's open
   epoch). Then return.

3. **Wire it at the TOP of `Close()`, gated.** A config field
   `GracefulLeaveDrainTimeout time.Duration`: `0` = disabled = today's
   behavior (the gap remains; also the break-demo state). When `> 0` AND
   `multiReplicated()`, `Close()` calls `DrainForLeave(timeout)` FIRST -
   before ANY teardown, while the loops are still running - then proceeds
   with the existing teardown unchanged. Putting the drain at the top of
   `Close()` keeps it self-contained: the SIGTERM handler in `runtime.go`
   just calls `Close()` as today, no run-loop change.

DRAINING vs THE RESIDUAL. A position the leaving node owns is taken over by a
SURVIVING node - a single-hop move (the leaving node held rP of unit K; a
survivor now holds rP of K) - which the overlap machine resolves to
`Draining` on the leaving side. So in the common case EVERY served position
goes `Draining` and is covered by the drain wait. Two residuals are NOT
covered (and do not need to be):

- A position hitting the PLAIN clean-cut RELEASE path (overlap-move vs
  plain-release, below) - the leaving node simply drops out of a unit's
  replica set and some OTHER already-mounted replica covers W with no
  successor taking its exact position. Nothing to drain; the eager release is
  correct and the surviving replicas keep the position available. This is NOT
  a gap, just outside the drain machinery.
- A position whose successor is STUCK (its `Acquiring` mount never completes
  within the grace budget): the drain wait times out, `Close()` proceeds, and
  that one position is unserved from teardown until the survivor finally
  mounts - exactly today's gap, for that position only, bounded by the grace
  budget. No worse than the disabled (timeout 0) behavior.

THE INVARIANT. For a position with a REACHABLE single-hop successor, a
graceful leave has NO unserved window: the leaving node keeps serving
(directly + via the survivor's forward) until the successor is `Ready` and
`drainCheck` releases, and only then closes. No-acked-write-lost and
single-writer-fence are unchanged (the drain reuses the exact overlap release
rule; the leaving node never releases a position before its successor is
provably serving).

OPERATOR CONCERN. The orchestrator's termination grace period MUST exceed
`GracefulLeaveDrainTimeout`, or the orchestrator SIGKILLs the process
mid-drain and reopens the gap. On a k8s StatefulSet that is
`terminationGracePeriodSeconds`, set STRICTLY GREATER than the drain timeout
(with headroom for the leave broadcast to gossip and the post-drain teardown
to finish). The code enforces only its own timeout, not the orchestrator's.

ACCEPTANCE (break-demo). A continuous writer through a graceful one-node
leave asserts ~100% ack + zero unserved window; the paired break demo sets
`GracefulLeaveDrainTimeout = 0` (drain disabled) and the same leave shows the
gap (ack rate drops while the just-closed positions are unserved until the
survivors mount), proving the drain wait holds availability. Reuses the
slow-`OpenReplicaUnit` injection from the overlap availability gate so the
hand-off takes a measurable, mount-dominated interval.

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
  per-replica durable-manifest fence; the durable fence epoch is a LIVENESS
  HINT only (strictly weaker than the serving marker and NEVER a release
  trigger), and the open-time fence is the same acquire fence as before.

## What DOES change (explicit, since the original "nothing routing-side
changes" claim is now false)

- `mountMap` / `replicaPos` re-keyed to `ReplicaUnit` (and `replicaPos`
  folded into the key), rippling through every mount reader.
- The NEW owner gains an `Acquiring`-state behavior: forward routed ops to
  the recorded predecessor (read AND write) via the POSITION-ADDRESSED
  forward, stop on `Ready`.
- The forwarded-op wire gains a `ReplicaUnit` field (NEW-P0) so the
  predecessor serves the draining position by explicit ru, and the
  predecessor's forwarded-op handler resolves `mountMap[ru]` directly for a
  position-addressed forward. This is a real proto change.
- A new `priorDesiredReplicas` snapshot (the multi analogue of legacy
  `lastEvalRing`), captured at the END of each reconcile and consulted ONLY
  to identify the predecessor. Overlap is SINGLE-HOP: an ambiguous/multi-hop
  predecessor records no predecessor and degrades to Option A.
- The OLD owner's release trigger is "new owner Ready," detected POLL-ONLY
  by reading the durable SERVING MARKER (a marker at an epoch STRICTLY ABOVE
  (`>`) the old owner's open epoch; strict `>` rejects the old owner's own
  stale gain-marker at exactly its open epoch), NOT a bare durable
  FENCE-epoch advance and NOT a push
  RPC. The marker read is a point-in-time liveness OBSERVATION (NEW-P1-3): a
  NEW crash in the read-to-release gap leaves the position unserved until the
  next reconcile, with no acked-write loss.
- A new `WriteServingMarker` / `ReadServingMarker` pair on the
  `ReplicaBackendFactory` seam (in-memory on `sharedfactory`, a small
  durable per-`ru` object on the slate factory). This is the ONLY new
  cross-node signal; there is NO `ReplicaHandoffReady` RPC and NO
  readiness-probe RPC.
- The intra-`OpenReplicaUnit` ordering is pinned (NEW-P1-4): the fence must
  be effective no later than the WAL-recovery cutoff, and the pin test is
  widened to a write-through-the-forward-CONCURRENT-with-the-flip case.
- A new `handoffPhase` map.
