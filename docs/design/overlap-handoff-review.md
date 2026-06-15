# Adversarial review: Option B overlap handoff

Status: REVIEW of the DESIGN in `docs/design/overlap-handoff.md` + SPEC
"v0.8 Phase 2e". No code exists yet. This is the cheapest place to catch a
correctness hole, so the bar is "find the hole," not "rubber-stamp."

Verdict: the CUT (epoch fence) reasoning is sound: the durable-manifest
writer-epoch is a real linearization point and the "no acked write
invisible to the new owner" argument holds. BUT the design as written does
NOT actually deliver availability through the overlap, and has one
genuine data-availability gap, because of how the EXISTING routing and
mount-map machinery work. Two P0 findings below are correctness/behavior
holes that would make the implementation either not compile cleanly into
the current model or silently fail to keep writes available. The rest are
P1/P2 underspecifications that would force an implementer to guess
(= future spaghetti).

---

## P0-1 (CORRECTNESS / AVAILABILITY HOLE): the write fan-out never routes to the OLD owner once the ring converges, so "old owner keeps serving" does not keep writes available

This is the central hole. The whole premise of Option B is "the OLD owner
keeps serving the position until the NEW owner is ready." But in this
codebase the OLD owner is only ever ASKED to serve if it is in the replica
set the write fan-out dispatches to, and that set is recomputed from the
LIVE (already-converged) ring on every write.

Evidence:

- `putReplicatedUnitAttempt` (`pkg/cluster/multibackend_replicated.go:373`)
  computes `replicas := c.replicasForKey(key)` PER WRITE.
- `replicasForKey` -> `unitReplicas` -> `c.ring.LocateKeyN(genUnitBytes(gu), R)`
  (`multibackend_replicated.go:68-80`) returns the replica set off the LIVE
  ring.
- The fan-out (`fanout(ctx, replicas, w, ...)`) only dispatches to members
  of THAT set. `getReplicatedUnit` (`:417`) does the same for reads.

Once membership has converged to the new topology, the moving position's
replica set is `{..., NEW}` and NO LONGER contains OLD. So:

- The originator never sends the write to OLD. OLD's "still mounted, still
  serving" backend is never contacted for this position.
- The write fan-out targets NEW. NEW is in `Acquiring`, has no mountMap
  entry, returns `errUnitAcquiring`. That is exactly the Option-A acquiring
  window.

So under the design as written, during the entire `Acquiring` window the
ONLY thing keeping the write available is Option A's retry, bounded by
`WriteTimeout`, which is precisely the bound Phase 2e claims to remove. The
overlap mount on OLD is dead weight: nobody routes to it. The design's
sentence "writes routed to rP land on OLD (still mounted), get acked
normally" (overlap-handoff.md:111-113) is FALSE in this codebase once the
ring has converged, because routing is ring-derived, not mount-derived.

Why the design author missed it: the design implicitly assumes routing
follows the MOUNT MAP ("THE mountMap INSERT IS THE FLIP", SPEC:1237). But
the mount map only governs the LOCAL-SELF leg AFTER the ring has already
selected this node as a replica. The ring is what selects WHICH nodes are
asked at all, and the ring flips at convergence, BEFORE the new owner is
ready. There are two different "flips" and the design conflates them:

1. The RING flip (which nodes are in the replica set): happens at
   membership convergence, before any mount. This is what actually steers
   the write fan-out.
2. The MOUNT flip (does the resolved node have the backend open): happens
   at the new owner's mountMap insert. This only matters once the ring has
   already routed to that node.

Option B's correctness argument is about flip (2), but availability is
governed by flip (1), which Option B does not touch. The overlap window
only helps if the write fan-out is told to ALSO contact OLD during the
transition.

REQUIRED AMENDMENT. The design must make the write/read fan-out address the
UNION of the old and new replica sets for a position that is mid-transition,
so the in-flight write is dispatched to BOTH the still-serving OLD owner and
the acquiring NEW owner, and W is satisfied by whichever is ready (OLD,
during Acquiring; NEW, after the flip). Concretely one of:

- (a) An OVERLAP-AWARE replica-set resolver: while a `GenUnit` has an
  in-flight handoff phase recorded, `replicasForKey` returns the union of
  the pre-change and post-change replica sets for that unit (the "expanded
  view"), and W is computed against the STABLE replica count (R), not the
  expanded count, so the expansion adds a fallback target without raising
  the ack bar. This is the standard "read/write to old+new during a
  range-move" pattern (Spanner/Bigtable move, Cassandra pending-ranges).
  It requires the design to define: how the originator learns the
  pre-change set (it must remember the ring shape before convergence, or
  derive old+new from the membership delta), and the exact W math under the
  expanded set.

- (b) OLD-owner FORWARDING: the NEW owner, while `Acquiring`, does not
  return `errUnitAcquiring` but instead FORWARDS the routed op to the OLD
  owner (whom it knows from the ring delta) until its own mount completes.
  This keeps the routing surface unchanged (fan-out still targets the new
  set) but makes the not-yet-ready new replica transparently proxy to the
  draining old one. This is arguably cleaner (no expanded-set W math) but
  introduces a forward hop and needs the new owner to know the old owner's
  address.

Either way this is a SUBSTANTIVE addition the current design omits
entirely, and without it Phase 2e does not beat Phase 2d. The acceptance
gate (a slow `OpenReplicaUnit` + assert ~100% ack) would FAIL on a faithful
implementation of the design as written, because the slow mount keeps NEW
in `Acquiring` and nothing routes to OLD. (If the gate passes, it is only
because Option A's retry is masking the gap, which means the gate is not
actually testing Option B: the break-demo must force Option-A retry OFF as
well as overlap off to be honest.)

## P0-2 (MODEL HOLE): `mountMap`/`replicaPos` hold ONE entry per `GenUnit`, but overlap requires the OLD owner to keep a position mounted that has moved; and on a SINGLE node that both loses position p and gains position q for the same unit, the maps cannot represent both

The mount map is `map[GenUnit]backend.Backend` and the position map is
`map[GenUnit]uint8` (`cluster.go:322,332`). There is exactly ONE mounted
backend and ONE position per unit per node. The design says the phase map
is "keyed by `GenUnit`, sibling of `replicaPos`" (SPEC:1233) and that the
old owner "does NOT delete the mountMap entry; it KEEPS SERVING"
(SPEC:1239).

Two distinct problems fall out of the single-entry-per-GenUnit model:

(a) THE OLD OWNER'S POSITION VS THE STEADY-STATE POSITION. When position p
of unit K moves OLD -> NEW, on OLD the entry `mountMap[K]` currently holds
K at position p. OLD must keep serving p (good, the entry stays). But the
local-self read/write leg resolves `localBackendForKey -> mountMap[gu]`
with NO position check: it serves whatever position is mounted. After the
ring converges, OLD is no longer a replica of K at all (or is a replica at
a DIFFERENT position). If OLD is no longer ANY replica of K, then per P0-1
nobody routes to it anyway. But if OLD is STILL a replica of K at a
different position p' (e.g. K's replica set was {OLD@0, X@1} and becomes
{X@0, OLD@1, NEW@2} so OLD moves 0 -> 1 while NEW takes a new slot), the
single `replicaPos[K]` cannot simultaneously say "I still hold p=0 draining"
and "I now desire p=1." The design's claim that a position's state is
"phase value + mountMap presence" breaks: mountMap presence is per-UNIT,
not per-POSITION, so it cannot encode "draining position 0 while acquiring
position 1 of the same unit."

(b) A NODE THAT IS BOTH OLD AND NEW FOR ONE UNIT. The reconcile diff in
`reconcileReplicaUnits` (`:224`) already handles "position changed" by
RELEASE-then-ACQUIRE keyed on `GenUnit`. Replacing that with the overlap
machine has to keep BOTH the old-position backend (draining) and the
new-position backend (acquiring/ready) open for the SAME GenUnit at once.
The maps as typed cannot hold two backends for one GenUnit. This is not a
corner case: any reshuffle where a node's position within a unit's replica
list shifts (very common when R>1 and the ring gains/loses a member that
sorts between existing owners) hits it.

REQUIRED AMENDMENT. The design must state the keying change explicitly:
either (i) re-key `mountMap`, `replicaPos`, and the new phase map by
`ReplicaUnit` (the `(GenUnit, position)` pair) so old and new positions of
one unit are independent entries (this is the honest model and matches the
durable identity, which is already `ReplicaUnit`), OR (ii) restrict Phase
2e's scope to the case where a node is EITHER losing OR gaining a unit, never
both, and prove the reconcile never produces a same-unit position shuffle
(it does, so this is unlikely to hold). Option (i) is almost certainly
right and it is a non-trivial refactor that ripples through
`localBackendForKey`, `localWriteBackendForKey`, `evictStaleMount`,
`mountedBackends`, and every mountMap reader. The design currently hand-waves
this as "sibling of replicaPos" and the SPEC asserts the single-entry model
is sufficient; it is not. Calling it out now prevents the implementer from
discovering mid-build that the map type is wrong and bolting on a parallel
"draining map" (the exact scattered-flags spaghetti the user forbade).

## P1-1 (UNDERSPECIFIED, RELEASE SAFETY): the old owner's release signal `DurableEpochReplica(ru) > myOpenEpoch` can fire on a THIRD party's acquire, releasing while no live owner is ready

The release-check is "durable epoch crossed my open epoch" (overlap-handoff
md:80, SPEC:1245). The durable epoch is a global monotonic on the manifest.
It is bumped by ANY open at a higher epoch, not specifically by "the NEW
owner the ring picked." Construct: position p moves OLD -> NEW. NEW starts
acquiring, bumps the durable epoch to E, then CRASHES before inserting its
mountMap entry (before the flip). OLD's release-check sees
`DurableEpochReplica == E > myOpenEpoch` and RELEASES (case 3 in the
design, overlap-handoff.md:143-150, says OLD stays mounted-but-fenced and
is torn down on its next fenced write). So the moment the durable epoch
crosses, OLD considers itself releasable EVEN IF the new owner never
reached `Ready`. Combined with P0-1's fix (if OLD is the fallback target),
this reopens the unserved window: OLD has released, NEW has crashed
pre-flip, position is down until the next reconcile re-acquires. The design
says case 1 (NEW crashes mid-acquire) keeps OLD `Draining` and serving "the
durable epoch never advances" - but that is only true if NEW crashes BEFORE
fencing. NEW fences (bumps durable epoch) and THEN does the slow part is the
wrong order to assume; `OpenReplicaUnit` fences (`fenceEpochReplica`,
factory.go:613) and then `openSlateReplica` mounts, and the manifest bump is
durable the instant the fence is written, which is BEFORE the mount finishes
and well before `Ready`. So a NEW crash AFTER fence BEFORE Ready advances the
durable epoch, fires OLD's release, and leaves nobody serving. This is a real
correctness regression vs the design's stated case-1 guarantee.

REQUIRED AMENDMENT. The release signal must be "the new owner is READY,"
not "the durable epoch advanced." The durable epoch advancing proves only
that SOMEONE fenced, not that a live writer is serving. The fast-path RPC
`ReplicaHandoffReady` is actually the CORRECT ground truth (it is sent only
after the mountMap insert, i.e. after the flip); the durable-epoch poll is
the UNSAFE fallback, not the safe one. The design has the priority
inverted: it calls the RPC "fast-path / latency-only" and the durable-epoch
poll "the GROUND TRUTH" (SPEC:1245). For SAFETY the relationship is the
reverse. The fix: either (a) make the RPC the authoritative release trigger
and the durable-epoch poll only a LIVENESS hint that triggers a re-VERIFY
(OLD, on seeing the epoch cross, actively probes whether a live owner is
serving p before releasing, e.g. via a readiness RPC to the new replica
set), or (b) keep OLD serving until the ring no longer lists OLD as a
replica AND a positive readiness confirmation arrives, never releasing on a
bare epoch advance. The current "epoch crossed => release" is the hole.

## P1-2 (UNDERSPECIFIED, CONSISTENT-MOUNT TAIL): "durable-before-ack" guarantees acked writes are durable, but the NEW owner mounting WHILE OLD writes can still miss an acked write if OLD's ack and NEW's manifest snapshot race on slatedb's WAL visibility

The seal argument (SPEC:1248) rests on: AwaitDurable=true means an acked
write is in the durable WAL/SST before its ack, and the new owner opening
the same prefix reads that durable state. This is correct for writes acked
BEFORE the new owner reads the manifest. The subtle case the design dismisses
too quickly: the new owner's `fenceEpochReplica` reads the manifest at some
instant T_fence and opens above it. An OLD-owner write whose
durable-before-ack completes at T_write where T_write < T_fence is durable
in the WAL but may NOT yet be reflected in the MANIFEST the new owner read
(slatedb's WAL is durable independently of manifest flushes; the manifest
points at SSTs + a WAL id, and a freshly-durable WAL entry is recovered on
open via WAL replay, not via the manifest). The design needs to assert that
the new owner's open performs WAL RECOVERY over the full durable WAL tail,
not merely loads the manifest snapshot, so a write durable-but-not-yet-
manifested by OLD is still recovered by NEW. This is almost certainly true
of slatedb (open replays the WAL), but the SPEC's seal argument only invokes
"reads that durable state" and "the manifest writer-epoch," never WAL
replay. An implementer reading the SPEC could wrongly conclude a manifest
snapshot is sufficient and a write in OLD's not-yet-checkpointed WAL is
lost. Make the WAL-replay-on-open assumption EXPLICIT and cite where slatedb
guarantees it; if it does not, this is a P0 lost-write.

Related: the design says OLD's writes past the fence "fail with
CloseReasonFenced" so no acked-past-fence write exists. But there is a
window between NEW writing the bumped epoch to the manifest (T_fence) and
OLD's NEXT write attempt observing the fence. slatedb detects the fence on
the old writer's next manifest-touching operation, not instantaneously. A
write OLD started before T_fence and acks just after T_fence: is it fenced
or acked? The SPEC asserts "a write the old owner acks AFTER the new owner
has fenced is impossible" - this needs the fence-detection granularity
spelled out (slatedb fences on WAL/manifest write, and AwaitDurable forces
that write before ack, so the fenced write cannot ack: state this chain
explicitly rather than asserting the conclusion).

## P1-3 (UNDERSPECIFIED, LOCK ORDER / CONCURRENCY): `drainCheck` reads `DurableEpochReplica` (object-store I/O) and mutates mountMap; the lock discipline and where it runs are not pinned

`drainCheck` (overlap-handoff.md:219) "reads `DurableEpochReplica(ru)`" (a
MinIO round-trip) and then "compare-and-delete the mountMap entry ...
`CloseReplicaUnit`." The design must state:

- It MUST NOT hold `mountMu` (or `reconcileMu`) across the
  `DurableEpochReplica` I/O, or a slow MinIO read blocks every routed op's
  mountMap read. Where does the I/O happen relative to the locks?
- It is "armed as a bounded periodic re-check on the existing settle /
  self-heal cadence and woken early by the RPC." Two wakeups (poll + RPC)
  racing into the same `Draining -> Releasing` edge must be exactly-once.
  The SPEC says "the phase machine forbids a second `Releasing` edge"
  (SPEC:1260) but the phase map mutation + the `CloseReplicaUnit` are two
  steps; the CAS-on-mountMap-delete is the real exactly-once guard, and the
  phase transition must be CAS'd under the same lock as the delete. Specify
  that the phase compare-and-advance and the mountMap compare-and-delete are
  ONE critical section under `mountMu`, with the I/O done BEFORE entering it.
- Lock order: this runs off the self-heal / settle cadence which already
  takes `reshardMu -> reconcileMu`. `drainCheck` mutating mountMap from a
  DIFFERENT goroutine (the RPC handler wake) must respect the same order or
  state the it only touches `mountMu` + the phase map and never the
  reconcile locks.

Left unspecified, an implementer will either serialize everything (slow) or
get the lock order wrong (deadlock or a torn flip).

## P1-4 (CRASH CASE GAP): OLD crashes while Draining is NOT bounded by "the other replicas serve" when R=2 and W=2 (write-all)

Design case (b) (SPEC:1256) says OLD crashing while Draining is fine because
"at R>1 the other replicas still serve toward W." That holds for W=1 or
quorum at R>=3. At R=2 with `WriteConsistency` requiring 2 acks (write-all,
a legitimate config), losing OLD mid-Draining drops the available replica
count to 1 (NEW, still Acquiring) and the write CANNOT reach W=2 until NEW
finishes its slow mount. So for R=2/W=2, OLD crashing mid-handoff degrades
to exactly the Option-A mount-bounded window the phase claims to remove, and
the SPEC's "bounded by the new owner's mount, and only when the churn also
kills the old owner" caveat understates it: at R=2/W=2 it is not a rare
double-fault, it is any single OLD crash during any handoff. State the
R/W combinations for which the crash-safety guarantee is "fully available"
vs "degrades to mount-bounded," rather than implying it is always the
former.

## P2-1 (CLARITY): "the FLIP" is used for two different events

As noted in P0-1, "flip" names both the ring-replica-set change and the
mountMap insert. The doc + SPEC should rename one. Suggest: "ring rotation"
(membership convergence changes the replica set) vs "mount flip" (the new
owner inserts its mountMap entry). The correctness argument is about the
mount flip; the availability argument needs the ring rotation handled
(P0-1). Disambiguating the term is what surfaces P0-1 for the reader.

## P2-2 (CLARITY): the pure transition functions are underspecified for the two-sided machine

`handoff.go` proposes `CanRelease`, `NextOnReady`, `NextOnDrain`
(overlap-handoff.md:195-198). But the machine is TWO state machines (gainer
Acquiring/Ready, loser Draining/Releasing) that interact only through the
durable epoch. The pure types should make explicit which phases are legal
on which SIDE (a node cannot be both Acquiring and Draining the same
position - or CAN it, per P0-2's same-unit-shuffle case?). If P0-2 is
resolved by re-keying on `ReplicaUnit`, then a node CAN be Draining
position p and Acquiring position q of one unit simultaneously, and the pure
type must be keyed by `ReplicaUnit` too, not `GenUnit`. The
`HandoffState{Phase, Pos, OpenEpoch}` keyed by `GenUnit`
(overlap-handoff.md:191) is therefore wrong for the same reason the mount
map keying is wrong. Pin the key type to `ReplicaUnit`.

## P2-3 (CLARITY): exactly-once release vs the existing eager-delete release path

`releaseReplicaUnit` (`multibackend_replicated.go:282`) currently deletes
the mountMap entry BEFORE `CloseReplicaUnit` and is called from the reconcile
diff. Phase 2e replaces the position-change branch but the SPEC does not say
what happens to `releaseReplicaUnit` for the "unit no longer desired AT ALL"
case (this node leaves the replica set entirely, not a position shuffle).
Is that still an eager clean-cut release (fine, nobody routes to it after
the ring rotation per P0-1), or does it also go through Draining? Spell out
which removals use the overlap machine (a position MOVING to a specific new
owner) vs a plain release (this node simply drops out of the set and some
OTHER already-mounted replica covers W). Conflating them is where a stray
flag would creep in.

---

## What is genuinely sound (justified against the attack list)

- THE CUT IS A REAL LINEARIZATION POINT. The durable manifest writer-epoch
  is monotonic and the fence-on-open semantics are exactly slatedb's
  single-writer protocol (factory.go:578-627). At most one writer holds the
  position at the highest epoch; everyone else fails `CloseReasonFenced`.
  The "no double-apply" argument (SPEC:1254) is correct GIVEN the routing
  resolves to exactly one ready writer, and apply-if-newer makes any raced
  replay a no-op. This part needs no change.

- NO ACKED-WRITE-LOST ACROSS THE FENCE (modulo P1-2's WAL-replay caveat).
  AwaitDurable=true + open-reads-durable-state is the right backbone. The
  argument that the new owner needs only ACKED writes (durable before the
  fence) and not OLD's in-flight un-acked writes is correct.

- THE STATE-MACHINE-OVER-FLAGS INSTINCT IS RIGHT. One explicit phase per
  side beats `isDraining`/`hasFlipped` booleans. The cleanliness goal is
  achievable; P0-2 and P2-2 just require the key type to be `ReplicaUnit`,
  not `GenUnit`, for the machine to actually represent the states that occur.

The design's core idea (overlap the two ownerships, fence on the durable
epoch, flip routing only when the new owner is ready) is correct and worth
building. The holes are: (P0-1) it never wires the OLD owner INTO the live
routing during overlap, so the overlap mount is unreachable and availability
still bottoms out on Option A; (P0-2) the mount/phase maps cannot represent
the overlapping positions because they are keyed per-unit not per-position;
(P1-1) the release trigger fires on a bare epoch advance, which a crashed-
mid-acquire new owner satisfies without ever serving. Fix those three and
the design delivers what it claims.
