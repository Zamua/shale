# Pending ranges (graceful membership transition) for R>1 multi-backend

Status: DESIGN (rebuilt on the PENDING-RANGES model). This is the design
companion to the SPEC section "v0.8 Phase 2e: pending ranges". The SPEC is
the source of truth for behavior; this doc carries the current/pending/union
computation, the handoff sequence, the package/file layout, and the R/W
crash matrix. No pending-ranges code has landed yet.

The file name `overlap-handoff.md` is kept for continuity, but the design is
no longer the per-position OVERLAP/FORWARDING model. The prior forwarding
design (the OLD owner excluded from the ring + the NEW owner forwarding
routed ops back to it per position) is SUPERSEDED. Its two adversarial
reviews remain as history (`docs/design/overlap-handoff-review.md`,
`docs/design/overlap-handoff-rereview.md`); the holes they found are closed
DIRECTLY by pending ranges (see "Resolved review findings" below), without
the forwarding machinery the re-review then had to patch.

## Why this rebuild

The prior-art survey (`docs/research/graceful-scaledown-prior-art.md`, 24
sources, adversarially verified) identifies the canonical fix for a graceful
membership transition in a consistent-hash + gossip store (Cassandra,
ScyllaDB pre-Raft, Riak/Dynamo - shale's family): PENDING RANGES. The
coordinator DUAL-WRITES a position to BOTH its current owners AND its new
(pending) owners, and READS from their UNION during a transition; the leaving
node STAYS in the read+write set until the handoff completes, removed only
after (Cassandra CEP-21 orders it: add new owners to writes -> stream -> add
to reads + drop leaver from reads -> drop leaver from writes). Writes always
go to ALL replicas; the consistency level controls only the ACK COUNT, not
which replicas receive the write.

The forwarding draft did the OPPOSITE and it is the fragile path: the
draining-state EXCLUDED the leaver from the ownership ring immediately and
relied on per-position FORWARDING from the new owner back to the old. That
produced an unstable convergence window. Write availability held at ~99.7%
via forwarding, but post-leave READBACK failed: the new owner was
owned-but-unmounted and the read routed to it, while the stale mount sat on a
third node. Pending ranges fixes BOTH read and write because the routed UNION
always contains a node that PHYSICALLY HAS the data - the leaver stays a
current owner (mounted, serving) until its successor is provably serving.

Shale already has the pieces pending ranges needs:

- The SERVING MARKER - a durable, poll-observable, object-store record that a
  new owner writes the instant it has MOUNTED a position. This is exactly the
  "the new owner has the data now" gate that CEP-21 / ScyllaDB implement with
  a linearized topology epoch. Shale gets it without consensus.
- The DRAINING Meta bit (gossiped node-state) - the signal that a node is
  leaving, the input to the current/pending split.
- Shared object storage - "streaming" is a near-zero-cost MOUNT, so the
  transition window is tiny.
- slatedb / object-store CAS fencing for single-writer safety per replica.
- The per-`(unit, replica)` mount map (re-keyed by `ReplicaUnit`).

## The one-paragraph design

During a graceful membership transition (a node `Draining`, or a recently
joined node not yet confirmed-mounted for a position), `replicasForKey`
routes a position to the UNION of its CURRENT replica set (over the ring
INCLUDING draining members) and its PENDING replica set (over the ring
EXCLUDING draining members). The fan-out DUAL-WRITES every union member; the
ack bar W is held at the STABLE R quorum (the pending replica is a bonus
write target, not a higher bar). Reads fan out across the union and any
member that physically has the position mounted serves it. Pending owners
mount the position in the background via the normal reconcile and write a
SERVING MARKER on mount-complete. ORDERED REMOVAL drops the leaver from a
position's routed set - collapsing the union onto the pending set - only once
that position's serving marker is present above the leaver's epoch, and only
then is the leaver fenced/closed. So the leaver is never removed from the
read+write set before a successor physically has the data, and the union
always contains a node that holds it: no unserved window, no lost acked
write, independent of mount time.

## Current / pending / union (the routing contract)

Routing is computed per-op inside `replicasForKey(key)`. Let
`gu = genUnitForKey(key)`.

- **CURRENT** = `unitReplicas(gu)` over the ring INCLUDING draining members
  = `ring.LocateKeyN(genUnitBytes(gu), R)` with NO draining-exclusion. The
  nodes that own the position TODAY and have it mounted (a leaving node is
  here).
- **PENDING** = `unitReplicas(gu)` over the ring EXCLUDING draining members.
  The nodes that will own the position once the leaver is gone (the successor
  taking the leaver's exact position is here).
- **TRANSITION DETECTION**: a position is IN TRANSITION when CURRENT !=
  PENDING. Two gossip-observable causes:
  - LEAVE: some member of CURRENT is `Draining`, so excluding it changes the
    set. The `Draining` bit is the linearization-free analogue of
    Cassandra/ScyllaDB's topology epoch.
  - JOIN: a member recently entered the ring and is a PENDING owner of a
    position whose CURRENT owner has not yet released - detected as "this node
    is a pending owner of a position whose serving marker for that
    `ReplicaUnit` is ABSENT." The new member shifts the `LocateKeyN`
    successor chain, so CURRENT (pre-join owners) != PENDING (post-join
    owners).
- **ROUTED** = the set `replicasForKey` actually fans out to. Stable replica
  set when not in transition (CURRENT == PENDING, R nodes); UNION(CURRENT,
  PENDING) when in transition (at R=2, up to 3 distinct nodes). The union is
  a SUPERSET of every node's possibly-disagreeing ownership opinion during
  gossip lag, so any single-node routing decision still reaches a node that
  physically holds the position. This is the property that fixes the
  forwarding model's post-leave readback failure.

The implementation puts the include/exclude split INSIDE `replicasForKey`,
NOT in the ring: `reconcileRingFromMembership` does NOT exclude draining
members (the prior draft's draining-exclusion is REMOVED). The ring carries
every alive member; the per-op split derives current vs pending from the
`Draining` bits on the live snapshot.

## Dual-write with the ack bar at the stable R

The fan-out dual-writes the position to EVERY routed union member (writes
always go to all routed replicas, exactly as Cassandra sends every write to
all replicas regardless of consistency level). The ack bar
`W = requiredWriteAcks(WriteConsistency, R)` is held at the STABLE quorum over
the configured R; it is NOT raised to cover the transient extra union member.

- At R=2 / `WriteConsistency=Quorum`, `W = floor(R/2)+1 = 2`. A transition
  write fans out to up to 3 union members and acks once ANY 2 of those 3 have
  durably applied it. The leaver, which already holds the data, satisfies W
  instantly while a pending owner is still mounting.
- The extra pending replica is a BONUS target: it lowers latency-to-durability
  and pre-warms the successor with live writes during its mount. It is NOT a
  higher ack bar.
- This is DELIBERATELY DIFFERENT from Cassandra's `blockFor`, which raises the
  required-ack count to cover pending endpoints. Shale holds the bar at the
  stable R because raising it to ALL-3 transiently would make any one slow
  union node halve write availability on an R=2 store, defeating the point
  (the survey's open question (a): the answer is NO, do not raise the bar).
- `WriteConsistency=All` is held at the stable R too: `W = R`, the number of
  STABLE replicas, NOT the transient union size. Widening All to the union would
  block every write on a mid-mount successor for the whole mount window and
  collapse scale-down availability. This departs from Cassandra's CL=ALL (which
  raises blockFor for pending endpoints); the pending owners inherit the data
  through the shared object-storage db on mount, so All's durability holds
  without requiring their in-flight ack. All means all STABLE replicas.

Reads (union reads): a read fans out across the routed union per
`ReadConsistency` (`Nearest` returns on the first answer; `Quorum`/`All` wait
for floor(R/2)+1 / all routed). Any union member that physically has the
position MOUNTED serves it; a pending owner still mid-mount returns the
transient acquiring code (`errUnitAcquiring` -> `codes.ResourceExhausted` on
the replica leg), which the read fan-out SKIPS while another union member
(the leaver) answers. LWW winner selection, read-repair, and tombstone ->
`ErrNotFound` are reused verbatim over the union.

## The serving-marker handoff gate (reused unchanged)

The serving marker is the durable, poll-observable signal that a pending
owner has mounted a position. It is keyed by `ReplicaUnit`, carries the
writer's open epoch E, written EXACTLY ONCE by a node right after it inserts
its `mountMap[ru]` entry. `ReadServingMarker(ru) (epoch, ok)` reads it without
opening the database.

- Release / removal rule: a node drops a position from its routed set (the
  leaver leaves the union) only when it reads a marker at an epoch STRICTLY
  ABOVE (`>`) its own open epoch - positive proof a live SUCCESSOR is serving.
- Strict `>` (not `>=`) rejects a node's OWN stale gain-marker at exactly its
  open epoch: a node that GAINED `ru` at epoch E wrote `WriteServingMarker(ru,
  E)`; if the ring later moves `ru` off it, its open epoch stays E, and a
  `>=` gate would read its own marker and remove prematurely. A genuine
  successor opens at `durable+1 >= E+1` and writes a marker strictly above E.
- A BARE durable fence-epoch advance (`DurableEpochReplica(ru)`, which bumps
  at open-START before the mount completes) NEVER triggers removal - it is
  strictly weaker than the serving marker (a successor that fences then
  crashes mid-mount advanced the fence WITHOUT serving and WITHOUT writing a
  marker). The marker is the only positive "live owner serving" confirmation.
- The marker read is a POINT-IN-TIME liveness observation, not a lease: if the
  successor crashes in the gap between the leaver reading the marker and
  completing its removal, the position is unserved until the next reconcile,
  with NO acked-write loss (durable-before-ack). The marker closes the
  bare-fence hole, not the read-to-removal gap; the gap's recovery is the
  benign next reconcile.

This is the survey's open question (b) answered: the AUTHORITATIVE signal that
advances the leaver out of the read+write set is the durable serving marker
over shared storage. It cannot race in a way that loses data because it is
durable (not a best-effort push), monotonic, and gated by a strict epoch
comparison; the only race is a crash in the read-to-removal gap, which is
benign.

## Author-attributed serving markers (the routed-successor release gate)

The epoch-only marker above is AUTHOR-ANONYMOUS: it proves that SOMEBODY is
serving a position at epoch E, and says nothing about WHO. That is sufficient
only while every node agrees on who a position's successors are, and during a
transition they do not.

The reason is that the two transition bits are INDEPENDENT gossip facts. A node
can observe a joiner's `Joining` bit before a leaver's `Draining` bit, and in
that STALE view it computes `current` = ring-minus-joiner (excluding the
newcomer) and `pending` = ring-minus-nobody (the leaver still included). For a
unit whose ENTIRE replica set turns over at once - a FULL MOVE - the resulting
routed union can contain NONE of the true post-transition owners. The node then:

1. sees a position it holds as current-but-not-pending and arms a drain,
2. observes the true successor's anonymous marker above its own open epoch, and
3. releases its last local copy.

Its routed union now names only nodes that never held the unit. Every leg of a
read answers transiently, the all-legs-transient retry spins to `ReadTimeout`,
and the client sees a `DeadlineExceeded` `Get` (or the retryable "unit for key
is handing off" `ScanPrefix`). It is an AVAILABILITY defect, bounded to reads
and scans through nodes whose view is stale, for as long as the view stays
stale. No ACKED WRITE can be lost by it: the ack bar never falls below R (the
quorum floor in the current-set computation), so a write that cannot reach its
bar returns a retryable error rather than acking.

THE RULE. A marker additionally carries its AUTHOR (the writing node's ID), and
a draining owner releases only when that author is a node it ROUTES the position
to. It never surrenders its copy to a successor invisible to its own readers:
either the author is routed, or the release waits for the view to converge -
which is precisely when the true successors enter the union. The strict `>` epoch
comparison above is unchanged and still required; the author check is an
ADDITIONAL conjunct, never a replacement.

### SCOPE: this rule does NOT by itself close the availability hole

Measured, and important to state plainly so the rule is not mistaken for a fix it
is not. Under production fence semantics the successor's `OpenReplicaUnit` bumps
the position's durable epoch at OPEN-START (real slatedb's `DbBuilder.Build`
timing), which FENCES the predecessor immediately - and the serving marker is
written only AFTER the successor's mount completes. So by the time the
predecessor can observe any marker, its own handle has already been unable to
serve for the entire duration of the successor's open. Holding the release
therefore preserves a handle that CANNOT ANSWER, and a read through the stale
node fails whether it holds or releases.

The consequence is that the full-move hole is a ROUTING defect, not a
release-timing one:

- it OPENS at the successor's open-START, EARLIER than any release, and
- the nodes that hold the data are simply ABSENT from the stale view's routed
  union, so no release-side gate can make them reachable.

Closing it requires making the true holder REACHABLE from the stale node. The
author attribution is exactly the datum that makes that possible - the marker
names a node that PROVABLY SERVES the position, which is precisely the routing
information the stale view lacks - but consuming it as a ROUTING HINT (adding the
marker's author to the routed set for that position, as a BONUS target that does
NOT change `stableR` or the write ack bar) is a separate change to the hot path
and is not part of this rule. `TestOverlap_StaleView_HeldCopyIsFencedBySuccessor_
ReadStillFails` pins the measured behavior and should FLIP to asserting success
when that routing change lands.

What the rule DOES give: a node never destroys its last local copy on the word of
a successor it cannot see, which is the safety property the anonymous marker was
missing, and the attribution machinery the routing fix needs.

The rule applies at BOTH release gates, not just the drain poll:

- `drainCheck`, the displaced (join-direction) owner's release, and
- the graceful-leave COMPLETION gate, because `Close` tears the mount down once
  that gate reports true. A leave that completed on an unrouted author would
  destroy the last routable copy even while `drainCheck` was correctly holding
  it.

For a GENUINE leaver the check is satisfied by construction: its own view has
`draining = {self}`, so its pending set IS the true post-leave placement and the
successor writing the marker is in it. The rule bites only the joiner-displaced
case, which is the defect.

TWO ESCAPE HATCHES, both mandatory:

- LIVENESS BACKSTOP. The hold is BOUNDED. An unbounded hold would be worse than
  the hole it closes - it could wedge a graceful leave indefinitely. Past the
  budget the node logs loudly (naming the position and the unrouted author) and
  releases, degrading to exactly the pre-attribution behavior for that one
  position. The budget only has to outlive gossip convergence, and is well under
  `GracefulLeaveDrainTimeout` so it can never be what wedges a leave.
- ROLLING-UPGRADE COMPAT. An UNKNOWN author (`""`) falls back to the epoch-only
  rule IMMEDIATELY. Unknown covers a marker written before attribution existed, a
  marker written by a node that lacks it, and a factory without the capability. A
  new node that held on every author-less marker would wedge against every old
  node in a mixed-version fleet.

The attribution is an OPTIONAL factory capability
(`storageunit.AuthoredMarkerFactory`), not a widening of the marker contract, and
the epoch record keeps its exact prior representation on disk. An OLD node
therefore reads a NEW node's marker unchanged, and a NEW node reading an old
marker gets `""`. Compatibility holds in both directions, so the change is safe
to roll out one node at a time.

## Ordered removal (the CEP-21 ordering on pending ranges)

The transition advances in the canonical order that keeps every replica set
covered:

```
  1. ADD pending owners to the routed set (writes + reads).
     -> the union forms the instant gossip observes the transition;
        pending owners receive every write while they mount.
  2. pending owners STREAM = near-zero-cost MOUNT from shared object storage;
     on mount-complete each writes WriteServingMarker(ru, E).
  3. DROP the leaver from a position's routed set (union collapses onto the
     pending set) ONLY ONCE that position's serving marker is present at an
     epoch strictly above the leaver's open epoch (handoff durably complete).
  4. RELEASE the leaver's position (CloseReplicaUnit(ru)) after it is out of
     the routed set. The leaver is fenced out of writing only after its
     successor is provably serving.
```

`replicasForKey` implements step 3 implicitly: once a position's serving
marker is present above the leaver's epoch, the routing layer stops counting
the leaver as a current owner for that position (the position transitions from
"in transition, union" to "pending set is the stable set"), and the leaver's
`drainCheck` releases its mount. There is no instant where the only durable
copy of an acked write is on a fenced/closed node: at the moment the leaver
releases, the successor is already serving (marker present) and its database
already contains every acked write durable below E.

## Keying by ReplicaUnit (reused; HandoffState simplified)

`mountMap` is `map[ReplicaUnit]backend.Backend`, the separate `replicaPos`
map is folded into the key (`ru.Replica`), and `handoffPhase
map[ReplicaUnit]HandoffState` is a `mountMu`-guarded sibling. Required because
a ring change can shuffle this node's index within a unit's replica list, so
one node can hold the OLD position (draining, still serving union writes) AND
the NEW position (acquiring/ready) of the SAME unit at once. The re-keying
ripples through `localBackendForKey` / `localWriteBackendForKey` /
`evictStaleMount` / `mountedBackends` and the reconcile diff.

`HandoffState` is SIMPLER than the forwarding design: `{Phase HandoffPhase,
OpenEpoch Epoch}`, with NO `Predecessor` / `PredecessorAddr` fields (there is
no forwarding target to remember - the union routes directly to the leaver).
The phases collapse to:

- `PhaseAcquiring` (this node is a pending owner mid-mount; other union
  members cover the position; routed ops to THIS node for the position return
  `errUnitAcquiring`, which the union read/write tolerates).
- `PhaseDraining` -> `PhaseReleasing` (this node is the leaver; it stays a
  routed current owner and serves until the marker gate, then releases).

There is NO `Acquiring`-state forward behavior and no position-addressed
forwarded-op wire change. A pending owner mid-mount simply refuses (the union
covers it).

`localBackendForKey(key)` resolves the position THIS node holds for the key's
unit from the live ring index (this node's index in `unitReplicas(gu)`) and
looks up `mountMap[ReplicaUnit{gu, pos}]`. During a same-unit shuffle both
the draining old position and the acquiring new position can be present; the
resolver picks the one the ring currently assigns this node. No
position-addressed direct lookup is needed (no forward), so the
draining-served path is the node's own ring-routed local read, not a
cross-node ru-addressed handler.

## State diagram

Two halves, each a small phase machine keyed by `ReplicaUnit`. The phase map
holds only IN-FLIGHT positions; absence means steady state (Owned if mounted,
Absent if not).

PENDING owner (the node GAINING the position):

```
  Absent
    | ring assigns rP here under the PENDING set, not yet mounted
    v
  Acquiring  --- OpenReplicaUnit running (slow MinIO mount + fence) ----.
    |  the union covers rP via the still-mounted current owner(s).      |
    |  routed ops landing on THIS node for rP return errUnitAcquiring    |
    |  (the union skips this leg; another union member answers).         |
    |  OpenReplicaUnit returns (mounted + durably fenced at E)           |
    v                                                                    |
  Ready  --- mountMap[ru]=b inserted under mountMu --------------------- '
    |    this node now serves rP LOCALLY (it is a routed union member).
    |    WriteServingMarker(ru, E) to shared storage EXACTLY ONCE.
    v
  Owned   (steady state; phase entry dropped)
```

CURRENT owner that is leaving (the LEAVER, or any node losing rP to a join):

```
  Owned   (mounted + serving)
    | reconcile + the Draining split compute rP as current-not-pending here
    v
  Draining  --- STAY a routed current owner; keep serving rP ----------.
    |   (dual-written via the union; reads served from here).          |
    |   drainCheck POLLS the serving marker on the settle/self-heal     |
    |     cadence (no RPC wake). It fires when ReadServingMarker(ru)     |
    |     returns a marker at epoch > myOpenEpoch (STRICT >).           |
    |   A BARE DurableEpochReplica(ru) advance NEVER removes.          |
    v                                                                   |
  Releasing  --- compare-and-delete mountMap[ru], CloseReplicaUnit(ru) -'
    |    (flush is a no-op for acked writes: durable-before-ack).
    v
  Absent  (phase entry dropped)
```

## Sequence of one graceful leave (happy path)

```
node L sets Draining; for a position rP of unit K owned by L,
the survivor S is the PENDING owner taking L's exact position.

  t0  L sets Draining (membership.SetDraining -> UpdateNode gossips).
      every node recomputes: CURRENT(rP)=[..., L], PENDING(rP)=[..., S],
      so rP is IN TRANSITION; ROUTED(rP) = UNION = {L, S, (other replicas)}.
  t1  S reconcile: rP is a pending position not mounted -> Acquiring;
      OpenReplicaUnit(ru) starts the slow mount.
  t1  L reconcile: rP is current-not-pending -> Draining (NOT released);
      L stays a routed current owner; arms drainCheck (poll the marker).
  t1..t2  TRANSITION: writes DUAL-WRITE the union {L, S, ...}. L (mounted)
      applies + acks instantly; S (mid-mount) returns errUnitAcquiring and is
      skipped; W=2 is met by L + one other current owner. Reads served from L.
      Mount time on S is irrelevant to availability.
  t2  S: OpenReplicaUnit returns at openedEpoch E (E > durable). S inserts
      mountMap[ru] -> Ready; S now serves rP locally AND receives union
      writes into its own copy. S writes WriteServingMarker(ru, E) once.
  t3  L drainCheck (next poll tick): ReadServingMarker(ru) returns E > L's
      open epoch -> L drops rP from its routed set (union collapses onto the
      pending set {S, ...}), compare-and-deletes mountMap[ru],
      CloseReplicaUnit(ru) -> Releasing -> Absent.
  t4  once ALL of L's positions reach t3, DrainForLeave returns; L does the
      real memberlist.Leave() + Shutdown().
  done. No instant between t0 and t3 had rP unserved or under-replicated for
        acked writes; the union always held a mounted copy.
```

## Durability invariant: no acked write lost (the argument)

A write acked at W over the stable R is durable on >= W of the routed
replicas. The argument, made explicit:

1. **Fan-out ack accounting.** A write is acked to the client ONLY after W
   routed replicas durably applied it. Every routed replica opens
   `AwaitDurable=true`, so an ack means durable-before-ack on each. Any of
   those W can be a CURRENT owner OR a PENDING owner; they each hold an
   independent durable copy at `dbNameReplica(ru)` for their replica index.

2. **The leaver is fenced only AFTER its successor is serving** (the marker
   gate + ordered removal). The leaver stays a routed current owner -
   mounted, serving, durably holding every write it acked - until a pending
   owner's serving marker proves the successor physically has the position.
   So there is NEVER an instant where the only durable copy of an acked write
   is on a fenced/closed node: at the moment the leaver releases, the
   successor is already serving and its database already contains every acked
   write durable below E (mounted from the same shared-storage replica OR
   received live via the union during its mount; either way WAL recovery over
   the full durable tail on open recovers them - see the seal).

3. **Idempotency across the union** (the survey's open question (c)). A write
   dual-written to several union members applies the SAME pre-stamped LWW
   envelope to each member's independent replica database. Apply-if-newer
   (`txApplyIfNewer`: write only if the incoming stamp strictly beats the
   stored stamp) makes a re-applied or reordered envelope a silent NO-OP on
   any copy that already has an equal-or-newer stamp. So dual-write cannot
   double-apply, cannot move any copy backward, and a union write that lands
   on a copy the next ring opinion no longer routes to is harmless (an extra
   durable copy). A retry re-dispatches the same stamped envelope to the
   re-resolved live union, idempotent for the same reason. Confirmed: the
   LWW stamp is computed ONCE per logical write (before the first fan-out) and
   reused across union members and retries, so the comparator is stable.

## Single-writer fence stays intact

Dual-write writes to INDEPENDENT per-`(unit, replica)` databases (Phase 2b's
independence invariant), each with one highest-epoch writer; it never puts two
writers on one database. The leaver is the authoritative writer of its replica
copy until it releases; a pending owner is the authoritative writer of ITS
replica copy from epoch E. A stale write that races the leaver's release lands
on its now-fenced handle, fails (`CloseReasonFenced`), is evicted
(`evictStaleMount` -> `errUnitAcquiring`), and is retried onto the live union.

The cut (the fence point) is a single instant on the per-replica durable
manifest: the epoch E at which the pending owner opened. Below E the leaver is
authoritative for its copy; at/above E the new owner is authoritative for its
copy. These are DIFFERENT databases, so "two unfenced writers" never arises;
the fence orders writers WITHIN one replica database across an
ownership-of-that-replica handoff (the legacy fence semantics), and dual-write
across the union touches DISTINCT replica databases.

## The slate manifest seal (consistent mount under dual-write)

A pending owner must mount a view containing every write acked while it was
mounting. Pending ranges makes this CLEANER than the forwarding design: a
write during the transition is dual-written DIRECTLY to the pending owner's
own replica database (not forwarded to the leaver), so once the pending owner
is mounted it receives the write into ITS database with no cross-node seal
needed.

The only seal concern is writes acked on the leaver (or other current owners)
BEFORE the pending owner finished mounting. Those are durable in the OTHER
replica copies, and the pending owner's copy catches up because:

- **Durable-before-ack** on every routed replica (`AwaitDurable=true`).
- **WAL recovery over the full durable tail** on the pending owner's
  `OpenReplicaUnit`: it recovers the full durable WAL tail of its own
  per-replica database, not just the manifest snapshot at fence time. (Same
  P1-2 assumption as before.)
- **Intra-open ordering**: the manifest-epoch FENCE effective NO LATER THAN
  the WAL-recovery cutoff (fence-then-recover), so a write that races the
  mount is either inside the recovered tail or fenced (never an
  acked-but-invisible gap). (Same NEW-P1-4 assumption.)

### EXPLICIT ASSUMPTION (implementation-must-verify, release-gating)

The slatedb-go binding (`slatedb.io/slatedb-go v0.13.1`) is a uniffi FFI over
the Rust core; the Go source does not expose the open/recovery path, so we
CANNOT confirm from the module that `Db` open replays the full durable WAL
tail OR that the fence is effective at-or-before the recovery cutoff. It is
consistent with the binding's independent `WalReader` surface but not proven.

PIN with two tests before Phase 2e ships:

1. (P1-2) a write acked on a current owner JUST BEFORE a pending owner's fence
   (durable in the WAL but possibly not yet manifest-checkpointed) must be
   readable on the pending owner AFTER the handoff.
2. (NEW-P1-4, the WIDENED case) a write DUAL-WRITTEN CONCURRENTLY with the
   pending owner's mount/recovery (acked below E on its own replica copy,
   racing its open/recovery) must be readable on it AFTER the handoff.

If slatedb open does NOT replay the durable WAL tail, OR the fence is not
effective at-or-before the recovery cutoff, this is a P0 lost-write and Phase
2e is blocked until the gap is closed (force a current-owner-side
`Db.Flush`/checkpoint before the pending owner opens, pin the slate
`OpenReplicaUnit` to re-scan the WAL tail after the fence, or another seal
protocol). The acceptance gate's loss oracle covers it in aggregate; the two
targeted tests are the honest pins.

The slate `OpenReplicaUnit` (`backends/slate/factory.go`) TODAY orders
`fenceEpochReplica` (writes the bumped manifest epoch) BEFORE
`openSlateReplica` (opens the db, where the Rust core recovers), which is the
correct fence-then-recover order at the Go level. If the Rust core recovers
before the bumped epoch is effective (FFI-opaque), the slate open's internal
ordering must be pinned/adjusted (re-scan the WAL tail after the fence). Flag
for implementation; do not write that code here.

### Fence-detection chain (why no acked-past-fence write exists)

1. slatedb detects a fence on the OLD writer's next manifest/WAL-touching
   write (its epoch is below the manifest's writer-epoch E), AT the durable
   write, not asynchronously.
2. `AwaitDurable=true` forces that durable write to complete BEFORE the ack
   returns.
3. Therefore a write that WOULD be fenced (`CloseReasonFenced`) CANNOT have
   acked: the rejection is at the durable-write step, strictly before the ack.
   Either the write completed its durable write before E (acked, durable,
   visible to the successor) or it is fenced (never acked). No "acked on old,
   invisible to new" gap.

This holds identically across the union (each replica does its own durable
write + fence detection on its own copy).

## Crash handling (the four cases)

1. A PENDING owner crashes mid-mount (`Acquiring`, before its marker): it
   never wrote the serving marker, so the leaver is never removed from the
   routed set (ordered removal requires the marker) and keeps serving; the
   union still covers the position via the leaver. A bare fence-epoch advance
   from the crashed pending owner's pre-crash fence does NOT remove the leaver.
   The next reconcile reassigns the pending position. No data loss, no
   unavailability of the position (the leaver holds it).

2. The LEAVER crashes while still a routed current owner (before any successor
   is serving): the union loses the only mounted copy for the position until a
   pending owner mounts, degrading THIS position to the Option-A mount-bounded
   window, and only when the churn also kills the leaver mid-flight.
   Availability during this window depends on the config (see the R/W matrix):
   at R>=3/W=majority or R=2/W=1 the other replicas cover W; at R=2/W=2 the
   position is write-unavailable until a pending owner mounts. Every acked
   write is already durable on its independent replica copies
   (durable-before-ack), so a pending owner sees them all once it mounts; no
   acked write is lost.

3. A pending owner crashes AFTER writing its marker but before the leaver
   removed the position: the marker is durable, so the leaver removes the
   position on its next poll and the next reconcile reassigns it; no acked
   write is lost.

   **Residual (NEW-P1-3, honesty fix - the marker read is point-in-time).**
   The leaver reads the marker (successor serving at read time), proceeds into
   its removal critical section, and the successor crashes CONCURRENTLY (in the
   read-to-removal gap). The leaver already has its positive observation and
   does not re-read inside the lock (I/O under the lock is forbidden). So the
   leaver removes the position AND the successor is gone: the position is
   UNSERVED until the NEXT RECONCILE reassigns it. Benign - NO acked-write
   loss (durable-before-ack; every acked write is durable below E and
   recovered by whoever next acquires) - and the recovery is the same
   next-reconcile path. The marker closes the bare-fence hole, not the
   point-in-time-liveness gap; the doc does not claim case 3 is fully
   crash-safe.

4. Both nodes survive but the leaver is slow to observe the marker: removal is
   POLL-ONLY, so the leaver always converges to remove on its next `drainCheck`
   tick once the durable marker is visible. No "lost signal" case (the marker
   is durable, not a best-effort push); the only variable is poll latency,
   bounded by the settle / self-heal cadence. Correctness unaffected.

## R/W availability matrix during a crash (review P1-4)

The happy path is mount-time-independent for all R/W configs (the leaver
stays a routed current owner and serves via the union throughout the pending
owner's mount). A crash DURING a transition degrades the AFFECTED position,
depending on the config:

| Config                    | LEAVER crashes mid-Draining          | PENDING owner crashes mid-mount |
| ------------------------- | ------------------------------------ | ------------------------------- |
| R>=3, W=majority (quorum) | available (other replicas cover W)   | available (leaver still serving) |
| R=2, W=1                  | available (the other replica is W)   | available (leaver still serving) |
| R=2, W=2 (write-all)      | mount-bounded: writes cannot reach   | available (leaver still serving) |
|                           | W=2 until a pending owner mounts     |                                 |
|                           | (only 1 live replica)                |                                 |

The honest statement: pending ranges makes the HAPPY path
mount-time-independent, but a crash during a transition degrades the affected
position to mount-bounded for configs where the surviving replica count
cannot satisfy W. For R=2 write-all this is ANY single leaver crash mid-handoff
(not a rare double-fault). Quorum configs (R>=3, W=majority) stay available
under a single crash. Do NOT state crash safety as "always fully available."

## Planned package / file layout

Domain (pure, no I/O) in `pkg/storageunit`:

- `handoff.go` (new): the pure transition types, keyed by `ReplicaUnit`.
  - `HandoffPhase` enum: `PhaseAcquiring`, `PhaseReady`, `PhaseDraining`,
    `PhaseReleasing` (steady states Owned / Absent are mountMap presence +
    phase absence).
  - `HandoffState` value object: `{Phase HandoffPhase, OpenEpoch Epoch}`. NO
    `Predecessor` / `PredecessorAddr` (no forwarding target). The cluster keys
    the in-flight record by `ReplicaUnit`.
  - Pure transition guards: `NextOnReady`, `NextOnDrain`, `NextOnRelease`,
    returning the next phase or an error for an illegal edge; the guards
    encode which phase is legal on which side (a node is `Acquiring/Ready` on
    the gainer side, `Draining/Releasing` on the loser side, and CAN be in
    both for DIFFERENT positions of one unit). A `Releasable(state, ready
    bool)` predicate: removal requires positive readiness (a serving marker at
    an epoch STRICTLY ABOVE the leaver's open epoch, computed in the controller
    from `ReadServingMarker`), NEVER a bare fence-epoch compare. Table-driven,
    unit-testable with no ring or factory.

- `factory.go` (edit): extend `ReplicaBackendFactory` with
  `DurableEpochReplica(ru) (Epoch, error)` (the bare fence liveness hint, kept
  as a contrast) PLUS the SERVING-MARKER seam:
  `WriteServingMarker(ru, epoch) error` and `ReadServingMarker(ru) (Epoch,
  bool, error)`. The test `sharedfactory` implements all three over an
  in-memory registry (the serving marker is a per-`ru` epoch entry in a shared
  map), plus the SLOW-mount injection the acceptance gate needs. The slate
  factory implements the serving marker as a small durable object keyed by
  `ru` (real I/O), exercised only in staging.
  Implementation-must-verify (NEW-P1-4): see the seal section.

Membership (the draining node-state) in `pkg/membership`:

- `membership.go` (edit): add `Draining bool` to the `Member` value object and
  carry it in the node's `Meta` alongside the gRPC address (extend the
  `metaDelegate.NodeMeta` encoding + the `nodeToMember` decode in
  `NotifyJoin` / `NotifyUpdate`). Add `SetDraining(bool)` that updates the
  local `Meta` var and calls `memberlist.UpdateNode` so the bit gossips out. A
  `Draining` member stays in the snapshot (alive, address known, A CURRENT
  OWNER); `SetDraining` is distinct from `Leave()`.

Controller (the wiring) in `pkg/cluster`:

- `cluster.go` (edit): `reconcileRingFromMembership` does NOT exclude draining
  members (the current/pending split is computed per-op in `replicasForKey`).
  REMOVE the `leaving atomic.Bool` flag, its early-return guard, and the
  remove-self-only step (the ring-freeze stopgap), AND the draining-exclusion
  the prior draft added. Re-key `mountMap` to `map[ReplicaUnit]backend.Backend`,
  remove the separate `replicaPos` map (subsumed by the key), add
  `handoffPhase map[ReplicaUnit]HandoffState` guarded by `mountMu`. Update
  every reader (`localBackendForKey`, `localWriteBackendForKey`,
  `evictStaleMount`, `mountedBackends`, the reconcile diff) per "Keying by
  ReplicaUnit". REMOVE the `priorDesiredReplicas` / `priorAddrs` snapshot (no
  predecessor identification needed).

- `replicasForKey` (the routing contract): compute CURRENT (ring incl
  draining) + PENDING (ring excl draining); return the stable set when equal,
  UNION when in transition. The transition test is `CURRENT != PENDING`
  (leave: a current member is `Draining`; join: this node is a pending owner of
  a position with no serving marker). Consulted by Put / Get / Delete /
  CommitCASApply fan-out. The dual-write is the existing `fanout` over the
  ROUTED set with `W = requiredWriteAcks` UNCHANGED (the union just makes the
  routed set up to R+1 members transiently; W stays the stable-R quorum).

- `multibackend_pending.go` (new): the pending-ranges controller, replacing the
  position-change branch of `reconcileReplicaUnits`.
  - `reconcileReplicaUnitsPending()`: for a position this node owns under
    CURRENT but not under PENDING (it is a current-not-pending owner, i.e. it
    is `Draining` or a join shifted it out), set `Draining` instead of
    releasing (stay a routed current owner). For a position this node owns
    under PENDING but not mounted, set `Acquiring` + start
    `acquireReplicaUnitPending`. A position the surviving replicas already
    cover (this node drops out and PENDING already has R mounted owners
    without it) stays a PLAIN clean-cut `releaseReplicaUnit`.
  - `acquireReplicaUnitPending(ru)`: OpenReplicaUnit; on success insert the
    `mountMap[ru]` entry under mountMu, set `Ready -> Owned`, then
    `WriteServingMarker(ru, E)` to shared storage EXACTLY ONCE. No RPC.
  - `drainCheck(ru)`: the leaver's removal-check. The `ReadServingMarker(ru)`
    poll (shared-storage I/O) runs BEFORE entering the critical section. The
    phase compare-and-advance (`Draining -> Releasing`) and the `mountMap[ru]`
    compare-and-delete are ONE critical section under `mountMu` (exactly-once
    via the CAS-on-delete, reusing the `evictStaleMount` CAS shape), then
    `CloseReplicaUnit(ru)` once. Removal fires ONLY on a serving marker at an
    epoch STRICTLY ABOVE this node's open epoch, never on a bare fence advance.
    Armed as a bounded periodic re-check on the existing settle / self-heal
    cadence; POLL-ONLY (no RPC wake). Lock discipline below.

- `multibackend_pending_leave.go` (edit): the graceful-leave drain.
  `DrainForLeave(ctx)` SETS THIS NODE DRAINING (`membership.SetDraining(true)`
  so the bit gossips and every node computes the pending split + routes the
  union), then drives the reconcile + `drainCheck` and BLOCKS until
  `ownedPositionCount() == 0` OR `ctx` done. It does NOT call
  `memberlist.Leave()` and does NOT freeze the ring and does NOT exclude itself
  from the ring; the real `Leave()` + `Shutdown()` run in `Close()`'s teardown
  AFTER the drain returns. Drop the `c.leaving.Store(true)` +
  `c.ring.Remove(self)` + `SHALE_DRAIN_DIAG` scaffolding (the stopgap + the
  investigation prints).

- `multibackend_handoff_retry.go` (kept): Option A's retry stays as the belt
  for the residual cases (crash mid-handoff, a pure-new-mount initial
  convergence). Pending ranges shrinks the common-case window to near-zero; the
  retry is the safety net, not the primary mechanism.

RPC proto (`proto/shale.proto`): NO proto change. The union routes via the
EXISTING key-only forwarded ops (`PutRequest{forwarded}` etc.) to each routed
replica's ring-resolved local position - there is no position-addressed
forward and no `ReplicaUnit` field on the wire. (The forwarding draft's
proto change is REMOVED.) There is NO `ReplicaHandoffReady` message and NO
readiness-probe rpc: release is POLL-ONLY via the durable serving marker on
the factory seam.

## Lock discipline (review P1-3, reused)

- The `drainCheck` I/O (the `ReadServingMarker` poll) runs OUTSIDE any cluster
  lock. A slow MinIO read must NOT block routed ops' `mountMap` reads.
- The phase compare-and-advance (`Draining -> Releasing`) and the
  `mountMap[ru]` compare-and-delete are ONE critical section under `mountMu`.
  Overlapping poll ticks racing the same edge are exactly-once via the
  CAS-on-`mountMap`-delete (delete only if the entry still points at the same
  backend) under the same `mountMu` hold as the phase advance. The phase
  machine also rejects a second `Releasing` edge, but the `mountMap` CAS is the
  real exactly-once guard.
- Lock order: `drainCheck` touches ONLY `mountMu` + the `handoffPhase` map. It
  does NOT take `reconcileMu` or `reshardMu`. `CloseReplicaUnit` happens AFTER
  the `mountMu` critical section (the entry is already removed), so a slow
  close does not hold `mountMu`.

## Overlap-move vs plain-release (review P2-3, reused)

The pending-ranges drain applies ONLY when a position MOVES from a leaving
current owner to a specific pending owner (the union dual-writes both; the
leaver drains until the pending owner is serving). A node that simply DROPS
OUT of a unit's replica set entirely - where PENDING already has R mounted
owners without it - stays a PLAIN clean-cut `releaseReplicaUnit`: nobody is
acquiring its exact position and the surviving replicas cover W, so there is
nothing to drain. The reconcile diff distinguishes them by whether PENDING has
a NEW owner acquiring the leaver's exact position (drain) or already covers W
without it (plain release). No stray "isDraining" flag for the plain case.

## Graceful leave (scale-down) - the DRAINING node-state

The pending-ranges machine is symmetric between scale-UP and scale-DOWN: a
JOIN makes a survivor a PENDING owner while the existing owner stays a CURRENT
owner serving via the union; a deliberate REMOVAL makes the survivors PENDING
owners of the leaving node's positions while the LEAVER stays a CURRENT owner
serving via the union. So a graceful leave is the transition seen from the
LOSING side, for ALL of a node's positions at once. Two pieces:

THE ROOT CAUSE (a design flaw, found by instrumentation). The graceful drain
delivered almost no availability (~58% in-process, ~70% real staging) because
the obvious shutdown fix is SELF-CONTRADICTORY. The leaving node was asked to
be BOTH:

- ALIVE - so it can keep serving routed ops during the drain, and
- GONE - so the survivors take over its ownership positions,

and `memberlist.Leave()`-while-keeping-the-transport-up cannot represent that.
`memberlist.Leave()` broadcasts a "leaving" intent, but the node then KEEPS
GOSSIPING AS ALIVE. Survivors receive the leave, briefly drop the node, then
keep hearing it alive and RE-ADD it. Because ownership is derived from the live
membership ring on EVERY node, a survivor that re-added the leaver computed it
as STILL OWNING its positions and never started the handoff. `memberlist.Leave()`
says "I am gone" while the transport says "I am alive"; membership cannot hold
both, so the handoff never starts.

THE GAP ON SHUTDOWN (the second half). On SIGTERM the run loop
(`pkg/shaled/runtime.go`) calls `Cluster.Close()`. Even if the survivors DID
re-own the positions, `Close()` IMMEDIATELY tears down the reconcile loops +
peer clients + `closeMountedUnits()`, WITHOUT waiting for the survivors' slow
mount to complete. Every position the leaving node served was UNSERVED from the
instant `Close()` closed its mounts until the survivors mounted.

THE FIX: a distinct DRAINING node-state - REACHABLE, a CURRENT OWNER, and the
source of the PENDING split. The node stays a full, alive, addressable member
AND a current owner of its positions (so the union keeps dual-writing it and it
keeps serving), while its `Draining` bit makes every node compute the PENDING
set (ring-minus-this-node) and route the UNION. Advertised by a gossiped
per-member `Draining` bit, NOT by leaving the cluster. Four pieces:

1. **Membership: a gossiped `Draining` bit (`pkg/membership`).** Add a
   per-member `Draining bool` carried in the node's `Meta` (which already
   encodes the gRPC dial address). Add `SetDraining(bool)` -> `UpdateNode`.
   Expose `Draining` on the `Member` returned by `Members()` / `Snapshot()`. A
   draining node STAYS in the snapshot - alive, address known, A CURRENT OWNER.
   `SetDraining(true)` is NOT `Leave()`.

2. **Routing: the current/pending split in `replicasForKey` (`pkg/cluster`).**
   The ring keeps every alive member (no draining-exclusion); `replicasForKey`
   computes CURRENT (ring incl draining) + PENDING (ring excl draining) per op
   and routes the UNION while CURRENT != PENDING. The moment the draining
   `Meta` gossips, every node routes the leaver's positions to the union
   {leaver + pending successors}; survivors that are pending owners acquire in
   the background and write markers; the leaver is removed position-by-position
   on each marker.

3. **Drain flow: set draining, serve + wait, THEN real leave + shutdown
   (`DrainForLeave`).** `Cluster.DrainForLeave(ctx)` SETS THIS NODE DRAINING
   (the `Meta` flag via `SetDraining(true)`) - it does NOT call
   `memberlist.Leave()` here. It then BLOCKS until every position this node
   owns has a serving successor (`drainCheck` released them all;
   `ownedPositionCount() == 0`) OR `ctx` cancels / the timeout fires. The
   reconcile loop, serving, and `drainCheck` STAY ALIVE during the wait. Each
   position releases on the SAME marker rule as any transition. ONLY AFTER the
   drain completes does the node do the REAL `membership.Leave()` + the
   existing `Close` teardown (`Shutdown`). Order: SET DRAINING -> SERVE + WAIT
   -> REAL LEAVE + SHUTDOWN.

4. **Wire it at the TOP of `Close()`, gated.** `GracefulLeaveDrainTimeout
   time.Duration`: `0` = disabled = today's behavior (the gap remains; also the
   break-demo state). When `> 0` AND `multiReplicated()`, `Close()` calls
   `DrainForLeave(timeout)` FIRST - before ANY teardown, while the loops are
   still running - then proceeds with the existing teardown (now including the
   real membership `Leave()`) unchanged. The SIGTERM handler in `runtime.go`
   just calls `Close()` as today, no run-loop change.

### Two memberlist verbs, split - but `Leave()` is used at the END

memberlist exposes `Leave()` (broadcast the graceful departure so peers record
a clean leave and STOP re-adding the node) DISTINCT from `Shutdown()` (tear
down the transport). The REAL `Leave()` is DEFERRED to AFTER the drain, NOT
used to START it. Starting the drain with `Leave()` is the self-contradiction
above. The drain is started by the `Draining` `Meta` bit (the node stays fully
alive, a current owner). Only once the drain is done does the node call the
real `Leave()` followed by `Shutdown()`.

### This supersedes the ring-freeze stopgap AND the draining-exclusion

An earlier attempt called `memberlist.Leave()` at the START of the drain and
papered over the snapshot collapse with a `leaving atomic.Bool` flag plus a
remove-self-only step; a later draft replaced that with draining-EXCLUSION
(drop the draining node from the ring + per-position back-forward). Pending
ranges removes the need for ALL of it: the node stays ALIVE AND a CURRENT
OWNER, so its snapshot does NOT collapse, `reconcileRingFromMembership` works
normally and does NOT exclude draining members, there is no frozen ring, and
there is no per-position forward. REMOVE the `leaving` flag + its early-return
guard, the remove-self-only step, the draining-exclusion, and the entire
forward path (`acquiringForwardTarget` + the predecessor snapshot), replacing
them with the draining-`Meta`-driven UNION routing in `replicasForKey`.

### Remove the SHALE_DRAIN_DIAG scaffolding

The `SHALE_DRAIN_DIAG`-gated `fmt.Fprintf` diagnostics (investigation-only;
they captured the "54 acquire decisions, leaver still in the live ring"
evidence) are removed once the pending-ranges fix verifies.

DRAINING vs THE RESIDUAL. A position the leaving node owns is taken over by a
SURVIVING pending owner - so the union forms and the leaver drains on the
successor's marker. In the common case EVERY served position is covered. Two
residuals are NOT covered (and do not need to be):

- A position where the leaving node simply drops out and PENDING already
  covers W with no new owner taking its exact position: nothing to drain, the
  eager release is correct, the surviving replicas keep it available. NOT a
  gap, just outside the drain machinery.
- A position whose PENDING successor is STUCK (its mount never completes within
  the grace budget): the drain wait times out, `Close()` proceeds, that one
  position is unserved from teardown until the successor mounts - exactly
  today's gap, for that position only, bounded by the grace budget. No worse
  than the disabled (timeout 0) behavior.

THE INVARIANT. For a position with a REACHABLE pending successor, a graceful
leave has NO unserved window: the leaver stays a routed current owner and
serves the position (dual-written via the union) until the successor is serving
(marker present) and `drainCheck` removes it, and only then closes.
No-acked-write-lost and single-writer-fence are unchanged.

OPERATOR CONCERN. The orchestrator's termination grace period MUST exceed
`GracefulLeaveDrainTimeout`, or the orchestrator SIGKILLs the process mid-drain
and reopens the gap. On a k8s StatefulSet that is
`terminationGracePeriodSeconds`, set STRICTLY GREATER than the drain timeout
(with headroom for the leave broadcast to gossip and the post-drain teardown).
The code enforces only its own timeout.

ACCEPTANCE (break-demo). A continuous writer through a graceful one-node leave
asserts ~100% ack + zero unserved window + post-leave readback of every
baseline key from the post-transition routed set; the paired break demo sets
`GracefulLeaveDrainTimeout = 0` (drain disabled) and the same leave shows the
gap. Reuses the slow-`OpenReplicaUnit` injection so the hand-off takes a
measurable, mount-dominated interval.

## Resolved review findings (pending ranges closes them directly)

The prior reviews (`overlap-handoff-review.md`, `overlap-handoff-rereview.md`,
kept as history) found holes in the forwarding design; pending ranges closes
the same holes WITHOUT the forwarding machinery the re-review then had to
patch:

- P0-1 (routing never reaches the still-serving old owner): closed by KEEPING
  the old owner in the routed UNION (it stays a current owner), so routing
  reaches it DIRECTLY. No back-forward.
- P0-2 (per-`GenUnit` maps cannot represent overlapping positions): closed by
  RE-KEYING `mountMap` / phase map / `HandoffState` by `ReplicaUnit` (reused).
- P1-1 (release on a bare durable-epoch advance): closed by the SERVING MARKER
  gate (reused) - removal requires a marker strictly above the leaver's epoch.
- P1-2 / NEW-P1-4 (consistent-mount WAL tail + intra-open fence<=recovery):
  reused as the implementation-must-verify pin, simplified because writes are
  dual-written to the pending owner's own copy rather than forwarded.
- P1-3 (release-check lock discipline): reused unchanged.
- P1-4 (crash-safety not always fully available): reused via the R/W matrix.
- NEW-P0 / NEW-P1-1 / NEW-P1-2 (position-addressed forward wire change,
  predecessor identification, single-hop scope): MOOT under pending ranges
  (no forward, no predecessor, no single-hop scope - the union routes
  directly). They document machinery now REMOVED.
- NEW-P1-3 (point-in-time marker gap): reused in crash case 3(c).

## What does NOT change

- The replicated apply-if-newer, LWW envelope stamping, and the W math
  (`requiredWriteAcks` against the stable R): untouched. Pending ranges changes
  only WHICH ring nodes are contacted during a transition (the union) and WHEN
  the old owner is removed (the marker gate); it does NOT change how W is
  computed.
- The legacy per-node path and the R=1 multi-backend lease handoff (Phase 3):
  untouched. Pending ranges lives behind `multiReplicated()`.
- Epoch fencing semantics: unchanged per replica database. The durable fence
  epoch is a LIVENESS HINT only (strictly weaker than the serving marker and
  NEVER a removal trigger); the open-time fence is the same acquire fence as
  before.

## What DOES change

- `replicasForKey` computes current / pending / union and dual-writes the
  union during a transition (the routing core).
- `mountMap` / `replicaPos` re-keyed to `ReplicaUnit` (reused).
- The `Draining` member stays a CURRENT OWNER (no ring exclusion); the split is
  per-op in `replicasForKey`.
- The OLD owner's removal trigger is "successor serving," detected POLL-ONLY by
  the durable SERVING MARKER (a marker strictly above the leaver's open epoch),
  NOT a bare fence advance and NOT a push RPC.
- The serving marker + `Draining` Meta bit + `DrainForLeave` are REUSED.
- The intra-`OpenReplicaUnit` ordering is pinned (fence effective no later than
  the WAL-recovery cutoff; pin test widened to a dual-write-concurrent-with-
  mount case).

## REMOVED (superseded by pending ranges)

- Per-position forwarding: `acquiringForwardTarget`, `forwardPutToPredecessor`,
  `forwardGetToPredecessor`, the predecessor address machinery.
- The position-addressed forwarded-op wire change (the `ReplicaUnit` field on
  `PutRequest` / `GetRequest` / `DeleteRequest` / CAS-apply-forward). NO proto
  change in pending ranges.
- The `Acquiring`-state forward behavior on the new owner.
- `PredecessorAddr` / `Predecessor` fields on `HandoffState`.
- The retained `priorDesiredReplicas` / `priorAddrs` snapshot (predecessor
  identification).
- The single-hop scope + the ambiguous-predecessor Option-A fallback.
- The draining-exclusion in `reconcileRingFromMembership`.
- The ring-freeze stopgap (`leaving atomic.Bool` + early-return guard +
  remove-self-only) and `SHALE_DRAIN_DIAG`.
