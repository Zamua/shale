# Declarative, decentralized split/merge resharding (design proposal, v2)

Status: **PROPOSAL, v2** (post adversarial review). v1 framed this as "mostly an
assembly job"; a 5-lens adversarial review found blocking holes in every lens and
that framing was wrong. v2 corrects the design: the pending-ranges primitives are
reusable *building blocks*, but the **cross-generation orchestration is genuinely
new**, and the cross-node flip must be ordered by a **durable, CAS-guarded arbiter
in shared object storage**, not node-local state. Once accepted + built, this
folds into `docs/SPEC.md` as the v0.9 reshard section.

## 0. What the review changed (read this first)

The review confirmed the *foundations* are sound (verified against source):

- `dbNameFor` gen-namespacing puts generation ahead of unit, so gen-g `K` and
  gen-(g+1) `K`/`K+N` are disjoint object-store prefixes: opening a child never
  fences or touches the parent.
- `apply-if-newer`/LWW (`txApplyIfNewer`, `Stamp.Greater`) is genuinely idempotent
  and order-independent, so a stale copied value can never clobber a newer live
  write **once the copy is routed through it**.
- The serving-marker + epoch-fence + `drainCheck` strict-`>`-against-`ownOpenEpoch`
  discipline (the #410 trap) is the right engine and is correctly understood.
- The `#1` gap (`desiredReplicaUnits` is static / cutover-blind at
  `multibackend_replicated.go:111`, while its R=1 sibling `desiredGenUnits` carries
  the cut-over-child branch) is the right gap, and "mirror `desiredGenUnits`" is the
  right shape of fix.
- `UnitCount.Halve()` is implemented + tested (Phase A, committed).

It found these **blocking** problems, all of which v2 must (and below does) close:

1. **No cross-node flip-ordering primitive.** `cutOver` is node-local and never
   gossiped; two nodes can route the same key to different generations, and a
   write acked only on a child (single replica) can be lost when another node
   retires the parent. The freeze barrier exists for exactly this
   (`multibackend_reshard_barrier.go:7-9`, `multibackend_reshard.go:329-338`);
   removing it requires *re-providing the ordering decentrally*, not deleting it.
2. **Two-generation union routing does not exist.** `genUnitForKey` /
   `resolveGenUnit` resolve exactly ONE `GenUnit` per key (parent OR child via
   `cutOver`), never both. Today's union (`routedReplicasWithUnit`) spans
   membership (`drainingIDs`), with no generation dimension. So "dual-write to
   parent AND child" has no routing path.
3. **`writeAckBar` stable-R only holds for same-unit position shuffles.** For a
   split the child is a DIFFERENT `GenUnit`; the existing flat fan-out cannot keep
   "ack over the parent set, child legs supplementary" without partitioning acks
   by unit.
4. **Ordered removal cannot reuse `drainCheck` across generations.** `drainCheck`
   reads the SAME ReplicaUnit's marker; parent `K@g` and child `K@(g+1)` are
   different ReplicaUnits with different marker objects. Gating parent retirement
   on the children's markers is NEW cross-unit logic; and the marker proves
   *serving*, not *caught-up*, so retiring on a bare marker can drop un-copied
   acked writes.
5. **`copyUnitInto` uses bare `Put` of raw bytes.** At R>1 those are LWW Envelopes;
   the copy must go through the envelope-aware apply-if-newer path, and the
   clear+recopy catch-up cannot run on a child that is already dual-receiving.
6. **The reused overlap engine is R>1-only; the reused reshard machinery is
   R=1-only.** They are disjoint code paths that have never run together. The
   decentralized online reshard is a **greenfield R>1** feature; R=1 multi-node
   keeps the freeze barrier (or is declared unsupported for online reshard).
7. **Merge is structurally harder than split** (the survivor's keyspace spans two
   parents addressed by different key hashes), has no two-source copy path, needs
   a per-parent caught-up gate, single-writer CAS, and tombstone semantics.
8. **Agreement gaps:** no durable monotonic reshard generation; joiner-mid-reshard
   (`learnGenerationFromSeed` carries only `{gen,count}`) can route to a retired
   parent; no decentralized abort/reclaim for a stuck split; `Transact`/CAS
   spanning a mid-split unit takes a separate snapshot and relies on the freeze
   gate being removed.

## 1. Goal and non-goals

**Goal.** Declare the unit count; the cluster reconciles toward it online, split
(`N -> 2N`) and merge (`2N -> N`), with **zero acked-write loss** (absolute),
reads fully available, writes available except a **brief, retryable per-unit blip**
at the flip, **no cluster-wide freeze, no central planner, no per-range Raft**.

**Non-goals.** Fixed-large-count (our units are heavyweight SlateDB instances, not
logical slots, so a 1-pod cluster can't host thousands of them — variable count is
required); strictly-zero-blip (sub-second retryable is the target); `count = 0`
(invalid, rejected); R=1 multi-node *online* reshard (keeps the coordinated freeze
barrier — out of scope for the decentralized path).

## 2. The decentralized arbiter: a CAS-guarded reshard epoch in shared storage

This is the centerpiece and the answer to blocking holes 1 and 8. Replace
node-local `cutOver` as the cross-node authority with **durable objects in the
shared object store**, coordinated by conditional writes (`If-None-Match` /
`If-Match` — the SAME MinIO/S3 CAS slatedb already relies on for manifest fencing,
and that the SPEC already requires of the metadata store):

- **`__reshard/epoch`** — `{epoch, count, plan, prevCount}`. To start a reshard, a
  node that observes `agreed-desired != live-count` computes the next step
  (`Double()` to split, `Halve()` to merge) and CAS-advances `epoch -> epoch+1`
  with the new `(count, plan)`. Exactly one writer wins the conditional write;
  every other node reads the winning `(epoch, count, plan)` and adopts it. This is
  Redis's monotonic `configEpoch` made durable: **determinism + a CAS race replace
  the elected coordinator.** A split and a merge cannot both claim `epoch+1` —
  the CAS serializes them; the loser re-derives against the new epoch.
- **Per-unit cut-over markers** — `__reshard/cutover/g<gen>/u<K>` written (CAS,
  once) when unit `K`'s successors are **caught-up** (3.3). This durable marker —
  not node-local `cutOver` — is the per-unit flip signal *every* node observes
  (poll-only, monotone), so the flip is cluster-ordered without a coordinator.

`cutOver` in `genState` stays as a node-LOCAL cache of "I have observed unit `K`'s
durable cut-over marker," but its AUTHORITY is the durable marker. A node sets its
local `cutOver[K]` (and routes `K` to gen+1) only after reading the durable marker.

The desired count is declared once (preferred: a single config source the nodes
read, e.g. a ConfigMap; acceptable: gossiped per-pod via `Meta` + agreed). Gossip
(`membership.Meta`, extended with a NUL-separated `desired-count`/`epoch` segment
that `decodeMeta` already tolerates) is a fast *hint* to "go read the CAS epoch";
the CAS object is the authority, so a rolling-update disagreement window cannot
cause two plans.

## 3. The split protocol (`N -> 2N`)

### 3.1 Plan + pending children (declarative)

On observing the CAS epoch advance to `(count=2N, plan=split)`, every node
deterministically derives `K -> {K, K+N}` (`ChildUnits`). Extend the desired-set
derivation so the gen-(g+1) child positions this node's ring owns enter its
**pending** set (close blocking hole 4 / the #1 gap): mirror `desiredGenUnits`'
cut-over-child branch into the R>1 `desiredReplicaUnits` /
`desiredPendingReplicaUnits`, keyed on the live epoch's `nextCount`. The existing
`reconcileReplicaUnitsOverlap` ACQUIRE half then background-mounts each child as a
fresh disjoint DB (`acquireInFlight`-guarded), with the in-flight child placed in
the desired set the instant it mounts so the self-heal reconcile never RELEASEs it
(close major hole: self-heal vs in-flight).

### 3.2 Two-generation routing (NEW; closes holes 2, 3)

Add a reshard-aware router. When `genSnapshot` reports a reshard is in flight
(`nextCount != 0`) and key `h`'s old unit `K` is mid-split (epoch advanced, `K`'s
durable cut-over marker NOT yet observed):

```
routedReplicasForReshard(h):
  parentSet = replicas over genUnitBytes(GenUnit{g,   K})
  childSet  = replicas over genUnitBytes(GenUnit{g+1, ChildUnit(h, N)})   # the one child h lands in
  return UNION(parentSet, childSet), with each leg tagged PARENT or CHILD,
         and ackBar = requiredWriteAcks over PARENT legs only
```

A write **dual-writes** to parent `K@g` and the single relevant child, each
position-addressed (`PutAtReplica`). **The ack bar is counted over the PARENT
replica legs only** (a structured write-attempt that partitions acks by
parent-vs-child, NOT the existing flat fan-out): child legs are strictly
supplementary, so every acked write has a durable home on the parent set
throughout the slow copy, and the child being mid-mount never blocks or fails a
write. Reads fan across the union; a mid-copy child returns the transient
`errUnitAcquiring` the fan-out already skips. After `K`'s durable cut-over marker
is observed, the router flips: authoritative = child set, ack bar over the child
legs, parent becomes supplementary then retires (3.4).

### 3.3 Online copy + the caught-up marker (NEW; closes holes 4, 5)

Reuse `bisectUnit`'s child-create + the `copyUnitInto` *scan-and-route-by-hash-bit*
shape, but: (a) route every copied key through the **envelope-aware apply-if-newer
path** (`applyEnvelopeIfNewerToBackend`/`txApplyIfNewer`) carrying the SCANNED
envelope's stamp, NEVER a bare `Put` — so a stale copied value loses to a newer
dual-write and vice versa; (b) **never `clearBackend` a dual-receiving child** —
drop the clear+recopy catch-up entirely and rely on apply-if-newer idempotence
(a re-scanned key is a stamp no-op). Delete-correctness comes from R>1 tombstone
semantics (the merge/split online path is R>1 only; a delete is a stamped
tombstone envelope that LWW-resolves, not a physical absence).

When a child has **drained its copy** (a full scan completed with no
newer-stamp losers outstanding) AND is confirmed receiving dual-writes, it is
**caught-up**. The unit's cut-over is gated on **both** children being caught-up
(not merely mounted/serving): the node that confirms both children caught-up
CAS-writes the durable `__reshard/cutover/g/u<K>` marker. The serving marker stays
the per-position liveness signal; the cut-over marker is the new per-UNIT,
caught-up, cluster-observable flip signal.

### 3.4 Per-unit flip + ordered removal (NEW two-generation gate; closes holes 1, 4)

The flip is the durable cut-over marker becoming visible, which every node
observes (poll). On observing it, a node: sets local `cutOver[K]` (routes `K` to
children), moves the ack bar to the child legs, and marks its parent `K@g`
position(s) `Draining`. The parent retires via a **new two-generation gate** (NOT
`drainCheck` reuse): retire `K@g` only after (a) the durable cut-over marker for
`K` is present, AND (b) both children's serving markers are strictly above the
parent's recorded `ownOpenEpoch`, AND (c) the parent's own copy has been observed
drained into the children. A lagging node that still routes a write to `K@g`
before observing the marker dual-writes to the children too (the children are
caught-up + authoritative), so the write is never stranded; a node that hits a
fenced/retired parent re-resolves to the children and retries (the brief blip),
apply-if-newer making the retry a no-op if it already landed.

**Why no acked write is lost (the corrected argument):** during the whole split
window the ack bar is over the PARENT set, so every acked write is durable on a
parent. The cut-over marker is written only after both children are *caught-up*
(they hold everything the parents hold). After the marker, writes ack over the
children; the parent is retired only after the marker + both child markers + copy
drain. There is no instant where the only durable home of an acked write is a unit
being closed, and the **single cluster-ordered cut point is the durable cut-over
marker**, observed identically by every node — replacing the freeze's global
ordering with a per-unit durable one. (This argument must be written into SPEC and
empirically gated by the staggered-node-timing oracle in §6, since it is precisely
what the freeze barrier currently buys.)

### 3.5 Post-flip redistribution

`bumpRingGen` + the existing reconcile move the gen-(g+1) units to their ring
owners by the normal zero-copy lease handoff. Reused unchanged.

## 4. The merge protocol (`2N -> N`) — the harder, second-built half

Two parents `K`, `K+N` (gen g) collapse into survivor `K` (gen g+1, `Halve()`).
`ParentUnit` maps both parents to `K`. The asymmetries the review demands:

- **Two-source superposition routing** (closes merge hole 1): a key under either
  parent must reach BOTH its parent AND the survivor.
  `routedReplicasForMerge(h) = parentReplicas(genUnitForKey(h)) +
  survivorReplicas(GenUnit{g+1, ParentUnit(parentID, 2N)})`, survivor legs carried
  as their own `(member, survivorRu)` pairs, **ack bar over the parent legs of the
  key's own parent**.
- **Two-source merge-copy** (closes merge hole 2): stream BOTH parents into the
  survivor through the envelope-aware apply-if-newer path; **no `clearBackend`** on
  the dual-receiving survivor; tombstones carry the deletes (R>1 only — no R=1
  merge). A key deleted in one parent and present in the other LWW-resolves by
  stamp in the survivor.
- **Per-parent caught-up gate** (closes merge hole 3): the survivor publishes its
  cut-over marker only after BOTH source copies have drained; each parent retires
  only after that marker — a parent never retires while it holds un-copied acked
  writes. The ordered-removal surface is two parents on one survivor marker, each
  with its own per-parent catch-up water-mark.
- **Single live survivor writer** (closes merge major hole): the survivor's first
  mount/marker write is a CAS (`If-None-Match`) so a membership flap cannot create
  two live survivor writers; the monotone marker floor alone is insufficient.
- **Stamp ordering across two parents** (closes merge major hole): LWW across two
  originators uses wall-clock stamps with no HLC, so a clock-skew loser is possible
  at the merge boundary. Mitigate by carrying the higher-resolution `(TimestampNanos,
  NodeID)` stamp already in the envelope and, if skew proves real on staging,
  promoting the stamp to a hybrid logical clock (a separate, scoped change).

## 5. Scope, joiners, transactions, abort

- **R-scope (closes hole 6):** the decentralized online reshard is **R>1 only**,
  built greenfield on the overlap engine. The R=1 multi-node path keeps the
  coordinated freeze barrier; the 9 `isFrozen()` gates are removed ONLY on the R>1
  path. State this explicitly in SPEC; do not silently disable the R=1 freeze.
- **Joiner mid-reshard (closes hole 8a):** extend `GenStateResponse` with
  `next_count`, the agreed `reshard_epoch`, and the observed cut-over unit ids;
  `learnGenerationFromSeed` commits the full mid-reshard `genState` before mounting
  and **fails closed** if seeds disagree on the reshard state (they shouldn't —
  the CAS epoch is authoritative — but a partial gossip view must not let a joiner
  route to a retired parent).
- **Transact / CAS spanning a mid-split unit (closes hole 8c):** the CAS commit
  re-resolves the unit under the per-unit pause and returns `codes.FailedPrecondition`
  if the unit cut over mid-transaction, so `Transact` retries at the new generation
  (reuse `commitRetryable`, `cas.go`). Co-located `{hash-tag}` sets stay in one unit
  because `ChildUnit` routes a whole tag-set's shared hash to one child (verify with
  a co-location-preserving test across the doubling).
- **Decentralized abort / reclaim (closes hole 8b + the straddle):** a per-split-unit
  deadline (analogous to `GracefulLeaveDrainTimeout`) — on expiry with no
  caught-up children, the split for that unit aborts: release the half-built child
  (a distinct gen-(g+1) prefix, never routed because its cut-over marker was never
  written, so harmless), and a reconcile sweep GCs orphan gen-(g+1) prefixes whose
  epoch is not the live one. The CAS epoch is the single source of truth for "is
  this generation live," so an orphan child is unambiguously identifiable.

## 6. Implementation phases (TDD, each adversarially reviewed + oracle-gated)

- **A. `UnitCount.Halve()`** — DONE (committed `fa3864d`).
- **B. The CAS reshard-epoch arbiter** (pure-ish, no reshard yet): the
  `__reshard/epoch` object + conditional advance + read/adopt; the `Meta`
  desired-count hint; the agreed-target derivation; `DebugState` surfacing. Test:
  concurrent CAS advances serialize to exactly one `(epoch,count,plan)`; a node
  adopts the winner; a rolling desired-count bump converges to one plan.
- **C. Decentralized SPLIT (R>1, greenfield).** Two-generation router + ack-over-
  parent partitioning + cutover-aware desired sets + apply-if-newer copy + the
  caught-up cut-over marker + the two-generation ordered-removal gate. Remove the
  R>1 freeze gates only. **Gate: a lossless-split oracle** — continuous writes
  through a live `2 -> 4`, record-only-acked, read-back-from-every-node, ZERO acked
  loss, ack-rate > 95% — run under a **staggered-node-timing adversary** (nodes
  observe the cut-over marker and flip in deliberately different orders, the
  durability lens's exact demand), WITH a break-demonstration (skip a dual-write
  leg / retire a parent before the cut-over marker → oracle must catch the loss).
- **D. Decentralized MERGE (R>1).** Two-source routing + two-source apply-if-newer
  copy + per-parent caught-up gate + survivor CAS single-writer. Gate: a
  lossless-merge oracle (`4 -> 2`, both parents drained on per-parent catch-up,
  zero loss) + break-demo (retire a parent before its source drained).
- **E. Composition + hardening.** Reshard composed with a concurrent membership
  change on the same unit; joiner-mid-reshard; `Transact` mid-split; orphan-child
  GC; the slate-backed chaos soak (`chaos slatedb`); then a staging run via the
  `staging-availability-probe.sh` harness across a declared `2 -> 4 -> 2`.

Reuse the existing oracle discipline (`lossless_multinode_reshard_gate`,
`membership_change_write_availability`): record only acked keys, read back from
every node, assert zero loss AND high ack-rate, keep the gate honest with a
break-demonstration. **Add the staggered-node-timing adversary** — continuous
writes is not enough; the oracle must flip nodes in different orders to exercise
the cross-node cut-over window the freeze used to eliminate.

## 7. Open questions (validation gates, not blockers)

1. Is the per-unit flip strictly zero-blip or only shrunk to a marker-poll
   interval? DynamoDB's mature split still costs ~1s; "sub-second retryable" is the
   realistic target. Measure on staging.
2. The formal cross-node flip-ordering proof: write into SPEC the precise argument
   that "ack-over-parent until the durable cut-over marker + caught-up children +
   marker-gated ordered removal" is sufficient without the freeze, and let the
   staggered-timing oracle (§6 Phase C) be the empirical proof.
3. Merge stamp ordering under clock skew: is the existing `(TimestampNanos,NodeID)`
   stamp enough, or is an HLC needed? (Defer to a measured skew on staging.)
4. CAS availability on the chosen object store: the design assumes `If-None-Match`/
   `If-Match` on the metadata store (MinIO/S3 conditional writes / R2). Confirm the
   deployed store provides it for the epoch + cut-over + survivor objects (slatedb
   already requires CAS for manifest fencing, so this is the same dependency).
