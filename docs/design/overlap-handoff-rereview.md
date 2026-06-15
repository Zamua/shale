# Adversarial RE-review: amended Option B overlap handoff

Status: RE-REVIEW of the AMENDED design in `docs/design/overlap-handoff.md` +
SPEC "v0.8 Phase 2e" (commit 0d51e5b). The first review
(`overlap-handoff-review.md`) is kept as history. This pass (1) confirms
each prior P0/P1 hole is closed by the amended text, and (2) hunts for NEW
holes the forwarding approach introduces. Reading + reasoning only; no code.

Verdict: the three architectural amendments (new-owner forwarding,
ReplicaUnit keying, Ready-RPC-authoritative release) DO close the prior
P0/P1 holes at the level of the behavioral argument. BUT the forwarding
approach introduces ONE genuine P0 (the forwarded op cannot reach the
predecessor's draining mount through the predecessor's own resolver, because
the amended resolver is ring-index-keyed and the predecessor is no longer on
the ring for that position) and TWO P1s (predecessor identification relies on
a prior-ring snapshot the multi path does not keep; the forward->local
cutover has an unargued same-key reordering window). The state-machine
discipline is preserved (no scattered flags reintroduced).

---

## PART 1: confirmation the prior holes are closed

### P0-1 (routing never reaches the old owner) -> CLOSED at the argument level

The amendment adopts NEW-OWNER FORWARDING. While `Acquiring`, the new owner
forwards the routed op to the predecessor instead of refusing
(overlap-handoff.md:144-205; SPEC:1239, guarantee 1 at SPEC:1256). This is
the correct shape: routing still ring-derives to the new owner (the ring
rotation), and the new owner bridges to the still-mounted old owner. The
"dead mount nobody routes to" problem is gone IN PRINCIPLE. (It is NOT gone
in MECHANISM - see NEW-P0 below: the forwarded op still has to actually land
on the draining mount, and the amended resolver blocks it.)

### P0-2 (per-GenUnit maps cannot represent overlapping positions) -> CLOSED

Re-keying `mountMap` to `map[ReplicaUnit]Backend`, folding `replicaPos` into
the key, and keying `handoffPhase` + the pure `HandoffState` by `ReplicaUnit`
(overlap-handoff.md:93-142; SPEC:1235) genuinely fixes the model. A node can
now hold `{gu,p}` draining and `{gu,q}` acquiring as two independent entries.
The ripple list (localBackendForKey / localWriteBackendForKey /
evictStaleMount / mountedBackends / reconcile diff) is enumerated, so the
implementer will not bolt on a parallel draining map. Model-soundness: closed.

### P1-1 (release on a bare durable-epoch advance) -> CLOSED

`ReplicaHandoffReady(ru, E)` is now the AUTHORITATIVE release trigger, sent
only AFTER the mount flip; the durable epoch is demoted to a LIVENESS HINT
that triggers a readiness RE-VERIFY probe, and a bare epoch advance NEVER
releases (overlap-handoff.md:81-91, 244-251; SPEC:1249-1250, guarantee 5 at
SPEC:1264). Crash case 1 (NEW fences then crashes pre-mount) is now correct:
the epoch crosses but the re-verify probe to the dead NEW fails, so OLD stays
Draining-serving (overlap-handoff.md:314-327; SPEC:1260a). The priority
inversion the first review flagged is fixed. Closed - SUBJECT to the
re-verify race in NEW-P1-3 below (the probe is point-in-time, not a latch).

### P1-2 (consistent-mount WAL tail) -> CLOSED as an explicit gate

The WAL-replay-on-open requirement is now an EXPLICIT, release-gating
IMPLEMENTATION-MUST-VERIFY assumption with a named pin test
(overlap-handoff.md:359-426; SPEC:1252). This is the honest treatment: it is
flagged as a P0-lost-write-if-false and blocks the phase. Closed as far as a
design doc can close an FFI-opacity question.

### P1-3 (release-check lock discipline) -> CLOSED

I/O (DurableEpochReplica read + readiness probe) runs outside the lock; the
phase compare-and-advance + mountMap CAS-delete are one `mountMu` critical
section; `drainCheck` touches only `mountMu` + the phase map and never
`reconcileMu`/`reshardMu` (overlap-handoff.md:526-542; SPEC:1264). The
exactly-once guard is the CAS-on-delete, with the phase machine as a second
belt. Lock-order inversion is explicitly avoided. Closed.

### P1-4 (crash safety is not always "fully available") -> CLOSED

The R/W matrix (overlap-handoff.md:560-579; SPEC:1260b, guarantee 3) states
honestly that a crash mid-handoff degrades the affected position to
mount-bounded for configs where the surviving replica count cannot meet W
(any single OLD crash at R=2/W=2). No longer overstated. Closed.

### P2-1/P2-2/P2-3 -> CLOSED

Flip disambiguated into RING ROTATION vs MOUNT FLIP (used consistently);
pure types keyed by ReplicaUnit with side-legal guards; overlap-move vs
plain-release distinguished (overlap-handoff.md:29-49, 433-450, 544-557;
SPEC:1274-1276). Closed.

---

## PART 2: NEW holes the forwarding approach introduces

### NEW-P0 (CORRECTNESS / AVAILABILITY): the forwarded op cannot reach the predecessor's DRAINING mount, because the predecessor's own resolver is now ring-index-keyed and the predecessor is no longer on the ring for that position

This is the forwarding approach's central new hole, and it is the mirror
image of the original P0-1. The design fixed "the originator never contacts
OLD" by having NEW forward to OLD. But it ALSO re-keyed the resolver so that
`localBackendForKey` "picks the one the ring currently assigns THIS node"
(overlap-handoff.md:121-128). Those two amendments collide on the predecessor.

The chain:

1. The forward reuses the EXISTING forwarded-op RPCs `PutForwarded` /
   `forwardGet` / the CAS-apply forward (overlap-handoff.md:181-190;
   SPEC:1239). The design is explicit that "the predecessor's RPC handler
   lands in its normal local-apply / local-read path"
   (overlap-handoff.md:185-188).
2. That handler is `LocalReplicaPut` -> (multiReplicated branch)
   `applyEnvelopeIfNewerToUnit(key, ...)` (`replicate.go:597`), and for reads
   the local-read path. Both resolve the unit via `localBackendForKey` /
   `localWriteBackendForKey` (`multibackend.go:272`, `multibackend_reshard.go:204`).
3. TODAY `localBackendForKey` resolves `mountMap[gu]` with NO position check
   (`multibackend.go:272-278`): whatever single backend is mounted for the
   unit. Under the amendment this MUST change to resolve by the position the
   LIVE ring assigns this node (overlap-handoff.md:121-128: "the resolver
   derives the position from the live ring (this node's index in
   `unitReplicas(gu)`)").
4. On the PREDECESSOR, after the ring rotated, the live ring NO LONGER
   assigns it this position (that is the whole premise: the position moved
   OLD->NEW). So `unitReplicas(gu)` on the predecessor does not contain the
   predecessor at the draining position. The resolver returns ok=false (or
   resolves a DIFFERENT position the predecessor still holds for the unit).
5. The design itself says the draining entry "is reachable ONLY via the
   predecessor-forward path (below), never via this node's own ring index
   after rotation" (overlap-handoff.md:127-128). But the predecessor-forward
   path lands in the SAME `LocalReplicaPut` -> same ring-index resolver. The
   forwarded op is NOT tagged "serve the draining mount"; it is an ordinary
   `PutForwarded` that re-resolves locally by ring index. So the forwarded
   op hits ok=false and returns `errUnitAcquiring` (or worse, applies against
   the WRONG position if the predecessor still holds a different position of
   the same unit).

Net: the forward arrives at the predecessor, the predecessor cannot find the
draining mount through its own (now ring-index-keyed) resolver, and the
forward fails with the acquiring error. The new owner then falls through to
its OWN `errUnitAcquiring` (overlap-handoff.md:191-196), i.e. availability
bottoms out on Option A's retry - exactly the bound Phase 2e claims to
remove. The acceptance gate (slow OpenReplicaUnit + assert ~100% ack) would
FAIL on a faithful implementation, OR pass only because Option-A retry masks
the gap (the gate's break-demo must keep Option-A off to catch this; it
does, SPEC:1278, so the gate would correctly RED here).

This is subtle precisely because the two amendments are individually correct
but jointly contradictory: ReplicaUnit-keying + ring-index resolution
(P0-2's fix) makes the draining mount unreachable by exactly the forward
path P0-1's fix relies on.

REQUIRED AMENDMENT. The predecessor must be able to serve the DRAINING
position for a forwarded op even though the live ring no longer assigns it
that position. The forward must carry the explicit `ReplicaUnit` (the
position), and the predecessor's forwarded-op handler must resolve
`mountMap[ru]` BY THAT EXPLICIT ReplicaUnit (including a `Draining`-phase
entry), NOT by re-deriving the position from its own live ring index.
Concretely one of:

- (a) A position-addressed forward: a NEW forwarded-op variant (or an added
  `ReplicaUnit` field on the existing one) that names the target position,
  so the predecessor's handler does `mountMap[ru]` directly and is willing
  to serve a `Draining` entry. This contradicts the design's claim that the
  forward "adds NO new proto" (overlap-handoff.md:511-512, SPEC:1239) and
  that it "reuses the EXISTING forwarded-op RPCs ... no new wire type" - that
  claim is FALSE given the ring-index resolver, because the existing RPCs
  carry only the key, and the key re-resolves to the wrong/absent position
  on the predecessor.
- (b) The predecessor's resolver, on a forwarded op, consults the phase map:
  if `{gu, p}` is `Draining`, serve that mount for the key's unit even
  though the live ring no longer lists this node at p. This keeps the wire
  unchanged but means the resolver is NOT purely ring-index-keyed (it ALSO
  reads the draining phase), which the design must state.

Either way, the design's current text is internally inconsistent: it
simultaneously asserts the draining mount is reachable "only via the forward
path" AND that the forward lands in the normal ring-index resolver AND that
the forward adds no new addressing. Pick (a) or (b) and make the predecessor
serve-by-ReplicaUnit explicit. Without this the overlap mount is, once
again, unreachable.

### NEW-P1-1 (UNDERSPECIFIED, PREDECESSOR IDENTIFICATION): the multi-backend reconcile keeps NO prior-ring snapshot, so "derive the predecessor from the pre-change replica set" has no source to derive from

The design says the new owner records the predecessor "from the pre-change
replica set ... derived from the membership/ring delta ... `unitReplicas(gu)`
evaluated against the prior ring generation/membership snapshot"
(overlap-handoff.md:171-179, 287-289; SPEC:1239).

But the MULTI-backend path does not keep a prior-ring snapshot. The LEGACY
per-node path keeps `lastEvalRing` and diffs (old, current) in `runEvaluate`
(`rebalance.go:282-283`, 667). The MULTI reconcile `reconcileReplicaUnits`
(`multibackend_replicated.go:224-254`) computes ONLY `desiredReplicaUnits()`
against the LIVE ring; it has no `old`/prior argument and never reads
`lastEvalRing`. `reconcileUnits`'s own doc (multibackend_rebalance.go:170-180)
says the multi reconcile diffs desired-vs-MOUNTED, not old-ring-vs-new-ring.

So "evaluate `unitReplicas(gu)` against the prior ring" is not a thing the
multi path can do today: there is no retained prior ring to evaluate against.
The design treats predecessor derivation as a given ("the cluster already
observes topology change events", overlap-handoff.md:176) but the artifact it
needs (the replica set computed against the ring shape BEFORE this
convergence) is not retained in the multi path.

Worse, the ring mutates IN PLACE on membership events (the ring object is
updated, ringGen bumped, reconcile debounced). By the time the debounced
reconcile runs, the live ring is already the post-change ring, and any
intermediate ring states (multi-hop churn during the settle window, see
NEW-P1-2) are gone. A single retained "ring at last reconcile" is the most
that is recoverable, and even that is not currently retained in the multi path.

REQUIRED AMENDMENT. The design must state that the multi reconcile gains a
retained prior-ring (or prior-desired-replica-set) snapshot - the multi
analogue of `lastEvalRing` - captured at the END of each reconcile, and that
the predecessor is `unitReplicas(gu)@prior \ unitReplicas(gu)@live` at the
moving position (the node that held position p before, that no longer does).
Name where this snapshot lives, when it is captured (must be the ring used by
the PREVIOUS reconcile, not "ring minus this event"), and that it is consulted
ONLY to identify the predecessor (not for routing). Without this the
implementer has nothing to derive the predecessor from and will either guess
(wrong) or invent an ad-hoc snapshot (un-reviewed).

### NEW-P1-2 (CORRECTNESS, MULTI-HOP / DOUBLE-MOVE): a position that moves twice within one settle window mis-identifies the predecessor, forwarding to a node that no longer holds the mount

Even granting a prior-ring snapshot (NEW-P1-1), the snapshot is point-to-
point: it captures the ring as of the LAST reconcile, not every intermediate
state. The settle timer debounces a BURST of membership events into one
reconcile (`scheduleReconcile`/`bumpRingGen` re-arm the timer). So position p
can move OLD -> MID -> NEW across two membership events that land in ONE
debounce window. When the reconcile finally runs:

- NEW computes predecessor = "who held p in the prior snapshot" = OLD.
- But OLD already dropped out and MID transiently became the owner (and may
  itself already be Draining or gone). The position's CURRENT live holder of
  the still-mounted draining copy might be MID, not OLD - or NOBODY mounted
  it at all because MID never finished acquiring before NEW took over.
- NEW forwards to OLD. OLD may have ALREADY released (its own Draining
  release-check fired against MID's readiness), or OLD may never have been
  the immediate predecessor in the first place.

The design's predecessor model is strictly single-hop ("the node that held
this exact ReplicaUnit in the PRE-change replica set", overlap-handoff.md:
171-174). It does not address a position that changes ownership more than
once between two reconciles, which the debounce window makes reachable under
real churn (a big-bang scale-out is exactly when multiple nodes sort into a
unit's replica list in quick succession).

The failure is not a lost write (durable-before-ack still holds; the
forwarded write either lands somewhere durable or fails and retries). It is
an AVAILABILITY failure: forwarding to a stale predecessor returns
errUnitAcquiring (predecessor released or never held it), so the position
falls back to Option-A retry for the duration - the bound Phase 2e claims to
remove, hit precisely under the big-churn case Phase 2e exists for.

REQUIRED AMENDMENT. Either (a) prove the debounce cannot coalesce a
double-move of one position (it can, so this is unlikely), or (b) make the
predecessor identification robust to multi-hop: e.g. the predecessor is
whoever currently has the position `Draining` (discoverable by probing the
candidate set from BOTH the prior and any intermediate snapshots), or fall
back to forwarding to ANY node currently reporting a Draining mount for the
position, or (c) explicitly scope Phase 2e to single-hop moves and route
multi-hop coalesced moves through the Option-A clean-cut belt (state that the
overlap is best-effort and degrades to Option A when the predecessor is
ambiguous). Option (c) is the honest minimal fix and keeps the blast radius
small, but it must be WRITTEN, because silently the design assumes single-hop.

### NEW-P1-3 (RELEASE SAFETY, RE-VERIFY RACE): the readiness re-verify is point-in-time, so OLD can release on the epoch-hint path moments before NEW crashes, after NEW sent Ready but before serving stabilized

The epoch-hint fallback releases OLD when `DurableEpochReplica(ru) >
myOpenEpoch` AND a readiness probe to NEW confirms NEW is serving
(overlap-handoff.md:244-251; SPEC:1250). The crash analysis (case 3,
overlap-handoff.md:342-350) argues that if NEW crashes after Ready, the
re-verify probe "now FAILS (NEW is gone)" so OLD does not release. That is
correct WHEN the probe and the crash are ordered probe-then-crash. But the
probe is a single point-in-time RPC, not a lease/latch:

- t: OLD's epoch-hint fires, OLD probes NEW, NEW answers "serving" (it IS, at
  t, mounted and Ready).
- t+e: OLD, having a positive confirmation, proceeds into the release
  critical section (mountMap CAS-delete + CloseReplicaUnit).
- t+e (concurrently): NEW crashes.

Between the positive probe and the completion of OLD's release there is a
window where NEW can die. OLD releases anyway (it already has its positive
confirmation; the design does not say OLD re-probes inside the critical
section, and it could not without holding I/O under the lock, which P1-3
forbids). Now OLD has released AND NEW is gone: the position is unserved
until the next reconcile reassigns it. The forwarded-op belt does not help
(OLD's mount is closed; NEW is gone).

This is a strictly smaller window than the bare-epoch hole P1-1 closed (it
needs NEW to crash in the probe-to-release gap, not merely to crash any time
after fencing), and on the AUTHORITATIVE RPC path it is even smaller (the
RPC arriving means NEW reached Ready; NEW crashing right after is the same
residual). It is a genuine residual, not a regression: ANY release decision
based on a point-in-time liveness check has this window, and the position is
recovered by the next reconcile (no lost write, durable-before-ack holds).

REQUIRED AMENDMENT (clarification, not a redesign). State explicitly that the
re-verify is a point-in-time liveness HINT and that a NEW crash in the
probe-to-release gap degrades to the same "reassign on next reconcile"
recovery as case 3, with NO acked-write loss (durable-before-ack). The
design currently implies the re-verify makes case 3 fully safe; it makes it
safe against a CRASHED-BEFORE-READY new owner, not against a
crash-in-the-release-gap. This is a P1 honesty fix (the recovery is the same
benign next-reconcile path), not a P0 - but the doc overclaims and should
not.

---

## PART 3: lost/double-write across the forward->local cutover

The prompt asks specifically about the forward->local cutover. The fence
argument (no acked write below E invisible, no acked write above E on OLD)
is sound and unchanged (confirmed in part 1, P1-2). The remaining question is
the SAME-KEY ordering at the instant NEW flips from forwarding to local:

- Just before the flip, NEW is forwarding writes for key k to OLD (durable on
  OLD below E).
- At the flip, NEW inserts `mountMap[ru]` (opened at E) and starts serving k
  locally.
- NEW's open recovered OLD's durable tail (the seal, P1-2), so the locally-
  served handle already reflects OLD's acked writes.

Could a write be lost or double-applied here? Analysis:

- LOST: a write forwarded to OLD just before the flip is acked only after it
  is durable on OLD (AwaitDurable). If its ack returned, it was durable
  before NEW's open completed its WAL recovery? NOT NECESSARILY - the open's
  WAL recovery snapshot is taken at open time, and a forwarded write can be
  durable on OLD AFTER NEW's open read the WAL but BEFORE the flip is visible.
  That write is below E (OLD is authoritative below E) and durable on OLD, but
  NEW already finished recovery and will NOT see it on the handle it just
  opened. After the flip, NEW serves locally and never re-reads OLD. >>> This
  is a potential LOST-READ-AFTER-WRITE: forwarded-write-acked-via-OLD,
  durable on OLD, NOT in NEW's recovered view, NEW now serving locally. <<<

  This is actually the SAME risk class as P1-2's WAL-tail (a write durable on
  OLD but not in NEW's mounted view), but P1-2 framed it as "writes acked
  BEFORE the fence". The forward path EXTENDS the window: a forwarded write
  can be acked via OLD AFTER NEW's open/recovery but still below E, because
  forwarding continues until the flip, and the flip is strictly after the
  open completes. So there is a sub-window [open-recovery-done .. flip-
  visible] where a forwarded write lands durable on OLD below E yet outside
  NEW's recovered tail.

  Whether this loses a write depends on the fence timing: does NEW's open at
  E fence OLD at the moment recovery completes, or at the moment the manifest
  bump is durable (earlier)? The fence is the manifest writer-epoch bump,
  which `OpenReplicaUnit` writes as part of the open (SPEC:1245,
  overlap-handoff.md:258-267). If the manifest bump (the fence) is durable
  AT open completion and recovery happens BEFORE the bump, then any write OLD
  acks after the bump is FENCED (rejected, never acked) - so the only writes
  that ack on OLD are below the bump, i.e. before recovery's cutoff, i.e.
  recovered. In that ordering there is NO lost write. But the design never
  states the INTERNAL ordering of {WAL recovery, manifest-epoch bump} within
  a single `OpenReplicaUnit`. If recovery reads the WAL tail and THEN the
  bump is written, a write that lands on OLD between those two steps is acked
  (OLD not yet fenced) and missed (recovery already done).

  REQUIRED: the seal argument must pin the INTRA-open ordering: the
  fence (manifest epoch bump) must be effective NO LATER than the WAL-
  recovery cutoff, so every write OLD can still ack is in the recovered tail.
  Equivalently: forwarding must stop and OLD must be fenced BEFORE NEW's
  recovery snapshot is finalized, OR NEW must re-scan the WAL tail after the
  fence is effective. This is a refinement of P1-2, but P1-2 as written only
  covers "acked before T_fence"; the forward path needs "acked before the
  fence is EFFECTIVE, and the recovery cutoff is at-or-after the fence." Add
  this to the explicit-assumption pin test: write via the forward path
  CONCURRENTLY with the flip, assert readable on NEW after.

- DOUBLE-APPLY: apply-if-newer (LWW envelope) makes a replayed/reordered
  envelope a no-op (SPEC:1258, guarantee 2). A write that lands on both OLD
  (forwarded, below E) and is re-driven to NEW (retry after the flip) is
  idempotent by the LWW stamp. No double-apply. This is sound.

So: no double-apply. The lost-write risk is a real REFINEMENT of P1-2 (the
intra-open fence-vs-recovery ordering), promoted here because the forward
path widens the window beyond "before T_fence" to "before the fence is
effective, concurrent with the flip." Call it NEW-P1-4.

### NEW-P1-4 (LOST WRITE, INTRA-OPEN ORDERING): forwarding continues until the flip, which is after open completes, so a forwarded write acked on OLD between NEW's WAL-recovery cutoff and the fence becoming effective is durable below E but outside NEW's mounted view

See the analysis above. Fix: pin the intra-`OpenReplicaUnit` ordering so the
manifest-epoch fence is effective no later than the WAL-recovery cutoff (so
no write OLD can still ack escapes NEW's recovered tail), and extend the
explicit-assumption pin test to write-through-the-forward-CONCURRENT-with-
the-flip, not only write-just-before-fence. P1 not P0 because it is gated by
the same MUST-VERIFY mechanism as P1-2 and shares its blocking discipline;
but the doc's current "acked before T_fence" framing does not cover it and
must be widened.

---

## PART 4: forwarded op landing on OLD after OLD released

The prompt asks: order is NEW flips -> stops forwarding -> sends Ready RPC ->
OLD releases. Can a forwarded op land on OLD AFTER OLD released?

The dovetail (overlap-handoff.md:269-279; SPEC:1243) argues no: NEW forwards
ONLY while `Acquiring`; it stops forwarding at the flip; it sends Ready only
AFTER leaving Acquiring. So NEW issues its LAST forward strictly before it
sends Ready, and OLD releases only on Ready. Sequentially on NEW this holds.
BUT a forwarded op is an async RPC in flight on the network:

- NEW issues forward F for key k at the last instant of Acquiring (just
  before the flip).
- NEW flips, stops issuing NEW forwards, sends Ready.
- Ready reaches OLD; OLD releases (CloseReplicaUnit).
- F (still in flight, issued before the flip) arrives at OLD AFTER OLD
  released.

F lands on OLD's `LocalReplicaPut` after CloseReplicaUnit. The mount is gone,
so the resolver returns ok=false -> `errUnitAcquiring`, recoded transient,
returned to NEW. NEW is now serving locally (flipped); the originator's
fan-out retries F and it lands on NEW locally. So F is NOT lost (it failed on
OLD, retried onto NEW). No double-apply (LWW). This is SAFE - but only
because F FAILED on OLD rather than succeeding against a torn-down handle.

The residual concern: if OLD's CloseReplicaUnit is NOT atomic with the
mountMap delete (it is not - the design closes AFTER the critical section,
overlap-handoff.md:540-542, SPEC:1264), there is a window where the mountMap
entry is deleted but CloseReplicaUnit is still flushing. A forward F arriving
in that window finds ok=false (entry deleted first) -> errUnitAcquiring ->
retry onto NEW. Good. And a forward arriving AFTER CloseReplicaUnit finds the
backend closed -> error -> retry. Also good. So the mountMap-delete-before-
close ordering (already the existing `releaseReplicaUnit` shape,
multibackend_replicated.go:282-288) protects this. NO new hole here, PROVIDED
the drainCheck release uses the same delete-before-close ordering (it does,
SPEC:1264). Confirmed safe; documenting it because the prompt asked.

One refinement worth stating: this safety depends on NEW having ALREADY
flipped when F's retry lands (so the retry resolves locally on NEW). NEW
sends Ready AFTER flipping, and OLD releases AFTER Ready, so by the time OLD
has released NEW is definitely flipped - the retry of any in-flight F
necessarily lands after NEW flipped. The ordering is self-consistent. This
case is CLOSED.

---

## PART 5: scattered-flags check

The amended design preserves state-machine discipline. The complete state is
"phase value + mount entry + (in Acquiring) recorded predecessor"
(overlap-handoff.md:64-66; SPEC:1235), all keyed by ReplicaUnit, no
`isDraining`/`hasFlipped`/`pendingRelease` booleans. The plain-release path
stays the existing eager-delete with NO stray flag (overlap-handoff.md:
544-557; SPEC:1276). The predecessor address rides INSIDE `HandoffState`, not
as a sidecar map. NO scattered flags reintroduced. CONFIRMED.

One watch-item for the implementer (not a finding): NEW-P0's fix (predecessor
serving a Draining mount for a forwarded op) must NOT become a parallel
"draining backend" lookup outside the phase map. If fix (b) is chosen
(resolver consults the phase map for a Draining entry), that read must go
through the same `handoffPhase`/`mountMap[ru]` structures, not a new map.

---

## Summary (prioritized)

Prior holes: ALL CLOSED at the argument level (P0-1, P0-2, P1-1, P1-2, P1-3,
P1-4, P2-1/2/3), with P1-1 subject to the benign re-verify-race honesty fix
(NEW-P1-3) and P1-2 subject to the widened-window fix (NEW-P1-4).

NEW holes the forwarding approach introduces:

- NEW-P0 (must fix before build): the forwarded op cannot reach the
  predecessor's DRAINING mount, because the ReplicaUnit-keying amendment made
  the resolver ring-index-keyed and the predecessor is off the ring for that
  position. The "reuses existing forwarded-op RPCs, no new proto" claim is
  false. Fix: position-addressed forward (carry the ReplicaUnit) OR make the
  predecessor's forwarded-op resolver serve a Draining entry by explicit ru.
  overlap-handoff.md:121-128 vs 181-190 vs 511-512 are internally
  contradictory.

- NEW-P1-1: predecessor identification has no source - the multi reconcile
  keeps NO prior-ring snapshot (only the legacy path has `lastEvalRing`). Add
  a retained prior-replica-set snapshot to the multi reconcile and define the
  predecessor as the prior-minus-live holder of the moving position.

- NEW-P1-2: multi-hop / double-move within one debounce window mis-identifies
  the predecessor (forwards to a node that already released). Scope to
  single-hop with an Option-A fallback for ambiguous predecessors, OR make
  predecessor discovery robust to intermediate ring states.

- NEW-P1-3: the readiness re-verify is point-in-time; a NEW crash in the
  probe-to-release gap lets OLD release with nobody serving. Benign (next
  reconcile recovers, no acked-write loss) but the doc overclaims case-3
  safety; state the residual.

- NEW-P1-4: the forward path widens P1-2's lost-write window from "acked
  before T_fence" to "acked before the fence is EFFECTIVE, concurrent with
  the flip." Pin the intra-open ordering (fence effective <= WAL-recovery
  cutoff) and extend the pin test to write-concurrent-with-flip.

No new P2s beyond the doc-consistency cleanups implied by NEW-P0 (remove the
"no new proto" / "reuses existing RPCs unchanged" claim once the forward
carries a ReplicaUnit).

The core idea remains sound and worth building; NEW-P0 is the one that, left
unfixed, makes the amended design fail its own acceptance gate the same way
the original P0-1 did.
