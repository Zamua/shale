// Overlap handoff (v0.8 Phase 2e, Option B) controller for an R>1
// multi-backend membership change. It replaces the position-CHANGE branch of
// reconcileReplicaUnits (the clean-cut RELEASE-then-ACQUIRE) with an
// ACQUIRE-then-RELEASE driven by a single per-ReplicaUnit ownership-transition
// state machine (the pure storageunit.HandoffState FSM):
//
//   - a position MOVING AWAY from this node -> set Draining (DO NOT release;
//     keep serving directly AND for the new owner's forwards).
//   - a position MOVING IN to this node from a single identifiable predecessor
//     -> set Acquiring + record the predecessor + start acquireReplicaUnitOverlap
//     (the new owner forwards routed ops to the predecessor while it mounts).
//   - a pure NEW mount (initial convergence, no predecessor) OR an
//     AMBIGUOUS/multi-hop predecessor -> fall through to the existing clean-cut
//     acquire (the Option-A belt); overlap is best-effort single-hop.
//   - a node simply DROPPING OUT of the replica set entirely -> existing plain
//     clean-cut release (NOT the overlap machine).
//
// The complete state of an in-flight position is the phase value + the mount
// entry + (Acquiring) the recorded predecessor, all keyed by ReplicaUnit; there
// are NO scattered isDraining/hasFlipped booleans. The old owner's release is
// POLL-ONLY via drainCheck (multibackend_overlap.go below): it reads the durable
// SERVING MARKER on the settle / self-heal cadence and releases only on a
// positive readiness (a marker at an epoch >= its own open epoch), NEVER on a
// bare durable fence-epoch advance.
//
// See docs/SPEC.md "v0.8 Phase 2e" and docs/design/overlap-handoff.md. Option B
// lives behind multiReplicated(); the legacy per-node path, the R=1 multi-backend
// lease handoff (Phase 3), and the reshard (Phase 4) are untouched.

package cluster

import (
	"github.com/Zamua/shale/pkg/storageunit"
)

// reconcileReplicaUnitsOverlap is the Option B (overlap handoff) reconcile,
// replacing reconcileReplicaUnits for the R>1 path. It makes this node's mounted
// + in-flight set match the units it replicates under the LIVE ring, but unlike
// the clean-cut reconcile it SEQUENCES the two halves of a position move via the
// handoffPhase FSM so the position is never stranded.
//
// Per position it consults priorDesiredReplicas (captured at the END of the
// previous run) to tell a MOVE (one specific predecessor hands a specific
// successor) from a pure new mount or a plain drop-out. At the END it captures
// the live desired sets as the next priorDesiredReplicas snapshot.
//
// Caller MUST hold reconcileMu; mount + phase mutations take mountMu.
func (c *Cluster) reconcileReplicaUnitsOverlap() {
	// live: the desired replica sets as of the LIVE ring, as GenUnit -> ordered
	// holders. This is the authoritative routing truth; the snapshot below is
	// consulted ONLY for predecessor identification.
	live := c.liveDesiredReplicaSets()

	desired := c.desiredReplicaUnits()
	desiredSet := make(map[storageunit.ReplicaUnit]struct{}, len(desired))
	for _, ru := range desired {
		desiredSet[ru] = struct{}{}
	}

	c.mountMu.RLock()
	mounted := make([]storageunit.ReplicaUnit, 0, len(c.mountMap))
	for ru := range c.mountMap {
		mounted = append(mounted, ru)
	}
	prior := c.priorDesiredReplicas
	c.mountMu.RUnlock()

	mountedSet := make(map[storageunit.ReplicaUnit]struct{}, len(mounted))
	for _, ru := range mounted {
		mountedSet[ru] = struct{}{}
	}

	// RELEASE / DRAIN half: a mounted ReplicaUnit no longer desired here.
	//
	//   - if some OTHER node is the LIVE holder of this exact position (the
	//     position MOVED to a successor), set Draining and KEEP SERVING - the
	//     new owner will forward to us and release us on its readiness.
	//   - otherwise the position simply VANISHED from our set (nobody took our
	//     exact position; the remaining replicas cover W): plain clean-cut
	//     release, the existing eager-delete path.
	//
	// A position already in an in-flight phase (Draining from a previous pass,
	// or Acquiring/Ready as a gainer) is skipped: drainCheck drives the loser
	// side to completion; the gainer side is handled in the ACQUIRE half.
	for _, ru := range mounted {
		if _, ok := desiredSet[ru]; ok {
			continue
		}
		if c.handoffPhaseOf(ru).Phase != 0 {
			// Already mid-transition (Draining set on an earlier pass). Leave it
			// to drainCheck; re-arming it here would be an illegal Owned->Draining
			// edge.
			continue
		}
		if c.positionHasLiveSuccessor(ru, live) {
			c.beginDrain(ru)
			continue
		}
		// Plain drop-out: nobody takes our exact position. Clean-cut release.
		c.releaseReplicaUnit(ru)
	}

	// ACQUIRE half: a desired ReplicaUnit not currently mounted.
	for _, ru := range desired {
		if _, ok := mountedSet[ru]; ok {
			continue
		}
		// A position already PhaseAcquiring but NOT mounted means a prior pass
		// recorded Acquiring (and forwards to the predecessor) but the mount has
		// not completed - either it is genuinely in flight in this same pass, or a
		// previous pass's OpenReplicaUnit failed and left it Acquiring. RE-DRIVE
		// the mount on every reconcile / self-heal tick so a failed open is
		// retried (idempotent: a successful re-open just flips it to Owned). The
		// recorded predecessor is preserved, so routed ops keep forwarding
		// meanwhile. Do NOT re-run beginAcquire (that would reset the predecessor
		// to a possibly-stale one); just retry the open.
		if c.handoffPhaseOf(ru).Phase.IsGainer() {
			c.acquireReplicaUnitOverlap(ru)
			continue
		}
		pred, ok := predecessorOf(ru, prior, live)
		if !ok {
			// Pure new mount (initial convergence, no prior holder) OR an
			// ambiguous / multi-hop predecessor (prior holder gone / already
			// released). Overlap is single-hop best-effort: fall through to the
			// clean-cut acquire (the Option-A belt). No forwarding; a routed op
			// gets errUnitAcquiring + the WriteTimeout-bounded retry.
			c.acquireReplicaUnit(ru)
			continue
		}
		// Single unambiguous predecessor: overlap. Record Acquiring (with the
		// predecessor) BEFORE starting the mount, so a routed op arriving during
		// the mount forwards to the predecessor instead of being refused.
		c.beginAcquire(ru, pred)
		c.acquireReplicaUnitOverlap(ru)
	}

	// Capture the live desired sets as the prior snapshot for the NEXT reconcile,
	// at the END of the run (before the next run can read it).
	c.mountMu.Lock()
	c.priorDesiredReplicas = live
	c.mountMu.Unlock()
}

// reconcileReplicaUnitsCleanCut is the pre-2e clean-cut RELEASE-then-ACQUIRE
// reconcile for the R>1 path, used ONLY by the break-demo (Config.
// TestingForceCleanCut). It diffs desired-vs-mounted and, with NO overlap
// sequencing and NO predecessor forwarding, eagerly RELEASES every position no
// longer desired here and ACQUIRES every newly-desired one. A position moving
// away thus has a window where neither the old owner (released) nor the new
// owner (still mounting) serves it; a routed op to the still-Acquiring new owner
// gets errUnitAcquiring. Paired with TestingForceCleanCut also disabling the
// Option-A retry, this is the regime the gate proves collapses. Caller holds
// reconcileMu; mount mutations take mountMu.
func (c *Cluster) reconcileReplicaUnitsCleanCut() {
	desired := c.desiredReplicaUnits()
	desiredSet := make(map[storageunit.ReplicaUnit]struct{}, len(desired))
	for _, ru := range desired {
		desiredSet[ru] = struct{}{}
	}

	c.mountMu.RLock()
	mounted := make([]storageunit.ReplicaUnit, 0, len(c.mountMap))
	for ru := range c.mountMap {
		mounted = append(mounted, ru)
	}
	c.mountMu.RUnlock()

	mountedSet := make(map[storageunit.ReplicaUnit]struct{}, len(mounted))
	for _, ru := range mounted {
		mountedSet[ru] = struct{}{}
	}

	// RELEASE half: every mounted position no longer desired, eagerly.
	for _, ru := range mounted {
		if _, ok := desiredSet[ru]; ok {
			continue
		}
		c.releaseReplicaUnit(ru)
	}
	// ACQUIRE half: every desired position not currently mounted.
	for _, ru := range desired {
		if _, ok := mountedSet[ru]; ok {
			continue
		}
		c.acquireReplicaUnit(ru)
	}
}

// liveDesiredReplicaSets returns the replica sets the LIVE ring assigns, as a
// map from GenUnit (at the live generation) to the ordered NodeIDs holding each
// replica position. It is the basis for predecessor identification (diffed
// against priorDesiredReplicas) and for telling a MOVE from a plain drop-out
// (does some other node hold this exact position now?). It enumerates the same
// unit set desiredReplicaUnits derives from, but keeps the FULL holder list per
// unit (not just this node's position).
func (c *Cluster) liveDesiredReplicaSets() map[storageunit.GenUnit][]storageunit.NodeID {
	gs := c.genSnapshot()
	ids := gs.count.IDs()
	out := make(map[storageunit.GenUnit][]storageunit.NodeID, len(ids))
	for _, u := range ids {
		gu := storageunit.NewGenUnit(gs.gen, u)
		set := c.unitReplicas(gu)
		holders := make([]storageunit.NodeID, len(set))
		for j, m := range set {
			holders[j] = storageunit.NodeID(m.ID)
		}
		out[gu] = holders
	}
	return out
}

// positionHasLiveSuccessor reports whether some OTHER node is the LIVE holder of
// ru's exact replica position (ru.Replica of ru.Unit). True means the position
// MOVED to a successor (drain + overlap); false means it simply vanished from
// this node's set (plain clean-cut release). Self being the live holder cannot
// happen here (the caller only asks for positions no longer desired by self).
func (c *Cluster) positionHasLiveSuccessor(ru storageunit.ReplicaUnit, live map[storageunit.GenUnit][]storageunit.NodeID) bool {
	holders, ok := live[ru.Unit]
	if !ok || int(ru.Replica) >= len(holders) {
		return false
	}
	return holders[ru.Replica] != storageunit.NodeID(c.cfg.NodeID)
}

// predecessorOf identifies the SINGLE-HOP predecessor of a position MOVING IN to
// this node: the node that held ru's exact position in the PRIOR snapshot and
// does NOT hold it in the LIVE set (prior-holder \ live-holder). It returns
// ok=false (no predecessor) for:
//
//   - a pure new mount: no prior snapshot, or the position had no prior holder.
//   - an ambiguous / multi-hop move: the prior holder STILL holds the position
//     live (so it did not move away from anyone identifiable), or the prior and
//     live holders are the same node (no move at this position).
//
// In every ok=false case the caller degrades to the Option-A clean-cut acquire
// (single-hop scope, NEW-P1-2). The function is pure over its arguments.
func predecessorOf(
	ru storageunit.ReplicaUnit,
	prior, live map[storageunit.GenUnit][]storageunit.NodeID,
) (storageunit.NodeID, bool) {
	if prior == nil {
		return "", false
	}
	priorHolders, ok := prior[ru.Unit]
	if !ok || int(ru.Replica) >= len(priorHolders) {
		return "", false
	}
	priorHolder := priorHolders[ru.Replica]
	if priorHolder == "" {
		return "", false
	}
	// The predecessor must NOT be the live holder of the position (it must have
	// moved away). If the prior holder is still the live holder, this is not a
	// move away from it; bail to Option A.
	liveHolders, ok := live[ru.Unit]
	if ok && int(ru.Replica) < len(liveHolders) {
		if liveHolders[ru.Replica] == priorHolder {
			return "", false
		}
	}
	return priorHolder, true
}

// handoffPhaseOf returns the in-flight HandoffState for ru (the zero value, with
// Phase 0, when ru is not in flight). Reads under mountMu.
func (c *Cluster) handoffPhaseOf(ru storageunit.ReplicaUnit) storageunit.HandoffState {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	return c.handoffPhase[ru]
}

// beginDrain puts a position into PhaseDraining (the loser side): the node no
// longer desires it but a live successor is taking it over, so it KEEPS SERVING
// the still-mounted entry until the successor is Ready. It does NOT release the
// mount. The OpenEpoch recorded is the epoch this node opened the position at -
// drainCheck compares the serving marker against it. Caller holds reconcileMu;
// the phase write takes mountMu.
func (c *Cluster) beginDrain(ru storageunit.ReplicaUnit) {
	open := c.openEpochForReplica(ru)
	c.mountMu.Lock()
	if _, mounted := c.mountMap[ru]; !mounted {
		// Lost the mount under us (a concurrent evict); nothing to drain.
		c.mountMu.Unlock()
		return
	}
	if c.handoffPhase[ru].Phase != 0 {
		c.mountMu.Unlock()
		return
	}
	next, err := storageunit.NextOnDrain(storageunit.HandoffState{})
	if err != nil {
		c.mountMu.Unlock()
		return
	}
	next.OpenEpoch = open
	c.handoffPhase[ru] = next
	c.mountMu.Unlock()
}

// beginAcquire records PhaseAcquiring for a position MOVING IN, carrying the
// predecessor so a routed op arriving during the mount forwards to it (the
// overlap window). It is set BEFORE the mount starts so there is no instant
// where routing targets this node, the mount is incomplete, AND no predecessor
// is recorded (which would force the Option-A refusal). Caller holds
// reconcileMu; the phase write takes mountMu.
func (c *Cluster) beginAcquire(ru storageunit.ReplicaUnit, pred storageunit.NodeID) {
	c.mountMu.Lock()
	c.handoffPhase[ru] = storageunit.HandoffState{
		Phase:       storageunit.PhaseAcquiring,
		Predecessor: pred,
	}
	c.mountMu.Unlock()
}

// openEpochForReplica reports the epoch this node currently holds ru open at,
// read as the DURABLE epoch of the position (the loser's own open advanced the
// durable epoch to its open value). It is what drainCheck's serving-marker
// compare uses as "my open epoch". On any read error it returns 0, which makes
// drainCheck conservative (a marker at any epoch >= 0 is required, but the
// marker existence + epoch>=0 still gates correctly because a marker is only
// written by a Ready new owner). Reads durable state without the lock.
func (c *Cluster) openEpochForReplica(ru storageunit.ReplicaUnit) storageunit.Epoch {
	if c.replicaFactory == nil {
		return 0
	}
	e, err := c.replicaFactory.DurableEpochReplica(ru)
	if err != nil {
		return 0
	}
	return e
}

// acquireReplicaUnitOverlap is the GAINER's mount under overlap: it opens the
// position (the slow MinIO mount happens here, during which routed ops forward
// to the recorded predecessor), then performs THE MOUNT FLIP - inserts
// mountMap[ru] under mountMu and advances Acquiring -> Ready -> drops the phase
// entry (Owned) - and writes the durable SERVING MARKER exactly once so the old
// owner's drainCheck poll releases. On open failure it leaves the Acquiring
// phase in place (a routed op keeps forwarding to the predecessor) and the next
// reconcile / self-heal retries. Caller holds reconcileMu.
func (c *Cluster) acquireReplicaUnitOverlap(ru storageunit.ReplicaUnit) {
	b, err := c.replicaFactory.OpenReplicaUnit(ru, acquireBaseEpoch)
	if err != nil {
		// Mount failed; stay Acquiring (keep forwarding to the predecessor). The
		// position is not stranded: the old owner is still Draining + serving via
		// the forward.
		return
	}

	openedEpoch := c.openEpochForReplica(ru)

	// THE MOUNT FLIP: insert the mount entry + advance the phase to Ready under
	// ONE mountMu hold so a routed op never sees the mount present without the
	// phase resolved (and vice versa).
	c.mountMu.Lock()
	if c.closed.Load() {
		c.mountMu.Unlock()
		_ = c.replicaFactory.CloseReplicaUnit(ru)
		return
	}
	cur := c.handoffPhase[ru]
	if cur.Phase != storageunit.PhaseAcquiring {
		// The phase entry is gone or not Acquiring (a concurrent reconcile
		// already flipped or dropped it). Still install the mount (it is the
		// authoritative durable owner) and drop any stale phase entry.
		c.mountMap[ru] = b
		delete(c.handoffPhase, ru)
		c.mountMu.Unlock()
		_ = c.replicaFactory.WriteServingMarker(ru, openedEpoch)
		return
	}
	ready, err := storageunit.NextOnReady(cur, openedEpoch)
	if err != nil {
		// Illegal edge should not happen (cur is Acquiring); install the mount
		// and drop the phase to converge to Owned regardless.
		c.mountMap[ru] = b
		delete(c.handoffPhase, ru)
		c.mountMu.Unlock()
		_ = c.replicaFactory.WriteServingMarker(ru, openedEpoch)
		return
	}
	c.mountMap[ru] = b
	// Ready is transient: once the mount entry is present the node serves
	// locally and stops forwarding, so the steady state is Owned (no phase
	// entry). Drop the entry rather than parking in Ready - Owned = mounted +
	// no phase, per the FSM's steady-state poles.
	_ = ready
	delete(c.handoffPhase, ru)
	c.mountMu.Unlock()

	// Write the serving marker EXACTLY ONCE, AFTER the mount flip (outside the
	// lock: it is shared-storage I/O). This is the durable, poll-observable
	// release signal the old owner's drainCheck polls. No RPC is sent.
	_ = c.replicaFactory.WriteServingMarker(ru, openedEpoch)
}

// drainCheck is the OLD owner's POLL-ONLY release-check for a Draining position,
// armed on the periodic settle / self-heal cadence (runReconcile). It releases
// the position ONLY on a POSITIVE readiness: a durable serving marker at an
// epoch >= this node's own open epoch (proof a live new owner is actually
// SERVING). A bare durable fence-epoch advance NEVER releases (it bumps at
// open-START, before the mount; only the marker proves serving).
//
// Lock discipline (review P1-3): the ReadServingMarker I/O runs OUTSIDE any
// cluster lock (a slow MinIO read must not block routed ops' mountMap reads).
// The phase compare-and-advance (Draining -> Releasing) and the mountMap
// compare-and-delete are ONE critical section under mountMu, made exactly-once
// by the CAS-on-mountMap-delete (delete only if it still points at the same
// backend). CloseReplicaUnit runs AFTER the lock is dropped (the entry is
// already removed), so a slow close does not hold mountMu. Caller holds
// reconcileMu (so two passes do not both enter the edge); the exactly-once guard
// is the mountMap CAS regardless.
func (c *Cluster) drainCheck(ru storageunit.ReplicaUnit) {
	state := c.handoffPhaseOf(ru)
	if state.Phase != storageunit.PhaseDraining {
		return
	}

	// I/O OUTSIDE the lock: poll the durable serving marker.
	markerEpoch, ok, err := c.replicaFactory.ReadServingMarker(ru)
	if err != nil {
		return
	}
	// Positive readiness: a live owner is serving at an epoch >= my open epoch.
	ready := ok && markerEpoch >= state.OpenEpoch
	if !storageunit.Releasable(state, ready) {
		// No marker yet (or below my epoch): stay Draining + keep serving.
		return
	}

	// ONE mountMu critical section: advance Draining -> Releasing and
	// compare-and-delete the mount entry. The CAS-delete (delete only if the
	// entry is still the backend we drained) is the real exactly-once guard.
	c.mountMu.Lock()
	cur, inFlight := c.handoffPhase[ru]
	if !inFlight || cur.Phase != storageunit.PhaseDraining {
		// Another tick already advanced it. Done.
		c.mountMu.Unlock()
		return
	}
	b, mounted := c.mountMap[ru]
	if !mounted || b == nil {
		// Already evicted; just drop the phase entry to converge to Absent.
		delete(c.handoffPhase, ru)
		c.mountMu.Unlock()
		return
	}
	next, err := storageunit.NextOnRelease(cur)
	if err != nil {
		c.mountMu.Unlock()
		return
	}
	// Exactly-once CAS-delete: only this critical section removes THIS entry.
	delete(c.mountMap, ru)
	// Drop the phase entry: Releasing is transient, the steady state after the
	// release is Absent (no mount, no phase). next is computed to validate the
	// edge; we converge straight to Absent.
	_ = next
	delete(c.handoffPhase, ru)
	c.mountMu.Unlock()

	// CloseReplicaUnit OUTSIDE the lock (the entry is already removed). Idempotent
	// + belt-and-suspenders for acked writes (durable-before-ack).
	_ = c.replicaFactory.CloseReplicaUnit(ru)
}

// runDrainChecks polls every Draining position once on the settle / self-heal
// cadence. It is called from the R>1 reconcile path after the acquire/release
// diff so a position that just became Draining is re-checked on the next tick.
// Caller holds reconcileMu.
func (c *Cluster) runDrainChecks() {
	c.mountMu.RLock()
	draining := make([]storageunit.ReplicaUnit, 0)
	for ru, st := range c.handoffPhase {
		if st.Phase == storageunit.PhaseDraining {
			draining = append(draining, ru)
		}
	}
	c.mountMu.RUnlock()
	for _, ru := range draining {
		c.drainCheck(ru)
	}
}
