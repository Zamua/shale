// Pending-ranges handoff (v0.8 Phase 2e) controller for an R>1 multi-backend
// membership transition. It replaces the position-CHANGE branch of the
// clean-cut reconcile (RELEASE-then-ACQUIRE) with an ACQUIRE-then-RELEASE driven
// by a single per-ReplicaUnit transition state machine (the pure
// storageunit.HandoffState FSM) and the current/pending split:
//
//   - CURRENT = unitReplicas over the ring INCLUDING draining members (who owns
//     a position TODAY). PENDING = unitReplicas over the ring EXCLUDING draining
//     members (who owns it once the leaver is gone). A position is IN TRANSITION
//     when CURRENT != PENDING.
//   - a position this node holds in CURRENT but not in PENDING -> set Draining
//     (DO NOT release; STAY a routed current owner and keep serving, dual-written
//     via the union, until the successor's serving marker gates the release).
//   - a position this node holds in PENDING but not in CURRENT (it WILL own it
//     once the leaver departs) -> set Acquiring + start acquireReplicaUnitOverlap
//     (mount the per-(unit, replica) database in the background; the union covers
//     the position via the still-mounted current owner during the mount, then on
//     mount-complete this node writes its SERVING MARKER which gates the leaver's
//     release). There is NO predecessor to remember and NO per-position forward:
//     the union routes writes + reads DIRECTLY to both the current and pending
//     owners.
//   - a pure NEW mount (initial convergence, no transition) -> the existing
//     clean-cut acquire.
//   - a position simply DROPPING OUT of this node's replica set with no pending
//     successor taking its exact slot -> the existing plain clean-cut release.
//
// The complete state of an in-flight position is the phase value + the mount
// entry, both keyed by ReplicaUnit; there are NO scattered booleans and NO
// predecessor address. The old owner's release is POLL-ONLY via drainCheck
// (below): it reads the durable SERVING MARKER on the settle / self-heal cadence
// and releases only on a positive readiness (a marker at an epoch strictly above
// its own open epoch), NEVER on a bare durable fence-epoch advance.
//
// See docs/SPEC.md "v0.8 Phase 2e" and docs/design/overlap-handoff.md. The
// pending-ranges path lives behind multiReplicated(); the legacy per-node path,
// the R=1 multi-backend lease handoff (Phase 3), and the reshard (Phase 4) are
// untouched.

package cluster

import (
	"github.com/Zamua/shale/pkg/storageunit"
)

// reconcileReplicaUnitsOverlap is the pending-ranges reconcile, replacing
// reconcileReplicaUnits for the R>1 path. It makes this node's mounted +
// in-flight set match the units it replicates under the LIVE ring, but unlike
// the clean-cut reconcile it SEQUENCES the two halves of a position move via the
// handoffPhase FSM so the position is never stranded:
//
//   - DRAIN half: a mounted position this node holds in CURRENT but not in
//     PENDING is set Draining (it keeps serving + receiving union dual-writes;
//     drainCheck releases it on the successor's serving marker).
//   - ACQUIRE half: a position this node holds in PENDING but not in CURRENT, not
//     yet mounted, is set Acquiring + mounted in the background (the gainer); on
//     mount-complete it writes its serving marker. A pure new mount with no
//     transition takes the clean-cut acquire.
//
// Caller MUST hold reconcileMu; mount + phase mutations take mountMu.
func (c *Cluster) reconcileReplicaUnitsOverlap() {
	draining := c.drainingIDs()

	// current = positions the LIVE ring (including draining members) assigns this
	// node. desired = current here: the set this node owns + must keep mounted
	// (current owners stay mounted throughout a transition).
	current := c.desiredReplicaUnits()
	currentSet := make(map[storageunit.ReplicaUnit]struct{}, len(current))
	for _, ru := range current {
		currentSet[ru] = struct{}{}
	}

	// pending = positions this node WILL own once the draining members depart
	// (the ring EXCLUDING draining). In steady state (no draining members)
	// pending == current and the transition halves below are empty.
	pending := c.desiredPendingReplicaUnits(draining)
	pendingSet := make(map[storageunit.ReplicaUnit]struct{}, len(pending))
	for _, ru := range pending {
		pendingSet[ru] = struct{}{}
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

	// RECLAIM half: a position this node is DRAINING (gave up on an earlier pass)
	// but now holds in CURRENT again, while it is STILL mounted. This happens when
	// the ring flip-flops a position back (e.g. a draining node's gradual gossip
	// convergence, or a draining member that recovered). The drain is waiting for
	// a SUCCESSOR's serving marker, but this node is the live holder again, so
	// drainCheck would never release it and the position would be stranded in
	// Draining forever. Abort the drain: clear the in-flight phase so the position
	// returns to Owned (it has been mounted + serving the whole time, so no
	// availability gap). Reads under mountMu; the phase clear takes the lock.
	for _, ru := range mounted {
		if _, ok := currentSet[ru]; !ok {
			continue
		}
		if c.handoffPhaseOf(ru).Phase.IsLoser() {
			c.reclaimDrainingPosition(ru)
		}
	}

	// DRAIN half: a mounted position this node holds in CURRENT but no longer in
	// PENDING (a draining successor split removed it). Set Draining + KEEP SERVING
	// (stay a routed current owner; the union dual-writes it; drainCheck releases
	// on the successor's marker).
	//
	// A position that VANISHED from CURRENT entirely (nobody, not even via a
	// draining split, keeps it here - e.g. this node simply dropped out of the
	// replica set) takes the plain clean-cut release: there is no pending
	// successor mounting this exact slot, so there is nothing to drain.
	for _, ru := range mounted {
		if _, inCurrent := currentSet[ru]; inCurrent {
			// Still a current owner. Drain it only if it is leaving the PENDING set
			// (a draining successor is taking it over); otherwise keep it Owned.
			if _, inPending := pendingSet[ru]; inPending {
				continue // owned in both current + pending: steady state, keep it.
			}
			if c.handoffPhaseOf(ru).Phase != 0 {
				// Already mid-transition (Draining from an earlier pass). Leave it to
				// drainCheck; re-arming would be an illegal Owned->Draining edge.
				continue
			}
			c.beginDrain(ru)
			continue
		}
		// Not in CURRENT at all: this node is no longer a live owner of this exact
		// position. If it is already mid-transition (Draining), leave it to
		// drainCheck. Otherwise plain clean-cut release (no successor mounts THIS
		// node's exact slot; the surviving replicas cover W).
		if c.handoffPhaseOf(ru).Phase != 0 {
			continue
		}
		c.releaseReplicaUnit(ru)
	}

	// ACQUIRE half (PENDING owner): a position this node holds in PENDING but not
	// in CURRENT, and not yet mounted. This is the pending-owner acquire TRIGGER:
	// the draining-exclusion split makes this node the future owner of the
	// leaver's exact slot, so it mounts that ReplicaUnit (the leaver's durable
	// copy in shared storage) in the background and writes its serving marker on
	// mount-complete, which gates the leaver's release. The union covers the
	// position via the still-mounted leaver during the mount.
	for _, ru := range pending {
		if _, inCurrent := currentSet[ru]; inCurrent {
			continue // already a current owner + mounted; nothing to acquire.
		}
		if _, isMounted := mountedSet[ru]; isMounted {
			// Already mounted this pending position (a prior pass acquired it). If it
			// is still PhaseAcquiring with the mount present (the flip races), let it
			// resolve; otherwise it is Owned and serving.
			continue
		}
		st := c.handoffPhaseOf(ru)
		if st.Phase.IsLoser() {
			// A loser-phase entry that is now RE-DESIRED as a pending position must
			// NOT be re-acquired here: drainCheck must finish the drain first (it
			// deletes the phase entry on release); a later reconcile then sees Phase 0
			// + unmounted + pending and acquires cleanly.
			continue
		}
		if st.Phase.IsGainer() {
			// Already Acquiring but not mounted: a prior pass recorded Acquiring but
			// the mount has not completed, or a previous OpenReplicaUnit failed. RE-DRIVE
			// the mount on every tick so a failed open retries (idempotent: a
			// successful re-open flips it to Owned).
			c.acquireReplicaUnitOverlap(ru)
			continue
		}
		// Fresh pending acquire: record Acquiring BEFORE the mount starts so a
		// routed op arriving during the mount returns errUnitAcquiring (the union
		// covers it via the current owner) rather than racing an absent mount.
		c.beginAcquire(ru)
		c.acquireReplicaUnitOverlap(ru)
	}

	// ACQUIRE half (pure new mount / initial convergence): a CURRENT position not
	// yet mounted with no transition. This is the Phase 2b initial convergence
	// (each node mounting the units the ring assigns it as peers join). It is the
	// clean-cut acquire (no overlap sequencing needed: no predecessor is serving
	// this exact slot, so there is nothing to drain from).
	for _, ru := range current {
		if _, isMounted := mountedSet[ru]; isMounted {
			continue
		}
		st := c.handoffPhaseOf(ru)
		if st.Phase.IsLoser() {
			// Re-desired while still draining: let drainCheck finish first.
			continue
		}
		if st.Phase.IsGainer() {
			c.acquireReplicaUnitOverlap(ru)
			continue
		}
		c.acquireReplicaUnit(ru)
	}
}

// desiredPendingReplicaUnits returns the positions this node owns under the
// PENDING view (the ring EXCLUDING draining members): the slots it WILL hold once
// every draining member departs. It is the pending-owner acquire trigger's input.
// With no draining members it is identical to desiredReplicaUnits (steady state),
// so the transition halves of the reconcile are empty.
//
// It mirrors desiredReplicaUnits exactly but supplies the DRAINING-EXCLUDED
// replica lookup (pendingUnitReplicas) to the pure storageunit.OwnedReplicaUnits.
func (c *Cluster) desiredPendingReplicaUnits(draining map[string]struct{}) []storageunit.ReplicaUnit {
	if len(draining) == 0 {
		return c.desiredReplicaUnits()
	}
	gs := c.genSnapshot()
	self := storageunit.NodeID(c.cfg.NodeID)

	replicas := storageunit.ReplicaLookupFunc(func(u storageunit.UnitID) []storageunit.NodeID {
		set := c.pendingUnitReplicas(storageunit.NewGenUnit(gs.gen, u), draining)
		nodes := make([]storageunit.NodeID, len(set))
		for i, m := range set {
			nodes[i] = storageunit.NodeID(m.ID)
		}
		return nodes
	})

	owned := storageunit.OwnedReplicaUnits(self, gs.count, replicas)
	out := make([]storageunit.ReplicaUnit, 0, len(owned))
	for _, o := range owned {
		out = append(out, storageunit.NewReplicaUnit(storageunit.NewGenUnit(gs.gen, o.Unit), o.Replica))
	}
	return out
}

// reconcileReplicaUnitsCleanCut is the pre-2e clean-cut RELEASE-then-ACQUIRE
// reconcile for the R>1 path, used ONLY by the break-demo (Config.
// TestingForceCleanCut). It diffs desired-vs-mounted and, with NO overlap
// sequencing and NO pending split, eagerly RELEASES every position no longer
// desired here and ACQUIRES every newly-desired one. A position moving away thus
// has a window where neither the old owner (released) nor the new owner (still
// mounting) serves it; a routed op to the still-Acquiring new owner gets
// errUnitAcquiring. Paired with TestingForceCleanCut also disabling the Option-A
// retry, this is the regime the gate proves collapses. Caller holds reconcileMu;
// mount mutations take mountMu.
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

// handoffPhaseOf returns the in-flight HandoffState for ru (the zero value, with
// Phase 0, when ru is not in flight). Reads under mountMu.
func (c *Cluster) handoffPhaseOf(ru storageunit.ReplicaUnit) storageunit.HandoffState {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	return c.handoffPhase[ru]
}

// beginDrain puts a position into PhaseDraining (the loser side): the node no
// longer owns it under the PENDING view but a pending successor is taking it
// over, so it KEEPS SERVING the still-mounted entry (dual-written via the union)
// until the successor is Ready. It does NOT release the mount. The OpenEpoch
// recorded is the epoch this node opened the position at - drainCheck compares
// the serving marker against it. Caller holds reconcileMu; the phase write takes
// mountMu.
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

// beginAcquire records PhaseAcquiring for a pending position MOVING IN. There is
// NO predecessor address: under the pending-ranges model the union routes ops
// DIRECTLY to both the current owner (still serving) and this pending owner, so a
// routed op arriving on this node during the mount simply returns errUnitAcquiring
// (the union covers the position via the current owner). The phase is set BEFORE
// the mount starts so there is no instant where routing targets this node, the
// mount is incomplete, AND no phase entry exists. Caller holds reconcileMu; the
// phase write takes mountMu.
func (c *Cluster) beginAcquire(ru storageunit.ReplicaUnit) {
	c.mountMu.Lock()
	c.handoffPhase[ru] = storageunit.HandoffState{Phase: storageunit.PhaseAcquiring}
	c.mountMu.Unlock()
}

// reclaimDrainingPosition aborts an in-flight drain for a position this node
// now owns again: it clears the loser-phase entry so the position converges back
// to Owned (mounted + no phase). It is a no-op unless the position is still
// mounted AND in a loser phase (a concurrent drainCheck may have already
// released it, in which case the acquire half re-mounts it cleanly). The mount
// is never touched here - the position has been served locally the entire time -
// so the reclaim has no availability gap. Takes mountMu.
func (c *Cluster) reclaimDrainingPosition(ru storageunit.ReplicaUnit) {
	c.mountMu.Lock()
	defer c.mountMu.Unlock()
	if _, mounted := c.mountMap[ru]; !mounted {
		return
	}
	if cur, ok := c.handoffPhase[ru]; ok && cur.Phase.IsLoser() {
		delete(c.handoffPhase, ru)
	}
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

// acquireReplicaUnitOverlap is the GAINER's mount under the pending-ranges model:
// it opens the position (the slow MinIO mount happens here, during which the
// union covers the position via the still-mounted current owner), then performs
// THE MOUNT FLIP - inserts mountMap[ru] under mountMu and advances Acquiring ->
// Ready -> drops the phase entry (Owned) - and writes the durable SERVING MARKER
// exactly once so the old owner's drainCheck poll releases. On open failure it
// leaves the Acquiring phase in place (the union still covers the position via
// the current owner) and the next reconcile / self-heal retries. Caller holds
// reconcileMu.
//
// acquireReplicaUnitOverlap spawns the GAINER's slow mount in a BACKGROUND
// goroutine so a node gaining many positions at once mounts them CONCURRENTLY
// rather than serializing one multi-second OpenReplicaUnit after another inside
// the reconcile (which holds reconcileMu). Serial opens would make a graceful
// scale-down's drain exceed its budget - the leaving node would depart with
// positions still un-handed-off. The position stays PhaseAcquiring (the union
// covers it) until the goroutine completes the mount flip. The acquireInFlight
// set (under mountMu) is the idempotency guard: a reconcile that re-drives an
// already-in-flight acquire does not spawn a second open. The goroutine is
// tracked by loopWG so Close awaits it.
func (c *Cluster) acquireReplicaUnitOverlap(ru storageunit.ReplicaUnit) {
	c.mountMu.Lock()
	if c.closed.Load() {
		c.mountMu.Unlock()
		return
	}
	if _, inFlight := c.acquireInFlight[ru]; inFlight {
		// An open for this position is already running; do not spawn a second.
		c.mountMu.Unlock()
		return
	}
	c.acquireInFlight[ru] = struct{}{}
	c.mountMu.Unlock()

	c.loopWG.Add(1)
	go func() {
		defer c.loopWG.Done()
		defer func() {
			c.mountMu.Lock()
			delete(c.acquireInFlight, ru)
			c.mountMu.Unlock()
		}()
		c.acquireReplicaUnitOverlapBlocking(ru)
	}()
}

// acquireReplicaUnitOverlapBlocking performs the GAINER's slow mount + flip
// synchronously. It is run from acquireReplicaUnitOverlap's background goroutine
// (so concurrent positions overlap) and directly by tests that want the
// deterministic blocking behavior. It must NOT be called while holding mountMu
// (it takes mountMu for the flip).
func (c *Cluster) acquireReplicaUnitOverlapBlocking(ru storageunit.ReplicaUnit) {
	b, err := c.replicaFactory.OpenReplicaUnit(ru, acquireBaseEpoch)
	if err != nil {
		// Mount failed; stay Acquiring. The position is not stranded: the old owner
		// is still a routed current owner serving via the union.
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
		// The phase entry is gone or not Acquiring (a concurrent reconcile already
		// flipped or dropped it). Still install the mount (it is the authoritative
		// durable owner) and drop any stale phase entry.
		c.mountMap[ru] = b
		delete(c.handoffPhase, ru)
		c.mountMu.Unlock()
		_ = c.replicaFactory.WriteServingMarker(ru, openedEpoch)
		return
	}
	ready, err := storageunit.NextOnReady(cur, openedEpoch)
	if err != nil {
		// Illegal edge should not happen (cur is Acquiring); install the mount and
		// drop the phase to converge to Owned regardless.
		c.mountMap[ru] = b
		delete(c.handoffPhase, ru)
		c.mountMu.Unlock()
		_ = c.replicaFactory.WriteServingMarker(ru, openedEpoch)
		return
	}
	c.mountMap[ru] = b
	// Ready is transient: once the mount entry is present the node serves locally,
	// so the steady state is Owned (no phase entry). Drop the entry rather than
	// parking in Ready - Owned = mounted + no phase, per the FSM's steady-state
	// poles.
	_ = ready
	delete(c.handoffPhase, ru)
	c.mountMu.Unlock()

	// Write the serving marker EXACTLY ONCE, AFTER the mount flip (outside the
	// lock: it is shared-storage I/O). This is the durable, poll-observable release
	// signal the old owner's drainCheck polls. No RPC is sent.
	_ = c.replicaFactory.WriteServingMarker(ru, openedEpoch)
}

// drainCheck is the OLD owner's POLL-ONLY release-check for a Draining position,
// armed on the periodic settle / self-heal cadence (runReconcile). It releases
// the position ONLY on a POSITIVE readiness: a durable serving marker at an
// epoch STRICTLY ABOVE (>) this node's own open epoch (proof a live new owner is
// actually SERVING). The gate is strict to reject this node's OWN stale
// gain-marker (written at exactly its open epoch); a genuine successor opens at
// durable+1 and writes a marker strictly higher. A bare durable fence-epoch
// advance NEVER releases (it bumps at open-START, before the mount; only the
// marker proves serving).
//
// Lock discipline (review P1-3): the ReadServingMarker I/O runs OUTSIDE any
// cluster lock (a slow MinIO read must not block routed ops' mountMap reads). The
// phase compare-and-advance (Draining -> Releasing) and the mountMap
// compare-and-delete are ONE critical section under mountMu, made exactly-once by
// the CAS-on-mountMap-delete (delete only if it still points at the same
// backend). CloseReplicaUnit runs AFTER the lock is dropped (the entry is already
// removed), so a slow close does not hold mountMu. Caller holds reconcileMu (so
// two passes do not both enter the edge); the exactly-once guard is the mountMap
// CAS regardless.
func (c *Cluster) drainCheck(ru storageunit.ReplicaUnit) {
	state := c.handoffPhaseOf(ru)
	if state.Phase != storageunit.PhaseDraining {
		return
	}

	// An ORPHANED Draining phase (the mount is already gone) converges straight to
	// Absent regardless of marker readiness. Without this, a Draining phase whose
	// mount was evicted but whose successor never wrote a marker would loop forever
	// in the readiness poll below, and the acquire half's loser-phase skip would
	// never let the position re-mount - permanently stranding a re-desired position
	// as unowned/unserved. The mount being gone means there is nothing to drain.
	c.mountMu.Lock()
	if cur, ok := c.handoffPhase[ru]; ok && cur.Phase == storageunit.PhaseDraining {
		if _, mounted := c.mountMap[ru]; !mounted {
			delete(c.handoffPhase, ru)
			c.mountMu.Unlock()
			return
		}
	}
	c.mountMu.Unlock()

	// I/O OUTSIDE the lock: poll the durable serving marker.
	markerEpoch, ok, err := c.replicaFactory.ReadServingMarker(ru)
	if err != nil {
		return
	}
	// Positive readiness: a live SUCCESSOR is serving at an epoch STRICTLY ABOVE
	// my open epoch. The gate is STRICT (>, not >=) to reject this node's OWN
	// stale gain-marker: a node that GAINED ru at open epoch E wrote
	// WriteServingMarker(ru, E); if the ring later moves ru OFF this node,
	// beginDrain sets OpenEpoch = DurableEpochReplica(ru) = E (unchanged until the
	// NEW gainer opens), so a >= gate would read this node's OWN marker E and
	// RELEASE while the real successor is still mid-mount. A genuine successor
	// always opens at durable+1 >= E+1 and writes a marker strictly above E, so >
	// still releases on a real successor.
	ready := ok && markerEpoch > state.OpenEpoch
	if !storageunit.Releasable(state, ready) {
		// No marker yet (or below my epoch): stay Draining + keep serving.
		return
	}

	// ONE mountMu critical section: advance Draining -> Releasing and
	// compare-and-delete the mount entry. The CAS-delete (delete only if the entry
	// is still the backend we drained) is the real exactly-once guard.
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
	// release is Absent (no mount, no phase). next validates the edge; we converge
	// straight to Absent.
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
