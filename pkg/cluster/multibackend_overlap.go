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
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/ring"
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
	joining, draining := c.transitionSets()

	// current = positions this node owns under the CURRENT view (the ring
	// EXCLUDING joining members, quorum-floored, draining members INCLUDED): the
	// set it holds + must keep mounted as a stable current owner throughout a
	// transition. With no joining members this is desiredReplicaUnits (the full
	// ring), so the leave path is unchanged. Excluding a JOINING newcomer here is
	// what routes a moving position's DRAIN half onto the still-mounted displaced
	// owner (it stays in current-not-pending) and the ACQUIRE half onto the
	// newcomer (it is pending-not-current), the exact mirror of a leave.
	current := c.desiredCurrentReplicaUnits(joining)
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
	// but now holds in CURRENT again AND back in PENDING, while it is STILL
	// mounted. This happens when the ring flip-flops a position back (e.g. a
	// draining member that recovered): the position re-enters this node's PENDING
	// set, so it will keep it post-transition. The drain is waiting for a
	// SUCCESSOR's serving marker, but this node is the stable holder again, so
	// drainCheck would never release it and the position would be stranded in
	// Draining forever. Abort the drain: clear the in-flight phase so the position
	// returns to Owned (it has been mounted + serving the whole time, so no
	// availability gap). Reads under mountMu; the phase clear takes the lock.
	//
	// The PENDING gate is load-bearing: a position in CURRENT but NOT in PENDING is
	// being HANDED OFF (this node is itself draining, or a draining successor split
	// moved it out of pending), so its drain MUST run to completion - it is never
	// reclaimed. Without the pending check a gracefully-leaving node (still a ring
	// member, hence still a CURRENT owner of all its positions) reclaims every
	// position every tick and re-drains it, re-capturing a climbing open epoch each
	// pass, so the release gate never catches the successor's marker and the drain
	// hangs to the GracefulLeaveDrainTimeout (the real on-object-storage gap, #410).
	for _, ru := range mounted {
		if _, ok := currentSet[ru]; !ok {
			continue
		}
		if _, inPending := pendingSet[ru]; !inPending {
			continue // current-but-not-pending: being handed off, never reclaim.
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
		// Not in CURRENT at all. If it is in PENDING, this node is a SUCCESSOR
		// holding the leaver's position as a pending mount (acquired + serving-
		// marked, serving via the union): HOLD it - it becomes the stable owner the
		// moment the draining member leaves (it then enters CURRENT and the steady-
		// state branch keeps it). Releasing it here would tear down the very handoff
		// target the leaver's drainCheck is polling, and the successor would churn
		// release -> re-acquire -> re-mark at a climbing epoch, destabilizing the
		// handoff (the successor-side half of the #410 oscillation).
		if _, inPending := pendingSet[ru]; inPending {
			continue
		}
		// Absent from BOTH current and pending: genuinely abandoned. If it is
		// already mid-transition (Draining), leave it to drainCheck. Otherwise plain
		// clean-cut release (no successor mounts THIS node's exact slot; the
		// surviving replicas cover W).
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
			// Already mounted this pending position. If it is still PhaseAcquiring
			// with the mount present AND no acquire goroutine is in flight, the flip
			// raced and exited WITHOUT dropping the phase / writing the serving marker
			// - the position is stuck Acquiring forever, so the leaver's drainCheck
			// never sees a marker and its drain runs to the timeout (the staging
			// scale-down availability gap). Finish the flip here. Otherwise (Owned, or
			// a flip still running) it is a no-op.
			c.finishStuckFlipIfNeeded(ru)
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
		if _, inPending := pendingSet[ru]; !inPending {
			// Current-but-not-pending = being HANDED OFF (this node is itself
			// draining, or a draining split moved it out of pending). The DRAIN half
			// + drainCheck own these positions; the ACQUIRE half must NOT (re-)mount
			// one. After drainCheck RELEASES a drained position its phase is cleared
			// to 0, so it is no longer caught by the IsLoser skip below - without this
			// gate it would fall through to acquireReplicaUnit and the leaver would
			// RE-GRAB a position it just handed off (re-fencing the successor at a
			// climbed epoch), so ownedPositionCount never reaches 0 and the graceful
			// leave never completes (the leaver-side half of the #410 oscillation).
			continue
		}
		if _, isMounted := mountedSet[ru]; isMounted {
			// A mounted current position stuck in a gainer phase with no acquire
			// goroutine in flight is the same stuck-flip race: finish it.
			c.finishStuckFlipIfNeeded(ru)
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

// finishStuckFlipIfNeeded completes a handoff flip that the background acquire
// goroutine left half-done: a position that is MOUNTED + in a GAINER phase
// (PhaseAcquiring/PhaseReady) with NO acquire goroutine in flight. In that state
// the mount is present + durable but the phase was never dropped to Owned and the
// serving marker was never written, so the position never reads as "serving":
// the leaver's drainCheck never sees a successor marker and its graceful drain
// runs to the timeout (the staging scale-down availability gap). The mount being
// present means the open already succeeded + the durable copy is recovered, so it
// is safe to advance to Owned (drop the phase) and write the serving marker at
// the position's durable open epoch (strictly above the leaver's, which releases
// its drain). Caller holds reconcileMu. A no-op unless genuinely stuck.
func (c *Cluster) finishStuckFlipIfNeeded(ru storageunit.ReplicaUnit) {
	c.mountMu.Lock()
	if _, inFlight := c.acquireInFlight[ru]; inFlight {
		c.mountMu.Unlock()
		return // a flip goroutine is running; let it complete.
	}
	st, ok := c.handoffPhase[ru]
	if !ok || !st.Phase.IsGainer() {
		c.mountMu.Unlock()
		return // Owned (no phase) or a loser phase: nothing to finish.
	}
	if _, mounted := c.mountMap[ru]; !mounted {
		c.mountMu.Unlock()
		return // not mounted; the acquire half re-drives the mount.
	}
	delete(c.handoffPhase, ru) // mounted + no phase = Owned (serving locally).
	c.mountMu.Unlock()

	// Write the serving marker OUTSIDE the lock (shared-storage I/O), at THIS
	// node's EXACT open epoch (the recorded factory return) - exactly what the
	// in-goroutine flip would have written, NOT a re-read of the climbing durable.
	_ = c.replicaFactory.WriteServingMarker(ru, c.ownOpenEpoch(ru))
}

// ownOpenEpoch returns the EXACT epoch this node opened ru at (recorded from
// OpenReplicaUnit's return value), which is the drain-release gate AND the
// serving-marker epoch. It falls back to the live durable epoch ONLY if no open
// epoch was recorded (defensive; a mounted position this node opened always has
// one). It must NOT be replaced by openEpochForReplica at the call sites: that
// re-reads the shared, climbing durable, which is the bug this fix removes.
func (c *Cluster) ownOpenEpoch(ru storageunit.ReplicaUnit) storageunit.Epoch {
	if v, ok := c.myOpenEpoch.Load(ru); ok {
		return v.(storageunit.Epoch)
	}
	return c.openEpochForReplica(ru)
}

// desiredPendingReplicaUnits returns the positions this node owns under the
// PENDING view (the ring EXCLUDING draining members): the slots it WILL hold once
// every draining member departs. It is the pending-owner acquire trigger's input.
// With no draining members it is identical to desiredReplicaUnits (steady state),
// so the transition halves of the reconcile are empty.
//
// It mirrors desiredReplicaUnits exactly (including the v0.9 gen-(g+1) split
// children) but supplies the DRAINING-EXCLUDED replica lookup
// (pendingUnitReplicas) as replicaAt, sharing the core desiredReplicaUnitsVia.
// Keeping it a thin call over the shared core is what guarantees the current and
// pending sets stay in lockstep.
func (c *Cluster) desiredPendingReplicaUnits(draining map[string]struct{}) []storageunit.ReplicaUnit {
	if len(draining) == 0 {
		return c.desiredReplicaUnits()
	}
	return c.desiredReplicaUnitsVia(func(gu storageunit.GenUnit) []ring.Member {
		return c.pendingUnitReplicas(gu, draining)
	})
}

// desiredCurrentReplicaUnits returns the positions this node owns under the
// CURRENT view (the ring EXCLUDING joining members, quorum-floored): the slots it
// holds + must keep mounted RIGHT NOW as a stable current owner. It is the
// entry-side mirror of desiredPendingReplicaUnits and the reconcile's current
// input. With no joining members it is identical to desiredReplicaUnits (steady
// state), so the transition halves of the reconcile are empty.
//
// It mirrors desiredReplicaUnits exactly (including the v0.9 gen-(g+1) split
// children) but supplies the JOINING-EXCLUDED-with-floor replica lookup
// (currentUnitReplicas) as replicaAt, sharing the core desiredReplicaUnitsVia -
// exactly as desiredPendingReplicaUnits supplies the draining-excluded lookup.
// Keeping current + pending as thin calls over the shared core guarantees they
// stay in lockstep (the reconcile treats any spurious divergence as a transition).
func (c *Cluster) desiredCurrentReplicaUnits(joining map[string]struct{}) []storageunit.ReplicaUnit {
	if len(joining) == 0 {
		return c.desiredReplicaUnits()
	}
	// Build the reduced (joining-excluded) ring ONCE and reuse it across every
	// unit's replica lookup, rather than rebuilding per unit inside currentUnitReplicas.
	reduced := c.buildReducedRing(joining)
	return c.desiredReplicaUnitsVia(func(gu storageunit.GenUnit) []ring.Member {
		return c.currentReplicasFromReduced(reduced, gu)
	})
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
	// Gate the drain on THIS node's EXACT open epoch (recorded from the factory
	// return), NOT the live durable: the live durable climbs as the SUCCESSOR
	// opens, which would push the release threshold above the successor's serving
	// marker and hang the drain to its timeout (the graceful-scale-down
	// availability gap). The recorded value is stable across reclaim/re-drain.
	// Read OUTSIDE mountMu (ownOpenEpoch's defensive fallback can do shared-storage
	// I/O, which must never run under the lock routed ops need). The read races the
	// mountMap check below only benignly: myOpenEpoch never holds a LOWER epoch for
	// a live mount, so a stale read can only INFLATE the gate (hang direction, self-
	// corrected next poll), never release early.
	open := c.ownOpenEpoch(ru)
	c.mountMu.Lock()
	b, mounted := c.mountMap[ru]
	if !mounted {
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

	// DISPLACEMENT FLUSH (docs/SPEC.md "Displacement flush"): this call is the
	// Owned -> Draining edge - reached ONLY when the phase was just armed above
	// (the phase!=0 return keeps re-entrant drain ticks out), so the flush fires
	// EXACTLY ONCE per displacement transition. Flushing the displaced owner's
	// memtable NOW means the successor's fencing open (already racing this)
	// replays a minimal WAL tail instead of the whole unflushed tail. A reclaim
	// + re-drain is a NEW edge and flushes again by design.
	c.flushDisplaced(b)

	// FAST DRAIN POLL (docs/SPEC.md "Displacement flush" sibling change): while
	// any position is Draining, poll the release gate on the short
	// displacedDrainPollInterval cadence instead of waiting for the periodic
	// reconcile tick, so the displaced owner releases (close + cleanup) within
	// ~half a second of the successor's serving marker landing rather than up
	// to a full reconcileInterval later. Poll-only is preserved: this is a
	// LOCAL cadence change, no push RPC. The leaver's own DrainForLeave already
	// fast-polls; this covers the JOIN direction's displaced owner.
	c.ensureDrainPoller()
}

// displacedDrainPollInterval is the short cadence a displaced owner polls its
// Draining positions' release gate at (drainCheck: one serving-marker read per
// draining position per poll). Coarser than DrainForLeave's drainPollInterval
// because the displaced owner is not blocking a process exit; fine enough that
// release cleanup follows the successor's marker within ~half a second.
const displacedDrainPollInterval = 500 * time.Millisecond

// drainPollerMaxLife caps one fast-poller run. The fast cadence exists for
// the NORMAL case (the successor's marker lands within seconds of the drain
// edge); a drain still unresolved after this long is stuck on a slow or
// failed successor, and burning a marker read every half second at the store
// indefinitely buys nothing - the poller exits and the periodic reconcile's
// tick-cadence drainCheck remains the backstop, exactly the pre-poller
// behavior. A NEW Owned -> Draining edge arms a fresh poller.
const drainPollerMaxLife = 30 * time.Second

// ensureDrainPoller starts, at most one at a time, a background poller that
// re-runs the drain release checks every displacedDrainPollInterval while ANY
// loser-phase (Draining) position exists, for at most drainPollerMaxLife,
// then exits. Armed from beginDrain (the Owned -> Draining edge). The
// periodic reconcile keeps running the same checks as the backstop;
// drainCheck's mountMap CAS-delete keeps the release exactly-once regardless
// of how many pollers observe the marker.
func (c *Cluster) ensureDrainPoller() {
	if !c.drainPollerActive.CompareAndSwap(false, true) {
		return // a poller is already running.
	}
	c.loopWG.Add(1)
	go func() {
		defer c.loopWG.Done()
		expired := false
		defer func() {
			c.drainPollerActive.Store(false)
			// Close the exit-vs-new-drain race: a beginDrain that ran after
			// this poller's final "no losers left" check but before the
			// Store(false) above found drainPollerActive still true and did
			// not start a poller. Re-check and re-arm so that drain is not
			// left to the slow tick. NEVER re-arm on expiry: a stuck drain
			// would otherwise re-spawn the poller forever, defeating the cap.
			if !expired && !c.closed.Load() && c.hasLoserPhase() {
				c.ensureDrainPoller()
			}
		}()
		t := time.NewTicker(displacedDrainPollInterval)
		defer t.Stop()
		expiry := time.NewTimer(drainPollerMaxLife)
		defer expiry.Stop()
		for {
			select {
			case <-c.closeCh:
				return
			case <-expiry.C:
				expired = true
				return // stuck drain: hand back to the tick backstop.
			case <-t.C:
				if c.closed.Load() {
					return
				}
				c.reconcileMu.Lock()
				c.runDrainChecks()
				c.reconcileMu.Unlock()
				if !c.hasLoserPhase() {
					return // every drain resolved; the next edge re-arms.
				}
			}
		}
	}()
}

// hasLoserPhase reports whether any position is currently in a loser
// (Draining) phase. Reads under mountMu.
func (c *Cluster) hasLoserPhase() bool {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	for _, st := range c.handoffPhase {
		if st.Phase.IsLoser() {
			return true
		}
	}
	return false
}

// flushDisplaced asks a displaced (newly-Draining) position's backend to make
// its in-memory write state durable, best-effort. It never creates an error
// path: a backend without the OPTIONAL backend.Flusher capability is skipped,
// and a flush failure is discarded (the only failure modes are benign - the
// successor's open already fenced this owner, which is exactly the pre-flush
// behavior, or the backend is closing). The flush runs in a background
// goroutine because it is backing-store I/O and the caller holds reconcileMu
// (a slow flush must never stall the reconcile tick or routed ops); loopWG
// tracks it so Close awaits it.
func (c *Cluster) flushDisplaced(b backend.Backend) {
	fl, ok := flushableBackend(b)
	if !ok || c.closed.Load() {
		return
	}
	c.loopWG.Add(1)
	go func() {
		defer c.loopWG.Done()
		_ = fl.Flush()
	}()
}

// flushableBackend reports whether b (a mountMap entry) supports the OPTIONAL
// Flusher capability, unwrapping the fencedSelfHealing decorator storeMount
// wraps every mounted backend in. The unwrap keeps the capability honest: the
// decorator itself must not advertise Flush for an inner backend that cannot.
func flushableBackend(b backend.Backend) (backend.Flusher, bool) {
	if fsh, ok := b.(*fencedSelfHealing); ok {
		b = fsh.inner
	}
	fl, ok := b.(backend.Flusher)
	return fl, ok
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
		// NODE-WIDE OPEN BOUND: take a permit around each open attempt, so a
		// node gaining many positions at once runs at most
		// Config.OpenConcurrency (default 1) real-data FFI opens concurrently -
		// the SAME knob the boot mount pool enforces, because concurrent
		// real-data opens are the documented "empty SSTable" corruption trigger
		// in the shipped binding (see defaultOpenConcurrency). While queued the
		// position simply stays PhaseAcquiring and the union keeps covering it
		// via the still-mounted current owner, so the bound sequences the opens
		// without any availability cost. Queued goroutines block on the permit
		// channel, so when one open finishes the next queued open starts
		// IMMEDIATELY (event-driven chaining, never a reconcile-tick wait). The
		// acquireInFlight entry is held across the whole loop, so a reconcile
		// re-drive never spawns a duplicate waiter.
		//
		// FAILURE RE-DRIVE (handoff-cycle latency): a FAILED open used to
		// strand the position as Acquiring until the NEXT periodic reconcile
		// re-drove it - quantizing every transient open failure to a full
		// reconcileInterval. Retry here instead on a short jittered backoff,
		// RELEASING the permit between attempts so a failing position never
		// starves the queued positions behind it. Bounded: once the backoff
		// exceeds acquireRedriveCap the goroutine exits and the periodic
		// reconcile remains the backstop (identical to the pre-fix behavior
		// from that point on). Success/failure is read off lastAcquireErr,
		// the same signal the reconcile re-drive path uses.
		backoff := acquireRedriveBase
		for {
			if c.closed.Load() {
				return
			}
			release := c.acquireOpenPermit()
			if c.closed.Load() {
				release()
				return // Close ran while queued; nothing to open.
			}
			c.acquireReplicaUnitOverlapBlocking(ru)
			release()
			if _, failed := c.lastAcquireErr.Load(ru); !failed {
				return // mounted (or superseded); done.
			}
			if backoff > acquireRedriveCap {
				return // hand the retry back to the periodic reconcile backstop.
			}
			select {
			case <-c.closeCh:
				return
			case <-time.After(jitteredBackoff(backoff)):
			}
			backoff *= 2
		}
	}()
}

// acquireRedriveBase / acquireRedriveCap bound the in-goroutine retry of a
// FAILED overlap open: attempts after ~250ms, 500ms, 1s, 2s (jittered), then
// the periodic reconcile takes over. Short enough that a transient open
// failure costs sub-second latency instead of a full reconcileInterval;
// bounded so a permanently failing position degrades to exactly the old
// tick-driven retry cadence.
const (
	acquireRedriveBase = 250 * time.Millisecond
	acquireRedriveCap  = 2 * time.Second
)

// acquireReplicaUnitOverlapBlocking performs the GAINER's slow mount + flip
// synchronously. It is run from acquireReplicaUnitOverlap's background goroutine
// (so concurrent positions overlap) and directly by tests that want the
// deterministic blocking behavior. It must NOT be called while holding mountMu
// (it takes mountMu for the flip).
func (c *Cluster) acquireReplicaUnitOverlapBlocking(ru storageunit.ReplicaUnit) {
	b, openedEpoch, err := c.replicaFactory.OpenReplicaUnit(ru, acquireBaseEpoch)
	if err != nil {
		// SELF-HEAL the same factory/cluster desync as the clean-cut path (#408):
		// this position is mid-acquire with no mount installed yet, so if the
		// factory refuses because it still holds ru open on this handle ("already
		// open"), that handle state is stale - close it to re-sync, then reopen
		// once. Safe: no mount entry points at the stale handle.
		_ = c.replicaFactory.CloseReplicaUnit(ru)
		b, openedEpoch, err = c.replicaFactory.OpenReplicaUnit(ru, acquireBaseEpoch)
		if err != nil {
			// Mount still failing; stay Acquiring. The position is not stranded: the
			// old owner is still a routed current owner serving via the union.
			c.lastAcquireErr.Store(ru, err.Error())
			return
		}
	}
	c.lastAcquireErr.Delete(ru)

	// openedEpoch is THIS node's EXACT open epoch (factory return), used as the
	// serving-marker epoch + recorded as this node's drain gate. NOT a re-read of
	// the climbing durable. Record it before the mount flip so a beginDrain that
	// sees mountMap[ru] also sees the epoch.
	c.myOpenEpoch.Store(ru, openedEpoch)

	// THE MOUNT FLIP: insert the mount entry + advance the phase to Ready under
	// ONE mountMu hold so a routed op never sees the mount present without the
	// phase resolved (and vice versa).
	c.mountMu.Lock()
	if c.closed.Load() {
		c.mountMu.Unlock()
		c.myOpenEpoch.Delete(ru) // no mount installed; don't leak the recorded epoch.
		_ = c.replicaFactory.CloseReplicaUnit(ru)
		return
	}
	cur := c.handoffPhase[ru]
	if cur.Phase != storageunit.PhaseAcquiring {
		// The phase entry is gone or not Acquiring (a concurrent reconcile already
		// flipped or dropped it). Still install the mount (it is the authoritative
		// durable owner) and drop any stale phase entry.
		c.storeMount(ru, b)
		delete(c.handoffPhase, ru)
		c.mountMu.Unlock()
		_ = c.replicaFactory.WriteServingMarker(ru, openedEpoch)
		return
	}
	ready, err := storageunit.NextOnReady(cur, openedEpoch)
	if err != nil {
		// Illegal edge should not happen (cur is Acquiring); install the mount and
		// drop the phase to converge to Owned regardless.
		c.storeMount(ru, b)
		delete(c.handoffPhase, ru)
		c.mountMu.Unlock()
		_ = c.replicaFactory.WriteServingMarker(ru, openedEpoch)
		return
	}
	c.storeMount(ru, b)
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
	// beginDrain sets OpenEpoch = ownOpenEpoch(ru) = E (this node's EXACT open
	// epoch, recorded from the OpenReplicaUnit return value and FIXED for the life
	// of the mount - NOT a re-read of the climbing durable), so a >= gate would
	// read this node's OWN marker E and RELEASE while the real successor is still
	// mid-mount. A genuine successor always opens at durable+1 >= E+1 and writes a
	// marker strictly above E, so > still releases on a real successor.
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
	c.myOpenEpoch.Delete(ru) // released; a re-acquire records a fresh open epoch.
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
