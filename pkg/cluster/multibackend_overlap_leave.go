// Graceful-leave drain (v0.8 Phase 2e, pending ranges, scale-down). Under the
// pending-ranges model a leaving node publishes coord.RoleDraining through the
// coordinator but STAYS a full, alive member AND a current owner of every
// position it serves: it is NOT removed from the consistent-hash ownership
// ring. Every node's
// routedReplicasForKey then computes the leaver's positions as
// CURRENT-but-not-PENDING (current = ring
// including the leaver, pending = ring excluding it), forms the routed UNION, and
// DUAL-WRITES the leaver + its pending successors. The leaver keeps serving
// (dual-written via the union) until each successor mounts the position and
// writes its serving marker; ordered removal then collapses the union onto the
// pending set and drainCheck releases the leaver's mount.
//
// This file adds the SHUTDOWN-path wait (DrainForLeave) and the completion gate.
// Close() calls DrainForLeave at the TOP (gated on GracefulLeaveDrainTimeout >
// 0 + multiReplicated()), BEFORE any teardown, while the reconcile loop +
// drainCheck still run. Only after the drain completes (or its bounded timeout
// fires) does the real departure (the coordinator Close that announces the
// leave and tears the transport down) run.
//
// See docs/SPEC.md "v0.8 Phase 2e: Graceful leave (scale-down)" and
// docs/design/overlap-handoff.md "Graceful leave (scale-down)".

package cluster

import (
	"context"
	"time"

	"github.com/Zamua/shale/pkg/storageunit"
)

// drainPollInterval is how often DrainForLeave re-drives the reconcile +
// drainCheck and re-tests for completion. It is deliberately tighter than
// reconcileInterval: the drain is a bounded, latency-sensitive shutdown wait, so
// it actively drives the loop rather than waiting for the background reconcile
// tick. The reconcile is idempotent, so an extra drive is cheap.
const drainPollInterval = 50 * time.Millisecond

// DrainForLeave performs the graceful-leave drain for a scale-down: it SETS THIS
// NODE DRAINING (publishing the Draining role, so every node recomputes the
// pending split current-minus-this-node, forms the routed union, and dual-writes
// the leaver + its successors while it stays alive + a current owner + serving),
// then BLOCKS until every position this node owns has a PENDING SUCCESSOR that is
// PROVABLY SERVING (its serving marker is present at an epoch strictly above this
// node's open epoch) OR ctx is cancelled / its deadline fires. The real
// the real departure (coordinator Close) happens later, in Close.
//
// THE COMPLETION GATE (allOwnedPositionsHandedOff): this node stays a CURRENT
// OWNER in the ring throughout the drain, so it KEEPS its positions mounted and
// keeps serving them - the old mount-table-empties completion is therefore NOT the
// signal (a current owner's mount does not vanish on its own). Instead the drain
// is done when EVERY position this node still has mounted as a Draining position
// has a serving marker strictly above its open epoch. drainCheck runs on the
// same cadence and releases (CloseReplicaUnit) each such position once its marker
// appears; a released position is no longer mounted, so it trivially satisfies
// the gate. The gate thus holds the instant every owned position has a provably
// serving successor, whether or not drainCheck has finished closing the mount.
//
// Throughout the wait the reconcile loop, the serving path, and drainCheck all
// stay alive (Close has not torn them down yet - DrainForLeave runs at the TOP of
// Close). DrainForLeave additionally DRIVES the reconcile + drainCheck itself on
// a tight cadence so the hand-off converges at poll latency rather than waiting
// for the background reconcile tick.
//
// It is a no-op (returns nil immediately) outside the multi-backend replicated
// path: a graceful leave only has positions to drain when multiReplicated().
//
// RESIDUALS (not covered, by design):
//   - a position taking the PLAIN clean-cut release path (this node simply drops
//     out of a unit's replica set, another already-mounted replica covers W, no
//     successor takes its exact slot) is released eagerly by the reconcile; there
//     is nothing to drain and the surviving replicas keep it available. Its mount
//     drops, so it does not block completion.
//   - a position whose successor is STUCK (its Acquiring mount never completes
//     within the grace budget): the wait times out and Close proceeds, leaving
//     that one position unserved from teardown until the successor finally mounts
//   - exactly today's gap, for that position only, no worse than the disabled
//     (timeout 0) behavior.
func (c *Cluster) DrainForLeave(ctx context.Context) error {
	if !c.multiReplicated() {
		return nil
	}

	// SET THIS NODE DRAINING and publish it. The node stays ALIVE, a full
	// addressable member, AND a CURRENT OWNER (it is NOT removed from
	// ownership - the include/exclude split is per-op in routedReplicasForKey).
	// The moment the Draining role propagates, every node's routedReplicasForKey
	// computes the PENDING split (placement EXCLUDING this node) for this node's
	// positions, forms the routed union, and dual-writes this node + its pending
	// successors. This node keeps serving its mounts (receiving union writes +
	// reads) throughout the drain. The REAL departure (the coordinator's Close,
	// which announces the leave and tears the transport down) stays in the
	// existing Close teardown, run AFTER this drain returns.
	c.selfDraining.Store(true)
	c.publishRoles()

	// Drive reconcile + drainCheck and poll for completion until every owned
	// position has a serving successor or ctx is done. The first drive runs
	// immediately so a fast hand-off does not pay one interval of latency.
	t := time.NewTicker(drainPollInterval)
	defer t.Stop()
	for {
		// Re-drive the unit reconcile so this node's positions (current-but-not-
		// pending under its own Draining bit) convert to Draining, then drainCheck
		// releases the ones whose successors wrote a serving marker. runReconcile is
		// idempotent + runs the overlap reconcile + runDrainChecks.
		c.runReconcile()
		if c.allOwnedPositionsHandedOff() {
			return nil
		}
		select {
		case <-ctx.Done():
			// THE BUDGET EXPIRED WITH THE HAND-OFF INCOMPLETE. Departing anyway
			// is deliberate (a leave must not hang on a stuck successor), and
			// the cost is real: each position below is unserved from teardown
			// until its successor finishes mounting, so a routed read or write
			// for it finds NO holder in the union and gets the retryable
			// acquiring refusal. Name them - the window is indistinguishable
			// from a routing bug downstream unless the node that opened it says
			// so on the way out.
			if waiting := c.positionsAwaitingSuccessor(); len(waiting) > 0 {
				c.logf("shale: LEAVING WITH %d POSITION(S) NOT HANDED OFF after the %s drain budget: %v - "+
					"each is unserved until its successor mounts; routed ops for them refuse with the "+
					"retryable acquiring error until then",
					len(waiting), c.cfg.GracefulLeaveDrainTimeout, waiting)
			}
			return ctx.Err()
		case <-t.C:
		}
	}
}

// allOwnedPositionsHandedOff is the DrainForLeave completion gate: it reports
// whether EVERY position this node still has mounted (under a Draining phase, or
// any mount it has not yet released) has a PENDING SUCCESSOR provably serving -
// i.e. a serving marker present at an epoch STRICTLY ABOVE this node's open epoch
// for that ReplicaUnit. A position with no such marker is still being handed off
// (its successor has not mounted) and blocks completion.
//
// It snapshots the mounted set from the table, then reads the durable serving
// marker for each position OUTSIDE any lock (a slow shared-storage read must not
// block routed ops' mount lookups). The strict (>) gate matches drainCheck's
// release rule: a marker strictly above the leaver's open epoch is positive proof
// a live SUCCESSOR is serving (the leaver's own stale gain-marker at exactly its
// open epoch does not count). A position that drainCheck has already RELEASED is
// no longer mounted, so it is not in the snapshot and trivially satisfies the
// gate. With no positions mounted, the gate is true (the leaver has handed off
// everything).
func (c *Cluster) allOwnedPositionsHandedOff() bool {
	return len(c.positionsAwaitingSuccessor()) == 0
}

// positionsAwaitingSuccessor returns the mounted positions with NO successor
// provably serving above this node's own open epoch. It is the gate's evidence
// half: a leave that ends on its budget rather than on hand-off leaves exactly
// these positions unserved from teardown until a successor mounts, and that
// window is invisible unless the leaver names them on the way out.
func (c *Cluster) positionsAwaitingSuccessor() []storageunit.ReplicaUnit {
	if !c.replicaLayout() {
		return nil
	}
	var waiting []storageunit.ReplicaUnit
	for _, ru := range c.mounts.mountedList() {
		// Gate on THIS node's OWN open epoch (the recorded factory return), NOT the
		// live durable: the durable climbs as the successor opens, which would push
		// `open` up to the successor's serving-marker epoch and make this gate (a
		// strict marker > open) stay false forever - the leave would never complete
		// and the preStop drain would run to its timeout (the #410 availability gap).
		open := c.ownOpenEpoch(ru)
		markerEpoch, ok, err := c.factory.ReadServingMarker(storageunit.ReplicaMount(ru))
		if err != nil || !ok || markerEpoch <= open {
			// No successor serving this position above the leaver's epoch yet: still
			// handing off (or a transient marker-read error - retry next poll).
			waiting = append(waiting, ru)
		}
	}
	return waiting
}

// ownedPositionCount returns how many ReplicaUnit positions this node still has
// mounted. Kept as an introspection helper for tests; the DrainForLeave
// completion gate is allOwnedPositionsHandedOff (which the leaver can satisfy
// while still serving its mounts), not the mount table emptying.
func (c *Cluster) ownedPositionCount() int {
	return c.mounts.mountedCount()
}
