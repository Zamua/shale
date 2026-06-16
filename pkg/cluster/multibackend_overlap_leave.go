// Graceful-leave drain (v0.8 Phase 2e, scale-down). The overlap handoff
// machine in multibackend_overlap.go is symmetric between scale-UP and
// scale-DOWN: a JOIN makes a survivor Acquiring while the existing owner
// Draining-serves; a deliberate REMOVAL makes the survivors Acquiring the
// leaving node's positions while the LEAVING node Draining-serves them. So a
// graceful leave is just an overlap drain seen from the LOSING side, for ALL
// of a node's positions at once. The one missing piece is on the SHUTDOWN
// path, not the reconcile path: Close() does not WAIT for that drain. This
// file adds the wait (DrainForLeave) and the helper that decides when it is
// done; Close() calls it (gated) BEFORE any teardown.
//
// See docs/SPEC.md "v0.8 Phase 2e: Graceful leave (scale-down)" and
// docs/design/overlap-handoff.md "Graceful leave (scale-down)".

package cluster

import (
	"context"
	"time"
)

// drainPollInterval is how often DrainForLeave re-drives the reconcile +
// drainCheck and re-tests for completion. It is deliberately tighter than
// reconcileInterval: the drain is a bounded, latency-sensitive shutdown wait,
// so it actively drives the loop rather than waiting for the background
// reconcile tick. The reconcile is idempotent, so an extra drive is cheap.
const drainPollInterval = 50 * time.Millisecond

// DrainForLeave performs the graceful-leave drain for a scale-down: it SETS
// THIS NODE DRAINING (gossiping the yield-ownership bit, so survivors re-own
// this node's positions and start forwarding to it via the overlap machine
// while it stays alive + addressable), then BLOCKS until this node owns no more
// positions (every position handed off to its successor and released by
// drainCheck, or plain-released) OR ctx is cancelled / its deadline fires.
// The real membership.Leave() + transport Shutdown happen later, in Close.
//
// Throughout the wait the reconcile loop, the serving path, drainCheck, and the
// forward path all stay alive (Close has not torn them down yet - DrainForLeave
// runs at the TOP of Close). DrainForLeave additionally DRIVES the reconcile +
// drainCheck itself on a tight cadence so the hand-off converges at poll
// latency rather than waiting for the background reconcile tick. Each Draining
// position releases on the SAME rule as any overlap drain (a successor's
// serving marker at an epoch strictly above this node's open epoch); this
// reuses drainCheck's release, it does not reimplement it.
//
// It is a no-op (returns nil immediately) outside the multi-backend overlap
// path: a graceful leave only has positions to drain when multiReplicated().
//
// RESIDUALS (not covered, by design):
//   - a position taking the PLAIN clean-cut release path (the leaving node
//     simply drops out of a unit's replica set, another already-mounted replica
//     covers W, no successor takes its exact position) is released eagerly by
//     the reconcile; there is nothing to drain and the surviving replicas keep
//     it available. mountMap drops it, so it does not block completion.
//   - a position whose successor is STUCK (its Acquiring mount never completes
//     within the grace budget): the wait times out and Close proceeds, leaving
//     that one position unserved from teardown until the survivor finally
//     mounts - exactly today's gap, for that position only, no worse than the
//     disabled (timeout 0) behavior.
func (c *Cluster) DrainForLeave(ctx context.Context) error {
	if !c.multiReplicated() {
		return nil
	}

	// SET THIS NODE DRAINING and gossip it. The node stays alive + a full,
	// addressable member (its transport stays up, its Snapshot does not
	// collapse), but it advertises that it is YIELDING OWNERSHIP. The moment
	// the draining Meta gossips, reconcileRingFromMembership on EVERY node
	// (including this one) drops this node from the consistent-hash ownership
	// ring, so LocateKeyN redistributes its positions to the survivors. The
	// survivors then enter the overlap-Acquire path with this node as
	// predecessor and forward writes to it (via the stored predecessor address)
	// while it keeps serving throughout the drain. The REAL membership.Leave()
	// + transport Shutdown stay in the existing Close teardown, run AFTER this
	// drain returns.
	if c.membership != nil {
		_ = c.membership.SetDraining(true)
	}

	// Drive reconcile + drainCheck and poll for completion until this node owns
	// no positions or ctx is done. The first drive runs immediately so a fast
	// hand-off does not pay one interval of latency.
	t := time.NewTicker(drainPollInterval)
	defer t.Stop()
	for {
		// Re-drive the unit reconcile so this node's now-shrunken desired set
		// (it excluded itself from the ownership ring the moment it gossiped
		// Draining) converts its yielded positions to Draining, then drainCheck
		// releases the ones whose successors wrote a serving marker. runReconcile is
		// idempotent + runs the overlap reconcile + runDrainChecks.
		//
		// NOTE: this drives the UNIT reconcile only, NOT reconcileRingFromMembership.
		// The membership-driven ring refresh (which drops this Draining node from
		// its own ownership ring) is left to the background runReconcileLoop tick.
		// Driving the ring reconcile here on the tight poll cadence was tried and
		// REVERTED: it amplifies transient membership-snapshot dips into eager
		// plain-releases of this node's still-needed positions, collapsing the
		// during-leave availability. See the KNOWN RESIDUAL note below.
		//
		// KNOWN RESIDUAL (handed to the next session): the survivors' ownership
		// convergence under this scale-down is not yet reliable - a survivor's ring
		// can transiently collapse, so the successor for some of this node's
		// replica-0 (primary) positions never writes a serving marker, drainCheck
		// never releases them, and the drain runs to the timeout. During-leave
		// availability holds (this node keeps serving its mounts throughout), but
		// the post-leave loss oracle can still see a primary position that no
		// survivor mounted. The root cause is survivor-side ring convergence.
		c.runReconcile()
		if c.ownedPositionCount() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// ownedPositionCount returns how many ReplicaUnit positions this node still has
// mounted. It is the completion signal for DrainForLeave: a Draining position
// keeps its mount until drainCheck releases it (deletes mountMap[ru]), and a
// plain-released position is deleted eagerly, so mountMap emptying to zero means
// every position has been handed off or released. Reads under mountMu.
func (c *Cluster) ownedPositionCount() int {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	return len(c.mountMap)
}
