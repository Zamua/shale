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
	"fmt"
	"os"
	"time"

	"github.com/Zamua/shale/pkg/storageunit"
)

// drainPollInterval is how often DrainForLeave re-drives the reconcile +
// drainCheck and re-tests for completion. It is deliberately tighter than
// reconcileInterval: the drain is a bounded, latency-sensitive shutdown wait,
// so it actively drives the loop rather than waiting for the background
// reconcile tick. The reconcile is idempotent, so an extra drive is cheap.
const drainPollInterval = 50 * time.Millisecond

// DrainForLeave performs the graceful-leave drain for a scale-down: it
// broadcasts the memberlist leave (so survivors re-own this node's positions
// and start forwarding to it via the overlap machine), then BLOCKS until this
// node owns no more positions (every position handed off to its successor and
// released by drainCheck, or plain-released) OR ctx is cancelled / its deadline
// fires.
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

	// FREEZE the ring at "survivors minus self" BEFORE broadcasting the leave.
	// Once Leave() goes out this node stops gossiping and its OWN membership
	// Snapshot collapses to empty within seconds (its failure detector marks
	// every peer dead), so reconcileRingFromMembership (the background loop AND
	// the drain below) would rebuild the ring from an empty snapshot and wipe
	// every survivor, leaving this node unable to see where its positions moved.
	// Setting leaving makes reconcileRingFromMembership a no-op; removing only
	// self rotates this node off its own positions while KEEPING the survivors
	// it must hand off to. Order matters: set leaving + remove self while the
	// ring still has the survivors, THEN broadcast the leave.
	c.leaving.Store(true)
	c.ring.Remove(c.cfg.NodeID)

	// Broadcast the graceful departure WITHOUT shutting the transport down, so
	// peers re-own this node's positions and begin forwarding to it while this
	// node keeps serving gRPC throughout the drain. The transport Shutdown stays
	// in the existing Close teardown after the drain returns.
	if c.membership != nil {
		_ = c.membership.Leave()
	}

	// Drive reconcile + drainCheck and poll for completion until this node owns
	// no positions or ctx is done. The first drive runs immediately so a fast
	// hand-off does not pay one interval of latency.
	t := time.NewTicker(drainPollInterval)
	defer t.Stop()
	for {
		if os.Getenv("SHALE_DRAIN_DIAG") != "" {
			snap := c.membership.Snapshot()
			fmt.Fprintf(os.Stderr, "[DRAINDIAG-SNAP node=%s snapshotLen=%d]\n", c.cfg.NodeID, len(snap))
		}
		// Re-drive the reconcile so the frozen ring's "this node no longer owns
		// these positions" converts them to Draining, then drainCheck releases
		// the ones whose successors wrote a serving marker. runReconcile is
		// idempotent + runs the overlap reconcile + runDrainChecks.
		c.runReconcile()
		if c.ownedPositionCount() == 0 {
			return nil
		}
		if os.Getenv("SHALE_DRAIN_DIAG") != "" {
			c.mountMu.RLock()
			stuck := make([]storageunit.ReplicaUnit, 0, len(c.mountMap))
			phases := make(map[storageunit.ReplicaUnit]storageunit.HandoffState, len(c.mountMap))
			for ru := range c.mountMap {
				stuck = append(stuck, ru)
				phases[ru] = c.handoffPhase[ru]
			}
			nMembers := len(c.ring.Members())
			c.mountMu.RUnlock()
			diag := fmt.Sprintf("[DRAINDIAG node=%s owned=%d ringMembers=%d]", c.cfg.NodeID, len(stuck), nMembers)
			for _, ru := range stuck {
				st := phases[ru]
				me, ok, _ := c.replicaFactory.ReadServingMarker(ru)
				diag += fmt.Sprintf("\n  ru=%v phase=%v openEpoch=%v marker=(%v,ok=%v) wouldRelease=%v",
					ru, st.Phase, st.OpenEpoch, me, ok, ok && me > st.OpenEpoch)
			}
			fmt.Fprintln(os.Stderr, diag)
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
