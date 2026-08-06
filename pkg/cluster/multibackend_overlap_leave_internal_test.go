package cluster

// White-box tests for the graceful-leave drain (v0.8 Phase 2e, pending ranges,
// scale-down): DrainForLeave + the completion gate. They drive the drain against
// a per-replica shared-backing factory + a real ring, with NO membership / gRPC
// (membership is nil; the leave is modeled by injecting the Draining bit via the
// test hook - under the pending-ranges model the leaver STAYS in the ring, so its
// positions become current-but-not-pending and the reconcile sets them Draining).
// The wired cross-node leave + the slow-mount loss-oracle acceptance gate are
// covered by the integration tests.

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// seedOwnedPositions mounts every position the live ring assigns self, as if
// self had been serving them before the leave. Returns the mounted positions.
func seedOwnedPositions(t *testing.T, c *Cluster, backing *sharedfactory.Backing) []storageunit.ReplicaUnit {
	t.Helper()
	desired := c.desiredReplicaUnits()
	if len(desired) == 0 {
		t.Fatalf("self desires no positions; ring fixture broken")
	}
	h := backing.Handle()
	for _, target := range desired {
		b, _, err := h.OpenReplicaUnit(target, 1)
		if err != nil {
			t.Fatalf("seed mount %v: %v", target, err)
		}
		c.mounts.mountUndecorated(target, b)
	}
	return desired
}

// markDraining models the leave under the pending-ranges model: self STAYS in
// the ring (it is not removed) but advertises the Draining bit. Every node's
// routedReplicasForKey then computes self's positions as current-but-not-pending, so
// the reconcile sets them Draining. With no coordination transport in this
// white-box test, the draining set is injected directly via the test hook.
func markDraining(c *Cluster) {
	c.draining = map[storageunit.NodeID]struct{}{storageunit.NodeID(c.cfg.NodeID): {}}
}

// TestLeave_DrainForLeave_ReleasesAllWhenSuccessorsReady: the happy path. Self
// owns several positions; it leaves; each survivor becomes Ready (writes a
// serving marker at an epoch strictly above self's open epoch); DrainForLeave
// drives the reconcile + drainCheck and returns nil once self owns no positions.
func TestLeave_DrainForLeave_ReleasesAllWhenSuccessorsReady(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3", "n4")

	owned := seedOwnedPositions(t, c, backing)
	if c.ownedPositionCount() != len(owned) {
		t.Fatalf("seed: ownedPositionCount = %d, want %d", c.ownedPositionCount(), len(owned))
	}

	// Self leaves: advertise Draining (it stays in the ring) so its positions
	// become current-but-not-pending and the reconcile drains them.
	markDraining(c)

	// Each successor is Ready: write a serving marker at epoch 2 (strictly above
	// self's open epoch 1) for every owned position. This is what drainCheck
	// polls to release.
	h := backing.Handle()
	for _, target := range owned {
		if err := h.WriteServingMarker(storageunit.ReplicaMount(target), 2); err != nil {
			t.Fatalf("write serving marker %v: %v", target, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.DrainForLeave(ctx); err != nil {
		t.Fatalf("DrainForLeave returned %v, want nil (all positions handed off)", err)
	}
	if got := c.ownedPositionCount(); got != 0 {
		t.Fatalf("after a successful drain self must own no positions, got %d", got)
	}
}

// TestLeave_DrainForLeave_TimesOutWhenSuccessorStuck: the residual. A successor
// never becomes Ready (no serving marker), so the position stays Draining; the
// drain times out (ctx deadline) and the position is still mounted (Close then
// proceeds to tear it down, accepting the gap - no worse than the disabled
// behavior). This proves the wait is bounded.
func TestLeave_DrainForLeave_TimesOutWhenSuccessorStuck(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3", "n4")

	owned := seedOwnedPositions(t, c, backing)
	markDraining(c)
	// Deliberately write NO serving markers: every successor is "stuck".

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := c.DrainForLeave(ctx)
	if err == nil {
		t.Fatalf("DrainForLeave must time out when no successor becomes Ready, got nil")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("DrainForLeave error = %v, want context.DeadlineExceeded", err)
	}
	// The stuck positions are still Draining + mounted (they did NOT release).
	if c.ownedPositionCount() != len(owned) {
		t.Fatalf("stuck positions must stay mounted on timeout: ownedPositionCount = %d, want %d",
			c.ownedPositionCount(), len(owned))
	}
	for _, target := range owned {
		if st := c.handoffPhaseOf(target); st.Phase != storageunit.PhaseDraining {
			t.Fatalf("stuck position %v phase = %v, want Draining", target, st.Phase)
		}
	}
}

// TestLeave_DrainForLeave_NoOpOutsideMultiReplicated: DrainForLeave is a no-op
// (returns nil immediately, does not even broadcast) when the cluster is not
// multi-backend overlap (R=1), consistent with the feature living behind
// multiReplicated().
func TestLeave_DrainForLeave_NoOpOutsideMultiReplicated(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 1, backing, "n1", "n2") // R=1: not replicated
	if c.multiReplicated() {
		t.Fatalf("R=1 fixture must not be multiReplicated")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.DrainForLeave(ctx); err != nil {
		t.Fatalf("DrainForLeave outside multiReplicated must be a no-op nil, got %v", err)
	}
}

// TestLeave_ownedPositionCount tracks the mount-table size.
func TestLeave_ownedPositionCount(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")
	if c.ownedPositionCount() != 0 {
		t.Fatalf("fresh cluster owns no positions, got %d", c.ownedPositionCount())
	}
	h := backing.Handle()
	target := ru(0, 0, 0)
	b, _, err := h.OpenReplicaUnit(target, 1)
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	c.mounts.mountUndecorated(target, b)
	if c.ownedPositionCount() != 1 {
		t.Fatalf("after one mount ownedPositionCount = %d, want 1", c.ownedPositionCount())
	}
}

// TestLeave_BudgetExpiry_NamesTheUnservedPositions pins the evidence half of
// the graceful-leave trade. A leave whose budget expires departs anyway, which
// is deliberate (it must not hang on a stuck successor) and leaves exactly
// those positions unserved from teardown until a successor mounts: a routed op
// for one finds NO holder in the union and gets the retryable acquiring
// refusal. That window is indistinguishable from a routing bug downstream
// unless the departing node names it, so the leaver must log which positions it
// abandoned and how many.
func TestLeave_BudgetExpiry_NamesTheUnservedPositions(t *testing.T) {
	backing := sharedfactory.NewBacking()
	var log bytes.Buffer
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3", "n4")
	c.cfg.LogOutput = &log
	c.cfg.GracefulLeaveDrainTimeout = 200 * time.Millisecond

	owned := seedOwnedPositions(t, c, backing)
	markDraining(c)
	// No serving markers: every successor is stuck, so the budget is what ends
	// the drain and every owned position is left unserved.

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.DrainForLeave(ctx); err == nil {
		t.Fatal("DrainForLeave must time out when no successor becomes Ready")
	}

	got := log.String()
	if !strings.Contains(got, "NOT HANDED OFF") {
		t.Fatalf("a leave that abandoned %d position(s) said nothing about it; log:\n%s", len(owned), got)
	}
	if !strings.Contains(got, fmt.Sprintf("%d POSITION(S)", len(owned))) {
		t.Fatalf("the count of abandoned positions is missing or wrong (want %d); log:\n%s", len(owned), got)
	}
	for _, ru := range owned {
		if !strings.Contains(got, ru.String()) {
			t.Fatalf("abandoned position %v is not named in the log; log:\n%s", ru, got)
		}
	}
}

// TestLeave_PositionsAwaitingSuccessor_EmptyWhenAllHandedOff pins the other
// direction: a clean hand-off names nothing, so the loud line above cannot fire
// on a leave that did what it promised.
func TestLeave_PositionsAwaitingSuccessor_EmptyWhenAllHandedOff(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3", "n4")

	owned := seedOwnedPositions(t, c, backing)
	markDraining(c)
	h := backing.Handle()
	for _, ru := range owned {
		// A successor serving strictly above this node's open epoch.
		if err := h.WriteServingMarker(storageunit.ReplicaMount(ru), c.ownOpenEpoch(ru)+1); err != nil {
			t.Fatalf("plant marker for %v: %v", ru, err)
		}
	}
	if waiting := c.positionsAwaitingSuccessor(); len(waiting) != 0 {
		t.Fatalf("every position has a serving successor, but %d still reported as awaiting: %v", len(waiting), waiting)
	}
}
