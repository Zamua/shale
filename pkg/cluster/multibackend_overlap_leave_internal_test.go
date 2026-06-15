package cluster

// White-box tests for the graceful-leave drain (v0.8 Phase 2e, scale-down):
// DrainForLeave + ownedPositionCount. They drive the drain against a per-replica
// shared-backing factory + a real ring, with NO membership / gRPC (membership is
// nil; the leave is modeled by removing self from the ring, exactly what
// reconcileRingFromMembership would do once the leave gossips out). The wired
// cross-node leave + the slow-mount loss-oracle acceptance gate are covered by
// the integration tests.

import (
	"context"
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
		b, err := h.OpenReplicaUnit(target, 1)
		if err != nil {
			t.Fatalf("seed mount %v: %v", target, err)
		}
		c.mountMap[target] = b
	}
	// Capture the prior snapshot as the pre-leave live sets so that after self
	// leaves, each of its positions is a single-hop move to its successor.
	c.priorDesiredReplicas = c.liveDesiredReplicaSets()
	return desired
}

// leaveRing removes self from the ring, modeling the post-leave ring the
// background reconcileRingFromMembership would converge to once the memberlist
// Leave gossips out. After this, self is no longer in any position's live
// replica set, so its mounted positions are moves to survivors (Draining).
func leaveRing(c *Cluster) {
	c.ring.Remove(c.cfg.NodeID)
	c.bumpRingGen()
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

	// Self leaves: drop it from the ring so its positions become moves.
	leaveRing(c)

	// Each successor is Ready: write a serving marker at epoch 2 (strictly above
	// self's open epoch 1) for every owned position. This is what drainCheck
	// polls to release.
	h := backing.Handle()
	for _, target := range owned {
		if err := h.WriteServingMarker(target, 2); err != nil {
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
	leaveRing(c)
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

// TestLeave_ownedPositionCount tracks the mountMap size.
func TestLeave_ownedPositionCount(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")
	if c.ownedPositionCount() != 0 {
		t.Fatalf("fresh cluster owns no positions, got %d", c.ownedPositionCount())
	}
	h := backing.Handle()
	target := ru(0, 0, 0)
	b, err := h.OpenReplicaUnit(target, 1)
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	c.mountMap[target] = b
	if c.ownedPositionCount() != 1 {
		t.Fatalf("after one mount ownedPositionCount = %d, want 1", c.ownedPositionCount())
	}
}
