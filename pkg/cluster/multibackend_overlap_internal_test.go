package cluster

// White-box tests for the v0.8 Phase 2e (Option B) overlap-handoff controller.
// They drive the unexported controller helpers (predecessorOf,
// positionHasLiveSuccessor, reconcileReplicaUnitsOverlap, acquireReplicaUnitOverlap,
// drainCheck) against a per-replica shared-backing factory + a real ring, with no
// membership / gRPC, so they run fast and deterministically (no memberlist). The
// pure HandoffState FSM is covered in pkg/storageunit/handoff_test.go; the wired
// cross-node forward + the slow-mount loss oracle are covered by the integration
// acceptance gate.

import (
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

func gu(g uint64, u uint32) storageunit.GenUnit {
	return storageunit.NewGenUnit(storageunit.Generation(g), storageunit.UnitID(u))
}

func ru(g uint64, u uint32, r uint8) storageunit.ReplicaUnit {
	return storageunit.NewReplicaUnit(gu(g, u), r)
}

// -- predecessorOf --------------------------------------------------------

func TestOverlap_predecessorOf_SingleHopMove(t *testing.T) {
	target := ru(0, 5, 0)
	prior := map[storageunit.GenUnit][]storageunit.NodeID{
		gu(0, 5): {"old", "x"},
	}
	live := map[storageunit.GenUnit][]storageunit.NodeID{
		gu(0, 5): {"new", "x"}, // position 0 moved old -> new
	}
	got, ok := predecessorOf(target, prior, live)
	if !ok || got != "old" {
		t.Fatalf("predecessorOf = (%q, %v), want (old, true)", got, ok)
	}
}

func TestOverlap_predecessorOf_NoPriorSnapshot(t *testing.T) {
	if _, ok := predecessorOf(ru(0, 5, 0), nil, nil); ok {
		t.Fatalf("nil prior snapshot must yield no predecessor (pure new mount)")
	}
}

func TestOverlap_predecessorOf_PriorHolderStillLive_Ambiguous(t *testing.T) {
	target := ru(0, 5, 0)
	prior := map[storageunit.GenUnit][]storageunit.NodeID{gu(0, 5): {"old", "x"}}
	// The prior holder STILL holds the position live: this is not a move away
	// from it, so no single-hop predecessor (degrade to Option A).
	live := map[storageunit.GenUnit][]storageunit.NodeID{gu(0, 5): {"old", "x"}}
	if _, ok := predecessorOf(target, prior, live); ok {
		t.Fatalf("prior holder still live must yield no predecessor")
	}
}

func TestOverlap_predecessorOf_NoPriorHolderForPosition(t *testing.T) {
	target := ru(0, 5, 1)
	// prior snapshot has only position 0 (shorter than replica index 1).
	prior := map[storageunit.GenUnit][]storageunit.NodeID{gu(0, 5): {"old"}}
	live := map[storageunit.GenUnit][]storageunit.NodeID{gu(0, 5): {"old", "new"}}
	if _, ok := predecessorOf(target, prior, live); ok {
		t.Fatalf("position with no prior holder must yield no predecessor (new mount)")
	}
}

// -- positionHasLiveSuccessor --------------------------------------------

func TestOverlap_positionHasLiveSuccessor(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 8, 2, backing, "self", "n2", "n3")

	// A position whose live holder is some OTHER node = a move (drain).
	moved := ru(0, 0, 0)
	live := map[storageunit.GenUnit][]storageunit.NodeID{
		gu(0, 0): {"n2", "n3"},
	}
	if !c.positionHasLiveSuccessor(moved, live) {
		t.Fatalf("position held live by another node must report a successor")
	}

	// A position absent from the live set = plain drop-out (no successor).
	gone := ru(0, 1, 0)
	if c.positionHasLiveSuccessor(gone, map[storageunit.GenUnit][]storageunit.NodeID{}) {
		t.Fatalf("position absent from live set must report no successor")
	}
}

// -- reconcileReplicaUnitsOverlap: DRAIN (position moving away) -----------

// TestOverlap_Reconcile_MoveAway_SetsDrainingKeepsMount: a mounted position the
// live ring now assigns to ANOTHER node (and the prior snapshot had this node
// holding it) is set Draining and KEPT MOUNTED (not released).
func TestOverlap_Reconcile_MoveAway_SetsDrainingKeepsMount(t *testing.T) {
	backing := sharedfactory.NewBacking()
	// Live ring does NOT contain self for the units it should drain: build a
	// cluster where self is NOT in the live replica set, but mount + prior say
	// it held position 0.
	c := newReplicatedCluster(t, "self", 4, 2, backing, "n2", "n3", "n4")

	// Pick a unit and mount position 0 on self as if it were the prior holder.
	target := ru(0, 0, 0)
	h := backing.Handle()
	b, err := h.OpenReplicaUnit(target, 1)
	if err != nil {
		t.Fatalf("seed mount: %v", err)
	}
	c.mountMap[target] = b

	// Prior snapshot: self held position 0 of unit 0.
	c.priorDesiredReplicas = map[storageunit.GenUnit][]storageunit.NodeID{
		gu(0, 0): {"self", "n2"},
	}

	c.reconcileReplicaUnitsOverlap()

	st := c.handoffPhaseOf(target)
	if st.Phase != storageunit.PhaseDraining {
		t.Fatalf("moved-away position phase = %v, want Draining", st.Phase)
	}
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("Draining position must stay mounted (keep serving), but mount was removed")
	}
}

// TestOverlap_Reconcile_PlainDropOut_Releases: a mounted position NOT desired
// and with NO live successor (vanished from the set) is plain clean-cut
// released, NOT drained.
func TestOverlap_Reconcile_PlainDropOut_Releases(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "n2", "n3", "n4")

	// Mount a position whose unit does not exist in the live set at this index:
	// use an out-of-range replica index so positionHasLiveSuccessor is false.
	target := ru(0, 0, 5) // replica index 5 has no live holder (R=2)
	h := backing.Handle()
	b, err := h.OpenReplicaUnit(target, 1)
	if err != nil {
		t.Fatalf("seed mount: %v", err)
	}
	c.mountMap[target] = b
	// No prior snapshot entry for this position -> not a move.

	c.reconcileReplicaUnitsOverlap()

	if st := c.handoffPhaseOf(target); st.Phase != 0 {
		t.Fatalf("plain drop-out must NOT enter a handoff phase, got %v", st.Phase)
	}
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("plain drop-out position must be released (unmounted)")
	}
}

// -- reconcileReplicaUnitsOverlap: ACQUIRE (position moving in) -----------

// TestOverlap_Reconcile_MoveIn_SinglePredecessor_AcquiresAndRecords: a desired
// position not yet mounted, whose prior holder (a single identifiable node)
// moved away, enters Acquiring with the predecessor recorded, then completes the
// mount flip to Owned (sharedfactory mounts instantly) and writes the serving
// marker.
func TestOverlap_Reconcile_MoveIn_SinglePredecessor_AcquiresAndRecords(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	// Find a unit/position self DESIRES (is in the live replica set for).
	desired := c.desiredReplicaUnits()
	if len(desired) == 0 {
		t.Fatalf("self desires no units; ring fixture broken")
	}
	target := desired[0]

	// Prior snapshot: a DIFFERENT node held target's exact position; self did
	// not. Build the prior set as the live set but with target's position held
	// by "pred" instead of self.
	live := c.liveDesiredReplicaSets()
	priorHolders := append([]storageunit.NodeID(nil), live[target.Unit]...)
	priorHolders[target.Replica] = "n2" // a node still in the live ring elsewhere
	// ensure prior holder differs from live holder (self)
	if priorHolders[target.Replica] == live[target.Unit][target.Replica] {
		t.Fatalf("prior holder must differ from live holder for a move")
	}
	c.priorDesiredReplicas = map[storageunit.GenUnit][]storageunit.NodeID{
		target.Unit: priorHolders,
	}

	c.reconcileReplicaUnitsOverlap()

	// sharedfactory mounts instantly, so acquireReplicaUnitOverlap completes the
	// flip synchronously: the position is now Owned (mounted, no phase entry).
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("acquired position must be mounted after the (instant) mount flip")
	}
	if st := c.handoffPhaseOf(target); st.Phase != 0 {
		t.Fatalf("after the mount flip the position is Owned (no phase entry), got %v", st.Phase)
	}
	// The serving marker was written at the flip.
	if _, ok := backing.ServingMarker(target); !ok {
		t.Fatalf("mount flip must write the serving marker exactly once")
	}
}

// TestOverlap_Reconcile_PureNewMount_NoPredecessor_FallsThroughToCleanAcquire:
// a desired position not mounted with NO prior holder (initial convergence) is
// acquired clean-cut (no Acquiring phase, no predecessor, no overlap forward).
func TestOverlap_Reconcile_PureNewMount_NoPredecessor_FallsThroughToCleanAcquire(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	desired := c.desiredReplicaUnits()
	target := desired[0]

	// No prior snapshot at all -> pure new mount.
	c.priorDesiredReplicas = nil

	c.reconcileReplicaUnitsOverlap()

	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("pure-new-mount position must be acquired (mounted)")
	}
	if st := c.handoffPhaseOf(target); st.Phase != 0 {
		t.Fatalf("pure new mount must not enter a handoff phase, got %v", st.Phase)
	}
}

// -- drainCheck -----------------------------------------------------------

// TestOverlap_drainCheck_ReleasesOnServingMarker: a Draining position releases
// exactly when the durable serving marker is present at epoch >= its open epoch.
func TestOverlap_drainCheck_ReleasesOnServingMarker(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	target := ru(0, 0, 0)
	h := backing.Handle()
	b, err := h.OpenReplicaUnit(target, 1) // open epoch 1 on self
	if err != nil {
		t.Fatalf("seed mount: %v", err)
	}
	c.mountMap[target] = b
	c.handoffPhase[target] = storageunit.HandoffState{Phase: storageunit.PhaseDraining, OpenEpoch: 1}

	// No marker yet: drainCheck must NOT release (keeps serving).
	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("with no serving marker the Draining position must stay mounted")
	}
	if st := c.handoffPhaseOf(target); st.Phase != storageunit.PhaseDraining {
		t.Fatalf("with no marker the phase must stay Draining, got %v", st.Phase)
	}

	// A serving marker at a HIGHER epoch (the new owner became Ready): release.
	if err := h.WriteServingMarker(target, 2); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("with a serving marker >= open epoch the Draining position must release")
	}
	if st := c.handoffPhaseOf(target); st.Phase != 0 {
		t.Fatalf("after release the phase entry must be dropped (Absent), got %v", st.Phase)
	}
}

// TestOverlap_drainCheck_NeverReleasesOnBareFenceEpoch: the durable fence epoch
// advancing (DurableEpochReplica) WITHOUT a serving marker must NOT release the
// old owner (crash-case-1 protection: a new owner that fenced then crashed
// mid-mount advances the fence but never writes the marker).
func TestOverlap_drainCheck_NeverReleasesOnBareFenceEpoch(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	target := ru(0, 0, 0)
	h := backing.Handle()
	b, err := h.OpenReplicaUnit(target, 1)
	if err != nil {
		t.Fatalf("seed mount: %v", err)
	}
	c.mountMap[target] = b
	c.handoffPhase[target] = storageunit.HandoffState{Phase: storageunit.PhaseDraining, OpenEpoch: 1}

	// Simulate a new owner FENCING (advancing the durable epoch) WITHOUT ever
	// writing the serving marker (it crashed mid-mount). A second handle opens
	// at a higher epoch, bumping the durable fence epoch, but writes no marker.
	h2 := backing.Handle()
	if _, err := h2.OpenReplicaUnit(target, 5); err != nil {
		t.Fatalf("fence open: %v", err)
	}
	if got, _ := h.DurableEpochReplica(target); got < 5 {
		t.Fatalf("durable fence epoch should have advanced to >=5, got %d", got)
	}

	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("a bare fence-epoch advance (no serving marker) must NOT release the old owner")
	}
	if st := c.handoffPhaseOf(target); st.Phase != storageunit.PhaseDraining {
		t.Fatalf("phase must stay Draining on a bare fence advance, got %v", st.Phase)
	}
}
