package cluster

import (
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// These tests pin the drain-release gate to THIS node's OWN open epoch (the
// epoch the factory returned when this node opened the position), NOT the live
// shared durable epoch. They are the regression guard for the real-MinIO
// graceful-scale-down availability gap (#410).
//
// The bug they guard against: beginDrain used to set state.OpenEpoch =
// openEpochForReplica(ru) = a re-read of the LIVE shared durable epoch. The
// durable is a monotone counter that EVERY node's open bumps, so a SUCCESSOR
// opening the position (legitimately, during the handoff) pushes the durable -
// and therefore the captured release threshold - up to (or past) the
// successor's own serving-marker epoch. The release gate is strict
// (markerEpoch > state.OpenEpoch), so once the threshold caught up to the
// marker the drain could NEVER release and ran to its 90s timeout: the position
// the leaver was still serving had no transparent successor, which is the
// availability gap. The fix records the leaver's EXACT open epoch (the factory
// return) at mount and gates on that immutable value, so a successor's marker is
// always strictly above it.

// TestOverlap_beginDrain_GatesOnOwnOpenEpoch_ReleasesAfterSuccessorMarker is the
// DETERMINISTIC gate-level reproduction. The leaver opens at epoch 1; a successor
// then opens (bumping the live durable to 2) and writes its serving marker at 2
// BEFORE the leaver drains - the real-infra ordering (a slow successor mount + a
// ring-flap reclaim -> re-drain cycle means the leaver re-drains AFTER the
// successor already mounted). With the gate fixed to the leaver's own open epoch
// (1), the successor marker (2) is strictly above it and the leaver releases.
//
// Pre-fix (gate on the live durable = 2) the marker 2 is NOT > 2 and the drain
// hangs. The open-epoch record here is exactly what the real mount path
// (acquireReplicaUnitOverlapBlocking) records from the factory return.
func TestOverlap_beginDrain_GatesOnOwnOpenEpoch_ReleasesAfterSuccessorMarker(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	target := ru(0, 0, 0)

	// 1) The LEAVER opens + mounts the position. acquireBaseEpoch=1 over a fresh
	//    durable (0) lands at open epoch 1; record it as this node's gate exactly
	//    as the real mount flip does (from the factory's RETURNED epoch).
	hLeaver := backing.Handle()
	bLeaver, leaverEpoch, err := hLeaver.OpenReplicaUnit(target, acquireBaseEpoch)
	if err != nil {
		t.Fatalf("leaver open: %v", err)
	}
	if leaverEpoch != 1 {
		t.Fatalf("precondition: leaver open epoch should be 1, got %d", leaverEpoch)
	}
	c.mounts.mountUndecorated(target, bLeaver)
	c.mounts.recordOpenEpoch(target, leaverEpoch)

	// 2) A SUCCESSOR opens the position (bumping the live durable to 2) and writes
	//    its serving marker at its open epoch (2) -- BEFORE the leaver drains.
	hSucc := backing.Handle()
	_, succEpoch, err := hSucc.OpenReplicaUnit(target, acquireBaseEpoch)
	if err != nil {
		t.Fatalf("successor open: %v", err)
	}
	if succEpoch != 2 {
		t.Fatalf("precondition: successor open epoch should be 2, got %d", succEpoch)
	}
	if err := hSucc.WriteServingMarker(storageunit.ReplicaMount(target), succEpoch); err != nil {
		t.Fatalf("successor marker: %v", err)
	}

	// 3) NOW the leaver drains. The fixed gate captures the leaver's OWN open epoch
	//    (1) from the recorded open epoch, NOT the live durable (2, the successor's bump).
	c.beginDrain(target)
	st := c.handoffPhaseOf(target)
	if st.Phase != storageunit.PhaseDraining {
		t.Fatalf("beginDrain should have set Draining, got %v", st.Phase)
	}
	if st.OpenEpoch != leaverEpoch {
		t.Fatalf("drain gate should be the leaver's own open epoch %d, got %d (gated on the live durable - the #410 bug)",
			leaverEpoch, st.OpenEpoch)
	}

	// 4) drainCheck: the successor IS serving (marker 2 > gate 1) and holds the
	//    leaver's data (shared bytes), so the leaver releases.
	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("DRAIN HANG: gate=%d, successor marker=%d; the leaver should have released on the successor marker",
			st.OpenEpoch, succEpoch)
	}
}

// TestOverlap_beginDrain_RealMountPath_CapturesOwnOpenEpoch drives the REAL mount
// flip (acquireReplicaUnitOverlapBlocking) rather than installing the mount by
// hand, so it exercises the actual capture mechanism end-to-end: the flip records
// the open epoch from the factory return, and beginDrain later reads it. A successor
// then opens + marks above the leaver's epoch and the leaver releases.
func TestOverlap_beginDrain_RealMountPath_CapturesOwnOpenEpoch(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	target := ru(0, 0, 0)

	// The leaver mounts through the real overlap-blocking flip. With no Acquiring
	// phase set up the flip installs the mount, drops to Owned, records
	// the open epoch from the factory return, and writes its own serving marker - all
	// at its EXACT open epoch (1).
	_ = c.acquireReplicaUnitOverlapBlocking(target)
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("leaver should have mounted via the real flip")
	}
	gotEpoch, ok := c.mounts.openEpochOf(target)
	if !ok || gotEpoch != 1 {
		t.Fatalf("real mount flip should have recorded an open epoch of 1, got %v ok=%v", gotEpoch, ok)
	}

	// A successor opens (durable -> 2) + marks at 2.
	hSucc := backing.Handle()
	_, succEpoch, err := hSucc.OpenReplicaUnit(target, acquireBaseEpoch)
	if err != nil {
		t.Fatalf("successor open: %v", err)
	}
	if err := hSucc.WriteServingMarker(storageunit.ReplicaMount(target), succEpoch); err != nil {
		t.Fatalf("successor marker: %v", err)
	}

	c.beginDrain(target)
	if st := c.handoffPhaseOf(target); st.OpenEpoch != 1 {
		t.Fatalf("drain gate should be the captured own open epoch 1, got %d", st.OpenEpoch)
	}
	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("DRAIN HANG: leaver should have released (own gate 1 < successor marker %d)", succEpoch)
	}
}

// TestOverlap_beginDrain_EscalationImmunity_MultiOpenCascade pins that the gate is
// IMMUNE to durable escalation: multiple successors open the position (durable
// climbs to 3, far above the leaver's open epoch 1), yet the leaver still
// releases on the latest serving marker. Pre-fix, beginDrain would have captured
// the escalated durable (3) and the marker (3) could never be strictly above it.
func TestOverlap_beginDrain_EscalationImmunity_MultiOpenCascade(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	target := ru(0, 0, 0)

	// Leaver opens + mounts (epoch 1), records its gate from the factory return.
	hLeaver := backing.Handle()
	bLeaver, leaverEpoch, err := hLeaver.OpenReplicaUnit(target, acquireBaseEpoch)
	if err != nil {
		t.Fatalf("leaver open: %v", err)
	}
	c.mounts.mountUndecorated(target, bLeaver)
	c.mounts.recordOpenEpoch(target, leaverEpoch)

	// TWO successors open in cascade (a slow handoff that re-acquires), each on its
	// own handle, bumping the live durable 1 -> 2 -> 3. The latest serving owner
	// writes the marker (3).
	var lastMarker storageunit.Epoch
	for i := 0; i < 2; i++ {
		h := backing.Handle()
		_, ep, err := h.OpenReplicaUnit(target, acquireBaseEpoch)
		if err != nil {
			t.Fatalf("successor %d open: %v", i, err)
		}
		if err := h.WriteServingMarker(storageunit.ReplicaMount(target), ep); err != nil {
			t.Fatalf("successor %d marker: %v", i, err)
		}
		lastMarker = ep
	}
	if lastMarker != 3 {
		t.Fatalf("precondition: durable should have escalated to 3, last marker=%d", lastMarker)
	}

	c.beginDrain(target)
	st := c.handoffPhaseOf(target)
	if st.OpenEpoch != leaverEpoch {
		t.Fatalf("gate must stay the leaver's own open epoch %d despite durable escalation to %d, got %d",
			leaverEpoch, lastMarker, st.OpenEpoch)
	}
	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("DRAIN HANG under escalation: gate=%d, latest marker=%d; the leaver should have released",
			st.OpenEpoch, lastMarker)
	}
}

// TestOverlap_allOwnedPositionsHandedOff_GatesOnOwnOpenEpoch pins the preStop
// graceful-leave COMPLETION gate (DrainForLeave -> allOwnedPositionsHandedOff) to
// the leaver's own open epoch. This is the gate whose hang IS the real-MinIO
// availability gap: the leaver keeps serving until every owned position reports a
// successor serving marker strictly above its open epoch; if the gate read the
// climbing durable instead, a successor's open would lift the threshold to the
// marker and the leave would never complete (the preStop drain runs to timeout
// while the position has no transparent successor). A successor (and a cascade
// that escalates the durable to 3) still satisfies the gate against the stable
// own epoch (1).
func TestOverlap_allOwnedPositionsHandedOff_GatesOnOwnOpenEpoch(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	target := ru(0, 0, 0)

	// Leaver mounts at epoch 1 and records its gate from the factory return.
	hLeaver := backing.Handle()
	bLeaver, leaverEpoch, err := hLeaver.OpenReplicaUnit(target, acquireBaseEpoch)
	if err != nil {
		t.Fatalf("leaver open: %v", err)
	}
	c.mounts.mountUndecorated(target, bLeaver)
	c.mounts.recordOpenEpoch(target, leaverEpoch)

	// Before any successor marker the position is NOT handed off (the leaver keeps
	// serving).
	if c.allOwnedPositionsHandedOff() {
		t.Fatalf("no successor marker yet: position should NOT report handed-off")
	}

	// A cascade of successors opens (durable escalates 1 -> 2 -> 3), latest marks at 3.
	var lastMarker storageunit.Epoch
	for i := 0; i < 2; i++ {
		h := backing.Handle()
		_, ep, err := h.OpenReplicaUnit(target, acquireBaseEpoch)
		if err != nil {
			t.Fatalf("successor %d open: %v", i, err)
		}
		if err := h.WriteServingMarker(storageunit.ReplicaMount(target), ep); err != nil {
			t.Fatalf("successor %d marker: %v", i, err)
		}
		lastMarker = ep
	}

	// With the gate on the leaver's stable own epoch (1), marker 3 > 1 => handed
	// off, even though the durable escalated to 3. Pre-fix (gate on live durable 3)
	// the marker 3 is NOT > 3 and the leave would hang.
	if !c.allOwnedPositionsHandedOff() {
		t.Fatalf("LEAVE HANG: own gate=%d, latest marker=%d; the leave should complete (successor serving above the leaver's own epoch)",
			leaverEpoch, lastMarker)
	}
}
