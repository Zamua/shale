package cluster

import (
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// These tests pin the reconcile invariant that fixes the REAL on-object-storage
// graceful-scale-down hang (#410, #404): a mounted position that is in the
// PENDING set is HELD stable across reconcile passes; only a position ABSENT
// from PENDING is drained (if still current) or released (if not). The bug they
// guard against is the reconcile tearing down transitional mounts, which makes
// the handoff oscillate and never complete (the leaver fights its own drain to
// the GracefulLeaveDrainTimeout). The single-pass DrainSplit / PendingOwner
// tests miss it because the oscillation only shows across MULTIPLE passes.

// TestOverlap_Reconcile_SelfDraining_GateStable_NoReclaimEscalation reproduces the
// LEAVER side. A gracefully-leaving node is still a ring member, so it computes
// itself as a CURRENT owner of all its positions; they are current-but-not-pending
// (the drain split excludes self from pending). The RECLAIM half must NOT fire for
// them - they are being handed off, not flip-flopped back. With the bug, reclaim
// un-drains every position each tick and the drain half re-drains it, re-running
// beginDrain and re-capturing the climbing durable epoch, so the gate escalates and
// the drain never releases. The fix leaves the position Draining at a STABLE gate.
func TestOverlap_Reconcile_SelfDraining_GateStable_NoReclaimEscalation(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3", "n4")
	c.draining = map[string]struct{}{"self": {}}

	current := c.desiredReplicaUnits()
	if len(current) == 0 {
		t.Fatal("self owns no positions; ring fixture broken")
	}
	// Mount undecorated + directly (no open epoch recorded) so ownOpenEpoch takes the
	// durable fallback - the same climbing source the staging escalation came
	// through - making a reclaim re-capture observable as gate escalation.
	h := backing.Handle()
	for _, target := range current {
		b, _, err := h.OpenReplicaUnit(target, 1)
		if err != nil {
			t.Fatalf("seed mount %v: %v", target, err)
		}
		c.mounts.mountUndecorated(target, b)
	}

	// Pass 1: positions enter Draining at the current durable epoch.
	c.reconcileReplicaUnitsOverlap()
	gate1 := make(map[storageunit.ReplicaUnit]storageunit.Epoch, len(current))
	for _, target := range current {
		st := c.handoffPhaseOf(target)
		if st.Phase != storageunit.PhaseDraining {
			t.Fatalf("pass1: %v phase=%v want Draining", target, st.Phase)
		}
		gate1[target] = st.OpenEpoch
	}

	// SUCCESSORS open each position several times (distinct handles), climbing the
	// shared durable far above the leaver's captured gate.
	for _, target := range current {
		for i := 0; i < 3; i++ {
			hs := backing.Handle()
			if _, _, err := hs.OpenReplicaUnit(target, 1); err != nil {
				t.Fatalf("successor open %v: %v", target, err)
			}
		}
	}

	// Passes 2-3: a self-draining (current-but-not-pending) position must NOT be
	// reclaimed, so beginDrain does not re-run and the gate does NOT escalate.
	c.reconcileReplicaUnitsOverlap()
	c.reconcileReplicaUnitsOverlap()
	for _, target := range current {
		st := c.handoffPhaseOf(target)
		if st.Phase != storageunit.PhaseDraining {
			t.Fatalf("%v must stay Draining across passes, got %v", target, st.Phase)
		}
		if st.OpenEpoch != gate1[target] {
			t.Fatalf("DRAIN GATE ESCALATED for %v: %d -> %d. The reclaim half un-drained a self-draining position and the re-drain re-captured the climbed durable; the drain would never release. A self-draining leaver must NOT reclaim its own (current-but-not-pending) positions.",
				target, gate1[target], st.OpenEpoch)
		}
		if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
			t.Fatalf("%v must stay mounted (keep serving) while draining", target)
		}
	}
}

// TestOverlap_Reconcile_PendingMount_HeldAcrossReconciles reproduces the SUCCESSOR
// side. A pending-only owner (a peer is draining) acquires + mounts + serving-marks
// the leaver's position. A LATER reconcile must HOLD that mount, not tear it down:
// the not-current RELEASE path must skip a position that is in the PENDING set.
// With the bug it releases the mount (it is not a CURRENT owner) and re-acquires it,
// churning the backend at a climbing epoch and destabilizing the handoff the
// leaver's drainCheck is waiting on.
func TestOverlap_Reconcile_PendingMount_HeldAcrossReconciles(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 16, 2, backing, "self", "n2", "n3")
	c.draining = map[string]struct{}{"n2": {}} // n2 leaving; self gains pending positions

	current := c.desiredReplicaUnits()
	curSet := make(map[storageunit.ReplicaUnit]struct{}, len(current))
	for _, r := range current {
		curSet[r] = struct{}{}
	}
	h := backing.Handle()
	for _, target := range current {
		b, _, err := h.OpenReplicaUnit(target, 1)
		if err != nil {
			t.Fatalf("seed current mount %v: %v", target, err)
		}
		c.mounts.mountUndecorated(target, b)
	}

	pending := c.desiredPendingReplicaUnits(c.draining)
	var pendingOnly []storageunit.ReplicaUnit
	for _, r := range pending {
		if _, ok := curSet[r]; !ok {
			pendingOnly = append(pendingOnly, r)
		}
	}
	if len(pendingOnly) == 0 {
		t.Fatal("expected at least one pending-only position")
	}

	// Acquire the pending positions (mount flip + serving marker).
	c.reconcileReplicaUnitsOverlap()
	c.loopWG.Wait()
	before := make(map[storageunit.ReplicaUnit]backend.Backend, len(pendingOnly))
	for _, target := range pendingOnly {
		b, mounted := c.localBackendForReplicaUnit(target)
		if !mounted {
			t.Fatalf("setup: %v should be mounted after the acquire flip", target)
		}
		before[target] = b
	}

	// A SECOND reconcile (n2 still draining) must HOLD the pending mounts: same
	// backend instance, marker intact - NOT release + re-acquire.
	c.reconcileReplicaUnitsOverlap()
	c.loopWG.Wait()
	for _, target := range pendingOnly {
		after, mounted := c.localBackendForReplicaUnit(target)
		if !mounted {
			t.Fatalf("PENDING MOUNT RELEASED: %v was torn down by a later reconcile while still a pending owner.", target)
		}
		if after != before[target] {
			t.Fatalf("PENDING MOUNT CHURNED: %v was released + re-acquired (backend identity changed) by a later reconcile. A successor's pending mount must be HELD for the whole handoff, not torn down + remounted.", target)
		}
		if _, ok := backing.ServingMarker(target); !ok {
			t.Fatalf("%v serving marker must persist while the pending mount is held", target)
		}
	}
}

// TestOverlap_Reconcile_SelfDraining_ReleasedPositionNotReacquired is the LEAVER
// COMPLETION guard: once drainCheck RELEASES a drained position (on the successor's
// marker), the position is unmounted with its phase cleared to 0 - but it is STILL
// in CURRENT (the leaver is still a ring member). The ACQUIRE-new half must NOT
// re-acquire it: re-grabbing a handed-off position re-fences the successor at a
// climbed epoch and ownedPositionCount never reaches 0, so the graceful leave never
// completes. The IsLoser skip in the ACQUIRE-new half does NOT cover this (the
// phase is 0 after release, not a loser phase); the pending-membership gate does.
func TestOverlap_Reconcile_SelfDraining_ReleasedPositionNotReacquired(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3", "n4")
	c.draining = map[string]struct{}{"self": {}}

	current := c.desiredReplicaUnits()
	if len(current) == 0 {
		t.Fatal("self owns no positions; ring fixture broken")
	}
	h := backing.Handle()
	for _, target := range current {
		b, ep, err := h.OpenReplicaUnit(target, 1)
		if err != nil {
			t.Fatalf("seed mount %v: %v", target, err)
		}
		c.mounts.mountUndecorated(target, b)
		c.mounts.recordOpenEpoch(target, ep)
	}

	c.reconcileReplicaUnitsOverlap()
	for _, target := range current {
		if c.handoffPhaseOf(target).Phase != storageunit.PhaseDraining {
			t.Fatalf("%v should be Draining", target)
		}
	}

	// Successors open + serving-mark above the leaver's gate; drainCheck releases.
	hs := backing.Handle()
	for _, target := range current {
		_, se, err := hs.OpenReplicaUnit(target, 1)
		if err != nil {
			t.Fatalf("successor open %v: %v", target, err)
		}
		if err := hs.WriteServingMarker(storageunit.ReplicaMount(target), se); err != nil {
			t.Fatalf("successor marker %v: %v", target, err)
		}
		c.drainCheck(target)
		if _, mounted := c.localBackendForReplicaUnit(target); mounted {
			t.Fatalf("%v should have RELEASED on the successor marker", target)
		}
	}

	// CRITICAL: a subsequent reconcile must NOT re-acquire the released positions.
	c.reconcileReplicaUnitsOverlap()
	c.loopWG.Wait()
	for _, target := range current {
		if _, mounted := c.localBackendForReplicaUnit(target); mounted {
			t.Fatalf("RE-GRABBED A HANDED-OFF POSITION: %v was re-acquired by the reconcile after drainCheck released it. A self-draining leaver must NOT re-acquire its current-but-not-pending positions, or ownedPositionCount never reaches 0 and the leave never completes.",
				target)
		}
		if st := c.handoffPhaseOf(target); st.Phase != 0 {
			t.Fatalf("%v should stay released (no phase), got %v", target, st.Phase)
		}
	}
}

// TestOverlap_evictStaleMount_DoesNotEvictDrainingPosition guards the write-path
// half: a union write that lands on the leaver's now-fenced handle (the successor
// opened the shared db at a higher epoch) returns a fence error and calls
// evictStaleMount. It must NOT drop a Draining mount - that is the expected
// successor-fence signal, not a stale-handle desync. Evicting it drops the leaver's
// mount before the successor's marker and triggers the re-acquire ping-pong.
func TestOverlap_evictStaleMount_DoesNotEvictDrainingPosition(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3", "n4")
	c.draining = map[string]struct{}{"self": {}}

	current := c.desiredReplicaUnits()
	if len(current) == 0 {
		t.Fatal("self owns no positions; ring fixture broken")
	}
	target := current[0]
	h := backing.Handle()
	b, ep, err := h.OpenReplicaUnit(target, 1)
	if err != nil {
		t.Fatalf("mount: %v", err)
	}
	c.mounts.mountUndecorated(target, b)
	c.mounts.recordOpenEpoch(target, ep)

	c.reconcileReplicaUnitsOverlap()
	if c.handoffPhaseOf(target).Phase != storageunit.PhaseDraining {
		t.Fatalf("%v should be Draining", target)
	}

	// A fenced union write calls evictStaleMount with the failed (fenced) backend.
	c.evictStaleMount(target, b)

	got, mounted := c.localBackendForReplicaUnit(target)
	if !mounted || got != b {
		t.Fatalf("evictStaleMount must NOT drop a Draining mount (a fenced write is the expected handoff signal): mounted=%v same-backend=%v", mounted, got == b)
	}
	if c.handoffPhaseOf(target).Phase != storageunit.PhaseDraining {
		t.Fatalf("%v must stay Draining after a fenced write, got %v", target, c.handoffPhaseOf(target).Phase)
	}
}
