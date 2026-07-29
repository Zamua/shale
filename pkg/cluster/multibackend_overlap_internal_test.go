package cluster

// White-box tests for the v0.8 Phase 2e (pending ranges) handoff controller.
// They drive the unexported controller helpers (desiredPendingReplicaUnits,
// pendingUnitReplicas, reconcileReplicaUnitsOverlap, acquireReplicaUnitOverlap,
// drainCheck) against a per-replica shared-backing factory + a real ring, with no
// membership / gRPC, so they run fast and deterministically (no memberlist). The
// pure HandoffState FSM is covered in pkg/storageunit/handoff_test.go; the wired
// cross-node union dual-write + the slow-mount loss oracle are covered by the
// integration acceptance gate.
//
// The leave (a draining member) is modeled by removing the leaver from the ring
// directly: a draining node STAYS in the ring under the pending-ranges model, so
// to exercise the PENDING owner's acquire trigger in a membership-free white-box
// test we instead drive the per-unit current/pending split helpers and the
// reconcile against a ring that mounts a pending position on self.

import (
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/storageunit"
)

func gu(g uint64, u uint32) storageunit.GenUnit {
	return storageunit.NewGenUnit(storageunit.Generation(g), storageunit.UnitID(u))
}

func ru(g uint64, u uint32, r uint8) storageunit.ReplicaUnit {
	return storageunit.NewReplicaUnit(gu(g, u), r)
}

// -- pendingUnitReplicas (the draining-excluded split) -------------------

// TestOverlap_pendingUnitReplicas_DropsDrainingMember pins that excluding a
// draining member from the replica-set resolution shifts the position to the next
// clockwise survivor, while a non-draining set is unchanged.
func TestOverlap_pendingUnitReplicas_DropsDrainingMember(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 8, 2, backing, "self", "n2", "n3", "n4")

	target := gu(0, 0)
	current := c.unitReplicas(target)
	if len(current) != 2 {
		t.Fatalf("R=2 current set must have 2 members, got %d", len(current))
	}
	// No draining members: pending == current.
	if got := c.pendingUnitReplicas(target, nil); !sameMemberSet(got, current) {
		t.Fatalf("with no draining members pending must equal current")
	}
	// Drain the primary (index 0): pending must drop it and pull in a survivor.
	draining := map[storageunit.NodeID]struct{}{current[0].ID: {}}
	pending := c.pendingUnitReplicas(target, draining)
	if len(pending) != 2 {
		t.Fatalf("pending must still have R=2 survivors, got %d", len(pending))
	}
	for _, m := range pending {
		if m.ID == current[0].ID {
			t.Fatalf("draining member %q must not appear in the pending set", current[0].ID)
		}
	}
}

// TestOverlap_pendingUnitReplicas_ExactPostLeavePlacement pins the EXACTNESS
// contract (docs/SPEC.md "PENDING replica set"): for EVERY unit, the pending
// set under a draining member equals the placement over a ring GENUINELY
// rebuilt without that member. The removed successor-chain drop-trick
// approximation violated this under bounded-load hashing (a FULL-MOVE unit's
// approximated pending was disjoint from the true post-leave placement),
// which mis-aimed the whole ordered-removal protocol for that unit and opened
// the post-exit read hole. The member names here are the fixture the
// integration repro uses; sg-a is the leaver and unit 5 is the historical
// full-move divergence.
func TestOverlap_pendingUnitReplicas_ExactPostLeavePlacement(t *testing.T) {
	backing := sharedfactory.NewBacking()
	members := []string{"sg-a", "sg-b", "sg-c", "sg-d", "sg-e"}
	c := newReplicatedCluster(t, "sg-b", 16, 2, backing, members...)

	survivorIDs := make([]string, 0, len(members))
	for _, id := range members {
		if id == "sg-a" {
			continue
		}
		survivorIDs = append(survivorIDs, id)
	}
	survivors := staticCoord("sg-b", nodesFor(survivorIDs...))
	draining := map[storageunit.NodeID]struct{}{"sg-a": {}}
	mismatches := 0
	for _, u := range storageunit.MustUnitCount(16).IDs() {
		gu := storageunit.NewGenUnit(0, u)
		got := c.pendingUnitReplicas(gu, draining)
		want := survivors.Locate(gu, 2, coord.Placement{})
		if len(got) != len(want) {
			t.Errorf("unit %d: pending size %d, want %d", u, len(got), len(want))
			mismatches++
			continue
		}
		for i := range want {
			if got[i].ID != want[i].ID {
				t.Errorf("unit %d index %d: pending=%s, post-leave placement=%s", u, i, got[i].ID, want[i].ID)
				mismatches++
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("pending diverges from the genuinely-rebuilt post-leave placement on %d positions - "+
			"the ordered-removal protocol drains onto the wrong successors for those units", mismatches)
	}
}

// TestOverlap_MarkerHold_Conditions pins rule 6's firing conditions
// (docs/SPEC.md "Transitional mounts are HELD", rule 6), including the two
// carve-outs whose absence broke split convergence in v0.10.0:
//
//   - RESHARD IN FLIGHT: rule 6 disarms entirely (generation machinery owns
//     retirement; every future acquire happens at the NEW generation, so no
//     old-gen marker is coming - holding wedged the holder's own flip).
//   - MARKER ABSENT: releases clean-cut (boot mounts never mark, so a
//     never-acquired position has no marker and no future marker-writer).
//   - Marker at-or-below our epoch: HELD (the flap-interleaving defense).
//   - Marker strictly above: released (the successor provably serves).
func TestOverlap_MarkerHold_Conditions(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2")

	desired := c.desiredReplicaUnits()
	if len(desired) == 0 {
		t.Fatalf("fixture: self owns nothing")
	}
	ru := desired[0]
	h := backing.Handle()
	b, opened, err := h.OpenReplicaUnit(ru, 1)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	c.mounts.mountUndecorated(ru, b)
	c.mounts.recordOpenEpoch(ru, opened)

	// MARKER ABSENT (a boot-mounted position): must NOT hold.
	if c.heldForMissingSuccessorMarker(ru) {
		t.Fatalf("marker-absent position must release clean-cut, not hold (no future marker-writer)")
	}

	// Marker at our own epoch: HELD (no successor has re-opened above us).
	if err := h.WriteServingMarker(storageunit.ReplicaMount(ru), opened); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if !c.heldForMissingSuccessorMarker(ru) {
		t.Fatalf("marker at own epoch must HOLD (successor not provably serving)")
	}

	// RESHARD IN FLIGHT: rule 6 disarms even though the marker condition
	// would hold - the split's own machinery owns retirement.
	c.genMu.Lock()
	c.genState.nextCount = storageunit.MustUnitCount(8)
	c.genMu.Unlock()
	if c.heldForMissingSuccessorMarker(ru) {
		t.Fatalf("rule 6 must disarm while a reshard is in flight (the v0.10.0 split-convergence wedge)")
	}
	c.genMu.Lock()
	c.genState.nextCount = storageunit.UnitCount{}
	c.genMu.Unlock()

	// Marker strictly above our epoch: released (successor provably serving).
	if err := h.WriteServingMarker(storageunit.ReplicaMount(ru), opened+1); err != nil {
		t.Fatalf("mark above: %v", err)
	}
	if c.heldForMissingSuccessorMarker(ru) {
		t.Fatalf("marker strictly above own epoch must release")
	}
}

// -- desiredPendingReplicaUnits (the pending-owner enumeration) -----------

// TestOverlap_desiredPendingReplicaUnits_NoDrainingEqualsCurrent pins that with
// no draining members the pending desired set is identical to the current desired
// set (steady state: nothing to acquire, nothing to drain).
func TestOverlap_desiredPendingReplicaUnits_NoDrainingEqualsCurrent(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 8, 2, backing, "self", "n2", "n3")

	current := c.desiredReplicaUnits()
	pending := c.desiredPendingReplicaUnits(nil)
	if len(current) != len(pending) {
		t.Fatalf("steady-state pending desired set size = %d, want current %d", len(pending), len(current))
	}
	cur := make(map[storageunit.ReplicaUnit]struct{}, len(current))
	for _, ru := range current {
		cur[ru] = struct{}{}
	}
	for _, ru := range pending {
		if _, ok := cur[ru]; !ok {
			t.Fatalf("pending position %v not in current set (should be equal in steady state)", ru)
		}
	}
}

// TestOverlap_desiredPendingReplicaUnits_GainsLeaversPositions pins the
// pending-owner acquire trigger's INPUT: when a peer drains, self's pending
// desired set GROWS to include positions the leaver vacates (positions self does
// NOT own in the current view). Those extra positions are exactly what the
// reconcile's pending-acquire half mounts.
func TestOverlap_desiredPendingReplicaUnits_GainsLeaversPositions(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 16, 2, backing, "self", "n2", "n3")

	current := c.desiredReplicaUnits()
	curSet := make(map[storageunit.ReplicaUnit]struct{}, len(current))
	for _, ru := range current {
		curSet[ru] = struct{}{}
	}

	// Drain n2: self should pick up some of n2's positions in the pending view.
	pending := c.desiredPendingReplicaUnits(map[storageunit.NodeID]struct{}{"n2": {}})
	extra := 0
	for _, ru := range pending {
		if _, ok := curSet[ru]; !ok {
			extra++
		}
	}
	if extra == 0 {
		t.Fatalf("draining a peer must give self at least one new pending position to acquire; got none")
	}
}

// -- reconcileReplicaUnitsOverlap: DRAIN (a draining-split move away) -----

// TestOverlap_Reconcile_DrainSplit_SetsDrainingKeepsMount: a position self holds
// in CURRENT but no longer in PENDING (because self is draining, so the pending
// split excludes it) is set Draining and KEPT MOUNTED (keep serving via the
// union), not released.
func TestOverlap_Reconcile_DrainSplit_SetsDrainingKeepsMount(t *testing.T) {
	backing := sharedfactory.NewBacking()
	// self is in the ring (a current owner) AND draining, so its positions are
	// current-but-not-pending.
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3", "n4")
	c.draining = map[storageunit.NodeID]struct{}{"self": {}}

	// Mount every position self currently owns, as if it had been serving them.
	current := c.desiredReplicaUnits()
	if len(current) == 0 {
		t.Fatalf("self owns no positions; ring fixture broken")
	}
	h := backing.Handle()
	for _, target := range current {
		b, _, err := h.OpenReplicaUnit(target, 1)
		if err != nil {
			t.Fatalf("seed mount %v: %v", target, err)
		}
		c.mounts.mountUndecorated(target, b)
	}

	c.reconcileReplicaUnitsOverlap()

	for _, target := range current {
		st := c.handoffPhaseOf(target)
		if st.Phase != storageunit.PhaseDraining {
			t.Fatalf("draining-split position %v phase = %v, want Draining", target, st.Phase)
		}
		if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
			t.Fatalf("Draining position %v must stay mounted (keep serving)", target)
		}
	}
}

// TestOverlap_Reconcile_PlainDropOut_Releases: a mounted position NOT in CURRENT
// and NOT in PENDING (it vanished from this node's set entirely, with no draining
// split keeping it and no same-unit position still desired here) is plain
// clean-cut released, NOT drained.
func TestOverlap_Reconcile_PlainDropOut_Releases(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3", "n4")

	// Pick a unit self does NOT replicate at all, so a mount of it (at an
	// out-of-range index no set ever holds) is genuinely abandoned: neither in
	// current nor pending, and NOT protected by the intra-node position-move
	// hold (self desires no other position of the unit). A unit self DOES own
	// takes the hold path instead; that behavior is pinned separately below.
	ownedUnits := make(map[storageunit.GenUnit]struct{})
	for _, d := range c.desiredReplicaUnits() {
		ownedUnits[d.Unit] = struct{}{}
	}
	var unowned uint32
	found := false
	for u := uint32(0); u < 4; u++ {
		if _, own := ownedUnits[gu(0, u)]; !own {
			unowned, found = u, true
			break
		}
	}
	if !found {
		t.Fatalf("self replicates every unit under this fixture; cannot model a plain drop-out")
	}
	target := ru(0, unowned, 5)
	h := backing.Handle()
	b, _, err := h.OpenReplicaUnit(target, 1)
	if err != nil {
		t.Fatalf("seed mount: %v", err)
	}
	c.mounts.mountUndecorated(target, b)

	c.reconcileReplicaUnitsOverlap()

	if st := c.handoffPhaseOf(target); st.Phase != 0 {
		t.Fatalf("plain drop-out must NOT enter a handoff phase, got %v", st.Phase)
	}
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("plain drop-out position must be released (unmounted)")
	}
}

// TestOverlap_Reconcile_PositionMoveHold_HeldUntilSameUnitMounts pins the
// INTRA-NODE POSITION-MOVE HOLD (docs/SPEC.md "Transitional mounts are HELD",
// rule 5): a mounted position in neither current nor pending is NOT released
// while self still desires the SAME unit at a different, not-yet-mounted
// position (the post-leave reshuffle window: the old-index copy is the node's
// only readable copy of the unit until the new index mounts). Once the desired
// position mounts, the next pass releases the held copy via the plain
// clean-cut branch.
func TestOverlap_Reconcile_PositionMoveHold_HeldUntilSameUnitMounts(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3", "n4")

	// Pick a unit self DOES replicate; leave its desired position UNMOUNTED and
	// mount a stale other-index copy of the same unit (the shuffled old index).
	desired := c.desiredReplicaUnits()
	if len(desired) == 0 {
		t.Fatalf("self owns no positions; ring fixture broken")
	}
	want := desired[0]
	held := ru(0, uint32(want.Unit.ID), 5) // same unit, an index no set holds
	h := backing.Handle()
	b, _, err := h.OpenReplicaUnit(held, 1)
	if err != nil {
		t.Fatalf("seed mount: %v", err)
	}
	c.mounts.mountUndecorated(held, b)

	// PASS 1: the release half must HOLD the stale copy (same-unit desire not
	// yet mounted when the release half runs), while the acquire half arms the
	// desired position's background mount; wait for the flip to land before
	// asserting (the fresh-mount acquire is backgrounded, not inline).
	c.reconcileReplicaUnitsOverlap()
	c.loopWG.Wait()
	if _, mounted := c.localBackendForReplicaUnit(held); !mounted {
		t.Fatalf("position-move hold: stale same-unit copy must stay mounted while the desired position is unmounted")
	}
	if st := c.handoffPhaseOf(held); st.Phase != 0 {
		t.Fatalf("held copy must NOT enter a handoff phase, got %v", st.Phase)
	}
	if _, mounted := c.localBackendForReplicaUnit(want); !mounted {
		t.Fatalf("desired position %v must have been acquired by the pass's background mount", want)
	}

	// PASS 2: the desired position is mounted, the hold lapses, the stale copy
	// takes the plain clean-cut release.
	c.reconcileReplicaUnitsOverlap()
	if _, mounted := c.localBackendForReplicaUnit(held); mounted {
		t.Fatalf("held copy must be released once the same-unit desired position is mounted")
	}
}

// -- reconcileReplicaUnitsOverlap: ACQUIRE (a pending position moving in) -

// TestOverlap_Reconcile_PendingOwner_AcquiresAndMarks: a position self holds in
// PENDING but not in CURRENT (a peer is draining, so the split makes self the
// future owner) enters Acquiring, then completes the mount flip to Owned
// (sharedfactory mounts instantly) and writes the serving marker that gates the
// leaver's release.
func TestOverlap_Reconcile_PendingOwner_AcquiresAndMarks(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 16, 2, backing, "self", "n2", "n3")

	// Drain n2: self gains some of n2's positions as pending-only.
	c.draining = map[storageunit.NodeID]struct{}{"n2": {}}

	current := c.desiredReplicaUnits()
	curSet := make(map[storageunit.ReplicaUnit]struct{}, len(current))
	for _, ru := range current {
		curSet[ru] = struct{}{}
	}
	// Mount the positions self currently owns (it is a current owner of them).
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
	for _, ru := range pending {
		if _, ok := curSet[ru]; !ok {
			pendingOnly = append(pendingOnly, ru)
		}
	}
	if len(pendingOnly) == 0 {
		t.Fatalf("expected at least one pending-only position to acquire")
	}

	c.reconcileReplicaUnitsOverlap()
	// The overlap acquire opens in a background goroutine; wait for the flip.
	c.loopWG.Wait()

	for _, target := range pendingOnly {
		if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
			t.Fatalf("pending-acquired position %v must be mounted after the mount flip", target)
		}
		if st := c.handoffPhaseOf(target); st.Phase != 0 {
			t.Fatalf("after the mount flip pending position %v is Owned (no phase), got %v", target, st.Phase)
		}
		if _, ok := backing.ServingMarker(target); !ok {
			t.Fatalf("pending-acquire mount flip must write the serving marker for %v", target)
		}
	}
}

// TestOverlap_Reconcile_PureNewMount_BackgroundAcquiresAndMarks: a desired
// CURRENT position not mounted with no transition (initial convergence /
// boot-defer warm-up) is acquired through the BACKGROUND bounded machinery -
// the reconcile pass arms it (Acquiring) and returns; the flip mounts it,
// resolves the phase to Owned, and writes the serving marker. The inline
// serial acquire this replaced was the boot-gap residual (N deferred
// positions warmed at N x open latency under reconcileMu).
func TestOverlap_Reconcile_PureNewMount_BackgroundAcquiresAndMarks(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	desired := c.desiredReplicaUnits()
	target := desired[0]

	c.reconcileReplicaUnitsOverlap()
	c.loopWG.Wait()

	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("pure-new-mount position must be acquired (mounted)")
	}
	if st := c.handoffPhaseOf(target); st.Phase != 0 {
		t.Fatalf("after the mount flip a pure new mount is Owned (no phase), got %v", st.Phase)
	}
	// The fresh-mount acquire must ALSO write the serving marker so a draining
	// leaver of this position (whose slot landed here via the fresh-mount path)
	// can release.
	if _, ok := backing.ServingMarker(target); !ok {
		t.Fatalf("fresh-mount acquire must write the serving marker so a draining leaver releases")
	}
}

// TestOverlap_CleanCutAcquire_WritesServingMarker pins directly that
// acquireReplicaUnit (the clean-cut path) writes the serving marker after
// mounting, at the epoch it opened. Without it a leaving node draining this exact
// position never observes a marker and waits out the full grace timeout.
func TestOverlap_CleanCutAcquire_WritesServingMarker(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	target := ru(0, 0, 0)
	c.acquireReplicaUnit(target)

	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("clean-cut acquire must mount the position")
	}
	markerEpoch, ok := backing.ServingMarker(target)
	if !ok {
		t.Fatalf("clean-cut acquire must write the serving marker")
	}
	durable, _ := backing.Handle().DurableEpochReplica(target)
	if markerEpoch != durable {
		t.Fatalf("serving marker epoch = %d, want the opened/durable epoch %d", markerEpoch, durable)
	}
}

// -- drainCheck -----------------------------------------------------------

// TestOverlap_drainCheck_ReleasesOnServingMarker: a Draining position releases
// exactly when the durable serving marker is present at epoch > its open epoch.
func TestOverlap_drainCheck_ReleasesOnServingMarker(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	target := ru(0, 0, 0)
	h := backing.Handle()
	b, _, err := h.OpenReplicaUnit(target, 1) // open epoch 1 on self
	if err != nil {
		t.Fatalf("seed mount: %v", err)
	}
	c.mounts.mountUndecorated(target, b)
	c.mounts.setPhase(target, storageunit.HandoffState{Phase: storageunit.PhaseDraining, OpenEpoch: 1})

	// No marker yet: drainCheck must NOT release (keeps serving).
	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("with no serving marker the Draining position must stay mounted")
	}
	if st := c.handoffPhaseOf(target); st.Phase != storageunit.PhaseDraining {
		t.Fatalf("with no marker the phase must stay Draining, got %v", st.Phase)
	}

	// A serving marker at a HIGHER epoch (the new owner became Ready): release.
	if err := h.WriteServingMarker(storageunit.ReplicaMount(target), 2); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("with a serving marker above the open epoch the Draining position must release")
	}
	if st := c.handoffPhaseOf(target); st.Phase != 0 {
		t.Fatalf("after release the phase entry must be dropped (Absent), got %v", st.Phase)
	}
}

// TestOverlap_drainCheck_StaleSelfMarkerDoesNotRelease pins the STRICT-gate fix
// (review P1): the serving marker is keyed by ReplicaUnit (node-independent) and
// monotonic, so a node that GAINED ru at open epoch E wrote
// WriteServingMarker(ru, E). If the ring later moves ru OFF that node, beginDrain
// sets OpenEpoch = DurableEpochReplica(ru) = E (unchanged until the NEW gainer
// opens). Under a >= gate the node would read its OWN stale marker E and release
// while the real successor is still mid-mount. The strict > gate rejects the
// stale self-marker (exactly E) and releases ONLY when a genuine successor writes
// a marker STRICTLY above E. Deterministic, no gossip timing.
func TestOverlap_drainCheck_StaleSelfMarkerDoesNotRelease(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	const E = storageunit.Epoch(3)
	target := ru(0, 0, 0)
	h := backing.Handle()

	// 1) self GAINED ru at open epoch E and wrote its own serving marker at E.
	b, _, err := h.OpenReplicaUnit(target, E)
	if err != nil {
		t.Fatalf("seed gain mount: %v", err)
	}
	if err := h.WriteServingMarker(storageunit.ReplicaMount(target), E); err != nil {
		t.Fatalf("write self gain-marker: %v", err)
	}

	// 2) the ring now moves ru OFF self: beginDrain sets OpenEpoch = E.
	c.mounts.mountUndecorated(target, b)
	c.mounts.setPhase(target, storageunit.HandoffState{Phase: storageunit.PhaseDraining, OpenEpoch: E})

	if got, _ := h.DurableEpochReplica(target); got != E {
		t.Fatalf("durable fence epoch should still be E=%d (no successor opened), got %d", E, got)
	}
	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("stale self-marker at exactly the open epoch must NOT release (premature release bug)")
	}
	if st := c.handoffPhaseOf(target); st.Phase != storageunit.PhaseDraining {
		t.Fatalf("with only the stale self-marker the phase must stay Draining, got %v", st.Phase)
	}

	// 3) a genuine SUCCESSOR opens at durable+1 and writes a marker STRICTLY above
	//    E. Now drainCheck must release.
	h2 := backing.Handle()
	if _, _, err := h2.OpenReplicaUnit(target, E+1); err != nil {
		t.Fatalf("successor open: %v", err)
	}
	if err := h2.WriteServingMarker(storageunit.ReplicaMount(target), E+1); err != nil {
		t.Fatalf("write successor marker: %v", err)
	}
	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("a successor marker strictly above the open epoch must release the old owner")
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
	b, _, err := h.OpenReplicaUnit(target, 1)
	if err != nil {
		t.Fatalf("seed mount: %v", err)
	}
	c.mounts.mountUndecorated(target, b)
	c.mounts.setPhase(target, storageunit.HandoffState{Phase: storageunit.PhaseDraining, OpenEpoch: 1})

	// Simulate a new owner FENCING (advancing the durable epoch) WITHOUT ever
	// writing the serving marker (it crashed mid-mount).
	h2 := backing.Handle()
	if _, _, err := h2.OpenReplicaUnit(target, 5); err != nil {
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
