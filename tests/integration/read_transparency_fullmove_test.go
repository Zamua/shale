package integration

// THE FULL-MOVE READ HOLE (deterministic repro; the residual behind the surge
// gate's intermittent failure).
//
// WHAT THIS TEST MEASURES, AND WHAT IT DOES NOT.
// It measures ONE thing: client-visible read failures (Get AND ScanPrefix)
// through a leave+join overlap on a unit whose replica set turns over
// COMPLETELY. It does NOT measure WHY those reads failed. Several distinct
// mechanisms produce the same client-visible symptom, so a failure here is a
// signal to go diagnose, NOT a diagnosis. Do not read a cause out of this
// test's verdict string; earlier revisions of it hardcoded one, and that
// misattribution sent a downstream consumer's investigation after a mechanism
// that had already been fixed.
//
// THE HISTORICAL MECHANISM (fixed; NOT what this test exercises today).
// The graceful-leave drain acquires successors per the PENDING set. When that
// set was computed with the successor-chain drop-trick approximation ("locate
// R+|draining| over the full ring, drop the draining ids"), it could DIVERGE
// from the genuinely-rebuilt post-leave ring (bounded-load consistent hashing
// is not removal-invariant), so the drain mounted the WRONG successors and at
// the leaver's exit no routed member held the unit. That approximation is GONE:
// pendingUnitReplicas now rebuilds the reduced ring EXACTLY, so pending equals
// the post-transition placement by construction. dropTrickPending below is the
// fixture's OWN reimplementation of the retired approximation, kept purely as a
// scenario SELECTOR (it picks a maximally demanding unit); it is not a
// production value, and the "shape-detector" line it prints is fixture
// bookkeeping that appears on PASSING runs too.
//
// THE MECHANISM THAT REMAINS (measured, and the one to suspect first).
// The successor that a holder ROUTES to is not necessarily the successor that
// takes over from it. A successor's OpenReplicaUnit bumps the durable epoch at
// OPEN-START, so the predecessor's still-mounted, still-routed copy becomes
// FENCED (and its read leg recodes to the transient acquiring error, evicting
// the mount) the moment the successor BEGINS mounting - well before any
// release. Nothing feeds that fence back into ROUTING: the routed union comes
// only from the gossip membership view, so a node whose view has not yet
// converged routes exclusively to legs the fence already made unreadable and
// has nowhere else to go. It self-heals when the view converges. The bound is
// TRANSIENT READ/SCAN UNAVAILABILITY: handoff moves a mount rather than bytes,
// failing legs are skipped rather than counted as not-found, and the stable-R
// write-ack bar is untouched, so no acked write is ever lost.
//
// Two things make this test DETERMINISTIC where the surge gate flaked ~1-in-8:
//
//   - The node names are FIXED and the full-move precondition is ASSERTED at
//     test start from the same ring math the cluster routes with (loud fixture
//     failure if the ring library's placement ever changes).
//   - BOOT-ERA LEFTOVER MOUNTS are drained before the transition. The
//     staggered boot transiently rings {a}, {a,b}, {a,b,c}, ... and each shape
//     mounts positions the final ring does not assign; whether those leftovers
//     are still mounted when the leave starts is reconcile-cadence dice. On
//     the shared-backing test factory a leftover (even fenced) handle still
//     READS the shared per-position store, so a surviving leftover on a
//     post-leave owner silently MASKS the hole (real slatedb closes fenced
//     handles, so production has no such mask) - that mask was the observed
//     nondeterminism. Running reconcile passes until no node holds an
//     undesired mount removes the mask deterministically.

import (
	"strings"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

// dropTrickPending reimplements the HISTORICAL drop-trick pending
// approximation (locate rf+1 over the full ring, drop the leaver, keep the
// first rf) as a test-side shape detector. It is deliberately independent of
// the production pending computation: the fixture preconditions below assert
// the SHAPE that made the approximation diverge, so they keep guarding the
// fixture even after the production code stops using the approximation.
func dropTrickPending(memberIDs []string, u storageunit.UnitID, rf int, leaver string) []string {
	chain := replicaIDsForMembers(memberIDs, u, rf+1)
	out := make([]string, 0, rf)
	for _, id := range chain {
		if id == leaver {
			continue
		}
		out = append(out, id)
		if len(out) == rf {
			break
		}
	}
	return out
}

func intersects(a, b []string) bool {
	for _, x := range a {
		if contains(b, x) {
			return true
		}
	}
	return false
}

// plantServingMarkersAtDurable writes a serving marker for every position at
// that position's CURRENT durable writer-epoch - the state of a long-lived
// healthy cluster, where the latest opener of every position has served (and
// marked) it. Unlike the epoch-1 planting the mass-restart tests use, this
// also RELEASES boot-era Draining leftovers: a leftover's open epoch sits
// BELOW the durable (a later boot opener fenced it), so the durable-epoch
// marker is strictly above it and drainCheck releases; the current holder's
// open epoch EQUALS the durable, so it is never falsely released.
func plantServingMarkersAtDurable(t *testing.T, h *sharedfactory.Handle, unitCount, rf int) {
	t.Helper()
	for _, u := range storageunit.MustUnitCount(unitCount).IDs() {
		for p := 0; p < rf; p++ {
			ru := storageunit.NewReplicaUnit(storageunit.NewGenUnit(0, u), uint8(p))
			epoch, err := h.DurableEpochReplica(ru)
			if err != nil || epoch < 1 {
				epoch = 1
			}
			if err := h.WriteServingMarker(ru, epoch); err != nil {
				t.Fatalf("plant serving marker %s: %v", ru, err)
			}
		}
	}
}

// waitNoUndesiredMounts drives reconcile passes on every node until none holds
// a mounted position it desires neither currently nor pending (the boot-era
// leftovers of the staggered-join transient rings), so the transition under
// test starts from the same clean state a long-running cluster is in.
func waitNoUndesiredMounts(t *testing.T, nodes []*sharedNode, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		clean := true
		for _, n := range nodes {
			n.Cluster.TestingRunReconcile()
			if strings.Contains(n.Cluster.DebugState(), "desired=false pending=false mounted=true") {
				clean = false
			}
		}
		if clean {
			return
		}
		if time.Now().After(deadline) {
			for _, n := range nodes {
				t.Logf("state %s:\n%s", n.ID, n.Cluster.DebugState())
			}
			t.Fatalf("boot-era undesired mounts never drained; fixture cannot start from a clean steady state")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestLeaveJoinOverlap_FullMoveUnit_ReadTransparent(t *testing.T) {
	const uc, rf = 16, 2
	base := []string{"sg-a", "sg-b", "sg-c", "sg-d"} // sg-a leaves
	joiner := "sg-e"
	leaver := "sg-a"
	with5 := append(append([]string(nil), base...), joiner)
	finalIDs := []string{"sg-b", "sg-c", "sg-d", "sg-e"}

	// PRECONDITION (the full-move unit): a unit the leaver holds whose true
	// post-transition placement shares NO member with its pre-transition
	// holders NOR with the drop-trick approximated pending set. Under the
	// historical approximation this unit's drain-time successors are the wrong
	// nodes, so at the leaver's exit no routed member holds it.
	fullMove := -1
	for u := 0; u < uc; u++ {
		uid := storageunit.UnitID(u)
		old4 := replicaIDsForMembers(base, uid, rf)
		fin := replicaIDsForMembers(finalIDs, uid, rf)
		appr := dropTrickPending(with5, uid, rf, leaver)
		five := replicaIDsForMembers(with5, uid, rf)
		if !contains(old4, leaver) {
			continue
		}
		// The final owners must appear in NO pre-flip view - not the old
		// placement, not the approximated drain pending, not the full 5-ring
		// (the joiner-in-ring view) - so no interleaving of gossip/bit arrival
		// can hand a final owner a copy before the leaver exits. That is what
		// makes the post-exit zero-holder window STRUCTURAL, not a race.
		if intersects(fin, old4) || intersects(fin, appr) || intersects(fin, five) {
			continue
		}
		fullMove = u
		break
	}
	if fullMove < 0 {
		t.Fatalf("FIXTURE DRIFT: no drain-approximation full-move unit exists for this name set; " +
			"the ring placement changed - search a new name set (see the shape conditions above)")
	}
	// NB: the retired-approximation column is the FIXTURE's own shape detector
	// (see dropTrickPending), printed on passing runs too. It is NOT a value the
	// cluster computes or routes with - production pending is the exact rebuild.
	// Do not read it as evidence of what the SUT did.
	t.Logf("full-move unit %d: old=%v fixture-selector[retired-approximation]=%v final=%v", fullMove,
		replicaIDsForMembers(base, storageunit.UnitID(fullMove), rf),
		dropTrickPending(with5, storageunit.UnitID(fullMove), rf, leaver),
		replicaIDsForMembers(finalIDs, storageunit.UnitID(fullMove), rf))

	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence semantics, both toggles): zero-tolerance
	// single-attempt read probes through the leave+join overlap require
	// the draining leaver and the physical holders to keep serving reads
	// while successors slow-mount (fence-at-completion, reads pass
	// through); see the justification on
	// TestJoinReadTransparent_GetScanEveryNode. The permissive-read MASK
	// this file's header describes (a fenced leftover handle still
	// reading the shared store) is already removed deterministically by
	// waitNoUndesiredMounts below, so the opt-out does not reintroduce
	// the nondeterminism.
	backing.SetEagerFence(false)
	backing.SetStrictReadFencing(false)
	mutate := func(cfg *cluster.Config) {
		cfg.GracefulLeaveDrainTimeout = 25 * time.Second
		cfg.OpenConcurrency = 8
		// The joiner fixture does not set the snappy test debounce by default;
		// without it the joiner's reconcile idles on the 5s production settle
		// (re-armed by every gossip event of the in-flight transition) and the
		// timeline degenerates to the drain timeout.
		cfg.RebalanceSettleDelay = 300 * time.Millisecond
	}
	na := startReplicatedNodeCfg(t, "sg-a", "", uc, rf, backing, mutate)
	nb := startReplicatedNodeCfg(t, "sg-b", na.BindAddr, uc, rf, backing, mutate)
	nc := startReplicatedNodeCfg(t, "sg-c", na.BindAddr, uc, rf, backing, mutate)
	nd := startReplicatedNodeCfg(t, "sg-d", na.BindAddr, uc, rf, backing, mutate)
	all4 := []*cluster.Cluster{na.Cluster, nb.Cluster, nc.Cluster, nd.Cluster}
	if err := waitForMembersAll(all4, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)

	uk := oneKeyPerUnit(t, nb.Cluster, uc)
	plantServingMarkersAtDurable(t, nb.Handle, uc, rf)
	waitNoUndesiredMounts(t, []*sharedNode{na, nb, nc, nd}, 20*time.Second)

	// Slow survivor acquires so every post-transition mount is a real window.
	for _, n := range []*sharedNode{nb, nc, nd} {
		n.Handle.SetAcquireDelay(2 * time.Second)
	}

	// Join the 5th node FIRST (it boot-defers via the planted markers, sets
	// Joining, and slow-warms through the whole drain - the leave+join overlap),
	// then gracefully remove sg-a once every node sees 5 members. Joining before
	// the leave removes the last interleaving mask: with the joiner in the ring
	// for the entire drain, no pre-flip view (old placement, approximated
	// pending, full 5-ring) ever assigns the full-move unit to a final owner,
	// so nothing can hand one a copy early (asserted by the precondition above).
	ne := startReplicatedNodeSlowAcquireCfg(t, joiner, nb.BindAddr, uc, rf, backing, 2*time.Second, 2*time.Second, mutate)
	all5 := []*cluster.Cluster{na.Cluster, nb.Cluster, nc.Cluster, nd.Cluster, ne.Cluster}
	if err := waitForMembersAll(all5, 5, 30*time.Second); err != nil {
		t.Fatalf("5-node convergence: %v", err)
	}
	drainDone := make(chan struct{})
	go func() { defer close(drainDone); _ = na.Cluster.Close() }()

	// Probe every unit through every stable survivor, through the drain AND
	// for a fixed post-exit window (the full-move hole opens AT the exit).
	entries := []*sharedNode{nb, nc, nd}
	var fails []string
	drained := false
	postRounds := 0
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		fails = append(fails, probeReadsOnce(entries, uk, []byte("seed"))...)
		select {
		case <-drainDone:
			drained = true
		default:
		}
		if drained {
			postRounds++
			if postRounds >= 14 { // ~3.5s of post-exit probing at 250ms cadence
				break
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !drained {
		t.Fatalf("leaver never finished its graceful drain within the budget")
	}

	// Let the joiner finish warming, then require a clean steady state.
	for _, n := range []*sharedNode{nb, nc, nd, ne} {
		n.Handle.SetAcquireDelay(0)
	}
	if err := waitForMembersAll([]*cluster.Cluster{nb.Cluster, nc.Cluster, nd.Cluster, ne.Cluster}, 4, 30*time.Second); err != nil {
		t.Fatalf("post-leave convergence: %v", err)
	}
	settleDeadline := time.Now().Add(20 * time.Second)
	var post []string
	for time.Now().Before(settleDeadline) {
		post = probeReadsOnce([]*sharedNode{nb, nc, nd, ne}, uk, []byte("seed"))
		if len(post) == 0 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if len(post) > 0 {
		summarizeFails(t, "FULL-MOVE steady state (still failing after settle)", post, 12)
		t.Fatalf("reads still failing at steady state after the transition")
	}

	summarizeFails(t, "FULL-MOVE window", fails, 16)
	if len(fails) > 0 {
		// SYMPTOM ONLY. This test cannot tell these apart, so it names none of
		// them as the cause; go measure which one fired before acting:
		//   1. a routed holder was FENCED by a successor's open-start epoch bump
		//      while this node's membership view was still stale, so every routed
		//      leg was transient and the union could not widen (the mechanism that
		//      remains on current main; suspect this first);
		//   2. the drain mounted successors that the post-leave ring does not route
		//      to (the RETIRED drop-trick divergence; pendingUnitReplicas rebuilds
		//      the reduced ring exactly, so this should be impossible - if it is
		//      really back, that is a regression in the exact rebuild);
		//   3. the true owners' mounts simply had not finished inside the probe
		//      budget (slow open, not a routing gap at all).
		t.Fatalf("full-move unit %d is not read-transparent: %d client-visible read failures through the "+
			"leave+join overlap. This is the SYMPTOM, not a diagnosis - see the numbered candidate "+
			"mechanisms above and this file's header before attributing a cause", fullMove, len(fails))
	}
}
