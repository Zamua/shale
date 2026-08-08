package integration

// THE FULL-MOVE UNIT: read behaviour when a unit's ENTIRE replica set turns
// over in one membership transition.
//
// This file gates reads (Get AND ScanPrefix) through a leave+join overlap for
// the hardest unit shape available: one whose post-transition owners share no
// member with any pre-transition view. Two mechanisms have been associated with
// failures here, and they must not be confused with each other.
//
// HISTORICAL, AND FIXED. The graceful-leave drain used to acquire successors
// per a PENDING set computed with the successor-chain drop-trick approximation
// ("locate R+|draining| over the full ring, drop the draining ids"), which can
// DIVERGE from the genuinely-rebuilt post-leave ring (bounded-load consistent
// hashing is not removal-invariant). That divergence let the "wrong" successors
// mount + serving-mark a unit, so at the leaver's exit no routed member held it.
// That mechanism is GONE: pendingUnitReplicas now rebuilds the reduced ring
// exactly, so the pending set equals the post-transition placement by
// construction. Do not attribute a failure here to it without measuring first.
//
// THE MECHANISM THAT REMAINS (documented and accepted; see the tolerance block
// on the assertions at the bottom of this file). A successor fences the
// predecessor at OPEN-START, but nothing feeds a fence back into ROUTING. A
// node whose gossip view is momentarily stale - the joiner's Joining bit has
// arrived, the leaver's Draining bit has not - therefore routes the unit only
// to legs it can no longer read, while the true post-leave owners are not in its
// routed set at all. Every such leg is SKIPPED rather than counted as
// not-found, so the read fails loudly with the transient acquiring-window
// refusal rather than answering wrongly.
//
// IMPACT BOUND of the remaining mechanism: TRANSIENT read/scan unavailability,
// self-healing on membership convergence. It cannot lose an acked write (handoff
// moves a mount, not bytes; the stable-R write-ack bar is untouched) and it
// cannot serve a wrong or stale result. That bound is what this test's tolerance
// is scoped to, and everything outside it still fails.
//
// The unit selected below is the strictest repro (disjoint from EVERY pre-flip
// view). The tolerance, though, is scoped to the computed STRAND SET - every
// unit whose true post-leave owners go un-routed under the stale view, which is
// the precise population the mechanism can affect. See strandUnits.
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
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

// isAcquiringWindowErr reports whether err belongs to the TRANSIENT
// acquiring-window refusal class - the terminal a read produces when every leg
// it can route to is mid-handoff, and the only class the full-move tolerance
// below accepts.
//
// Three shapes, all of them the same refusal seen from different distances:
//
//  1. cluster.ErrAcquiring. The typed, path-independent signal, and the primary
//     match: it holds whether the position was mounted on the node the call
//     entered or the op was forwarded to a peer that was mid-acquire.
//  2. codes.Unavailable carrying the handing-off-to-this-node refusal text. The
//     same refusal arriving on a path where the typed ErrorInfo detail did not
//     survive. Matched on the message deliberately narrowly: a BARE
//     codes.Unavailable (a dial failure, "no replicas available for key") is NOT
//     this class and is NOT tolerated.
//  3. A deadline terminal. The all-legs-transient read re-polls within its
//     ReadTimeout budget, so a caller whose own deadline lands at or below that
//     budget observes the window as DeadlineExceeded instead of as the typed
//     refusal.
//
// Everything else - not-found, ResourceExhausted, decode failures, dial errors -
// falls outside and fails the test.
func isAcquiringWindowErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, cluster.ErrAcquiring) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if st, ok := status.FromError(err); ok && st != nil {
		switch st.Code() {
		case codes.Unavailable:
			return strings.Contains(st.Message(), "handing off to this node")
		case codes.DeadlineExceeded:
			return true
		}
	}
	return false
}

// memberIDsOf snapshots a node's OWN live membership view, ordered, so the
// fixture can rebuild the placement that node is actually routing with.
func memberIDsOf(n *sharedNode) []string {
	var ids []string
	for _, m := range n.Cluster.Members() {
		ids = append(ids, m.ID)
	}
	sort.Strings(ids)
	return ids
}

// convergedStrandUnits reports, for each unit in strands, whether EVERY probing
// node's own ring view already places that unit on exactly its true post-leave
// owners. Evaluated per unit because each unit converges on its own placement.
//
// This is the measured form of "membership has converged" for the specific
// window being tolerated. The documented hole is defined by a node routing the
// unit somewhere other than its true post-leave owners because its view is
// stale; once a node's view produces the true placement, that node's routed set
// covers the real holders and the documented mechanism can no longer explain a
// failure it observes. Tolerating only while this is false is what keeps the
// tolerance bounded by convergence rather than open-ended.
func convergedStrandUnits(nodes []*sharedNode, strands map[storageunit.UnitID]bool, finalIDs []string, rf int) map[storageunit.UnitID]bool {
	views := make([][]string, 0, len(nodes))
	for _, n := range nodes {
		views = append(views, memberIDsOf(n))
	}
	out := map[storageunit.UnitID]bool{}
	for u := range strands {
		want := replicaIDsForMembers(finalIDs, u, rf)
		sort.Strings(want)
		converged := true
		for _, view := range views {
			got := replicaIDsForMembers(view, u, rf)
			sort.Strings(got)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				converged = false
				break
			}
		}
		out[u] = converged
	}
	return out
}

// fullMoveObservation is one probe failure plus the MEASURED cluster state at
// the moment it was seen. The state travels with the failure so the test's
// messages can report what was observed (which unit, which class, converged or
// not, how far into the transition) instead of asserting a cause.
type fullMoveObservation struct {
	readFailure
	Converged  bool          // routing had converged on the true post-leave owners
	Drained    bool          // the leaver had finished its graceful drain
	SinceLeave time.Duration // elapsed since the leave was initiated
}

func (o fullMoveObservation) String() string {
	return fmt.Sprintf("%s [t+%s converged=%v drained=%v]",
		o.readFailure, o.SinceLeave.Round(100*time.Millisecond), o.Converged, o.Drained)
}

// strandUnits computes, from the same ring math the cluster routes with, the
// set of units that can exhibit the DOCUMENTED window - a full replica-set
// turnover that STRANDS a stale-view node's routed set.
//
// Two conditions, both structural:
//
//  1. the leaver holds the unit, so this transition moves it off a departing
//     node at all, and
//  2. the unit's TRUE post-leave owners are DISJOINT from the routed set a node
//     computes under the stale mid-transition view (joiner already in the ring,
//     leaver not yet marked draining). That disjointness IS the documented
//     mechanism: every leg such a node routes to has been fenced at the
//     successor's open-start, and no true owner is routed at all, so the read
//     has nowhere to go until the view converges.
//
// Both conditions are needed. Condition 2 alone is what the write-up's own
// generality sweep counted, and on this fixture's fixed name set the two
// conditions select exactly 2 of the 16 units. Every other unit's true owners
// remain routed throughout, so the mechanism cannot produce a failure there and
// one is treated as a regression. The fixture asserts that count below, so a
// ring-placement change cannot silently widen the tolerance.
func strandUnits(base, with5, finalIDs []string, leaver string, unitCount, rf int) map[storageunit.UnitID]bool {
	out := map[storageunit.UnitID]bool{}
	for u := 0; u < unitCount; u++ {
		uid := storageunit.UnitID(u)
		if !contains(replicaIDsForMembers(base, uid, rf), leaver) {
			continue
		}
		if intersects(replicaIDsForMembers(finalIDs, uid, rf), replicaIDsForMembers(with5, uid, rf)) {
			continue
		}
		out[uid] = true
	}
	return out
}

// classifyFullMoveFails splits a round's failures into the ones the recorded
// design decision tolerates and the ones that fall outside it.
//
// TOLERATED, and only this: an acquiring-window REFUSAL, on a unit whose replica
// set turns over completely AND whose true post-leave owners are un-routed under
// the stale view, observed while routing had not converged. All three conditions
// are required.
//
// NOT tolerated, ever:
//   - a wrong or absent VALUE (failWrongValue / failEmptyScan). The accepted
//     property is that failing legs are skipped rather than counted as
//     not-found, so an incorrect answer is a correctness regression, not the
//     availability gap.
//   - a failure on any unit outside the computed strand set. Those units keep a
//     true owner routed throughout, so the documented mechanism cannot explain a
//     failure on them.
//   - any error outside the acquiring-window class.
//   - anything at all once routing has converged; the accepted property is that
//     the window SELF-HEALS, so a failure that outlives convergence is a
//     regression.
func classifyFullMoveFails(round []readFailure, strands, converged map[storageunit.UnitID]bool, st fullMoveObservation) (tolerated, outsideWindow []fullMoveObservation) {
	for _, f := range round {
		o := st
		o.readFailure = f
		o.Converged = converged[f.Unit]
		switch {
		case f.Kind != failErr:
			outsideWindow = append(outsideWindow, o)
		case !strands[f.Unit]:
			outsideWindow = append(outsideWindow, o)
		case o.Converged:
			outsideWindow = append(outsideWindow, o)
		case !isAcquiringWindowErr(f.Err):
			outsideWindow = append(outsideWindow, o)
		default:
			tolerated = append(tolerated, o)
		}
	}
	return tolerated, outsideWindow
}

// sortedUnits renders a unit set in a stable order for test messages.
func sortedUnits(set map[storageunit.UnitID]bool) []int {
	out := make([]int, 0, len(set))
	for u := range set {
		out = append(out, int(u))
	}
	sort.Ints(out)
	return out
}

// describeFails renders up to limit observations for a test message, without
// asserting why any of them happened.
func describeFails(fails []fullMoveObservation, limit int) string {
	var b strings.Builder
	for i, f := range fails {
		if i >= limit {
			fmt.Fprintf(&b, "\n  ... and %d more", len(fails)-limit)
			break
		}
		fmt.Fprintf(&b, "\n  %s", f)
	}
	return b.String()
}

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
			if err := h.WriteServingMarker(storageunit.ReplicaMount(ru), epoch); err != nil {
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
			if strings.Contains(n.Cluster.DebugState(), "desired=false pendingOwner=false mounted=true") {
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
	// NB the middle column is FIXTURE BOOKKEEPING, not a value the cluster
	// computes: it is this file's own reimplementation of the RETIRED drop-trick
	// approximation, used only to select the hardest unit shape. It prints on
	// PASSING runs too. It was previously labelled "approx-pending", which read
	// as a live cluster quantity and led a reader to attribute a failure here to
	// a pending-set bug that had already been fixed. Do not read it as evidence.
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
	// for a fixed post-exit window.
	//
	// Each round records the MEASURED routing-convergence state at the time it
	// was probed, so a failure can be judged against the state the cluster was
	// actually in rather than against an assumption about where in the timeline
	// it fell.
	entries := []*sharedNode{nb, nc, nd}
	leaveStarted := time.Now()

	// The population the documented window can affect. Computed from the ring,
	// not assumed: the fixture's selected repro unit is the strictest member of
	// this set, but it is not the only unit whose true post-leave owners go
	// un-routed under the stale view, and a tolerance scoped to it alone would
	// mis-classify the others as regressions.
	strands := strandUnits(base, with5, finalIDs, leaver, uc, rf)
	if !strands[storageunit.UnitID(fullMove)] {
		t.Fatalf("FIXTURE DRIFT: the selected full-move unit %d is not in the computed strand set %v",
			fullMove, sortedUnits(strands))
	}
	// Drift guard. The tolerance is justified by this being a RARE structural
	// shape. If a ring change ever made most of the keyspace qualify, the
	// tolerance would quietly become "any read failure is fine", so cap it here
	// and fail loudly instead.
	if maxStrands := uc / 4; len(strands) > maxStrands {
		t.Fatalf("FIXTURE DRIFT: %d of %d units qualify as strand cases (cap %d); the documented "+
			"tolerance is scoped to a rare shape and must not be widened silently: %v",
			len(strands), uc, maxStrands, sortedUnits(strands))
	}
	t.Logf("strand set (units whose true post-leave owners are un-routed under the stale view): %v of %d units",
		sortedUnits(strands), uc)
	var tolerated, outsideWindow []fullMoveObservation
	drained := false
	postRounds := 0
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		// Convergence is sampled on BOTH sides of the round. A round can span
		// seconds (a read that burns its whole budget), so a single sample taken
		// before it could credit a post-convergence failure to the pre-
		// convergence window. Requiring not-converged at both ends means the
		// tolerance only ever applies to a read that ran entirely inside the
		// unconverged window; a round that straddles the boundary is judged
		// converged and gets no tolerance.
		before := convergedStrandUnits(entries, strands, finalIDs, rf)
		round := probeReadsOnceDetailed(entries, uk, []byte("seed"))
		after := convergedStrandUnits(entries, strands, finalIDs, rf)
		converged := map[storageunit.UnitID]bool{}
		for u := range strands {
			converged[u] = before[u] || after[u]
		}
		st := fullMoveObservation{Drained: drained, SinceLeave: time.Since(leaveStarted)}
		tol, bad := classifyFullMoveFails(round, strands, converged, st)
		tolerated = append(tolerated, tol...)
		outsideWindow = append(outsideWindow, bad...)
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
	// Window classification, logged before any gate fires so a failing run always
	// shows how the transition-window probes were classified even when a later
	// assertion is the one that trips.
	t.Logf("transition window classified: %d tolerated (accepted acquiring-window refusals on strand "+
		"units %v while unconverged), %d outside the window%s",
		len(tolerated), sortedUnits(strands), len(outsideWindow), describeFails(outsideWindow, 8))

	// Let the joiner finish warming, then require a clean steady state.
	for _, n := range []*sharedNode{nb, nc, nd, ne} {
		n.Handle.SetAcquireDelay(0)
	}
	if err := waitForMembersAll([]*cluster.Cluster{nb.Cluster, nc.Cluster, nd.Cluster, ne.Cluster}, 4, 30*time.Second); err != nil {
		t.Fatalf("post-leave convergence: %v", err)
	}
	// SELF-HEAL GATE (zero tolerance). Past convergence the accepted window is
	// over, so nothing is excused here: any error, any wrong value, any absent
	// key, on any unit, is a regression. This is also the acked-write readback -
	// every key probed was written and acked before the transition, so a key
	// that will not read back at steady state is acked-write loss, which the
	// accepted behaviour explicitly cannot produce.
	const settleBudget = 20 * time.Second
	settleDeadline := time.Now().Add(settleBudget)
	var post []fullMoveObservation
	for time.Now().Before(settleDeadline) {
		post = nil
		for _, f := range probeReadsOnceDetailed([]*sharedNode{nb, nc, nd, ne}, uk, []byte("seed")) {
			post = append(post, fullMoveObservation{
				readFailure: f, Converged: true, Drained: true, SinceLeave: time.Since(leaveStarted),
			})
		}
		if len(post) == 0 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if len(post) > 0 {
		t.Fatalf("OBSERVED: %d read failures still present after membership converged and the "+
			"settle budget expired (%s). The accepted full-move window SELF-HEALS on convergence, so a "+
			"failure that outlives it is outside the recorded design decision. Reported as observation, "+
			"not diagnosis: measure before attributing a cause.%s",
			len(post), settleBudget, describeFails(post, 12))
	}

	// TOLERATED WINDOW (recorded design decision, not a quieted flake).
	//
	// The full-move availability write-up measured this exact mechanism on this
	// exact scenario and reached the verdict REQUIRES_DESIGN_CHANGE with no fix
	// scheduled: a successor fences the predecessor at open-start, nothing feeds
	// that fence back into routing, and a node whose gossip view is briefly
	// stale is left routing only to legs it can no longer read. Every option
	// that actually closes it costs a durable format change, new synchronous I/O
	// on the read hot path, or an inversion of an existing safety invariant, so
	// the recommendation adopted was to ACCEPT and DOCUMENT the transient
	// window. Its bound is transient read/scan unavailability that self-heals on
	// membership convergence, never acked-write loss and never a wrong or stale
	// value.
	//
	// So this test tolerates that window and nothing else. The self-heal gate
	// above and the classifier below are what keep the tolerance from
	// degenerating into "any read failure is fine": a value fault, a failure on
	// a unit that is not fully turning over, an error outside the
	// acquiring-window class, or anything surviving convergence all still fail.
	if len(tolerated) > 0 {
		t.Logf("tolerated %d acquiring-window refusals on strand units %v while routing had not "+
			"converged; this is the accepted, self-healing window.%s",
			len(tolerated), sortedUnits(strands), describeFails(tolerated, 6))
	}
	if len(outsideWindow) > 0 {
		t.Fatalf("OBSERVED: %d read failures outside the accepted full-move window, through the "+
			"leave+join overlap. Each one is at least one of: a wrong or absent value; a failure on a "+
			"unit outside the strand set %v; an error outside the acquiring-window class; or a failure "+
			"observed after routing had already converged on the true post-leave owners. (%d further "+
			"failures WERE inside the accepted window and were tolerated.) Each line below reports what "+
			"was OBSERVED - unit, entry node, error, elapsed time, convergence state - and deliberately "+
			"asserts no cause: measure before attributing one.%s",
			len(outsideWindow), sortedUnits(strands), len(tolerated), describeFails(outsideWindow, 16))
	}
}
