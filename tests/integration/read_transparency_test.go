package integration

// READ transparency through membership transitions (the read-side mirror of
// TestJoinIsWriteTransparent / TestLeaveIsWriteTransparent).
//
// The pending-ranges model (docs/SPEC.md "v0.8 Phase 2e") promises that BOTH
// reads and writes stay available through a graceful membership transition:
// "reads fan out across the union and any mounted member serves". The write
// side is gated by the join/leave write-transparency tests; THESE tests gate
// the READ side, for BOTH point reads (Get) and single-shard scans
// (ScanPrefix), issued through EVERY live node as the entry point - the shape
// a load-balanced client (a k8s Service) actually produces.
//
// Three scenarios, mirroring a rolling surge deployment (join a new node,
// later gracefully remove an old one, then join the next):
//
//   - JOIN alone: a 4th node boot-defers (Joining bit) and slow-mounts its
//     positions while probes read every unit through every node.
//   - LEAVE alone: one node of four drains gracefully (Draining bit) while
//     survivors slow-mount its positions; probes run through the drain AND
//     through the post-exit window right after the leaver departs the ring
//     (where the consistent-hash reshuffle can shift surviving replicas to
//     NEW position indices they have not mounted yet).
//   - SURGE composition: join, then a leave of a different node overlapping
//     the settling, then the next join 2s later - the k8s rollout timeline.
//
// Every probe is a SINGLE attempt (no retry): the gate is transparency, not
// eventual success. A probe failure is a client-visible read error.

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

// probeReadsOnce issues one Get and one full ScanPrefix for every seeded unit
// key through every entry node, single-attempt, and returns the collected
// failures (empty = fully transparent this round). The scan uses the seeded
// key itself as the prefix, so a healthy scan yields exactly that key with its
// seeded value; an iterator error at open OR at Next counts as a failure (the
// remote scan stream surfaces server-side errors at Next).
func probeReadsOnce(entries []*sharedNode, uk map[storageunit.UnitID]string, want []byte) []string {
	var fails []string
	for _, n := range entries {
		for u, k := range uk {
			got, err := n.Cluster.Get([]byte(k))
			if err != nil {
				fails = append(fails, fmt.Sprintf("get %s (unit %d) via %s: %v", k, u, n.ID, err))
			} else if !bytes.Equal(got, want) {
				fails = append(fails, fmt.Sprintf("get %s (unit %d) via %s: wrong value %q", k, u, n.ID, got))
			}

			it, err := n.Cluster.ScanPrefix([]byte(k))
			if err != nil {
				fails = append(fails, fmt.Sprintf("scan %s (unit %d) via %s: %v", k, u, n.ID, err))
				continue
			}
			// NB: at R>1 the stored bytes are LWW envelopes and ScanPrefix
			// surfaces them raw, so the scan check is PRESENCE (the seeded key
			// comes back), not value equality - value fidelity is pinned by the
			// Get above.
			seen := 0
			for {
				kk, _, nerr := it.Next()
				if nerr != nil {
					fails = append(fails, fmt.Sprintf("scan next %s (unit %d) via %s: %v", k, u, n.ID, nerr))
					break
				}
				if kk == nil {
					if seen == 0 {
						fails = append(fails, fmt.Sprintf("scan %s (unit %d) via %s: empty result for a seeded key", k, u, n.ID))
					}
					break
				}
				seen++
			}
			_ = it.Close()
		}
	}
	return fails
}

// summarizeFails logs a get-vs-scan breakdown plus up to max failure lines.
func summarizeFails(t *testing.T, label string, fails []string, limit int) {
	t.Helper()
	if len(fails) == 0 {
		return
	}
	gets, scans := 0, 0
	for _, f := range fails {
		if strings.Contains(f, "get ") {
			gets++
		} else {
			scans++
		}
	}
	t.Logf("%s: %d read failures (%d get, %d scan); first %d:", label, len(fails), gets, scans, min(limit, len(fails)))
	for i, f := range fails {
		if i >= limit {
			break
		}
		t.Logf("  %s", f)
	}
}

// TestJoinReadTransparent_GetScanEveryNode: a JOIN alone must not produce
// client-visible read failures. A 4th node with serving markers planted
// boot-defers everything (Joining bit) and slow-mounts via the overlap
// reconcile; Get AND ScanPrefix for every unit through every node must keep
// answering from the union (the displaced owner / co-replica still physically
// hold every moving position).
func TestJoinReadTransparent_GetScanEveryNode(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence semantics, both toggles): this gate pins
	// the union READ mechanism in isolation with ZERO-tolerance
	// single-attempt probes - the displaced owner must keep serving reads
	// through rt-d's 8s modeled mount, which needs fence-at-completion
	// timing (eager fence would fence it at open-START) AND
	// reads-pass-through on the sub-second post-completion fence windows
	// (strict read fencing would turn those into client-visible blips this
	// gate counts as failures). Production close-on-fence read behavior is
	// covered by TestUnionReadRetry_ServesThroughFenceWindow (pkg/cluster)
	// and the real-slatedb TestMinIO_WriterEpochFencing pins; the eager
	// wedge residual by TestJoinResidual_FenceAtOpenStart_MovingShardsWedge.
	backing.SetEagerFence(false)
	backing.SetStrictReadFencing(false)

	n1 := startReplicatedNodeCfg(t, "rt-a", "", uc, rf, backing, nil)
	n2 := startReplicatedNodeCfg(t, "rt-b", n1.BindAddr, uc, rf, backing, nil)
	n3 := startReplicatedNodeCfg(t, "rt-c", n1.BindAddr, uc, rf, backing, nil)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)

	uk := oneKeyPerUnit(t, n1.Cluster, uc)
	plantAllServingMarkers(t, n1.Handle, uc, rf)

	// Require at least one unit whose PRIMARY moves onto the newcomer - the
	// single-owner-routed scan path's worst case.
	new4 := []string{"rt-a", "rt-b", "rt-c", "rt-d"}
	primaryMoves := 0
	for u := range uk {
		if replicaIDsForMembers(new4, u, rf)[0] == "rt-d" {
			primaryMoves++
		}
	}
	if primaryMoves == 0 {
		t.Fatalf("FIXTURE DRIFT: no unit's primary moves onto the 4th node under this hashing - the ring placement " +
			"changed; recompute node names so the single-owner-routed scan worst case stays covered")
	}
	t.Logf("%d of %d units get rt-d as PRIMARY on the 4-node ring", primaryMoves, uc)

	const mountDelay = 8 * time.Second
	n4 := startReplicatedNodeSlowAcquire(t, "rt-d", n1.BindAddr, uc, rf, backing, mountDelay, 2*time.Second)
	all := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}
	if err := waitForMembersAll(all, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}

	// Probe while rt-d is provably mid-mount.
	entries := []*sharedNode{n1, n2, n3, n4}
	var fails []string
	rounds := 0
	for r := 0; r < 4 && desiredButUnmounted(n4.Cluster) > 0; r++ {
		fails = append(fails, probeReadsOnce(entries, uk, []byte("seed"))...)
		rounds++
		time.Sleep(300 * time.Millisecond)
	}
	for _, n := range []*sharedNode{n1, n2, n3, n4} {
		n.Handle.SetAcquireDelay(0)
	}
	if rounds == 0 {
		t.Fatalf("rt-d mounted before any during-join probe ran; raise mountDelay")
	}
	t.Logf("probed %d rounds during rt-d's mount window", rounds)
	summarizeFails(t, "JOIN", fails, 12)
	if len(fails) > 0 {
		t.Fatalf("JOIN is not read-transparent: %d client-visible read failures during the newcomer's mount "+
			"(reads must be served by the union's still-mounted holders)", len(fails))
	}
}

// TestLeaveReadTransparent_GetScanEveryNode: a graceful LEAVE must not produce
// client-visible read failures - through the drain window AND through the
// post-exit window right after the leaver departs the ring, where the
// consistent-hash reshuffle can move surviving replicas to NEW position
// indices (whose independent databases they have not mounted yet) while the
// bytes still sit mounted at their OLD indices.
func TestLeaveReadTransparent_GetScanEveryNode(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence semantics, both toggles): zero-tolerance
	// single-attempt read probes through the drain + post-exit windows
	// require the draining leaver (and the old-index holders) to keep
	// serving reads while successors slow-mount; see the justification on
	// TestJoinReadTransparent_GetScanEveryNode above.
	backing.SetEagerFence(false)
	backing.SetStrictReadFencing(false)
	mutate := func(cfg *cluster.Config) {
		cfg.GracefulLeaveDrainTimeout = 20 * time.Second
		cfg.OpenConcurrency = 8
	}
	n1 := startReplicatedNodeCfg(t, "rl-a", "", uc, rf, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "rl-b", n1.BindAddr, uc, rf, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "rl-c", n1.BindAddr, uc, rf, backing, mutate)
	n4 := startReplicatedNodeCfg(t, "rl-d", n1.BindAddr, uc, rf, backing, mutate)
	all := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}
	if err := waitForMembersAll(all, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	uk := oneKeyPerUnit(t, n1.Cluster, uc)

	// Sanity: fully transparent before the transition.
	if pre := probeReadsOnce([]*sharedNode{n1, n2, n3, n4}, uk, []byte("seed")); len(pre) > 0 {
		summarizeFails(t, "PRE-LEAVE (steady state)", pre, 12)
		t.Fatalf("reads failing before the transition; fixture broken")
	}

	// Slow the survivors' acquires so both the drain window and the post-exit
	// reshuffle window are wide enough to probe deterministically.
	for _, n := range []*sharedNode{n1, n2, n3} {
		n.Handle.SetAcquireDelay(2500 * time.Millisecond)
	}

	drainDone := make(chan struct{})
	go func() { defer close(drainDone); _ = n4.Cluster.Close() }()
	time.Sleep(600 * time.Millisecond) // let the Draining bit gossip

	survivors := []*sharedNode{n1, n2, n3}
	var fails []string
	drainRounds := 0
drainLoop:
	for {
		select {
		case <-drainDone:
			break drainLoop
		default:
		}
		fails = append(fails, probeReadsOnce(survivors, uk, []byte("seed"))...)
		drainRounds++
		time.Sleep(300 * time.Millisecond)
		if drainRounds > 100 {
			break
		}
	}
	summarizeFails(t, "LEAVE (drain window)", fails, 12)

	// Post-exit window: the leaver has left the ring; survivors converge to 3
	// and reconcile toward the genuinely-rebuilt 3-node placement, which can
	// differ from the drain-time successor-chain approximation (index shuffles).
	// The acquire delay is still armed, so any newly-assigned position is
	// provably mid-mount while we probe.
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}, 3, 20*time.Second); err != nil {
		t.Fatalf("post-leave convergence: %v", err)
	}
	var postFails []string
	for r := 0; r < 6; r++ {
		postFails = append(postFails, probeReadsOnce(survivors, uk, []byte("seed"))...)
		time.Sleep(300 * time.Millisecond)
	}
	for _, n := range survivors {
		n.Handle.SetAcquireDelay(0)
	}
	summarizeFails(t, "LEAVE (post-exit window)", postFails, 12)
	fails = append(fails, postFails...)

	if len(fails) > 0 {
		t.Fatalf("LEAVE is not read-transparent: %d client-visible read failures (drain rounds=%d) - reads must be "+
			"served by whoever in the union physically holds the position", len(fails), drainRounds)
	}
}

// TestSurgeReadTransparent_JoinLeaveJoinComposition mirrors the k8s surge
// rollout (maxSurge=1 maxUnavailable=0): a new node joins, an old node is
// gracefully removed once the join settles, and the NEXT new node joins 2s
// after the leave starts. Reads (Get + ScanPrefix) through every stable live
// node must stay transparent through the whole composition.
func TestSurgeReadTransparent_JoinLeaveJoinComposition(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence semantics, both toggles): zero-tolerance
	// single-attempt read probes through the whole join/leave/join surge
	// timeline; see the justification on
	// TestJoinReadTransparent_GetScanEveryNode above.
	backing.SetEagerFence(false)
	backing.SetStrictReadFencing(false)
	mutate := func(cfg *cluster.Config) {
		cfg.GracefulLeaveDrainTimeout = 20 * time.Second
		cfg.OpenConcurrency = 8
	}
	na := startReplicatedNodeCfg(t, "sg-a", "", uc, rf, backing, mutate)
	nb := startReplicatedNodeCfg(t, "sg-b", na.BindAddr, uc, rf, backing, mutate)
	nc := startReplicatedNodeCfg(t, "sg-c", na.BindAddr, uc, rf, backing, mutate)
	if err := waitForMembersAll([]*cluster.Cluster{na.Cluster, nb.Cluster, nc.Cluster}, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)

	uk := oneKeyPerUnit(t, nb.Cluster, uc)
	plantAllServingMarkers(t, nb.Handle, uc, rf)

	var fails []string
	record := func(phase string, fs []string) {
		for _, f := range fs {
			fails = append(fails, phase+": "+f)
		}
	}

	// PHASE 1: join sg-d (slow mount, boot-defers via the planted markers).
	const mountDelay = 5 * time.Second
	nd := startReplicatedNodeSlowAcquireCfg(t, "sg-d", nb.BindAddr, uc, rf, backing, mountDelay, 2*time.Second, mutate)
	if err := waitForMembersAll([]*cluster.Cluster{na.Cluster, nb.Cluster, nc.Cluster, nd.Cluster}, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	for r := 0; r < 3 && desiredButUnmounted(nd.Cluster) > 0; r++ {
		record("join-d", probeReadsOnce([]*sharedNode{na, nb, nc, nd}, uk, []byte("seed")))
		time.Sleep(300 * time.Millisecond)
	}
	// Let sg-d finish warming (the surge waits ~35s between transitions).
	warmDeadline := time.Now().Add(30 * time.Second)
	for desiredButUnmounted(nd.Cluster) > 0 && time.Now().Before(warmDeadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if desiredButUnmounted(nd.Cluster) > 0 {
		t.Fatalf("sg-d never finished warming")
	}

	// PHASE 2: gracefully remove sg-a while sg-d's join has just settled, and
	// PHASE 3: join sg-e 2s later - the leave+join overlap the surge produces.
	for _, n := range []*sharedNode{nb, nc, nd} {
		n.Handle.SetAcquireDelay(2 * time.Second)
	}
	drainDone := make(chan struct{})
	go func() { defer close(drainDone); _ = na.Cluster.Close() }()
	time.Sleep(2 * time.Second)
	ne := startReplicatedNodeSlowAcquireCfg(t, "sg-e", nb.BindAddr, uc, rf, backing, 4*time.Second, 2*time.Second, mutate)

	// Probe continuously through the stable live nodes (sg-b, sg-c, sg-d),
	// adding sg-e as an entry once its ring has converged on the final 4-node
	// membership, until the leave has completed AND sg-e has fully warmed.
	stable := []*sharedNode{nb, nc, nd}
	surgeDeadline := time.Now().Add(45 * time.Second)
	drained := false
	for time.Now().Before(surgeDeadline) {
		entries := stable
		if len(ne.Cluster.Members()) == 4 {
			entries = append(entries, ne)
		}
		record("leave-a+join-e", probeReadsOnce(entries, uk, []byte("seed")))
		select {
		case <-drainDone:
			drained = true
		default:
		}
		if drained && desiredButUnmounted(ne.Cluster) == 0 && len(ne.Cluster.Members()) == 4 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	for _, n := range []*sharedNode{nb, nc, nd, ne} {
		n.Handle.SetAcquireDelay(0)
	}
	if !drained {
		select {
		case <-drainDone:
		case <-time.After(30 * time.Second):
			t.Fatalf("sg-a's graceful leave never completed")
		}
	}

	// Steady state after the surge: everything must be transparent again.
	if err := waitForMembersAll([]*cluster.Cluster{nb.Cluster, nc.Cluster, nd.Cluster, ne.Cluster}, 4, 30*time.Second); err != nil {
		t.Fatalf("post-surge convergence: %v", err)
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
		summarizeFails(t, "POST-SURGE steady state (still failing after settle)", post, 12)
		t.Fatalf("reads still failing at steady state after the surge")
	}

	summarizeFails(t, "SURGE", fails, 20)
	if len(fails) > 0 {
		t.Fatalf("SURGE composition is not read-transparent: %d client-visible read failures across the "+
			"join/leave/join timeline - the k8s rollout probe failure reproduced", len(fails))
	}
}

// TestLeaveEntryServesWhileDraining pins ENTRY-DURING-DRAIN (docs/SPEC.md
// "Union reads"): a gracefully-leaving node keeps serving Get and ScanPrefix
// as the ENTRY node for the whole drain - DrainForLeave runs at the top of
// Close BEFORE the closed flip, so the not-ready fast-fail engages only once
// Close's teardown actually begins. After Close returns, entry ops fail fast
// with backend.ErrClosed (the pinned boundary); covering that teardown tail is
// the orchestrator's routing-drain job, not shale's.
func TestLeaveEntryServesWhileDraining(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence semantics, both toggles): the draining
	// leaver serves ENTRY reads (including its own local legs) for the
	// whole drain while survivors slow-mount its positions; see the
	// justification on TestJoinReadTransparent_GetScanEveryNode above.
	backing.SetEagerFence(false)
	backing.SetStrictReadFencing(false)
	mutate := func(cfg *cluster.Config) {
		cfg.GracefulLeaveDrainTimeout = 25 * time.Second
		cfg.OpenConcurrency = 8
		cfg.RebalanceSettleDelay = 300 * time.Millisecond
	}
	n1 := startReplicatedNodeCfg(t, "le-a", "", uc, rf, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "le-b", n1.BindAddr, uc, rf, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "le-c", n1.BindAddr, uc, rf, backing, mutate)
	n4 := startReplicatedNodeCfg(t, "le-d", n1.BindAddr, uc, rf, backing, mutate)
	all := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}
	if err := waitForMembersAll(all, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)
	uk := oneKeyPerUnit(t, n1.Cluster, uc)

	// Slow the survivors' acquires so the drain window is wide enough to probe.
	for _, n := range []*sharedNode{n1, n2, n3} {
		n.Handle.SetAcquireDelay(2500 * time.Millisecond)
	}
	drainDone := make(chan struct{})
	go func() { defer close(drainDone); _ = n4.Cluster.Close() }()
	time.Sleep(600 * time.Millisecond) // let the Draining bit gossip

	// ENTRY THROUGH THE LEAVER, for the whole drain: every Get and ScanPrefix
	// must serve (single attempt per op; the union + the leaver's still-mounted
	// copies cover everything).
	var fails []string
	rounds := 0
drainLoop:
	for {
		select {
		case <-drainDone:
			break drainLoop
		default:
		}
		roundFails := probeReadsOnce([]*sharedNode{n4}, uk, []byte("seed"))
		// The entry fast-fail (raw backend.ErrClosed) begins the moment Close
		// flips the closed flag - INSIDE Close, before the teardown finishes
		// and drainDone closes. On a slow runner that post-flip stretch fits
		// whole probe rounds, so drainDone is NOT a reliable end-of-serving
		// signal; the observed ErrClosed IS. The first sighting marks the end
		// of the drain's serving phase (the boundary asserted separately
		// below). Stop there; only NON-closed failures inside the serving
		// window count against the pin.
		sawClosed := false
		for _, f := range roundFails {
			if strings.Contains(f, backend.ErrClosed.Error()) {
				sawClosed = true
			} else {
				fails = append(fails, f)
			}
		}
		if sawClosed {
			break drainLoop
		}
		select {
		case <-drainDone:
			break drainLoop
		default:
		}
		rounds++
		time.Sleep(250 * time.Millisecond)
		if rounds > 120 {
			break
		}
	}
	for _, n := range []*sharedNode{n1, n2, n3} {
		n.Handle.SetAcquireDelay(0)
	}
	if rounds < 3 {
		t.Fatalf("drain completed after only %d probe rounds; widen the acquire delay for a provable window", rounds)
	}
	summarizeFails(t, "ENTRY-THROUGH-LEAVER (drain window)", fails, 12)
	if len(fails) > 0 {
		t.Fatalf("a draining node refused entry reads %d times across %d rounds - entry traffic must be served "+
			"for the whole drain (notReady flips only at the actual close)", len(fails), rounds)
	}

	// After Close returns: the pinned fast-fail boundary.
	t0 := time.Now()
	_, err := n4.Cluster.Get([]byte(uk[0]))
	if !errors.Is(err, backend.ErrClosed) {
		t.Fatalf("entry Get after Close = %v, want backend.ErrClosed", err)
	}
	if el := time.Since(t0); el > 100*time.Millisecond {
		t.Fatalf("post-Close entry Get took %v - must fail fast", el)
	}
	t.Logf("entry served through %d drain rounds; post-Close entry fails fast with ErrClosed", rounds)
}
