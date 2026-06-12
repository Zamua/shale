package integration

// Multi-node reshard REVIEW-GAP regressions for v0.8 (the cluster-wide-freeze
// barrier). The fix commit closed four hazards the review found in UNTESTED
// paths; these in-process multi-node integration tests (on the shared-backing
// factory, reusing the existing helpers) PIN those fixes so a regression that
// re-opens any gap fails loudly:
//
//	1. ABORT+RETRY no resurrection (P1-B). A reshard that ABORTS mid-flight
//	   (a pre-FLIP failure) CloseUnit's but does not wipe its half-built
//	   gen-(g+1) children. A key DELETED between the aborted run and a later
//	   successful reshard must stay deleted at gen g+1, not be resurrected from
//	   the aborted run's stale child bytes (clear-before-copy).
//	2. RESUME-DROP self-heal (P1-C). A node whose RESUME RPC is dropped is left
//	   FLIPPED-but-frozen and would reject every write forever. The node-side
//	   stale-freeze self-heal must EVENTUALLY auto-unfreeze it (past the grace)
//	   so it accepts writes again - no permanent stuck-frozen.
//	3. TRANSACT-UNDER-FREEZE (P2-A). A Transact / Begin (LOCAL-pinned AND
//	   REMOTE-pinned) issued during the freeze must get the RETRYABLE
//	   codes.Unavailable (never a hard failure, never a lost commit); the
//	   commit then succeeds at gen g+1 after RESUME.
//	5. MEMBERSHIP-SWAP abort (P2-C). A count-preserving membership swap (X
//	   leaves, Y joins, count unchanged) ACROSS a barrier must ABORT the
//	   reshard - a count-only check would miss it and straddle.
//
// (The P1-A in-flight-write-vs-bisect race is pinned white-box in
// pkg/cluster/multibackend_reshard_barrier_race_internal_test.go, where the
// production write path can be suspended mid-Put deterministically.)

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deleteWithRetryUnavailable issues a Delete, retrying the retryable
// acquiring-window / freeze-window error (codes.Unavailable) until success or
// timeout - the Delete analogue of putWithRetryUnavailable.
func deleteWithRetryUnavailable(t *testing.T, c *cluster.Cluster, key string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := c.Delete([]byte(key))
		if err == nil {
			return nil
		}
		if st, _ := status.FromError(err); st.Code() != codes.Unavailable || time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// remoteKeyFor returns a key whose unit (at unitCount) is owned by a node OTHER
// than n on the ring built from n's Members() - so a Put / CAS pin of it on n
// FORWARDS to a peer, exercising the remote (RPC) commit path's freeze gate.
func remoteKeyFor(t *testing.T, n *sharedNode, unitCount int) string {
	t.Helper()
	for i := range 100000 {
		k := fmt.Sprintf("rk-%d", i)
		if unitOwnerOnRing(n.Cluster, k, unitCount) != n.ID {
			return k
		}
	}
	t.Fatalf("no remote key found for node %s", n.ID)
	return ""
}

// TestMultiNodeReshard_AbortThenRetryDoesNotResurrectDeletedKey is the P1-B
// integration regression. On a 2-node shared-backing cluster: seed a dataset,
// drive a reshard that ABORTS mid-flight (FREEZE + BISECT on both nodes, then a
// forced ABORT before FLIP - the pre-FLIP failure the coordinator catches), then
// DELETE a key at gen g, then run a REAL successful Reshard(). The deleted key
// must stay DELETED at gen g+1 - not resurrected from the aborted run's
// half-built (CloseUnit'd-but-not-wiped) children. Without clear-before-copy in
// bisectUnitStatic the retry would re-open the stale child still holding the
// victim's bytes and FLIP would publish it.
func TestMultiNodeReshard_AbortThenRetryDoesNotResurrectDeletedKey(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "p1b-1", "", unitCount, backing)
	n2 := startSharedNode(t, "p1b-2", n1.BindAddr, unitCount, backing)
	nodes := []*sharedNode{n1, n2}
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 20*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// Seed a dataset spanning every unit.
	want := make(map[string][]byte)
	const nKeys = 200
	for i := range nKeys {
		k := fmt.Sprintf("res-%05d", i)
		v := fmt.Appendf(nil, "rv-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 10*time.Second); err != nil {
			t.Fatalf("seed Put %q: %v", k, err)
		}
		want[k] = v
	}

	// Run 1 (the ABORTED reshard): FREEZE + BISECT both nodes, then ABORT before
	// FLIP. This models the coordinator catching a pre-FLIP phase failure: every
	// node unfreezes, discards its half-built gen-1 children (CloseUnit, bytes
	// retained in the shared backing), and stays at gen 0.
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FREEZE, 1); err != nil {
			t.Fatalf("run1 FREEZE %s: %v", n.ID, err)
		}
	}
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_BISECT, 1); err != nil {
			t.Fatalf("run1 BISECT %s: %v", n.ID, err)
		}
	}
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_ABORT, 1); err != nil {
			t.Fatalf("run1 ABORT %s: %v", n.ID, err)
		}
	}

	// The cluster is back at gen 0, unfrozen. DELETE a victim key (which the
	// aborted run already copied into its now-discarded gen-1 child).
	victim := "res-00000"
	if err := putWithRetryUnavailable(t, n1.Cluster, victim+"-noop", "x", 5*time.Second); err != nil {
		t.Fatalf("sanity Put after abort (cluster should be unfrozen): %v", err)
	}
	delete(want, victim+"-noop") // not part of the recorded dataset
	if err := deleteWithRetryUnavailable(t, n1.Cluster, victim, 8*time.Second); err != nil {
		t.Fatalf("delete victim %q between runs: %v", victim, err)
	}
	delete(want, victim)
	// Confirm the delete took at gen 0.
	if got, err := getWithRetryUnavailable(t, n1.Cluster, victim, 5*time.Second); err == nil && got != nil {
		t.Fatalf("victim %q still present at gen 0 after delete (= %q); cannot test resurrection", victim, got)
	}

	// Run 2 (the REAL successful reshard): a coordinator-driven Reshard() that
	// re-bisects. bisectUnitStatic must CLEAR the children before re-copying, so
	// the deleted victim does NOT survive in the rebuilt child.
	if err := n1.Cluster.Reshard(); err != nil {
		t.Fatalf("run2 Reshard: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// === ASSERTION (P1-B): the deleted key stays DELETED at gen g+1. ===
	for _, n := range nodes {
		got, err := getWithRetryUnavailable(t, n.Cluster, victim, 8*time.Second)
		if err == nil && got != nil {
			t.Fatalf("P1-B VIOLATED: deleted key %q resurrected at gen g+1 via %s (= %q); the aborted run's stale child bytes were not cleared before the re-copy", victim, n.ID, got)
		}
	}
	// Also confirm it is physically absent from its gen-1 child store.
	newUC := storageunit.MustUnitCount(2 * unitCount)
	victimU1 := storageunit.UnitForShardKey(ring.ShardKey([]byte(victim)), newUC)
	if found, ok := sharedStoreHas(backing, storageunit.NewGenUnit(1, victimU1), victim, []byte("rv-00000")); ok && found {
		t.Fatalf("P1-B VIOLATED: deleted key %q present in its gen-1 child store (resurrected from the aborted run)", victim)
	}

	// === ASSERTION: every SURVIVING key is intact at gen g+1 (the reshard was
	// otherwise lossless). ===
	for k, v := range want {
		for _, n := range nodes {
			got, err := getWithRetryUnavailable(t, n.Cluster, k, 10*time.Second)
			if err != nil {
				t.Fatalf("surviving key %q unreadable via %s after abort+retry reshard: %v", k, n.ID, err)
			}
			if !bytes.Equal(got, v) {
				t.Fatalf("surviving key %q via %s = %q, want %q", k, n.ID, got, v)
			}
		}
	}
}

// TestMultiNodeReshard_DroppedResumeSelfHealsUnfreeze is the P1-C integration
// regression. We drive the barrier manually and DROP one node's RESUME RPC,
// leaving it FLIPPED to gen 1 but still frozen - which would reject every write
// FOREVER. The node-side stale-freeze self-heal (driven by the reconcile loop)
// must EVENTUALLY auto-unfreeze it once it has aged past staleFreezeGrace, so it
// accepts writes again. We shrink the grace (test hook) and tick the reconcile
// to avoid waiting out the 25s production grace.
func TestMultiNodeReshard_DroppedResumeSelfHealsUnfreeze(t *testing.T) {
	// Shrink the stale-freeze grace so the self-heal fires in milliseconds, not
	// the 25s production default. Restored at test end.
	restore := cluster.TestingSetStaleFreezeGrace(30 * time.Millisecond)
	defer restore()

	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "p1c-1", "", unitCount, backing)
	n2 := startSharedNode(t, "p1c-2", n1.BindAddr, unitCount, backing)
	nodes := []*sharedNode{n1, n2}
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 20*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// Seed so every unit has data to bisect.
	for i := range 120 {
		k := fmt.Sprintf("sh-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, fmt.Sprintf("v-%05d", i), 10*time.Second); err != nil {
			t.Fatalf("seed Put %q: %v", k, err)
		}
	}

	// Drive FREEZE -> BISECT -> FLIP on BOTH nodes, but RESUME only n1. n2 is the
	// node whose RESUME was "dropped": it is flipped to gen 1 yet still frozen.
	dropped := n2
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FREEZE, 1); err != nil {
			t.Fatalf("FREEZE %s: %v", n.ID, err)
		}
	}
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_BISECT, 1); err != nil {
			t.Fatalf("BISECT %s: %v", n.ID, err)
		}
	}
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FLIP, 1); err != nil {
			t.Fatalf("FLIP %s: %v", n.ID, err)
		}
	}
	// RESUME n1 only - n2's RESUME is dropped on the floor.
	if err := n1.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_RESUME, 1); err != nil {
		t.Fatalf("RESUME n1: %v", err)
	}

	// Precondition: the dropped node is stranded FLIPPED-but-frozen, so it
	// refuses writes with the retryable error.
	if !dropped.Cluster.TestingIsFrozen() {
		t.Fatalf("precondition: dropped node %s should be stranded frozen after FLIP-without-RESUME", dropped.ID)
	}
	frozenKey := localKeyFor(t, dropped, 2*unitCount)
	if err := dropped.Cluster.Put([]byte(frozenKey), []byte("blocked")); err == nil {
		t.Fatalf("precondition: write on the stranded node should be refused while frozen")
	} else if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
		t.Fatalf("precondition: stranded-node write error = %v, want codes.Unavailable", st.Code())
	}

	// Age past the (shrunk) grace, then drive the reconcile self-heal on the
	// dropped node until it auto-unfreezes. This is what the periodic reconcile
	// loop does in production; we tick it on demand for determinism.
	time.Sleep(60 * time.Millisecond)
	healed := waitUntil(5*time.Second, func() bool {
		dropped.Cluster.TestingRunReconcile()
		return !dropped.Cluster.TestingIsFrozen()
	})
	if !healed {
		t.Fatal("P1-C VIOLATED: a node left frozen by a dropped RESUME RPC never auto-unfroze; it is permanently stuck-frozen")
	}

	// === ASSERTION (P1-C): the self-healed node ACCEPTS WRITES again, at gen 1. ===
	if err := putWithRetryUnavailable(t, dropped.Cluster, frozenKey, "after-self-heal", 8*time.Second); err != nil {
		t.Fatalf("P1-C VIOLATED: the self-healed node still refuses writes: %v", err)
	}
	got, err := getWithRetryUnavailable(t, dropped.Cluster, frozenKey, 8*time.Second)
	if err != nil || !bytes.Equal(got, []byte("after-self-heal")) {
		t.Fatalf("P1-C VIOLATED: write after self-heal not readable: got=%q err=%v", got, err)
	}
	// The node is at gen 1 (it flipped); confirm a doubled-only-unit key routes.
	if dropped.Cluster.TestingIsFrozen() {
		t.Fatal("P1-C VIOLATED: node frozen again after self-heal + write")
	}
}

// TestMultiNodeReshard_TransactUnderFreezeIsRetryableThenCommits is the P2-A
// integration regression. While the cluster is frozen for a reshard, a Begin /
// Transact - LOCAL-pinned AND REMOTE-pinned - must get the RETRYABLE
// codes.Unavailable, never a hard failure. A Transact running across the freeze
// must transparently retry and COMMIT at gen g+1 after RESUME. We assert both:
// (a) Begin while frozen returns Unavailable for a local- and a remote-pinned
// key; (b) a Transact spanning the whole barrier commits exactly once, at gen 1.
func TestMultiNodeReshard_TransactUnderFreezeIsRetryableThenCommits(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "p2a-1", "", unitCount, backing)
	n2 := startSharedNode(t, "p2a-2", n1.BindAddr, unitCount, backing)
	nodes := []*sharedNode{n1, n2}
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 20*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// Seed so every unit has data to bisect.
	for i := range 120 {
		k := fmt.Sprintf("tx-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, fmt.Sprintf("sv-%05d", i), 10*time.Second); err != nil {
			t.Fatalf("seed Put %q: %v", k, err)
		}
	}

	// A LOCAL-pinned counter key (owned by n1) and a REMOTE-pinned counter key
	// (owned by n2, so the commit forwards). Both run through Transact.
	localCounter := localKeyFor(t, n1, unitCount)
	remoteCounter := remoteKeyFor(t, n1, unitCount)

	// Two Transact goroutines, one local-pin one remote-pin, kick off and span
	// the whole barrier. fn is a PURE pinned write (it pins via tx.Put, no read),
	// so the only retryable signal the transaction can hit is the COMMIT-path
	// freeze gate - exactly the P2-A behavior under test. A read inside fn could
	// also hit the orthogonal post-RESUME redistribution acquiring-window (a
	// Phase-3 retryable Transact surfaces as a hard fn-error, not a commit
	// retry), which would muddy this test; keeping fn write-only isolates the
	// freeze-commit retry. fn is idempotent under Transact's re-run (it writes the
	// same value each time).
	type txResult struct {
		err error
	}
	results := make(chan txResult, 2)
	runTx := func(counter string) {
		err := n1.Cluster.Transact([]byte(counter), func(tx backend.Transaction) error {
			return tx.Put([]byte(counter), []byte("1"))
		})
		results <- txResult{err: err}
	}

	// FREEZE both nodes FIRST so the transactions begin under the freeze.
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FREEZE, 1); err != nil {
			t.Fatalf("FREEZE %s: %v", n.ID, err)
		}
	}

	// === ASSERTION (P2-A): Begin while frozen returns the RETRYABLE Unavailable.
	// Begin checks the LOCAL freeze flag (it does not route), so EVERY frozen node
	// must refuse it with the retryable code - so a client does not buffer a
	// transaction doomed to fail at the (also-frozen) commit. (The LOCAL-pin and
	// REMOTE-pin COMMIT-path freeze gates are exercised by the two Transact
	// goroutines below: one pins a key owned by n1, one a key owned by n2, so the
	// commit hits the local CommitCASApply freeze gate AND the forwarded CommitCAS
	// RPC freeze gate respectively.) ===
	for _, n := range nodes {
		_, err := n.Cluster.Begin(backend.SnapshotIsolation)
		if err == nil {
			t.Fatalf("P2-A VIOLATED: Begin on frozen node %s returned no error; want a retryable Unavailable", n.ID)
		}
		if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
			t.Fatalf("P2-A VIOLATED: frozen Begin on %s code = %v, want codes.Unavailable", n.ID, st.Code())
		}
	}

	// Now launch the Transacts (they retry the frozen-commit Unavailable
	// internally) and complete the barrier so they commit at gen 1.
	go runTx(localCounter)
	go runTx(remoteCounter)
	// Let the transactions reach (and retry) the frozen commit before we drive
	// the rest of the barrier, so the commit genuinely traverses the freeze.
	time.Sleep(120 * time.Millisecond)

	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_BISECT, 1); err != nil {
			t.Fatalf("BISECT %s: %v", n.ID, err)
		}
	}
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FLIP, 1); err != nil {
			t.Fatalf("FLIP %s: %v", n.ID, err)
		}
	}
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_RESUME, 1); err != nil {
			t.Fatalf("RESUME %s: %v", n.ID, err)
		}
	}
	// Let the post-RESUME redistribution settle so forwarded commits route.
	time.Sleep(900 * time.Millisecond)

	// === ASSERTION (P2-A): both Transacts COMMITTED (no hard failure across the
	// freeze). ===
	for range 2 {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("P2-A VIOLATED: a Transact spanning the freeze failed instead of retrying-then-committing: %v", r.err)
			}
		case <-time.After(35 * time.Second):
			t.Fatal("P2-A VIOLATED: a Transact never returned; the freeze retry did not converge")
		}
	}

	// === ASSERTION: the committed values are readable at gen 1 from every node. ===
	for _, counter := range []string{localCounter, remoteCounter} {
		got, err := getWithRetryUnavailable(t, n2.Cluster, counter, 10*time.Second)
		if err != nil {
			t.Fatalf("P2-A VIOLATED: committed counter %q unreadable at gen 1: %v", counter, err)
		}
		if !bytes.Equal(got, []byte("1")) {
			t.Fatalf("P2-A VIOLATED: counter %q = %q, want %q (the freeze-spanning commit landed exactly once at gen 1)", counter, got, "1")
		}
	}
}

// TestMultiNodeReshard_MembershipSwapAcrossBarrierAborts is the P2-C integration
// regression. The coordinator snapshots the member IDENTITY set at the start and
// re-checks it before each barrier. A count-preserving swap (one member leaves,
// another joins, count unchanged) ACROSS the FREEZE barrier must ABORT the
// reshard - a count-only check would miss the swap, drive the stale set, and
// straddle. We inject the swap deterministically via a post-FREEZE test hook that
// re-keys the coordinator's ring to a same-size DIFFERENT identity set, and
// assert Reshard() returns an abort error AND the cluster stays at gen g,
// unfrozen, with data intact.
func TestMultiNodeReshard_MembershipSwapAcrossBarrierAborts(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "p2c-1", "", unitCount, backing)
	n2 := startSharedNode(t, "p2c-2", n1.BindAddr, unitCount, backing)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 20*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// Seed a dataset spanning every unit.
	want := make(map[string][]byte)
	for i := range 200 {
		k := fmt.Sprintf("ms-%05d", i)
		v := fmt.Appendf(nil, "mv-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 10*time.Second); err != nil {
			t.Fatalf("seed Put %q: %v", k, err)
		}
		want[k] = v
	}

	// Snapshot the real 2-member set, then build a SAME-SIZE swapped set: keep
	// the coordinator (n1) but replace the peer (n2) with a phantom "ghost". The
	// count is preserved (2 -> 2); only the identity changed. The coordinator's
	// pre-BISECT membershipChanged must catch this and ABORT.
	realMembers := n1.Cluster.Members()
	if len(realMembers) != 2 {
		t.Fatalf("precondition: want 2 members, got %d", len(realMembers))
	}
	var n1Member ring.Member
	for _, m := range realMembers {
		if m.ID == n1.ID {
			n1Member = m
		}
	}
	// Keep the coordinator (n1), replace the peer with a phantom "ghost-joiner":
	// count preserved (2 -> 2), identity changed.
	swapped := []ring.Member{
		n1Member,
		{ID: "ghost-joiner", Addr: "127.0.0.1:1"},
	}

	// Install the post-FREEZE hook: it fires once, after the FREEZE barrier acks
	// and BEFORE the pre-BISECT membership re-check, and swaps the coordinator's
	// ring identity set. Clear it once it has fired so a retry is clean.
	var swapOnce sync.Once
	n1.Cluster.TestingSetAfterFreezeHook(func() {
		swapOnce.Do(func() {
			n1.Cluster.TestingSetRingMembers(swapped)
		})
	})
	defer n1.Cluster.TestingSetAfterFreezeHook(nil)

	// === ASSERTION (P2-C): the reshard ABORTS on the count-preserving swap. ===
	err := n1.Cluster.Reshard()
	if err == nil {
		t.Fatal("P2-C VIOLATED: Reshard succeeded despite a count-preserving membership swap across the barrier; a count-only check missed the identity change")
	}
	t.Logf("reshard aborted as expected on the count-preserving swap: %v", err)

	// Restore the real ring so the post-abort assertions route correctly, and let
	// any abort-driven unfreeze settle.
	n1.Cluster.TestingSetRingMembers(realMembers)
	time.Sleep(300 * time.Millisecond)

	// === ASSERTION (P2-C): the cluster STAYED at gen g (0), unfrozen. A fresh
	// write must land in a gen-0 store (routing did not advance). ===
	if n1.Cluster.TestingIsFrozen() {
		t.Fatal("P2-C VIOLATED: coordinator still frozen after the aborted reshard")
	}
	freshKey := "post-swap-abort-fresh"
	freshVal := []byte("written-at-gen-0-after-abort")
	if err := putWithRetryUnavailable(t, n1.Cluster, freshKey, string(freshVal), 8*time.Second); err != nil {
		t.Fatalf("P2-C VIOLATED: writes did not resume after the aborted reshard: %v", err)
	}
	freshU0 := storageunit.UnitForShardKey(ring.ShardKey([]byte(freshKey)), storageunit.MustUnitCount(unitCount))
	if found, ok := sharedStoreHas(backing, storageunit.NewGenUnit(0, freshU0), freshKey, freshVal); !ok || !found {
		t.Fatalf("P2-C VIOLATED: fresh key %q did not land in a gen-0 store %s (ok=%v found=%v); routing advanced past gen 0 despite the abort", freshKey, storageunit.NewGenUnit(0, freshU0), ok, found)
	}

	// === ASSERTION: the seeded dataset is intact (the aborted reshard touched no
	// live gen-0 data). ===
	for k, v := range want {
		got, err := getWithRetryUnavailable(t, n1.Cluster, k, 8*time.Second)
		if err != nil {
			t.Fatalf("seeded key %q unreadable after aborted reshard: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("seeded key %q = %q, want %q after aborted reshard", k, got, v)
		}
	}
}
