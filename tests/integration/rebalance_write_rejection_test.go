package integration

// Write-rejection scenario: per docs/SPEC.md "Cutover", a Put against
// a key whose partition is currently being migrated MUST be rejected
// with ResourceExhausted (plus a retry-after hint).
// FailedPrecondition is reserved for the forwarding loop-guard
// (docs/SPEC.md "Failure handling"). After the migration completes,
// the same Put MUST succeed.
//
// WHICH NODE REJECTS: the DESTINATION, not the source. On the R=1
// single-backend path the rejection is produced by the destination's
// IsReceiving guard (LocalReplicaPut in pkg/cluster/replicate.go) on
// the forwarded leg, NOT by the source's IsMigrating guard. The
// source's guard in Cluster.Put sits inside the `if local` branch,
// and ring ownership flips to the destination the instant memberlist
// gossips the join, which is well BEFORE the source's settle-delayed
// Evaluate registers StateSending. So by the time the source is
// migrating, it no longer routes the key locally and never evaluates
// its own guard. The source-side term stays live for the R>1 fan-out
// and multi-backend paths; on this path it is unreachable from a
// client-originated write. See docs/SPEC.md "Cutover".
//
// That makes the destination's StateReceiving window the thing under
// test, and on a loopback fixture it is single-digit milliseconds
// wide: registered inside Open, closed as soon as FetchRange finishes
// streaming one partition. Hammering Puts hoping to land inside it is
// bimodal, not slow-but-correct - either the first probe hits it or
// none of several thousand over 20s does. So the test HOLDS the
// window open with Config.TestingReceiveGate (test-only seam) instead
// of racing it:
//
//   1. Picks a key whose ring owner is GUARANTEED to change when
//      the new node joins (we pre-compute new vs old owner from two
//      ring snapshots).
//   2. Brings n3 online with its FetchRange parked on a gate, so the
//      range sits in StateReceiving until we say otherwise.
//   3. Probes the target key until one Put returns ResourceExhausted.
//      With the window held open this is deterministic; failing to
//      observe is now a real regression, not bad luck.
//   4. Releases the gate, then asserts the migration COMPLETES and
//      the same Put succeeds against the new owner.
//
// Failure modes the test catches:
//   - Nothing rejects writes during migration (correctness bug: the
//     destination's migration apply is a raw backend Put with no LWW
//     compare, so a write accepted mid-stream would be clobbered by
//     the in-flight copy).
//   - Rejection persists long after migration completes (liveness
//     bug: the per-range state never transitioned out of Receiving /
//     Sending / HandedOff).
//   - The cluster never observes the join, so no migration runs +
//     no rejection ever fires (integration not wired).

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRebalance_WriteRejectionDuringMigration(t *testing.T) {
	t.Parallel()

	// --- 2-node baseline ---
	n1 := startTestNode(t, "wr-n1", "")
	n2 := startTestNode(t, "wr-n2", n1.BindAddr)
	pair := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(pair, 2, 10*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}

	// Seed some keys so the migrating range has something to send.
	const seed = 200
	_ = putN(t, n1.Cluster, "wr", seed)

	// Pre-compute the future ring (n1, n2, n3) so we can pick a key
	// whose owner WILL change on the join. The destination (wr-n3) is
	// the node we expect to do the rejecting, on the leg n1 forwards.
	futureRing := ring.New()
	for _, m := range n1.Cluster.Members() {
		futureRing.Add(m)
	}
	futureRing.Add(ring.Member{ID: "wr-n3", Addr: "127.0.0.1:0"})

	currentRing := ring.New()
	for _, m := range n1.Cluster.Members() {
		currentRing.Add(m)
	}

	var target string
	for i := range 5000 {
		candidate := fmt.Sprintf("wr-target-%05d", i)
		oldOwner := currentRing.LocateKey([]byte(candidate)).ID
		newOwner := futureRing.LocateKey([]byte(candidate)).ID
		if oldOwner != newOwner && newOwner == "wr-n3" {
			target = candidate
			break
		}
	}
	if target == "" {
		t.Fatalf("could not find a key whose owner moves to wr-n3 on join (ring distribution unexpectedly skewed)")
	}
	// Make sure the target has a value to migrate (the spec's scan
	// filter wouldn't ship it otherwise).
	if err := n1.Cluster.Put([]byte(target), []byte(expectedValue(target))); err != nil {
		t.Fatalf("seed target %s: %v", target, err)
	}
	t.Logf("target key %q current-owner=%s future-owner=wr-n3", target, currentRing.LocateKey([]byte(target)).ID)

	// --- launch n3 with its receive window pinned open ---
	//
	// The gate parks n3's FetchRange before it dials n1, with the range
	// already registered StateReceiving. Release is deferred BEFORE the
	// node starts so any t.Fatalf below still unblocks FetchRange and
	// lets teardown (and goleak) run clean.
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	defer release()

	n3 := startGatedDestTestNode(t, "wr-n3", n1.BindAddr, gate)
	trio := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}

	// Probe target until a Put returns ResourceExhausted. The window is
	// held open, so this normally lands on attempt 1; the deadline is a
	// regression bound, not a race budget.
	//
	// Keep it SHORT (2s, ~1000x the observed latency): n1 is a plain
	// startTestNode with RebalanceHandoffTimeout=4s, and holding the
	// gate past that would time out n1's send-side handoff and break
	// the post-release convergence assertions below.
	deadline := time.Now().Add(2 * time.Second)
	observedRejection := false
	var lastErr error
	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		err := n1.Cluster.Put([]byte(target), []byte(expectedValue(target)))
		if err != nil {
			lastErr = err
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.ResourceExhausted {
					observedRejection = true
					t.Logf("observed ResourceExhausted after %d attempts: %v", attempts, err)
					break
				}
			}
		}
		// Small sleep so we don't spin so tight the forwarding leg
		// can't make progress.
		time.Sleep(2 * time.Millisecond)
	}

	if !observedRejection {
		// The receive window was HELD OPEN for the whole probe, so this
		// is not a timing miss. Either the migration-guard rejection is
		// gone (a write during the streaming copy would be clobbered by
		// the in-flight copy, costing the write), the join never
		// produced a migration at all, or the rejection changed code
		// shape (the SPEC is explicit about ResourceExhausted).
		if err := waitForMembersAll(trio, 3, 10*time.Second); err != nil {
			t.Fatalf("3-node convergence: %v (no rejection observed either; rebalance hook likely not wired)", err)
		}
		t.Fatalf("no ResourceExhausted on target key while the receive window was HELD OPEN (attempts=%d, last err=%v). per docs/SPEC.md \"Cutover\", a Put for a migrating key MUST be rejected with codes.ResourceExhausted; on this R=1 path that rejection comes from the destination's IsReceiving guard in pkg/cluster/replicate.go.",
			attempts, lastErr)
	}

	// Let the migration finish, then assert it actually completes.
	release()

	// --- wait for the rebalance to settle, then assert the same Put
	// succeeds. ---
	if err := waitForMembersAll(trio, 3, 10*time.Second); err != nil {
		t.Fatalf("3-node convergence post-rejection: %v", err)
	}
	if err := waitForRebalanceIdle(t, n1.Cluster, []*testNode{n1, n2, n3}, []string{target}, 20*time.Second); err != nil {
		t.Fatalf("rebalance did not converge for target key: %v", err)
	}

	// Retry should succeed via any node (the new owner is wr-n3, but
	// n1 forwards under the routing layer).
	if err := n1.Cluster.Put([]byte(target), []byte("post-rebalance")); err != nil {
		t.Fatalf("Put %s after rebalance idle: still rejected: %v (ResourceExhausted leak: range never left the migrating state)", target, err)
	}
	got, err := n3.Cluster.Get([]byte(target))
	if err != nil {
		t.Fatalf("Get %s via new owner wr-n3 after rebalance: %v", target, err)
	}
	if string(got) != "post-rebalance" {
		t.Fatalf("Get %s after rebalance: got %q want %q", target, got, "post-rebalance")
	}

	// Defensive: a final probe should NOT hit ResourceExhausted.
	if err := n2.Cluster.Put([]byte(target), []byte("post-rebalance-2")); err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.ResourceExhausted {
			t.Fatalf("Put via n2 after settle: still ResourceExhausted: %v", err)
		}
		t.Fatalf("Put via n2 after settle: unexpected err: %v", err)
	}
	_ = lastErr // referenced above; keep for diagnostic logging only
}
