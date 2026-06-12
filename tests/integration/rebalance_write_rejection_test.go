package integration

// Write-rejection scenario: per docs/SPEC.md "Cutover", a Put against
// a key whose partition is currently being migrated MUST be rejected
// by the source with ResourceExhausted (plus a retry-after hint).
// FailedPrecondition is reserved for the forwarding loop-guard
// (docs/SPEC.md "Failure handling"). After the migration completes,
// the same Put MUST succeed.
//
// Timing reality: catching the in-flight migration window for a
// specific key is racy in a black-box test, because we don't pause
// the migration via a test hook (no such hook is on the public
// surface yet, and the SPEC does not require one). To compensate,
// the test:
//
//   1. Picks a key whose ring owner is GUARANTEED to change when
//      the new node joins (we pre-compute new vs old owner from two
//      ring snapshots).
//   2. Spawns a join goroutine that brings n3 online.
//   3. Hammers the target key with Put requests in a tight loop for
//      a budget that covers (settle + stream) windows. Any single Put
//      that returns FailedPrecondition during that hammering counts
//      as observing the rejection.
//   4. Once the membership stabilizes + rebalance idles, asserts the
//      same Put succeeds.
//
// Failure modes the test catches:
//   - Source never rejects writes during migration (correctness bug:
//     a write would silently land on the soon-to-be-former owner +
//     get swept later, costing the write).
//   - Source still rejects long after migration completes (liveness
//     bug: the per-range state never transitioned out of Sending /
//     HandedOff).
//   - The cluster never observes the join, so no migration runs +
//     no rejection ever fires (integration not wired).

import (
	"fmt"
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
	// whose owner WILL change on the join + whose current owner is
	// known (the source we expect to do the rejecting).
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

	// --- launch n3 ---
	n3 := startTestNode(t, "wr-n3", n1.BindAddr)
	trio := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}

	// Hammer target with Puts until either (a) we observe one
	// returning ResourceExhausted or (b) the time budget expires. The
	// settle delay is 5s + stream is small; 20s budget is generous.
	deadline := time.Now().Add(20 * time.Second)
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
		// Small sleep so we don't spin so tight the source goroutine
		// can't make progress.
		time.Sleep(2 * time.Millisecond)
	}

	if !observedRejection {
		// Could be: (a) integration not wired, no rejection ever
		// fires; (b) migration finished faster than we could hit the
		// window; (c) source rejects with a different code shape.
		// We require (a) to fail loudly. If membership did reach 3
		// AND distribution is balanced, the most likely explanation
		// is (b) or (c) and we still flag it because the SPEC is
		// explicit about ResourceExhausted.
		if err := waitForMembersAll(trio, 3, 10*time.Second); err != nil {
			t.Fatalf("3-node convergence: %v (no rejection observed either; rebalance hook likely not wired)", err)
		}
		t.Fatalf("never observed ResourceExhausted on target key during migration window (attempts=%d, last err=%v). per docs/SPEC.md \"Cutover\", the source MUST reject writes for a migrating key with codes.ResourceExhausted. If the cluster v0.3 wiring lands without this rejection path, a write during the streaming copy is silently lost.",
			attempts, lastErr)
	}

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
