package integration

// Failure scenario: destination dies mid-migration.
//
// docs/SPEC.md "Failure handling" calls out the destination-crash
// case: source's stream returns an error, source remains owner, the
// next Evaluate pass re-runs the plan against the now-2-node ring.
// The post-conditions this test pins:
//
//   1. The remaining two nodes converge to a 2-member ring.
//   2. No range on either survivor sits stuck in a non-terminal
//      state forever (no orphan ranges).
//   3. Every key written before the failure is gettable via either
//      survivor (assuming its ring-owner is still alive; we filter
//      to that subset, same R=1 caveat as the shrink test).
//
// The shape mirrors a real-world rolling deploy where a node we just
// rebalanced TO falls over before the data settled; the cluster has
// to detect this + retry without operator intervention.

import (
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
)

func TestRebalance_DestinationFailureMidMigration(t *testing.T) {
	t.Parallel()

	// --- bring up a 2-node cluster + write a body of keys so the
	// growth migration has real work to do. ---
	n1 := startTestNode(t, "fail-n1", "")
	n2 := startTestNode(t, "fail-n2", n1.BindAddr)
	pair := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(pair, 2, 10*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}

	const total = 400
	keys := putN(t, n1.Cluster, "fail", total)

	// --- add a third node + immediately schedule it to die. ---
	n3 := startTestNode(t, "fail-n3", n1.BindAddr)
	trio := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(trio, 3, 10*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}

	// Mid-migration: hit n3 right when the source is most likely
	// streaming to it. The spec's settle delay is 5s + the stream
	// runs immediately after, so a window of [5s, 5s+stream] is the
	// danger zone. Wait 5.5s to be inside that window, then close.
	//
	// Worst case (rebalance not wired yet): close fires during steady
	// state + the rest of the test still proves the survivors
	// converge to a 2-node ring with their existing data intact.
	time.Sleep(5500 * time.Millisecond)
	n3.Close()

	// Survivors converge to a 2-member ring.
	if err := waitForMembersAll(pair, 2, 10*time.Second); err != nil {
		t.Fatalf("post-failure convergence to 2 members: %v", err)
	}

	// Filter the test set to keys whose POST-failure ring owner is
	// one of the survivors. R=1: anything stranded on n3 at the
	// moment it died is gone.
	finalRing := ring.New()
	for _, m := range n1.Cluster.Members() {
		finalRing.Add(m)
	}
	survivable := make([]string, 0, len(keys))
	for _, k := range keys {
		if owner := finalRing.LocateKey([]byte(k)).ID; owner == "fail-n1" || owner == "fail-n2" {
			survivable = append(survivable, k)
		}
	}
	if len(survivable) < total/2 {
		t.Fatalf("expected most keys' new owner to be a survivor, got %d/%d (ring distribution suspect)",
			len(survivable), total)
	}

	// --- wait for the next Evaluate pass to converge against the
	// 2-node ring. ---
	if err := waitForRebalanceIdle(t, n1.Cluster, []*testNode{n1, n2}, survivable, 25*time.Second); err != nil {
		after := perNodeKeyCount(t, []*testNode{n1, n2})
		t.Fatalf("post-failure rebalance did not converge for survivable set: %v\npost-failure distribution: %v\n(if survivable keys are still missing from their new ring-owner, the cluster either never re-evaluated after n3 died or left orphan ranges in a non-terminal state)",
			err, after)
	}

	// --- assertion 1: every survivable key is gettable via either
	// survivor. ---
	survivors := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	assertAllGettable(t, survivors, survivable)

	// --- assertion 2: no survivor holds zero keys (the rebalance
	// did NOT collapse onto one node). ---
	dist := perNodeKeyCount(t, []*testNode{n1, n2})
	t.Logf("post-failure distribution: %v", dist)
	for _, n := range []*testNode{n1, n2} {
		if dist[n.ID] == 0 {
			t.Fatalf("post-failure: survivor %s holds zero keys; distribution=%v", n.ID, dist)
		}
	}

	// --- assertion 3: writes still work end-to-end (the cluster
	// isn't stuck in some "migration cancelled, all writes rejected"
	// state). ---
	if err := n1.Cluster.Put([]byte("fail-post-write"), []byte("ok")); err != nil {
		t.Fatalf("Put after failure recovery: %v (cluster may have stuck range state from the aborted migration)", err)
	}
	got, err := n2.Cluster.Get([]byte("fail-post-write"))
	if err != nil {
		t.Fatalf("Get after failure recovery: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("Get after failure recovery: got %q want ok", got)
	}
}
