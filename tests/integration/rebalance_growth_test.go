package integration

// Growth scenario: 2-node cluster grows to 3-node. v0.3 should
// physically migrate the partitions whose ring-owner changed, so the
// new node ends up holding its share of the keyspace + every key is
// still gettable via any node.
//
// Why this matters: a v0.2 cluster's ring would already redirect
// reads for the migrating partitions to the new (empty) node, which
// returns not-found. v0.3 closes that gap by moving the bytes. A
// regression that drops the rebalance trigger or breaks the
// destination-pull stream surfaces here as either:
//   - a node with zero physical keys (the new node never received)
//   - a Get-after-rebalance returning ErrNotFound (data lost on the
//     hand-off boundary)
// The test asserts both negatively.

import (
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/cluster"
)

func TestRebalance_GrowthTwoToThree(t *testing.T) {
	t.Parallel()

	// --- bring up the initial 2-node cluster ---
	n1 := startTestNode(t, "grow-n1", "")
	n2 := startTestNode(t, "grow-n2", n1.BindAddr)
	pair := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(pair, 2, 10*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}

	// --- populate 200 keys via n1 (ring routes to whichever owner) ---
	const total = 200
	keys := putN(t, n1.Cluster, "grow", total)

	before := perNodeKeyCount(t, []*testNode{n1, n2})
	t.Logf("pre-growth distribution (n=2): %v", before)
	if before["grow-n1"]+before["grow-n2"] != total {
		t.Fatalf("pre-growth key count mismatch: %d total, got %v", total, before)
	}

	// --- add a third node + wait for membership convergence ---
	n3 := startTestNode(t, "grow-n3", n1.BindAddr)
	trio := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(trio, 3, 10*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}

	// --- wait for v0.3 rebalance to catch the data state up ---
	// Budget: spec's T_settle (5s) + streaming window. 20s is loose
	// enough for the cluster integration's settle + sweep to land
	// without making the test feel slow; tight enough that a missing
	// integration fails fast.
	if err := waitForRebalanceIdle(t, n1.Cluster, []*testNode{n1, n2, n3}, keys, 20*time.Second); err != nil {
		after := perNodeKeyCount(t, []*testNode{n1, n2, n3})
		t.Fatalf("post-growth rebalance did not converge: %v\npost-distribution: %v\n(if n3 has 0 keys + n1/n2 still hold all 200, the cluster-layer v0.3 rebalance hook is not wired off membership joins yet)", err, after)
	}

	after := perNodeKeyCount(t, []*testNode{n1, n2, n3})
	t.Logf("post-growth distribution (n=3): %v", after)

	// --- assertion 1: keys distributed across all 3 nodes ---
	if after["grow-n3"] == 0 {
		t.Fatalf("post-growth: n3 holds zero keys; rebalance never delivered any partition to the new node. distribution=%v", after)
	}
	zeros := 0
	for _, v := range after {
		if v == 0 {
			zeros++
		}
	}
	if zeros > 0 {
		t.Fatalf("post-growth: at least one node holds zero keys; distribution=%v", after)
	}

	// --- assertion 2: rough balance (within 2x of mean) ---
	mean := float64(total) / 3.0
	for id, v := range after {
		if float64(v) > 2*mean || float64(v) < 0.5*mean {
			t.Fatalf("post-growth: node %s holds %d keys, expected within 2x of mean %.1f. distribution=%v",
				id, v, mean, after)
		}
	}

	// --- assertion 3: every key still gettable via every node ---
	assertAllGettable(t, trio, keys)
}
