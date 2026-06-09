package integration

// Shrink scenario: 3-node cluster loses one node, the survivors take
// over its keys via v0.3 migration.
//
// R=1 caveat: the spec's "Failure handling" section is explicit that
// at R=1, keys whose ONLY copy lived on the closed node are gone (no
// replica to promote). So this test cannot assert "all 300 keys still
// gettable" the way the growth test does. Instead, it:
//
//   1. Pre-computes which node will be closed.
//   2. Writes keys whose ring owner is one of the SURVIVING nodes
//      (not the doomed node), so the test set has at-least-one
//      surviving owner BEFORE the close.
//   3. After the close + rebalance, asserts every test-set key is
//      still gettable, and that the survivors share the keyspace.
//
// A clean shrink also needs the survivors to migrate keys among
// themselves: when n3 leaves, partitions that were on n1 may now hash
// to n2 (or vice versa) under the bounded-loads ring. Test does not
// pin which keys move, only that the end state matches the new ring
// + every key is readable.

import (
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
)

func TestRebalance_ShrinkThreeToTwo(t *testing.T) {
	t.Parallel()

	// --- bring up 3-node cluster ---
	n1 := startTestNode(t, "shrink-n1", "")
	n2 := startTestNode(t, "shrink-n2", n1.BindAddr)
	n3 := startTestNode(t, "shrink-n3", n1.BindAddr)
	trio := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(trio, 3, 10*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}

	const doomed = "shrink-n3"

	// --- write 300 keys, keeping only those whose CURRENT owner is
	// not the doomed node. This gives the test set the property "owned
	// by a survivor before the close," so the close cannot strand the
	// only copy at R=1.
	candidates := putN(t, n1.Cluster, "shr", 300)

	// Build a snapshot ring matching n1's pre-shrink view + filter.
	r := ring.New()
	for _, m := range n1.Cluster.Members() {
		r.Add(m)
	}
	survivorOwned := make([]string, 0, len(candidates))
	for _, k := range candidates {
		if r.LocateKey([]byte(k)).ID != doomed {
			survivorOwned = append(survivorOwned, k)
		}
	}
	if len(survivorOwned) < 50 {
		t.Fatalf("expected substantial survivor-owned set, got %d of %d (ring distribution suspect)",
			len(survivorOwned), len(candidates))
	}
	t.Logf("survivor-owned keys: %d / %d (rest were on %s and are R=1-lost on close)",
		len(survivorOwned), len(candidates), doomed)

	// --- close the doomed node ---
	// Graceful Close broadcasts the leave; the survivors should
	// converge to a 2-member ring.
	n3.Close()
	pair := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(pair, 2, 10*time.Second); err != nil {
		t.Fatalf("post-leave convergence: %v", err)
	}

	// --- wait for rebalance to redistribute among the survivors ---
	if err := waitForRebalanceIdle(t, n1.Cluster, []*testNode{n1, n2}, survivorOwned, 20*time.Second); err != nil {
		after := perNodeKeyCount(t, []*testNode{n1, n2})
		t.Fatalf("post-shrink rebalance did not converge for survivor-owned set: %v\nsurvivor distribution: %v\n(if every survivor-owned key still lives on its old owner with no movement, the cluster-layer rebalance hook is not firing on membership leaves)",
			err, after)
	}

	// --- assertion 1: every survivor-owned key is gettable via both
	// surviving cluster handles. (Use Get rather than the per-backend
	// peek because Get exercises the public routing path.) ---
	assertAllGettable(t, pair, survivorOwned)

	// --- assertion 2: survivors share the keyspace; neither holds
	// zero keys. ---
	after := perNodeKeyCount(t, []*testNode{n1, n2})
	t.Logf("post-shrink distribution: %v", after)
	for _, n := range []*testNode{n1, n2} {
		if after[n.ID] == 0 {
			t.Fatalf("post-shrink: survivor %s holds zero keys; distribution=%v (one node may have absorbed every range, which is a routing or rebalance bug under bounded-loads)",
				n.ID, after)
		}
	}

	// --- assertion 3 (sanity): no survivor-owned key is missing ---
	missing := 0
	for _, k := range survivorOwned {
		_, err := n1.Cluster.Get([]byte(k))
		if err != nil {
			missing++
		}
	}
	if missing > 0 {
		t.Fatalf("post-shrink: %d/%d survivor-owned keys missing via n1 (rebalance did not preserve survivor data)",
			missing, len(survivorOwned))
	}

	// Document why we don't assert on the doomed-owned subset:
	// at R=1 those keys live on exactly one node, and that node is
	// gone. v0.4 replication is the planned remedy. This test will
	// need to add an "all 300 keys gettable" assertion when R>=2 is
	// the default.
}
