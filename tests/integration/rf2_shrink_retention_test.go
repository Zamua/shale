package integration

// Symmetric (shrink + over-retention) coverage for the replica-aware
// rebalance fix landed alongside rf2_membership_change_test.go.
//
// rf2_membership_change_test.go pins the GROW/join direction: at N=2 R=2
// the founder must KEEP every key it still replica-owns when a joiner
// arrives (the DELETE side of the generalization), and the joiner must
// backfill a full replica (the DELIVER side).
//
// The two tests here pin the other two corners of the same fix:
//
//  1. TestRF2_NoKeyDropsBelowReplicationFactorOnLeave - the SHRINK
//     direction. A 3-node R=2 cluster loses a node; after rebalance
//     settles every key must still (a) be readable, (b) live on R=2
//     physical replicas among the survivors (no key silently demoted to
//     a single copy), (c) lose nothing that was acked. This is the
//     mirror image of the join case: when a holder leaves, a survivor
//     that newly enters LocateKeyN(key, R) must RECEIVE the key so the
//     replica count is restored.
//
//  2. TestRF2_OverRetention_GrowTwoToThree - the negative control for
//     the DELETE side. The fix must NOT swing the other way into
//     unbounded retention: a node that is genuinely NO LONGER in a
//     key's replica set must still delete it. Grow 2->3 at R=2 and
//     assert the total physical copy count converges to N*R (== N*2),
//     not N*nodes (== N*3). If every node kept every key the fix would
//     have leaked: replica-blind-in-the-other-direction.

import (
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
)

// physicalHolders returns, for each key, the set of node IDs whose
// backend physically holds that key (direct backend Get, under the
// routing layer). Used to assert replica COUNT, not just reachability.
func physicalHolders(t *testing.T, nodes []*testNode, keys []string) map[string]map[string]bool {
	t.Helper()
	out := make(map[string]map[string]bool, len(keys))
	for _, k := range keys {
		holders := make(map[string]bool)
		for _, n := range nodes {
			v, err := n.Backend.Get([]byte(k))
			if err == nil && v != nil {
				holders[n.ID] = true
			}
		}
		out[k] = holders
	}
	return out
}

// totalPhysicalCopies sums per-node physical key counts across nodes
// for the given key set (a key on 2 nodes counts twice). Used to assert
// the cluster converged to exactly N*R copies (no over-retention, no
// under-replication).
func totalPhysicalCopies(holders map[string]map[string]bool) int {
	total := 0
	for _, h := range holders {
		total += len(h)
	}
	return total
}

func TestRF2_NoKeyDropsBelowReplicationFactorOnLeave(t *testing.T) {
	// 3-node R=2 cluster. Every key has a replica set of exactly 2 of
	// the 3 nodes. When one node leaves, each key whose replica set
	// included the departed node must re-replicate onto the survivor
	// that newly enters its replica set - so it stays at 2 copies, not
	// 1. With R=2 and 3 survivors -> 2 survivors, EVERY key's post-leave
	// replica set is "both survivors" (LocateKeyN(key,2) over 2 members
	// = both), so after settle every survivor must hold every key.
	n1 := startTestNodeWithReplication(t, "rf2s-n1", "", 2, cluster.WriteQuorum, cluster.ReadQuorum)
	n2 := startTestNodeWithReplication(t, "rf2s-n2", n1.BindAddr, 2, cluster.WriteQuorum, cluster.ReadQuorum)
	n3 := startTestNodeWithReplication(t, "rf2s-n3", n1.BindAddr, 2, cluster.WriteQuorum, cluster.ReadQuorum)
	trio := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	// waitForClusterReady (not just membership convergence) so the first
	// R=2 write does not race a peer that has not yet become ack-ready.
	waitForClusterReady(t, trio, 15*time.Second)

	const total = 200
	keys := seedN(t, n1.Cluster, "rf2s", total)

	// Sanity: at R=2 across 3 nodes every key starts on exactly 2
	// physical replicas (let the writes settle first).
	all3 := []*testNode{n1, n2, n3}
	if err := waitForReplicaCount(t, all3, keys, 2, 15*time.Second); err != nil {
		t.Fatalf("precondition (pre-leave R=2 replication): %v", err)
	}

	const doomed = "rf2s-n3"
	t.Logf("closing %s; survivors must restore every key to R=2 (= both survivors)", doomed)
	n3.Close()

	survivors := []*testNode{n1, n2}
	pair := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(pair, 2, 10*time.Second); err != nil {
		t.Fatalf("post-leave convergence: %v", err)
	}

	// After the leave + rebalance settles, every key must again live on
	// R=2 physical replicas. At N=2 R=2 that means BOTH survivors hold
	// every key. Poll until the replica count is restored.
	if err := waitForReplicaCount(t, survivors, keys, 2, 25*time.Second); err != nil {
		holders := physicalHolders(t, survivors, keys)
		under := 0
		var firstUnder string
		for _, k := range keys {
			if len(holders[k]) < 2 {
				under++
				if firstUnder == "" {
					firstUnder = k
				}
			}
		}
		dist := perNodeKeyCount(t, survivors)
		t.Fatalf("UNDER-REPLICATION on leave: %d/%d keys hold < R=2 physical copies after %s left "+
			"(first: %s held by %v). The departing replica's slot was not refilled on the survivor that "+
			"newly entered LocateKeyN(key,2) = the symmetric (shrink) half of the replica-blind bug. "+
			"survivor distribution=%v; underlying: %v", under, total, doomed, firstUnder,
			keysOf(holders[firstUnder]), dist, err)
	}

	// (a) every key still readable via both survivors, (b) no acked key
	// lost: assertAllGettable fails on the first missing/wrong value.
	assertAllGettable(t, pair, keys)

	// (b/c) explicit: every key holds exactly R=2 physical copies among
	// the survivors (both survivors hold it), total copies == N*R.
	holders := physicalHolders(t, survivors, keys)
	for _, k := range keys {
		if len(holders[k]) != 2 {
			t.Errorf("key %s holds %d physical copies post-leave, want 2; holders=%v",
				k, len(holders[k]), keysOf(holders[k]))
		}
	}
	if got, want := totalPhysicalCopies(holders), total*2; got != want {
		t.Errorf("total physical copies post-leave = %d, want N*R = %d", got, want)
	}
	t.Logf("post-leave distribution: %v (every key restored to R=2 across survivors)",
		perNodeKeyCount(t, survivors))
}

func TestRF2_OverRetention_GrowTwoToThree(t *testing.T) {
	// Negative control for the DELETE side. A node that genuinely leaves
	// a key's replica set MUST still delete it: the fix must not turn
	// into unbounded retention. At N=2 R=2 both nodes hold all keys; grow
	// to N=3 R=2 and now each key's replica set is only 2 of the 3 nodes,
	// so the node that fell OUT of LocateKeyN(key,2) must drop the key.
	// Total physical copies must converge to N*R (== total*2), NOT
	// total*3 (every node keeping every key = the over-retention leak).
	// Write into the FOUNDER while it is alone: at R=2 with one member,
	// LocateKeyN(key,2) is the single founder, so a quorum write needs
	// just one ack and cannot race a peer that has not yet become
	// ack-ready (the transient "needed 2 acks, got 1" startup flake). The
	// joiner then backfills via reconcile so both hold all keys, exactly
	// the precondition this test needs - reached without a 2-node write.
	n1 := startTestNodeWithReplication(t, "rf2o-n1", "", 2, cluster.WriteQuorum, cluster.ReadQuorum)

	const total = 200
	keys := putN(t, n1.Cluster, "rf2o", total)

	// Joiner arrives + backfills. Precondition: at N=2 R=2 both nodes
	// hold every key (total*2 copies).
	n2 := startTestNodeWithReplication(t, "rf2o-n2", n1.BindAddr, 2, cluster.WriteQuorum, cluster.ReadQuorum)
	pair := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(pair, 2, 10*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	two := []*testNode{n1, n2}
	if err := waitForReplicaCount(t, two, keys, 2, 20*time.Second); err != nil {
		t.Fatalf("precondition (N=2 R=2 both hold all after joiner backfill): %v", err)
	}

	// Grow: a third node joins.
	n3 := startTestNodeWithReplication(t, "rf2o-n3", n1.BindAddr, 2, cluster.WriteQuorum, cluster.ReadQuorum)
	trio := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(trio, 3, 10*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}

	all3 := []*testNode{n1, n2, n3}
	// After settle, each key must hold EXACTLY R=2 copies. Some keys'
	// replica sets moved to include n3 and drop n1 or n2 - that drop is
	// a REQUIRED delete. Total converges to total*2.
	if err := waitForReplicaCount(t, all3, keys, 2, 25*time.Second); err != nil {
		holders := physicalHolders(t, all3, keys)
		over, under := 0, 0
		for _, k := range keys {
			switch {
			case len(holders[k]) > 2:
				over++
			case len(holders[k]) < 2:
				under++
			}
		}
		t.Fatalf("post-grow replica count not == R=2: %d keys over-retained (>2 copies), %d under (<2). "+
			"over-retention means a node that left LocateKeyN(key,2) failed to delete = the fix leaking "+
			"in the opposite direction. distribution=%v; underlying: %v",
			over, under, perNodeKeyCount(t, all3), err)
	}

	// Hard assertion: total physical copies == N*R, not N*nodes.
	holders := physicalHolders(t, all3, keys)
	if got, want, leak := totalPhysicalCopies(holders), total*2, total*3; got != want {
		t.Errorf("total physical copies post-grow = %d, want N*R = %d (over-retention leak would be N*nodes = %d)",
			got, want, leak)
	}

	// Each node holds only the keys it replica-owns per the settled ring.
	r := ring.New()
	for _, m := range n1.Cluster.Members() {
		r.Add(m)
	}
	for _, k := range keys {
		want := make(map[string]bool)
		for _, m := range r.LocateKeyN([]byte(k), 2) {
			want[m.ID] = true
		}
		got := holders[k]
		if len(got) != 2 {
			t.Errorf("key %s held by %v (want the 2-node replica set %v)", k, keysOf(got), keysOf(want))
			continue
		}
		for id := range got {
			if !want[id] {
				t.Errorf("key %s physically held by %s which is NOT in its replica set %v (over-retention: stale copy not swept)",
					k, id, keysOf(want))
			}
		}
	}

	assertAllGettable(t, trio, keys)
	t.Logf("post-grow distribution: %v (each key on exactly its R=2 replica set, total=%d copies)",
		perNodeKeyCount(t, all3), totalPhysicalCopies(holders))
}

// waitForReplicaCount polls until every key in keys holds exactly want
// physical copies across nodes, or the deadline expires. Returns nil on
// convergence. Used to wait out the rebalance settle/handoff/sweep
// window before asserting the steady-state replica count.
func waitForReplicaCount(t *testing.T, nodes []*testNode, keys []string, want int, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastBad int
	for time.Now().Before(deadline) {
		holders := physicalHolders(t, nodes, keys)
		bad := 0
		for _, k := range keys {
			if len(holders[k]) != want {
				bad++
			}
		}
		if bad == 0 {
			return nil
		}
		lastBad = bad
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("%d/%d keys did not reach exactly %d physical replicas within %s",
		lastBad, len(keys), want, timeout)
}

// seedN is putN for the steady-state SEEDING write at the top of an R>1
// test, where a freshly-converged multi-node cluster can briefly reject a
// quorum write with Unavailable ("needed N acks, got M") because a peer
// has not yet become ack-ready. putWithRetry deliberately does NOT retry
// Unavailable (it reserves that code for genuine peer-down in the
// replication fanout), so the seeding path needs its own bounded retry
// that also tolerates that transient startup window. Used only for the
// initial seed, never for assertions about live failure handling.
func seedN(t *testing.T, c *cluster.Cluster, prefix string, n int) []string {
	t.Helper()
	keys := make([]string, n)
	for i := range n {
		k := fmt.Sprintf("%s-%04d", prefix, i)
		var lastErr error
		ok := false
		for range 100 {
			err := c.Put([]byte(k), []byte(expectedValue(k)))
			if err == nil {
				ok = true
				break
			}
			lastErr = err
			time.Sleep(100 * time.Millisecond)
		}
		if !ok {
			t.Fatalf("seed Put %s: %v", k, lastErr)
		}
		keys[i] = k
	}
	return keys
}

// keysOf turns a set into a sorted-ish slice for log output (order not
// guaranteed; for human-readable failure messages only).
func keysOf(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}
