package integration

// THE JOIN-AFTER-RESHARD GENERATION GATE for the v0.8 join-after-reshard fix
// (a focused data-loss oracle, complementary to the chaos/soak harness).
//
// THE PROPERTY THIS PINS:
//
//	A NODE THAT JOINS AN ALREADY-RESHARDED CLUSTER MUST ARRIVE AT THE LIVE
//	GENERATION AND LOSE NO ACKED WRITE.
//
// Routing is generation-qualified end to end: a key resolves to GenUnit{gen,
// UnitForHash(h, count)} and the ring places a unit by hashing the generation
// AHEAD of the unit id, so the gen-0 id and the live gen-g id of the same key
// hash to DIFFERENT ring positions (and potentially different owners). A node
// that boots at generation 0 after the cluster has resharded therefore routes
// every key to the wrong place: as an originator it forwards to whichever node
// owns its gen-0 unit, and that node - routing at gen g - disclaims it
// ("forwarding loop refused: this node does not own the key"). The acked write
// becomes unreachable, and nothing in the steady-state machinery raises a
// passive joiner's generation, so the loss does not self-heal.
//
// The fix: a multi-backend joiner learns the live {generation, unit-count}
// BEFORE it mounts any unit - from the __cluster/init durable marker when a
// ConditionalStore is wired (this test's shape: a multi-node reshard requires
// the store), or from a seed's GenState RPC otherwise - and seeds its routing
// state from the answer, deferring while a reshard is in flight. This test
// exercises the path end to end: stand up a multi-node cluster, write a
// recorded dataset, reshard it (so the live generation is g >= 1), then ADD a
// node and assert
//
//  1. THE JOINER LEARNED THE LIVE GENERATION. Its GenStateSnapshot() reports
//     the post-reshard generation + unit-count, not gen 0 / N.
//  2. THE ORACLE (zero loss). Every acked key written before the join is still
//     readable, with its EXACT recorded value, FROM THE JOINER (via gRPC
//     forwarding through the joiner's own routing) after the post-join
//     reconcile settles. A gen-0 joiner would route these to the wrong owner
//     and surface the "does not own the key" loss.
//  3. THE JOINER SERVES NEW WRITES. A write routed THROUGH the joiner after it
//     joined is durable + readable, proving the joiner participates in routing
//     at the live generation rather than orphaning at gen 0.

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

func TestJoinAfterReshard_JoinerLearnsGenerationNoLoss(t *testing.T) {
	const unitCount = 8 // N; one reshard doubles it to 16 at gen 1
	backing := sharedfactory.NewBacking()
	store := storageunit.NewMemConditionalStore()

	// --- Stand up 2 nodes, multi-backend, N=8 units, sharing one conditional
	// store (the arbiter-driven multi-node reshard requires it), and converge. ---
	n1 := startR1StoreNode(t, "jar1", "", unitCount, backing, store)
	n2 := startR1StoreNode(t, "jar2", n1.ClusterToken, unitCount, backing, store)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster}, 2, 20*time.Second); err != nil {
		t.Fatalf("initial 2-node convergence: %v", err)
	}
	// Let the join-driven reconcile settle so ownership is stable before the
	// reshard snapshots the participant set.
	time.Sleep(900 * time.Millisecond)

	// --- Write a recorded dataset spanning many units. ---
	want := make(map[string][]byte)
	const nKeys = 300
	for i := range nKeys {
		k := fmt.Sprintf("rec-%05d", i)
		v := fmt.Appendf(nil, "val-%05d-payload", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 10*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	// --- RESHARD the 2-node cluster: N=8 -> 16 at gen 1, cluster-wide (the
	// delegated arbiter flow; the pump stands in for the reconcile cadence). ---
	stopPump := startReconcilePump([]*cluster.Cluster{n1.Cluster, n2.Cluster})
	err := n1.Cluster.Reshard()
	stopPump()
	if err != nil {
		t.Fatalf("multi-node Reshard (delegated, n1): %v", err)
	}
	// Let the post-finalize redistribution reconcile settle.
	time.Sleep(900 * time.Millisecond)

	// Sanity: the existing members are now at gen 1, count 16.
	wantGen, wantCount := n1.Cluster.GenStateSnapshot()
	if wantGen != 1 {
		t.Fatalf("after one reshard the cluster should be at gen 1, got gen %d", wantGen)
	}
	if wantCount != 2*unitCount {
		t.Fatalf("after one reshard the unit count should be %d, got %d", 2*unitCount, wantCount)
	}
	// The pre-join dataset must still be readable from the resharded members.
	readAcrossNodes(t, []*sharedNode{n1, n2}, "rec-00000", want["rec-00000"])

	// --- ADD A NODE to the already-resharded cluster (THE FIX UNDER TEST). ---
	// The joiner must learn {gen 1, count 16} (here: from the __cluster/init
	// marker in the shared store) before it mounts any unit, so it never routes
	// / owns a key at gen 0. seedAddr is n1's bind.
	n3 := startR1StoreNode(t, "jar3", n1.ClusterToken, unitCount, backing, store)
	nodes := []*sharedNode{n1, n2, n3}
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence after join: %v", err)
	}
	// Let the join-driven reconcile redistribute the gen-1 units to the joiner.
	time.Sleep(1200 * time.Millisecond)

	// PROPERTY 1: the joiner learned the LIVE generation, not gen 0.
	gotGen, gotCount := n3.Cluster.GenStateSnapshot()
	if gotGen != wantGen {
		t.Fatalf("PROPERTY VIOLATED: joiner started at gen %d, want the live gen %d "+
			"(a gen-0 joiner would orphan acked writes)", gotGen, wantGen)
	}
	if gotCount != wantCount {
		t.Fatalf("PROPERTY VIOLATED: joiner reports unit-count %d, want the live count %d", gotCount, wantCount)
	}

	// PROPERTY 2 (THE ORACLE): every acked pre-join key is still readable FROM
	// THE JOINER with its exact value. A gen-0 joiner would forward these to the
	// wrong owner and surface the "does not own the key" loss.
	for k, v := range want {
		got, err := getWithRetryUnavailable(t, n3.Cluster, k, 10*time.Second)
		if err != nil {
			t.Fatalf("ORACLE VIOLATED [LOST]: key %q unreadable via the JOINER %s after join-into-resharded-cluster: %v "+
				"(NO ACKED WRITE LOST)", k, n3.ID, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("ORACLE VIOLATED: key %q via the JOINER %s = %q, want %q (value corrupted)", k, n3.ID, got, v)
		}
	}

	// PROPERTY 3: a NEW write routed THROUGH the joiner is durable + readable
	// from every node, proving the joiner routes at the live generation.
	const newKey = "post-join-key"
	newVal := []byte("post-join-value")
	if err := putWithRetryUnavailable(t, n3.Cluster, newKey, string(newVal), 10*time.Second); err != nil {
		t.Fatalf("write through the joiner after join: %v", err)
	}
	for _, n := range nodes {
		got, err := getWithRetryUnavailable(t, n.Cluster, newKey, 10*time.Second)
		if err != nil {
			t.Fatalf("post-join write %q unreadable via node %s: %v", newKey, n.ID, err)
		}
		if !bytes.Equal(got, newVal) {
			t.Fatalf("post-join write %q via node %s = %q, want %q", newKey, n.ID, got, newVal)
		}
	}

	// Belt + suspenders: the live unit-count derived from the generation matches
	// the doubling (the count carried over the wire, not re-derived locally).
	if got := storageunit.MustUnitCount(int(gotCount)); got.N() != 2*unitCount {
		t.Fatalf("joiner unit-count %s does not match the doubled count", got)
	}
}
