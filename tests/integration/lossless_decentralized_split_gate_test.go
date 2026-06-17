package integration

// THE LOSSLESS DECENTRALIZED-SPLIT GATE (v0.9). A 3-node R=2 cluster declares a
// doubled shard count (Retarget the CAS reshard epoch) and the cluster reshards
// ONLINE - no coordinator, no cluster-wide freeze - while a continuous writer
// keeps acking writes through the whole split. Child mounts are STAGGERED across
// nodes (SetAcquireDelay) so nodes observe each unit's durable cut-over marker
// and flip in different orders, exercising the cross-node flip window the freeze
// used to eliminate.
//
// The gate asserts: (1) THE ORACLE - every baseline + acked key is readable with
// its exact value from EVERY node, ZERO acked loss; (2) the ack rate stayed high
// through the split; (3) every node reached gen 1 (2N units). It is kept honest
// by a BREAK-DEMO (TestingForceUncleanReshard: flip + retire parents WITHOUT
// copying their data into the children) that asserts the oracle CATCHES the loss.

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/reshard"
	"github.com/Zamua/shale/pkg/storageunit"
)

type splitGateResult struct {
	attempted int64
	acked     int64
	want      map[string][]byte
	nodes     []*sharedNode
}

const splitAckThreshold = 0.85

// allAtGeneration reports whether every node's live genState is exactly (gen,
// count).
func allAtGeneration(nodes []*sharedNode, gen uint64, count uint32) bool {
	for _, n := range nodes {
		g, c := n.Cluster.GenStateSnapshot()
		if g != gen || c != count {
			return false
		}
	}
	return true
}

func genStatesOf(nodes []*sharedNode) string {
	var b []byte
	for _, n := range nodes {
		g, c := n.Cluster.GenStateSnapshot()
		b = fmt.Appendf(b, "%s=g%d/n%d ", n.ID, g, c)
	}
	return string(b)
}

// pumpReconciles drives one reconcile tick on every node (the periodic self-heal
// loop, accelerated for the test). When concurrent is true it drives all nodes'
// reconciles SIMULTANEOUSLY (each on its own goroutine, as production does), so
// two nodes can be in finalizeParent (pause + cross-node forward) at the same
// instant - the cross-node-concurrency dimension a serial pump never exercises.
func pumpReconciles(nodes []*sharedNode, concurrent bool) {
	if !concurrent {
		for _, n := range nodes {
			n.Cluster.TestingRunReconcile()
		}
		return
	}
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n *sharedNode) {
			defer wg.Done()
			n.Cluster.TestingRunReconcile()
		}(n)
	}
	wg.Wait()
}

type splitGateOpts struct {
	forceUnclean        bool          // break-demo: flip + retire without copying
	writeOne            bool          // WriteOne (single-slot ack) - the harshest loss regime
	finalizeDelay       time.Duration // widen the finalize retire window
	startCount          int           // initial unit count (default 4)
	targetCount         int           // declared count to converge to (default 2*startCount, a split)
	concurrentReconcile bool          // drive all nodes' reconciles simultaneously (cross-node overlap)
}

// runDecentralizedReshard drives a live, online reshard (split OR merge per
// start/target count) on a 3-node R=2 cluster under continuous writes + staggered
// child/survivor mounts, and returns the ack tally + the want set (baseline +
// acked) + the nodes for the loss oracle.
func runDecentralizedReshard(t *testing.T, opts splitGateOpts) splitGateResult {
	t.Helper()
	unitCount := opts.startCount
	if unitCount == 0 {
		unitCount = 4
	}
	targetCount := opts.targetCount
	if targetCount == 0 {
		targetCount = 2 * unitCount
	}
	backing := sharedfactory.NewBacking()
	store := storageunit.NewMemConditionalStore()
	mutate := func(cfg *cluster.Config) {
		cfg.ConditionalStore = store
		cfg.TestingForceUncleanReshard = opts.forceUnclean
		cfg.TestingFinalizeRetireDelay = opts.finalizeDelay
		if opts.writeOne {
			cfg.WriteConsistency = cluster.WriteOne
			cfg.ReadConsistency = cluster.ReadAll
		}
	}

	n1 := startReplicatedNodeCfg(t, "dsp-a", "", unitCount, 2, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "dsp-b", n1.BindAddr, unitCount, 2, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "dsp-c", n1.BindAddr, unitCount, 2, backing, mutate)
	nodes := []*sharedNode{n1, n2, n3}
	cs := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(cs, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)

	// Baseline (pre-split, single-generation on the parents). These MUST survive
	// the split by being copied into the children.
	want := make(map[string][]byte)
	for i := range 80 {
		k := fmt.Sprintf("dspbase-%05d", i)
		v := fmt.Appendf(nil, "dspval-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 15*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	var (
		mu        sync.Mutex
		ackedKeys = make(map[string][]byte)
		attempted atomic.Int64
		acked     atomic.Int64
		stop      atomic.Bool
		wg        sync.WaitGroup
	)
	const writers = 4
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				entry := nodes[(w+i)%len(nodes)]
				k := fmt.Sprintf("dspprobe-%d-%07d", w, i)
				v := fmt.Appendf(nil, "dsppv-%d-%07d", w, i)
				attempted.Add(1)
				// Put DIRECTLY (no external retry) so the ack rate reflects the
				// cluster's INTERNAL availability through the online split.
				if err := entry.Cluster.Put([]byte(k), []byte(v)); err == nil {
					acked.Add(1)
					mu.Lock()
					ackedKeys[k] = v
					mu.Unlock()
				}
				time.Sleep(3 * time.Millisecond)
			}
		}(w)
	}

	// Staggered child-mount timing across nodes: nodes reach caught-up + observe
	// the cut-over markers in different orders (the durability lens's adversary).
	n1.Handle.SetAcquireDelay(0)
	n2.Handle.SetAcquireDelay(80 * time.Millisecond)
	n3.Handle.SetAcquireDelay(160 * time.Millisecond)

	// Declare the target shard count (the whole point: declarative).
	arb := reshard.NewArbiter(store)
	if _, err := arb.Retarget(storageunit.MustUnitCount(targetCount)); err != nil {
		t.Fatalf("retarget: %v", err)
	}

	// Drive the reshard to completion by pumping reconciles until every node is at
	// gen 1 (targetCount units), bounded.
	deadline := time.Now().Add(45 * time.Second)
	for !allAtGeneration(nodes, 1, uint32(targetCount)) {
		pumpReconciles(nodes, opts.concurrentReconcile)
		if time.Now().After(deadline) {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("split did not converge to gen 1: %s", genStatesOf(nodes))
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Keep writing briefly past the flip, then stop.
	time.Sleep(400 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	// Drop the mount delay and let the post-flip redistribution settle.
	for _, n := range nodes {
		n.Handle.SetAcquireDelay(0)
	}
	for range 60 {
		pumpReconciles(nodes, opts.concurrentReconcile)
		time.Sleep(25 * time.Millisecond)
	}

	mu.Lock()
	for k, v := range ackedKeys {
		want[k] = v
	}
	mu.Unlock()

	return splitGateResult{
		attempted: attempted.Load(),
		acked:     acked.Load(),
		want:      want,
		nodes:     nodes,
	}
}

// TestDecentralizedSplitGate is the gate: a live, online 4 -> 8 split under
// continuous writes + staggered cut-over loses ZERO acked writes and stays
// available.
func TestDecentralizedSplitGate(t *testing.T) {
	r := runDecentralizedReshard(t, splitGateOpts{})

	// (1) THE ORACLE: zero acked loss, readable from every node.
	assertNoAckedLoss(t, overlapGateResult{want: r.want, nodes: r.nodes})

	// (2) Availability: the ack rate stayed high through the split.
	if r.attempted == 0 {
		t.Fatalf("no writes attempted")
	}
	rate := float64(r.acked) / float64(r.attempted)
	if rate < splitAckThreshold {
		t.Fatalf("ack rate %.3f below %.2f through the split", rate, splitAckThreshold)
	}

	// (3) Every node reached gen 1 (2N units).
	for _, n := range r.nodes {
		g, c := n.Cluster.GenStateSnapshot()
		if g != 1 || c != 2*4 {
			t.Fatalf("node %s ended at gen %d count %d, want gen 1 count 8", n.ID, g, c)
		}
	}
	t.Logf("decentralized split: %d/%d acked (%.1f%%), zero acked loss, all nodes at gen 1",
		r.acked, r.attempted, 100*rate)
}

// TestDecentralizedSplitGate_BreakDemoCatchesLoss keeps the gate honest: with the
// copy + caught-up safeguard disabled (flip + retire parents WITHOUT copying
// their data), the split drops acked writes and the oracle MUST catch it. If this
// run ever passed the loss check, the gate above would be rubber-stamping.
func TestDecentralizedSplitGate_BreakDemoCatchesLoss(t *testing.T) {
	r := runDecentralizedReshard(t, splitGateOpts{forceUnclean: true})

	lost := 0
	for k, v := range r.want {
		got, err := getWithRetryUnavailable(t, r.nodes[0].Cluster, k, 5*time.Second)
		if err != nil || !bytes.Equal(got, v) {
			lost++
		}
	}
	if lost == 0 {
		t.Fatalf("BREAK NOT CAUGHT: an unclean reshard (no copy) lost no acked write - the oracle would rubber-stamp")
	}
	t.Logf("break-demo: unclean reshard lost %d/%d acked keys (oracle correctly catches it)", lost, len(r.want))
}

// TestDecentralizedSplitGate_WriteOneAcrossFinalize is the harshest durability
// regime, targeting the P0 the adversarial review found: WriteOne (a single
// parent-slot ack is enough) + a WIDENED finalize retire window
// (TestingFinalizeRetireDelay) + writers running THROUGH the staggered finalize.
// A parent-leg write that lands after a parent's final copy but before it retires
// would be lost WITHOUT the per-unit finalize write-quiesce; with it, every acked
// write is captured into the child or rejected (re-routed), so loss stays ZERO.
// (Run against the un-quiesced finalize this loses writes; it is the end-to-end
// counterpart of the white-box TestFinalizeSplit_QuiescesParentWrites.)
func TestDecentralizedSplitGate_WriteOneAcrossFinalize(t *testing.T) {
	r := runDecentralizedReshard(t, splitGateOpts{writeOne: true, finalizeDelay: 40 * time.Millisecond})

	assertNoAckedLoss(t, overlapGateResult{want: r.want, nodes: r.nodes})
	for _, n := range r.nodes {
		g, c := n.Cluster.GenStateSnapshot()
		if g != 1 || c != 2*4 {
			t.Fatalf("node %s ended at gen %d count %d, want gen 1 count 8", n.ID, g, c)
		}
	}
	rate := float64(r.acked) / float64(r.attempted)
	t.Logf("WriteOne-across-finalize: %d/%d acked (%.1f%%), zero acked loss through a widened finalize window",
		r.acked, r.attempted, 100*rate)
}

// TestDecentralizedMergeGate is the merge-direction gate: a live, online 8 -> 4
// merge (two parents collapse into one survivor at its gen-(g+1) ring home, a
// CROSS-NODE two-source copy) under continuous writes + staggered timing loses
// ZERO acked writes and converges every node to gen 1 (4 units).
func TestDecentralizedMergeGate(t *testing.T) {
	r := runDecentralizedReshard(t, splitGateOpts{startCount: 8, targetCount: 4})

	assertNoAckedLoss(t, overlapGateResult{want: r.want, nodes: r.nodes})
	if r.attempted == 0 {
		t.Fatalf("no writes attempted")
	}
	rate := float64(r.acked) / float64(r.attempted)
	if rate < splitAckThreshold {
		t.Fatalf("ack rate %.3f below %.2f through the merge", rate, splitAckThreshold)
	}
	for _, n := range r.nodes {
		g, c := n.Cluster.GenStateSnapshot()
		if g != 1 || c != 4 {
			t.Fatalf("node %s ended at gen %d count %d, want gen 1 count 4", n.ID, g, c)
		}
	}
	t.Logf("decentralized merge: %d/%d acked (%.1f%%), zero acked loss, all nodes at gen 1 (4 units)",
		r.acked, r.attempted, 100*rate)
}

// TestDecentralizedMergeGate_BreakDemoCatchesLoss keeps the merge gate honest:
// flip + retire parents WITHOUT copying their data into the survivor drops acked
// writes, which the oracle must catch.
func TestDecentralizedMergeGate_BreakDemoCatchesLoss(t *testing.T) {
	r := runDecentralizedReshard(t, splitGateOpts{startCount: 8, targetCount: 4, forceUnclean: true})

	lost := 0
	for k, v := range r.want {
		got, err := getWithRetryUnavailable(t, r.nodes[0].Cluster, k, 5*time.Second)
		if err != nil || !bytes.Equal(got, v) {
			lost++
		}
	}
	if lost == 0 {
		t.Fatalf("BREAK NOT CAUGHT: an unclean merge (no copy) lost no acked write - the oracle would rubber-stamp")
	}
	t.Logf("merge break-demo: unclean merge lost %d/%d acked keys (oracle correctly catches it)", lost, len(r.want))
}

// TestDecentralizedMergeGate_WriteOneAcrossFinalize confirms the per-unit finalize
// write-quiesce also holds on the cross-node MERGE path (shared finalizeParent):
// WriteOne + a widened finalize window + writers through the flip lose ZERO acked
// writes.
func TestDecentralizedMergeGate_WriteOneAcrossFinalize(t *testing.T) {
	r := runDecentralizedReshard(t, splitGateOpts{
		startCount: 8, targetCount: 4, writeOne: true, finalizeDelay: 40 * time.Millisecond,
	})
	assertNoAckedLoss(t, overlapGateResult{want: r.want, nodes: r.nodes})
	for _, n := range r.nodes {
		g, c := n.Cluster.GenStateSnapshot()
		if g != 1 || c != 4 {
			t.Fatalf("node %s ended at gen %d count %d, want gen 1 count 4", n.ID, g, c)
		}
	}
	t.Logf("merge WriteOne-across-finalize: %d/%d acked, zero acked loss through a widened finalize window",
		r.acked, r.attempted)
}

// TestDecentralizedMergeGate_ConcurrentReconcile drives all three nodes'
// reconciles SIMULTANEOUSLY through the merge (not serially), so two nodes can be
// in finalizeParent (parent write-pause + cross-node forward into the survivor) at
// the same instant - the cross-node-concurrency dimension the serial pump cannot
// reach (the review's P1-B). Run under -race it would surface any deadlock or data
// race the merge's cross-node forwards introduce. Asserts ZERO acked loss.
func TestDecentralizedMergeGate_ConcurrentReconcile(t *testing.T) {
	r := runDecentralizedReshard(t, splitGateOpts{startCount: 8, targetCount: 4, concurrentReconcile: true})

	assertNoAckedLoss(t, overlapGateResult{want: r.want, nodes: r.nodes})
	for _, n := range r.nodes {
		g, c := n.Cluster.GenStateSnapshot()
		if g != 1 || c != 4 {
			t.Fatalf("node %s ended at gen %d count %d, want gen 1 count 4", n.ID, g, c)
		}
	}
	t.Logf("merge concurrent-reconcile: %d/%d acked, zero acked loss with all nodes finalizing concurrently",
		r.acked, r.attempted)
}
