package integration

// Handoff-cycle LATENCY pin (docs/SPEC.md "The pending owners acquire in the
// background"): with the node-wide open bound at the PRODUCTION DEFAULT (1)
// and N positions moving in a real-gossip join, the full acquire cycle must
// cost ~N x the open itself - the queued opens CHAIN event-driven off the
// permit (each next open starts the moment the previous finishes), never
// waiting a reconcile tick between opens. Pre-dissection this was the feared
// failure shape: ~2 ticks (10s) of choreography PER position on top of every
// open, turning an N-position join into minutes of serialized churn.
//
// The joiner boots against planted serving markers, so it BOOT-DEFERS every
// owned position and re-acquires them all through the bounded overlap path
// with a 1s mock open each - the exact shape of a pod restart or a join
// against live peers. The factory handle's OpenTimeline gives the per-open
// (start, end) spans the assertions read.

import (
	"sort"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

func TestHandoffCycle_BoundedAcquires_ChainWithoutTicks(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	mutate := func(cfg *cluster.Config) { cfg.WriteTimeout = 300 * time.Millisecond }

	n1 := startReplicatedNodeCfg(t, "hc-a", "", uc, rf, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "hc-b", n1.BindAddr, uc, rf, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "hc-c", n1.BindAddr, uc, rf, backing, mutate)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)

	// Plant markers for every position so the joiner boot-defers all its owned
	// positions and re-acquires them via the bounded overlap path.
	plantAllServingMarkers(t, n1.Handle, uc, rf)

	// How many positions land on the joiner under the 4-node ring: that is the
	// number of serialized 1s opens the cycle needs.
	new4 := []string{"hc-a", "hc-b", "hc-c", "hc-d"}
	moving := 0
	for u := 0; u < uc; u++ {
		if contains(replicaIDsForMembers(new4, storageunit.UnitID(u), rf), "hc-d") {
			moving++
		}
	}
	if moving < 4 {
		t.Skipf("only %d positions move onto hc-d under this hashing; need >=4 for a meaningful chain", moving)
	}

	const mountDelay = 1 * time.Second
	// PRODUCTION DEFAULT bound: OpenConcurrency 1 (override the gate helper's 8).
	n4 := startReplicatedNodeSlowAcquireCfg(t, "hc-d", n1.BindAddr, uc, rf, backing, mountDelay, 300*time.Millisecond,
		func(cfg *cluster.Config) { cfg.OpenConcurrency = 1 })
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}

	// Wait for the whole batch of overlap acquires to complete. Budget: the
	// serialized opens (moving x 1s) + generous edge-triggering slack (settle
	// delay + a couple of reconcile ticks) - NOT the per-position tick
	// compounding this test exists to rule out.
	budget := time.Duration(moving)*mountDelay + 25*time.Second
	deadline := time.Now().Add(budget)
	for len(n4.Handle.OpenTimeline()) < moving {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d moving positions opened within %v: the cycle is tick-quantized (or opens were dropped)",
				len(n4.Handle.OpenTimeline()), moving, budget)
		}
		time.Sleep(50 * time.Millisecond)
	}
	spans := n4.Handle.OpenTimeline()

	// The bound held under the real join.
	if peak := n4.Handle.MaxConcurrentOpens(); peak != 1 {
		t.Fatalf("max concurrent opens = %d, want 1 (the production default bound)", peak)
	}

	// EVENT-DRIVEN CHAINING: each next open starts within a fraction of a
	// reconcile tick of the previous open's end. A tick-quantized queue would
	// show ~5s gaps; the permit hand-off shows near-zero.
	sort.Slice(spans, func(i, j int) bool { return spans[i].Start.Before(spans[j].Start) })
	var worstGap time.Duration
	for i := 1; i < len(spans); i++ {
		gap := spans[i].Start.Sub(spans[i-1].End)
		if gap > worstGap {
			worstGap = gap
		}
		if gap > 2500*time.Millisecond {
			t.Fatalf("open %d started %v after open %d finished: queued acquires are tick-quantized, not event-driven (spans: %s -> %s)",
				i, gap, i-1, spans[i-1].RU, spans[i].RU)
		}
	}

	// TOTAL CYCLE: first open start -> last open end is ~the serialized opens,
	// with no per-position tick accumulation. The discriminating bound: the
	// feared shape (>=2 ticks per position) would cost >= moving x 11s.
	total := spans[len(spans)-1].End.Sub(spans[0].Start)
	maxTotal := time.Duration(moving)*mountDelay + time.Duration(moving)*2500*time.Millisecond
	if total > maxTotal {
		t.Fatalf("acquire chain took %v for %d positions (> %v): per-position choreography is accumulating ticks", total, moving, maxTotal)
	}
	t.Logf("CONFIRMED: %d serialized 1s opens chained in %v total (worst inter-open gap %v, peak concurrency 1) - the cycle costs ~the opens themselves, no per-position tick accumulation",
		moving, total.Round(time.Millisecond), worstGap.Round(time.Millisecond))
}
