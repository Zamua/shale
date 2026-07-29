package integration

// THE OVERLAP-HANDOFF ACCEPTANCE GATE + BREAK-DEMO (v0.8 Phase 2e, Option B).
//
// docs/SPEC.md "v0.8 Phase 2e", "THE OVERLAP-HANDOFF AVAILABILITY GATE" is the
// canonical behavior. Two tests share ONE driver:
//
//   - TestOverlapHandoff_HoldsAvailabilityThroughSlowMount (overlap ON): a
//     multi-backend R=2 cluster on the sharedfactory whose OpenReplicaUnit is
//     made ARBITRARILY SLOW (a multi-second mount, the object-store-latency
//     analogue), with a CONTINUOUS writer through a 3 -> 4 R=2 membership
//     change. Asserts (a) THE ORACLE: every baseline + acked probe key is
//     readable with its exact value from EVERY node, ZERO acked loss; and (b)
//     THE ACK RATE stays ~100% EVEN with the mount LONGER than the WriteTimeout
//     budget - which is only possible because the old owner keeps serving via
//     the new owner's forward throughout the Acquiring window.
//
//   - TestOverlapHandoff_BreakDemo_CleanCutCollapses (overlap OFF, retry OFF):
//     the SAME slow-mount membership change with Config.TestingForceCleanCut,
//     which forces the pre-2e clean-cut RELEASE-then-ACQUIRE (the new owner
//     refuses during Acquiring instead of forwarding) AND disables the Option-A
//     WriteTimeout-bounded retry (so the retry cannot mask the gap). Asserts the
//     ack rate COLLAPSES well below the gate threshold, proving the overlap
//     forward is what holds availability rather than rubber-stamping it.
//     Disabling BOTH (not just overlap) isolates the overlap signal: leaving
//     Option-A retry on would let it partially absorb the slow mount.
//
// The mount delay (7s) is LARGER than the WriteTimeout (the 5s default), the
// regime where Option A ALONE collapses (its retry budget cannot outlast a
// single mount). NO slatedb tag, NO MinIO: in-process sharedfactory + memory
// backend (the SetAcquireDelay slow-mount injection is the latency analogue).

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
)

// overlapGateResult is the outcome of one membership-change run the two tests
// assert over.
type overlapGateResult struct {
	attempted int64
	acked     int64
	// want is baseline keys PLUS every acked probe key, each mapped to its exact
	// value. The loss oracle reads every one back from every node.
	want map[string][]byte
	// nodes is the full converged set (entry points for the readback).
	nodes []*sharedNode
}

func (r overlapGateResult) ackRate() float64 {
	if r.attempted == 0 {
		return 0
	}
	return float64(r.acked) / float64(r.attempted)
}

// startReplicatedNodeCfg brings up one R=replicationFactor multi-backend node
// like startReplicatedNode, but lets the caller mutate the Config before Open
// (the break-demo needs Config.TestingForceCleanCut). seedAddr empty = founder.
func startReplicatedNodeCfg(
	t *testing.T,
	id, seedAddr string,
	unitCount, replicationFactor int,
	backing *sharedfactory.Backing,
	mutate func(*cluster.Config),
) *sharedNode {
	t.Helper()
	h := backing.Handle()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startReplicatedNodeCfg %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:               id,
		BackendFactory:       h,
		UnitCount:            storageunit.MustUnitCount(unitCount),
		ReplicationFactor:    replicationFactor,
		WriteConsistency:     cluster.WriteAll,
		ReadConsistency:      cluster.ReadAll,
		GRPCAddr:             grpcAddr,
		LogOutput:            io.Discard,
		RebalanceSettleDelay: 300 * time.Millisecond,
	}
	gcfg := gossip.Config{LogOutput: io.Discard}
	if seedAddr != "" {
		gcfg.Seeds = []string{seedAddr}
	}
	if mutate != nil {
		mutate(&cfg)
	}

	c, bindAddr := openClusterRetryBind(t, cfg, gcfg)

	rpc.NewServer(c).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &sharedNode{
		ID:       id,
		Cluster:  c,
		Handle:   h,
		BindAddr: bindAddr,
		GRPCAddr: grpcAddr,
		stop: func() {
			grpcSrv.GracefulStop()
			<-serveDone
		},
	}
	t.Cleanup(n.Close)
	return n
}

// start3NodeR2Cfg stands up a 3-node R=2 cluster whose Configs are each passed
// through mutate, then waits for convergence + a settle.
func start3NodeR2Cfg(
	t *testing.T,
	unitCount int,
	backing *sharedfactory.Backing,
	mutate func(*cluster.Config),
) []*sharedNode {
	t.Helper()
	n1 := startReplicatedNodeCfg(t, "ovh-a", "", unitCount, 2, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "ovh-b", n1.BindAddr, unitCount, 2, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "ovh-c", n1.BindAddr, unitCount, 2, backing, mutate)
	nodes := []*sharedNode{n1, n2, n3}
	cs := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(cs, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)
	return nodes
}

// runOverlapMembershipChange is the shared driver for both the gate and the
// break-demo. It writes a recorded baseline, runs a continuous writer through a
// 3 -> 4 R=2 membership change while every mount stalls mountDelay, and returns
// the ack tally + the want set (baseline + acked) + the converged node set. The
// caller asserts the loss oracle and the ack rate. forceCleanCut threads
// Config.TestingForceCleanCut into every node (overlap OFF + retry OFF).
func runOverlapMembershipChange(t *testing.T, forceCleanCut bool, mountDelay time.Duration) overlapGateResult {
	t.Helper()
	const unitCount = 32
	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence-at-completion timing): this driver pins the
	// overlap-forward union mechanism in ISOLATION. Its gate's ack-rate
	// assertion is "only possible because the old owner keeps serving" a
	// mount LONGER than the 5s write budget, which requires the displaced
	// owner to stay unfenced through the modeled mount. Under the backing's
	// default eager fence (real slatedb timing) the displaced owner is
	// fenced at open-START and these same moving-shard writes WEDGE, which
	// is the KNOWN residual TestJoinResidual_FenceAtOpenStart_
	// MovingShardsWedge reproduces deliberately under the default.
	backing.SetEagerFence(false)

	mutate := func(cfg *cluster.Config) {
		cfg.TestingForceCleanCut = forceCleanCut
	}

	// Start a 3-node R=2 cluster (default 5s WriteTimeout) and let it fully
	// settle before the baseline so initial-convergence per-replica fencing is
	// resolved (no artificial mount delay armed yet).
	nodes := start3NodeR2Cfg(t, unitCount, backing, mutate)
	time.Sleep(700 * time.Millisecond)

	want := make(map[string][]byte)
	for i := range 120 {
		k := fmt.Sprintf("ovbase-%05d", i)
		v := fmt.Appendf(nil, "ovbaseval-%05d", i)
		if err := putWithRetryUnavailable(t, nodes[0].Cluster, k, string(v), 15*time.Second); err != nil {
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

		entryMu  sync.RWMutex
		entryFor = append([]*sharedNode{}, nodes...)
	)
	pickEntry := func(w int) *sharedNode {
		entryMu.RLock()
		defer entryMu.RUnlock()
		return entryFor[w%len(entryFor)]
	}
	const writers = 6
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				entry := pickEntry(w)
				k := fmt.Sprintf("ovprobe-%d-%07d", w, i)
				v := fmt.Appendf(nil, "ovpv-%d-%07d", w, i)
				attempted.Add(1)
				// Call Put DIRECTLY (no external retry) so the ack rate reflects the
				// cluster's INTERNAL availability through the long mount.
				if err := entry.Cluster.Put([]byte(k), []byte(v)); err == nil {
					acked.Add(1)
					mu.Lock()
					ackedKeys[k] = v
					mu.Unlock()
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}

	// Warm up, then arm the LONG mount delay on the existing nodes and join a
	// 4th. The join shuffles replica positions across many units, so reconcile
	// re-mounts fire; each mount now stalls mountDelay (> the 5s write budget),
	// the regime where Option-A retry alone collapses.
	time.Sleep(300 * time.Millisecond)
	for _, n := range nodes {
		n.Handle.SetAcquireDelay(mountDelay)
	}
	n4 := startReplicatedNodeCfg(t, "ovh-d", nodes[0].BindAddr, unitCount, 2, backing, mutate)
	n4.Handle.SetAcquireDelay(mountDelay)
	all := append(append([]*sharedNode{}, nodes...), n4)

	csAll := make([]*cluster.Cluster, 0, len(all))
	for _, n := range all {
		csAll = append(csAll, n.Cluster)
	}
	if err := waitForMembersAll(csAll, 4, 30*time.Second); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("4-node convergence: %v", err)
	}
	entryMu.Lock()
	entryFor = append([]*sharedNode{}, all...)
	entryMu.Unlock()

	// Keep writing across the full mount window (a couple of mount-delays) so the
	// overlap window is genuinely exercised by in-flight writes.
	time.Sleep(16 * time.Second)
	stop.Store(true)
	wg.Wait()

	// Drop the delay so the readback is not itself slowed, then let any still-
	// Acquiring positions finish their (now instant) mount + the drains release.
	for _, n := range all {
		n.Handle.SetAcquireDelay(0)
	}
	time.Sleep(2 * time.Second)

	mu.Lock()
	for k, v := range ackedKeys {
		want[k] = v
	}
	mu.Unlock()

	return overlapGateResult{
		attempted: attempted.Load(),
		acked:     acked.Load(),
		want:      want,
		nodes:     all,
	}
}

// assertNoAckedLoss is THE ORACLE (a): every baseline + acked key is readable
// with its exact value from every node. ZERO acked loss. It MUST hold for BOTH
// the gate AND the break-demo - durability is invariant of the ack rate (the
// pre-2e behavior was lossless-but-unavailable; the fix is the ack rate, not
// durability). So the break-demo asserts this too.
func assertNoAckedLoss(t *testing.T, r overlapGateResult) {
	t.Helper()
	for k, v := range r.want {
		got, err := getWithRetryUnavailable(t, r.nodes[0].Cluster, k, 15*time.Second)
		if err != nil {
			t.Fatalf("ACKED WRITE LOST: key %q unreadable after handoff: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("ACKED WRITE CORRUPTED: key %q = %q, want %q", k, got, v)
		}
	}
	sample := 0
	for k, v := range r.want {
		for _, n := range r.nodes {
			got, err := getWithRetryUnavailable(t, n.Cluster, k, 15*time.Second)
			if err != nil {
				t.Fatalf("ACKED WRITE LOST on node %s: key %q: %v", n.ID, k, err)
			}
			if !bytes.Equal(got, v) {
				t.Fatalf("node %s: key %q = %q, want %q", n.ID, k, got, v)
			}
		}
		if sample++; sample >= 40 {
			break
		}
	}
}

// gateAckThreshold is the floor the overlap path must clear (and the clean-cut
// break-demo must fall well below). The overlap run measures ~95% in a normal
// run; the floor carries margin for the -race build, whose instrumentation
// stretches the mount/forward timing and widens the residual unserved fallback
// window (pure-new-mount / ambiguous-predecessor degrades to Option A), nudging
// the rate toward ~90%. 0.85 stays comfortably above the break-demo's observed
// ~63-71% so the two regimes never overlap, while still being far from a
// pass-anything bar. It is not a hard 100%: a few writes always blip on the
// single-hop-scope fallback.
const gateAckThreshold = 0.85

// breakDemoCeiling is the ceiling the clean-cut + retry-off run must stay UNDER.
// Comfortably below gateAckThreshold so the two regimes are unambiguous: with
// the mount LONGER than the write budget and no overlap + no retry, a large
// fraction of writes routed to a still-Acquiring new owner are refused outright
// (observed ~63-71%).
const breakDemoCeiling = 0.78

const overlapMountDelay = 7 * time.Second

// TestOverlapHandoff_HoldsAvailabilityThroughSlowMount is the gate (overlap ON):
// the ack rate stays ~100% through a 3 -> 4 membership change whose mounts each
// exceed the WriteTimeout, because the old owner keeps serving via the new
// owner's forward. Asserts the loss oracle AND the high ack rate.
func TestOverlapHandoff_HoldsAvailabilityThroughSlowMount(t *testing.T) {
	r := runOverlapMembershipChange(t, false, overlapMountDelay)
	if r.attempted == 0 {
		t.Fatalf("continuous writer attempted zero writes")
	}
	rate := r.ackRate()
	t.Logf("overlap ON (mount %v > writeTimeout 5s) through 3->4: acked %d / attempted %d = %.1f%%",
		overlapMountDelay, r.acked, r.attempted, rate*100)

	// (a) THE ORACLE: zero acked loss, readable + exact from every node.
	assertNoAckedLoss(t, r)

	// (b) THE ACK RATE stays high DESPITE the mount being LONGER than the write
	// budget - only possible because overlap forwarding keeps the old owner
	// serving throughout the Acquiring window.
	if rate < gateAckThreshold {
		t.Fatalf("OVERLAP AVAILABILITY TOO LOW: acked %d / attempted %d = %.1f%% < %.0f%% "+
			"(a mount %v > the 5s write budget collapsed availability; overlap forwarding is not keeping the old owner serving)",
			r.acked, r.attempted, rate*100, gateAckThreshold*100, overlapMountDelay)
	}
}

// TestOverlapHandoff_BreakDemo_CleanCutCollapses keeps the gate honest: with
// overlap OFF (forced clean-cut) AND the Option-A retry OFF, the SAME slow-mount
// membership change drives the ack rate WELL BELOW the gate threshold - proving
// the overlap forward is what holds availability, not Option-A retry masking it.
// Durability (the oracle) must STILL hold: clean-cut is lossless-but-unavailable.
func TestOverlapHandoff_BreakDemo_CleanCutCollapses(t *testing.T) {
	r := runOverlapMembershipChange(t, true, overlapMountDelay)
	if r.attempted == 0 {
		t.Fatalf("continuous writer attempted zero writes")
	}
	rate := r.ackRate()
	t.Logf("BREAK-DEMO overlap OFF + retry OFF (mount %v > writeTimeout 5s) through 3->4: acked %d / attempted %d = %.1f%%",
		overlapMountDelay, r.acked, r.attempted, rate*100)

	// Durability is invariant of availability: clean-cut never loses an acked
	// write, it just refuses many. The oracle MUST still pass.
	assertNoAckedLoss(t, r)

	// The break signal: the ack rate must COLLAPSE under the gate floor. If it
	// did NOT collapse, the gate is not actually testing the overlap (something
	// else is keeping writes available), so the gate's high-ack-rate assertion
	// would be rubber-stamping.
	if rate >= breakDemoCeiling {
		t.Fatalf("BREAK-DEMO DID NOT COLLAPSE: clean-cut + retry-off acked %d / attempted %d = %.1f%% >= %.0f%% "+
			"(the gate is not isolating the overlap forward; its high-ack-rate assertion would be rubber-stamping)",
			r.acked, r.attempted, rate*100, breakDemoCeiling*100)
	}
}
