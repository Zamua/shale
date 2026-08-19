package integration

// A REAL CLUSTER ON THE CAS COORDINATOR (pkg/coord/cas).
//
// These tests stand up the full in-process cluster - real gRPC forwarding,
// the shared sharedfactory backing, the full mount/handoff machinery - with
// membership coordinated ENTIRELY through one JSON document in a
// storageunit.ConditionalStore: no extra transport, no seed addresses. The
// same MemConditionalStore doubles as
// cluster.Config.ConditionalStore (the reshard-arbiter / bootstrap-marker
// seam), which is exactly the production shape the adapter exists for: one
// CAS-capable store is the whole coordination substrate.
//
// What only THIS tree can prove about the adapter: the port contract tests
// (internal/coordcontract) pin each method in isolation; these tests pin
// that the CLUSTER's consumption of those methods - bootstrap discovery,
// dual member bases, role-gated overlap warm-up, the change-hint debounce,
// takeover after a leave - composes into a working store when the answers
// come from a poll over a document instead of a gossip mesh.
//
// TIMING KNOBS (test-sized, same ratios as production): renewal 100ms, poll
// 50ms, ExpiryPolls 5. Detection latency for a SILENT death is therefore
// ExpiryPolls*poll = 250ms of observed silence against a 100ms renewal -
// the 2.5x margin the production defaults (5 polls * 1s vs 2s renewal)
// carry. A GRACEFUL leave is faster still: Close CAS-removes the row, so
// survivors observe the departure within one poll (50ms). The takeover
// waits below are sized far above BOTH bounds because the dominant cost is
// remounting the departed node's positions, not detecting the departure.

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/clustertest"
	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/coord/cas"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
)

const (
	// casTestRenewal / casTestPoll / casTestExpiryPolls are the adapter
	// knobs every node in these fixtures runs. See the file comment for
	// the derived detection-latency bounds.
	casTestRenewal     = 100 * time.Millisecond
	casTestPoll        = 50 * time.Millisecond
	casTestExpiryPolls = 5
)

// startCASNode brings up one multi-backend node whose Coordinator is the CAS
// adapter over cond - the CAS counterpart of startReplicatedNodeSlowAcquireCfg.
// There is no bind address and no seed: the store IS the discovery mechanism,
// so the first node to Start founds the cluster and every later one joins by
// CAS-appending its row. acquireDelay > 0 arms the sharedfactory's slow-mount
// injection BEFORE Open, so it applies to boot-time mounts too.
func startCASNode(t *testing.T, id string, unitCount, rf int, backing *sharedfactory.Backing, cond *storageunit.MemConditionalStore, acquireDelay, writeTimeout time.Duration, mutate func(*cluster.Config)) *sharedNode {
	t.Helper()

	h := backing.Handle()
	if acquireDelay > 0 {
		h.SetAcquireDelay(acquireDelay)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startCASNode %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:            id,
		BackendFactory:    h,
		UnitCount:         storageunit.MustUnitCount(unitCount),
		ReplicationFactor: rf,
		WriteConsistency:  cluster.WriteAll,
		ReadConsistency:   cluster.ReadAll,
		GRPCAddr:          grpcAddr,
		LogOutput:         io.Discard,
		WriteTimeout:      writeTimeout,
		// Snappy debounce, as the other integration fixtures use; tests that
		// need the production cadence override via mutate.
		RebalanceSettleDelay: 300 * time.Millisecond,
		// The production shape: the SAME conditional store serves the
		// cluster-side arbiter seams (bootstrap marker, reshard arbiter)
		// and, below, the coordinator. Distinct keys, one truth channel.
		ConditionalStore: cond,
		Coordinator: cas.New(cas.Config{
			Store:           cond,
			RenewalInterval: casTestRenewal,
			PollInterval:    casTestPoll,
			ExpiryPolls:     casTestExpiryPolls,
		}),
	}
	if mutate != nil {
		mutate(&cfg)
	}

	c, err := cluster.Open(cfg)
	if err != nil {
		t.Fatalf("startCASNode %s: cluster.Open: %v", id, err)
	}
	rpc.NewServer(c).Register(grpcSrv)
	go func() { defer close(serveDone); _ = grpcSrv.Serve(lis) }()

	n := &sharedNode{
		ID:       id,
		Cluster:  c,
		Handle:   h,
		GRPCAddr: grpcAddr,
		stop:     func() { grpcSrv.GracefulStop(); <-serveDone },
	}
	t.Cleanup(n.Close)
	return n
}

// basesAgree asserts that on node n BOTH member bases - the membership view
// and the placement basis - hold exactly the same IDs. The CAS adapter
// publishes {view, ring} as one snapshot, so any divergence here is a bug,
// not a window (contrast the removed gossip adapter's documented
// event-loop-hop lag).
func basesAgree(t *testing.T, n *sharedNode) {
	t.Helper()
	view := n.Cluster.Members()
	placement := n.Cluster.PlacementMembers()
	if len(view) != len(placement) {
		t.Fatalf("%s: view has %d members, placement %d - the single-snapshot promise is broken", n.ID, len(view), len(placement))
	}
	for i := range view {
		if view[i].ID != placement[i].ID {
			t.Fatalf("%s: member bases disagree at %d: view=%s placement=%s", n.ID, i, view[i].ID, placement[i].ID)
		}
	}
}

// getRideTakeover reads key through entry, retrying the documented transient
// classes (ErrAcquiring while a survivor mounts a taken-over position;
// Unavailable/ResourceExhausted while routing catches up with a departure)
// within budget. The property asserted is that reads CONVERGE to correct
// within a bounded window, never that a mid-takeover read cannot refuse.
func getRideTakeover(entry *cluster.Cluster, key []byte, budget time.Duration) ([]byte, error) {
	deadline := time.Now().Add(budget)
	wait := 25 * time.Millisecond
	for {
		v, err := entry.Get(key)
		if err == nil {
			return v, nil
		}
		if !errors.Is(err, cluster.ErrAcquiring) && !clustertest.IsTransientWarmupErr(err) {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("read did not converge within the %s takeover budget: %w", budget, err)
		}
		time.Sleep(wait)
		if wait < 200*time.Millisecond {
			wait *= 2
		}
	}
}

// TestCASCoordinator_ThreeNodeLifecycle is the core end-to-end proof: a
// 3-node R=2 cluster whose ONLY coordination mechanism is the CAS membership
// document converges (both member bases, on every node), lands quorum
// writes, survives a graceful departure with the survivors taking over the
// leaver's positions, and keeps every acked write readable throughout.
func TestCASCoordinator_ThreeNodeLifecycle(t *testing.T) {
	const (
		unitCount = 8
		rf        = 2
		keyCount  = unitCount * 4
	)
	backing := sharedfactory.NewBacking()
	cond := storageunit.NewMemConditionalStore()

	// Start the three nodes back to back - deliberately no readiness stagger,
	// so bootstrap discovery (PutIfAbsent founding vs CAS-append joining) is
	// exercised under contention, not choreographed.
	nodes := []*sharedNode{
		startCASNode(t, "cas-1", unitCount, rf, backing, cond, 0, 8*time.Second, nil),
		startCASNode(t, "cas-2", unitCount, rf, backing, cond, 0, 8*time.Second, nil),
		startCASNode(t, "cas-3", unitCount, rf, backing, cond, 0, 8*time.Second, nil),
	}
	cs := make([]*cluster.Cluster, len(nodes))
	for i, n := range nodes {
		cs[i] = n.Cluster
	}

	// CONVERGENCE, both member bases, then write readiness - the same gates
	// every multi-node fixture passes, here answered from the document poll.
	waitForClusterReady(t, cs, 45*time.Second)
	waitForWriteReady(t, cs, 45*time.Second)
	for _, n := range nodes {
		basesAgree(t, n)
	}

	// WRITES AT R=2: WriteAll demands BOTH replicas ack, so every accepted
	// Put below is a proven quorum write over CAS-coordinated placement.
	// Entry point rotates so forwarding is exercised from every node.
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("cas-lc-%03d", i))
		val := []byte(fmt.Sprintf("v%03d", i))
		if err := nodes[i%len(nodes)].Cluster.Put(key, val); err != nil {
			t.Fatalf("Put %q via %s: %v", key, nodes[i%len(nodes)].ID, err)
		}
	}

	// Reads stay correct on the intact cluster, through every entry point.
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("cas-lc-%03d", i))
		want := fmt.Sprintf("v%03d", i)
		for _, n := range nodes {
			got, err := n.Cluster.Get(key)
			if err != nil {
				t.Fatalf("Get %q via %s: %v", key, n.ID, err)
			}
			if string(got) != want {
				t.Fatalf("Get %q via %s: got %q, want %q", key, n.ID, got, want)
			}
		}
	}

	// GRACEFUL DEPARTURE: close cas-3. Its coordinator CAS-removes its row on
	// the way out, so the survivors observe the departure within one poll
	// (50ms; even the SILENT-death bound is only ExpiryPolls*poll = 250ms).
	// The wait below is dominated by the survivors REMOUNTING the departed
	// node's positions, not by detection, hence the generous budget.
	leaver := nodes[2]
	leaver.Close()
	survivors := []*sharedNode{nodes[0], nodes[1]}
	scs := []*cluster.Cluster{survivors[0].Cluster, survivors[1].Cluster}

	if err := waitForMembersAll(scs, 2, 20*time.Second); err != nil {
		t.Fatalf("survivors never converged on the 2-member cluster after the graceful leave: %v\nsurvivor 0:\n%s\nsurvivor 1:\n%s",
			err, survivors[0].Cluster.DebugState(), survivors[1].Cluster.DebugState())
	}
	for _, n := range survivors {
		basesAgree(t, n)
	}

	// READS STAY CORRECT THROUGH THE TAKEOVER: every acked write must come
	// back with its exact value from BOTH survivors. Transient acquiring /
	// routing refusals are ridden within a bounded budget; a wrong value or
	// a non-transient error fails immediately.
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("cas-lc-%03d", i))
		want := fmt.Sprintf("v%03d", i)
		for _, n := range survivors {
			got, err := getRideTakeover(n.Cluster, key, 20*time.Second)
			if err != nil {
				t.Fatalf("post-leave read %q via %s: %v\nstate:\n%s", key, n.ID, err, n.Cluster.DebugState())
			}
			if string(got) != want {
				t.Fatalf("post-leave read %q via %s: got %q, want %q - an acked write went missing across the takeover", key, n.ID, got, want)
			}
		}
	}

	// And the shrunken cluster still accepts quorum writes: the survivors
	// genuinely took over the departed positions rather than merely agreeing
	// the leaver is gone.
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("cas-post-%03d", i))
		if err := putRideAcquiring(survivors[i%2].Cluster, key, []byte("after"), 10*time.Second); err != nil {
			t.Fatalf("post-leave Put %q via %s: %v", key, survivors[i%2].ID, err)
		}
	}
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("cas-post-%03d", i))
		got, err := getRideTakeover(survivors[(i+1)%2].Cluster, key, 10*time.Second)
		if err != nil || string(got) != "after" {
			t.Fatalf("post-leave read-back %q: got %q err %v", key, got, err)
		}
	}
}

// TestCASCoordinator_FreshBootStagger_NoWriteGap is the v0.14.2 fresh-boot
// smoke on the CAS coordinator: the boot-defer + serving-marker machinery
// must compose with CAS membership exactly as it did under the removed
// gossip adapter.
//
// Node A boots alone: its coordinator's PutIfAbsent founds the cluster (so
// the requested Joining role is dropped, per the port), it mounts every
// position and publishes serving markers. Node B then boots into the
// document, joins with the Joining role riding its FIRST row (atomic with
// its appearance), finds A's markers on the positions the converged ring
// assigns to B, boot-defers them, and warms through the PROMPT acquire path.
// The assertions are the fresh-boot gate's, verbatim:
//
//   - no write gap: a write issued the moment B's Open returns succeeds
//     within the consumer retry budget (putRideAcquiring, 3s) - a budget
//     that rides the bounded-concurrency prompt warm-up (~2 waves of 500ms
//     opens) but NOT a serialized warm-up nor the production 5s settle
//     debounce parked in front of one;
//   - no cross-fence: after settling, every key reads back through BOTH
//     nodes, and cross-shard scans converge clean (the drain-release wedge
//     detector: B's boot-written markers are what let A's displaced
//     positions drain).
func TestCASCoordinator_FreshBootStagger_NoWriteGap(t *testing.T) {
	const (
		// 16 units for the same reason the retired homogeneous stagger gate
		// used 16:
		// at 4 units an unlucky ID pair can leave B primary for nothing,
		// deferring nothing, and the test would pass vacuously. The vacuity
		// guard below makes the residual case loud.
		unitCount = 16
		rf        = 2
	)
	backing := sharedfactory.NewBacking()
	cond := storageunit.NewMemConditionalStore()

	// Real open latency (500ms per open) makes the warm-up's CONCURRENCY
	// observable, and RebalanceSettleDelay 0 selects the PRODUCTION debounce
	// so the prompt path - not a snappy test cadence - is what must close
	// the gap. Same teeth as the other fresh-boot gates.
	bootCfg := func(cfg *cluster.Config) {
		cfg.RebalanceSettleDelay = 0 // 0 = the production default debounce
		cfg.OpenConcurrency = 8
	}

	a := startCASNode(t, "cas-boot-a", unitCount, rf, backing, cond, 500*time.Millisecond, 3*time.Second, bootCfg)

	// A alone must reach the fully-mounted solo state: every position
	// mounted, every serving marker durably published in the shared backing.
	deadlineA := time.Now().Add(15 * time.Second)
	for !a.Cluster.MountReadiness().Ready(1.0) {
		if time.Now().After(deadlineA) {
			t.Fatalf("node A never reached fully-mounted solo state: %+v", a.Cluster.MountReadiness())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// B boots into the incumbent document.
	b := startCASNode(t, "cas-boot-b", unitCount, rf, backing, cond, 500*time.Millisecond, 3*time.Second, bootCfg)

	// VACUITY GUARD: the scenario is "B boot-deferred positions a live peer
	// serves". If B deferred nothing, a pass proves nothing - fail loudly
	// instead of going vacuously green.
	if !strings.Contains(b.Cluster.DebugState(), "boot-deferred") {
		t.Fatalf("node B deferred nothing at boot - the marker/defer scenario did not arm under the CAS coordinator. B state:\n%s",
			b.Cluster.DebugState())
	}

	// THE MOMENT OF TRUTH: writes through both entry points the instant B's
	// Open returns, each riding only the documented acquiring window.
	for i := 0; i < unitCount*4; i++ {
		key := []byte(fmt.Sprintf("cas-gap-%03d", i))
		entry := a.Cluster
		via := a.ID
		if i%2 == 1 {
			entry, via = b.Cluster, b.ID
		}
		if err := putRideAcquiring(entry, key, []byte("v"), 3*time.Second); err != nil {
			t.Fatalf("write %q via %s failed during the post-boot window: %v\nnodeA:\n%s\nnodeB:\n%s",
				key, via, err, a.Cluster.DebugState(), b.Cluster.DebugState())
		}
	}

	// Settle: B's warming completes, A's displaced positions drain out.
	deadlineSettle := time.Now().Add(30 * time.Second)
	for {
		ra, rb := a.Cluster.MountReadiness(), b.Cluster.MountReadiness()
		if ra.PendingUnits == 0 && rb.PendingUnits == 0 && ra.FailedOpenUnits == 0 && rb.FailedOpenUnits == 0 {
			break
		}
		if time.Now().After(deadlineSettle) {
			t.Fatalf("topology never settled after the staggered CAS boot:\nA: %+v\nB: %+v\nnodeA:\n%s\nnodeB:\n%s",
				ra, rb, a.Cluster.DebugState(), b.Cluster.DebugState())
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Every write lands on real, un-fenced replicas: read-your-write through
	// both entry points.
	for i := 0; i < unitCount*4; i++ {
		key := []byte(fmt.Sprintf("cas-gap-%03d", i))
		for _, n := range []*sharedNode{a, b} {
			got, err := n.Cluster.Get(key)
			if err != nil || string(got) != "v" {
				t.Fatalf("read-back %q via %s: got %q err %v", key, n.ID, got, err)
			}
		}
	}

	// Wedge detector: cross-shard scans through each node converge clean
	// within a bound. A refusal that never stops means a fenced mount is
	// stuck - the drain-release failure the serving markers exist to prevent.
	scanDeadline := time.Now().Add(20 * time.Second)
	for _, n := range []*sharedNode{a, b} {
		for {
			nKeys, serr := countScan(n, []byte("cas-gap-"))
			if serr == nil && nKeys == unitCount*4 {
				break
			}
			if time.Now().After(scanDeadline) {
				t.Fatalf("cross-shard scan via %s never converged (last err=%v, keys=%d, want %d)\n%s",
					n.ID, serr, nKeys, unitCount*4, n.Cluster.DebugState())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
