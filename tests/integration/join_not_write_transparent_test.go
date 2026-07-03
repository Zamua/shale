package integration

// JOIN IS NOT WRITE-TRANSPARENT (the mirror gap of the graceful-leave path).
//
// shale keeps writes AVAILABLE through a graceful LEAVE: the leaving node sets
// its Draining bit, stays a CURRENT owner in the ring, and the write fan-out
// dual-writes to the routed UNION (current-ring + pending-ring), so a node that
// physically holds the data is always in the write set until the successor
// mounts (graceful_leave_test.go + overlap_handoff_test.go pin this). That whole
// mechanism is GATED on the Draining bit (drainingIDs() -> the current/pending
// union). A plain membership ADD (a new node joins and the ring reassigns a
// replica slot from an existing node to the newcomer) sets NO Draining bit, so:
//
//   - routedReplicasWithUnit / routedReplicasForKey take the len(draining)==0
//     fast path and return ONLY the NEW ring's replica set - which INCLUDES the
//     mid-acquire newcomer and EXCLUDES the displaced old replica (which still
//     physically holds the data). (pkg/cluster/multibackend_pending_route.go)
//   - the newcomer mounts the moving unit via the CLEAN-CUT acquireReplicaUnit
//     (no handoffPhase, no predecessor forward, synchronous open), and a routed
//     op to a not-yet-mounted position returns errUnitAcquiring with NO forward
//     (resolveAndApplyReplicaPut). (pkg/cluster/multibackend_replicated.go +
//     multibackend_reshard_route.go)
//
// Net: a write to a MOVING shard routes to [newcomer(mid-acquire), survivor] and
// collects only ONE ack -> "write needed 2 acks, got 1 (replicas mid-acquire)"
// for the whole acquire window. With a real slow (object-storage) mount and many
// moving units the window is minutes - the live 3->4 wedge.
//
// This file reproduces that deterministically and pins the asymmetry: the JOIN
// wedges the moving shards while non-moving shards stay available (this test),
// and a graceful LEAVE keeps the moving shards available (the companion below).

import (
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
)

// startReplicatedNodeSlowAcquire brings up an R=rf node whose factory handle has
// the acquire delay armed BEFORE cluster.Open, so the delay applies to the very
// first mount/acquire of every position (unlike SetAcquireDelay called after
// startReplicatedNode returns, which misses the Open-time mount). Paired with
// planted serving markers this models a JOINING node that BOOT-DEFERS its owned
// positions and then re-ACQUIRES them slowly via the reconcile - the real
// object-storage acquire window a live join hits.
func startReplicatedNodeSlowAcquire(t *testing.T, id, seedAddr string, unitCount, rf int, backing *sharedfactory.Backing, delay time.Duration, writeTimeout time.Duration) *sharedNode {
	t.Helper()
	h := backing.Handle()
	h.SetAcquireDelay(delay) // armed BEFORE Open

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gRPC: %v", err)
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
		WriteTimeout:      writeTimeout,
	}
	if seedAddr != "" {
		cfg.Seeds = []string{seedAddr}
	}
	c, bindAddr := openClusterRetryBind(t, cfg)
	rpc.NewServer(c).Register(grpcSrv)
	go func() { defer close(serveDone); _ = grpcSrv.Serve(lis) }()
	n := &sharedNode{
		ID: id, Cluster: c, Handle: h, BindAddr: bindAddr, GRPCAddr: grpcAddr,
		stop: func() { grpcSrv.GracefulStop(); <-serveDone },
	}
	t.Cleanup(n.Close)
	return n
}

// oneKeyPerUnit returns a representative key for every unit id, seeding each so a
// real value exists on both replicas before the topology change.
func oneKeyPerUnit(t *testing.T, c *cluster.Cluster, unitCount int) map[storageunit.UnitID]string {
	t.Helper()
	out := make(map[storageunit.UnitID]string, unitCount)
	for i := 0; len(out) < unitCount; i++ {
		k := fmt.Sprintf("jtk-%06d", i)
		u := unitOf(k, unitCount)
		if _, ok := out[u]; ok {
			continue
		}
		if err := putWithRetryUnavailable(t, c, k, "seed", 10*time.Second); err != nil {
			t.Fatalf("seed %q: %v", k, err)
		}
		out[u] = k
	}
	return out
}

// probeClass tallies immediate (non-retrying-past-WriteTimeout) writes to the
// given units through entry, classifying each as ok / mid-acquire / other.
func probeClass(entry *cluster.Cluster, uk map[storageunit.UnitID]string, units []storageunit.UnitID) (ok, mid, other int) {
	for _, u := range units {
		err := entry.Put([]byte(uk[u]), []byte("probe"))
		switch {
		case err == nil:
			ok++
		case isMidAcquire(err):
			mid++
		default:
			other++
		}
	}
	return
}

// TestJoinNotWriteTransparent_MovingShardsWedge is the deterministic JOIN repro:
// adding a 4th node with a slow mount wedges writes to every shard that moves
// ONTO it (mid-acquire), while non-moving shards stay available.
func TestJoinNotWriteTransparent_MovingShardsWedge(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	// Short WriteTimeout so each probe returns fast (its internal retry cannot mask
	// a mount longer than the budget) - the write budget a real client faces.
	mutate := func(cfg *cluster.Config) { cfg.WriteTimeout = 300 * time.Millisecond }

	n1 := startReplicatedNodeCfg(t, "jn-a", "", uc, rf, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "jn-b", n1.BindAddr, uc, rf, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "jn-c", n1.BindAddr, uc, rf, backing, mutate)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)

	uk := oneKeyPerUnit(t, n1.Cluster, uc)
	// Plant a serving marker for every position so the JOINING node BOOT-DEFERS its
	// owned positions at Open (a live long-running cluster has markers everywhere)
	// and then re-acquires them via the reconcile - the path the slow acquire delay
	// governs. Without this the joiner mounts everything instantly at Open and the
	// acquire window (hence the wedge) never opens in-process.
	plantAllServingMarkers(t, n1.Handle, uc, rf)

	// Classify units by whether the 4th node ("jn-d") becomes a replica: a MOVING
	// unit (jn-d enters the replica set, displacing an old replica) vs a stable one.
	old3 := []string{"jn-a", "jn-b", "jn-c"}
	new4 := []string{"jn-a", "jn-b", "jn-c", "jn-d"}
	var moving, stable []storageunit.UnitID
	for u := range uk {
		o := replicaIDsForMembers(old3, u, rf)
		n := replicaIDsForMembers(new4, u, rf)
		if contains(n, "jn-d") && !contains(o, "jn-d") {
			moving = append(moving, u)
		} else {
			stable = append(stable, u)
		}
	}
	sort.Slice(moving, func(i, j int) bool { return moving[i] < moving[j] })
	if len(moving) == 0 {
		t.Skip("no unit moves onto the 4th node under this hashing; adjust node ids")
	}
	t.Logf("of %d units: %d MOVE onto jn-d, %d stay put", uc, len(moving), len(stable))

	// Join the 4th node with the acquire delay armed BEFORE Open: it boot-defers
	// its owned positions (markers planted) then re-acquires them SLOWLY via the
	// reconcile, so every moving shard sits owned-but-unmounted on jn-d for the
	// window - the real object-storage join wedge.
	const mountDelay = 12 * time.Second
	n4 := startReplicatedNodeSlowAcquire(t, "jn-d", n1.BindAddr, uc, rf, backing, mountDelay, 300*time.Millisecond)
	cs4 := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}
	if err := waitForMembersAll(cs4, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	// Ensure the entry node (n1, an original) routes with the NEW 4-node ring.
	if len(n1.Cluster.Members()) != 4 {
		t.Fatalf("entry node not converged to 4 members")
	}

	// PROBE while jn-d is mid-acquire (12s window). Aggregate over a few rounds so
	// the signal is robust. The entry is n1 (fully converged, routes to the new
	// replica set that includes the mid-acquire jn-d for moving units).
	var movOK, movMid, movOther, stOK, stMid, stOther int
	const rounds = 3
	for r := 0; r < rounds; r++ {
		o, m, x := probeClass(n1.Cluster, uk, moving)
		movOK += o
		movMid += m
		movOther += x
		o, m, x = probeClass(n1.Cluster, uk, stable)
		stOK += o
		stMid += m
		stOther += x
		time.Sleep(1 * time.Second)
	}
	movTotal := movOK + movMid + movOther
	stTotal := stOK + stMid + stOther
	t.Logf("MOVING-shard writes during acquire: ok=%d midAcquire=%d other=%d (total=%d)", movOK, movMid, movOther, movTotal)
	t.Logf("STABLE-shard writes during acquire: ok=%d midAcquire=%d other=%d (total=%d)", stOK, stMid, stOther, stTotal)

	// Drop the delay so cleanup + any readback is fast.
	for _, n := range []*sharedNode{n1, n2, n3, n4} {
		n.Handle.SetAcquireDelay(0)
	}

	// ASSERTIONS (the deterministic JOIN wedge):
	// 1. Moving shards WEDGE: mid-acquire dominates their writes.
	if movMid == 0 {
		t.Fatalf("expected moving-shard writes to WEDGE with 'replicas mid-acquire' during the join acquire, saw none "+
			"(ok=%d other=%d) - the join may have become write-transparent (hypothesis refuted)", movOK, movOther)
	}
	if rate := float64(movMid) / float64(movTotal); rate < 0.6 {
		t.Fatalf("join wedge not dominant: only %.0f%% of moving-shard writes were mid-acquire (want >=60%%); "+
			"ok=%d mid=%d other=%d", rate*100, movOK, movMid, movOther)
	}
	// 2. Stable shards stay AVAILABLE: zero mid-acquire (they never route to jn-d).
	if stMid > 0 {
		t.Fatalf("unexpected: %d STABLE-shard writes wedged mid-acquire (only MOVING shards should wedge on a join)", stMid)
	}
	t.Logf("CONFIRMED: JOIN is NOT write-transparent - %d/%d moving-shard writes wedged 'needed 2 acks, got 1 "+
		"(replicas mid-acquire)' during jn-d's acquire, while stable shards stayed 100%% available (%d/%d ok). "+
		"The displaced replica is dropped from routing (no Draining bit -> no current/pending union) and the "+
		"newcomer refuses (mid-acquire, no forward) until it mounts.", movMid, movTotal, stOK, stTotal)
}

// TestLeaveIsWriteTransparent_MovingShardsStayAvailable is the COMPANION that
// pins the asymmetry: on the SAME kind of cluster, a graceful LEAVE keeps writes
// to the shards moving OFF the leaver AVAILABLE throughout the successor's slow
// mount - because the leaver sets its Draining bit, stays a CURRENT owner, and
// the write fan-out dual-writes to the current/pending UNION (leaver + survivor +
// mid-acquire successor), acking over the two still-mounted stable replicas. This
// is the exact coverage the JOIN path lacks.
func TestLeaveIsWriteTransparent_MovingShardsStayAvailable(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	mutate := func(cfg *cluster.Config) {
		cfg.WriteTimeout = 300 * time.Millisecond
		cfg.GracefulLeaveDrainTimeout = 12 * time.Second // enable the graceful drain (small: keeps teardown fast)
	}
	n1 := startReplicatedNodeCfg(t, "lv-a", "", uc, rf, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "lv-b", n1.BindAddr, uc, rf, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "lv-c", n1.BindAddr, uc, rf, backing, mutate)
	n4 := startReplicatedNodeCfg(t, "lv-d", n1.BindAddr, uc, rf, backing, mutate)
	survivors := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	all := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}
	if err := waitForMembersAll(all, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	uk := oneKeyPerUnit(t, n1.Cluster, uc)

	// Units where the leaver lv-d is a replica: these MOVE OFF it when it drains
	// (its successor must acquire them), the mirror of the join's moving shards.
	var leaverShards []storageunit.UnitID
	ids4 := []string{"lv-a", "lv-b", "lv-c", "lv-d"}
	for u := range uk {
		if contains(replicaIDsForMembers(ids4, u, rf), "lv-d") {
			leaverShards = append(leaverShards, u)
		}
	}
	sort.Slice(leaverShards, func(i, j int) bool { return leaverShards[i] < leaverShards[j] })
	if len(leaverShards) == 0 {
		t.Skip("leaver holds no shards under this hashing")
	}
	t.Logf("lv-d is a replica of %d units (these move off it on drain)", len(leaverShards))

	// Arm a slow mount on the SURVIVORS (they must Acquire the leaver's positions),
	// so the drain window is long and the availability claim is meaningfully tested
	// (a write must stay available while the successor is still Acquiring).
	for _, n := range []*sharedNode{n1, n2, n3} {
		n.Handle.SetAcquireDelay(6 * time.Second)
	}

	// Gracefully LEAVE lv-d: Close() runs DrainForLeave, which sets the Draining
	// bit + keeps lv-d serving until its successors are Ready. Close blocks for the
	// drain, so run it in the background and probe during the window.
	drainDone := make(chan struct{})
	go func() { defer close(drainDone); _ = n4.Cluster.Close() }()

	// Let the Draining bit gossip to the survivors so their routing switches to the
	// current/pending union.
	time.Sleep(600 * time.Millisecond)

	// PROBE the leaver's shards through a survivor (lv-a) during the drain window.
	// They must stay AVAILABLE: the union write acks over lv-d (still serving) +
	// the stable co-replica, even though the successor is mid-acquire.
	var ok, mid, other int
	probeEnd := time.Now().Add(3 * time.Second)
	rounds := 0
	for time.Now().Before(probeEnd) {
		o, m, x := probeClass(n1.Cluster, uk, leaverShards)
		ok += o
		mid += m
		other += x
		rounds++
		time.Sleep(500 * time.Millisecond)
	}
	total := ok + mid + other
	t.Logf("LEAVER-shard writes during drain (successor mid-acquire): ok=%d midAcquire=%d other=%d (total=%d)", ok, mid, other, total)

	// Release the delay so the drain can finish + Close returns, then wait it out.
	for _, n := range []*sharedNode{n1, n2, n3} {
		n.Handle.SetAcquireDelay(0)
	}
	select {
	case <-drainDone:
	case <-time.After(30 * time.Second):
		t.Logf("note: drain still in progress at teardown")
	}

	// ASSERTION (the mirror of the join): writes to the leaver's moving shards stay
	// AVAILABLE during the drain - the union keeps a mounted holder in the write set
	// the whole time. A high availability here vs the join's total wedge is the
	// asymmetry.
	if total == 0 {
		t.Fatalf("no probes ran")
	}
	if rate := float64(ok) / float64(total); rate < 0.9 {
		t.Fatalf("LEAVE availability too low: only %.0f%% of leaver-shard writes acked during the drain "+
			"(want >=90%%); ok=%d mid=%d other=%d - the graceful-leave union is not keeping the leaver serving",
			rate*100, ok, mid, other)
	}
	_ = survivors
	t.Logf("CONFIRMED (asymmetry): a graceful LEAVE keeps the moving shards AVAILABLE (%d/%d acked through the "+
		"successor's slow mount) - the Draining bit engages the current/pending union so a mounted holder is always "+
		"in the write set. The JOIN has no such coverage (see TestJoinNotWriteTransparent_MovingShardsWedge): "+
		"same slow mount, but the moving shards WEDGE because a plain ADD sets no Draining bit.", ok, total)
}
