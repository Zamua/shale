package integration

// JOIN IS WRITE-TRANSPARENT (the quorum-floor-guarded MIRROR of graceful leave).
//
// shale keeps writes AVAILABLE through a graceful LEAVE: the leaving node sets its
// Draining bit, stays a CURRENT owner in the ring, and the write fan-out dual-
// writes to the routed UNION, so a node that physically holds the data is always
// in the write set until the successor mounts. v0.8 Phase 2e now makes a JOIN the
// exact mirror via a gossiped Joining bit and the UNIFIED SPLIT: current = ring
// EXCLUDING joining (quorum-floored); pending = ring EXCLUDING draining. A node
// that boots into a live cluster with DEFERRED positions advertises Joining, so:
//
//   - it is EXCLUDED from the CURRENT set (currentUnitReplicas), so the still-
//     mounted DISPLACED old replica stays a current owner and keeps serving; the
//     newcomer is a PENDING owner that acquires via the background overlap path
//     (acquireReplicaUnitOverlap) and serving-marks on mount-complete, which gates
//     the displaced owner's drain. (pkg/cluster/multibackend_overlap*.go)
//   - routedReplicasWithUnit forms the current/pending UNION with stableR held at
//     R: a moving-shard write dual-writes [displaced-owner (mounted), survivor
//     (mounted), newcomer (mid-acquire)] and acks over the two MOUNTED holders,
//     so it stays available for the whole slow mount. (multibackend_pending_route.go)
//   - THE QUORUM FLOOR keeps current from shrinking below R: if 2+ of a unit's
//     replicas warm at once (mass boot) current reverts to the full ring, the SAFE
//     wedge (never acks below R durable copies) - covered by the mass-boot gate.
//
// BEFORE the fix a plain ADD set no bit, so routing returned only the NEW ring
// (newcomer mid-acquire IN, displaced owner OUT) and a moving-shard write collected
// only ONE ack -> "needed 2 acks, got 1 (replicas mid-acquire)" for the whole
// window (the live 3->4 wedge: this test previously measured 24/24 moving-shard
// writes WEDGED). It now measures those same moving shards staying AVAILABLE.
//
// This file pins the SYMMETRY: the JOIN keeps the moving shards available (this
// test), exactly as a graceful LEAVE does (the companion below).

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
	return startReplicatedNodeSlowAcquireCfg(t, id, seedAddr, unitCount, rf, backing, delay, writeTimeout, nil)
}

// startReplicatedNodeSlowAcquireCfg is startReplicatedNodeSlowAcquire with a
// final config mutate hook, applied AFTER the defaults below - so a test can
// override OpenConcurrency (e.g. the handoff-cycle latency pin runs the
// production default bound of 1 instead of the gates' 8).
func startReplicatedNodeSlowAcquireCfg(t *testing.T, id, seedAddr string, unitCount, rf int, backing *sharedfactory.Backing, delay time.Duration, writeTimeout time.Duration, mutate func(*cluster.Config)) *sharedNode {
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
		// The gates this helper serves arm MULTI-SECOND per-open delays across
		// MANY positions at once; at the production default open bound (1) those
		// would serialize into N x delay and bust the probe windows. The mock
		// double has no FFI concurrency hazard, so lift the bound to keep these
		// gates testing what they test (transparency DURING concurrent slow
		// mounts). The bound itself is pinned by
		// TestOverlapAcquire_BoundedByOpenConcurrency in pkg/cluster.
		OpenConcurrency: 8,
	}
	if seedAddr != "" {
		cfg.Seeds = []string{seedAddr}
	}
	if mutate != nil {
		mutate(&cfg)
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

// TestJoinIsWriteTransparent_MovingShardsStayAvailable is the deterministic JOIN
// transparency gate (the FIXED behavior; this test previously asserted the wedge
// and measured 24/24 moving-shard writes WEDGED). Adding a 4th node with a slow
// mount now keeps writes to every shard that moves ONTO it AVAILABLE throughout
// the mount: the newcomer advertises the Joining bit, so the still-mounted
// displaced replica stays a CURRENT owner and the union write acks over the two
// mounted holders while the newcomer mounts in the background.
// TestJoinResidual_FenceAtOpenStart_MovingShardsWedge reproduces the RESIDUAL
// measured on the real cluster (a clean 3->4 scale-up wedged writes ~42s during
// the newcomer's real-MinIO mount, even with the Joining-bit fix). It is the SAME
// setup as TestJoinIsWriteTransparent, but the backing is switched to
// fence-at-open-START timing (SetEagerFence) - real slatedb's DbBuilder.Build
// fences the prior owner the INSTANT the newcomer begins its slow open, not at
// mount completion. Under that timing the displaced owner is fenced for the whole
// mount and CANNOT serve the union, so a moving-shard write reaches only the
// un-displaced co-replica (1 ack of 2) and WEDGES - exactly the residual. The
// default fence-at-completion timing (which the passing test uses) hides this: it
// keeps the displaced owner unfenced during the modeled mount.
//
// This proves the residual is the FENCE TIMING, not the gossip-ordering race the
// Joining bit already closes (it rides the first Meta; cluster.go startJoining).
func TestJoinResidual_FenceAtOpenStart_MovingShardsWedge(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	backing.SetEagerFence(true) // real slatedb: fence at open-START, not completion
	mutate := func(cfg *cluster.Config) { cfg.WriteTimeout = 300 * time.Millisecond }

	n1 := startReplicatedNodeCfg(t, "jr-a", "", uc, rf, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "jr-b", n1.BindAddr, uc, rf, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "jr-c", n1.BindAddr, uc, rf, backing, mutate)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)

	uk := oneKeyPerUnit(t, n1.Cluster, uc)
	plantAllServingMarkers(t, n1.Handle, uc, rf)

	old3 := []string{"jr-a", "jr-b", "jr-c"}
	new4 := []string{"jr-a", "jr-b", "jr-c", "jr-d"}
	var moving, stable []storageunit.UnitID
	for u := range uk {
		if contains(replicaIDsForMembers(new4, u, rf), "jr-d") && !contains(replicaIDsForMembers(old3, u, rf), "jr-d") {
			moving = append(moving, u)
		} else {
			stable = append(stable, u)
		}
	}
	sort.Slice(moving, func(i, j int) bool { return moving[i] < moving[j] })
	if len(moving) == 0 {
		t.Skip("no unit moves onto the 4th node under this hashing")
	}
	t.Logf("of %d units: %d MOVE onto jr-d, %d stay put (eager-fence: real-slatedb timing)", uc, len(moving), len(stable))

	const mountDelay = 25 * time.Second
	n4 := startReplicatedNodeSlowAcquire(t, "jr-d", n1.BindAddr, uc, rf, backing, mountDelay, 300*time.Millisecond)
	cs4 := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}
	if err := waitForMembersAll(cs4, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	if len(n1.Cluster.Members()) != 4 {
		t.Fatalf("entry node not converged to 4 members")
	}

	// WAIT for jr-d to actually START opening (under eager-fence, OpenReplicaUnit
	// bumps the durable epoch + fences the displaced owner BEFORE its long sleep):
	// poll moving-shard writes until one FAILS mid-acquire while jr-d is still
	// mid-mount. That is the fence engaging. If NO moving-shard write ever fails
	// while jr-d warms (it stays available the whole time), the union covers the
	// mount even under fence-at-open-start, and the fence-timing is REFUTED as the
	// residual - we say so.
	fenceEngaged := false
	fenceDeadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(fenceDeadline) {
		if desiredButUnmounted(n4.Cluster) == 0 {
			break // jr-d finished warming without ever wedging
		}
		if isMidAcquire(n1.Cluster.Put([]byte(uk[moving[0]]), []byte("engage"))) {
			fenceEngaged = true
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if !fenceEngaged {
		t.Logf("REFUTED: no moving-shard write ever wedged under eager-fence while jr-d warmed - the union covers "+
			"the mount even with fence-at-open-start. The fence-timing is NOT the residual (jr-d unmounted=%d).",
			desiredButUnmounted(n4.Cluster))
		for _, n := range []*sharedNode{n1, n2, n3, n4} {
			n.Handle.SetAcquireDelay(0)
		}
		t.Skip("fence-timing did not reproduce the residual; see log")
	}

	// MEASURE the sustained wedge while jr-d is still mid-mount (fence engaged).
	var movOK, movMid, movOther, stOK, stMid, stOther int
	rounds := 0
	for r := 0; r < 5 && desiredButUnmounted(n4.Cluster) > 0; r++ {
		o, m, x := probeClass(n1.Cluster, uk, moving)
		movOK += o
		movMid += m
		movOther += x
		o, m, x = probeClass(n1.Cluster, uk, stable)
		stOK += o
		stMid += m
		stOther += x
		rounds++
		time.Sleep(400 * time.Millisecond)
	}
	for _, n := range []*sharedNode{n1, n2, n3, n4} {
		n.Handle.SetAcquireDelay(0)
	}
	movTotal := movOK + movMid + movOther
	t.Logf("RESIDUAL (eager-fence) MOVING-shard writes during mount: ok=%d midAcquire=%d other=%d (total=%d)", movOK, movMid, movOther, movTotal)
	t.Logf("RESIDUAL (eager-fence) STABLE-shard writes during mount: ok=%d midAcquire=%d other=%d", stOK, stMid, stOther)

	if rounds == 0 || movTotal == 0 {
		t.Fatalf("no during-mount moving-shard probes ran (jr-d mounted too fast; raise mountDelay)")
	}
	if movMid == 0 {
		t.Fatalf("expected the fence-at-open-start RESIDUAL to WEDGE moving-shard writes (mid-acquire), saw none "+
			"(ok=%d) - the residual did not reproduce", movOK)
	}
	if rate := float64(movMid) / float64(movTotal); rate < 0.6 {
		t.Fatalf("residual not dominant: only %.0f%% of moving-shard writes wedged under eager-fence (ok=%d mid=%d)", rate*100, movOK, movMid)
	}
	t.Logf("RESIDUAL REPRODUCED: with fence-at-open-start (real slatedb), %d/%d moving-shard writes WEDGE during "+
		"jr-d's mount because the displaced owner is fenced the instant jr-d begins opening its position - so the "+
		"union's displaced-owner leg returns the fence (transient) and only the un-displaced co-replica acks (1 of 2). "+
		"The default fence-at-completion timing hides this. Stable shards stay available (%d ok).", movMid, movTotal, stOK)
}

func TestJoinIsWriteTransparent_MovingShardsStayAvailable(t *testing.T) {
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
	// owned positions at Open (a live long-running cluster has markers everywhere),
	// advertises the Joining bit, and then re-acquires them SLOWLY via the reconcile
	// overlap path - the path the slow acquire delay governs. Without this the joiner
	// mounts everything instantly at Open and never advertises Joining.
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

	// Join the 4th node with the acquire delay armed BEFORE Open: it boot-defers its
	// owned positions (markers planted) -> advertises Joining -> re-acquires them
	// SLOWLY via the overlap reconcile, so every moving shard sits owned-but-unmounted
	// on jn-d for the window while the DISPLACED owner keeps serving it via the union.
	// A long mount so jn-d is provably still warming through the whole measurement,
	// with generous headroom for the slower convergence under `-race`.
	const mountDelay = 25 * time.Second
	n4 := startReplicatedNodeSlowAcquire(t, "jn-d", n1.BindAddr, uc, rf, backing, mountDelay, 300*time.Millisecond)
	cs4 := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}
	if err := waitForMembersAll(cs4, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	// Ensure the entry node (n1, an original) routes with the NEW 4-node ring.
	if len(n1.Cluster.Members()) != 4 {
		t.Fatalf("entry node not converged to 4 members")
	}

	// TRANSPARENCY-ENGAGED GATE: poll until a moving-shard write SUCCEEDS through n1
	// (proving n1 observed jn-d's Joining bit and formed the union), REQUIRING that
	// jn-d is still mid-mount at that instant (it still has owned-but-unmounted
	// positions). Keying on jn-d's ACTUAL mount progress (desiredButUnmounted), not
	// wall-clock, makes this robust under -race: if the write is available while
	// jn-d has not finished mounting, that is transparency DURING the mount, not the
	// wedge healing at mount-end.
	probeUnit := moving[0]
	engaged := false
	var lastErr error
	engageDeadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(engageDeadline) {
		if desiredButUnmounted(n4.Cluster) == 0 {
			break // jn-d finished mounting before we could observe the transition (see below)
		}
		err := n1.Cluster.Put([]byte(uk[probeUnit]), []byte("engage"))
		if err == nil {
			engaged = true
			break
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	unmountedAtEngage := desiredButUnmounted(n4.Cluster)
	if !engaged {
		t.Logf("DIAG: engage never succeeded; lastErr=%v jn-d desired-but-unmounted=%d", lastErr, unmountedAtEngage)
		t.Logf("DIAG probeUnit=%d old-replicas=%v new-replicas=%v", probeUnit,
			replicaIDsForMembers(old3, probeUnit, rf), replicaIDsForMembers(new4, probeUnit, rf))
		for _, n := range []*sharedNode{n1, n2, n3, n4} {
			t.Logf("DIAG node %s state:\n%s", n.ID, n.Cluster.DebugState())
		}
		t.Fatalf("moving-shard write never became available while jn-d was mid-mount - the JOIN did not become " +
			"write-transparent (the union did not form / Joining bit did not engage)")
	}
	if unmountedAtEngage == 0 {
		t.Fatalf("jn-d finished mounting before the transition could be observed - raise mountDelay for headroom")
	}
	t.Logf("transparency ENGAGED while jn-d still has %d owned-but-unmounted moving positions - provably mid-mount", unmountedAtEngage)

	// MEASURE while jn-d is still mid-acquire (still has owned-but-unmounted
	// positions), so this is a during-mount measurement, not the post-mount steady
	// state. Gate every round on jn-d's mount progress rather than wall-clock.
	var movOK, movMid, movOther, stOK, stMid, stOther int
	rounds := 0
	for r := 0; r < 4 && desiredButUnmounted(n4.Cluster) > 0; r++ {
		o, m, x := probeClass(n1.Cluster, uk, moving)
		movOK += o
		movMid += m
		movOther += x
		o, m, x = probeClass(n1.Cluster, uk, stable)
		stOK += o
		stMid += m
		stOther += x
		rounds++
		time.Sleep(400 * time.Millisecond)
	}
	movTotal := movOK + movMid + movOther
	stTotal := stOK + stMid + stOther
	if rounds == 0 || movTotal == 0 {
		t.Fatalf("no during-mount moving-shard probes ran (jn-d mounted too fast; raise mountDelay)")
	}
	t.Logf("measured %d rounds while jn-d mid-mount", rounds)
	t.Logf("MOVING-shard writes during acquire: ok=%d midAcquire=%d other=%d (total=%d)", movOK, movMid, movOther, movTotal)
	t.Logf("STABLE-shard writes during acquire: ok=%d midAcquire=%d other=%d (total=%d)", stOK, stMid, stOther, stTotal)

	// Drop the delay so cleanup + any readback is fast.
	for _, n := range []*sharedNode{n1, n2, n3, n4} {
		n.Handle.SetAcquireDelay(0)
	}

	// ASSERTIONS (the JOIN is now write-transparent, the mirror of the leave gate):
	// 1. Moving shards stay AVAILABLE through the slow mount (the inverse of the old
	//    24/24 wedge): the union acks over the displaced owner + survivor.
	if movTotal == 0 {
		t.Fatalf("no moving-shard probes ran")
	}
	if rate := float64(movOK) / float64(movTotal); rate < 0.9 {
		t.Fatalf("JOIN not write-transparent: only %.0f%% of moving-shard writes acked during jn-d's slow mount "+
			"(want >=90%%); ok=%d mid=%d other=%d - the displaced owner is not being kept in the write set via the "+
			"Joining-bit union", rate*100, movOK, movMid, movOther)
	}
	// 2. Stable shards stay AVAILABLE too (they never route to jn-d): zero mid-acquire.
	if stMid > 0 {
		t.Fatalf("unexpected: %d STABLE-shard writes wedged mid-acquire (stable shards must never wedge)", stMid)
	}
	t.Logf("CONFIRMED: JOIN is WRITE-TRANSPARENT - %d/%d moving-shard writes ACKED during jn-d's slow mount "+
		"(before the fix this measured 24/24 WEDGED), and stable shards stayed available (%d/%d ok). The newcomer's "+
		"Joining bit keeps the displaced replica a CURRENT owner so the union always has a mounted holder in the "+
		"write set - the exact mirror of the graceful-leave path.", movOK, movTotal, stOK, stTotal)
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
		// Survivors acquire the leaver's positions with a 6s mock delay armed
		// across several positions at once; the default node-wide open bound (1)
		// would serialize them past the drain budget. No FFI hazard in the mock
		// double; the bound is pinned separately in pkg/cluster.
		cfg.OpenConcurrency = 8
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
	t.Logf("CONFIRMED (symmetry): a graceful LEAVE keeps the moving shards AVAILABLE (%d/%d acked through the "+
		"successor's slow mount) - the Draining bit engages the current/pending union so a mounted holder is always "+
		"in the write set. The JOIN now has the mirror coverage (see TestJoinIsWriteTransparent_MovingShardsStayAvailable): "+
		"same slow mount, the moving shards stay available because the Joining bit keeps the displaced owner a current "+
		"owner - the exact entry-side mirror of this leave path.", ok, total)
}
