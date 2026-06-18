package integration

// THE ALL-PODS-RESTART REGRESSION GATE (v0.8 Phase 2g, the CAS-lease fix).
//
// A full-cluster restart - EVERY node of an R=2 multi-backend cluster goes down
// and comes back near-simultaneously on the SAME durable backing - used to WEDGE
// replica positions. Two flaws in the pre-fix serving marker combined:
//
//   1. The marker payload was a BARE EPOCH (no owner identity, no heartbeat), so a
//      booting node could not tell "a LIVE peer is serving this position" from "a
//      DEAD peer left this marker behind". On an all-pods restart every marker is
//      stale-but-present, so every node deferred to ghosts ("0 mounted, N
//      DEFERRED") - OR, with the boot-defer disabled, every node treated every
//      marker as a ghost to take over.
//   2. The marker was written last-write-wins (no CAS), so it could not act as a
//      single-winner ownership lock. The actual ownership claim WAS opening the
//      slatedb writer, and a writer-open BUMPS THE DURABLE EPOCH = a fence. So when
//      multiple booting nodes opened the SAME position in parallel, they FENCE-
//      FOUGHT: each open fenced the others, leaving positions in a permanent
//      re-acquire loop the cluster never converged out of.
//
// The fix (docs/SPEC.md "v0.8 Phase 2g"): the marker carries {epoch, owner,
// heartbeat}; staleness is detectable (owner not live OR heartbeat aged out); and
// a position is CLAIMED by winning a compare-and-swap on the lease object BEFORE
// opening, so exactly ONE node opens per contended round and the fence is always
// uncontested. Convergence is gated on a settled gossip ring so a node does not
// CAS-steal from a peer whose gossip has not yet arrived.
//
// This file pins the property end to end on the in-process shared-backing double
// (which simulates the MinIO conditional-PUT CAS + the durable per-replica fence
// epoch). It is kept honest by a BREAK-DEMO (the _ForceClaimWin subtest) that
// flips Config.TestingForceLeaseClaimWin to reproduce the pre-fix "claim = open,
// no single-winner" behavior and asserts the fence-fight reappears - proving the
// CAS claim is load-bearing, not rubber-stamped.

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
)

// startReplicatedNodeCfgHandle is startReplicatedNodeCfg but takes a PRE-BUILT
// handle (so the caller can pass a node-id-labeled HandleFor for open accounting)
// in addition to the Config mutate hook. Same R=2 WriteAll/ReadAll multi-backend
// shape as the other shared-backing fixtures.
func startReplicatedNodeCfgHandle(
	t *testing.T,
	id, seedAddr string,
	unitCount, replicationFactor int,
	h *sharedfactory.Handle,
	mutate func(*cluster.Config),
) *sharedNode {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startReplicatedNodeCfgHandle %s: listen gRPC: %v", id, err)
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
	if seedAddr != "" {
		cfg.Seeds = []string{seedAddr}
	}
	if mutate != nil {
		mutate(&cfg)
	}

	c, bindAddr := openClusterRetryBind(t, cfg)

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

// startReplicatedNodeFenceCfg is startReplicatedNode + a Config mutate hook AND a
// node-id-labeled handle (HandleFor), so the test can (a) thread
// TestingForceLeaseClaimWin into every node for the break-demo and (b) attribute
// each position's opens to a node id for the exactly-one-opener assertion.
func startReplicatedNodeFenceCfg(
	t *testing.T,
	id, seedAddr string,
	unitCount, replicationFactor int,
	backing *sharedfactory.Backing,
	mutate func(*cluster.Config),
) *sharedNode {
	t.Helper()
	return startReplicatedNodeCfgHandle(t, id, seedAddr, unitCount, replicationFactor, backing.HandleFor(id), mutate)
}

// allDesiredReplicaUnits enumerates EVERY desired gen-0 replica position in the
// cluster: for each unit 0..unitCount-1, the ordered R replica nodes under the
// ring give R positions (unit, replica-index). This is the full set the cluster
// must serve - the assertion target for "every desired position converged". It is
// derived purely from the ring (the same deterministic hashing the cluster uses),
// so it needs no cluster internals.
func allDesiredReplicaUnits(c *cluster.Cluster, unitCount, r int) []storageunit.ReplicaUnit {
	rg := ring.New()
	for _, m := range c.Members() {
		rg.Add(m)
	}
	out := make([]storageunit.ReplicaUnit, 0, unitCount*r)
	for u := 0; u < unitCount; u++ {
		gu := storageunit.NewGenUnit(0, storageunit.UnitID(u))
		members := rg.LocateKeyN(gen0UnitBytes(storageunit.UnitID(u)), r)
		for pos := range members {
			out = append(out, storageunit.NewReplicaUnit(gu, uint8(pos)))
		}
	}
	return out
}

// TestAllPodsRestart_CASLease_ConvergesNoFenceFight is the positive gate: with the
// CAS-lease claim ON (default), an all-pods-at-once restart of a 3-node R=2
// cluster on the same backing must converge to a serving state with NO fence-fight
// (each position opened by exactly ONE node during recovery) and full durability.
func TestAllPodsRestart_CASLease_ConvergesNoFenceFight(t *testing.T) {
	runAllPodsRestart(t, false)
}

// TestAllPodsRestart_ForceClaimWin_ReproducesFenceFight is the BREAK-DEMO: flip
// TestingForceLeaseClaimWin so the boot claim ALWAYS "wins" (no live-defer, no CAS
// single-winner) - the pre-fix behavior. Every node then opens every stale-marked
// position in parallel and they fence-fight. The test asserts the fence-fight is
// OBSERVABLE (at least one position opened by MULTIPLE distinct nodes during
// recovery), proving the positive gate's exactly-one-opener assertion has teeth.
func TestAllPodsRestart_ForceClaimWin_ReproducesFenceFight(t *testing.T) {
	runAllPodsRestart(t, true)
}

// runAllPodsRestart is the shared driver. forceClaimWin = false exercises the FIX
// (assert convergence + no fence-fight + durability); forceClaimWin = true
// exercises the BREAK (assert the fence-fight reappears). Splitting the assertion
// on forceClaimWin keeps the two halves reading off ONE code path so they cannot
// drift.
func runAllPodsRestart(t *testing.T, forceClaimWin bool) {
	const (
		unitCount = 8
		r         = 2
	)
	backing := sharedfactory.NewBacking()

	mutate := func(cfg *cluster.Config) {
		cfg.TestingForceLeaseClaimWin = forceClaimWin
	}

	// Phase 1: stand up a 3-node R=2 cluster (CAS claim default-on for the initial
	// convergence in BOTH variants, so the baseline is identically placed) and write
	// a recorded dataset. The break toggle only matters at the RESTART boot, which is
	// where the fence-fight lives.
	n1 := startReplicatedNodeFenceCfg(t, "apr-a", "", unitCount, r, backing, nil)
	n2 := startReplicatedNodeFenceCfg(t, "apr-b", n1.BindAddr, unitCount, r, backing, nil)
	n3 := startReplicatedNodeFenceCfg(t, "apr-c", n1.BindAddr, unitCount, r, backing, nil)
	nodes := []*sharedNode{n1, n2, n3}
	cs := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(cs, 3, 20*time.Second); err != nil {
		t.Fatalf("initial 3-node convergence: %v", err)
	}
	// Gate on the routed write path completing a CLEAN sweep through every node
	// before the baseline: ring convergence does not prove every position finished
	// its initial acquire, and a baseline Put into a still-acquiring position is a
	// spurious Unavailable, not the bug under test. waitForWriteReady retries the
	// transient acquiring-window errors until a full clean pass (every entry, spread
	// of keys) succeeds.
	waitForWriteReady(t, cs, 60*time.Second)

	want, _ := writeRecordedDataset(t, n1.Cluster)
	t.Logf("all-pods-restart: wrote %d recorded keys, killing all nodes (forceClaimWin=%v)", len(want), forceClaimWin)

	// Snapshot every desired position so we can read the per-position open log after
	// recovery. The ring is identical across all members, so any cluster computes it.
	desired := allDesiredReplicaUnits(n1.Cluster, unitCount, r)

	// Reset the per-position open accounting so the assertion counts ONLY the opens
	// that happen during RECOVERY (the initial convergence opened every position
	// once already; the fence-fight - if any - is a property of the restart boot).
	backing.ResetOpens()

	// Phase 2: MASS KILL - tear every node down near-simultaneously. The durable
	// backing (markers + per-replica bytes + fence epochs) persists; the processes
	// do not.
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n *sharedNode) { defer wg.Done(); n.stop(); _ = n.Cluster.Close() }(n)
	}
	wg.Wait()
	t.Logf("all-pods-restart: all nodes down; restarting on the same backing (founder-first to stage the convergence race)")

	// Phase 3: RESTART all nodes on the SAME backing, same ids. The markers persist
	// (every one stale: no recorded owner is in the freshly-forming member set), so
	// the boot path must re-form ownership.
	//
	// To make the convergence-RACE fence-fight DETERMINISTIC (the regime the CAS
	// single-winner defends), the founder boots FIRST and is given a beat to run its
	// boot mount while it is STILL ALONE: a 1-node ring desires replica-0 of EVERY
	// unit, so the solo founder opens every replica-0 position. THEN the joiners
	// arrive; the ring re-forms and the REAL replica-0 owners (apr-b / apr-c for the
	// units the founder does not own in the 3-node ring) reconcile to those
	// positions. With the CAS claim ON, the founder's solo opens lost/deferred and a
	// position has ONE opener; with it bypassed (break toggle), BOTH the founder and
	// the real owner open the SAME position -> fence-fight. The founder's boot-defer
	// does NOT save it: post-restart every marker is stale (no live owner), so the
	// solo founder treats them all as claimable. This sequencing is the in-process
	// stand-in for the staggered gossip convergence a real all-pods rollout has.
	r1 := startReplicatedNodeFenceCfg(t, "apr-a", "", unitCount, r, backing, mutate)
	// Let the solo founder form its 1-node ring and run its boot mount (open every
	// replica-0 position) before any peer arrives.
	waitForSomeOpens(t, backing, desired, 10*time.Second)
	r2 := startReplicatedNodeFenceCfg(t, "apr-b", r1.BindAddr, unitCount, r, backing, mutate)
	r3 := startReplicatedNodeFenceCfg(t, "apr-c", r1.BindAddr, unitCount, r, backing, mutate)
	restarted := []*sharedNode{r1, r2, r3}
	rcs := []*cluster.Cluster{r1.Cluster, r2.Cluster, r3.Cluster}
	if err := waitForMembersAll(rcs, 3, 30*time.Second); err != nil {
		t.Fatalf("post-restart membership convergence: %v", err)
	}

	if forceClaimWin {
		assertFenceFightReproduced(t, backing, desired, restarted)
		return
	}
	assertConvergedNoFenceFight(t, backing, desired, restarted, want)
}

// assertConvergedNoFenceFight is the FIX-ON oracle. After the all-pods restart with
// the CAS claim active, every desired position must (1) converge to a serving state
// - writes + reads to every key succeed (W=2 reachable, nothing wedged), (2) have
// been opened during recovery by AT MOST ONE distinct node (no fence-fight), and
// (3) carry a LIVE lease owned by a current member.
func assertConvergedNoFenceFight(
	t *testing.T,
	backing *sharedfactory.Backing,
	desired []storageunit.ReplicaUnit,
	restarted []*sharedNode,
	want map[string][]byte,
) {
	t.Helper()

	// (1) AUTO-RECOVERY: a canary write must succeed through SOME entry without any
	// manual nudge. A permanently-wedged position makes this time out = the bug.
	if !pollWriteRecovers(t, restarted, 90*time.Second) {
		t.Fatalf("cluster did NOT auto-recover writes within 90s after all-pods restart")
	}
	t.Logf("all-pods-restart: writes auto-recovered")

	// Give the reconcile loop room to settle every position past the convergence
	// gate (15s settle window) before reading the open log + leases, so a position
	// that needed one post-convergence re-acquire is not misread as wedged.
	if err := waitForEveryPositionLive(t, backing, desired, restarted, 90*time.Second); err != nil {
		t.Fatalf("not every desired position reached a live lease after restart: %v", err)
	}

	// (2) NO FENCE-FIGHT: each desired position opened by AT MOST one distinct node
	// during recovery. The CAS makes a second opener impossible by construction; a
	// regression (e.g. opening before/without the CAS) shows up as 2+ distinct
	// openers here.
	multiOpened := 0
	for _, ru := range desired {
		openers := backing.DistinctOpeners(ru)
		if len(openers) > 1 {
			multiOpened++
			if multiOpened <= 5 {
				t.Errorf("FENCE-FIGHT: position %s opened by %d distinct nodes during recovery: %v (CAS must yield exactly one opener)", ru, len(openers), openers)
			}
		}
	}
	if multiOpened > 0 {
		t.Fatalf("FENCE-FIGHT detected after all-pods restart: %d of %d positions opened by >1 node", multiOpened, len(desired))
	}
	t.Logf("all-pods-restart: no fence-fight (every position opened by <=1 node during recovery)")

	// (3) DURABILITY: every recorded (acked) key still readable + correct, proving
	// no position was lost to a wedge.
	lost, corrupt := 0, 0
	for k, v := range want {
		got, err := getWithConvergenceRetry(restarted[0].Cluster, k, 20*time.Second)
		if err != nil {
			lost++
			if lost <= 5 {
				t.Errorf("DURABILITY: acked key %q unreadable after all-pods restart: %v", k, err)
			}
			continue
		}
		if !bytes.Equal(got, v) {
			corrupt++
			if corrupt <= 5 {
				t.Errorf("DURABILITY: acked key %q corrupt: got %q want %q", k, got, v)
			}
		}
	}
	if lost > 0 || corrupt > 0 {
		t.Fatalf("DURABILITY VIOLATED after all-pods restart: %d lost, %d corrupt of %d", lost, corrupt, len(want))
	}
	t.Logf("all-pods-restart: durability held (%d keys all readable + correct)", len(want))
}

// assertFenceFightReproduced is the BREAK-DEMO oracle. With the CAS single-winner
// disabled (TestingForceLeaseClaimWin), every booting node opens every stale-marked
// position in parallel and they fence-fight. The signature of that fight in the
// open accounting is unmistakable: at least one position was opened by MULTIPLE
// distinct nodes during recovery. We give the boot a moment to run its parallel
// claims, then read the open log. (We do NOT assert auto-recovery or durability
// here: the point is to show the FIX's exactly-one-opener assertion CATCHES the old
// behavior - the cluster may or may not eventually limp to convergence, which is
// exactly the unreliability the fix removes.)
func assertFenceFightReproduced(
	t *testing.T,
	backing *sharedfactory.Backing,
	desired []storageunit.ReplicaUnit,
	restarted []*sharedNode,
) {
	t.Helper()
	// The break boot opens positions in parallel goroutines; poll the open log until
	// a multi-opener position appears or a bounded deadline. With the CAS bypassed,
	// multiple nodes claim->open the same stale-marked position, so a fence-fight
	// surfaces quickly (every node desires its ring positions + opens them with no
	// single-winner arbitration).
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		for _, ru := range desired {
			if len(backing.DistinctOpeners(ru)) > 1 {
				openers := backing.DistinctOpeners(ru)
				t.Logf("all-pods-restart BREAK-DEMO: fence-fight reproduced - position %s opened by %d distinct nodes: %v", ru, len(openers), openers)
				return
			}
		}
		// Nudge a write through each entry to drive the reconcile/acquire loop.
		for _, n := range restarted {
			_ = n.Cluster.Put([]byte("break-nudge"), []byte("x"))
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("BREAK-DEMO FAILED: forceClaimWin did NOT reproduce a fence-fight (no position opened by >1 node) - the positive gate's exactly-one-opener assertion would have no teeth")
}

// waitForSomeOpens blocks until the solo founder has opened at least a few
// positions (its 1-node-ring boot mount has run) or the deadline, so the joiners
// arrive AFTER the founder has staked its solo-ring claims - the deterministic
// convergence-race setup for the fence-fight break-demo.
func waitForSomeOpens(t *testing.T, backing *sharedfactory.Backing, desired []storageunit.ReplicaUnit, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		opened := 0
		for _, ru := range desired {
			if len(backing.OpenerHistory(ru)) > 0 {
				opened++
			}
		}
		if opened >= 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("waitForSomeOpens: founder opened no positions within %s (boot mount may not have run solo)", timeout)
}

// waitForEveryPositionLive polls until every desired position carries a serving
// lease whose owner is a CURRENT member (the converged, no-ghost steady state), or
// the deadline. It is the deterministic stand-in for "the reconcile loop has
// re-formed ownership for every position after the all-at-once boot."
func waitForEveryPositionLive(
	t *testing.T,
	backing *sharedfactory.Backing,
	desired []storageunit.ReplicaUnit,
	restarted []*sharedNode,
	timeout time.Duration,
) error {
	t.Helper()
	memberIDs := map[string]bool{}
	for _, n := range restarted {
		memberIDs[n.ID] = true
	}
	deadline := time.Now().Add(timeout)
	var lastMissing storageunit.ReplicaUnit
	var lastReason string
	for time.Now().Before(deadline) {
		allLive := true
		for _, ru := range desired {
			lease, _, present := backing.ServingLease(ru)
			if !present {
				allLive, lastMissing, lastReason = false, ru, "no lease present"
				break
			}
			if lease.Owner == "" || !memberIDs[lease.Owner] {
				allLive, lastMissing, lastReason = false, ru, fmt.Sprintf("owner %q not a current member", lease.Owner)
				break
			}
		}
		if allLive {
			return nil
		}
		// Drive the reconcile loop with a benign write per entry while we wait.
		for _, n := range restarted {
			_ = n.Cluster.Put([]byte("settle-nudge"), []byte("x"))
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("position %s never reached a live lease (%s)", lastMissing, lastReason)
}

// --- single-node-restart coverage (the case that worked pre-fix and must keep
// working post-fix): one node reboots while its peers stay UP. It must DEFER to the
// live peers' fresh leases at boot - never CAS-steal a position a live peer is
// serving - so no peer is fenced and the rebooted node only re-takes the positions
// the ring assigns it once gossip converges.

// TestSingleNodeRestart_DefersToLivePeers brings up a 3-node R=2 cluster, restarts
// ONE node, and asserts (1) writes/reads stay available throughout (the live peers
// keep serving) and (2) NO position the two SURVIVING peers were serving got opened
// by the rebooting node during its boot (no steal of a live lease).
func TestSingleNodeRestart_DefersToLivePeers(t *testing.T) {
	const (
		unitCount = 8
		r         = 2
	)
	backing := sharedfactory.NewBacking()
	n1 := startReplicatedNodeFenceCfg(t, "snr-a", "", unitCount, r, backing, nil)
	n2 := startReplicatedNodeFenceCfg(t, "snr-b", n1.BindAddr, unitCount, r, backing, nil)
	n3 := startReplicatedNodeFenceCfg(t, "snr-c", n1.BindAddr, unitCount, r, backing, nil)
	cs := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(cs, 3, 20*time.Second); err != nil {
		t.Fatalf("initial 3-node convergence: %v", err)
	}
	waitForWriteReady(t, cs, 60*time.Second)

	want, _ := writeRecordedDataset(t, n1.Cluster)
	desired := allDesiredReplicaUnits(n1.Cluster, unitCount, r)

	// Reset open accounting, then reboot node n3 only (n1+n2 stay up + keep serving).
	backing.ResetOpens()
	n3.stop()
	_ = n3.Cluster.Close()
	t.Logf("single-node-restart: n3 down; peers stay up")

	// While n3 is down + rebooting, writes must keep succeeding through a live peer.
	if !pollWriteRecovers(t, []*sharedNode{n1, n2}, 30*time.Second) {
		t.Fatalf("writes did NOT stay available with one node down")
	}

	r3 := startReplicatedNodeFenceCfg(t, "snr-c", n1.BindAddr, unitCount, r, backing, nil)
	rcs := []*cluster.Cluster{n1.Cluster, n2.Cluster, r3.Cluster}
	if err := waitForMembersAll(rcs, 3, 30*time.Second); err != nil {
		t.Fatalf("post-single-restart convergence: %v", err)
	}

	// CORE ASSERTION: any position whose live owner (per the lease) is a SURVIVING
	// peer (n1 or n2) must NOT have been opened by the rebooting node n3 during its
	// boot - the boot-defer keeps it from fencing the live peer. We let the cluster
	// settle, then check the open log: no position opened by snr-c whose lease owner
	// is snr-a/snr-b.
	survivors := map[string]bool{"snr-a": true, "snr-b": true}
	time.Sleep(2 * time.Second)
	stole := 0
	for _, ru := range desired {
		lease, _, present := backing.ServingLease(ru)
		if !present || !survivors[lease.Owner] {
			continue // owned by the rebooted node or unsettled; not a steal case.
		}
		for _, opener := range backing.OpenerHistory(ru) {
			if opener == "snr-c" {
				stole++
				if stole <= 5 {
					t.Errorf("STEAL: rebooting node snr-c opened position %s during boot though live peer %q serves it (must defer, not fence)", ru, lease.Owner)
				}
				break
			}
		}
	}
	if stole > 0 {
		t.Fatalf("single-node restart fenced live peers: %d positions stolen by the rebooting node", stole)
	}

	// Durability sanity: every recorded key still readable + correct.
	for k, v := range want {
		got, err := getWithConvergenceRetry(n1.Cluster, k, 20*time.Second)
		if err != nil {
			t.Fatalf("DURABILITY: key %q unreadable after single-node restart: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("DURABILITY: key %q corrupt: got %q want %q", k, got, v)
		}
	}
	t.Logf("single-node-restart: deferred to live peers (no steal), durability held")
}
