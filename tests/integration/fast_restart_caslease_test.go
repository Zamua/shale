package integration

// FAST-RESTART + SELF-HEAL coverage for the v0.8 Phase 2g CAS-lease marker.
//
// The all-pods-restart regression (all_pods_restart_caslease_test.go) only
// exercises the SLOW restart path: it injects enough real wall-clock (the
// baseline dataset write + the convergence waits) that every persisted lease
// heartbeat AGES PAST the lease window before recovery, so recovery is driven by
// the heartbeat-age BACKSTOP. The FAST path - a restart where heartbeats are
// still FRESHER than the lease window when the cluster comes back - was UNTESTED.
//
// This file pins the path the wall-clock test cannot reach. It uses the injectable
// CAS-lease timing knobs (TestingSetCASLeaseTiming) to:
//
//   - hold the heartbeat clock at a strictly-advancing-but-TINY delta and set the
//     lease window HUGE, so a persisted heartbeat NEVER ages out: the heartbeat-age
//     backstop is structurally DISABLED, and recovery can ONLY proceed through the
//     boot convergence seed (or a node re-claiming its OWN lease);
//   - shrink the convergence-settle window so the seeded settle clock crosses it
//     within a bounded, deterministic deadline instead of racing wall clock.
//
// Two tests:
//
//   1. TestAllPodsRestart_FastHeartbeats_ConvergesNoFenceFight - the behavioral
//      gate: with heartbeats fresh-forever, an all-pods restart still converges
//      with NO fence-fight and full durability, BOUNDED by the convergence window
//      rather than the (disabled) heartbeat backstop.
//   2. TestCASLeaseBootSeed_AllowsStealWithoutReconcile - the deterministic TEETH:
//      a freshly-booted node, with its convergence clock advanced past the settle
//      window but WITHOUT any reconcile pass having run, reports
//      convergedEnoughToSteal == true. That is true ONLY because mountReplicaUnits
//      SEEDED the convergence tracker at boot; reverting that one seed line makes
//      this assertion fail (the tracker stays unseeded until the first reconcile).

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

// freshHeartbeatClock returns a nowUnixMs function that STRICTLY ADVANCES (so two
// consecutive lease writes are never byte-identical - faithful to the content-hash
// ETag ABA concern the production monotonicUnixMs guards) but only by 1ms per call
// off a fixed base. Paired with a HUGE leaseWindow, the recorded heartbeats stay
// far inside the window forever, so no lease ever ages out: the heartbeat-age
// backstop is disabled and the test exercises the convergence-seed path alone.
func freshHeartbeatClock() func() int64 {
	var n int64
	base := time.Now().UnixMilli()
	return func() int64 { return base + atomic.AddInt64(&n, 1) }
}

// withFastCASLeaseTiming installs the fast-restart timing regime for the duration
// of the test: a strictly-advancing tiny heartbeat clock, a HUGE lease window (the
// heartbeat backstop can never fire), and a SHORT convergence-settle window (so
// recovery is bounded by the seeded settle clock, deterministically). The real
// convergence wall clock is kept (nil) so the settle window elapses by ordinary
// elapsed time once the ring stops changing. Returns the bound the test asserts
// recovery completes within. Restores every knob on cleanup.
func withFastCASLeaseTiming(t *testing.T) (settle time.Duration) {
	t.Helper()
	const (
		hugeLeaseWindow = time.Hour // heartbeats never age out
		settleWindow    = 400 * time.Millisecond
	)
	restore := cluster.TestingSetCASLeaseTiming(hugeLeaseWindow, settleWindow, freshHeartbeatClock(), nil)
	t.Cleanup(restore)
	return settleWindow
}

// withDeterministicCASLeaseTiming installs a DETERMINISTIC CAS-lease timing regime
// for the all-pods / single-node restart tests so their recovery no longer depends
// on incidental real wall-clock heartbeat aging. It uses a strictly-advancing tiny
// heartbeat clock (heartbeat freshness decoupled from how long the test happens to
// take), a lease window comfortably ABOVE the production reconcile interval (so a
// LIVE owner refreshing once per reconcile keeps its lease fresh - required for the
// single-node-restart test, where the rebooting node must keep deferring to its
// surviving peers' refreshed leases), and a SHORT convergence-settle window so the
// CAS-steal gate opens quickly once the ring stops changing. Restores on cleanup.
//
// The reconcile interval is the package default (5s) and is not reachable from this
// package, so the lease window is set to a multiple of it (30s) to leave generous
// refresh headroom. This is the SLOW/steady regime (heartbeats fresh, recovery via
// self-reclaim + the convergence seed); the dedicated fast-heartbeat test pins the
// complementary regime where the heartbeat backstop is disabled entirely.
func withDeterministicCASLeaseTiming(t *testing.T) {
	t.Helper()
	const (
		leaseWindow  = 30 * time.Second // > reconcileInterval (5s) so live leases stay fresh
		settleWindow = 3 * time.Second  // shorter than the 15s default; still > in-process gossip-convergence time
	)
	// Keep the REAL heartbeat clock (nil): the production monotonicUnixMs already
	// guarantees strictly-advancing heartbeats, and a real clock keeps heartbeat
	// aging aligned with the real convergence clock during the initial multi-node
	// bring-up (so a founder's solo-claimed lease for a position that moves to a
	// peer ages out / hands off on the same timeline the convergence gate expects).
	restore := cluster.TestingSetCASLeaseTiming(leaseWindow, settleWindow, nil, nil)
	t.Cleanup(restore)
}

// TestAllPodsRestart_FastHeartbeats_ConvergesNoFenceFight is the FAST-path twin of
// TestAllPodsRestart_CASLease_ConvergesNoFenceFight. With the heartbeat backstop
// structurally disabled (huge lease window + a clock that keeps every heartbeat
// fresh), an all-pods-at-once restart of a 3-node R=2 cluster on the same backing
// must STILL converge with NO fence-fight (each position opened by <= 1 distinct
// node during recovery) and full durability - recovery driven by self-reclaim of
// own leases plus the boot convergence seed, never the (disabled) heartbeat age.
// The existing wall-clock test cannot reach this regime because it lets heartbeats
// age past the window before it asserts.
func TestAllPodsRestart_FastHeartbeats_ConvergesNoFenceFight(t *testing.T) {
	// Bring the INITIAL cluster up under the ordinary (moderate) timing: a huge lease
	// window during the initial multi-node convergence would freeze a founder's
	// solo-claimed lease for a position that belongs to a peer (it never ages out, so
	// the peer could not take it over and W=2 would be unreachable). The FAST regime
	// (huge lease window, heartbeat backstop disabled) is installed below, just
	// before the restart - which is the window under test.
	withDeterministicCASLeaseTiming(t)

	const (
		unitCount = 8
		r         = 2
	)
	backing := sharedfactory.NewBacking()

	// Stand up a 3-node R=2 cluster and write a recorded dataset.
	n1 := startReplicatedNodeFenceCfg(t, "fhr-a", "", unitCount, r, backing, nil)
	n2 := startReplicatedNodeFenceCfg(t, "fhr-b", n1.BindAddr, unitCount, r, backing, nil)
	n3 := startReplicatedNodeFenceCfg(t, "fhr-c", n1.BindAddr, unitCount, r, backing, nil)
	nodes := []*sharedNode{n1, n2, n3}
	cs := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(cs, 3, 20*time.Second); err != nil {
		t.Fatalf("initial 3-node convergence: %v", err)
	}
	waitForWriteReady(t, cs, 60*time.Second)

	want, _ := writeRecordedDataset(t, n1.Cluster)
	desired := allDesiredReplicaUnits(n1.Cluster, unitCount, r)

	// NOW switch to the FAST regime for the restart: a HUGE lease window so the
	// persisted heartbeats NEVER age out (the heartbeat-age backstop is structurally
	// disabled) and a short convergence window. The persisted markers carry recent
	// real-clock heartbeats; under the 1h window they read FRESH at restart, so the
	// boot mount would DEFER to any owner still in the live set - recovery therefore
	// cannot lean on the backstop and must come from self-reclaim + the boot
	// convergence seed. Installed AFTER the dataset write so the initial convergence
	// (above) ran under sane timing.
	settle := withFastCASLeaseTiming(t)
	t.Logf("fast-restart: wrote %d recorded keys; switched to fresh-heartbeat regime (lease window=1h, settle=%s)", len(want), settle)
	backing.ResetOpens()

	// MASS KILL.
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n *sharedNode) { defer wg.Done(); n.stop(); _ = n.Cluster.Close() }(n)
	}
	wg.Wait()
	t.Logf("fast-restart: all nodes down; restarting on the same backing (heartbeats still fresh)")

	// RESTART all three, same ids, founder-first (the same staged convergence race
	// the slow test uses to make the fence-fight deterministic if the CAS regressed).
	r1 := startReplicatedNodeFenceCfg(t, "fhr-a", "", unitCount, r, backing, nil)
	waitForSomeOpens(t, backing, desired, 10*time.Second)
	r2 := startReplicatedNodeFenceCfg(t, "fhr-b", r1.BindAddr, unitCount, r, backing, nil)
	r3 := startReplicatedNodeFenceCfg(t, "fhr-c", r1.BindAddr, unitCount, r, backing, nil)
	restarted := []*sharedNode{r1, r2, r3}
	rcs := []*cluster.Cluster{r1.Cluster, r2.Cluster, r3.Cluster}
	if err := waitForMembersAll(rcs, 3, 30*time.Second); err != nil {
		t.Fatalf("post-restart membership convergence: %v", err)
	}

	// BOUNDED RECOVERY: writes must auto-recover. The bound is the convergence
	// window (plus ring-settle + reconcile slack), NOT the 1h heartbeat age. A
	// generous-but-finite ceiling proves recovery does not hang on the disabled
	// backstop; if recovery secretly depended on heartbeat aging it would never
	// complete here (the window is an hour).
	const recoveryBound = 30 * time.Second
	if !pollWriteRecovers(t, restarted, recoveryBound) {
		t.Fatalf("fast-restart: writes did NOT auto-recover within %s (heartbeats are fresh-forever, so recovery must come from the convergence seed)", recoveryBound)
	}
	t.Logf("fast-restart: writes auto-recovered within the convergence-bounded window")

	if err := waitForEveryPositionLive(t, backing, desired, restarted, recoveryBound); err != nil {
		t.Fatalf("fast-restart: not every position reached a live lease within %s: %v", recoveryBound, err)
	}

	// NO FENCE-FIGHT: each desired position opened by <= 1 distinct node.
	multiOpened := 0
	for _, ru := range desired {
		if openers := backing.DistinctOpeners(ru); len(openers) > 1 {
			multiOpened++
			if multiOpened <= 5 {
				t.Errorf("FENCE-FIGHT: position %s opened by %d distinct nodes during fast restart: %v", ru, len(openers), openers)
			}
		}
	}
	if multiOpened > 0 {
		t.Fatalf("fast-restart FENCE-FIGHT: %d of %d positions opened by >1 node", multiOpened, len(desired))
	}
	t.Logf("fast-restart: no fence-fight (every position opened by <=1 node)")

	// DURABILITY: every acked key still readable + correct.
	lost, corrupt := 0, 0
	for k, v := range want {
		got, err := getWithConvergenceRetry(restarted[0].Cluster, k, 20*time.Second)
		if err != nil {
			lost++
			continue
		}
		if !bytes.Equal(got, v) {
			corrupt++
		}
	}
	if lost > 0 || corrupt > 0 {
		t.Fatalf("fast-restart DURABILITY VIOLATED: %d lost, %d corrupt of %d", lost, corrupt, len(want))
	}
	t.Logf("fast-restart: durability held (%d keys all readable + correct)", len(want))
}

// TestCASLeaseBootSeed_AllowsStealWithoutReconcile is the deterministic TEETH for
// the boot convergence seed (Phase 1 MUST-FIX #1). The seed is what lets a node's
// CAS-steal gate become ready a settle-window after BOOT instead of a settle-window
// after the FIRST RECONCILE pass. With the heartbeat backstop disabled (huge lease
// window), a fast restart that needs to steal a DEPARTED owner's still-fresh lease
// can do so ONLY once the steal gate opens; the seed is what bounds that.
//
// The assertion is white-box and clock-controlled so it cannot flake: boot a single
// solo node against a CONTROLLED convergence clock, advance the clock past the
// settle window WITHOUT running a single reconcile pass, and assert the steal gate
// is OPEN. That can ONLY be true because mountReplicaUnits seeded convergedSince at
// boot. Reverting the seed line (multibackend_replicated.go mountReplicaUnits)
// makes convergedSince stay zero until the first reconcile notes membership, so the
// gate stays CLOSED here and the test FAILS - which is exactly how the teeth were
// confirmed.
func TestCASLeaseBootSeed_AllowsStealWithoutReconcile(t *testing.T) {
	const settleWindow = 500 * time.Millisecond

	// A convergence clock the TEST advances by hand. It starts at a fixed base; the
	// test moves it forward to cross the settle window deterministically, with no
	// dependence on wall-clock timing or on a reconcile having run.
	var advanceMs atomic.Int64
	base := time.Now()
	convClock := func() time.Time { return base.Add(time.Duration(advanceMs.Load()) * time.Millisecond) }

	// Huge lease window (heartbeat backstop disabled) + short settle window + the
	// controlled convergence clock. The heartbeat clock is irrelevant here (no
	// peers), so leave nowUnixMs at its default by passing the fresh tiny clock.
	restore := cluster.TestingSetCASLeaseTiming(time.Hour, settleWindow, freshHeartbeatClock(), convClock)
	defer restore()

	backing := sharedfactory.NewBacking()
	// Boot ONE solo node. Its mountReplicaUnits runs during Open and seeds the
	// convergence tracker with the {self} member set at the boot clock value (which
	// is `base`, advanceMs == 0). No reconcile has been driven; the periodic loop
	// will not have ticked this fast.
	n := startReplicatedNodeFenceCfg(t, "seed-a", "", 4, 2, backing, nil)
	t.Cleanup(n.Close)

	// Immediately after boot, BEFORE advancing the clock, the gate must be CLOSED:
	// only the boot moment has elapsed, which is < the settle window.
	if n.Cluster.TestingConvergedEnoughToSteal() {
		t.Fatalf("steal gate open at boot before the settle window elapsed - the seed must not pre-open the gate")
	}

	// Advance the CONVERGENCE clock past the settle window WITHOUT running a
	// reconcile. The gate flips on the next read of convergedEnoughToSteal only if
	// the tracker was SEEDED at boot (so convergedSince == base, and now - base >
	// settleWindow). Note: convergedEnoughToSteal itself only READS readiness; the
	// readiness is updated by noteMembershipForConvergence, which the periodic
	// reconcile calls. To make the teeth independent of the reconcile cadence we
	// drive ONE explicit reconcile AFTER advancing the clock - that single pass
	// observes (unchanged set {self}) + (clock now past the seeded convergedSince)
	// and flips ready. WITHOUT the boot seed, that same single pass instead SETS
	// convergedSince = now (first observation), leaving the window un-elapsed, so
	// the gate stays closed - the divergence that gives this test its teeth.
	advanceMs.Store(int64(settleWindow/time.Millisecond) + 50)
	n.Cluster.TestingRunReconcile()

	if !n.Cluster.TestingConvergedEnoughToSteal() {
		t.Fatalf("steal gate STILL CLOSED after advancing the convergence clock past the settle window: the boot seed did not start the settle clock at boot (this is the failure mode if mountReplicaUnits' noteMembershipForConvergence seed is reverted)")
	}
	t.Logf("boot-seed teeth: steal gate opened a settle-window after BOOT (seeded convergedSince), not after the first reconcile observed membership")
}

// TestCASLeaseSelfOwnedReclaim_NoSecondOpener covers the "CAS winner crashed after
// claim, before open" self-heal path (Phase 1 P3 #6). The hazard: a node wins the
// lease CAS for a position, then the open FAILS (or the node crashes) before
// OpenReplicaUnit returns. That leaves a SELF-OWNED lease behind with no mount. On
// the node's next acquire it must RE-CLAIM its own lease (the
// claimReplicaLease "a lease THIS node already owns is never deferred to" branch),
// not treat it as a live peer's lease and defer forever - and it must do so WITHOUT
// a second distinct node opening the position (no fence-fight, the single-writer
// invariant holds across the crash).
//
// The fault seam: SetOpenReplicaFault arms an OpenReplicaUnit failure on one
// position. The node CAS-wins that position's lease at boot, then the open faults
// (degraded-boot skip), leaving the self-owned lease. We then CLEAR the fault and
// RESTART the same node id on the same backing; it must re-acquire the position and
// DistinctOpeners must show exactly that ONE node (the faulted open recorded no
// opener, so a second distinct opener would mean a peer fenced in - impossible on a
// solo cluster, and the assertion that pins the invariant).
func TestCASLeaseSelfOwnedReclaim_NoSecondOpener(t *testing.T) {
	// Fast timing: keep the self-owned lease's heartbeat FRESH across the restart
	// (huge lease window) so the path under test is the SELF-OWNED re-claim
	// (owner==me exemption), not the heartbeat-age backstop reclaiming a stale lease.
	withFastCASLeaseTiming(t)

	backing := sharedfactory.NewBacking()
	// The position we poison: replica-0 of gen-0 unit 0, which a solo R=2 node
	// always desires (a 1-node ring places replica-0 of every unit on the sole node).
	ru := storageunit.NewReplicaUnit(storageunit.NewGenUnit(0, storageunit.UnitID(0)), 0)
	openFault := errors.New("sharedfactory: injected open fault (self-owned re-claim test)")

	// Arm the fault BEFORE boot: the node will CAS-WIN ru's lease, then its open
	// will FAIL - the "won the claim, crashed before opening" state.
	backing.SetOpenReplicaFault(ru, openFault)

	n := startReplicatedNodeFenceCfg(t, "sho-a", "", 4, 2, backing, nil)
	// Drive a couple of reconciles so the (still-faulted) acquire retries; each one
	// re-claims the self-owned lease and re-fails the open without recording an open.
	n.Cluster.TestingRunReconcile()
	n.Cluster.TestingRunReconcile()

	// The faulted open must NOT have recorded any opener for ru.
	if got := backing.DistinctOpeners(ru); len(got) != 0 {
		t.Fatalf("a FAULTED open recorded an opener for %s: %v (a failed open must not count as an open)", ru, got)
	}
	// A self-owned lease must exist (the claim wrote it even though the open failed).
	lease, _, present := backing.ServingLease(ru)
	if !present || lease.Owner != "sho-a" {
		t.Fatalf("expected a self-owned lease for %s after the failed open, got present=%v owner=%q", ru, present, lease.Owner)
	}
	t.Logf("self-owned re-claim: node won the CAS for %s then the open faulted; self-owned lease left behind, no opener recorded", ru)

	// RESTART: clear the fault, drop the node, bring the SAME id back on the SAME
	// backing. The fresh node reads its own prior-incarnation lease (owner=sho-a, in
	// the live set, heartbeat fresh) - it must NOT defer to it as a live peer's, it
	// must RE-CLAIM it and open.
	backing.SetOpenReplicaFault(ru, nil)
	n.Close()

	r := startReplicatedNodeFenceCfg(t, "sho-a", "", 4, 2, backing, nil)
	t.Cleanup(r.Close)

	// Drive reconciles until ru is opened (or a bounded deadline). The boot mount
	// DEFERS to the self-owned fresh lease; the reconcile-time acquire's owner==me
	// exemption is what re-claims + opens it.
	deadline := time.Now().Add(15 * time.Second)
	opened := false
	for time.Now().Before(deadline) {
		r.Cluster.TestingRunReconcile()
		if len(backing.OpenerHistory(ru)) > 0 {
			opened = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !opened {
		t.Fatalf("position %s was never re-acquired after the fault cleared: the self-owned lease was not re-claimed (it was deferred to as if a live peer held it)", ru)
	}

	// THE INVARIANT: the position was opened by exactly ONE distinct node (sho-a) -
	// the same node re-acquired its own lease, no second opener, no fence-fight.
	openers := backing.DistinctOpeners(ru)
	if len(openers) != 1 || openers[0] != "sho-a" {
		t.Fatalf("self-owned re-claim opened %s with the wrong opener set %v (want exactly [sho-a]); a second distinct opener means the self-owned lease was fence-fought instead of re-claimed", ru, openers)
	}
	t.Logf("self-owned re-claim: same node sho-a re-acquired %s with exactly one distinct opener (no fence-fight across the crash-before-open)", ru)
}
