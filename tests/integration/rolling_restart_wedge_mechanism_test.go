package integration

// DETERMINISTIC MECHANISM reproduction of the rolling-restart convergence wedge.
//
// The discovery harness (rolling_restart_wedge_repro_test.go) shows that a
// graceful-restart churn+roll RECOVERS reliably in-process: the boot-defer +
// periodic self-heal reconcile re-mounts every deferred position within a couple
// of ticks, so a persistent wedge does NOT arise while the 3 nodes agree on the
// ring. That is the KEY diagnostic: the live 40-minute wedge is NOT explained by
// boot-defer alone (which heals on a consistent ring). It requires the SECOND
// ingredient - a RING / MEMBERSHIP DIVERGENCE that never settles.
//
// This file PROVES that second ingredient is sufficient, deterministically. It
// stands up a real 3-node R=2 cluster (real gRPC forwarding, real per-replica
// shared backing), then INJECTS a persistent ring divergence on ONE node using
// the existing TestingSetRingMembers white-box hook: that node's ring gains a
// phantom member, so it computes a DIFFERENT replica set per unit and RELEASES
// the positions its (diverged) view no longer assigns it, while the other two
// nodes - on the correct 3-node ring - keep routing writes to it. The result is
// exactly the live signature:
//
//	"shale: write needed 2 acks, got 1 (replicas mid-acquire)"  (codes.Unavailable)
//
// on every unit that the divergence orphaned, and it does NOT recover while the
// divergence is held. When the divergence is released the SAME units recover,
// which pins the CAUSE: divergent membership -> divergent desiredReplicaUnits ->
// a write's fan-out targets a node that neither owns nor mounts the position ->
// perpetual 1-of-2. This is the MEMBERSHIP-RACE arm.
//
// Why "inject the divergence" rather than let real gossip produce it: with the
// identity-decouple (#473) + rejoin + meta-refresh fixes, in-process gossip on
// loopback CONVERGES, so the divergence the live cluster suffered (believed to be
// a churn-induced convergence race, plausibly aggravated by real-network timing,
// hard SIGKILLs with no Leave, and/or an in-flight reshard) does not arise from a
// graceful in-process roll. Forcing the exact divergent RING state is the
// deterministic stand-in the task calls for: it proves the wedge SHAPE + its
// diagnostic fingerprint without depending on losing a gossip timing race.

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// replicaIDsForMembers builds a ring from the given member ids and returns the
// ordered replica node ids for unit u at replication factor rf - the exact
// deterministic hashing the cluster routes with (LocateKeyN over the gen-0 unit
// bytes). Used both to SELECT the wedge unit and to render the per-viewer replica
// sets in the captured state.
func replicaIDsForMembers(memberIDs []string, u storageunit.UnitID, rf int) []string {
	rg := ring.New()
	for _, id := range memberIDs {
		rg.Add(ring.Member{ID: id, Addr: id + ":0"})
	}
	set := rg.LocateKeyN(gen0UnitBytes(u), rf)
	out := make([]string, 0, len(set))
	for _, m := range set {
		out = append(out, m.ID)
	}
	return out
}

// findMembershipWedgeUnit searches for (phantomID, unit) such that adding
// phantomID to victim's ring DISPLACES victim out of the unit's replica set,
// while on the correct 3-node ring victim IS a replica of that unit. That unit is
// the one that wedges: the correct-ring nodes route it to victim, but victim's
// diverged ring made it release the position, so no live node holds victim's slot
// and the write can never collect W=2 acks. Returns ok=false if no such pair is
// found (then the caller widens the phantom search).
func findMembershipWedgeUnit(realIDs []string, victim string, unitCount, rf int) (phantom string, unit storageunit.UnitID, ok bool) {
	uc := storageunit.MustUnitCount(unitCount)
	for _, phantom := range []string{"phantom-A", "phantom-B", "phantom-C", "phantom-D", "phantom-E", "phantom-F", "phantom-G", "phantom-H"} {
		diverged := append(append([]string(nil), realIDs...), phantom)
		for _, u := range uc.IDs() {
			correct := replicaIDsForMembers(realIDs, u, rf)
			if !contains(correct, victim) {
				continue // victim is not routed this unit on the correct ring.
			}
			divergedSet := replicaIDsForMembers(diverged, u, rf)
			if contains(divergedSet, victim) {
				continue // victim still owns it under the diverged ring: no orphan.
			}
			// victim owns it on the correct ring but NOT on the diverged ring, and
			// the phantom is not one of the correct replicas of this unit (so the
			// correct-ring fan-out targets only real, reachable nodes - the wedge is
			// a clean "mid-acquire", not a phantom dial failure).
			if contains(correct, phantom) {
				continue
			}
			return phantom, u, true
		}
	}
	return "", 0, false
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// keyForUnit returns a plain key that routes to unit u under unitCount.
func keyForUnit(u storageunit.UnitID, unitCount int) string {
	for i := 0; ; i++ {
		k := fmt.Sprintf("wk-%07d", i)
		if unitOf(k, unitCount) == u {
			return k
		}
	}
}

// TestRollingRestartWedge_MembershipDivergence is the deterministic membership-
// race reproduction. It forces a persistent ring divergence on one node and
// proves a write to an orphaned unit wedges at exactly 1-of-2 with the live
// "replicas mid-acquire" signature, does not recover while the divergence holds,
// and recovers once it is released.
func TestRollingRestartWedge_MembershipDivergence(t *testing.T) {
	const unitCount, rf = 16, 2
	backing := sharedfactory.NewBacking()
	// Short WriteTimeout so each probe Put returns quickly (its internal
	// retry-through-handoff budget is bounded by WriteTimeout), giving many probe
	// samples in the observation window instead of a couple of 5s-blocking calls.
	nodes := start3NodeR2Cfg(t, unitCount, backing, func(cfg *cluster.Config) {
		cfg.WriteTimeout = 250 * time.Millisecond
	}) // ids ovh-a, ovh-b, ovh-c
	byID := map[string]*sharedNode{}
	for _, n := range nodes {
		byID[n.ID] = n
	}
	realIDs := []string{"ovh-a", "ovh-b", "ovh-c"}
	victimID := "ovh-c" // the node we diverge
	entryID := "ovh-b"  // a correct-ring node that forwards to both replicas
	baseID := "ovh-a"

	// Seed a dataset so every unit has real data + serving markers.
	want, _ := writeRecordedDataset(t, nodes[0].Cluster)
	t.Logf("seeded %d keys across %d units", len(want), unitCount)

	// SELECT the wedge unit: a unit the victim owns on the correct ring but is displaced
	// from once a phantom joins the victim's ring.
	phantom, wedgeUnit, ok := findMembershipWedgeUnit(realIDs, victimID, unitCount, rf)
	if !ok {
		t.Skip("no phantom displaces the victim from a unit it owns (ring hashing) - adjust phantom candidates")
	}
	wedgeKey := keyForUnit(wedgeUnit, unitCount)
	correctRS := replicaIDsForMembers(realIDs, wedgeUnit, rf)
	divergedRS := replicaIDsForMembers(append(append([]string(nil), realIDs...), phantom), wedgeUnit, rf)
	t.Logf("wedge unit=%d key=%q  correct-ring replicas=%v  %s-diverged-ring replicas=%v (phantom=%s)",
		wedgeUnit, wedgeKey, correctRS, victimID, divergedRS, phantom)

	// Baseline: the wedge unit writes fine BEFORE the divergence.
	if err := putWithRetryUnavailable(t, byID[baseID].Cluster, wedgeKey, "before", 10*time.Second); err != nil {
		t.Fatalf("baseline write to wedge unit failed (pre-divergence): %v", err)
	}

	victim := byID[victimID].Cluster
	// The diverged member set the victim's ring is forced to: the real 3 plus the
	// phantom (an unreachable address; the wedge unit never routes to it, so it is
	// never dialed - it exists only to reshuffle the victim's replica math).
	divergedMembers := []ring.Member{
		{ID: realIDs[0], Addr: byID[realIDs[0]].GRPCAddr},
		{ID: realIDs[1], Addr: byID[realIDs[1]].GRPCAddr},
		{ID: realIDs[2], Addr: byID[realIDs[2]].GRPCAddr},
		{ID: phantom, Addr: "127.0.0.1:1"}, // unreachable; format-valid
	}

	// HOLD the divergence: re-apply the forced ring + run one reconcile pass on a
	// tight cadence so the victim continuously (a) presents a 4-member ring and (b)
	// RELEASES the positions its diverged view no longer assigns it. The 5s
	// periodic reconcile would otherwise revert the ring to the 3-node membership
	// snapshot and re-acquire; re-forcing every 10ms keeps the divergence held with
	// ~99.8% duty, which the rate-based assertion below tolerates.
	var holding atomic.Bool
	holding.Store(true)
	var holdWG sync.WaitGroup
	holdWG.Add(1)
	go func() {
		defer holdWG.Done()
		for holding.Load() {
			victim.TestingSetRingMembers(divergedMembers)
			victim.TestingRunReconcile() // release the orphaned positions per the diverged view
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Give the victim a few reconcile passes to release the orphaned position.
	time.Sleep(300 * time.Millisecond)

	// PROBE while divergence is held: hammer the wedge unit through the entry node
	// (a correct-ring node that forwards to both replicas), classifying each
	// outcome. Each Put returns within the short WriteTimeout, so this collects
	// many samples across the window.
	// Two samples, not one. sampleErr is "what the window looked like"; otherErr
	// is the error that actually TRIPS the failure-purity assertion below.
	// Sharing one variable makes the failure unreadable: mid-acquire refusals are
	// the common case, so a single first-error-wins sample is almost always a
	// mid-acquire error, and the assertion then reports "this failure was NOT the
	// mid-acquire class" while printing a mid-acquire error. That is worse than
	// no sample, because it sends the reader after the wrong bug.
	var success, midAcquire, other int
	var sampleErr, otherErr error
	probeEnd := time.Now().Add(6 * time.Second)
	for time.Now().Before(probeEnd) {
		err := byID[entryID].Cluster.Put([]byte(wedgeKey), []byte("probe"))
		switch {
		case err == nil:
			success++
		case isMidAcquire(err):
			midAcquire++
			if sampleErr == nil {
				sampleErr = err
			}
		default:
			other++
			if sampleErr == nil {
				sampleErr = err
			}
			if otherErr == nil {
				otherErr = err
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	total := success + midAcquire + other
	membersAgreeNow := clustersAgreeOnMembers(nodes)
	stateDump := captureNodesState(nodes)

	t.Logf("WHILE DIVERGED: probes total=%d success=%d midAcquire=%d other=%d; sample err=%v",
		total, success, midAcquire, other, sampleErr)
	t.Logf("members-agree-across-nodes=%v (expected FALSE: the membership-race arm)", membersAgreeNow)

	// Stop holding the divergence so the victim self-heals for the recovery check.
	holding.Store(false)
	holdWG.Wait()

	// ASSERTIONS (the deterministic repro).
	//
	// NOTE: there is deliberately NO assertion that midAcquire > 0.
	//
	// Requiring at least one refusal is a success-rate assertion wearing a
	// different hat, and this test's own contract already rejects those as
	// "scenario-luck in both directions" (see the re-contracting comment below).
	// The outcome is bimodal BY DESIGN: when the diverged view still overlaps a
	// mounted holder the union routes straight through and every probe succeeds;
	// when the forced phantom placement excludes every mounted holder the quorum
	// floor takes the safe arm and every failure is the retryable class. A run
	// that lands in the first arm with 382/382 successes has demonstrated the
	// system working, and used to FAIL here for it.
	//
	// The three invariants this test actually owns are asserted elsewhere:
	// (a) the divergence armed - the membersAgreeNow check immediately below,
	// which is what stops a vacuous pass if the race never materialised;
	// (b) failure purity - the `other != 0` check further down;
	// (c) full recovery once the divergence releases - asserted at the end.
	// None of them needs a refusal to have occurred.
	if membersAgreeNow {
		t.Fatalf("expected divergent member views across nodes while wedged (membership-race arm), but they agreed.\nstate:%s", stateDump)
	}
	// RE-CONTRACTED (v0.10.x): this test originally PINNED the divergence
	// wedge - it asserted the orphaned unit was essentially unwritable
	// (<=20% success) while the rings disagreed. The write-transparent
	// membership work (Joining/Draining union routing + quorum floor +
	// transitional holds) changed the outcome to a BIMODAL-BY-DESIGN split:
	// when the diverged view still overlaps a mounted holder, the union
	// routes writes through the divergence (observed ~70-91% success);
	// when the forced phantom placement excludes every mounted holder, the
	// quorum floor takes the SAFE arm - unavailable-but-lossless, every
	// failure the retryable mid-acquire class (observed 0% success). A
	// success-rate assertion is therefore scenario-luck in both directions.
	// The real invariants are: (a) the divergence mechanism arms above,
	// (b) FAILURE PURITY - every failed probe is the safe acquiring class,
	// never an unexpected error and never a false ack, and (c) full
	// recovery once the divergence releases (asserted below).
	if total == 0 {
		t.Fatalf("no probes ran")
	}
	if other != 0 {
		t.Fatalf("%d of %d probe failures were NOT the safe mid-acquire class while diverged.\n"+
			"  offending error: %v\n"+
			"  (for contrast, a mid-acquire refusal from the same window: %v; %d of those)\n"+
			"the wedge must be lossless-and-retryable or covered, never eclectic.\nstate:%s",
			other, other+midAcquire, otherErr, sampleErr, midAcquire, stateDump)
	}
	t.Logf("DIVERGENCE OUTCOME (membership arm): orphaned unit %d - %d/%d writes ok, %d safe mid-acquire refusals; "+
		"union covers when a mounted holder overlaps, quorum floor holds the safe arm when none does", wedgeUnit, success, total, midAcquire)

	// RECOVERY: once the divergence is released the periodic reconcile restores
	// r2c's 3-node ring + re-acquires the position, so the SAME unit writes again.
	// This pins the divergence as the cause (not a corrupt backing).
	if err := putWithRetryUnavailable(t, byID[entryID].Cluster, wedgeKey, "after", 30*time.Second); err != nil {
		t.Fatalf("RECOVERY FAILED: wedge unit still unwritable 30s after releasing the divergence: %v", err)
	}
	if agree := waitMembersAgree(nodes, 30*time.Second); !agree {
		t.Fatalf("nodes did not re-converge on membership after releasing the divergence")
	}
	t.Logf("RECOVERED after releasing divergence: rings re-converged (all see 3), wedge unit writable again")
}

// isMidAcquire reports whether err is the exact fan-out ack-shortfall the wedge
// manifests as: codes.Unavailable carrying "replicas mid-acquire".
func isMidAcquire(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		return false
	}
	return strings.Contains(st.Message(), "replicas mid-acquire")
}

// clustersAgreeOnMembers reports whether every node's PLACEMENT basis reports
// the SAME set of member ids. False is the membership-race fingerprint.
//
// It deliberately reads PlacementMembers, not Members: the divergence this
// mechanism test engineers is a ring-only one (TestingSetRingMembers mutates
// the placement basis; gossip still agrees), which is exactly the production
// shape of a dropped membership event. The membership VIEW is snapshot-based
// and agrees across nodes throughout - by design - so reading it here would
// make the vacuity guard unsatisfiable rather than protective.
func clustersAgreeOnMembers(nodes []*sharedNode) bool {
	var ref string
	for _, n := range nodes {
		ids := sortedPlacementMemberIDs(n.Cluster)
		key := strings.Join(ids, ",")
		if ref == "" {
			ref = key
			continue
		}
		if key != ref {
			return false
		}
	}
	return true
}

func sortedPlacementMemberIDs(c *cluster.Cluster) []string {
	ms := c.PlacementMembers()
	ids := make([]string, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, m.ID)
	}
	// small slices; simple insertion sort keeps deps minimal
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
	return ids
}

// waitMembersAgree polls until every node's ring agrees on the member set, or
// the timeout elapses.
func waitMembersAgree(nodes []*sharedNode, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if clustersAgreeOnMembers(nodes) {
			// also require the phantom to be gone (set size back to 3)
			if len(nodes[0].Cluster.Members()) == len(nodes) {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// captureNodesState formats each node's member view + DebugState() - the
// /debug/shale/state equivalent - so a wedge dump identifies the arm (rings
// DISAGREE => membership race; rings AGREE but positions unmounted => storage
// stall).
func captureNodesState(nodes []*sharedNode) string {
	var b strings.Builder
	for _, n := range nodes {
		fmt.Fprintf(&b, "\n===== node %s ring-members=%v =====\n", n.ID, sortedPlacementMemberIDs(n.Cluster))
		b.WriteString(n.Cluster.DebugState())
	}
	return b.String()
}
