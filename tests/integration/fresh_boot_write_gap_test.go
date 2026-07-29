package integration

// FRESH-CLUSTER BOOT WRITE GAP (the v0.14.0 regression, reproduced in-process).
//
// The downstream report (a 2-node R=2 real-MinIO bootstrap test): node A boots
// first, mounts its positions as a solo ring and publishes serving markers.
// Node B boots one second later into the converged ring, finds A's markers on
// the positions the NEW ring assigns to B, boot-defers them (correct: A is
// genuinely serving; fencing it would be the v0.13.0 silent-fence bug), mounts
// only the positions nobody holds, and declares "degraded boot complete". B's
// deferred positions then arrive via the background overlap acquire SECONDS
// later (the reconcile settle cadence). In that window the cluster cannot
// assemble W=2 for the affected units, and a write burns its whole retry
// budget and fails.
//
// The defer is right. The marker is right. The GAP between "Open returned" and
// "this node actually holds what it owns" is the bug, and before the marker fix
// it was invisible: a booting node would open positions a live peer was serving,
// FENCING the peer, and the suite's writes still passed because the fencing node
// served them itself. Wrong-but-functionally-equivalent, so nothing measured it.
//
// THIS TEST asserts the two properties that distinguish correct from
// wrong-but-working, on the exact staggered-boot shape:
//
//  1. NO CROSS-FENCE AT BOOT: after both boots settle, node A still holds every
//     position it is supposed to hold under the final ring. If B's boot had
//     fenced A, A's handles would be dead and A's mounted set would not match
//     the ring's assignment. (The v0.13.0 failure mode.)
//  2. NO UNBOUNDED WRITE GAP: a write issued the moment B's Open returns
//     succeeds within the Transact retry budget for EVERY unit. (The v0.14.0
//     failure mode: deferred positions took the multi-second settle cadence to
//     arrive, exhausting the budget.)
//
// It reuses the shared-backing harness: sharedfactory.Backing is the durable
// object-store analogue, so A's serving markers are visible to B's boot exactly
// as MinIO's are in the downstream test.

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

func TestFreshBoot_StaggeredJoin_NoWriteGap(t *testing.T) {
	const (
		unitCount = 4
		rf        = 2
	)
	backing := sharedfactory.NewBacking()

	// Node A boots alone: solo ring, so it desires (and mounts) position 0 of
	// every unit, and publishes a serving marker for each.
	a := startReplicatedNode(t, "boot-a", "", unitCount, rf, backing)

	// Wait for A to be a fully-formed solo cluster: every unit mounted. This is
	// the state the downstream test's node A was in when B arrived - markers
	// durably present for every position A holds.
	deadlineA := time.Now().Add(10 * time.Second)
	for !a.Cluster.MountReadiness().Ready(1.0) {
		if time.Now().After(deadlineA) {
			t.Fatalf("node A never reached fully-mounted solo state: %+v", a.Cluster.MountReadiness())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Node B boots into A with the PRODUCTION settle delay. The harness default
	// (300ms) hides the regression: the deferred positions arrive fast enough
	// for the write retry to ride the gap. Production runs the 5s debounce, and
	// the downstream failure was precisely that the gap outlived the write
	// budget. Reproducing THAT requires the real cadence.
	b := startReplicatedNodeSettle(t, "boot-b", a.BindAddr, unitCount, rf, backing, 0)

	// THE MOMENT OF TRUTH: B's Open has returned. Write to every unit NOW,
	// through BOTH entry nodes, with no settling sleep. Every write must
	// succeed within the cluster's own retry budget - that budget is the
	// contract (Transact rides transient unavailability), and the regression
	// was precisely that the boot gap outlived it.
	for i := 0; i < unitCount*4; i++ {
		key := []byte(fmt.Sprintf("gap-%03d", i))
		entry := a.Cluster
		if i%2 == 1 {
			entry = b.Cluster
		}
		if err := putRideAcquiring(entry, key, []byte("v"), 3*time.Second); err != nil {
			t.Fatalf("write %q via %s failed during the post-boot window: %v\n"+
				"nodeA state:\n%s\nnodeB state:\n%s",
				key, map[bool]string{true: "boot-a", false: "boot-b"}[i%2 == 0], err,
				a.Cluster.DebugState(), b.Cluster.DebugState())
		}
	}

	// PROPERTY 1: no cross-fence. Wait for the topology to fully settle (B's
	// warming completes, A's displaced positions drain), then check BOTH nodes
	// hold exactly what the final ring assigns them, with LIVE handles: a
	// fenced-but-unevicted mount would fail this because the drain/eviction
	// machinery only converges when the epochs are coherent.
	deadlineSettle := time.Now().Add(30 * time.Second)
	for {
		ra, rb := a.Cluster.MountReadiness(), b.Cluster.MountReadiness()
		if ra.PendingUnits == 0 && rb.PendingUnits == 0 && ra.FailedOpenUnits == 0 && rb.FailedOpenUnits == 0 {
			break
		}
		if time.Now().After(deadlineSettle) {
			t.Fatalf("topology never settled after the staggered join:\nA: %+v\nB: %+v\nnodeA:\n%s\nnodeB:\n%s",
				ra, rb, a.Cluster.DebugState(), b.Cluster.DebugState())
		}
		time.Sleep(50 * time.Millisecond)
	}

	// PROPERTY 2: every written key reads back from BOTH nodes (read-your-write
	// through either entry point proves the write landed on real, un-fenced
	// replicas rather than on a node serving positions it had stolen).
	for i := 0; i < unitCount*4; i++ {
		key := []byte(fmt.Sprintf("gap-%03d", i))
		for _, n := range []*sharedNode{a, b} {
			got, err := n.Cluster.Get(key)
			if err != nil {
				t.Fatalf("read-back %q via %s: %v", key, n.ID, err)
			}
			if string(got) != "v" {
				t.Fatalf("read-back %q via %s: got %q", key, n.ID, got)
			}
		}
	}

	// WEDGE DETECTOR, behavior-level: cross-shard scans through each node must
	// CONVERGE to clean within a bound. A transiently fenced mount during the
	// overlap handoff is by design (the acquiring refusal even evicts it, so
	// the next attempt heals); the WEDGE is when the refusal never stops. This
	// is exactly how the production incident presented, so the assertion is
	// "eventually clean", with the bound well above every cadence involved.
	scanDeadline := time.Now().Add(20 * time.Second)
	for _, n := range []*sharedNode{a, b} {
		for {
			nKeys, serr := countScan(n, []byte("gap-"))
			if serr == nil && nKeys == unitCount*4 {
				break
			}
			if time.Now().After(scanDeadline) {
				t.Fatalf("cross-shard scan via %s never converged (last err=%v, keys=%d, want %d) - "+
					"a fenced mount is stuck in the mount map (the wedge failure mode)\n%s",
					n.ID, serr, nKeys, unitCount*4, n.Cluster.DebugState())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// startReplicatedNodeSettle is startReplicatedNode with an explicit settle
// delay; 0 keeps the PRODUCTION default (the 5s debounce) instead of the
// harness's 300ms. The distinction is load-bearing here: the write-gap
// regression only manifests when the deferred positions take the production
// cadence to arrive.
func startReplicatedNodeSettle(t *testing.T, id, seedAddr string, unitCount, rf int, backing *sharedfactory.Backing, settle time.Duration) *sharedNode {
	t.Helper()
	// Zero acquire delay and the ample write timeout the other helpers use;
	// the mutate hook overrides ONLY the settle cadence, which for settle==0
	// falls through to the production default debounce.
	return startReplicatedNodeSlowAcquireCfg(t, id, seedAddr, unitCount, rf, backing, 0, 8*time.Second,
		func(cfg *cluster.Config) {
			cfg.RebalanceSettleDelay = settle
		})
}

// countScan runs one cross-shard scan through n, returning the key count or
// the refusal.
func countScan(n *sharedNode, prefix []byte) (int, error) {
	it, err := n.Cluster.LocalScanPrefix(prefix)
	if err != nil {
		return 0, err
	}
	defer func() { _ = it.Close() }()
	nKeys := 0
	for {
		k, _, ierr := it.Next()
		if ierr != nil {
			return nKeys, ierr
		}
		if k == nil {
			return nKeys, nil
		}
		nKeys++
	}
}

// TestFreshBoot_HomogeneousStagger_NoWriteGap is the SAME property on the
// HOMOGENEOUS deployment shape, and the difference is the whole point.
//
// In the plain-stagger test above, node A boots with NO seeds, so it never
// advertises the Joining bit. In a homogeneous deployment EVERY node carries
// the same seed list and a ConditionalStore (solo-start), so node A boots
// seeded - which raises Joining at membership-open - and the bit clears only
// once A's boot completes cleanly. The first version of the prompt warm-up
// validated only the seedless shape: A never said Joining, the established-
// peer gate passed, the test went green - while the downstream homogeneous
// test kept failing, because THEIR node A was still advertising Joining when
// node B booted one second later. The repro reproduced the SYMPTOM but not
// the SETUP. Failure parity is not setup parity; this test is the setup.
func TestFreshBoot_HomogeneousStagger_NoWriteGap(t *testing.T) {
	const (
		// 16 units, not 4: with 2 nodes the primary split is decided by ID
		// hashing, and at 4 units an unlucky pair of IDs can leave node B
		// primary for NOTHING - then B defers nothing, the gap never exists,
		// and the test passes without testing anything. (The first version of
		// this test did exactly that.) At 16 units both nodes get primaries
		// with near-certainty, and the vacuity guard below makes the residual
		// case loud instead of silently green.
		unitCount = 16
		rf        = 2
	)
	backing := sharedfactory.NewBacking()
	cond := storageunit.NewMemConditionalStore()

	// Node A boots HOMOGENEOUS: seeded (with a peer that is not up yet, the
	// headless-service shape) + a ConditionalStore, so solo-start is allowed
	// and - critically - startJoining raises the Joining bit at membership
	// open. A mounts everything (no markers yet), and must UN-advertise
	// Joining at boot completion; if that clear waits for a reconcile tick,
	// node B below reads A as not-established and the warm-up degrades to the
	// settle debounce - the downstream failure.
	a := startReplicatedNodeHomog(t, "hg-a", "127.0.0.1:1", unitCount, rf, backing, cond)

	deadlineA := time.Now().Add(10 * time.Second)
	for !a.Cluster.MountReadiness().Ready(1.0) {
		if time.Now().After(deadlineA) {
			t.Fatalf("node A never reached fully-mounted solo state: %+v", a.Cluster.MountReadiness())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Node B boots one second later, seeded at A - the staggered pod of the
	// same homogeneous deployment.
	time.Sleep(1 * time.Second)
	b := startReplicatedNodeHomog(t, "hg-b", a.BindAddr, unitCount, rf, backing, cond)

	// VACUITY GUARD: the whole scenario is "B boot-deferred positions a live
	// peer holds". If B deferred nothing (ID hashing gave it no primaries, or
	// a future change stops the defer firing), this test would pass while
	// testing nothing - the exact vacuous-green shape that hid this bug class
	// for months. DebugState names boot-deferred positions explicitly.
	if !strings.Contains(b.Cluster.DebugState(), "boot-deferred") {
		t.Fatalf("node B deferred nothing at boot - the scenario did not arm, "+
			"so a pass would be vacuous. B state:\n%s", b.Cluster.DebugState())
	}

	// Writes the moment B's Open returns, through both entry points.
	for i := 0; i < unitCount*4; i++ {
		key := []byte(fmt.Sprintf("hg-%03d", i))
		entry := a.Cluster
		if i%2 == 1 {
			entry = b.Cluster
		}
		if err := putRideAcquiring(entry, key, []byte("v"), 3*time.Second); err != nil {
			t.Fatalf("write %q failed during the post-boot window: %v\nnodeA:\n%s\nnodeB:\n%s",
				key, err, a.Cluster.DebugState(), b.Cluster.DebugState())
		}
	}

	// Settle, read back through both, scans converge - same bar as the
	// plain-stagger test.
	deadlineSettle := time.Now().Add(30 * time.Second)
	for {
		ra, rb := a.Cluster.MountReadiness(), b.Cluster.MountReadiness()
		if ra.PendingUnits == 0 && rb.PendingUnits == 0 && ra.FailedOpenUnits == 0 && rb.FailedOpenUnits == 0 {
			break
		}
		if time.Now().After(deadlineSettle) {
			t.Fatalf("topology never settled:\nA: %+v\nB: %+v", ra, rb)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for i := 0; i < unitCount*4; i++ {
		key := []byte(fmt.Sprintf("hg-%03d", i))
		for _, n := range []*sharedNode{a, b} {
			got, err := n.Cluster.Get(key)
			if err != nil || string(got) != "v" {
				t.Fatalf("read-back %q via %s: got %q err %v", key, n.ID, got, err)
			}
		}
	}
	scanDeadline := time.Now().Add(20 * time.Second)
	for _, n := range []*sharedNode{a, b} {
		for {
			nKeys, serr := countScan(n, []byte("hg-"))
			if serr == nil && nKeys == unitCount*4 {
				break
			}
			if time.Now().After(scanDeadline) {
				t.Fatalf("scan via %s never converged (err=%v keys=%d)\n%s", n.ID, serr, nKeys, n.Cluster.DebugState())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// putRideAcquiring writes key through entry, retrying ONLY the documented
// transient class (errors.Is(err, cluster.ErrAcquiring)) with backoff inside
// budget. This is the CONSUMER CONTRACT, verbatim from the taxonomy docs
// ("bounded retry with backoff; the handoff will finish") and from how the
// downstream consumer actually wires it. A bare Put would fail on the first
// W-shortfall of a warming window and assert nothing about the gap's LENGTH -
// the property under test is that the gap closes within a consumer-sized
// budget, not that it never exists for a microsecond.
func putRideAcquiring(entry *cluster.Cluster, key, val []byte, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	wait := 25 * time.Millisecond
	for {
		err := entry.Put(key, val)
		if err == nil {
			return nil
		}
		if !errors.Is(err, cluster.ErrAcquiring) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("acquiring window outlived the %s consumer budget: %w", budget, err)
		}
		time.Sleep(wait)
		if wait < 400*time.Millisecond {
			wait *= 2
		}
	}
}

// startReplicatedNodeHomog boots a node the way a homogeneous deployment does:
// seeded (solo-start tolerated via the ConditionalStore) with the production
// settle cadence, so the Joining-bit lifecycle matches a real pod's.
func startReplicatedNodeHomog(t *testing.T, id, seedAddr string, unitCount, rf int, backing *sharedfactory.Backing, cond storageunit.ConditionalStore) *sharedNode {
	t.Helper()
	// REAL OPEN LATENCY (500ms per open) is load-bearing: it is what makes the
	// warm-up's CONCURRENCY observable. Node B boot-defers ~12 of its 16
	// positions; a warm-up that opens them serially takes ~12 x 500ms = 6s and
	// blows the 3s budget below, while the bounded-concurrency background
	// acquire (OpenConcurrency 8) rides it in ~1s. With a 0 delay both shapes
	// finish in microseconds and the serial regression is invisible (the
	// boot-gap residual shipped exactly that way).
	//
	// A 3-second write budget: comfortably rides a bounded-concurrency warm-up
	// (~2 open waves) but NOT a serialized one, and NOT the production settle
	// debounce (5s). That asymmetry is the test's teeth - the contract is that
	// a write rides out a mount window, not a mount times the position count,
	// nor a mount plus an idle debounce parked in front of it.
	return startReplicatedNodeSlowAcquireCfg(t, id, seedAddr, unitCount, rf, backing, 500*time.Millisecond, 3*time.Second,
		func(cfg *cluster.Config) {
			cfg.RebalanceSettleDelay = 0 // 0 = the production default debounce
			cfg.ConditionalStore = cond
		})
}
