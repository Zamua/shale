package cluster

// White-box pins for the handoff-cycle LATENCY properties (docs/SPEC.md
// "The pending owners acquire in the background" + "Displacement flush"):
// the per-position cycle must cost ~the open itself, never accumulate
// reconcile ticks. Two tick-quantization traps are pinned here, each in a
// fixture that runs NO reconcile loop - so if the event-driven path were
// missing, the asserted transition could only happen via a (nonexistent)
// tick and the test would time out:
//
//   - a FAILED overlap open re-drives itself on a short backoff in the
//     acquire goroutine (not stranded until the next periodic reconcile);
//   - a Draining position releases on the fast drain poller within ~half a
//     second of the successor's serving marker (not the next tick).
//
// The event-driven QUEUE CHAINING itself (permit frees -> next queued open
// starts immediately) is pinned end-to-end on a real gossip cluster in
// tests/integration/handoff_cycle_latency_test.go.

import (
	"errors"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestOverlapAcquire_FailedOpenRedrives_WithoutReconcileTick pins the failure
// re-drive: an overlap open that fails transiently retries from its own
// goroutine on the acquireRedriveBase backoff. The fixture runs no reconcile
// loop, so ONLY the in-goroutine re-drive can complete the mount; pre-fix the
// position stayed Acquiring forever here (in production: until the next
// reconcileInterval tick, quantizing every transient failure to ~5s).
func TestOverlapAcquire_FailedOpenRedrives_WithoutReconcileTick(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1")
	target := c.desiredReplicaUnits()[0]

	boom := errors.New("injected transient open failure")
	backing.SetOpenReplicaFault(target, boom)

	start := time.Now()
	c.beginAcquire(target)
	c.acquireReplicaUnitOverlap(target)

	// Let the first attempt fail, then clear the fault. The goroutine's own
	// jittered ~250ms backoff must re-drive and mount well inside a single
	// reconcileInterval (5s).
	time.Sleep(100 * time.Millisecond)
	backing.SetOpenReplicaFault(target, nil)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, mounted := c.localBackendForReplicaUnit(target); mounted {
			if e := time.Since(start); e >= reconcileInterval {
				t.Fatalf("re-drive mounted only after %v (>= reconcileInterval %v): tick-quantized, not event-driven", e, reconcileInterval)
			}
			c.loopWG.Wait() // let the acquire goroutine finish cleanly
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("failed open never re-drove without a reconcile tick (position stranded Acquiring; count=%d)",
		len(backing.Handle().OpenTimeline()))
}

// TestOverlap_DrainPoller_ReleasesOnMarker_WithoutReconcileTick pins the fast
// drain poll: after beginDrain arms Draining, the at-most-one background
// poller observes the successor's serving marker and releases the mount
// within ~displacedDrainPollInterval - with NO manual drainCheck call and NO
// reconcile loop in the fixture, so only the poller can do it.
func TestOverlap_DrainPoller_ReleasesOnMarker_WithoutReconcileTick(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")
	target := ru(0, 0, 0)

	// Mount through the real flip (records myOpenEpoch = 1, writes own marker).
	c.acquireReplicaUnitOverlapBlocking(target)
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("precondition: target should be mounted")
	}

	// A successor opens (durable -> 2) and writes its marker at 2, BEFORE the
	// drain is armed - the poller's very first pass can release.
	hSucc := backing.Handle()
	_, succEpoch, err := hSucc.OpenReplicaUnit(target, acquireBaseEpoch)
	if err != nil {
		t.Fatalf("successor open: %v", err)
	}
	if err := hSucc.WriteServingMarker(target, succEpoch); err != nil {
		t.Fatalf("successor marker: %v", err)
	}

	start := time.Now()
	c.beginDrain(target)
	if st := c.handoffPhaseOf(target); st.Phase != storageunit.PhaseDraining {
		t.Fatalf("precondition: beginDrain should set Draining, got %v", st.Phase)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
			elapsed := time.Since(start)
			if elapsed >= reconcileInterval {
				t.Fatalf("drain released only after %v (>= reconcileInterval %v): tick-quantized, not the fast poller", elapsed, reconcileInterval)
			}
			// The poller must also terminate once no Draining position remains.
			pollerDeadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(pollerDeadline) {
				if !c.drainPollerActive.Load() {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatalf("fast drain poller did not terminate after the last drain resolved")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Draining position was never released by the fast poller (marker %d > gate; no reconcile loop in this fixture)", succEpoch)
}
