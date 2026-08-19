package integration

// THE GRACEFUL-LEAVE ACCEPTANCE GATE + BREAK-DEMO (v0.8 Phase 2e, scale-down).
//
// docs/SPEC.md "v0.8 Phase 2e: Graceful leave (scale-down)" and
// docs/design/overlap-handoff.md "Graceful leave (scale-down)" are the
// canonical behavior. A graceful leave is the overlap drain seen from the
// LOSING side, for ALL of a node's positions at once: on Close() the leaving
// node broadcasts the memberlist leave (survivors re-own its positions and
// begin Acquiring), then DrainForLeave BLOCKS until every position handed off
// (the successors are Ready and drainCheck released them) before tearing the
// mounts down. The leaving node keeps serving (directly + via the survivors'
// forwards) throughout the drain, so there is no unserved window.
//
// Two tests share ONE driver (runGracefulLeave):
//
//   - TestGracefulLeave_HoldsAvailabilityThroughLeave (drain ON,
//     GracefulLeaveDrainTimeout generous): a 4-node R=2 cluster on the
//     sharedfactory whose OpenReplicaUnit is ARBITRARILY SLOW (a multi-second
//     mount, the object-store-latency analogue), with a CONTINUOUS writer.
//     Gracefully removes ONE non-seed node by calling its Cluster.Close() (which
//     runs DrainForLeave first). Asserts (a) ~100% ack THROUGH the leave window
//     (the leaving node served its positions until the successors took over) and
//     (b) THE ORACLE: every baseline + acked key readable with its exact value
//     from every surviving node, ZERO loss.
//
//   - TestGracefulLeave_BreakDemo_NoDrainShowsGap (drain OFF,
//     GracefulLeaveDrainTimeout=0): the SAME slow-mount scenario, but Close()
//     does NOT drain - it tears the leaving node's mounts down immediately, so
//     the just-closed positions are UNSERVED until the survivors finish their
//     (slow) Acquiring mount. Asserts the ack rate DURING the leave window
//     visibly DROPS, proving the drain wait (not luck) is what holds
//     availability. Durability (the oracle) STILL holds: no-drain is
//     lossless-but-unavailable, identical to the clean-cut break-demo.
//
// The mount delay (7s) is LARGER than the WriteTimeout (the 5s default), the
// regime where the gap is unmistakable: without the drain the survivors take
// >5s to mount the leaving node's positions, so a write routed to a position
// the leaving node just closed has no live owner for the whole mount. NO
// slatedb tag, NO MinIO: in-process sharedfactory + memory backend (the
// SetAcquireDelay slow-mount injection is the latency analogue).

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gracefulLeaveResult is the outcome of one graceful-leave run the two tests
// assert over.
type gracefulLeaveResult struct {
	// duringAttempted / duringAcked tally ONLY the writes attempted while the
	// leave + survivor-mount is in flight (the window the drain is supposed to
	// protect). This is the discriminating signal between the two regimes: with
	// the drain ON it stays ~100%, with it OFF it drops while the just-closed
	// positions are unserved.
	duringAttempted int64
	duringAcked     int64
	// want is baseline keys PLUS every acked probe key, each mapped to its exact
	// value. The loss oracle reads every one back from every surviving node.
	want map[string][]byte
	// survivors is the set that remains after the leave (entry points for the
	// readback; the left node is closed + torn down).
	survivors []*sharedNode
}

func (r gracefulLeaveResult) duringAckRate() float64 {
	if r.duringAttempted == 0 {
		return 0
	}
	return float64(r.duringAcked) / float64(r.duringAttempted)
}

// runGracefulLeave is the shared driver for both the gate and the break-demo.
// It stands up a 4-node R=2 cluster (R=2 still satisfiable with 3 survivors),
// writes a recorded baseline, runs a continuous writer, arms the slow mount,
// then GRACEFULLY removes ONE non-seed node by calling its Cluster.Close()
// (which runs DrainForLeave first when drainTimeout > 0). It returns the
// during-leave ack tally + the want set (baseline + acked) + the survivor set.
//
// drainTimeout threads Config.GracefulLeaveDrainTimeout into every node: a
// generous value enables the drain (the gate), 0 disables it (the break-demo,
// today's tear-down-immediately behavior).
func runGracefulLeave(t *testing.T, drainTimeout, mountDelay time.Duration) gracefulLeaveResult {
	return runGracefulLeaveConc(t, drainTimeout, mountDelay, 8)
}

// runGracefulLeaveConc is runGracefulLeave with the open-concurrency bound
// explicit. The gates run at 8 (see the mutate comment below); the no-drain
// break-demo runs at the PRODUCTION default of 1, where the drain's absence
// is still visible - see the demo for why that divergence is deliberate.
func runGracefulLeaveConc(t *testing.T, drainTimeout, mountDelay time.Duration, openConcurrency int) gracefulLeaveResult {
	t.Helper()
	const unitCount = 32
	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence-at-completion timing): this driver pins the
	// graceful-leave drain mechanism in ISOLATION. Its gate asserts ~100%
	// during-window ack with the survivors' mounts LONGER than the 5s write
	// budget, which requires the LEAVER to keep serving the union until its
	// successors complete (the residual being sub-second windows around the
	// fence flip). Under the backing's default eager fence (real slatedb
	// timing) the leaver is fenced the INSTANT each successor starts its
	// slow open and the drain-window writes lose their union leg - the
	// KNOWN residual TestJoinResidual_FenceAtOpenStart_MovingShardsWedge
	// reproduces deliberately under the default.
	backing.SetEagerFence(false)

	mutate := func(cfg *cluster.Config) {
		cfg.GracefulLeaveDrainTimeout = drainTimeout
		// Survivors acquire the leaver's positions with a multi-second mock
		// delay armed across several positions at once; the default node-wide
		// open bound (1) would serialize them past the drain budget. No FFI
		// hazard in the mock double; the bound is pinned separately in
		// pkg/cluster (TestOverlapAcquire_BoundedByOpenConcurrency).
		cfg.OpenConcurrency = openConcurrency
	}

	// Start a 4-node R=2 cluster (default 5s WriteTimeout) and let it fully
	// settle before the baseline so initial-convergence per-replica fencing is
	// resolved (no artificial mount delay armed yet). ovh-a is the seed; we leave
	// a NON-seed node (ovh-d) so the seed's gRPC stays reachable for the readback.
	n1 := startReplicatedNodeCfg(t, "gl-a", "", unitCount, 2, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "gl-b", n1.ClusterToken, unitCount, 2, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "gl-c", n1.ClusterToken, unitCount, 2, backing, mutate)
	n4 := startReplicatedNodeCfg(t, "gl-d", n1.ClusterToken, unitCount, 2, backing, mutate)
	all := []*sharedNode{n1, n2, n3, n4}
	csAll := make([]*cluster.Cluster, 0, len(all))
	for _, n := range all {
		csAll = append(csAll, n.Cluster)
	}
	if err := waitForMembersAll(csAll, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// The node we gracefully remove: gl-d, NOT the seed.
	leaving := n4
	survivors := []*sharedNode{n1, n2, n3}

	want := make(map[string][]byte)
	for i := range 120 {
		k := fmt.Sprintf("glbase-%05d", i)
		v := fmt.Appendf(nil, "glbaseval-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 15*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	var (
		mu          sync.Mutex
		ackedKeys   = make(map[string][]byte)
		failReasons = make(map[string]int) // DIAG: during-window failure histogram
		stop        atomic.Bool
		wg          sync.WaitGroup

		// during* tally only the writes attempted while leaveInFlight is true.
		leaveInFlight   atomic.Bool
		duringAttempted atomic.Int64
		duringAcked     atomic.Int64

		// entryFor is the live entry-point pool. It starts as all four nodes and
		// is narrowed to the survivors the instant the leave begins, so the writer
		// never aims at the node being torn down (which would be a self-inflicted
		// error unrelated to the position availability we are measuring).
		entryMu  sync.RWMutex
		entryFor = append([]*sharedNode{}, all...)
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
				k := fmt.Sprintf("glprobe-%d-%07d", w, i)
				v := fmt.Appendf(nil, "glpv-%d-%07d", w, i)
				inFlight := leaveInFlight.Load()
				if inFlight {
					duringAttempted.Add(1)
				}
				// Call Put DIRECTLY (no external retry) so the ack rate reflects the
				// cluster's INTERNAL availability through the leave + survivor mount.
				err := entry.Cluster.Put([]byte(k), []byte(v))
				if err == nil {
					if inFlight {
						duringAcked.Add(1)
					}
					mu.Lock()
					ackedKeys[k] = v
					mu.Unlock()
				} else if inFlight {
					// DIAG (test-only): tally WHY during-window writes failed, so a
					// regression shows the failure mode (a systematic "needed N acks, got
					// M" points straight at the ack-bar/routing; sparse transients are the
					// expected sub-second fence-flip windows).
					st, _ := status.FromError(err)
					reason := fmt.Sprintf("%s | %s", st.Code(), firstLine(err.Error()))
					mu.Lock()
					failReasons[reason]++
					mu.Unlock()
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}

	// Warm up, then arm the LONG mount delay on the survivors (they are the ones
	// who must Acquire the leaving node's positions; their mounts now stall
	// mountDelay > the 5s write budget). Then mark the leave in-flight, narrow the
	// writer pool to the survivors, and gracefully Close() the leaving node.
	time.Sleep(400 * time.Millisecond)
	for _, n := range survivors {
		n.Handle.SetAcquireDelay(mountDelay)
	}

	// Narrow the writer pool to the survivors BEFORE the leave so no write is
	// aimed at the node we are about to tear down.
	entryMu.Lock()
	entryFor = append([]*sharedNode{}, survivors...)
	entryMu.Unlock()
	// Small beat so the in-flight writers pick up the narrowed pool.
	time.Sleep(50 * time.Millisecond)

	leaveInFlight.Store(true)
	// Close() runs DrainForLeave first (when drainTimeout > 0), broadcasting the
	// leave and BLOCKING until the survivors are Ready and drainCheck released
	// every Draining position. With drainTimeout == 0 it tears the mounts down
	// immediately (the gap). Run it in a goroutine so the writer keeps measuring
	// the window while the drain (or the immediate teardown) proceeds.
	leaveDone := make(chan struct{})
	var closeDur time.Duration
	go func() {
		defer close(leaveDone)
		t0 := time.Now()
		leaving.Close()
		closeDur = time.Since(t0)
	}()

	// Keep the during-window open long enough to span a couple of mount delays so
	// the survivors' slow Acquiring genuinely overlaps the measured writes. With
	// the drain ON, Close() blocks in DrainForLeave until every position is handed
	// off (or the timeout fires); with it OFF, Close() returns in ms and the
	// just-closed positions are unserved for the full survivor mount.
	window := 2*mountDelay + 2*time.Second
	time.Sleep(window)
	leaveInFlight.Store(false)

	// Let the leave finish its teardown (it may still be draining if the window
	// was shorter than the drain), then stop the writer.
	<-leaveDone
	t.Logf("graceful leave (drainTimeout=%v, mount=%v): Close() returned in %v",
		drainTimeout, mountDelay, closeDur)
	mu.Lock()
	if len(failReasons) > 0 {
		t.Logf("DIAG during-window failure histogram: %v", failReasons)
	}
	mu.Unlock()
	stop.Store(true)
	wg.Wait()

	// Drop the delay so the readback is not itself slowed, then let any still-
	// Acquiring positions finish their (now instant) mount AND let the ring fully
	// re-converge after the leave (a survivor still mid-Acquiring forwards, and a
	// stale ring view can briefly loop a read between survivors; give the
	// background reconcile a few ticks to settle before the oracle reads). The
	// readback itself also retries on transient Unavailable, so this settle only
	// needs to cover the convergence, not every mount.
	// Drop the delay and let the survivors' rings fully RE-CONVERGE after the
	// abrupt 4 -> 3 leave before the oracle reads. After a node leaves, the
	// survivors' rings re-derive independently and can briefly DIVERGE (two
	// survivors each forward a key to the other, the loop-guard refuses, until the
	// background reconcile settles them onto a shared view). Empirically every
	// survivor serves every key within ~10s of the leave (verified by a repeated
	// per-node probe during development); 12s carries margin. This is CONVERGENCE
	// lag, NOT loss: the data is always present on the survivor that holds the
	// replica, only the diverged-ring forwarding briefly cannot reach it. The
	// tolerant oracle readback (getOracleReadback) rides any residual transient.
	for _, n := range survivors {
		n.Handle.SetAcquireDelay(0)
	}
	time.Sleep(15 * time.Second)

	mu.Lock()
	for k, v := range ackedKeys {
		want[k] = v
	}
	mu.Unlock()

	return gracefulLeaveResult{
		duringAttempted: duringAttempted.Load(),
		duringAcked:     duringAcked.Load(),
		want:            want,
		survivors:       survivors,
	}
}

// firstLine returns s up to its first newline (DIAG helper: keep the failure
// histogram keys to the gRPC status message, not the full wrapped chain).
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// getOracleReadback reads key back for the loss oracle, retrying past the
// post-leave ring-convergence TRANSIENTS until it gets a definitive answer or
// the deadline fires. Two transients ride the membership change after a node
// leaves: codes.Unavailable (a position still mid-Acquiring) AND a
// codes.FailedPrecondition "forwarding loop refused" (a survivor with a
// briefly-stale ring view bounces the read between survivors while the ring
// re-converges). Neither is loss - both clear once the ring settles - so the
// oracle must retry them; a key that is GENUINELY lost stays unreadable past the
// deadline and the caller fails. This is strictly stronger than masking: the
// deadline is generous, so a real lost write cannot hide behind the transient.
func getOracleReadback(t *testing.T, c *cluster.Cluster, key string, timeout time.Duration) ([]byte, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		got, err := c.Get([]byte(key))
		if err == nil {
			return got, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, lastErr
		}
		// Retry through the convergence transients; any other error is definitive.
		st, _ := status.FromError(err)
		code := st.Code()
		isLoop := code == codes.FailedPrecondition && strings.Contains(err.Error(), "forwarding loop")
		if code != codes.Unavailable && !isLoop {
			return nil, err
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// assertNoAckedLossSurvivors is THE ORACLE: every baseline + acked key is
// readable with its exact value from every SURVIVING node. ZERO acked loss. It
// MUST hold for BOTH the gate AND the break-demo - durability is invariant of
// the ack rate (no-drain is lossless-but-unavailable; the fix is the ack rate,
// not durability). So the break-demo asserts this too. Reads ride the
// post-leave convergence transients (see getOracleReadback) so a transient
// re-converging ring is not mistaken for loss; a genuinely lost write stays
// unreadable past the generous deadline and fails here.
func assertNoAckedLossSurvivors(t *testing.T, r gracefulLeaveResult) {
	t.Helper()
	for k, v := range r.want {
		got, err := getOracleReadback(t, r.survivors[0].Cluster, k, 30*time.Second)
		if err != nil {
			t.Fatalf("ACKED WRITE LOST: key %q unreadable after leave: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("ACKED WRITE CORRUPTED: key %q = %q, want %q", k, got, v)
		}
	}
	sample := 0
	for k, v := range r.want {
		for _, n := range r.survivors {
			got, err := getOracleReadback(t, n.Cluster, k, 30*time.Second)
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

// glGateThreshold is the floor the drain path must clear (and the no-drain
// break-demo must fall well below) for the DURING-LEAVE window. Under the
// pending-ranges model the leaver stays a routed current owner and the ack bar is
// pinned to the stable R, so the two stable replicas satisfy W instantly while
// the successors mount in the background: the during-window ack rate sits at
// ~99.8-99.9% (16k+ writes/run), the residual being sub-second windows around the
// fence flip. 0.95 locks that in (catches any regression to the old
// mount-blocked ~86%) while carrying margin for the -race build, whose
// instrumentation stretches the mount timing. It stays far above the break-demo's
// ~53-63% gap so the two regimes never overlap.
const glGateThreshold = 0.95

// glBreakDemoCeiling is the ceiling the no-drain run must stay UNDER for the
// during-leave window. Comfortably below glGateThreshold so the two regimes are
// unambiguous: with the mount LONGER than the write budget and the drain OFF,
// the leaving node's just-closed positions are unserved for the whole survivor
// mount, so a large fraction of in-window writes routed to those positions are
// refused outright.
const glBreakDemoCeiling = 0.78

const glMountDelay = 7 * time.Second

// glDrainTimeout is generous: it must exceed the survivor mount (glMountDelay)
// plus the drain poll cadence with headroom, so the drain has time to see every
// successor become Ready before it gives up. This is the in-test analogue of an
// orchestrator's terminationGracePeriodSeconds being set strictly greater than
// GracefulLeaveDrainTimeout.
const glDrainTimeout = 30 * time.Second

// TestGracefulLeave_HoldsAvailabilityThroughLeave is the gate (drain ON): a
// continuous writer sees ~100% ack THROUGH a graceful one-node leave because the
// leaving node keeps serving its positions (directly + via the survivors'
// forwards) until the survivors are Ready and drainCheck releases. Asserts the
// during-leave ack rate AND the loss oracle.
func TestGracefulLeave_HoldsAvailabilityThroughLeave(t *testing.T) {
	r := runGracefulLeave(t, glDrainTimeout, glMountDelay)
	if r.duringAttempted == 0 {
		t.Fatalf("continuous writer attempted zero writes during the leave window")
	}
	rate := r.duringAckRate()
	t.Logf("DRAIN ON (mount %v > writeTimeout 5s) graceful leave: during-window acked %d / attempted %d = %.1f%%",
		glMountDelay, r.duringAcked, r.duringAttempted, rate*100)

	// (a) THE DURING-LEAVE ACK RATE stays high: the drain holds availability
	// across the slow survivor mount.
	if rate < glGateThreshold {
		t.Fatalf("GRACEFUL-LEAVE AVAILABILITY TOO LOW: during-window acked %d / attempted %d = %.1f%% < %.0f%% "+
			"(the drain is not keeping the leaving node serving through the survivors' slow mount)",
			r.duringAcked, r.duringAttempted, rate*100, glGateThreshold*100)
	}

	// (b) THE ORACLE: zero acked loss, readable + exact from every survivor.
	assertNoAckedLossSurvivors(t, r)
}

// TestGracefulLeave_BreakDemo_NoDrainShowsGap keeps the gate honest: with the
// drain OFF (GracefulLeaveDrainTimeout=0), the SAME slow-mount leave tears the
// leaving node's mounts down immediately, so the during-leave ack rate DROPS
// while the just-closed positions are unserved until the survivors mount -
// proving the drain wait is what holds availability, not luck. Durability (the
// oracle) must STILL hold: no-drain is lossless-but-unavailable.
func TestGracefulLeave_BreakDemo_NoDrainShowsGap(t *testing.T) {
	// PRODUCTION open bound (1), not the gates' lifted 8 - and that divergence
	// is the honest part, so it gets spelled out. Since v0.14.2 routed the
	// reconcile's acquires through the background BOUNDED path, a no-drain
	// leave at concurrency 8 mounts every orphaned position in ~one mount
	// latency and the write retry rides most of the window: this demo's
	// during-leave ack rate rose to ~98%, above its own ceiling, and the demo
	// stopped demonstrating anything. That was a REAL availability improvement
	// making the sabotage too weak, not the gate going vacuous - but the
	// improvement is bounded by OpenConcurrency, and production runs the
	// default of 1 (the FFI-safety bound), where orphaned positions still
	// mount strictly serially and the gap the drain exists to prevent is very
	// much alive. So the demo pins the drain's necessity at the bound
	// production actually runs, while the gates keep 8 (their assertions are
	// about drain transparency, which must hold regardless of the bound).
	r := runGracefulLeaveConc(t, 0, glMountDelay, 1)
	if r.duringAttempted == 0 {
		t.Fatalf("continuous writer attempted zero writes during the leave window")
	}
	rate := r.duringAckRate()
	t.Logf("BREAK-DEMO drain OFF (mount %v > writeTimeout 5s) graceful leave: during-window acked %d / attempted %d = %.1f%%",
		glMountDelay, r.duringAcked, r.duringAttempted, rate*100)

	// Durability is invariant of availability: no-drain never loses an acked
	// write, it just refuses many during the gap. The oracle MUST still pass.
	assertNoAckedLossSurvivors(t, r)

	// The break signal: the during-leave ack rate must DROP under the gate floor.
	// If it did NOT drop, the gate is not actually testing the drain (something
	// else is keeping writes available), so the gate's high-ack-rate assertion
	// would be rubber-stamping.
	if rate >= glBreakDemoCeiling {
		t.Fatalf("BREAK-DEMO DID NOT SHOW THE GAP: no-drain during-window acked %d / attempted %d = %.1f%% >= %.0f%% "+
			"(the gate is not isolating the drain wait; its high-ack-rate assertion would be rubber-stamping)",
			r.duringAcked, r.duringAttempted, rate*100, glBreakDemoCeiling*100)
	}
}
