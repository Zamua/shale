package integration

// READ transparency through membership transitions (the read-side mirror of
// TestJoinIsWriteTransparent / TestLeaveIsWriteTransparent).
//
// The pending-ranges model (docs/SPEC.md "v0.8 Phase 2e") promises that BOTH
// reads and writes stay available through a graceful membership transition:
// "reads fan out across the union and any mounted member serves". The write
// side is gated by the join/leave write-transparency tests; THESE tests gate
// the READ side, for BOTH point reads (Get) and single-shard scans
// (ScanPrefix), issued through EVERY live node as the entry point - the shape
// a load-balanced client (a k8s Service) actually produces.
//
// Three scenarios, mirroring a rolling surge deployment (join a new node,
// later gracefully remove an old one, then join the next):
//
//   - JOIN alone: a 4th node boot-defers (Joining bit) and slow-mounts its
//     positions while probes read every unit through every node.
//   - SURGE composition: join, then a leave of a different node overlapping
//     the settling, then the next join 2s later - the k8s rollout timeline.
//   - ENTRY DURING DRAIN: a gracefully-leaving node keeps answering as the
//     entry point for the whole drain.
//
// WHAT THE WIRE PATH IS FOR, AND WHAT IT IS NOT. These gates exist to catch
// what only appears once real nodes, real mounts and real gossip are in play.
// They are a poor instrument for the ROUTING DECISION itself - which replicas
// an op touches during a transition - because reaching a given topology through
// them means holding a mount window open against a drain that ends on its own,
// so the topologies actually observed are whatever the runner allowed. That
// decision is a pure function of three placements and R, and it is enumerated
// exhaustively as values in internal/decide (Route). Leave availability end to
// end stays gated by TestGracefulLeave_HoldsAvailabilityThroughLeave, which
// runs a continuous writer against an ack-rate gate and a no-acked-loss oracle
// rather than sampling a window.
//
// Every probe is a SINGLE attempt (no retry): the gate is transparency, not
// eventual success. A probe failure is a client-visible read error.
//
// THE OBSERVATION WINDOW IS PART OF THE ASSERTION. A gate here is only
// meaningful while its transition is actually in flight, and no such window
// stays open on its own: a modeled mount latency elapses, a drain finishes. A
// window left to whatever ran before the probes - a gossip convergence wait,
// most of all - fails in one of two equally useless directions. It fails LOUDLY
// but vacuously, reporting a fixture-window miss as a transparency verdict (the
// JOIN gate did exactly that in CI, on a merge that changed no executable
// code); or it fails SILENTLY, asserting nothing while reporting a pass. So a
// gate OPENS its window explicitly, PROVES it is open before issuing a read,
// and CLOSES it itself. A window that a slow runner can close is not a window;
// it is a race whose loss is reported as a result.
//
// Failure messages here report the MEASURED breakdown (refusals vs wrong or
// absent values, and within refusals the transient acquiring-window class vs
// anything else) and deliberately assert no cause. A gate that names a mechanism
// it did not measure sends the next reader after the wrong thing.

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

// readFailureKind separates the two fundamentally different things a probe can
// observe. The distinction is load-bearing for any caller that classifies
// failures: a REFUSAL is a liveness event (the read declined to answer, loudly),
// whereas a VALUE fault is a correctness event (the read answered, wrongly or
// not at all, for a key that was acked). No availability argument can ever
// excuse a value fault, so the two must not be flattened into one string.
type readFailureKind int

const (
	// failErr: the read returned an error. Err is non-nil.
	failErr readFailureKind = iota
	// failWrongValue: Get returned bytes that are not the seeded value.
	failWrongValue
	// failEmptyScan: ScanPrefix completed cleanly but yielded nothing for a
	// key that was seeded and acked.
	failEmptyScan
)

// readFailure is one client-visible probe failure, retaining the structure a
// caller needs to classify it rather than only the rendered string.
type readFailure struct {
	Op   string // "get", "scan", "scan next"
	Key  string
	Unit storageunit.UnitID
	Node string
	Kind readFailureKind
	Err  error  // non-nil iff Kind == failErr
	Got  []byte // the returned bytes, for Kind == failWrongValue
}

func (f readFailure) String() string {
	switch f.Kind {
	case failWrongValue:
		return fmt.Sprintf("get %s (unit %d) via %s: wrong value %q", f.Key, f.Unit, f.Node, f.Got)
	case failEmptyScan:
		return fmt.Sprintf("scan %s (unit %d) via %s: empty result for a seeded key", f.Key, f.Unit, f.Node)
	default:
		return fmt.Sprintf("%s %s (unit %d) via %s: %v", f.Op, f.Key, f.Unit, f.Node, f.Err)
	}
}

// probeReadsOnce issues one Get and one full ScanPrefix for every seeded unit
// key through every entry node, single-attempt, and returns the collected
// failures rendered as strings (empty = fully transparent this round).
func probeReadsOnce(entries []*sharedNode, uk map[storageunit.UnitID]string, want []byte) []string {
	details := probeReadsOnceDetailed(entries, uk, want)
	out := make([]string, 0, len(details))
	for _, f := range details {
		out = append(out, f.String())
	}
	return out
}

// probeReadsOnceDetailed is probeReadsOnce's structured form: same probes, same
// failure conditions, but the failures keep their unit / node / op / error so a
// caller can classify them. The scan uses the seeded key itself as the prefix,
// so a healthy scan yields exactly that key with its seeded value; an iterator
// error at open OR at Next counts as a failure (the remote scan stream surfaces
// server-side errors at Next).
func probeReadsOnceDetailed(entries []*sharedNode, uk map[storageunit.UnitID]string, want []byte) []readFailure {
	var fails []readFailure
	for _, n := range entries {
		for u, k := range uk {
			got, err := n.Cluster.Get([]byte(k))
			if err != nil {
				fails = append(fails, readFailure{Op: "get", Key: k, Unit: u, Node: n.ID, Kind: failErr, Err: err})
			} else if !bytes.Equal(got, want) {
				fails = append(fails, readFailure{Op: "get", Key: k, Unit: u, Node: n.ID, Kind: failWrongValue, Got: got})
			}

			it, err := n.Cluster.ScanPrefix([]byte(k))
			if err != nil {
				fails = append(fails, readFailure{Op: "scan", Key: k, Unit: u, Node: n.ID, Kind: failErr, Err: err})
				continue
			}
			// NB: at R>1 the stored bytes are LWW envelopes and ScanPrefix
			// surfaces them raw, so the scan check is PRESENCE (the seeded key
			// comes back), not value equality - value fidelity is pinned by the
			// Get above.
			seen := 0
			for {
				kk, _, nerr := it.Next()
				if nerr != nil {
					fails = append(fails, readFailure{Op: "scan next", Key: k, Unit: u, Node: n.ID, Kind: failErr, Err: nerr})
					break
				}
				if kk == nil {
					if seen == 0 {
						fails = append(fails, readFailure{Op: "scan", Key: k, Unit: u, Node: n.ID, Kind: failEmptyScan})
					}
					break
				}
				seen++
			}
			_ = it.Close()
		}
	}
	return fails
}

// summarizeFails logs a get-vs-scan breakdown plus up to max failure lines.
func summarizeFails(t *testing.T, label string, fails []string, limit int) {
	t.Helper()
	if len(fails) == 0 {
		return
	}
	gets, scans := 0, 0
	for _, f := range fails {
		if strings.Contains(f, "get ") {
			gets++
		} else {
			scans++
		}
	}
	t.Logf("%s: %d read failures (%d get, %d scan); first %d:", label, len(fails), gets, scans, min(limit, len(fails)))
	for i, f := range fails {
		if i >= limit {
			break
		}
		t.Logf("  %s", f)
	}
}

// readFailureBreakdown counts probe failures by the distinctions a reader has to
// make before they can triage one, so a gate's message can report WHAT WAS
// OBSERVED instead of restating the property that was violated.
//
// Two independent axes, and both matter:
//
//   - REFUSAL vs VALUE fault. A refusal is a liveness event (the read declined
//     to answer, loudly); a wrong or absent value is a correctness event. No
//     availability argument can excuse the second, so a message that renders
//     them as one count ("N read failures") hides the only distinction that
//     changes what to do next.
//   - within refusals, the transient acquiring-window class (isAcquiringWindowErr:
//     a mount is in progress and the read will succeed once it lands) vs
//     anything else (a dial failure, a decode error, ResourceExhausted). The
//     first is bounded by a mount, the second is not.
type readFailureBreakdown struct {
	Acquiring  int // refusal, transient acquiring-window class
	OtherErr   int // refusal, outside that class
	WrongValue int // Get returned bytes that are not the seeded value
	EmptyScan  int // ScanPrefix yielded nothing for a seeded, acked key
}

func (b readFailureBreakdown) total() int {
	return b.Acquiring + b.OtherErr + b.WrongValue + b.EmptyScan
}

// String renders the breakdown for a test message. It states counts only: the
// classes are measured, the cause of any of them is not.
func (b readFailureBreakdown) String() string {
	return fmt.Sprintf("%d total = %d acquiring-window refusals, %d other errors, %d wrong values, %d empty scans",
		b.total(), b.Acquiring, b.OtherErr, b.WrongValue, b.EmptyScan)
}

// classifyReadFailures counts fails onto the axes above.
func classifyReadFailures(fails []readFailure) readFailureBreakdown {
	var b readFailureBreakdown
	for _, f := range fails {
		switch {
		case f.Kind == failWrongValue:
			b.WrongValue++
		case f.Kind == failEmptyScan:
			b.EmptyScan++
		case isAcquiringWindowErr(f.Err):
			b.Acquiring++
		default:
			b.OtherErr++
		}
	}
	return b
}

// summarizeReadFailures logs the measured breakdown plus up to limit failure
// lines, and returns the breakdown so the caller's assertion can report the same
// numbers it just logged.
func summarizeReadFailures(t *testing.T, label string, fails []readFailure, limit int) readFailureBreakdown {
	t.Helper()
	b := classifyReadFailures(fails)
	if len(fails) == 0 {
		return b
	}
	t.Logf("%s: %s; first %d:", label, b, min(limit, len(fails)))
	for i, f := range fails {
		if i >= limit {
			break
		}
		t.Logf("  %s", f)
	}
	return b
}

// waitMountWindowOpen blocks until c reports at least one desired-but-unmounted
// position: the MEASURED form of "this node is mid-mount", which is the state
// the during-transition probes have to run in for their result to mean anything.
//
// It exists because the alternative - assuming the window is open because a
// modeled acquire latency was armed - is a race against everything that runs
// between arming it and probing (a gossip convergence wait, most of all). Losing
// that race produces a gate that fails having observed no reads at all, which is
// a fixture-window miss reported as a transparency result.
//
// The two non-positive readings are separated deliberately: desiredButUnmounted
// returns a NEGATIVE count when DebugState's summary line does not parse, which
// makes the reading unusable rather than zero, and silently treating that as
// "nothing pending" would skip the probes for a reason that has nothing to do
// with mounts.
func waitMountWindowOpen(t *testing.T, c *cluster.Cluster, who string, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := 0
	for time.Now().Before(deadline) {
		last = desiredButUnmounted(c)
		if last < 0 {
			t.Fatalf("%s: desiredButUnmounted could not parse DebugState's summary line (got %d); the "+
				"mount-window reading is unusable, so the probes below would be skipped for a reason "+
				"unrelated to mounting. State:\n%s", who, last, c.DebugState())
		}
		if last > 0 {
			return last
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s: no desired-but-unmounted position appeared within %s, so the mount window this gate "+
		"probes in never opened. OBSERVED: desiredButUnmounted=%d. This is a fixture-window miss, not a "+
		"read-transparency result - no read was issued. State:\n%s", who, timeout, last, c.DebugState())
	return 0
}

// TestJoinReadTransparent_GetScanEveryNode: a JOIN alone must not produce
// client-visible read failures. A 4th node with serving markers planted
// boot-defers everything (Joining bit) and slow-mounts via the overlap
// reconcile; Get AND ScanPrefix for every unit through every node must keep
// answering from the union (the displaced owner / co-replica still physically
// hold every moving position).
func TestJoinReadTransparent_GetScanEveryNode(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence semantics, both toggles): this gate pins
	// the union READ mechanism in isolation with ZERO-tolerance
	// single-attempt probes - the displaced owner must keep serving reads
	// through rt-d's 8s modeled mount, which needs fence-at-completion
	// timing (eager fence would fence it at open-START) AND
	// reads-pass-through on the sub-second post-completion fence windows
	// (strict read fencing would turn those into client-visible blips this
	// gate counts as failures). Production close-on-fence read behavior is
	// covered by TestUnionReadRetry_ServesThroughFenceWindow (pkg/cluster)
	// and the real-slatedb TestMinIO_WriterEpochFencing pins; the eager
	// wedge residual by TestJoinResidual_FenceAtOpenStart_MovingShardsWedge.
	backing.SetEagerFence(false)
	backing.SetStrictReadFencing(false)

	n1 := startReplicatedNodeCfg(t, "rt-a", "", uc, rf, backing, nil)
	n2 := startReplicatedNodeCfg(t, "rt-b", n1.BindAddr, uc, rf, backing, nil)
	n3 := startReplicatedNodeCfg(t, "rt-c", n1.BindAddr, uc, rf, backing, nil)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)

	uk := oneKeyPerUnit(t, n1.Cluster, uc)
	plantAllServingMarkers(t, n1.Handle, uc, rf)

	// Require at least one unit whose PRIMARY moves onto the newcomer - the
	// single-owner-routed scan path's worst case.
	new4 := []string{"rt-a", "rt-b", "rt-c", "rt-d"}
	primaryMoves := 0
	for u := range uk {
		if replicaIDsForMembers(new4, u, rf)[0] == "rt-d" {
			primaryMoves++
		}
	}
	if primaryMoves == 0 {
		t.Fatalf("FIXTURE DRIFT: no unit's primary moves onto the 4th node under this hashing - the ring placement " +
			"changed; recompute node names so the single-owner-routed scan worst case stays covered")
	}
	t.Logf("%d of %d units get rt-d as PRIMARY on the 4-node ring", primaryMoves, uc)

	// THE MOUNT WINDOW IS HELD OPEN, NOT TIMED. rt-d's modeled acquire latency is
	// armed far longer than any convergence and is closed EXPLICITLY below, once
	// the probes have run. Sizing it against wall clock instead raced the
	// UNBOUNDED gossip-convergence wait that follows: on a slow runner the whole
	// modeled mount elapses while the cluster is still converging, the probe loop
	// then finds nothing outstanding and the gate fails HAVING ISSUED NO READ. That
	// is a fixture-window miss reported as a transparency verdict, and it is the
	// failure this test produced in CI (the previous 8s latency against a
	// convergence wait that took longer). Holding the window open removes the race
	// rather than widening it; a longer fixed latency would only move it.
	//
	// The armed latency only has to outlive the BOUNDED waits between arming it
	// and the last probe: the 30s membership convergence, the 30s
	// waitMountWindowOpen, and ~1.2s of probing - about 62s worst case, each of
	// which fails loudly on its own timeout rather than silently eating the
	// window. 5 minutes is that bound with room to spare. It is never actually
	// slept: the probes take ~1.2s and the latency is released immediately after,
	// so the test still runs in ~3s.
	const mountDelay = 5 * time.Minute
	n4 := startReplicatedNodeSlowAcquire(t, "rt-d", n1.BindAddr, uc, rf, backing, mountDelay, 2*time.Second)
	all := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}
	if err := waitForMembersAll(all, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}

	// Prove the window is open BEFORE issuing a read, so "no rounds ran" can never
	// be silently reinterpreted as "the join was transparent".
	pending := waitMountWindowOpen(t, n4.Cluster, "rt-d", 30*time.Second)
	t.Logf("rt-d mid-mount with %d desired-but-unmounted positions; probing", pending)

	// Probe while rt-d is provably mid-mount.
	entries := []*sharedNode{n1, n2, n3, n4}
	var fails []readFailure
	const wantRounds = 4
	rounds := 0
	for r := 0; r < wantRounds; r++ {
		fails = append(fails, probeReadsOnceDetailed(entries, uk, []byte("seed"))...)
		rounds++
		if got := desiredButUnmounted(n4.Cluster); got <= 0 {
			t.Fatalf("rt-d's mount window closed mid-probe after %d of %d rounds (desiredButUnmounted=%d) "+
				"even though its acquire latency is held open until this loop finishes; the window is no "+
				"longer under the fixture's control", rounds, wantRounds, got)
		}
		time.Sleep(300 * time.Millisecond)
	}
	// Close the window explicitly: everything above ran with rt-d mid-mount.
	for _, n := range []*sharedNode{n1, n2, n3, n4} {
		n.Handle.SetAcquireDelay(0)
	}
	t.Logf("probed %d rounds during rt-d's mount window", rounds)
	b := summarizeReadFailures(t, "JOIN", fails, 12)
	if len(fails) > 0 {
		t.Fatalf("OBSERVED during rt-d's mount, across %d probe rounds through every entry node: %s. "+
			"A join must stay read-transparent - the union's still-mounted holders keep serving while the "+
			"newcomer warms - so every one of these is client-visible. Reported as observation, not "+
			"diagnosis: the counts above are measured, the cause is not. Measure before attributing one.",
			rounds, b)
	}
}

// TestSurgeReadTransparent_JoinLeaveJoinComposition mirrors the k8s surge
// rollout (maxSurge=1 maxUnavailable=0): a new node joins, an old node is
// gracefully removed once the join settles, and the NEXT new node joins 2s
// after the leave starts. Reads (Get + ScanPrefix) through every stable live
// node must stay transparent through the whole composition.
func TestSurgeReadTransparent_JoinLeaveJoinComposition(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence semantics, both toggles): zero-tolerance
	// single-attempt read probes through the whole join/leave/join surge
	// timeline; see the justification on
	// TestJoinReadTransparent_GetScanEveryNode above.
	backing.SetEagerFence(false)
	backing.SetStrictReadFencing(false)
	mutate := func(cfg *cluster.Config) {
		cfg.GracefulLeaveDrainTimeout = 20 * time.Second
		cfg.OpenConcurrency = 8
	}
	na := startReplicatedNodeCfg(t, "sg-a", "", uc, rf, backing, mutate)
	nb := startReplicatedNodeCfg(t, "sg-b", na.BindAddr, uc, rf, backing, mutate)
	nc := startReplicatedNodeCfg(t, "sg-c", na.BindAddr, uc, rf, backing, mutate)
	if err := waitForMembersAll([]*cluster.Cluster{na.Cluster, nb.Cluster, nc.Cluster}, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)

	uk := oneKeyPerUnit(t, nb.Cluster, uc)
	plantAllServingMarkers(t, nb.Handle, uc, rf)

	var fails []string
	record := func(phase string, fs []string) {
		for _, f := range fs {
			fails = append(fails, phase+": "+f)
		}
	}

	// PHASE 1: join sg-d (slow mount, boot-defers via the planted markers).
	const mountDelay = 5 * time.Second
	nd := startReplicatedNodeSlowAcquireCfg(t, "sg-d", nb.BindAddr, uc, rf, backing, mountDelay, 2*time.Second, mutate)
	if err := waitForMembersAll([]*cluster.Cluster{na.Cluster, nb.Cluster, nc.Cluster, nd.Cluster}, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	for r := 0; r < 3 && desiredButUnmounted(nd.Cluster) > 0; r++ {
		record("join-d", probeReadsOnce([]*sharedNode{na, nb, nc, nd}, uk, []byte("seed")))
		time.Sleep(300 * time.Millisecond)
	}
	// Let sg-d finish warming (the surge waits ~35s between transitions).
	warmDeadline := time.Now().Add(30 * time.Second)
	for desiredButUnmounted(nd.Cluster) > 0 && time.Now().Before(warmDeadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if desiredButUnmounted(nd.Cluster) > 0 {
		t.Fatalf("sg-d never finished warming")
	}

	// PHASE 2: gracefully remove sg-a while sg-d's join has just settled, and
	// PHASE 3: join sg-e 2s later - the leave+join overlap the surge produces.
	for _, n := range []*sharedNode{nb, nc, nd} {
		n.Handle.SetAcquireDelay(2 * time.Second)
	}
	drainDone := make(chan struct{})
	go func() { defer close(drainDone); _ = na.Cluster.Close() }()
	time.Sleep(2 * time.Second)
	ne := startReplicatedNodeSlowAcquireCfg(t, "sg-e", nb.BindAddr, uc, rf, backing, 4*time.Second, 2*time.Second, mutate)

	// Probe continuously through the stable live nodes (sg-b, sg-c, sg-d),
	// adding sg-e as an entry once its ring has converged on the final 4-node
	// membership, until the leave has completed AND sg-e has fully warmed.
	stable := []*sharedNode{nb, nc, nd}
	surgeDeadline := time.Now().Add(45 * time.Second)
	drained := false
	for time.Now().Before(surgeDeadline) {
		entries := stable
		if len(ne.Cluster.Members()) == 4 {
			entries = append(entries, ne)
		}
		record("leave-a+join-e", probeReadsOnce(entries, uk, []byte("seed")))
		select {
		case <-drainDone:
			drained = true
		default:
		}
		if drained && desiredButUnmounted(ne.Cluster) == 0 && len(ne.Cluster.Members()) == 4 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	for _, n := range []*sharedNode{nb, nc, nd, ne} {
		n.Handle.SetAcquireDelay(0)
	}
	if !drained {
		select {
		case <-drainDone:
		case <-time.After(30 * time.Second):
			t.Fatalf("sg-a's graceful leave never completed")
		}
	}

	// Steady state after the surge: everything must be transparent again.
	if err := waitForMembersAll([]*cluster.Cluster{nb.Cluster, nc.Cluster, nd.Cluster, ne.Cluster}, 4, 30*time.Second); err != nil {
		t.Fatalf("post-surge convergence: %v", err)
	}
	settleDeadline := time.Now().Add(20 * time.Second)
	var post []string
	for time.Now().Before(settleDeadline) {
		post = probeReadsOnce([]*sharedNode{nb, nc, nd, ne}, uk, []byte("seed"))
		if len(post) == 0 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if len(post) > 0 {
		summarizeFails(t, "POST-SURGE steady state (still failing after settle)", post, 12)
		t.Fatalf("reads still failing at steady state after the surge")
	}

	summarizeFails(t, "SURGE", fails, 20)
	if len(fails) > 0 {
		t.Fatalf("SURGE composition is not read-transparent: %d client-visible read failures across the "+
			"join/leave/join timeline - the k8s rollout probe failure reproduced", len(fails))
	}
}

// TestLeaveEntryServesWhileDraining pins ENTRY-DURING-DRAIN (docs/SPEC.md
// "Union reads"): a gracefully-leaving node keeps serving Get and ScanPrefix
// as the ENTRY node for the whole drain - DrainForLeave runs at the top of
// Close BEFORE the closed flip, so the not-ready fast-fail engages only once
// Close's teardown actually begins. After Close returns, entry ops fail fast
// with backend.ErrClosed (the pinned boundary); covering that teardown tail is
// the orchestrator's routing-drain job, not shale's.
func TestLeaveEntryServesWhileDraining(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence semantics, both toggles): the draining
	// leaver serves ENTRY reads (including its own local legs) for the
	// whole drain while survivors slow-mount its positions; see the
	// justification on TestJoinReadTransparent_GetScanEveryNode above.
	backing.SetEagerFence(false)
	backing.SetStrictReadFencing(false)
	mutate := func(cfg *cluster.Config) {
		cfg.GracefulLeaveDrainTimeout = 25 * time.Second
		cfg.OpenConcurrency = 8
		cfg.RebalanceSettleDelay = 300 * time.Millisecond
	}
	n1 := startReplicatedNodeCfg(t, "le-a", "", uc, rf, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "le-b", n1.BindAddr, uc, rf, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "le-c", n1.BindAddr, uc, rf, backing, mutate)
	n4 := startReplicatedNodeCfg(t, "le-d", n1.BindAddr, uc, rf, backing, mutate)
	all := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}
	if err := waitForMembersAll(all, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)
	uk := oneKeyPerUnit(t, n1.Cluster, uc)

	// Slow the survivors' acquires so the drain window is wide enough to probe.
	for _, n := range []*sharedNode{n1, n2, n3} {
		n.Handle.SetAcquireDelay(2500 * time.Millisecond)
	}
	drainDone := make(chan struct{})
	go func() { defer close(drainDone); _ = n4.Cluster.Close() }()
	time.Sleep(600 * time.Millisecond) // let the Draining bit gossip

	// ENTRY THROUGH THE LEAVER, for the whole drain: every Get and ScanPrefix
	// must serve (single attempt per op; the union + the leaver's still-mounted
	// copies cover everything).
	var fails []string
	rounds := 0
drainLoop:
	for {
		select {
		case <-drainDone:
			break drainLoop
		default:
		}
		roundFails := probeReadsOnce([]*sharedNode{n4}, uk, []byte("seed"))
		// The entry fast-fail (raw backend.ErrClosed) begins the moment Close
		// flips the closed flag - INSIDE Close, before the teardown finishes
		// and drainDone closes. On a slow runner that post-flip stretch fits
		// whole probe rounds, so drainDone is NOT a reliable end-of-serving
		// signal; the observed ErrClosed IS. The first sighting marks the end
		// of the drain's serving phase (the boundary asserted separately
		// below). Stop there; only NON-closed failures inside the serving
		// window count against the pin.
		sawClosed := false
		for _, f := range roundFails {
			if strings.Contains(f, backend.ErrClosed.Error()) {
				sawClosed = true
			} else {
				fails = append(fails, f)
			}
		}
		if sawClosed {
			break drainLoop
		}
		select {
		case <-drainDone:
			break drainLoop
		default:
		}
		rounds++
		time.Sleep(250 * time.Millisecond)
		if rounds > 120 {
			break
		}
	}
	for _, n := range []*sharedNode{n1, n2, n3} {
		n.Handle.SetAcquireDelay(0)
	}
	if rounds < 3 {
		t.Fatalf("drain completed after only %d probe rounds; widen the acquire delay for a provable window", rounds)
	}
	summarizeFails(t, "ENTRY-THROUGH-LEAVER (drain window)", fails, 12)
	if len(fails) > 0 {
		t.Fatalf("a draining node refused entry reads %d times across %d rounds - entry traffic must be served "+
			"for the whole drain (notReady flips only at the actual close)", len(fails), rounds)
	}

	// After Close returns: the pinned fast-fail boundary.
	t0 := time.Now()
	_, err := n4.Cluster.Get([]byte(uk[0]))
	if !errors.Is(err, backend.ErrClosed) {
		t.Fatalf("entry Get after Close = %v, want backend.ErrClosed", err)
	}
	if el := time.Since(t0); el > 100*time.Millisecond {
		t.Fatalf("post-Close entry Get took %v - must fail fast", el)
	}
	t.Logf("entry served through %d drain rounds; post-Close entry fails fast with ErrClosed", rounds)
}
