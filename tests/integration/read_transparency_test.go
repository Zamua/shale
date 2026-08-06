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
//   - LEAVE alone: one node of four drains gracefully (Draining bit) while
//     survivors slow-mount its positions; probes run through the drain AND
//     through the post-exit window right after the leaver departs the ring
//     (where the consistent-hash reshuffle can shift surviving replicas to
//     NEW position indices they have not mounted yet).
//   - SURGE composition: join, then a leave of a different node overlapping
//     the settling, then the next join 2s later - the k8s rollout timeline.
//
// Every probe is a SINGLE attempt (no retry): the gate is transparency, not
// eventual success. A probe failure is a client-visible read error.
//
// THE OBSERVATION WINDOW IS PART OF THE ASSERTION. Each gate here is only
// meaningful while its transition is actually in flight, and none of these
// windows stays open on its own: a modeled mount latency elapses, a drain
// finishes. Both gates below previously let their window be closed by whatever
// ran before the probes - a gossip convergence wait, most of all - and the two
// failed in OPPOSITE, equally useless directions:
//
//   - the JOIN gate failed LOUDLY but vacuously ("rt-d mounted before any
//     during-join probe ran"), reporting a fixture-window miss as a
//     transparency verdict. That is the failure it produced in CI, on a merge
//     that changed no executable code.
//   - the LEAVE gate failed SILENTLY: nothing required the drain window to have
//     been observed at all, so a fast drain left it asserting nothing while
//     reporting a pass. Its own sibling pin (TestLeaveEntryServesWhileDraining)
//     already had that guard; this one was simply missing it.
//
// So each gate now OPENS its window explicitly, PROVES it is open before issuing
// a read, and CLOSES it itself. A window that a slow runner can close is not a
// window; it is a race whose loss is reported as a result.
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

// classifyReadFailures counts fails onto the axes above. It is the single
// counting implementation the read-transparency gates report through, so a
// message in one of them cannot drift from a message in another.
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

// acquireInFlight reports how many replica positions c currently has an
// OpenReplicaUnit call OUTSTANDING for, parsed from DebugState's
// acquire-in-flight line. It returns a NEGATIVE count when that line is absent,
// which makes an unparseable dump an unusable reading rather than a zero.
//
// It is the LEAVE-side analogue of desiredButUnmounted, and it has to be a
// different reading. Through a graceful leave the leaver stays a CURRENT OWNER
// in the ring, so a survivor taking one of its positions over gains that
// position in the PENDING set, never the desired one - desiredButUnmounted
// reads 0 on every survivor for the whole drain (measured: 0/0/0 across all
// seven drain rounds of a passing local run). What IS observable is the
// survivor's outstanding open, which is precisely what the fixture's armed
// acquire delay holds, so it is the reading that can prove the fixture, and not
// a race, is holding the window open.
func acquireInFlight(c *cluster.Cluster) int {
	for _, line := range strings.Split(c.DebugState(), "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "acquire-in-flight=[")
		if !ok {
			continue
		}
		return len(strings.Fields(strings.TrimSuffix(rest, "]")))
	}
	return -1
}

// totalAcquireInFlight sums acquireInFlight over nodes, propagating an unusable
// (negative) reading instead of folding it into the total.
func totalAcquireInFlight(nodes []*sharedNode) int {
	total := 0
	for _, n := range nodes {
		got := acquireInFlight(n.Cluster)
		if got < 0 {
			return got
		}
		total += got
	}
	return total
}

// waitAcquireWindowOpen blocks until at least one of nodes has an outstanding
// OpenReplicaUnit call, and returns how many are outstanding: the MEASURED form
// of "the armed acquire delay is now holding a mount open on a survivor", which
// is the mechanism that keeps a graceful leave's drain from completing while the
// probes run.
//
// done is the drain's completion channel. If it closes first, the drain finished
// without any survivor open ever being held, which means the armed delay is NOT
// what gates this drain - so no value of that delay could hold the window and
// the fixture must say so rather than probe a window that is already shut.
func waitAcquireWindowOpen(t *testing.T, nodes []*sharedNode, done <-chan struct{}, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := 0
	for time.Now().Before(deadline) {
		last = totalAcquireInFlight(nodes)
		if last < 0 {
			t.Fatalf("acquireInFlight could not find DebugState's acquire-in-flight line (got %d); the "+
				"window reading is unusable, so the probes below would run on an unproven window", last)
		}
		if last > 0 {
			return last
		}
		select {
		case <-done:
			t.Fatalf("the drain completed before any survivor open was outstanding, so the armed acquire "+
				"delay is not what gates this drain and no value of it can hold the window open. OBSERVED: "+
				"acquire-in-flight=%d across the survivors. This is a fixture failure, not a read-transparency "+
				"result - no read was issued during the drain", last)
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("no survivor had an outstanding open within %s, so the mount the drain waits on never started "+
		"and the drain window this gate probes in was never held. OBSERVED: acquire-in-flight=%d", timeout, last)
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

// TestLeaveReadTransparent_GetScanEveryNode: a graceful LEAVE must not produce
// client-visible read failures - through the drain window AND through the
// post-exit window right after the leaver departs the ring, where the
// consistent-hash reshuffle can move surviving replicas to NEW position
// indices (whose independent databases they have not mounted yet) while the
// bytes still sit mounted at their OLD indices.
func TestLeaveReadTransparent_GetScanEveryNode(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	// OPT-OUT (permissive fence semantics, both toggles): zero-tolerance
	// single-attempt read probes through the drain + post-exit windows
	// require the draining leaver (and the old-index holders) to keep
	// serving reads while successors slow-mount; see the justification on
	// TestJoinReadTransparent_GetScanEveryNode above.
	backing.SetEagerFence(false)
	backing.SetStrictReadFencing(false)
	// drainBudget is the leaver's GracefulLeaveDrainTimeout, and it is NAMED
	// because the drain-window guard below reports it: the budget ends the leave
	// whether or not the hand-off converged, so it is the ceiling on the window
	// this gate probes in, and a message that quoted a stale copy of it would send
	// the next reader after the wrong knob.
	// Generous BY CONSTRUCTION, not by headroom. The window this gate probes in
	// must be closed by the fixture's own release below, never by a product
	// timer racing the runner: at 20s against a ~1.2s probe phase the margin
	// looked ample and still lost on a loaded CI runner, because "enough time"
	// is a bet on scheduling rather than a property. The successors are pinned
	// mid-mount for drainMountDelay, so hand-off CANNOT complete and this budget
	// is the only other way the drain ends - set it past any plausible probe
	// phase and the guard below still fails loudly if it is ever reached.
	const drainBudget = 4 * time.Minute
	mutate := func(cfg *cluster.Config) {
		cfg.GracefulLeaveDrainTimeout = drainBudget
		cfg.OpenConcurrency = 8
	}
	n1 := startReplicatedNodeCfg(t, "rl-a", "", uc, rf, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "rl-b", n1.BindAddr, uc, rf, backing, mutate)
	n3 := startReplicatedNodeCfg(t, "rl-c", n1.BindAddr, uc, rf, backing, mutate)
	n4 := startReplicatedNodeCfg(t, "rl-d", n1.BindAddr, uc, rf, backing, mutate)
	all := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster, n4.Cluster}
	if err := waitForMembersAll(all, 4, 30*time.Second); err != nil {
		t.Fatalf("4-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	uk := oneKeyPerUnit(t, n1.Cluster, uc)

	// Sanity: fully transparent before the transition.
	if pre := probeReadsOnceDetailed([]*sharedNode{n1, n2, n3, n4}, uk, []byte("seed")); len(pre) > 0 {
		b := summarizeReadFailures(t, "PRE-LEAVE (steady state)", pre, 12)
		t.Fatalf("reads were already failing BEFORE the transition, at steady state: %s. The fixture cannot "+
			"attribute anything to the leave from here", b)
	}

	// THE DRAIN WINDOW IS HELD OPEN, NOT TIMED. The drain ends when every position
	// the leaver owns has a successor PROVABLY SERVING, and a successor cannot
	// serve until its OpenReplicaUnit returns - so the survivors' armed acquire
	// delay IS what the drain waits on, and it is a knob this test holds and then
	// releases (SetAcquireDelay re-reads as it sleeps, so lowering it releases
	// opens ALREADY in flight rather than only later ones).
	//
	// Sizing that delay against wall clock is what broke: at 2.5s the window was
	// whatever fraction of 2.5s survived everything that ran before the probes, so
	// the round count was SAMPLED (6 to 52 on a developer machine, ONE on a 2-vCPU
	// CI runner) rather than chosen. Arming the delay far beyond any bounded wait
	// and releasing it explicitly removes that coupling instead of widening it.
	//
	// It is never slept. Measured cost of the probe phase below: a round's probes
	// take 0.04s to 0.09s even under GOMAXPROCS=2 (a stand-in for the 2-vCPU
	// runner), so four rounds plus their inter-round sleeps run in ~1.2s, after
	// which the delay drops to postExitMountDelay and the successors mount.
	//
	// RESIDUAL, stated rather than hidden: drainBudget is a SECOND way this window
	// can close, independent of the delay, because the leaver departs when its
	// drain budget expires whether or not its positions were handed off. That
	// budget is therefore the window's ceiling, and 20s against a measured ~1.2s
	// probe phase is better than an order of magnitude of headroom - but it is
	// headroom, not a guarantee, so the guard below fails LOUDLY if the ceiling is
	// ever hit instead of quietly asserting less.
	const drainMountDelay = 5 * time.Minute
	// The post-exit reshuffle window keeps the delay the whole test has always
	// used. Releasing to ZERO here would land the survivors' newly-assigned
	// positions instantly and shut the SECOND window this gate probes, trading one
	// unobserved window for another.
	const postExitMountDelay = 2500 * time.Millisecond
	survivors := []*sharedNode{n1, n2, n3}
	for _, n := range survivors {
		n.Handle.SetAcquireDelay(drainMountDelay)
	}
	// Registered AFTER the nodes' own t.Cleanup(Close), so it runs BEFORE them
	// (cleanups are LIFO): no teardown ever waits out an armed window, on ANY exit
	// path including a t.Fatalf from the probe loop below. This is what makes a
	// 5-minute delay affordable - the cost of arming a long window must not be
	// paid by a runner that never needed it.
	t.Cleanup(func() {
		for _, n := range survivors {
			n.Handle.SetAcquireDelay(0)
		}
	})

	drainDone := make(chan struct{})
	go func() { defer close(drainDone); _ = n4.Cluster.Close() }()
	time.Sleep(600 * time.Millisecond) // let the Draining bit gossip

	// PROVE THE WINDOW IS HELD BEFORE ISSUING A READ. A survivor with an
	// outstanding open is the drain's own blocker, so observing one is proof that
	// the delay - not luck - is what keeps the leaver draining while the probes
	// run.
	held := waitAcquireWindowOpen(t, survivors, drainDone, 30*time.Second)
	t.Logf("drain window held open: %d survivor opens outstanding under a %s acquire delay", held, drainMountDelay)

	var fails []readFailure
	drainRounds := 0
	// wantDrainRounds is CHOSEN, not sampled: the window is held until the loop
	// has run this many rounds, so the count no longer varies with the runner's
	// probe throughput. It matches the JOIN gate's round count above; each round
	// is 96 reads (16 units x 3 survivors x 2 ops).
	const wantDrainRounds = 4
drainLoop:
	for r := 0; r < wantDrainRounds; r++ {
		// CHECK BEFORE PROBING, not after. A probe issued while the leaver is
		// still draining can STRADDLE its departure: the budget expires
		// mid-probe, the leaver leaves the ring, and its successors are still
		// pinned mid-mount by the acquire delay above - so the tail of that
		// probe reads a unit with no holder anywhere in the union and records
		// refusals that did NOT happen in the drain window. Checking after the
		// probe counted them anyway, which is how this gate reported a
		// drain-window transparency violation for reads taken after the drain
		// ended.
		select {
		case <-drainDone:
			break drainLoop // the window shut under us; the guard below reports it
		default:
		}
		fails = append(fails, probeReadsOnceDetailed(survivors, uk, []byte("seed"))...)
		drainRounds++
		time.Sleep(300 * time.Millisecond)
	}
	// RELEASE THE WINDOW, down to the delay the post-exit window below runs on.
	// Every probe above ran with the leaver provably still draining.
	for _, n := range survivors {
		n.Handle.SetAcquireDelay(postExitMountDelay)
	}

	// THE DRAIN WINDOW MUST HAVE BEEN OBSERVED. Nothing about the leave itself
	// forces the drain to outlast the probes, so a drain that completed early
	// would leave this gate reporting "LEAVE is read-transparent" having issued no
	// read DURING the drain at all. A gate that can pass vacuously stops being a
	// gate, and that failure is silent, which is worse than the loud kind. Its
	// sibling pin TestLeaveEntryServesWhileDraining already requires its own
	// window to have been observed; this one was missing the same guard.
	//
	// Reaching the full count is now the fixture's job rather than the runner's:
	// the delay above is armed past any bounded wait here and is released only on
	// the line above this one. So a short count no longer means "this runner is
	// slow" - it means the delay stopped gating the drain, which is a fixture
	// fault worth failing on.
	if drainRounds < wantDrainRounds {
		t.Fatalf("the drain completed after only %d of %d probe rounds even though the survivors' acquire "+
			"delay is armed at %s and held until the loop finishes, so the drain window was never fully "+
			"observed and a pass here would assert less than it claims. The window is no longer under the "+
			"fixture's control - either the drain stopped waiting on a survivor mount, or it hit the "+
			"GracefulLeaveDrainTimeout ceiling (%s) that ends the leave regardless",
			drainRounds, wantDrainRounds, drainMountDelay, drainBudget)
	}
	// Log the window on PASSING runs too: the size of the window a gate observed
	// is the evidence that its pass means something, and a gate that reports it
	// only when it fails is one whose silence cannot be distinguished from having
	// looked at nothing.
	t.Logf("observed %d drain rounds while the leaver was draining", drainRounds)
	drainB := summarizeReadFailures(t, "LEAVE (drain window)", fails, 12)

	// Post-exit window: the leaver has left the ring; survivors converge to 3
	// and reconcile toward the genuinely-rebuilt 3-node placement, which can
	// differ from the drain-time successor-chain approximation (index shuffles).
	// The acquire delay is still armed at postExitMountDelay, so any newly-assigned
	// position is provably mid-mount while we probe.
	//
	// Wait for the leaver to actually depart first, and REPORT how it departed. A
	// leave whose hand-off converges returns shortly after the delay drops; one
	// whose hand-off does not converge returns when drainBudget fires and the
	// leave proceeds with positions still un-handed-off. Both genuinely end the
	// drain, and BOTH occur on this tree: across six runs of the pre-existing
	// fixture the leaver's Close took 3.0s, 3.0s, 3.0s, 10.4s, 10.8s and 20.1s,
	// that last one being the full budget. So the latency is logged, never
	// asserted - hand-off completeness is a leave-completion property that nothing
	// in this gate measures, and failing a READ gate on it would be naming a
	// mechanism it did not observe.
	tExit := time.Now()
	select {
	case <-drainDone:
		t.Logf("leaver departed %v after the acquire delay dropped to %s", time.Since(tExit).Round(time.Millisecond), postExitMountDelay)
	case <-time.After(90 * time.Second):
		t.Fatalf("the leaver's Close did not return within 90s of the acquire delay dropping to %s, which "+
			"is far past the %s drain budget that ends a leave unconditionally; the leave is stuck "+
			"somewhere other than the hand-off wait. State:\n%s", postExitMountDelay, drainBudget, n4.Cluster.DebugState())
	}
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}, 3, 20*time.Second); err != nil {
		t.Fatalf("post-leave convergence: %v", err)
	}
	var postFails []readFailure
	for r := 0; r < 6; r++ {
		postFails = append(postFails, probeReadsOnceDetailed(survivors, uk, []byte("seed"))...)
		time.Sleep(300 * time.Millisecond)
	}
	for _, n := range survivors {
		n.Handle.SetAcquireDelay(0)
	}
	postB := summarizeReadFailures(t, "LEAVE (post-exit window)", postFails, 12)
	fails = append(fails, postFails...)

	if len(fails) > 0 {
		t.Fatalf("OBSERVED across a leave: drain window (%d rounds) %s; post-exit window (6 rounds) %s. "+
			"A graceful leave must stay read-transparent - whoever in the union physically holds the "+
			"position keeps serving it - so every one of these is client-visible. Reported as observation, "+
			"not diagnosis: the windows and classes above are measured, the cause is not. The two windows "+
			"are reported separately because they fail for different reasons (the drain window is the "+
			"leaver still serving; the post-exit window is the consistent-hash reshuffle moving survivors "+
			"to positions they have not mounted).", drainRounds, drainB, postB)
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
