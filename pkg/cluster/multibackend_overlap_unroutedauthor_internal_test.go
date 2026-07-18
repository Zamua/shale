package cluster

// THE FULL-MOVE ROUTING GAP (deterministic, sub-second, no cluster).
//
// These tests pin the AUTHOR-ATTRIBUTED release gate: a draining owner must not
// surrender its last local copy of a position to a successor that is NOT in its
// own routed union, because after that release NOTHING it routes to holds the
// unit and every read through it fails until its membership view converges.
//
// The view under test is the STALE one gossip actually produces mid-transition:
// the JOINER's Joining bit has arrived, the LEAVER's Draining bit has not. It is
// reachable by construction - the two bits are independent gossip facts - and it
// is the view the prior tests never exercised (they hardcoded the CONVERGED
// joining={joiner} draining={leaver}, under which the pending computation is
// exact and nothing is stranded).
//
// The fixture DERIVES the affected position from ring arithmetic rather than
// hardcoding a unit number, and fails loudly if the ring placement ever changes
// such that no full-move position exists for this name set.

import (
	"strconv"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

const (
	uaSelf   = "sg-b"
	uaJoiner = "sg-e"
	uaLeaver = "sg-a"
)

var uaMembers = []string{"sg-a", "sg-b", "sg-c", "sg-d", "sg-e"}

// uaStrandedPosition finds a position under the STALE view that exhibits the
// full-move strand, and returns it with the true post-transition successor that
// will write its serving marker.
//
// The shape it searches for, all three conditions required:
//
//  1. self holds the position in CURRENT but not in PENDING -> self arms a drain
//     (the beginDrain trigger in reconcileReplicaUnitsOverlap).
//  2. the TRUE post-transition placement (computed independently of the SUT, over
//     a ring genuinely built from the survivors) assigns this position to some
//     OTHER node - the successor whose marker will gate self's release.
//  3. that successor is NOT in self's stale ROUTED union -> releasing to it
//     strands the unit behind an unroutable set.
func uaStrandedPosition(t *testing.T, c *Cluster, units int, rf int) (storageunit.ReplicaUnit, string) {
	t.Helper()

	// The true post-transition ring: everyone except the leaver. Built here, from
	// the member list, so it is not derived from the code under test.
	survivors := ring.New()
	for _, id := range uaMembers {
		if id == uaLeaver {
			continue
		}
		survivors.Add(ring.Member{ID: id, Addr: id + ":0"})
	}

	for u := 0; u < units; u++ {
		gu := storageunit.NewGenUnit(0, storageunit.UnitID(u))
		final := survivors.LocateKeyN(genUnitBytes(gu), rf)

		current := c.currentUnitReplicas(gu, c.joining)
		pending := c.pendingUnitReplicas(gu, c.draining)
		routed := unionMembers(current, pending)

		selfIdx := -1
		for i, m := range current {
			if m.ID == uaSelf {
				selfIdx = i
			}
		}
		if selfIdx < 0 || containsMember(pending, uaSelf) {
			continue // self does not arm a drain for this unit.
		}
		if selfIdx >= len(final) {
			continue
		}
		successor := final[selfIdx].ID
		if successor == uaSelf || containsMember(routed, successor) {
			continue // the successor IS reachable: not the stranding shape.
		}
		t.Logf("STRAND: %s r%d | self=%s stale routed=%v | true post-leave=%v | successor at r%d = %s (UNROUTED)",
			gu, selfIdx, uaSelf, memberIDs(routed), memberIDs(final), selfIdx, successor)
		return storageunit.NewReplicaUnit(gu, uint8(selfIdx)), successor
	}
	t.Fatalf("FIXTURE DRIFT: no full-move strand exists for members %v under the stale view "+
		"(joining=%s, draining unseen); the ring placement changed - pick a new name set",
		uaMembers, uaJoiner)
	return storageunit.ReplicaUnit{}, ""
}

// uaStaleViewCluster builds self's cluster in the STALE membership view: the
// joiner's Joining bit observed, the leaver's Draining bit NOT yet.
func uaStaleViewCluster(t *testing.T, backing *sharedfactory.Backing, units, rf int) *Cluster {
	t.Helper()
	c := newReplicatedCluster(t, uaSelf, units, rf, backing, uaMembers...)
	c.joining = map[string]struct{}{uaJoiner: {}}
	c.draining = map[string]struct{}{} // non-nil + empty: the bit has not arrived.
	return c
}

// uaMountAndDrain mounts target on c at a fresh open epoch, records the epoch the
// way the real mount flip does, and arms the drain. It returns c's open epoch.
func uaMountAndDrain(t *testing.T, c *Cluster, backing *sharedfactory.Backing, target storageunit.ReplicaUnit) storageunit.Epoch {
	t.Helper()
	b, epoch, err := backing.Handle().OpenReplicaUnit(target, acquireBaseEpoch)
	if err != nil {
		t.Fatalf("self open %s: %v", target, err)
	}
	c.mountMap[target] = b
	c.myOpenEpoch.Store(target, epoch)
	c.beginDrain(target)
	if st := c.handoffPhaseOf(target); st.Phase != storageunit.PhaseDraining {
		t.Fatalf("beginDrain should have armed Draining for %s, got %v", target, st.Phase)
	}
	return epoch
}

// uaSuccessorMarks has successorID open target (bumping the durable) and write
// its AUTHORED serving marker, exactly as its own mount flip would.
func uaSuccessorMarks(t *testing.T, backing *sharedfactory.Backing, target storageunit.ReplicaUnit, successorID string) storageunit.Epoch {
	t.Helper()
	h := backing.Handle()
	_, epoch, err := h.OpenReplicaUnit(target, acquireBaseEpoch)
	if err != nil {
		t.Fatalf("successor %s open %s: %v", successorID, target, err)
	}
	if err := h.WriteServingMarkerFrom(target, epoch, successorID); err != nil {
		t.Fatalf("successor %s marker: %v", successorID, err)
	}
	return epoch
}

// TestOverlap_drainCheck_HoldsLastCopyFromUnroutedAuthor is THE REPRO. Under the
// stale view self holds the only routable copy of the strand position. The true
// successor - a node self does NOT route to - mounts and writes its serving
// marker strictly above self's open epoch, so the EPOCH gate trips.
//
// Pre-fix the release rule was author-anonymous, so self released on that marker
// alone and the unit became unreadable through self: its routed union names only
// nodes that never held the position, every leg answers transiently, and the
// all-legs-transient retry spins to the ReadTimeout (the client-visible Get
// DeadlineExceeded / ScanPrefix handing-off error hostthis captured).
//
// Post-fix self HOLDS: the marker's author is not in its routed union, so it
// keeps serving until its view converges (or the liveness backstop fires).
func TestOverlap_drainCheck_HoldsLastCopyFromUnroutedAuthor(t *testing.T) {
	const units, rf = 16, 2
	backing := sharedfactory.NewBacking()
	c := uaStaleViewCluster(t, backing, units, rf)

	target, successor := uaStrandedPosition(t, c, units, rf)
	selfEpoch := uaMountAndDrain(t, c, backing, target)
	markerEpoch := uaSuccessorMarks(t, backing, target, successor)
	if markerEpoch <= selfEpoch {
		t.Fatalf("precondition: successor marker %d must be strictly above self's open epoch %d "+
			"(otherwise the epoch gate alone would hold and the test would not exercise the author gate)",
			markerEpoch, selfEpoch)
	}

	c.drainCheck(target)

	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("LAST-COPY RELEASE TO AN UNROUTED SUCCESSOR: self released %s on a marker authored by %q "+
			"(epoch %d > own %d), but %q is NOT in self's routed set %v - after this release NOTHING self routes "+
			"to holds the unit, so every read through self fails until its membership view converges",
			target, successor, markerEpoch, selfEpoch, successor, memberIDs(c.routedMembersForUnit(target.Unit)))
	}
	if st := c.handoffPhaseOf(target); st.Phase != storageunit.PhaseDraining {
		t.Fatalf("a held position must stay Draining (and keep serving), got %v", st.Phase)
	}
}

// TestOverlap_StaleView_HeldCopyIsFencedBySuccessor_ReadStillFails is a
// CHARACTERIZATION test: it pins the MEASURED behavior that holding the release
// is NOT sufficient to restore read availability under production fence
// semantics. It is deliberately an assertion about what the system does today,
// not about what it should do.
//
// The chain it pins, driving the ACTUAL read sweep a client Get runs:
//
//  1. Under the stale view self's routed union excludes every true holder, so
//     self's own copy is the only leg that could answer (the peers are blocked
//     to model routed legs that hold nothing).
//  2. The successor's OpenReplicaUnit bumps the position's durable epoch at
//     open-START (eager fencing, real slatedb's DbBuilder.Build timing, the
//     sharedfactory default). That FENCES self's handle.
//  3. The fence lands BEFORE the marker exists - the marker is written after the
//     successor's mount completes - so by the time self's drainCheck can observe
//     anything, self has already been unable to serve for the whole duration of
//     the successor's open.
//  4. So the read fails whether self holds or releases. The hold preserves a
//     handle that cannot answer.
//
// CONSEQUENCE, and the reason this test exists rather than a green one: the
// full-move availability hole is a ROUTING defect, not a release-timing defect.
// It opens at the successor's OPEN-START, not at the predecessor's release, and
// no release-side gate can close it - the nodes that hold the data are simply
// absent from the stale view's routed union. Closing it requires making the true
// holder REACHABLE from the stale node. The author attribution this file's other
// tests exercise is the datum that makes that possible (the marker names the node
// that provably serves the position), but consuming it as a ROUTING hint is a
// separate, unadjudicated change to the hot path.
//
// When that routing fix lands, this test should FLIP to asserting the read
// SUCCEEDS. Until then it documents why the release gate alone is not the fix.
func TestOverlap_StaleView_HeldCopyIsFencedBySuccessor_ReadStillFails(t *testing.T) {
	const units, rf = 16, 2
	backing := sharedfactory.NewBacking()
	c := uaStaleViewCluster(t, backing, units, rf)
	target, successor := uaStrandedPosition(t, c, units, rf)

	// A key that lands in the strand unit, so the read routes to that union.
	var key []byte
	for i := 0; i < 100000; i++ {
		k := []byte("probe-" + strconv.Itoa(i))
		if c.genUnitForKey(k) == target.Unit {
			key = k
			break
		}
	}
	if key == nil {
		t.Fatalf("no key hashes into %s", target.Unit)
	}

	// The two peers in the strand union are nodes that never held the unit. Block
	// peer dials so they answer the way an un-mounted routed leg does in the real
	// cluster (nothing usable), leaving self's own copy as the only possible
	// source of an answer. That is what makes the assertion below exact.
	c.peerClientsBlocked = true

	b, epoch, err := backing.Handle().OpenReplicaUnit(target, acquireBaseEpoch)
	if err != nil {
		t.Fatalf("self open: %v", err)
	}
	want := []byte("the-value")
	if err := b.Put(key, Encode(Envelope{Stamp: Stamp{TimestampNanos: 1, NodeID: uaSelf}, Payload: want})); err != nil {
		t.Fatalf("seed: %v", err)
	}
	c.mountMap[target] = b
	c.myOpenEpoch.Store(target, epoch)
	c.beginDrain(target)
	uaSuccessorMarks(t, backing, target, successor)

	// PRECONDITION: self is genuinely the only leg that can answer - the union
	// excludes every true holder, so this is the full-move strand, not a case
	// some other replica quietly covers.
	routed, _ := c.routedReplicasWithUnit(key)
	for _, rr := range routed {
		if rr.member.ID != uaSelf && containsMember([]ring.Member{{ID: successor}}, rr.member.ID) {
			t.Fatalf("precondition: the true successor %q must be absent from the routed union", successor)
		}
	}

	c.drainCheck(target)

	// The release gate HELD the copy (that part works, and is what this file's
	// other tests pin).
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("precondition: the release gate should have HELD %s against the unrouted author %q",
			target, successor)
	}

	// ...and the read STILL fails, because the held handle was fenced by the
	// successor's open before the marker it is gated on ever existed.
	if _, err := b.Get(key); err == nil {
		t.Fatalf("EXPECTED THE HELD COPY TO BE FENCED: self's handle served a read after %q opened the "+
			"position at a higher epoch. If this now passes, the fence timing changed and the whole "+
			"analysis below (and the routing-fix conclusion) must be re-derived", successor)
	}

	_, err = c.getReplicatedUnitOnce(time.Now().Add(2*time.Second), key)
	if err == nil {
		t.Fatalf("READ UNEXPECTEDLY SUCCEEDED for %s (value %q). The routed union %v contains no node "+
			"holding the position and self's copy is fenced, so an answer means routing changed - "+
			"re-derive whether the full-move hole is still open", target.Unit, want,
			memberIDs(c.routedMembersForUnit(target.Unit)))
	}
	t.Logf("MEASURED: copy HELD but FENCED by successor %q; read through self still fails: %v", successor, err)
	t.Logf("=> the full-move hole is a ROUTING defect (union %v excludes the true holders), "+
		"NOT a release-timing defect; it opens at the successor's open-START, before any marker exists",
		memberIDs(c.routedMembersForUnit(target.Unit)))
}

// TestOverlap_drainCheck_ReleasesToRoutedAuthor is the ANTI-REGRESSION companion:
// the same position, the same successor, but the CONVERGED view (both bits seen).
// The successor is now in self's routed union, so the normal handoff must still
// release promptly. Without this, "hold on an unrouted author" could be satisfied
// by never releasing at all.
func TestOverlap_drainCheck_ReleasesToRoutedAuthor(t *testing.T) {
	const units, rf = 16, 2
	backing := sharedfactory.NewBacking()
	c := uaStaleViewCluster(t, backing, units, rf)
	target, successor := uaStrandedPosition(t, c, units, rf)

	// CONVERGE the view: the leaver's Draining bit arrives.
	c.draining = map[string]struct{}{uaLeaver: {}}
	if !containsMember(c.routedMembersForUnit(target.Unit), successor) {
		t.Fatalf("precondition: under the CONVERGED view the successor %q must be routed (set %v)",
			successor, memberIDs(c.routedMembersForUnit(target.Unit)))
	}

	uaMountAndDrain(t, c, backing, target)
	uaSuccessorMarks(t, backing, target, successor)

	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("DRAIN HANG: %s should have released to routed successor %q (routed set %v)",
			target, successor, memberIDs(c.routedMembersForUnit(target.Unit)))
	}
}

// TestOverlap_drainCheck_ConvergenceReleasesHeldPosition pins that the hold is
// TRANSIENT, not a deadlock: the same position held under the stale view releases
// on the very next poll once the Draining bit arrives, with no new marker written.
// This is what makes the fix a latency cost bounded by gossip convergence rather
// than a permanent behavior change.
func TestOverlap_drainCheck_ConvergenceReleasesHeldPosition(t *testing.T) {
	const units, rf = 16, 2
	backing := sharedfactory.NewBacking()
	c := uaStaleViewCluster(t, backing, units, rf)
	target, successor := uaStrandedPosition(t, c, units, rf)
	uaMountAndDrain(t, c, backing, target)
	uaSuccessorMarks(t, backing, target, successor)

	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("precondition: %s should be HELD under the stale view", target)
	}

	// Gossip converges: the leaver's Draining bit arrives. Nothing else changes -
	// no new marker, no new mount.
	c.draining = map[string]struct{}{uaLeaver: {}}
	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("HOLD NOT RELEASED ON CONVERGENCE: %s stayed mounted after %q entered the routed set %v; "+
			"the hold must end the moment the view converges, not wait for the backstop",
			target, successor, memberIDs(c.routedMembersForUnit(target.Unit)))
	}
}

// TestOverlap_drainCheck_LivenessBackstopReleasesUnroutedAuthor pins the MANDATORY
// escape hatch: a view that never converges must NOT wedge the drain forever. Past
// unroutedAuthorHoldBudget the node logs loudly and releases, degrading to exactly
// the pre-fix behavior for that one position.
func TestOverlap_drainCheck_LivenessBackstopReleasesUnroutedAuthor(t *testing.T) {
	const units, rf = 16, 2
	backing := sharedfactory.NewBacking()
	c := uaStaleViewCluster(t, backing, units, rf)
	target, successor := uaStrandedPosition(t, c, units, rf)
	uaMountAndDrain(t, c, backing, target)
	uaSuccessorMarks(t, backing, target, successor)

	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("precondition: %s should be HELD on the first poll", target)
	}

	// Age the hold past its budget (the view stayed stale). Rewinding the clock
	// entry is the deterministic equivalent of waiting out the budget.
	c.unroutedAuthorHoldSince.Store(target, time.Now().Add(-unroutedAuthorHoldBudget-time.Second))

	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("LIVENESS BACKSTOP DID NOT FIRE: %s still held past %s against unrouted author %q; "+
			"an unbounded hold can wedge a graceful leave",
			target, unroutedAuthorHoldBudget, successor)
	}
}

// TestOverlap_drainCheck_LegacyAuthorlessMarkerStillReleases pins ROLLING-UPGRADE
// COMPAT: a marker written by a node that predates author attribution carries no
// author, which must read as UNKNOWN and fall back to the epoch-only rule. A
// mixed-version cluster has to make progress; a new node that HELD on every
// author-less marker would wedge against every old node in the fleet.
func TestOverlap_drainCheck_LegacyAuthorlessMarkerStillReleases(t *testing.T) {
	const units, rf = 16, 2
	backing := sharedfactory.NewBacking()
	c := uaStaleViewCluster(t, backing, units, rf)
	target, _ := uaStrandedPosition(t, c, units, rf)
	uaMountAndDrain(t, c, backing, target)

	// An OLD successor: opens and marks via the author-LESS write path.
	h := backing.Handle()
	_, epoch, err := h.OpenReplicaUnit(target, acquireBaseEpoch)
	if err != nil {
		t.Fatalf("legacy successor open: %v", err)
	}
	if err := h.WriteServingMarker(target, epoch); err != nil {
		t.Fatalf("legacy successor marker: %v", err)
	}
	if got := backing.ServingMarkerAuthor(target); got != "" {
		t.Fatalf("precondition: an author-less write must record no author, got %q", got)
	}

	c.drainCheck(target)
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("ROLLING-UPGRADE WEDGE: %s held against an author-LESS (legacy) marker; "+
			"an unknown author must fall back to the epoch-only rule", target)
	}
}

// TestOverlap_unroutedAuthorHold_ClampedToLeaveBudget pins that the hold can
// never be what fails a graceful leave. An operator may configure a leave budget
// SHORTER than the constant hold; a fixed hold would then consume the whole
// budget and turn a leave that would have completed into one that times out -
// trading an availability bug for a shutdown bug. The hold is clamped to half
// the leave budget so the backstop always fires with budget left over.
func TestOverlap_unroutedAuthorHold_ClampedToLeaveBudget(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, uaSelf, 4, 2, backing, uaMembers...)

	if got := c.unroutedAuthorHold(); got != unroutedAuthorHoldBudget {
		t.Fatalf("no leave budget configured: hold should be the constant %s, got %s",
			unroutedAuthorHoldBudget, got)
	}

	// A leave budget LONGER than twice the constant leaves the constant intact.
	c.cfg.GracefulLeaveDrainTimeout = 90 * time.Second
	if got := c.unroutedAuthorHold(); got != unroutedAuthorHoldBudget {
		t.Fatalf("generous leave budget: hold should stay the constant %s, got %s",
			unroutedAuthorHoldBudget, got)
	}

	// A leave budget SHORTER than twice the constant clamps to half of it, so the
	// backstop fires with the other half still available to observe the release.
	c.cfg.GracefulLeaveDrainTimeout = 10 * time.Second
	if got := c.unroutedAuthorHold(); got != 5*time.Second {
		t.Fatalf("tight leave budget (10s): hold should clamp to 5s, got %s - an unclamped hold "+
			"would consume the whole leave budget and time the leave out", got)
	}
}

// TestOverlap_allOwnedPositionsHandedOff_HoldsOnUnroutedAuthor pins the LEAVE-side
// half of the same rule. The completion gate matters as much as drainCheck: Close
// tears the mount down once it reports true, so a gate that ignored the author
// would destroy the last routable copy even while drainCheck was correctly holding
// it.
func TestOverlap_allOwnedPositionsHandedOff_HoldsOnUnroutedAuthor(t *testing.T) {
	const units, rf = 16, 2
	backing := sharedfactory.NewBacking()
	c := uaStaleViewCluster(t, backing, units, rf)
	target, successor := uaStrandedPosition(t, c, units, rf)
	uaMountAndDrain(t, c, backing, target)
	uaSuccessorMarks(t, backing, target, successor)

	if c.allOwnedPositionsHandedOff() {
		t.Fatalf("LEAVE GATE IGNORED THE AUTHOR: reported %s handed off to %q, which is not in the routed set %v; "+
			"Close would then tear down the last copy self's readers can reach",
			target, successor, memberIDs(c.routedMembersForUnit(target.Unit)))
	}

	// Converged view: the successor is routed, so the leave must complete.
	c.draining = map[string]struct{}{uaLeaver: {}}
	if !c.allOwnedPositionsHandedOff() {
		t.Fatalf("LEAVE HANG: %s should report handed off once %q is routed", target, successor)
	}
}
