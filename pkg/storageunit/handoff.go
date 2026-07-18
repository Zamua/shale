package storageunit

import "fmt"

// HandoffPhase is the phase of an IN-FLIGHT ownership transition of one
// ReplicaUnit (v0.8 Phase 2e, Option B overlap handoff). It is the pure core
// of the per-ReplicaUnit ownership-transition state machine that replaces the
// position-change branch of the cluster's reconcile. See
// docs/design/overlap-handoff.md and docs/SPEC.md "v0.8 Phase 2e".
//
// There are FOUR in-flight phases, split across the two nodes of a move:
//
//   - on the GAINER (the node taking over a position): PhaseAcquiring ->
//     PhaseReady. While Acquiring the new (pending) owner is still mounting; the
//     union covers the position via the still-mounted current owner. At Ready its
//     mount entry is inserted (the mount flip) and it serves locally + writes its
//     serving marker.
//   - on the LOSER (the node giving up a position): PhaseDraining ->
//     PhaseReleasing. While Draining the old owner stays a routed current owner
//     and keeps serving (dual-written via the union); at Releasing it tears down
//     its mount exactly once, after the successor's marker gates the release.
//
// The two STEADY-STATE poles (Owned and Absent) are NOT enum values: they are
// represented by mountMap presence (Owned = mounted, no phase entry) and
// mountMap absence (Absent = not mounted, no phase entry). Only in-flight
// positions carry a HandoffPhase. Keeping the steady states out of the enum is
// deliberate: an absent phase entry means "not transitioning," which is the
// common case, so it must not need an explicit value to encode.
type HandoffPhase uint8

const (
	// PhaseAcquiring is the GAINER (pending owner) mid-mount: the union already
	// routes this position to this node but OpenReplicaUnit has not completed, so
	// a routed op returns the transient acquiring error and the union covers the
	// position via the still-mounted current owner. The enum deliberately starts
	// at 1, so the zero value is not a valid phase and a zero-initialized
	// HandoffState is never mistaken for a real Acquiring entry (NextOnDrain
	// relies on this: it reads Phase == 0 as steady-state Owned).
	PhaseAcquiring HandoffPhase = iota + 1

	// PhaseReady is the GAINER after the mount flip: mountMap[ru] is inserted and
	// the node serves the position locally. It then writes its durable serving
	// marker exactly once, which gates the leaver's release.
	PhaseReady

	// PhaseDraining is the LOSER still serving the position it no longer
	// desires, awaiting the new owner's readiness before releasing.
	PhaseDraining

	// PhaseReleasing is the LOSER tearing down its mount (compare-and-delete
	// mountMap[ru] + CloseReplicaUnit) exactly once after the new owner is
	// proven Ready.
	PhaseReleasing
)

// String renders the phase for logs and test failures.
func (p HandoffPhase) String() string {
	switch p {
	case PhaseAcquiring:
		return "Acquiring"
	case PhaseReady:
		return "Ready"
	case PhaseDraining:
		return "Draining"
	case PhaseReleasing:
		return "Releasing"
	default:
		return fmt.Sprintf("HandoffPhase(%d)", uint8(p))
	}
}

// IsGainer reports whether p is a phase the GAINER side occupies
// (Acquiring or Ready).
func (p HandoffPhase) IsGainer() bool {
	return p == PhaseAcquiring || p == PhaseReady
}

// IsLoser reports whether p is a phase the LOSER side occupies
// (Draining or Releasing).
func (p HandoffPhase) IsLoser() bool {
	return p == PhaseDraining || p == PhaseReleasing
}

// HandoffState is the pure value object the cluster keeps per IN-FLIGHT
// ReplicaUnit (the cluster's handoffPhase map is map[ReplicaUnit]HandoffState).
// It is I/O-free: all transition logic over it is pure functions, trivially
// table-testable with no ring and no factory.
//
// The ReplicaUnit is the KEY of the cluster's map, NOT a field here: a node can
// be Draining position p AND Acquiring position q of the SAME unit at once (a
// ring change that shuffles this node's index within a unit's replica set), so
// the two records are distinct map entries under distinct ReplicaUnit keys.
// Folding the position into the key (rather than carrying it in the value) is
// what lets both coexist.
//
// Fields:
//
//   - Phase: which of the four in-flight phases this position is in.
//   - OpenEpoch: the epoch the GAINER opened the position at (the fence epoch
//     E). Meaningful from PhaseReady onward (it is what the serving marker
//     carries, and on the loser side it is the floor a successor's marker must
//     exceed before the release gate opens - see Releasable).
//     Zero while Acquiring (the open has not completed).
//
// Under the pending-ranges model (v0.8 Phase 2e) there is NO predecessor field:
// the routing layer's union dual-writes a position DIRECTLY to both its current
// (draining-inclusive) and pending (draining-exclusive) owners, so a pending
// owner mid-mount has no forwarding target to remember - a routed op arriving on
// it simply returns the transient acquiring error and the union covers the
// position via the still-mounted current owner.
type HandoffState struct {
	Phase     HandoffPhase
	OpenEpoch Epoch
}

// errIllegalEdge is the shape every illegal-transition error takes. Kept as a
// constructor so the messages are uniform and the tests can match on them.
func errIllegalEdge(edge string, from HandoffPhase) error {
	return fmt.Errorf("storageunit: illegal handoff edge %s from phase %s", edge, from)
}

// NextOnReady advances the GAINER side when its OpenReplicaUnit completes and
// it inserts mountMap[ru] (the mount flip). The only legal predecessor phase is
// PhaseAcquiring; the result is PhaseReady carrying the open epoch E. Calling it
// from any other phase is an illegal edge (the loser side never goes Ready, and
// a position cannot become Ready twice).
func NextOnReady(from HandoffState, openEpoch Epoch) (HandoffState, error) {
	if from.Phase != PhaseAcquiring {
		return from, errIllegalEdge("OnReady (Acquiring->Ready)", from.Phase)
	}
	return HandoffState{
		Phase:     PhaseReady,
		OpenEpoch: openEpoch,
	}, nil
}

// NextOnDrain advances the LOSER side when reconcile determines this node no
// longer desires the position but a new owner is taking it over (so the
// position must NOT be eagerly released - it is drained until the new owner is
// Ready). The only legal predecessor phase is PhaseOwned, which in this pure
// model is represented by the ABSENCE of a phase entry; callers therefore pass
// the zero HandoffState (Phase == 0, the invalid sentinel) to mean "currently
// Owned, no in-flight phase." Driving a Drain from any already-in-flight phase
// is illegal (a position is put into Draining exactly once, straight from
// steady-state Owned).
func NextOnDrain(from HandoffState) (HandoffState, error) {
	if from.Phase != 0 {
		return from, errIllegalEdge("OnDrain (Owned->Draining)", from.Phase)
	}
	return HandoffState{Phase: PhaseDraining}, nil
}

// NextOnRelease advances the LOSER side from Draining to Releasing once the new
// owner is proven Ready (the release decision itself is the controller's, gated
// by Releasable + the serving-marker read; this only encodes the legal phase
// edge). The only legal predecessor phase is PhaseDraining; releasing from any
// other phase is illegal (the gainer side never releases, and a position
// releases exactly once - a second Releasing edge is rejected here, though the
// controller's mountMap compare-and-delete is the real exactly-once guard).
func NextOnRelease(from HandoffState) (HandoffState, error) {
	if from.Phase != PhaseDraining {
		return from, errIllegalEdge("OnRelease (Draining->Releasing)", from.Phase)
	}
	return HandoffState{Phase: PhaseReleasing, OpenEpoch: from.OpenEpoch}, nil
}

// Releasable is the release predicate the controller consults: a position may
// proceed toward Releasing only when it is in PhaseDraining AND a positive
// readiness has been established (ready == true).
//
// ready is the SERVING-MARKER gate. The controller (the cluster's drainCheck)
// computes it by reading the position's durable serving marker and requiring
// the marker's epoch to be STRICTLY ABOVE this node's own open epoch
// (markerEpoch > s.OpenEpoch). A marker is written only on mount-complete, so
// its presence proves a successor is actually SERVING, not merely that it
// fenced. The compare is strict so a draining node cannot read back its OWN
// gain-marker (written at exactly its open epoch) and release while the real
// successor is still mid-mount; a genuine successor opens at durable+1 and
// therefore marks strictly higher.
//
// The bare durable writer-epoch is NOT the gate and must never be used as one:
// the epoch bumps at open-START, before the mount, so a successor that fences
// then crashes mid-mount advances it without ever serving. Gating on the epoch
// alone would release the last serving copy to a node that never came up. That
// is why ready comes from the marker and why ready == false always keeps the
// loser Draining and serving.
func Releasable(s HandoffState, ready bool) bool {
	return s.Phase == PhaseDraining && ready
}
