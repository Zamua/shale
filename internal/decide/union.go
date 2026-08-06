package decide

import "slices"

// PendingSetChoice names which candidate placement is a unit's PENDING replica
// set: where the unit sits once every draining member is gone.
type PendingSetChoice int

const (
	// PendingDrainerExcluded selects the placement computed AS IF the draining
	// members were not members. It is the genuine post-transition placement,
	// which is what lets ordered removal drain the leaver onto the successors
	// that will actually hold its positions.
	PendingDrainerExcluded PendingSetChoice = iota
	// PendingFullRing selects the full-ring placement, draining members
	// included: there is no post-transition placement to predict.
	PendingFullRing
)

func (p PendingSetChoice) String() string {
	if p == PendingFullRing {
		return "full-ring (no post-transition placement)"
	}
	return "drainer-excluded"
}

// PendingReplicaSet decides which placement is PENDING for one unit. Only the
// CARDINALITY of the drainer-excluded placement decides it.
//
// An empty drainer-excluded placement means there is nothing to predict: every
// member of the ring is draining, or there was no coordinator to ask. Reverting
// to the full ring makes PENDING equal CURRENT, so the unit reads as not in
// transition and routes its stable set - the same unavailable-but-SAFE wedge
// the join-side quorum floor buys, rather than a union assembled from a
// placement that does not exist.
func PendingReplicaSet(drainerExcluded int) PendingSetChoice {
	if drainerExcluded == 0 {
		return PendingFullRing
	}
	return PendingDrainerExcluded
}

// Placements is one unit's three candidate placements as member-ID lists:
//
//   - Full: the placement over every member. A DRAINING member is here - it
//     stays a current owner, serving, until it actually leaves the ring - and
//     so is a JOINING one, which owns positions it has not mounted yet.
//   - JoinerExcluded: the placement AS IF the joining members were not members.
//   - DrainerExcluded: the placement AS IF the draining members were not
//     members.
//
// The two exclusions must be HYPOTHETICAL PLACEMENTS, not the full placement
// with members filtered out. Bounded-load consistent hashing is not
// removal-invariant, so a filtered list is a placement nobody ever mounted,
// while a re-located one is the set those members genuinely hold. Which of the
// two the caller supplied is not observable from here: this decision is only as
// exact as its inputs.
//
// A placement holds no duplicates.
type Placements struct {
	Full            []string
	JoinerExcluded  []string
	DrainerExcluded []string
}

func (p Placements) currentSet(c CurrentSetChoice) []string {
	if c == CurrentFullRing {
		return p.Full
	}
	return p.JoinerExcluded
}

func (p Placements) pendingSet(c PendingSetChoice) []string {
	if c == PendingFullRing {
		return p.Full
	}
	return p.DrainerExcluded
}

// Routing is the routing decision for one unit: which replicas an op touches,
// and the replica count its ack bar is held at.
type Routing struct {
	// Current names which placement is CURRENT: the members that own the
	// position TODAY and have it mounted.
	Current CurrentSetChoice
	// Pending names which placement is PENDING: the members that WILL own it
	// once the transition completes.
	Pending PendingSetChoice
	// StableR is the size of the CURRENT set, and the only count a write ack
	// bar is computed over.
	StableR int
	// InTransition reports CURRENT and PENDING differing as member sets.
	InTransition bool
	// Routed is the set an op fans out to.
	Routed []string
}

// Route decides which replicas an op for one unit fans out to and the replica
// count the write ack bar is held at.
//
// ROUTED IS THE UNION, AND THE UNION IS CURRENT-FIRST. Off transition, CURRENT
// and PENDING coincide and routed is that set. In transition routed is
// UNION(CURRENT, PENDING), ordered so the still-mounted current owners lead and
// a pending owner still mid-mount trails: the leaders can satisfy the ack bar
// immediately, and a read that reaches a mid-mount pending owner gets the
// transient acquiring refusal the fan-out skips.
//
// THE UNION MAY ONLY GROW THE SET, NEVER SWAP IT. Every current owner stays
// routed for the whole transition, because a current owner is by definition the
// one holding the position mounted right now - drop it and an op can be routed
// exclusively at members that are still opening, which is a refusal for every
// caller until a mount lands. A draining member is a current owner and is
// covered by exactly this: it keeps serving until it leaves the ring.
//
// STABLE R IS THE ACK BAR AND THE UNION DOES NOT RAISE IT. The bar is computed
// over len(CURRENT), never over len(routed), so widening the routed set during
// a transition adds BONUS write targets rather than a higher requirement. The
// pending owners inherit the data through the shared object-storage db the
// moment they mount, so requiring their ack while they mount would make a
// graceful transition unavailable for the whole mount window.
//
// THE FLOOR IS WHAT KEEPS THE BAR FROM FALLING (CurrentReplicaSet). CURRENT is
// the joiner-excluded placement only while that placement costs the unit no
// holder and keeps at least R of them; otherwise it reverts to the full ring.
// Without it a mass boot computes CURRENT empty, and a bar of
// requiredWriteAcks(consistency, 0) is zero: a write acking with no durable
// copy at all.
func Route(p Placements, r int) Routing {
	rt := Routing{
		Current: CurrentReplicaSet(len(p.Full), len(p.JoinerExcluded), r),
		Pending: PendingReplicaSet(len(p.DrainerExcluded)),
	}
	current := p.currentSet(rt.Current)
	pending := p.pendingSet(rt.Pending)
	rt.StableR = len(current)
	if sameMembers(current, pending) {
		rt.Routed = current
		return rt
	}
	rt.InTransition = true
	rt.Routed = unionMembers(current, pending)
	return rt
}

// sameMembers reports whether a and b hold the same IDs, order independent. A
// placement is duplicate-free, so equal lengths plus one-way containment is set
// equality.
func sameMembers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, id := range a {
		if !slices.Contains(b, id) {
			return false
		}
	}
	return true
}

// unionMembers returns the de-duplicated union of the two placements, current
// order first and then whichever pending members it does not already hold.
func unionMembers(current, pending []string) []string {
	out := make([]string, 0, len(current)+len(pending))
	out = append(out, current...)
	for _, id := range pending {
		if !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}
