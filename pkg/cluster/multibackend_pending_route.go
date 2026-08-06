// Pending-ranges routing core (v0.8 Phase 2e). This is the CORE of the
// pending-ranges graceful-membership-transition model: the current / pending /
// union replica-set computation that routedReplicasForKey returns during a
// transition, plus the stable-R ack bar the dual-write fan-out holds.
//
// THE MODEL (see docs/SPEC.md "v0.8 Phase 2e" + docs/design/overlap-handoff.md).
// A node that is LEAVING gossips a Draining bit but STAYS a current owner in the
// ring (the ring is NOT pruned of draining members - that exclusion is REVERSED;
// see reconcileRingFromMembership). For a key's unit:
//
//   - CURRENT  = unitReplicas over the ring INCLUDING draining members.
//     The nodes that own the position TODAY and have it mounted (the leaver is
//     here).
//   - PENDING  = unitReplicas over the ring EXCLUDING draining members.
//     The nodes that WILL own the position once the leaver is gone (the
//     successor taking the leaver's exact position is here).
//   - A position is IN TRANSITION when CURRENT != PENDING (a leave: a current
//     member is Draining; a join is detected elsewhere via the serving marker).
//   - ROUTED   = the stable set when not in transition (CURRENT == PENDING), the
//     UNION(CURRENT, PENDING) when in transition (at R=2, up to 3 nodes).
//
// The fan-out DUAL-WRITES every routed union member, but the ack bar W is held
// at the STABLE R quorum (requiredWriteAcks over len(CURRENT), NOT over the
// union size) - the extra pending replica is a BONUS write target, not a higher
// bar. WriteAll is the one setting that widens with the union (All means every
// routed replica). Reads fan out across the union; any union member that
// physically has the position mounted serves it, a mid-mount pending owner
// returns the transient acquiring error the fan-out skips.
//
// THIS FILE IS THE ROUTING CONTRACT ONLY. The background acquire (pending owners
// mounting + writing serving markers), the ordered-removal drainCheck, and the
// DrainForLeave drive live in the controller files; this file just answers
// "which replicas does an op for this key touch, and what is the ack bar."
//
// AND IT IS THE SHELL, NOT THE DECISION. Everything above turns on the three
// placements alone - the full one, the joiner-excluded one, the drainer-excluded
// one - so it is decide.Route, exercised as values over every topology rather
// than sampled through a live cluster. What is left here is coordinator work:
// asking for those placements, and translating IDs back into nodes.

package cluster

import (
	"github.com/Zamua/shale/internal/decide"
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/storageunit"
)

// drainingIDs returns the set of node IDs currently advertising the Draining
// bit, read from the authoritative membership snapshot (NOT the ring, which now
// carries draining members). Empty (nil) in the common steady state, so the
// transition test below short-circuits to the stable replica set with no
// allocation. A nil membership (legacy / test harness without gossip) yields no
// draining members, collapsing every key to its stable set.
func (c *Cluster) drainingIDs() map[storageunit.NodeID]struct{} {
	// Test hook: white-box tests without a coordinator inject the draining set
	// directly. In production c.draining is nil and the view is authoritative.
	if c.draining != nil {
		return c.draining
	}
	if c.coord == nil {
		return nil
	}
	_, draining := c.coord.TransitionSets()
	return draining
}

// joiningIDs returns the set of node IDs currently advertising the Joining bit,
// read from the authoritative membership snapshot. It is the entry-side mirror of
// drainingIDs: empty (nil) in the common steady state, so the current-set
// computation short-circuits to the full ring with no allocation. A nil
// membership (legacy / test harness) yields no joining members.
func (c *Cluster) joiningIDs() map[storageunit.NodeID]struct{} {
	// Test hook: white-box tests without a coordinator inject the joining set
	// directly. In production c.joining is nil and the view is authoritative.
	if c.joining != nil {
		return c.joining
	}
	if c.coord == nil {
		return nil
	}
	joining, _ := c.coord.TransitionSets()
	return joining
}

// transitionSets returns the joining and draining ID sets. The hot routing
// path needs BOTH per op, so production defers straight to the coordinator's
// TransitionSets - ONE membership scan, nil maps in steady state, none of
// View's snapshot construction (see the port contract). It preserves the
// individual methods' white-box test-hook semantics exactly: when either hook
// is injected (test path), it defers to them.
func (c *Cluster) transitionSets() (joining, draining map[storageunit.NodeID]struct{}) {
	if c.joining != nil || c.draining != nil {
		return c.joiningIDs(), c.drainingIDs()
	}
	if c.coord == nil {
		return nil, nil
	}
	return c.coord.TransitionSets()
}

// currentUnitReplicas resolves the CURRENT replica set for gu: the ring EXCLUDING
// the joining members, with a QUORUM FLOOR. It is the unified split's current
// half (the pending half is pendingUnitReplicas over the draining set). Draining
// members are NOT excluded here (they stay current owners); only joining
// (warming) members are, so the still-mounted DISPLACED owner stays in current
// and the warming newcomer is dropped from the ack-bar set.
//
// THE QUORUM FLOOR: excluding joiners is safe only while >= R non-joining holders
// remain. If dropping the joiners would leave fewer than R members in current
// (the MASS-BOOT case: 2+ of a unit's replicas are freshly-booted at once), fall
// back to the FULL-ring set (joiners included). That reverts the unit to the
// pre-Joining-bit behavior: unavailable-but-SAFE (a routed op to a warming
// replica returns the mid-acquire transient, so the write WEDGES) with
// stableR = len(current) >= R, so requiredWriteAcks never drops below the normal
// bar and a write can NEVER ack below R durable copies. Without the floor a mass
// boot would compute current empty -> stableR 0 -> requiredWriteAcks(All,0)=0 ->
// a write acking with ZERO durable applies (a lost acked write). The leave path
// never needs the floor (a draining node stays a stable MOUNTED current owner, so
// stableR == R by construction). The floor itself is decide.CurrentReplicaSet.
func (c *Cluster) currentUnitReplicas(gu storageunit.GenUnit, joining map[storageunit.NodeID]struct{}) []coord.Node {
	if len(joining) == 0 {
		return c.unitReplicas(gu)
	}
	full := c.unitReplicas(gu)
	// Ask the coordinator where the unit would sit if the joining members were
	// NOT members. That is a genuinely different question from "locate over
	// everyone, then drop the joiners": bounded-load consistent hashing is not
	// removal-invariant, so the filtered answer is a placement nobody ever
	// mounted. Only the hypothetical placement reconstructs the set the
	// non-joining members actually hold.
	excl := c.locate(gu, c.replicationFactor(), coord.Placement{Exclude: joining})
	// An empty exclusion - every member joining, or no coordinator at all -
	// floors like any other shortfall.
	if decide.CurrentReplicaSet(len(full), len(excl), c.replicationFactor()) == decide.CurrentFullRing {
		return full
	}
	return excl
}

// unitPlacements is one unit's three candidate placements as coordinator nodes,
// alongside the bare-ID projection decide.Route is stated in. The two views are
// built together so the "an empty exclusion is the full placement" rule has one
// site rather than one per view.
type unitPlacements struct {
	full            []coord.Node
	joinerExcluded  []coord.Node
	drainerExcluded []coord.Node
	ids             decide.Placements
}

// placementsForUnit asks the coordinator where gu sits over every member, and
// where it would sit if the joining / draining members were NOT members.
//
// The exclusions are separate LOCATE calls, not the full placement filtered:
// bounded-load consistent hashing is not removal-invariant, so a filtered list
// is a placement nobody ever mounted while a re-located one is the set those
// members genuinely hold. An empty exclusion set skips the call - there is
// nothing to exclude, so the answer is the full placement.
func (c *Cluster) placementsForUnit(gu storageunit.GenUnit, joining, draining map[storageunit.NodeID]struct{}) unitPlacements {
	p := unitPlacements{full: c.unitReplicas(gu)}
	p.joinerExcluded, p.drainerExcluded = p.full, p.full
	p.ids.Full = nodeIDs(p.full)
	p.ids.JoinerExcluded, p.ids.DrainerExcluded = p.ids.Full, p.ids.Full
	if len(joining) > 0 {
		p.joinerExcluded = c.locate(gu, c.replicationFactor(), coord.Placement{Exclude: joining})
		p.ids.JoinerExcluded = nodeIDs(p.joinerExcluded)
	}
	if len(draining) > 0 {
		p.drainerExcluded = c.locate(gu, c.replicationFactor(), coord.Placement{Exclude: draining})
		p.ids.DrainerExcluded = nodeIDs(p.drainerExcluded)
	}
	return p
}

// current resolves the decision's CURRENT choice back to coordinator nodes.
func (p unitPlacements) current(rt decide.Routing) []coord.Node {
	if rt.Current == decide.CurrentFullRing {
		return p.full
	}
	return p.joinerExcluded
}

// pending resolves the decision's PENDING choice back to coordinator nodes.
func (p unitPlacements) pending(rt decide.Routing) []coord.Node {
	if rt.Pending == decide.PendingFullRing {
		return p.full
	}
	return p.drainerExcluded
}

// routedReplicasForKey resolves a key to the replica set an op fans out to,
// AND reports the STABLE replica count the ack bar is held at. It is the
// pending-ranges core consulted by every replicated fan-out (put / get / delete
// / read-repair).
//
//   - routed  = the set the op contacts: the stable set when not in transition,
//     UNION(current, pending) when in transition.
//   - stableR = len(current) = the replica count the write ack bar is computed
//     over (requiredWriteAcks(WriteConsistency, stableR)), so a transition does
//     NOT raise the bar even though len(routed) > stableR.
//
// This is the SHELL: it asks the coordinator for the three placements, hands
// them to decide.Route, and translates the answer back into coordinator nodes.
// The decision itself - which placement is current, which is pending, whether
// the unit is in transition, the union and its order - is decide.Route.
func (c *Cluster) routedReplicasForKey(key []byte) (routed []coord.Node, stableR int) {
	gu := c.genUnitForKey(key)
	joining, draining := c.transitionSets()
	p := c.placementsForUnit(gu, joining, draining)
	rt := decide.Route(p.ids, c.replicationFactor())
	if !rt.InTransition {
		return p.current(rt), rt.StableR
	}
	return membersByID(rt.Routed, p.current(rt), p.pending(rt)), rt.StableR
}

// pendingUnitReplicas resolves the PENDING replica set for an explicit GenUnit:
// the unit's placement AS IF the draining members were not members (the same
// hypothetical-placement question currentUnitReplicas asks for the joining
// exclusion). It is the unit-addressed sibling of pendingReplicasForKey (which
// takes a key), used by the reconcile to enumerate this node's pending
// positions per unit.
//
// EXACTNESS IS REQUIRED (docs/SPEC.md "PENDING replica set"): pending is the
// protocol's prediction of the post-leave placement - ordered removal drains
// the leaver against pending's successors and holds displaced owners against
// pending membership. An earlier implementation used the successor-chain drop
// trick (locate R+|draining| over everyone, drop the draining ids) on the
// theory that the future set did not need to be exact; but bounded-load
// consistent hashing is not removal-invariant, so the approximation can
// DIVERGE from the genuine post-leave placement. A unit whose approximated
// pending is disjoint from its true post-leave placement (a FULL MOVE) then
// drains onto the WRONG successors: at the leaver's exit the true owners hold
// nothing, every physical holder is un-routed, and reads fail for the whole
// post-exit acquire window (the hole pinned by
// TestLeaveJoinOverlap_FullMoveUnit_ReadTransparent). Asking the coordinator
// for the HYPOTHETICAL placement (coord.Placement.Exclude) rather than
// filtering the real one makes pending == the post-transition placement by
// construction, in both transition directions.
func (c *Cluster) pendingUnitReplicas(gu storageunit.GenUnit, draining map[storageunit.NodeID]struct{}) []coord.Node {
	if len(draining) == 0 {
		return c.unitReplicas(gu)
	}
	pend := c.locate(gu, c.replicationFactor(), coord.Placement{Exclude: draining})
	// Every member draining (or no coordinator) leaves nothing to predict; the
	// fallback to the full set is decide.PendingReplicaSet.
	if decide.PendingReplicaSet(len(pend)) == decide.PendingFullRing {
		return c.unitReplicas(gu)
	}
	return pend
}

// routedReplica pairs a routed union member with the ReplicaUnit it physically
// holds for the key's unit. The position index differs for a CURRENT owner (its
// index in the current set) versus a PENDING owner (its index in the pending
// set), so a position-addressed write must carry the member-specific ru. This is
// what lets a union dual-write land on the exact mounted copy a pending owner
// acquired, without the receiver re-deriving the position from its (current-set)
// ring index (where a pending owner does not appear).
type routedReplica struct {
	member coord.Node
	ru     storageunit.ReplicaUnit
}

// routedReplicasWithUnit resolves a key to its ROUTED union members, each paired
// with the ReplicaUnit it holds, AND the stable replica count for the ack bar. A
// current owner is paired with (gu, currentIndex); a pending-only owner (a node
// in the union solely because the draining-excluded split adds it) is paired with
// (gu, pendingIndex) - exactly the position the reconcile's pending-acquire
// mounted. In steady state every member is a current owner and pending == current.
func (c *Cluster) routedReplicasWithUnit(key []byte) (routed []routedReplica, stableR int) {
	gu := c.genUnitForKey(key)
	joining, draining := c.transitionSets()
	p := c.placementsForUnit(gu, joining, draining)
	// Same decision as routedReplicasForKey, so the two fan-out resolvers cannot
	// disagree about which set is current or whether the unit is in transition.
	// Only the DEDUP differs: this one keys on (member, position) pairs.
	rt := decide.Route(p.ids, c.replicationFactor())
	stableR = rt.StableR
	current := p.current(rt)

	withUnit := func(members []coord.Node) []routedReplica {
		out := make([]routedReplica, len(members))
		for i, m := range members {
			out[i] = routedReplica{member: m, ru: storageunit.NewReplicaUnit(gu, uint8(i))}
		}
		return out
	}

	if !rt.InTransition {
		return withUnit(current), stableR
	}
	pending := p.pending(rt)

	// Union over (member, POSITION) PAIRS - current-first, then every pending
	// pair not already present. A leave OR a join shuffles replica indices: a
	// survivor that moves from replica-1 (its CURRENT slot, which it keeps serving
	// until drain) to replica-0 (its PENDING slot, which it acquires + fences the
	// prior holder off) appears in BOTH sets at DIFFERENT indices, so it is routed to
	// BOTH (gu,1) and (gu,0). Deduping by member ID alone (keeping only the current
	// index) was the during-transition write-availability gap: the survivor was never
	// routed to the position it acquired, so within the fence cascade a write could
	// reach only one live db and miss W. Pairing it at both positions keeps a second
	// live copy reachable throughout. Dedup is by the (member, ru) pair so the steady
	// state (pending == current) is unchanged. (The join direction gets the mirror
	// treatment because current = ring-excluding-joining, pending = full ring, so the
	// displaced owner and the newcomer land at their respective indices.)
	curRR := withUnit(current)
	penRR := withUnit(pending)
	out := make([]routedReplica, 0, len(curRR)+len(penRR))
	out = append(out, curRR...)
	for _, rr := range penRR {
		if containsRoutedReplica(out, rr) {
			continue // same (member, position) already routed.
		}
		out = append(out, rr)
	}
	return out, stableR
}

// writeAckBar computes the write ack target W. It is ALWAYS the configured
// consistency over the STABLE replica count (requiredWriteAcks(wc, stableR)): a
// topology transition that widens the routed set to the union of current +
// pending owners NEVER raises the bar. The pending owners are BONUS dual-write
// targets - they inherit the data through the shared object-storage db the
// moment they mount, so requiring their ack WHILE THEY ARE MID-MOUNT would make a
// graceful leave unavailable for the whole mount window (a write would block on
// the acquiring-shortfall until the slow mount finishes, then time out).
//
// This includes WriteAll: "all" means all STABLE replicas, NOT all transient
// union members. That is a deliberate shale choice and it differs from Cassandra,
// whose CL=ALL raises blockFor to include pending replicas and is correspondingly
// unavailable during a topology change; shale keeps scale-down available because
// the stable replicas (the leaver, still serving, plus its surviving co-replica)
// can always satisfy the bar while the successors mount in the background.
func (c *Cluster) writeAckBar(stableR int) int {
	return requiredWriteAcks(c.cfg.WriteConsistency, stableR)
}

// sameMemberSet reports whether a and b contain the same node IDs (order
// independent). Used to detect "not in transition" (current == pending) cheaply
// for the small replica slices (R is single digit).
func sameMemberSet(a, b []coord.Node) bool {
	if len(a) != len(b) {
		return false
	}
	for _, m := range a {
		if !containsMember(b, m.ID) {
			return false
		}
	}
	return true
}

// containsRoutedReplica reports whether rs already holds the same (member,
// position) pair as want. Used to dedup the routed union so a member that owns
// the SAME position in both the current and pending sets is routed once, while a
// member that owns DIFFERENT positions across the sets (an index-shuffling
// survivor) is routed to each.
func containsRoutedReplica(rs []routedReplica, want routedReplica) bool {
	for _, r := range rs {
		if r.member.ID == want.member.ID && r.ru == want.ru {
			return true
		}
	}
	return false
}

// containsMember reports whether ms holds a member with the given ID.
func containsMember(ms []coord.Node, id storageunit.NodeID) bool {
	for _, m := range ms {
		if m.ID == id {
			return true
		}
	}
	return false
}

// nodeIDs projects a placement onto the bare IDs the pure routing decision is
// stated in.
func nodeIDs(ms []coord.Node) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = string(m.ID)
	}
	return out
}

// membersByID resolves a decision's routed ID set back to coordinator nodes.
// The decision works in bare IDs so it can stay free of the coordinator's node
// type; the addresses live only here. Every routed ID comes from the current or
// the pending placement by construction (the routed set is their union), so a
// and b together cover the whole answer.
func membersByID(ids []string, a, b []coord.Node) []coord.Node {
	out := make([]coord.Node, 0, len(ids))
	for _, id := range ids {
		if m, ok := findMember(a, id); ok {
			out = append(out, m)
			continue
		}
		if m, ok := findMember(b, id); ok {
			out = append(out, m)
		}
	}
	return out
}

func findMember(ms []coord.Node, id string) (coord.Node, bool) {
	for _, m := range ms {
		if string(m.ID) == id {
			return m, true
		}
	}
	return coord.Node{}, false
}
