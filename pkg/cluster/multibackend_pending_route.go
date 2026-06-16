// Pending-ranges routing core (v0.8 Phase 2e). This is the CORE of the
// pending-ranges graceful-membership-transition model: the current / pending /
// union replica-set computation that replicasForKey returns during a
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

package cluster

import (
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// drainingIDs returns the set of node IDs currently advertising the Draining
// bit, read from the authoritative membership snapshot (NOT the ring, which now
// carries draining members). Empty (nil) in the common steady state, so the
// transition test below short-circuits to the stable replica set with no
// allocation. A nil membership (legacy / test harness without gossip) yields no
// draining members, collapsing every key to its stable set.
func (c *Cluster) drainingIDs() map[string]struct{} {
	// Test hook: white-box tests without a membership layer inject the draining
	// set directly. In production c.draining is nil and the snapshot is
	// authoritative.
	if c.draining != nil {
		return c.draining
	}
	if c.membership == nil {
		return nil
	}
	var draining map[string]struct{}
	for _, m := range c.membership.Snapshot() {
		if m.Draining {
			if draining == nil {
				draining = make(map[string]struct{}, 2)
			}
			draining[m.ID] = struct{}{}
		}
	}
	return draining
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
// current is the ring located over ALL members (draining included); pending is
// current with draining members dropped from the chain and the chain extended
// to keep R distinct survivors. They are equal in steady state, so the common
// path returns current with no union work.
func (c *Cluster) routedReplicasForKey(key []byte) (routed []ring.Member, stableR int) {
	current := c.unitReplicas(c.genUnitForKey(key))
	stableR = len(current)

	draining := c.drainingIDs()
	if len(draining) == 0 {
		// Steady state: no transition, routed == stable set.
		return current, stableR
	}

	pending := c.pendingReplicasForKey(key, draining)
	if sameMemberSet(current, pending) {
		// A draining member exists somewhere in the cluster, but NOT in this
		// key's current replica set (or its removal does not change the set):
		// the position is not in transition. Route the stable set.
		return current, stableR
	}

	return unionMembers(current, pending), stableR
}

// pendingUnitReplicas resolves the PENDING replica set for an explicit GenUnit:
// the unit's replica set over the ring with the draining members EXCLUDED. It is
// the unit-addressed sibling of pendingReplicasForKey (which takes a key), used
// by the reconcile to enumerate this node's pending positions per unit. Same
// successor-chain semantics: locate (R + |draining|) over the full ring and drop
// the draining hits, keeping the first R survivors.
func (c *Cluster) pendingUnitReplicas(gu storageunit.GenUnit, draining map[string]struct{}) []ring.Member {
	if c.ring == nil || c.ring.Empty() {
		return c.unitReplicas(gu)
	}
	r := c.replicationFactor()
	chain := c.ring.LocateKeyN(genUnitBytes(gu), r+len(draining))
	out := make([]ring.Member, 0, r)
	for _, m := range chain {
		if _, isDraining := draining[m.ID]; isDraining {
			continue
		}
		out = append(out, m)
		if len(out) == r {
			break
		}
	}
	return out
}

// routedReplica pairs a routed union member with the ReplicaUnit it physically
// holds for the key's unit. The position index differs for a CURRENT owner (its
// index in the current set) versus a PENDING owner (its index in the pending
// set), so a position-addressed write must carry the member-specific ru. This is
// what lets a union dual-write land on the exact mounted copy a pending owner
// acquired, without the receiver re-deriving the position from its (current-set)
// ring index (where a pending owner does not appear).
type routedReplica struct {
	member ring.Member
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
	current := c.unitReplicas(gu)
	stableR = len(current)

	withUnit := func(members []ring.Member) []routedReplica {
		out := make([]routedReplica, len(members))
		for i, m := range members {
			out[i] = routedReplica{member: m, ru: storageunit.NewReplicaUnit(gu, uint8(i))}
		}
		return out
	}

	draining := c.drainingIDs()
	if len(draining) == 0 {
		return withUnit(current), stableR
	}
	pending := c.pendingUnitReplicas(gu, draining)
	if sameMemberSet(current, pending) {
		return withUnit(current), stableR
	}

	// Union, current-first, each member paired with the ru it holds: current
	// owners keep their current index; a pending-only member takes its pending
	// index (the slot it acquired).
	curRR := withUnit(current)
	out := make([]routedReplica, 0, len(curRR)+len(pending))
	out = append(out, curRR...)
	for i, m := range pending {
		if containsMember(current, m.ID) {
			continue // already added at its current index.
		}
		out = append(out, routedReplica{member: m, ru: storageunit.NewReplicaUnit(gu, uint8(i))})
	}
	return out, stableR
}

// pendingReplicasForKey resolves the PENDING replica set for a key: the
// unit's replica set over the ring with the draining members EXCLUDED. Removing
// a node from a consistent-hash ring shifts each of its keys to the next
// clockwise survivor, which is exactly the next distinct member in the
// GetClosestN successor chain - so locating (R + |draining|) members over the
// FULL ring and dropping the draining ones, keeping the first R survivors, is
// equivalent to re-locating R over a ring that never had the draining members,
// WITHOUT mutating the shared ring. (At most |draining| of the first
// R+|draining| chain entries can be draining, so R survivors always remain when
// the ring has R non-draining members.)
func (c *Cluster) pendingReplicasForKey(key []byte, draining map[string]struct{}) []ring.Member {
	return c.pendingUnitReplicas(c.genUnitForKey(key), draining)
}

// writeAckBar computes the write ack target W for a fan-out over routedN
// replicas whose STABLE replica count is stableR. Under the pending-ranges
// model a transition makes routedN > stableR (the union carries an extra
// pending owner mid-mount); the WriteOne / WriteQuorum bars stay PINNED to the
// stable R so a transition never raises the durability bar - the pending owner
// is a bonus write target, not a higher hurdle, which is what keeps writes
// available while ownership moves. WriteAll is the deliberate exception: "all"
// means every routed replica, so it widens with the union (a WriteAll caller
// has opted into all-replica durability and a mid-mount pending owner simply
// returns the acquiring-shortfall the retry wrapper waits out). In steady state
// routedN == stableR and this is identical to requiredWriteAcks(wc, stableR).
func (c *Cluster) writeAckBar(routedN, stableR int) int {
	if c.cfg.WriteConsistency == WriteAll {
		return requiredWriteAcks(WriteAll, routedN)
	}
	return requiredWriteAcks(c.cfg.WriteConsistency, stableR)
}

// sameMemberSet reports whether a and b contain the same node IDs (order
// independent). Used to detect "not in transition" (current == pending) cheaply
// for the small replica slices (R is single digit).
func sameMemberSet(a, b []ring.Member) bool {
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

// containsMember reports whether ms holds a member with the given ID.
func containsMember(ms []ring.Member, id string) bool {
	for _, m := range ms {
		if m.ID == id {
			return true
		}
	}
	return false
}

// unionMembers returns the de-duplicated union of two replica slices,
// CURRENT-first so the still-mounted current owners (which can satisfy the ack
// bar instantly) lead the fan-out and a pending owner mid-mount trails. Order is
// otherwise stable (current order, then any pending members not already in
// current).
func unionMembers(current, pending []ring.Member) []ring.Member {
	out := make([]ring.Member, 0, len(current)+len(pending))
	out = append(out, current...)
	for _, m := range pending {
		if !containsMember(out, m.ID) {
			out = append(out, m)
		}
	}
	return out
}
