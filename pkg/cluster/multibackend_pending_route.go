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
)

// drainingIDs returns the set of node IDs currently advertising the Draining
// bit, read from the authoritative membership snapshot (NOT the ring, which now
// carries draining members). Empty (nil) in the common steady state, so the
// transition test below short-circuits to the stable replica set with no
// allocation. A nil membership (legacy / test harness without gossip) yields no
// draining members, collapsing every key to its stable set.
func (c *Cluster) drainingIDs() map[string]struct{} {
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
	gu := c.genUnitForKey(key)
	if c.ring == nil || c.ring.Empty() {
		return c.unitReplicas(gu)
	}
	r := c.replicationFactor()
	// Locate enough of the chain that dropping every draining hit still leaves
	// R survivors: R + |draining|, clamped inside LocateKeyN to the ring size.
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
