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

// joiningIDs returns the set of node IDs currently advertising the Joining bit,
// read from the authoritative membership snapshot. It is the entry-side mirror of
// drainingIDs: empty (nil) in the common steady state, so the current-set
// computation short-circuits to the full ring with no allocation. A nil
// membership (legacy / test harness) yields no joining members.
func (c *Cluster) joiningIDs() map[string]struct{} {
	// Test hook: white-box tests without a membership layer inject the joining
	// set directly. In production c.joining is nil and the snapshot is
	// authoritative.
	if c.joining != nil {
		return c.joining
	}
	if c.membership == nil {
		return nil
	}
	var joining map[string]struct{}
	for _, m := range c.membership.Snapshot() {
		if m.Joining {
			if joining == nil {
				joining = make(map[string]struct{}, 2)
			}
			joining[m.ID] = struct{}{}
		}
	}
	return joining
}

// transitionSets returns the joining and draining ID sets from a SINGLE
// membership snapshot scan. The hot routing path needs BOTH per op; calling
// joiningIDs + drainingIDs separately would scan (and allocate) the snapshot
// twice on every Put/Get. It preserves the individual methods' white-box
// test-hook semantics exactly: when either hook is injected (test path), it
// defers to them; in production (no hooks) it does one combined scan. Both are
// nil in the common steady state, so the routing fast path short-circuits.
func (c *Cluster) transitionSets() (joining, draining map[string]struct{}) {
	if c.joining != nil || c.draining != nil {
		return c.joiningIDs(), c.drainingIDs()
	}
	if c.membership == nil {
		return nil, nil
	}
	for _, m := range c.membership.Snapshot() {
		if m.Joining {
			if joining == nil {
				joining = make(map[string]struct{}, 2)
			}
			joining[m.ID] = struct{}{}
		}
		if m.Draining {
			if draining == nil {
				draining = make(map[string]struct{}, 2)
			}
			draining[m.ID] = struct{}{}
		}
	}
	return joining, draining
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
// stableR == R by construction).
func (c *Cluster) currentUnitReplicas(gu storageunit.GenUnit, joining map[string]struct{}) []ring.Member {
	if len(joining) == 0 {
		return c.unitReplicas(gu)
	}
	return c.currentReplicasFromReduced(c.buildReducedRing(joining), gu)
}

// buildReducedRing returns a FRESH ring holding every current member EXCEPT the
// excluded ids. It is used to compute the CURRENT (pre-join) placement: because
// the consistent-hash library is NOT removal-invariant (adding a member reshuffles
// placement under bounded loads, so "locate R+k over the full ring then drop the
// excluded ids" does NOT reconstruct the ring as it was WITHOUT those members - it
// gives a different, wrong set), the ONLY way to recover the exact placement the
// non-excluded members mounted is to locate over a ring genuinely built from just
// them. That ring is byte-identical to the one those members used before the
// excluded (joining) member was added, so unitReplicas over it == the ACTUAL
// mounted placement. This differs from pendingUnitReplicas (which uses the drop
// trick for the DRAINING exclusion): draining exclusion feeds the PENDING/future
// set where the successor mounts from shared storage regardless of a small
// index shuffle, so exactness is not required there; the CURRENT set MUST be the
// real mounted placement or the still-mounted displaced owner is dropped from the
// write set and the join wedges. Only built when joining is non-empty (a rare,
// brief transition), so the per-op ring build is off the steady-state path.
func (c *Cluster) buildReducedRing(exclude map[string]struct{}) *ring.Ring {
	r := ring.New()
	for _, m := range c.ring.Members() {
		if _, ex := exclude[m.ID]; ex {
			continue
		}
		r.Add(m)
	}
	return r
}

// currentReplicasFromReduced resolves gu's CURRENT replica set over an
// already-built reduced ring (members minus joining), applying the QUORUM FLOOR:
// if locating over the reduced ring yields fewer than R members (or fewer than the
// full-ring set - the mass-boot case where 2+ of a unit's replicas are joining at
// once), fall back to the full-ring set. That keeps stableR = len(current) >= R,
// so requiredWriteAcks never drops below the normal bar and a write can NEVER ack
// below R durable copies. See currentUnitReplicas + buildReducedRing.
func (c *Cluster) currentReplicasFromReduced(reduced *ring.Ring, gu storageunit.GenUnit) []ring.Member {
	full := c.unitReplicas(gu)
	if reduced == nil || reduced.Empty() {
		return full
	}
	excl := reduced.LocateKeyN(genUnitBytes(gu), c.replicationFactor())
	if len(excl) < c.replicationFactor() || len(excl) < len(full) {
		// Quorum floor: not enough non-joining holders to keep current at full
		// size. Fall back to the full ring (the safe wedge for this unit).
		return full
	}
	return excl
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
	gu := c.genUnitForKey(key)
	joining, draining := c.transitionSets()

	// UNIFIED SPLIT: current = ring EXCLUDING joining (floored); pending = ring
	// EXCLUDING draining. In steady state both equal the full ring.
	current := c.currentUnitReplicas(gu, joining)
	stableR = len(current)

	if len(joining) == 0 && len(draining) == 0 {
		// Steady state: no transition, routed == stable set.
		return current, stableR
	}

	pending := c.pendingUnitReplicas(gu, draining)
	if sameMemberSet(current, pending) {
		// A joining/draining member exists somewhere in the cluster, but this
		// key's current and pending sets coincide (the member is not in this
		// unit's set, or the quorum floor reverted current to the full ring):
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
	joining, draining := c.transitionSets()

	// UNIFIED SPLIT: current = ring EXCLUDING joining (floored); pending = ring
	// EXCLUDING draining. In steady state both equal the full ring.
	current := c.currentUnitReplicas(gu, joining)
	stableR = len(current)

	withUnit := func(members []ring.Member) []routedReplica {
		out := make([]routedReplica, len(members))
		for i, m := range members {
			out[i] = routedReplica{member: m, ru: storageunit.NewReplicaUnit(gu, uint8(i))}
		}
		return out
	}

	if len(joining) == 0 && len(draining) == 0 {
		return withUnit(current), stableR
	}
	pending := c.pendingUnitReplicas(gu, draining)
	if sameMemberSet(current, pending) {
		return withUnit(current), stableR
	}

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
