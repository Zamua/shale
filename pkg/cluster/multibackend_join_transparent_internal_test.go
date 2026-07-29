package cluster

// White-box tests for the JOIN direction of the v0.8 Phase 2e unified split: the
// CURRENT set = ring EXCLUDING joining members (quorum-floored), the entry-side
// mirror of PENDING = ring EXCLUDING draining members. These pin the two crux
// properties directly on the routing helpers, with no membership / gRPC:
//
//   1. a SINGLE joining member is excluded from CURRENT (so the still-mounted
//      displaced owner stays the stable holder and the newcomer rides the
//      pending-owner acquire path) - the transparency mechanism; and
//   2. THE QUORUM FLOOR: when excluding joiners would drop CURRENT below R (the
//      mass-boot case where 2+ of a unit's replicas warm at once), CURRENT falls
//      back to the full ring, so stableR = len(CURRENT) >= R ALWAYS and
//      requiredWriteAcks never drops below the normal bar - a write can NEVER ack
//      below R durable copies (the data-safety wall the naive mirror would breach).
//
// The wired cross-node availability + the mass-boot loss oracle are covered by the
// integration gates (tests/integration/join_not_write_transparent_test.go and the
// mass-boot safety test).

import (
	"fmt"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestJoin_currentUnitReplicas_ExcludesSingleJoiner pins that a single joining
// member is dropped from CURRENT and a survivor pulled in (CURRENT != full ring),
// while a unit the joiner does not replicate is unchanged (CURRENT == full ring).
// This is the transparency mechanism: the newcomer leaves CURRENT so the still-
// mounted displaced owner remains the stable holder.
func TestJoin_currentUnitReplicas_ExcludesSingleJoiner(t *testing.T) {
	backing := sharedfactory.NewBacking()
	// 4 members so excluding ONE joiner still leaves >= R=2 non-joining holders in
	// every unit's chain (the floor never engages here).
	c := newReplicatedCluster(t, "self", 16, 2, backing, "n1", "n2", "n3", "n4")

	joinerFound, stableFound := false, false
	for _, u := range c.unitCount.IDs() {
		g := gu(0, uint32(u))
		full := c.unitReplicas(g)
		if len(full) != 2 {
			t.Fatalf("R=2 full set must have 2 members, got %d", len(full))
		}
		joiner := full[1].ID // pretend the 2nd replica just joined
		cur := c.currentUnitReplicas(g, map[storageunit.NodeID]struct{}{joiner: {}})

		// The quorum floor must NOT engage for a single joiner in a 4-member ring:
		// CURRENT always has R members.
		if len(cur) != 2 {
			t.Fatalf("unit %d: CURRENT excluding one joiner must keep R=2 members, got %d", u, len(cur))
		}
		if containsMember(cur, joiner) {
			t.Fatalf("unit %d: joining member %q must be EXCLUDED from CURRENT, got %v", u, joiner, cur)
		}
		joinerFound = true

		// A unit whose replica set does NOT include the joiner is unchanged.
		other := full[0].ID
		if !containsMember(c.unitReplicas(g), other) {
			continue
		}
		// Marking a NON-replica as joining leaves CURRENT == full ring.
		nonReplica := storageunit.NodeID("n-absent")
		curUnchanged := c.currentUnitReplicas(g, map[storageunit.NodeID]struct{}{nonReplica: {}})
		if !sameMemberSet(curUnchanged, full) {
			t.Fatalf("unit %d: excluding a non-replica joiner must leave CURRENT == full ring", u)
		}
		stableFound = true
	}
	if !joinerFound || !stableFound {
		t.Fatalf("fixture produced no joiner/stable units (joiner=%v stable=%v)", joinerFound, stableFound)
	}
}

// TestJoin_QuorumFloor_NeverShrinksCurrentBelowR is THE data-safety proof: on a
// 2-member R=2 cluster where BOTH replicas of every unit are joining at once (the
// mass-boot case), CURRENT must fall back to the FULL ring (both members), so
// stableR = R and the write ack bar stays at the normal quorum. WITHOUT the floor
// CURRENT would be empty, stableR = 0, and requiredWriteAcks(WriteAll, 0) = 0 - a
// write acking with ZERO durable applies (a lost acked write).
func TestJoin_QuorumFloor_NeverShrinksCurrentBelowR(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 16, 2, backing, "n1", "n2")
	bothJoining := map[storageunit.NodeID]struct{}{"n1": {}, "n2": {}}

	for _, u := range c.unitCount.IDs() {
		g := gu(0, uint32(u))
		full := c.unitReplicas(g)
		cur := c.currentUnitReplicas(g, bothJoining)

		// Floor engaged: CURRENT reverts to the full ring (never empty, never < R).
		if len(cur) < c.replicationFactor() {
			t.Fatalf("unit %d: QUORUM FLOOR breached - CURRENT has %d members, below R=%d "+
				"(a write would ack below R durable copies)", u, len(cur), c.replicationFactor())
		}
		if !sameMemberSet(cur, full) {
			t.Fatalf("unit %d: mass-boot floor must revert CURRENT to the full ring, got %v want %v", u, cur, full)
		}
	}
}

// TestJoin_routedWithUnit_FloorHoldsAckBar pins the floor at the ROUTING layer:
// routedReplicasWithUnit under an all-joining mass boot must return stableR = R
// (never 0), so writeAckBar (requiredWriteAcks over stableR) stays at the normal
// bar. This is the exact value the fan-out holds the write ack at, so it is the
// direct "never ack below RF durable copies" guarantee.
func TestJoin_routedWithUnit_FloorHoldsAckBar(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 16, 2, backing, "n1", "n2")
	c.cfg.WriteConsistency = WriteAll
	c.joining = map[storageunit.NodeID]struct{}{"n1": {}, "n2": {}}

	for _, u := range c.unitCount.IDs() {
		key := firstKeyForUnit(t, c, u)
		_, stableR := c.routedReplicasWithUnit(key)
		if stableR < c.replicationFactor() {
			t.Fatalf("unit %d: mass-boot stableR=%d < R=%d - the floor failed, ack bar would drop", u, stableR, c.replicationFactor())
		}
		if w := c.writeAckBar(stableR); w != c.replicationFactor() {
			t.Fatalf("unit %d: WriteAll ack bar = %d, want R=%d (a write must not ack below R durable copies)", u, w, c.replicationFactor())
		}
	}
}

// TestJoin_routedWithUnit_SingleJoinerFormsUnion pins that a single joiner makes
// routing form the current/pending UNION with stableR held at R: the union spans
// the displaced current owner (still mounted) plus the newcomer's pending slot, so
// a moving-shard write acks over the stable holders while the newcomer mounts.
func TestJoin_routedWithUnit_SingleJoinerFormsUnion(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 16, 2, backing, "n1", "n2", "n3", "n4")
	c.cfg.WriteConsistency = WriteAll

	// Find a key whose unit has n4 as a replica (n4 is the joiner).
	sawUnion := false
	for _, u := range c.unitCount.IDs() {
		g := gu(0, uint32(u))
		if !containsMember(c.unitReplicas(g), "n4") {
			continue
		}
		c.joining = map[storageunit.NodeID]struct{}{"n4": {}}
		key := firstKeyForUnit(t, c, u)
		routed, stableR := c.routedReplicasWithUnit(key)
		if stableR != 2 {
			t.Fatalf("unit %d: single-join stableR=%d, want R=2", u, stableR)
		}
		// Union spans MORE than R members (the displaced owner + the newcomer's slot).
		if len(routed) <= stableR {
			t.Fatalf("unit %d: single-join routed union size %d, want > stableR=%d", u, len(routed), stableR)
		}
		// The newcomer must be routed (it is in the union) but NOT counted in stableR.
		if !routedContainsMember(routed, "n4") {
			t.Fatalf("unit %d: newcomer n4 must be a routed (bonus) union member", u)
		}
		sawUnion = true
		break
	}
	if !sawUnion {
		t.Fatalf("fixture produced no unit with n4 as a replica")
	}
}

func routedContainsMember(rs []routedReplica, id storageunit.NodeID) bool {
	for _, r := range rs {
		if r.member.ID == id {
			return true
		}
	}
	return false
}

// firstKeyForUnit returns a key that routes to unit u under c's unit count.
func firstKeyForUnit(t *testing.T, c *Cluster, u storageunit.UnitID) []byte {
	t.Helper()
	for i := 0; i < 1_000_000; i++ {
		k := []byte(fmt.Sprintf("jk-%07d", i))
		if c.genUnitForKey(k).ID == u {
			return k
		}
	}
	t.Fatalf("no key found for unit %v", u)
	return nil
}
