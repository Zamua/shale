package storageunit

import "fmt"

// ReplicaUnit is the per-replica DURABLE STORAGE IDENTITY of a unit at R>1:
// the pair (GenUnit, replica-position). It is the v0.8 Phase 2b analogue of
// GenUnit for replicated multi-backend mode.
//
// At R=1 a unit gu has exactly ONE durable database, keyed by GenUnit alone:
// the lease moves between nodes but the bytes stay put (a handoff is
// copy-free). At R>1 the R replicas of gu are R INDEPENDENT durable databases
// that must each persist on their own, so the durable identity must carry the
// replica POSITION as well: replica 0 of gu and replica 1 of gu are distinct
// stores that survive each other. Losing the node holding replica 1 leaves
// replica 0 a complete independent copy. Keying durable storage by bare
// GenUnit at R>1 would alias the R replicas onto ONE store and defeat the
// whole point of replication. See docs/SPEC.md "v0.8 Phase 2b".
//
// Replica is the position in the unit's ordered replica set (primary = 0,
// then the ring successors), uint8 because R is small (a handful of copies).
type ReplicaUnit struct {
	Unit    GenUnit
	Replica uint8
}

// NewReplicaUnit builds a ReplicaUnit. A trivial constructor kept for
// call-site readability at the mount seams and so a future invariant has one
// place to live.
func NewReplicaUnit(gu GenUnit, replica uint8) ReplicaUnit {
	return ReplicaUnit{Unit: gu, Replica: replica}
}

// String renders the identity as "<genunit>/r<replica>" for logs and test
// failures, e.g. "g1/u5/r0".
func (ru ReplicaUnit) String() string {
	return fmt.Sprintf("%s/r%d", ru.Unit, ru.Replica)
}

// ReplicaLookup answers "which nodes hold this unit, in replica order?" It is
// the R>1 analogue of OwnerLookup: where OwnerLookup yields a unit's single
// owner, ReplicaLookup yields the ordered replica set (primary first, then
// the ring successors). In production it is the ring computed over the
// generation-qualified unit id via LocateKeyN(., R); modeling it as a
// one-method interface keeps OwnedReplicaUnits pure and trivially fakeable in
// tests (no ring, just a map).
//
// ReplicasOf returns the ordered replica nodes for unit u, or nil/empty when
// the unit has no owners (an empty ring, or a unit id out of range for the
// current count). A unit with no replicas appears in no node's owned set.
type ReplicaLookup interface {
	ReplicasOf(u UnitID) []NodeID
}

// ReplicaLookupFunc adapts a plain function to ReplicaLookup. Lets callers
// and tests supply the replica set as a closure.
type ReplicaLookupFunc func(u UnitID) []NodeID

// ReplicasOf calls f.
func (f ReplicaLookupFunc) ReplicasOf(u UnitID) []NodeID { return f(u) }

// OwnedReplicaUnits returns, for the node self, every unit self replicates
// together with the POSITION self holds in that unit's replica set. It is the
// R>1 analogue of OwnedUnits: it enumerates 0..N-1 and keeps every unit whose
// replica set contains self, pairing the unit with self's index in that set.
//
// The result is ascending by UnitID (the enumeration order) and freshly
// allocated; an empty (non-nil) slice means self replicates no units under c.
// A node mounts each returned (unit, replica-position) as an INDEPENDENT
// durable database (ReplicaUnit), which is what makes the R replicas survive
// each other. Keeping it pure (no ring, no factory) is what makes the mount
// derivation testable without standing up storage or membership.
//
// Each returned UnitID appears AT MOST ONCE per node: a replica set is a set
// of DISTINCT nodes, so self occupies one position. The first matching index
// is used.
func OwnedReplicaUnits(self NodeID, c UnitCount, replicas ReplicaLookup) []OwnedReplica {
	out := make([]OwnedReplica, 0)
	if c.IsZero() || replicas == nil {
		return out
	}
	for i := uint32(0); i < c.n; i++ {
		u := UnitID(i)
		set := replicas.ReplicasOf(u)
		for pos, node := range set {
			if node == self {
				out = append(out, OwnedReplica{Unit: u, Replica: uint8(pos)})
				break
			}
		}
	}
	return out
}

// OwnedReplica pairs a unit the node holds with the replica POSITION it holds
// it at. It is the element type of OwnedReplicaUnits: the cluster turns each
// into a generation-qualified ReplicaUnit at mount time (the generation comes
// from the live routing state, which the pure domain does not know about).
type OwnedReplica struct {
	Unit    UnitID
	Replica uint8
}
