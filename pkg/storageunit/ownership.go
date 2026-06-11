package storageunit

// NodeID identifies a node (compute) in the cluster. It mirrors
// ring.Member.ID: the cluster-unique node identity the ring places units
// on. Named here as its own type so the ownership derivation has no compile
// dependency on pkg/ring (the ring depends on this domain, not the other
// way around; the cluster layer wires them together).
type NodeID string

// OwnerLookup answers "which node owns this unit?" It is the dynamic
// ownership map (unit -> node), which in production is the ring computed
// over unit ids. Modeling it as a one-method interface keeps OwnedUnits pure
// and trivially fakeable in tests (no ring, no membership, just a map).
//
// OwnerOf returns the owning node and ok=true, or ("", false) when the unit
// has no owner (e.g. an empty ring, or a unit id out of range for the
// current count). A unit with no owner is simply not assigned to anyone, so
// it never appears in any node's OwnedUnits.
type OwnerLookup interface {
	OwnerOf(u UnitID) (NodeID, bool)
}

// OwnerLookupFunc adapts a plain function to OwnerLookup. Lets callers and
// tests supply ownership as a closure: OwnedUnits(self, c,
// OwnerLookupFunc(func(u UnitID) (NodeID, bool){ ... })).
type OwnerLookupFunc func(u UnitID) (NodeID, bool)

// OwnerOf calls f.
func (f OwnerLookupFunc) OwnerOf(u UnitID) (NodeID, bool) { return f(u) }

// OwnedUnits returns the set of units the node self owns, derived purely
// from the unit count c and the ownership lookup. It enumerates 0..N-1 and
// keeps every unit whose owner is self. The result is ascending and freshly
// allocated; an empty (non-nil) slice means self owns no units under c.
//
// This is the function a node uses to answer "which slatedb instances should
// I have mounted right now?" The anti-entropy reconcile compares this set
// against what is actually mounted: own-but-not-mounted -> acquire the
// lease (OpenUnit); mounted-but-not-owned -> release it (CloseUnit). Keeping
// it pure (no ring, no factory) is what makes that reconcile logic testable
// without standing up storage or membership.
func OwnedUnits(self NodeID, c UnitCount, owners OwnerLookup) []UnitID {
	out := make([]UnitID, 0)
	if c.IsZero() || owners == nil {
		return out
	}
	for i := uint32(0); i < c.n; i++ {
		u := UnitID(i)
		if owner, ok := owners.OwnerOf(u); ok && owner == self {
			out = append(out, u)
		}
	}
	return out
}

// OwnsUnit reports whether self owns unit u under the given lookup. A thin
// single-unit query for the routing fast path (a node checking "is this my
// unit?" before deciding to serve locally or forward) without materializing
// the whole OwnedUnits set.
func OwnsUnit(self NodeID, u UnitID, owners OwnerLookup) bool {
	if owners == nil {
		return false
	}
	owner, ok := owners.OwnerOf(u)
	return ok && owner == self
}
