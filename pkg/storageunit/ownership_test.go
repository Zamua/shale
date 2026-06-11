package storageunit_test

import (
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

// fakeOwners is a hand-built unit -> node map for testing OwnedUnits without
// a ring. A unit absent from the map has no owner (ok=false).
type fakeOwners map[storageunit.UnitID]storageunit.NodeID

func (f fakeOwners) OwnerOf(u storageunit.UnitID) (storageunit.NodeID, bool) {
	owner, ok := f[u]
	return owner, ok
}

func unitSetEqual(got []storageunit.UnitID, want ...storageunit.UnitID) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestOwnedUnits_DerivesNodesUnits(t *testing.T) {
	c := storageunit.MustUnitCount(8)
	owners := fakeOwners{
		0: "n1", 1: "n2", 2: "n1", 3: "n3",
		4: "n2", 5: "n1", 6: "n3", 7: "n2",
	}
	cases := []struct {
		self storageunit.NodeID
		want []storageunit.UnitID
	}{
		{"n1", []storageunit.UnitID{0, 2, 5}},
		{"n2", []storageunit.UnitID{1, 4, 7}},
		{"n3", []storageunit.UnitID{3, 6}},
		{"n4", []storageunit.UnitID{}}, // owns nothing
	}
	for _, tc := range cases {
		got := storageunit.OwnedUnits(tc.self, c, owners)
		if !unitSetEqual(got, tc.want...) {
			t.Errorf("OwnedUnits(%q) = %v, want %v", tc.self, got, tc.want)
		}
	}
}

func TestOwnedUnits_Ascending(t *testing.T) {
	c := storageunit.MustUnitCount(16)
	// Assign every unit to "self" so the result is the full 0..15 ascending.
	owners := storageunit.OwnerLookupFunc(func(_ storageunit.UnitID) (storageunit.NodeID, bool) {
		return "self", true
	})
	got := storageunit.OwnedUnits("self", c, owners)
	if len(got) != 16 {
		t.Fatalf("OwnedUnits should return all 16 units, got %d", len(got))
	}
	for i := range got {
		if got[i] != storageunit.UnitID(i) {
			t.Fatalf("OwnedUnits not ascending at %d: got %d", i, got[i])
		}
	}
}

func TestOwnedUnits_UnownedUnitsExcluded(t *testing.T) {
	c := storageunit.MustUnitCount(8)
	// Only some units have an owner; the rest report ok=false and must be
	// excluded from EVERY node's set (an empty ring assigns nothing).
	owners := fakeOwners{
		1: "self",
		5: "self",
		// units 0,2,3,4,6,7 have no owner
	}
	got := storageunit.OwnedUnits("self", c, owners)
	if !unitSetEqual(got, 1, 5) {
		t.Fatalf("OwnedUnits with partial ownership = %v, want [1 5]", got)
	}

	// A node that matches an unowned-unit's zero NodeID must NOT pick up the
	// gaps: an unowned unit (ok=false) belongs to no one, including "".
	gotEmpty := storageunit.OwnedUnits("", c, owners)
	if len(gotEmpty) != 0 {
		t.Fatalf(`OwnedUnits("") = %v, want empty: ok=false units must not be claimed by the zero NodeID`, gotEmpty)
	}
}

func TestOwnedUnits_NeverReturnsNil(t *testing.T) {
	c := storageunit.MustUnitCount(4)
	empty := fakeOwners{}
	got := storageunit.OwnedUnits("nobody", c, empty)
	if got == nil {
		t.Fatalf("OwnedUnits should return a non-nil empty slice, got nil")
	}
	if len(got) != 0 {
		t.Fatalf("OwnedUnits over empty ownership = %v, want empty", got)
	}
}

func TestOwnedUnits_GuardsZeroCountAndNilLookup(t *testing.T) {
	var zero storageunit.UnitCount
	owners := fakeOwners{0: "n1"}
	if got := storageunit.OwnedUnits("n1", zero, owners); len(got) != 0 {
		t.Fatalf("OwnedUnits with zero count should be empty, got %v", got)
	}
	c := storageunit.MustUnitCount(4)
	if got := storageunit.OwnedUnits("n1", c, nil); len(got) != 0 {
		t.Fatalf("OwnedUnits with nil lookup should be empty, got %v", got)
	}
}

// TestOwnedUnits_PartitionsTheUnitSpace pins the cluster-wide invariant: when
// every unit has exactly one owner, summing every node's OwnedUnits covers
// the whole 0..N-1 space once. No unit is owned by two nodes (single-writer)
// and none is orphaned.
func TestOwnedUnits_PartitionsTheUnitSpace(t *testing.T) {
	c := storageunit.MustUnitCount(16)
	nodes := []storageunit.NodeID{"a", "b", "c"}
	owners := fakeOwners{}
	for i := uint32(0); i < c.N(); i++ {
		owners[storageunit.UnitID(i)] = nodes[i%uint32(len(nodes))]
	}

	covered := make(map[storageunit.UnitID]int)
	for _, n := range nodes {
		for _, u := range storageunit.OwnedUnits(n, c, owners) {
			covered[u]++
		}
	}
	if len(covered) != 16 {
		t.Fatalf("union of OwnedUnits covered %d units, want 16", len(covered))
	}
	for u, n := range covered {
		if n != 1 {
			t.Fatalf("unit %d owned by %d nodes, want exactly 1 (single-writer invariant)", u, n)
		}
	}
}

func TestOwnsUnit(t *testing.T) {
	owners := fakeOwners{0: "n1", 1: "n2"}
	cases := []struct {
		self storageunit.NodeID
		u    storageunit.UnitID
		want bool
	}{
		{"n1", 0, true},
		{"n2", 0, false},
		{"n2", 1, true},
		{"n1", 2, false}, // unit 2 has no owner
	}
	for _, tc := range cases {
		if got := storageunit.OwnsUnit(tc.self, tc.u, owners); got != tc.want {
			t.Errorf("OwnsUnit(%q, %d) = %v, want %v", tc.self, tc.u, got, tc.want)
		}
	}
	if storageunit.OwnsUnit("n1", 0, nil) {
		t.Errorf("OwnsUnit with nil lookup should be false")
	}
}
