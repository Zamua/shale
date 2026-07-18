package storageunit

import (
	"reflect"
	"testing"
)

func TestReplicaUnit_String(t *testing.T) {
	ru := NewReplicaUnit(NewGenUnit(1, 5), 0)
	if got, want := ru.String(), "g1/u5/r0"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

// staticReplicas is a fixed unit -> ordered-replica-set map for the pure
// OwnedReplicaUnits tests (no ring needed).
type staticReplicas map[UnitID][]NodeID

func (s staticReplicas) ReplicasOf(u UnitID) []NodeID { return s[u] }

func TestOwnedReplicaUnits_KeepsSelfWithPosition(t *testing.T) {
	c := MustUnitCount(4)
	// unit 0: [A, B], unit 1: [B, C], unit 2: [C, A], unit 3: [A, B]
	repl := staticReplicas{
		0: {"A", "B"},
		1: {"B", "C"},
		2: {"C", "A"},
		3: {"A", "B"},
	}

	gotA := OwnedReplicaUnits("A", c, repl)
	wantA := []OwnedReplica{
		{Unit: 0, Replica: 0}, // primary of 0
		{Unit: 2, Replica: 1}, // successor of 2
		{Unit: 3, Replica: 0}, // primary of 3
	}
	if !reflect.DeepEqual(gotA, wantA) {
		t.Fatalf("OwnedReplicaUnits(A) = %v, want %v", gotA, wantA)
	}

	gotB := OwnedReplicaUnits("B", c, repl)
	wantB := []OwnedReplica{
		{Unit: 0, Replica: 1},
		{Unit: 1, Replica: 0},
		{Unit: 3, Replica: 1},
	}
	if !reflect.DeepEqual(gotB, wantB) {
		t.Fatalf("OwnedReplicaUnits(B) = %v, want %v", gotB, wantB)
	}
}

func TestOwnedReplicaUnits_EachReplicaSetIsRDistinctNodes(t *testing.T) {
	c := MustUnitCount(8)
	repl := staticReplicas{}
	for i := uint32(0); i < 8; i++ {
		repl[UnitID(i)] = []NodeID{"A", "B"} // R=2 everywhere
	}
	// Every unit appears for A at position 0 and for B at position 1, so the
	// two nodes together cover every unit exactly twice (R=2 placement).
	a := OwnedReplicaUnits("A", c, repl)
	b := OwnedReplicaUnits("B", c, repl)
	if len(a) != 8 || len(b) != 8 {
		t.Fatalf("expected 8 owned each, got A=%d B=%d", len(a), len(b))
	}
	for i := range a {
		if a[i].Replica != 0 || b[i].Replica != 1 {
			t.Fatalf("unit %d: A.Replica=%d B.Replica=%d, want 0 and 1", a[i].Unit, a[i].Replica, b[i].Replica)
		}
	}
}

func TestOwnedReplicaUnits_NodeNotInAnySetOwnsNothing(t *testing.T) {
	c := MustUnitCount(4)
	repl := staticReplicas{0: {"A", "B"}, 1: {"A", "B"}, 2: {"A", "B"}, 3: {"A", "B"}}
	got := OwnedReplicaUnits("Z", c, repl)
	if len(got) != 0 {
		t.Fatalf("Z owns nothing, got %v", got)
	}
	if got == nil {
		t.Fatalf("expected non-nil empty slice")
	}
}

func TestOwnedReplicaUnits_ZeroCountOrNilLookup(t *testing.T) {
	repl := staticReplicas{0: {"A"}}
	if got := OwnedReplicaUnits("A", UnitCount{}, repl); len(got) != 0 {
		t.Fatalf("zero count should own nothing, got %v", got)
	}
	if got := OwnedReplicaUnits("A", MustUnitCount(4), nil); len(got) != 0 {
		t.Fatalf("nil lookup should own nothing, got %v", got)
	}
}

// TestOwnedReplicaUnits_AscendingAndNonNil pins two shape guarantees the
// reconcile depends on. ASCENDING: the derivation enumerates 0..N-1, so the
// desired set comes back ordered by UnitID, which is what makes a
// desired-vs-mounted diff comparable without a sort. NON-NIL: an empty result
// is a freshly allocated empty slice, never nil, so a caller can range and
// append over it unconditionally.
func TestOwnedReplicaUnits_AscendingAndNonNil(t *testing.T) {
	c := MustUnitCount(16)

	// Every unit replicated by "self" at position 0: the full 0..15 ascending.
	all := ReplicaLookupFunc(func(UnitID) []NodeID { return []NodeID{"self", "other"} })
	got := OwnedReplicaUnits("self", c, all)
	if len(got) != 16 {
		t.Fatalf("OwnedReplicaUnits should return all 16 units, got %d", len(got))
	}
	for i, o := range got {
		if o.Unit != UnitID(i) {
			t.Fatalf("not ascending at index %d: got unit %d", i, o.Unit)
		}
		if o.Replica != 0 {
			t.Fatalf("unit %d: replica = %d, want 0 (self is first in the set)", o.Unit, o.Replica)
		}
	}

	// A node in no replica set owns nothing, as a non-nil empty slice.
	none := OwnedReplicaUnits("nobody", c, staticReplicas{})
	if none == nil {
		t.Fatalf("OwnedReplicaUnits should return a non-nil empty slice, got nil")
	}
	if len(none) != 0 {
		t.Fatalf("OwnedReplicaUnits over empty ownership = %v, want empty", none)
	}
}

// TestOwnedReplicaUnits_CoversTheUnitSpaceRTimes pins the cluster-wide
// placement invariant at R>1. Summing every node's owned set over a
// round-robin placement covers the whole 0..N-1 space with EXACTLY R holders
// per unit, and each unit's R holders occupy DISTINCT positions 0..R-1. That
// is the durability property the replicated mount rests on: R independent
// copies, no unit orphaned, no position held twice (which would be two writers
// on one durable database).
func TestOwnedReplicaUnits_CoversTheUnitSpaceRTimes(t *testing.T) {
	const r = 2
	c := MustUnitCount(16)
	nodes := []NodeID{"a", "b", "c"}

	// Round-robin: unit u is replicated by nodes[u], nodes[u+1], ... (R of them).
	repl := ReplicaLookupFunc(func(u UnitID) []NodeID {
		set := make([]NodeID, 0, r)
		for i := 0; i < r; i++ {
			set = append(set, nodes[(int(u)+i)%len(nodes)])
		}
		return set
	})

	holders := make(map[UnitID]int)
	positions := make(map[UnitID]map[uint8]bool)
	for _, n := range nodes {
		for _, o := range OwnedReplicaUnits(n, c, repl) {
			holders[o.Unit]++
			if positions[o.Unit] == nil {
				positions[o.Unit] = make(map[uint8]bool)
			}
			if positions[o.Unit][o.Replica] {
				t.Fatalf("unit %d: position %d held by two nodes (two writers on one database)", o.Unit, o.Replica)
			}
			positions[o.Unit][o.Replica] = true
		}
	}
	if len(holders) != 16 {
		t.Fatalf("union of OwnedReplicaUnits covered %d units, want 16", len(holders))
	}
	for u, n := range holders {
		if n != r {
			t.Fatalf("unit %d held by %d nodes, want exactly R=%d", u, n, r)
		}
	}
}

func TestReplicaLookupFunc_Adapts(t *testing.T) {
	f := ReplicaLookupFunc(func(u UnitID) []NodeID {
		if u == 0 {
			return []NodeID{"X", "Y"}
		}
		return nil
	})
	if got := f.ReplicasOf(0); !reflect.DeepEqual(got, []NodeID{"X", "Y"}) {
		t.Fatalf("ReplicasOf(0) = %v", got)
	}
	if got := f.ReplicasOf(1); got != nil {
		t.Fatalf("ReplicasOf(1) = %v, want nil", got)
	}
}
