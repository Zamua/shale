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
