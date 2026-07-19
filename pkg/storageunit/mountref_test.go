package storageunit_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

// TestMountRef_SoleAndReplicaZeroAreDistinct is the DATA-LOSS GUARD on the port
// collapse, stated at the domain level rather than inside one adapter.
//
// Collapsing two ports into one keyed by (unit, replica) makes R=1 "replica 0".
// If the identity carried only those two coordinates, a sole mount and replica
// 0 of the same unit would be the SAME value, every adapter would derive ONE
// location for both, and an existing R=1 deployment's bytes would resolve to
// the replica-0 location: an empty store, reported as a healthy fresh unit.
// The layout selector is the third coordinate that stops that, so the two must
// never compare equal.
func TestMountRef_SoleAndReplicaZeroAreDistinct(t *testing.T) {
	gu := storageunit.NewGenUnit(1, 5)
	sole := storageunit.SoleMount(gu)
	rep0 := storageunit.ReplicaMount(storageunit.NewReplicaUnit(gu, 0))

	if sole == rep0 {
		t.Fatalf("SoleMount(%s) == ReplicaMount(%s/r0): the layout selector is not part of the identity, so an adapter cannot tell the two mounts apart and an R=1 deployment's bytes would be orphaned", gu, gu)
	}
	if !rep0.Replicated() {
		t.Fatalf("ReplicaMount(%s/r0).Replicated() = false, want true", gu)
	}
	if sole.Replicated() {
		t.Fatalf("SoleMount(%s).Replicated() = true, want false", gu)
	}
}

// TestMountRef_DistinctMapKeys pins the property an adapter relies on to track
// every mount it holds in ONE map: the two layouts of the same (unit, position)
// do not evict each other.
func TestMountRef_DistinctMapKeys(t *testing.T) {
	gu := storageunit.NewGenUnit(1, 5)
	m := map[storageunit.MountRef]string{}
	m[storageunit.SoleMount(gu)] = "sole"
	m[storageunit.ReplicaMount(storageunit.NewReplicaUnit(gu, 0))] = "rep0"

	if len(m) != 2 {
		t.Fatalf("sole and replica-0 refs collided as map keys: map has %d entries, want 2", len(m))
	}
	if got := m[storageunit.SoleMount(gu)]; got != "sole" {
		t.Fatalf("the sole key was clobbered by the replica key: got %q", got)
	}
}

// TestMountRef_Accessors pins that R=1 IS replica 0 on the read side: a sole
// mount answers the replica question with 0 (so a caller keyed by position
// still resolves), while still reporting the sole LAYOUT.
func TestMountRef_Accessors(t *testing.T) {
	gu := storageunit.NewGenUnit(2, 7)
	sole := storageunit.SoleMount(gu)
	if sole.Unit() != gu {
		t.Fatalf("SoleMount(%s).Unit() = %s, want %s", gu, sole.Unit(), gu)
	}
	if sole.Replica() != 0 {
		t.Fatalf("SoleMount(%s).Replica() = %d, want 0 (R=1 is replica 0)", gu, sole.Replica())
	}
	if want := storageunit.NewReplicaUnit(gu, 0); sole.ReplicaUnit() != want {
		t.Fatalf("SoleMount(%s).ReplicaUnit() = %s, want %s", gu, sole.ReplicaUnit(), want)
	}

	ru := storageunit.NewReplicaUnit(gu, 3)
	rep := storageunit.ReplicaMount(ru)
	if rep.Unit() != gu || rep.Replica() != 3 || rep.ReplicaUnit() != ru {
		t.Fatalf("ReplicaMount(%s) accessors = (%s, %d, %s), want (%s, 3, %s)", ru, rep.Unit(), rep.Replica(), rep.ReplicaUnit(), gu, ru)
	}
}

// TestMountRef_StringNamesTheLayout pins that the rendered identity says WHICH
// mount it is. Two mounts whose remaining coordinates are identical must not
// render the same, or a log line or error naming one cannot be read back.
func TestMountRef_StringNamesTheLayout(t *testing.T) {
	gu := storageunit.NewGenUnit(1, 5)
	sole := storageunit.SoleMount(gu).String()
	rep0 := storageunit.ReplicaMount(storageunit.NewReplicaUnit(gu, 0)).String()

	if sole == rep0 {
		t.Fatalf("sole and replica-0 render identically as %q: a message naming one is ambiguous", sole)
	}
	if want := "unit g1/u5"; sole != want {
		t.Fatalf("SoleMount(%s).String() = %q, want %q", gu, sole, want)
	}
	if want := "replica g1/u5/r0"; rep0 != want {
		t.Fatalf("ReplicaMount(%s/r0).String() = %q, want %q", gu, rep0, want)
	}
	if !strings.Contains(rep0, gu.String()) {
		t.Fatalf("replica rendering %q does not contain the unit %q", rep0, gu)
	}
}

// TestCompareMountRefs_TotalOrder pins that the shared comparison orders by
// (generation, unit, replica, layout) and separates every distinct ref, so
// every BackendFactory's OpenUnits enumerates the same way.
func TestCompareMountRefs_TotalOrder(t *testing.T) {
	mk := func(gen storageunit.Generation, id storageunit.UnitID, rep uint8, replicated bool) storageunit.MountRef {
		gu := storageunit.NewGenUnit(gen, id)
		if replicated {
			return storageunit.ReplicaMount(storageunit.NewReplicaUnit(gu, rep))
		}
		return storageunit.SoleMount(gu)
	}
	want := []storageunit.MountRef{
		mk(0, 0, 0, false),
		mk(0, 0, 0, true),
		mk(0, 0, 1, true),
		mk(0, 3, 0, false),
		mk(1, 0, 0, false),
		mk(1, 2, 2, true),
	}
	got := []storageunit.MountRef{
		mk(1, 2, 2, true),
		mk(0, 0, 1, true),
		mk(0, 3, 0, false),
		mk(0, 0, 0, true),
		mk(1, 0, 0, false),
		mk(0, 0, 0, false),
	}
	slices.SortFunc(got, storageunit.CompareMountRefs)
	if !slices.Equal(got, want) {
		t.Fatalf("CompareMountRefs order = %v, want %v", got, want)
	}
	// Equal only for the identical ref: a total order over distinct refs.
	for i := range want {
		for j := range want {
			if c := storageunit.CompareMountRefs(want[i], want[j]); (c == 0) != (i == j) {
				t.Fatalf("CompareMountRefs(%s, %s) = %d, want 0 iff identical", want[i], want[j], c)
			}
		}
	}
}
