package storageunit_test

import (
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

// TestGenUnit_DistinctByGenerationAndID pins the equality semantics the
// mount-map + storage layer depend on: a GenUnit is identified by BOTH
// coordinates, so the same UnitID under two generations are distinct keys
// (the whole point of qualifying the identity for the doubling reshard).
func TestGenUnit_DistinctByGenerationAndID(t *testing.T) {
	g0u2 := storageunit.NewGenUnit(0, 2)
	g1u2 := storageunit.NewGenUnit(1, 2)
	g0u3 := storageunit.NewGenUnit(0, 3)

	if g0u2 == g1u2 {
		t.Fatalf("gen-0 unit 2 must differ from gen-1 unit 2 (generation qualifies identity)")
	}
	if g0u2 == g0u3 {
		t.Fatalf("gen-0 unit 2 must differ from gen-0 unit 3")
	}
	// Same coordinates compare equal (usable as a map key).
	if storageunit.NewGenUnit(1, 2) != g1u2 {
		t.Fatalf("equal coordinates must compare equal")
	}

	// As a map key, the two generations of the same unit id are separate
	// slots - this is exactly what keeps gen-g unit K and gen-(g+1) unit K
	// from colliding in the cluster mount map.
	m := map[storageunit.GenUnit]string{}
	m[g0u2] = "old"
	m[g1u2] = "new"
	if len(m) != 2 {
		t.Fatalf("map keyed by GenUnit collapsed two generations into %d slot(s)", len(m))
	}
	if m[g0u2] != "old" || m[g1u2] != "new" {
		t.Fatalf("GenUnit map slots crossed: %v", m)
	}
}

// TestGenUnit_String pins the human-readable form used in logs / failures.
func TestGenUnit_String(t *testing.T) {
	if got, want := storageunit.NewGenUnit(0, 5).String(), "g0/u5"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
	if got, want := storageunit.NewGenUnit(3, 17).String(), "g3/u17"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}
