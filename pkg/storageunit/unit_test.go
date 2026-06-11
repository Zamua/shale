package storageunit_test

import (
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

func TestNewUnitCount_PowerOfTwoOnly(t *testing.T) {
	cases := []struct {
		n     int
		valid bool
	}{
		{0, false},             // below min
		{1, true},              // min: a single unit
		{2, true},              //
		{3, false},             // not power of two
		{4, true},              //
		{6, false},             // not power of two
		{8, true},              //
		{16, true},             //
		{31, false},            // not power of two
		{32, true},             //
		{271, false},           // the shard/partition count is prime, NOT a valid unit count
		{1024, true},           //
		{1 << 30, true},        // max
		{(1 << 30) + 1, false}, // not power of two AND over max
		{1 << 31, false},       // over max
		{-1, false},            // below min
		{-8, false},            // negative power-of-two magnitude still rejected
	}
	for _, c := range cases {
		uc, err := storageunit.NewUnitCount(c.n)
		if c.valid {
			if err != nil {
				t.Errorf("NewUnitCount(%d) unexpected error: %v", c.n, err)
				continue
			}
			if got := uc.N(); got != uint32(c.n) {
				t.Errorf("NewUnitCount(%d).N() = %d, want %d", c.n, got, c.n)
			}
			if uc.IsZero() {
				t.Errorf("NewUnitCount(%d) reported IsZero for a valid count", c.n)
			}
		} else if err == nil {
			t.Errorf("NewUnitCount(%d) expected error, got valid count %s", c.n, uc)
		}
	}
}

func TestMustUnitCount_PanicsOnBad(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("MustUnitCount(3) should have panicked")
		}
	}()
	_ = storageunit.MustUnitCount(3)
}

func TestUnitCount_ZeroValue(t *testing.T) {
	var c storageunit.UnitCount
	if !c.IsZero() {
		t.Fatalf("zero-value UnitCount should report IsZero")
	}
	if c.N() != 0 {
		t.Fatalf("zero-value UnitCount N() = %d, want 0", c.N())
	}
	// IDs on a zero count is empty, not a panic.
	if got := c.IDs(); len(got) != 0 {
		t.Fatalf("zero-value UnitCount IDs() = %v, want empty", got)
	}
}

func TestUnitCount_Contains(t *testing.T) {
	c := storageunit.MustUnitCount(8)
	for u := storageunit.UnitID(0); u < 8; u++ {
		if !c.Contains(u) {
			t.Errorf("Contains(%d) = false, want true for N=8", u)
		}
	}
	for _, u := range []storageunit.UnitID{8, 9, 100, 1 << 20} {
		if c.Contains(u) {
			t.Errorf("Contains(%d) = true, want false for N=8", u)
		}
	}
}

func TestUnitCount_IDs(t *testing.T) {
	c := storageunit.MustUnitCount(4)
	got := c.IDs()
	want := []storageunit.UnitID{0, 1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("IDs() length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("IDs()[%d] = %d, want %d", i, got[i], want[i])
		}
	}
	// Freshly allocated: mutating the result must not affect a second call.
	got[0] = 99
	again := c.IDs()
	if again[0] != 0 {
		t.Fatalf("IDs() should return a fresh slice each call; got mutated %d", again[0])
	}
}

func TestUnitCount_Double(t *testing.T) {
	c := storageunit.MustUnitCount(4)
	d, err := c.Double()
	if err != nil {
		t.Fatalf("Double() unexpected error: %v", err)
	}
	if d.N() != 8 {
		t.Fatalf("Double() of N=4 = %s, want N=8", d)
	}

	// Doubling at the max must fail (2N would overflow the ceiling).
	maxCount := storageunit.MustUnitCount(storageunit.MaxUnitCount)
	if _, err := maxCount.Double(); err == nil {
		t.Fatalf("Double() at MaxUnitCount should error")
	}
}
