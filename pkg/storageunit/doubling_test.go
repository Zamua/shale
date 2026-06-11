package storageunit_test

import (
	"fmt"
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

// TestChildUnit_EqualsDirect2NMap is the core doubling invariant:
//
//	ChildUnit(h, N) == UnitForHash(h, 2N)
//
// for EVERY hash. The child computed by the single-bit bisect must equal the
// unit the key maps to directly under 2N. A wrong split (wrong bit, wrong
// addend) breaks this for some hash. We sweep low N and a large fixed-bit
// hash space so the bit boundary at every level is exercised.
func TestChildUnit_EqualsDirect2NMap(t *testing.T) {
	for _, n := range []int{1, 2, 4, 8, 16, 256} {
		old := storageunit.MustUnitCount(n)
		newCount, err := old.Double()
		if err != nil {
			t.Fatalf("Double(N=%d): %v", n, err)
		}
		// Sweep every residue across two full N-cycles plus the split bit,
		// i.e. hashes 0 .. 4N-1, which exercises both the low log2(N) bits
		// and the one new bit (and the bit above it, which must NOT affect
		// the child).
		for h := uint64(0); h < uint64(4*n); h++ {
			hash := storageunit.ShardHash(h)
			child, err := storageunit.ChildUnit(hash, old)
			if err != nil {
				t.Fatalf("ChildUnit(%d, N=%d): %v", h, n, err)
			}
			want := storageunit.UnitForHash(hash, newCount)
			if child != want {
				t.Fatalf("ChildUnit(%d, N=%d) = %d, want UnitForHash(%d, N=%d) = %d",
					h, n, child, h, newCount.N(), want)
			}
		}
	}
}

// TestChildUnit_LandsInExactlyKOrKPlusN exhaustively pins, for small N, that
// each parent unit K bisects into EXACTLY {K, K+N} and nothing else: every
// hash whose parent is K produces a child that is either K or K+N, and over
// the full residue space BOTH children are reached.
func TestChildUnit_LandsInExactlyKOrKPlusN(t *testing.T) {
	for _, n := range []int{4, 8} { // 4->8 and 8->16 as the task requires
		old := storageunit.MustUnitCount(n)

		// For each parent K, collect the set of children seen across all
		// hashes whose parent is K.
		childrenSeen := make(map[storageunit.UnitID]map[storageunit.UnitID]bool)
		for k := 0; k < n; k++ {
			childrenSeen[storageunit.UnitID(k)] = make(map[storageunit.UnitID]bool)
		}

		// Sweep 0 .. 2N-1: each value of (hash mod 2N) appears once, so every
		// (parent, split-bit) combination is covered exactly once.
		for h := uint64(0); h < uint64(2*n); h++ {
			hash := storageunit.ShardHash(h)
			parent := storageunit.UnitForHash(hash, old)
			child, err := storageunit.ChildUnit(hash, old)
			if err != nil {
				t.Fatalf("ChildUnit(%d, N=%d): %v", h, n, err)
			}
			k := uint32(parent)
			if uint32(child) != k && uint32(child) != k+uint32(n) {
				t.Fatalf("ChildUnit(%d, N=%d): parent=%d child=%d, want %d or %d",
					h, n, parent, child, k, k+uint32(n))
			}
			childrenSeen[parent][child] = true
		}

		// Every parent must have reached BOTH of its children (the bisect is
		// total: a unit with only one observed child would be a broken
		// split that happens to stay in range).
		for k := 0; k < n; k++ {
			got := childrenSeen[storageunit.UnitID(k)]
			wantLow := storageunit.UnitID(k)
			wantHigh := storageunit.UnitID(k + n)
			if !got[wantLow] || !got[wantHigh] {
				t.Fatalf("parent unit %d (N=%d) did not bisect into both {%d, %d}; saw %v",
					k, n, wantLow, wantHigh, got)
			}
			if len(got) != 2 {
				t.Fatalf("parent unit %d (N=%d) reached %d children %v, want exactly 2",
					k, n, len(got), got)
			}
		}
	}
}

func TestChildUnits_Enumerate(t *testing.T) {
	cases := []struct {
		k    storageunit.UnitID
		n    int
		low  storageunit.UnitID
		high storageunit.UnitID
	}{
		{0, 4, 0, 4},
		{1, 4, 1, 5},
		{3, 4, 3, 7},
		{0, 8, 0, 8},
		{7, 8, 7, 15},
		{0, 1, 0, 1}, // N=1 -> 2: unit 0 splits into {0, 1}
	}
	for _, c := range cases {
		old := storageunit.MustUnitCount(c.n)
		low, high, err := storageunit.ChildUnits(c.k, old)
		if err != nil {
			t.Errorf("ChildUnits(%d, N=%d) error: %v", c.k, c.n, err)
			continue
		}
		if low != c.low || high != c.high {
			t.Errorf("ChildUnits(%d, N=%d) = {%d, %d}, want {%d, %d}", c.k, c.n, low, high, c.low, c.high)
		}
	}
}

func TestChildUnits_OutOfRange(t *testing.T) {
	old := storageunit.MustUnitCount(4)
	if _, _, err := storageunit.ChildUnits(4, old); err == nil {
		t.Fatalf("ChildUnits(4, N=4) should error: 4 is out of range")
	}
	if _, _, err := storageunit.ChildUnits(99, old); err == nil {
		t.Fatalf("ChildUnits(99, N=4) should error: out of range")
	}
}

// TestChildUnits_PartitionsTheChildSpace pins that the two-children sets are
// DISJOINT across parents and TOGETHER cover the entire 2N space exactly
// once. This is what guarantees "no global re-partition": doubling is a
// clean bijection from N parents to 2N children.
func TestChildUnits_PartitionsTheChildSpace(t *testing.T) {
	for _, n := range []int{4, 8, 16} {
		old := storageunit.MustUnitCount(n)
		covered := make(map[storageunit.UnitID]int)
		for k := 0; k < n; k++ {
			low, high, err := storageunit.ChildUnits(storageunit.UnitID(k), old)
			if err != nil {
				t.Fatalf("ChildUnits(%d, N=%d): %v", k, n, err)
			}
			covered[low]++
			covered[high]++
		}
		if len(covered) != 2*n {
			t.Fatalf("N=%d: children cover %d distinct units, want %d", n, len(covered), 2*n)
		}
		for u := 0; u < 2*n; u++ {
			if covered[storageunit.UnitID(u)] != 1 {
				t.Fatalf("N=%d: child unit %d covered %d times, want exactly 1", n, u, covered[storageunit.UnitID(u)])
			}
		}
	}
}

func TestChildUnit_DoubleAtMaxErrors(t *testing.T) {
	maxCount := storageunit.MustUnitCount(storageunit.MaxUnitCount)
	if _, err := storageunit.ChildUnit(storageunit.ShardHash(123), maxCount); err == nil {
		t.Fatalf("ChildUnit at MaxUnitCount should error (2N overflows ceiling)")
	}
	if _, _, err := storageunit.ChildUnits(0, maxCount); err == nil {
		t.Fatalf("ChildUnits at MaxUnitCount should error (2N overflows ceiling)")
	}
}

// TestParentUnit_InvertsChildUnits checks the halving inverse: both K and
// K+N collapse back to K.
func TestParentUnit_InvertsChildUnits(t *testing.T) {
	for _, n := range []int{2, 4, 8, 16} {
		old := storageunit.MustUnitCount(n)
		newCount, _ := old.Double()
		for k := 0; k < n; k++ {
			low, high, err := storageunit.ChildUnits(storageunit.UnitID(k), old)
			if err != nil {
				t.Fatalf("ChildUnits(%d, N=%d): %v", k, n, err)
			}
			for _, child := range []storageunit.UnitID{low, high} {
				parent, err := storageunit.ParentUnit(child, newCount)
				if err != nil {
					t.Fatalf("ParentUnit(%d, N=%d): %v", child, newCount.N(), err)
				}
				if parent != storageunit.UnitID(k) {
					t.Fatalf("ParentUnit(%d, N=%d) = %d, want %d", child, newCount.N(), parent, k)
				}
			}
		}
	}
}

func TestParentUnit_MinCannotHalve(t *testing.T) {
	one := storageunit.MustUnitCount(1)
	if _, err := storageunit.ParentUnit(0, one); err == nil {
		t.Fatalf("ParentUnit at N=1 should error: cannot halve")
	}
}

// TestDoubling_CoLocationStableUnderGrowth checks the design-doc property
// that low bits are STABLE across doublings: a key never leaves its old
// unit's child set when N doubles. Concretely, after doubling, a key's unit
// is always one of its old unit's two children - it cannot jump to an
// unrelated unit. This is what makes the per-unit bisect a local operation.
func TestDoubling_CoLocationStableUnderGrowth(t *testing.T) {
	old := storageunit.MustUnitCount(8)
	newCount, _ := old.Double()
	for i := 0; i < 10000; i++ {
		h := storageunit.HashShardKey([]byte(fmt.Sprintf("k-%d", i)))
		parent := storageunit.UnitForHash(h, old)
		afterDouble := storageunit.UnitForHash(h, newCount)
		low, high, err := storageunit.ChildUnits(parent, old)
		if err != nil {
			t.Fatalf("ChildUnits(%d, N=8): %v", parent, err)
		}
		if afterDouble != low && afterDouble != high {
			t.Fatalf("key %d moved from unit %d to %d under doubling, not into either child {%d, %d}",
				i, parent, afterDouble, low, high)
		}
	}
}
