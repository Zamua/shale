package storageunit

import (
	"fmt"
	"math/bits"
)

// The doubling property (design doc, "Resharding"):
//
//	hash mod 2N = (hash mod N) + N * bit_log2(N)(hash)
//
// where bit_log2(N)(hash) is the single hash bit just above the low log2(N)
// bits that already decide the unit under N. So every unit K under N splits
// into EXACTLY TWO units under 2N:
//
//	K        (when that bit is 0)
//	K + N    (when that bit is 1)
//
// No global re-partition: each unit bisects independently by one bit. The
// functions below are that math, isolated so it can be tested exhaustively
// (unit K -> {K, K+N} for small N) and reused by the resharder.

// splitBitIndex returns the index of the hash bit that decides the split
// when doubling FROM count c (N -> 2N). It is log2(N): the bit immediately
// above the low log2(N) bits the unit-under-N map already consumes. For
// N == 1 (log2 == 0) the split is decided by bit 0.
func splitBitIndex(c UnitCount) uint {
	// N is a power of two, so TrailingZeros gives log2(N) exactly.
	return uint(bits.TrailingZeros32(c.n))
}

// ChildUnit returns the child UnitID that hash h lands in after doubling
// the unit count FROM old (N) TO 2N. The child is the unit the key would
// map to under 2N, which is always one of the two children of its old unit
// K = UnitForHash(h, old): either K (the split bit is 0) or K+N (the split
// bit is 1).
//
// It errors if old cannot be doubled (2N would exceed MaxUnitCount). On
// success the returned UnitID is guaranteed to equal UnitForHash(h, 2N) and
// to be one of old.UnitForHash(h, old)'s two ChildUnits; the tests pin both
// equalities.
//
// NOT A PRODUCTION ROUTING PATH, deliberately. The resharder and the routing
// layer both resolve a key's post-doubling unit by the DIRECT map
// UnitForHash(h, 2N) (see resolveGenUnit and the bisect's copy split), and
// enumerate a unit's two targets with ChildUnits. This function is the
// bit-arithmetic statement of WHY that direct map is correct: it computes the
// child the long way (parent K, plus N exactly when the one new hash bit is
// set), so the tests can assert ChildUnit(h, N) == UnitForHash(h, 2N) for
// every hash. That equivalence IS the doubling design - it is what makes a
// unit bisect independently into {K, K+N} with no global re-partition, and
// what lets a co-located {tag} set land wholly in one child. Keep it and its
// test: they pin the invariant the whole reshard rests on, and a regression
// here would otherwise only surface as silent data misplacement mid-reshard.
func ChildUnit(h ShardHash, old UnitCount) (UnitID, error) {
	if _, err := old.Double(); err != nil {
		return 0, err
	}
	// Spelled as "old unit + (split bit << log2(N))" to make the bisect
	// explicit: the child is the parent K plus N exactly when the one new
	// hash bit is set. The test pins that this equals the direct 2N map
	// UnitForHash(h, 2N) for every hash.
	parent := UnitForHash(h, old)
	bit := (uint64(h) >> splitBitIndex(old)) & 1
	return parent + UnitID(bit)*UnitID(old.n), nil
}

// ChildUnits returns the two child UnitIDs that unit K bisects into when the
// count doubles FROM old (N) TO 2N: exactly {K, K+N}, low child first. It
// errors if k is not a valid unit under old, or if old cannot be doubled.
//
// This is the structural inverse of ChildUnit: ChildUnit tells you which of
// these two a given key goes to; ChildUnits enumerates the pair so the
// resharder can create both target databases for K before streaming K's
// keys into them.
func ChildUnits(k UnitID, old UnitCount) (low, high UnitID, err error) {
	if !old.Contains(k) {
		return 0, 0, fmt.Errorf("storageunit: unit %d is out of range for %s", k, old)
	}
	if _, err := old.Double(); err != nil {
		return 0, 0, err
	}
	return k, k + UnitID(old.n), nil
}

// ParentUnit returns the unit that child c collapses back into when the
// count HALVES from cur (2N) to N. It is the inverse of ChildUnits: both K
// and K+N map back to K. Errors if c is out of range for cur, or if cur is
// already at the minimum (N == 1 cannot halve).
//
// Provided for symmetry and for migration/validation tooling that needs to
// reason backwards (which old unit does a 2N unit descend from); the live
// growth path only ever doubles.
func ParentUnit(c UnitID, cur UnitCount) (UnitID, error) {
	if !cur.Contains(c) {
		return 0, fmt.Errorf("storageunit: unit %d is out of range for %s", c, cur)
	}
	if cur.n <= MinUnitCount {
		return 0, fmt.Errorf("storageunit: %s cannot be halved", cur)
	}
	half := cur.n / 2
	return UnitID(uint32(c) % half), nil
}
