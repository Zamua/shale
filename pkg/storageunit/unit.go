// Package storageunit is the pure domain layer for shale's per-shard
// lease-handoff storage model (see the "Per-shard lease-handoff storage"
// section in docs/SPEC.md). It owns the routing math: a key's shard key
// hashes directly to a storage UNIT (unit = hash(ShardKey) & (N-1), the
// physical database + lease unit), co-located keys (same shard key, same
// hash) share a unit so a transaction stays in one engine, and the ring
// owns units to NODES. The unit is the single routing + storage + lease
// granularity; there is no separate coarse-partition layer.
//
// This package is deliberately I/O-free. It holds value types and pure
// functions only; the one interface it declares (BackendFactory) is the
// SEAM where the infrastructure layer (slatedb/memory mounting, gRPC lease
// handoff) plugs in later. Nothing here opens a database, dials a peer, or
// touches object storage. The only non-stdlib import is pkg/backend, and
// only for the Backend interface TYPE that BackendFactory hands back.
//
// Why a unit layer at all: slatedb is single-writer per database, so to get
// concurrent writers you need more than one database. A unit IS one slatedb
// (one object-store prefix) and IS the unit of ownership/lease. The unit
// count N is a small fixed power of two so the cluster can grow by DOUBLING
// (N -> 2N) where each unit cleanly bisects into {K, K+N}.
package storageunit

import (
	"fmt"
	"math/bits"
)

// UnitID identifies one storage unit. Units are numbered 0..N-1 for a unit
// count N. A UnitID is meaningful only paired with the UnitCount it was
// derived under: the same logical key lands on different UnitIDs at N and
// at 2N (that is the whole point of doubling). Callers that persist a
// UnitID MUST persist the generation/UnitCount alongside it.
type UnitID uint32

// UnitCount is the fixed number of storage units N. It is a power of two so
// that:
//
//   - shard -> unit is a cheap bit-mask: hash & (N-1) == hash mod N.
//   - growth is DOUBLING: N -> 2N adds exactly one hash bit, and every unit
//     K splits into exactly the two children K and K+N (see doubling.go).
//
// Construct one with NewUnitCount, which rejects non-power-of-two values so
// the bisect invariant can never be violated by a bad config.
type UnitCount struct {
	n uint32
}

// MinUnitCount is the smallest legal unit count. N == 1 means a single unit
// (the whole keyspace in one database, equivalent to the per-node model);
// it is a valid power of two and a valid starting point before any
// doubling.
const MinUnitCount = 1

// MaxUnitCount bounds N so a UnitID (uint32) and the K+N doubling arithmetic
// never overflow. 2^30 leaves headroom for one more doubling (K+N with
// N == 2^30 stays well under uint32 max) and is far above any realistic
// node count, which is the true ceiling on N (the design doc bounds N below
// by max node count and above by per-engine memtable memory).
const MaxUnitCount = 1 << 30

// NewUnitCount returns the UnitCount for n, or an error if n is not a power
// of two in [MinUnitCount, MaxUnitCount]. Failing closed here is what lets
// every downstream function assume N is a clean power of two: the bit-mask
// map and the single-bit bisect both depend on it.
func NewUnitCount(n int) (UnitCount, error) {
	if n < MinUnitCount {
		return UnitCount{}, fmt.Errorf("storageunit: unit count %d is below minimum %d", n, MinUnitCount)
	}
	if n > MaxUnitCount {
		return UnitCount{}, fmt.Errorf("storageunit: unit count %d exceeds maximum %d", n, MaxUnitCount)
	}
	if bits.OnesCount32(uint32(n)) != 1 {
		return UnitCount{}, fmt.Errorf("storageunit: unit count %d is not a power of two", n)
	}
	return UnitCount{n: uint32(n)}, nil
}

// MustUnitCount is NewUnitCount that panics on error. For constants and
// tests where n is a compile-time-known good power of two.
func MustUnitCount(n int) UnitCount {
	uc, err := NewUnitCount(n)
	if err != nil {
		panic(err)
	}
	return uc
}

// N returns the number of units as an int.
func (c UnitCount) N() uint32 { return c.n }

// mask returns N-1, the low-bit mask such that hash & mask == hash mod N.
// Valid only because N is a power of two (guaranteed by the constructor).
func (c UnitCount) mask() uint64 { return uint64(c.n) - 1 }

// IsZero reports whether c is the zero value (an uninitialized UnitCount,
// which has no valid N). The zero value never escapes NewUnitCount; this is
// a guard for callers that receive a UnitCount through a struct field.
func (c UnitCount) IsZero() bool { return c.n == 0 }

// Contains reports whether u is a valid unit id under this count, i.e.
// 0 <= u < N. A unit id derived under a DIFFERENT count may be out of
// range here; this is how a caller detects a stale generation.
func (c UnitCount) Contains(u UnitID) bool { return uint32(u) < c.n }

// IDs returns every valid UnitID under this count, 0..N-1 ascending. The
// slice is freshly allocated per call so the caller may retain or mutate
// it. Used by ownership derivation and by tests that enumerate the unit
// space.
func (c UnitCount) IDs() []UnitID {
	out := make([]UnitID, c.n)
	for i := uint32(0); i < c.n; i++ {
		out[i] = UnitID(i)
	}
	return out
}

// Double returns the UnitCount for 2N, the result of one doubling step. It
// errors if 2N would exceed MaxUnitCount. Doubling is the only supported
// growth operation (the design trades arbitrary-granularity resizing for
// hash uniformity + a copy-free split).
func (c UnitCount) Double() (UnitCount, error) {
	return NewUnitCount(int(c.n) * 2)
}

// Halve returns the UnitCount for N/2, the result of one merging step and the
// exact inverse of Double. It errors if N is already at MinUnitCount: a single
// unit is the whole keyspace in one database and cannot merge further (and the
// zero/invalid counts never reach here, NewUnitCount having rejected them).
// Halving is the only supported shrink operation, mirroring Double: every pair
// of units {K, K+N/2} collapses into the single parent K (see ParentUnit in
// doubling.go). Because N is a power of two >= 2 here, N/2 is always a valid
// power of two, so the NewUnitCount re-check never rejects a halve above the
// floor; the explicit floor guard is what gives a clear error at N == 1.
func (c UnitCount) Halve() (UnitCount, error) {
	if c.n <= MinUnitCount {
		return UnitCount{}, fmt.Errorf("storageunit: %s cannot be halved (already at minimum N=%d)", c, MinUnitCount)
	}
	return NewUnitCount(int(c.n) / 2)
}

// String renders the count as "N=<n>" for logs and test failures.
func (c UnitCount) String() string { return fmt.Sprintf("N=%d", c.n) }
