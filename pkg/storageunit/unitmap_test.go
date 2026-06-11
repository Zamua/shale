package storageunit_test

import (
	"fmt"
	"testing"

	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"

	"github.com/cespare/xxhash/v2"
)

func TestUnitForHash_InRange(t *testing.T) {
	// Every hash must land in [0, N) for several power-of-two counts.
	for _, n := range []int{1, 2, 4, 8, 16, 256, 1024} {
		c := storageunit.MustUnitCount(n)
		for i := 0; i < 5000; i++ {
			h := storageunit.HashShardKey([]byte(fmt.Sprintf("key-%d", i)))
			u := storageunit.UnitForHash(h, c)
			if !c.Contains(u) {
				t.Fatalf("UnitForHash landed out of range: hash=%d N=%d unit=%d", h, n, u)
			}
		}
	}
}

func TestUnitForHash_Deterministic(t *testing.T) {
	c := storageunit.MustUnitCount(16)
	h := storageunit.HashShardKey([]byte("repeatable"))
	first := storageunit.UnitForHash(h, c)
	for i := 0; i < 100; i++ {
		if got := storageunit.UnitForHash(h, c); got != first {
			t.Fatalf("UnitForHash not deterministic: first=%d iter %d=%d", first, i, got)
		}
	}
}

// TestUnitForHash_KnownVectors pins the actual mask math against hand-
// computed expectations. A broken map (e.g. dividing instead of masking, or
// using the wrong hasher) would fail here, not just smoke-pass.
func TestUnitForHash_KnownVectors(t *testing.T) {
	cases := []struct {
		hash storageunit.ShardHash
		n    int
		want storageunit.UnitID
	}{
		{0b0000, 8, 0},
		{0b0001, 8, 1},
		{0b0111, 8, 7},
		{0b1000, 8, 0},             // bit 3 is above the N=8 mask, so wraps to 0
		{0b1001, 8, 1},             // high bits ignored under N=8
		{0b1111, 4, 3},             // N=4 keeps only the low 2 bits
		{0b1111, 2, 1},             // N=2 keeps only the low bit
		{0xFFFFFFFFFFFFFFFF, 1, 0}, // N=1: everything is unit 0
		{0xDEADBEEF, 256, storageunit.UnitID(0xEF)}, // low byte for N=256
		{0xDEADBEEF, 16, storageunit.UnitID(0xF)},   // low nibble for N=16
	}
	for _, c := range cases {
		uc := storageunit.MustUnitCount(c.n)
		if got := storageunit.UnitForHash(c.hash, uc); got != c.want {
			t.Errorf("UnitForHash(%#x, N=%d) = %d, want %d", uint64(c.hash), c.n, got, c.want)
		}
	}
}

// TestColocation_SameHashSameUnit is the co-location-preservation property:
// any two keys that produce the same ShardHash MUST share a unit, so a
// co-located shard never straddles two databases.
func TestColocation_SameHashSameUnit(t *testing.T) {
	c := storageunit.MustUnitCount(64)
	// Keys sharing a Redis-style {hash-tag} extract to the same shard key,
	// hence the same hash, hence the same unit.
	tagged := [][]byte{
		[]byte("{alice}/profile"),
		[]byte("{alice}/pastes/abc"),
		[]byte("{alice}/versions/abc/v2"),
		[]byte("prefix{alice}suffix"),
	}
	var want storageunit.UnitID
	for i, k := range tagged {
		// Route by the SAME shard key the ring routes by.
		u := storageunit.UnitForShardKey(ring.ShardKey(k), c)
		if i == 0 {
			want = u
			continue
		}
		if u != want {
			t.Fatalf("co-located key %q landed on unit %d, want %d (same hash tag must share a unit)", k, u, want)
		}
	}
}

// TestUnitMap_AlignsWithRingHasher proves the unit map and the ring start
// from the SAME hash function. If pkg/ring ever swaps its hasher, this test
// (which hard-codes xxhash here) keeps the unit placement honest by failing
// when the two diverge.
func TestUnitMap_AlignsWithRingHasher(t *testing.T) {
	c := storageunit.MustUnitCount(32)
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("{user%d}/payload/%d", i, i))
		shardKey := ring.ShardKey(key)
		// The hash the ring uses on the shard key.
		ringHash := xxhash.Sum64(shardKey)
		want := storageunit.UnitID(ringHash & 31)
		got := storageunit.UnitForShardKey(shardKey, c)
		if got != want {
			t.Fatalf("unit map diverged from ring hasher for key %q: got %d want %d", key, got, want)
		}
	}
}

// TestUnitForHash_DistributesAcrossUnits guards against a degenerate map
// (e.g. one that always returns 0). With a uniform hash and many keys, every
// unit should receive at least one key.
func TestUnitForHash_DistributesAcrossUnits(t *testing.T) {
	c := storageunit.MustUnitCount(16)
	seen := make(map[storageunit.UnitID]int)
	for i := 0; i < 100000; i++ {
		u := storageunit.UnitForShardKey([]byte(fmt.Sprintf("key-%d", i)), c)
		seen[u]++
	}
	if len(seen) != 16 {
		t.Fatalf("expected keys to spread across all 16 units, got %d distinct (%v)", len(seen), seen)
	}
}
