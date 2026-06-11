package storageunit

import "github.com/cespare/xxhash/v2"

// ShardHash is the 64-bit hash of a key's SHARD KEY. It is the same value
// the ring hashes on (xxhash.Sum64 over ring.ShardKey(key)), surfaced as a
// named type so the unit map and the ring agree by construction: route a
// key with ring.LocateKey, place its bytes with UnitForHash, and both start
// from this one number.
//
// The cluster layer computes it ONCE per key (it already has the shard key
// in hand for ring routing) and threads it through both lookups. Callers
// that only have the raw shard-key bytes use HashShardKey.
type ShardHash uint64

// HashShardKey hashes shard-key bytes into a ShardHash using xxhash, the
// exact hasher pkg/ring uses (xxhasher.Sum64). Pass it the SHARD KEY (the
// hash-tag-extracted portion, ring.ShardKey(key)), NOT the raw key, or the
// unit placement will disagree with ring routing for hash-tagged keys.
//
// Kept as a thin wrapper so this package never reaches for a different hash
// function: drift between "where the ring routes" and "where the bytes
// live" is a co-location bug, and centralizing the hash here makes that
// drift impossible.
func HashShardKey(shardKey []byte) ShardHash {
	return ShardHash(xxhash.Sum64(shardKey))
}

// UnitForHash maps a shard hash to its storage UnitID under count c. Because
// N is a power of two this is the low log2(N) bits of the hash:
//
//	unit = hash mod N = hash & (N-1)
//
// Properties this pins (see unitmap_test.go):
//
//   - DETERMINISTIC: same (hash, N) always yields the same UnitID.
//   - CO-LOCATION-PRESERVING: two keys with the same ShardHash (e.g. keys
//     sharing a {hash-tag}) always land on the same unit, so a co-located
//     shard never straddles two databases.
//   - DOUBLING-COMPATIBLE: the unit under 2N is the unit under N plus one
//     higher bit (see ChildUnit). Low bits are stable across doublings.
//
// The result is always in [0, N), so it is a valid UnitID under c.
func UnitForHash(h ShardHash, c UnitCount) UnitID {
	return UnitID(uint64(h) & c.mask())
}

// UnitForShardKey is UnitForHash composed with HashShardKey: hash the shard
// key, then mask. Convenience for callers that have the shard-key bytes but
// not a precomputed ShardHash. The hot path (cluster routing) should prefer
// computing the ShardHash once and calling UnitForHash to avoid re-hashing.
func UnitForShardKey(shardKey []byte, c UnitCount) UnitID {
	return UnitForHash(HashShardKey(shardKey), c)
}
