// Package ring wraps buraksezer/consistent into a thin, shale-shaped
// abstraction: a set of cluster Members + a "which Member owns this
// key" lookup that honors Redis-style hash tags.
//
// The ring is pure logic - no I/O, no goroutines beyond the RWMutex
// that guards membership mutations. The cluster layer (pkg/cluster)
// owns wiring the ring to memberlist events + to gRPC forwarding;
// this package just answers ownership questions.
//
// Hashing model: ring-based consistent hashing with bounded loads
// (Karger 1997 + Mirrokni-Thorup 2017). Each Member gets
// ReplicationFactor virtual replicas on a 64-bit ring; a key hashes
// onto the ring + goes to the next clockwise virtual node. Adding or
// removing one Member moves ~K/N keys.
//
// See docs/SPEC.md ("Routing", "Shard keys + hash tags") for the
// full model.
package ring

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
)

// Tunables picked for small-to-medium clusters. The values match the
// Routing section of docs/SPEC.md: prime PartitionCount for good
// modulo distribution, ~40 virtual replicas per real node (the spec's
// "~64" rule of thumb rounded down so memory stays small), and the
// canonical 1.25 bounded-loads factor from Mirrokni-Thorup.
const (
	partitionCount    = 271
	replicationFactor = 40
	loadFactor        = 1.25
)

// Member is one node in the ring. ID is the cluster-unique node
// identity used for ring placement + Remove. Addr is the gRPC
// host:port the cluster layer dials when forwarding to this node.
type Member struct {
	ID   string
	Addr string
}

// String satisfies consistent.Member. We hash on the ID so that two
// processes computing the ring independently land on the same
// ownership map regardless of Addr (which can shift if a node is
// restarted on a new port).
func (m Member) String() string { return m.ID }

// xxhasher adapts cespare/xxhash to the consistent.Hasher interface.
type xxhasher struct{}

func (xxhasher) Sum64(data []byte) uint64 { return xxhash.Sum64(data) }

// Ring is a thread-safe wrapper around consistent.Consistent. Mutate
// it via Add/Remove; query it via LocateKey/Members/Empty.
type Ring struct {
	mu      sync.RWMutex
	hash    *consistent.Consistent
	members map[string]Member // id -> Member (for Addr lookup + snapshot)

	// rebuildPanics counts how many times rebuildHashLocked recovered a
	// panic out of the consistent-hash library (see rebuildHashLocked).
	// Observability only; a nonzero value means a membership change was
	// momentarily not reflected in routing (the prior ring was kept).
	rebuildPanics atomic.Uint64
}

// ringCfg is the consistent-hash configuration, shared by New + every
// rebuild so the ownership map is identical however the ring was built.
func ringCfg() consistent.Config {
	return consistent.Config{
		PartitionCount:    partitionCount,
		ReplicationFactor: replicationFactor,
		Load:              loadFactor,
		Hasher:            xxhasher{},
	}
}

// New returns an empty Ring. Add Members before calling LocateKey;
// locating against an empty Ring panics in consistent.LocateKey, so
// callers should guard with Empty().
func New() *Ring {
	return &Ring{
		hash:    consistent.New(nil, ringCfg()),
		members: make(map[string]Member),
	}
}

// rebuildHashLocked rebuilds the consistent hash FROM SCRATCH out of the
// authoritative member set (r.members). It deliberately does NOT use the
// library's incremental Remove: that path corrupts the library's internal
// ring/sortedSet under churn at higher member counts, leaving a sortedSet
// hash whose ring entry is nil so distributeWithLoad nil-derefs and crash-
// loops the whole process (#466: panic at consistent.go:194, observed at 12
// members). Building a fresh Consistent from the full member set never calls
// the buggy Remove, so the corruption cannot arise. A recover guards the
// build: if the library still panics on a pathological input, we KEEP THE
// PRIOR ring (the assignment below never ran) and let the next membership
// change retry, so a library bug degrades into brief routing staleness
// instead of a process crash. Caller holds r.mu.
func (r *Ring) rebuildHashLocked() {
	if len(r.members) == 0 {
		// consistent.New(nil, ...) is the known-safe empty ring; building
		// from an empty slice can fault inside the library.
		r.hash = consistent.New(nil, ringCfg())
		return
	}
	members := make([]consistent.Member, 0, len(r.members))
	for _, m := range r.members {
		members = append(members, m)
	}
	defer func() {
		if rec := recover(); rec != nil {
			// r.hash retains its prior (valid) value; r.members is already
			// correct, so the next Add/Remove re-attempts the rebuild.
			r.rebuildPanics.Add(1)
		}
	}()
	r.hash = consistent.New(members, ringCfg())
}

// Add inserts m into the ring, or replaces the existing entry with the same
// ID (e.g. when a node's gRPC Addr changes after restart). The ring is then
// rebuilt from the full member set (see rebuildHashLocked) so ownership is
// recomputed without touching the library's buggy incremental Remove.
func (r *Ring) Add(m Member) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.members[m.ID] = m
	r.rebuildHashLocked()
}

// Remove deletes the Member with the given ID. No-op if absent.
func (r *Ring) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[id]; !ok {
		return
	}
	delete(r.members, id)
	r.rebuildHashLocked()
}

// RebuildPanics returns the cumulative count of consistent-hash library
// panics recovered during ring rebuilds. Zero in healthy operation.
func (r *Ring) RebuildPanics() uint64 { return r.rebuildPanics.Load() }

// Members returns a snapshot of the current Members, sorted by ID for
// stable iteration (callers often log or diff this).
func (r *Ring) Members() []Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Member, 0, len(r.members))
	for _, m := range r.members {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Empty reports whether the ring has no Members. Callers should
// check this before LocateKey to avoid a panic from the underlying
// library when there is nothing to locate against.
func (r *Ring) Empty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.members) == 0
}

// LocateKey returns the Member that owns key. The hashed portion is
// extracted via ShardKey - keys containing a balanced "{...}" pair
// hash on the tag contents only, so callers can co-locate related
// keys (Redis hash-tag convention; see docs/SPEC.md).
func (r *Ring) LocateKey(key []byte) Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m := r.hash.LocateKey(ShardKey(key))
	// consistent.LocateKey returns a consistent.Member; we stored
	// Member{} so type-assert back to recover the Addr.
	return r.members[m.String()]
}

// LocateKeyN returns the primary owner of key followed by up to (n-1)
// successor Members on the ring, in primary-first order. Used by the
// v0.4 replication layer to pick the R nodes that hold a copy of each
// key.
//
// The hashed portion is the same ShardKey LocateKey uses, so hash
// tags ("{tag}") group keys onto the same primary + the same successor
// chain. n <= 0 returns an empty slice. n larger than the number of
// distinct Members returns ALL distinct Members (no duplicates); this
// keeps the caller from blocking on a phantom replica when the
// configured ReplicationFactor exceeds the live membership.
func (r *Ring) LocateKeyN(key []byte, n int) []Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n <= 0 || len(r.members) == 0 {
		return nil
	}
	want := min(n, len(r.members))
	raw, err := r.hash.GetClosestN(ShardKey(key), want)
	if err != nil {
		// GetClosestN's only documented failure is "count > members",
		// which we just clamped against; treat any other error as
		// "no replicas" so the caller falls through to its own
		// no-replicas handling.
		return nil
	}
	out := make([]Member, 0, len(raw))
	for _, m := range raw {
		out = append(out, r.members[m.String()])
	}
	return out
}

// PartitionID returns the partition id that key belongs to. This is
// the same partition the LocateKey lookup goes through; exposing it
// lets the rebalance layer talk about "which partition" without
// re-implementing the hash-to-partition math.
func (r *Ring) PartitionID(key []byte) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return uint64(r.hash.FindPartitionID(ShardKey(key)))
}

// Partitions returns the full set of partition ids the ring tracks,
// in ascending order. The set is fixed at ring construction time (it
// is derived from partitionCount, not from membership), so a caller
// can rely on the same slice shape across Add/Remove calls. Empty
// returns are reserved for the future case of a configurable
// partitionCount of zero; today the slice is always non-empty.
func (r *Ring) Partitions() []uint64 {
	out := make([]uint64, partitionCount)
	for i := range partitionCount {
		out[i] = uint64(i)
	}
	return out
}

// Owner returns the Member that owns the given partition id. If the
// ring has no Members (the partition table is empty) the zero Member
// is returned; callers should guard with Empty() before iterating.
// Partition ids outside the configured range also return the zero
// Member.
func (r *Ring) Owner(partitionID uint64) Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.members) == 0 {
		return Member{}
	}
	if partitionID >= uint64(partitionCount) {
		return Member{}
	}
	m := r.hash.GetPartitionOwner(int(partitionID))
	if m == nil {
		return Member{}
	}
	return r.members[m.String()]
}

// ReplicasForPartition returns the primary owner of partitionID followed
// by up to (n-1) successor Members on the ring, in primary-first order.
// It is the partition-level analogue of LocateKeyN: every key whose
// PartitionID is partitionID has exactly this replica set (the chain is a
// property of the partition, not the individual key), so the rebalance
// layer can decide replica membership per partition without a
// representative key.
//
// Clamping + error handling match LocateKeyN: n <= 0 or an empty ring
// returns nil; n larger than the live membership returns ALL distinct
// Members (no duplicates). partitionID out of range returns nil.
func (r *Ring) ReplicasForPartition(partitionID uint64, n int) []Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n <= 0 || len(r.members) == 0 {
		return nil
	}
	if partitionID >= uint64(partitionCount) {
		return nil
	}
	want := min(n, len(r.members))
	raw, err := r.hash.GetClosestNForPartition(int(partitionID), want)
	if err != nil {
		return nil
	}
	out := make([]Member, 0, len(raw))
	for _, m := range raw {
		out = append(out, r.members[m.String()])
	}
	return out
}

// ShardKey extracts the hashed portion of key per the hash-tag rule:
// if key contains a "{" followed by a "}", everything between the
// first "{" and the first "}" AFTER it is returned. Empty braces
// ("{}") and unmatched braces fall through to "hash the whole key" -
// this matches Redis Cluster's behavior + means apps that don't
// opt-in to tagging just get whole-key hashing.
//
// Exported so callers (memberlist event hooks, debug tools, tests)
// can ask "what would this key hash on" without invoking the ring.
func ShardKey(key []byte) []byte {
	open := -1
	for i, b := range key {
		if b == '{' {
			open = i
			break
		}
	}
	if open < 0 {
		return key
	}
	for j := open + 1; j < len(key); j++ {
		if key[j] == '}' {
			if j == open+1 {
				// "{}" - empty tag, fall through to whole-key.
				return key
			}
			return key[open+1 : j]
		}
	}
	return key
}
