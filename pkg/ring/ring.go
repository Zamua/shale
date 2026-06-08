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
}

// New returns an empty Ring. Add Members before calling LocateKey;
// locating against an empty Ring panics in consistent.LocateKey, so
// callers should guard with Empty().
func New() *Ring {
	return &Ring{
		hash: consistent.New(nil, consistent.Config{
			PartitionCount:    partitionCount,
			ReplicationFactor: replicationFactor,
			Load:              loadFactor,
			Hasher:            xxhasher{},
		}),
		members: make(map[string]Member),
	}
}

// Add inserts m into the ring, or replaces the existing entry with
// the same ID (e.g. when a node's gRPC Addr changes after restart).
// Replacement is a Remove + Add at the consistent layer so ownership
// is recomputed.
func (r *Ring) Add(m Member) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[m.ID]; ok {
		r.hash.Remove(m.ID)
	}
	r.members[m.ID] = m
	r.hash.Add(m)
}

// Remove deletes the Member with the given ID. No-op if absent.
func (r *Ring) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[id]; !ok {
		return
	}
	delete(r.members, id)
	r.hash.Remove(id)
}

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
