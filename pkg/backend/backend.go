// Package backend defines the interface every shale-pluggable storage
// engine implements. The cluster layer (pkg/cluster) routes keys to
// the owning node + invokes Backend operations there; the Backend has
// no awareness of clustering. As far as the Backend is concerned, it
// owns a slice of the keyspace + exposes raw KV operations on it.
//
// Implementations shipped:
//   - pkg/backend/memory   - in-process map; tests + local dev
//   - backends/slate       - SlateDB-on-object-storage (separate Go module)
//   - backends/pebble      - Pebble local-disk LSM (separate Go module)
//
// BYO: implement this interface for any KV-shaped engine. The contract
// is deliberately minimal so it's hard to write a wrong implementation.
package backend

// Backend is the per-node storage engine. All operations are LOCAL to
// this node's slice of the keyspace - the cluster layer ensures keys
// are routed to the right Backend before any of these methods are
// called.
type Backend interface {
	// Put stores value under key. Overwrites any existing value.
	Put(key, value []byte) error

	// Get returns the value for key, or (nil, ErrNotFound) if absent.
	Get(key []byte) ([]byte, error)

	// Delete removes key. Idempotent - deleting an absent key is OK.
	Delete(key []byte) error

	// ScanPrefix iterates over all keys (in key-ascending order) that
	// start with prefix. Caller MUST close the returned Iterator.
	ScanPrefix(prefix []byte) (Iterator, error)

	// Begin starts a transaction at the given isolation level. The
	// transaction is local to this Backend - cluster.go's transaction
	// proxy ensures all keys touched in one transaction belong to the
	// same shard before delegating here.
	Begin(level IsolationLevel) (Transaction, error)

	// Close releases all resources. After Close, no other method may
	// be called. Idempotent.
	Close() error
}

// Iterator walks a scan result. Next returns successive (key, value)
// pairs in key-ascending order. When the scan is exhausted Next
// returns nil for both key and value (and no error). Close MUST be
// called when iteration is abandoned early.
type Iterator interface {
	Next() (key, value []byte, err error)
	Close() error
}

// Transaction is a Backend-local transaction. Operations within a
// transaction either all commit atomically or all roll back. The
// cluster layer rejects cross-shard transactions before they reach
// any Backend.
type Transaction interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	ScanPrefix(prefix []byte) (Iterator, error)
	Commit() error
	Rollback() error
}

// ReadCheck is one entry in a CAS (optimistic-concurrency) read-set: the
// value a client observed for a key at read time. The owner re-reads the
// key inside its commit transaction and compares. ExpectAbsent=true means
// the key must NOT exist (a found value is a conflict) and ExpectedVal is
// ignored; otherwise the current value must match ExpectedVal byte-for-
// byte. It is a pure value object: no I/O, no clustering awareness.
type ReadCheck struct {
	Key          []byte
	ExpectedVal  []byte
	ExpectAbsent bool
}

// WriteOp is one entry in a CAS write-set. Del=true deletes Key (Value is
// ignored); otherwise it Puts Value under Key. Pure value object.
type WriteOp struct {
	Key   []byte
	Value []byte
	Del   bool
}

// CASResult is the outcome of an owner-side validate-and-apply. Exactly
// one of Committed / Conflict / Err is meaningful, mirroring the
// CommitCASResponse wire shape: a Conflict is NOT an Err (it is the
// expected OCC retry signal), and a successful apply sets Committed.
type CASResult struct {
	Committed bool
	Conflict  bool
	Err       error
}

// IsolationLevel is the strongest visibility guarantee a transaction
// makes. Backends may implement stronger; never weaker.
type IsolationLevel int

const (
	// SnapshotIsolation makes reads see a consistent snapshot from
	// transaction begin time; writes don't conflict unless they
	// overlap on the same key.
	SnapshotIsolation IsolationLevel = iota
	// SerializableSnapshot is snapshot isolation plus write-skew
	// detection (full serializability).
	SerializableSnapshot
)
