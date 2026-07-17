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

// Flusher is an OPTIONAL capability a Backend may implement: force all
// in-memory write state (e.g. an LSM memtable) durable to the backing
// store NOW, without closing the backend. The cluster layer uses it
// best-effort at handoff edges (the displacement flush, see docs/SPEC.md
// "Displacement flush") so a successor's open recovers a minimal WAL
// tail; a Backend that does not implement it is skipped silently.
//
// Contract: Flush is safe to call concurrently with reads and writes,
// never loses or reorders an acked write, and returns an error when the
// flush cannot complete (the writer was fenced by a higher-epoch owner,
// or the backend is closed). Callers treat any error as "no flush
// happened" - an optimization miss, never data loss.
type Flusher interface {
	Flush() error
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

// CASResult is the outcome of an owner-side validate-and-apply, mirroring
// the CommitCASResponse wire shape. A Conflict is NOT an Err (it is the
// expected OCC retry signal), and a successful apply sets Committed.
//
// Committed is the load-bearing discriminator. It is set the instant the
// owner-local Commit succeeds, BEFORE the R>1 replica fan-out runs, so a
// fan-out that misses W cannot un-commit the durable owner-local write:
// that outcome is {Committed: true, UnderReplicated: true} with Err
// carrying the under-W error for logging/observability ONLY. A consumer
// mapping CASResult to an error MUST treat any Committed==true result as
// success (returning nil) even when UnderReplicated / Err is set - the
// write is durable and must never be retried away. Only a NOT-committed
// failure (Committed==false, Err set) is a genuine error the caller may
// retry (a pre-commit freeze / fence / reshard-cutover refusal, which
// applied nothing). See docs/SPEC.md "The four commit outcomes".
type CASResult struct {
	// Committed is true once the owner-local Commit durably applied the
	// write-set (R=1) or the owner-local leg of an R>1 commit did.
	Committed bool
	// Conflict is true when a read-check failed pre-commit (nothing applied).
	Conflict bool
	// UnderReplicated is true only alongside Committed==true: the owner-local
	// commit is durable but the replica fan-out collected fewer than W acks.
	// A success with degraded replication, NOT a failure. Always false at R=1
	// (no fan-out) and false for a fully-replicated commit.
	UnderReplicated bool
	// Err carries a failure when Committed==false; when Committed==true it is
	// non-nil only to preserve the under-W fan-out error for logging.
	Err error
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
