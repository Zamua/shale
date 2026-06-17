package storageunit

import (
	"errors"
	"strconv"
	"sync"
)

// ErrPrecondition is returned by a ConditionalStore when a conditional write's
// precondition fails: PutIfAbsent on a key that already exists, or
// CompareAndSet against a stale version. It is the decentralized reshard
// arbiter's "you lost the race, re-read and retry" signal. The slate adapter
// maps S3/MinIO HTTP 412 PreconditionFailed onto it.
var ErrPrecondition = errors.New("storageunit: conditional write precondition failed")

// ErrCondNotFound is returned by ConditionalStore.Get when the key is absent.
// (Named distinctly from any backend not-found so the arbiter can branch on it.)
var ErrCondNotFound = errors.New("storageunit: conditional-store object not found")

// ConditionalStore is the small shared-storage capability the decentralized
// reshard arbiter needs: atomic create-if-absent and compare-and-set on a
// named object, plus a plain read. It is how a cluster agrees on the reshard
// epoch and the per-unit cut-over markers WITHOUT an elected coordinator -
// advancing the epoch is a conditional-write race that exactly one writer wins,
// and every other node reads the winner.
//
// Like BackendFactory, it is an infrastructure SEAM. The slate backend
// implements it over MinIO/S3 conditional PutObject (If-None-Match / If-Match;
// minio-go SetMatchETagExcept / SetMatchETag; HTTP 412 -> ErrPrecondition). The
// in-process MemConditionalStore implements it for fast, slatedb/MinIO-free
// tests, so the entire agreement protocol is exercised in milliseconds.
//
// version is an opaque token (an S3 ETag for the real store) that changes on
// every successful write and is never reused for a given key; callers treat it
// as a compare token only, never parse it.
type ConditionalStore interface {
	// PutIfAbsent creates key holding data only if key does not already exist.
	// Returns the new version on success, or ErrPrecondition if it exists.
	PutIfAbsent(key string, data []byte) (version string, err error)

	// CompareAndSet overwrites key with data only if its current version equals
	// expectedVersion. Returns the new version on success, or ErrPrecondition if
	// the stored version differs (someone else advanced it, or key is absent).
	CompareAndSet(key string, data []byte, expectedVersion string) (version string, err error)

	// Get returns key's data and current version, or ErrCondNotFound if absent.
	Get(key string) (data []byte, version string, err error)
}

// MemConditionalStore is an in-process ConditionalStore backed by a
// mutex-guarded map with EXACT compare-and-set semantics. It is the fast-test
// double for the reshard arbiter: the agreement protocol (epoch advance,
// cut-over markers, the conditional-write race) runs against it with no
// slatedb, no MinIO, no network, while the slate/MinIO adapter is pinned
// separately by one integration test. Safe for concurrent use.
type MemConditionalStore struct {
	mu  sync.Mutex
	obj map[string]memObject
	seq uint64 // monotone, opaque version generator; tokens are never reused
}

type memObject struct {
	data    []byte
	version string
}

// NewMemConditionalStore returns an empty in-process ConditionalStore.
func NewMemConditionalStore() *MemConditionalStore {
	return &MemConditionalStore{obj: make(map[string]memObject)}
}

// nextVersion mints a fresh opaque version token. Caller holds m.mu.
func (m *MemConditionalStore) nextVersion() string {
	m.seq++
	return "v" + strconv.FormatUint(m.seq, 10)
}

// PutIfAbsent implements ConditionalStore: create key only if it is absent.
func (m *MemConditionalStore) PutIfAbsent(key string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.obj[key]; ok {
		return "", ErrPrecondition
	}
	v := m.nextVersion()
	m.obj[key] = memObject{data: append([]byte(nil), data...), version: v}
	return v, nil
}

// CompareAndSet implements ConditionalStore: overwrite key only if its current
// version equals expectedVersion.
func (m *MemConditionalStore) CompareAndSet(key string, data []byte, expectedVersion string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.obj[key]
	if !ok || cur.version != expectedVersion {
		return "", ErrPrecondition
	}
	v := m.nextVersion()
	m.obj[key] = memObject{data: append([]byte(nil), data...), version: v}
	return v, nil
}

// Get implements ConditionalStore: return key's data and current version.
func (m *MemConditionalStore) Get(key string) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.obj[key]
	if !ok {
		return nil, "", ErrCondNotFound
	}
	return append([]byte(nil), cur.data...), cur.version, nil
}
