// Package slate implements backend.Backend on top of SlateDB, an
// embedded LSM-tree KV store with object-storage durability. Use this
// backend when you want durable, replication-friendly KV storage that
// lives on S3-compatible object storage (AWS S3, MinIO, R2, GCS via
// S3-compat, etc.) rather than local disk.
//
// Build/runtime requirements: this package is gated behind the
// `slatedb` build tag because it depends on the cgo-backed
// slatedb.io/slatedb-go binding. The default `go build ./...` (with
// CGO_ENABLED=0) excludes this package so consumers who only want the
// memory backend pay no cgo cost. To build with this backend:
//
//	CGO_ENABLED=1 \
//	CGO_LDFLAGS="-L/path/to/slatedb/target/release" \
//	DYLD_LIBRARY_PATH=/path/to/slatedb/target/release \
//	go build -tags slatedb ./...
//
// # Object store configuration
//
// SlateDB's underlying OpenDAL/object_store crate reads AWS_* process
// env vars; it does not honor URL query parameters. New() writes the
// relevant env vars from Config before calling ObjectStoreResolve.
// Don't open two Slate backends pointing at different buckets in the
// same process; the env-var writes would collide.

//go:build slatedb

package slate

import (
	"errors"
	"fmt"

	slatedb "slatedb.io/slatedb-go/uniffi"

	"github.com/Zamua/shale/pkg/backend"
)

// Slate is a SlateDB-backed Backend. It owns the underlying *slatedb.Db
// and *slatedb.ObjectStore; both are released on Close.
type Slate struct {
	db    *slatedb.Db
	store *slatedb.ObjectStore
}

// New opens a SlateDB instance backed by the configured object store.
// The caller must Close() the returned *Slate to flush + shut down
// cleanly. See package doc for the env-var caveat around running two
// Slate instances in one process.
func New(cfg Config) (*Slate, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	cfg.applyEnv()

	url := "s3://" + cfg.Bucket + "/"
	store, err := slatedb.ObjectStoreResolve(url)
	if err != nil {
		return nil, fmt.Errorf("slate: resolve object store %q: %w", url, err)
	}
	builder := slatedb.NewDbBuilder(cfg.DbName, store)
	defer builder.Destroy()
	db, err := builder.Build()
	if err != nil {
		store.Destroy()
		return nil, fmt.Errorf("slate: open db %q: %w", cfg.DbName, err)
	}
	return &Slate{db: db, store: store}, nil
}

// NewWithStore opens a SlateDB instance against an already-resolved
// ObjectStore. Useful for tests that want to point at a non-S3 store
// (e.g. "memory:///") without touching the AWS_* env vars. The Slate
// instance takes ownership of the store and will Destroy it on Close.
func NewWithStore(dbName string, store *slatedb.ObjectStore) (*Slate, error) {
	if dbName == "" {
		return nil, errors.New("slate: dbName required")
	}
	if store == nil {
		return nil, errors.New("slate: store required")
	}
	builder := slatedb.NewDbBuilder(dbName, store)
	defer builder.Destroy()
	db, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("slate: open db %q: %w", dbName, err)
	}
	return &Slate{db: db, store: store}, nil
}

// Put stores value under key.
func (s *Slate) Put(key, value []byte) error {
	if s.db == nil {
		return backend.ErrClosed
	}
	if _, err := s.db.Put(key, value); err != nil {
		return fmt.Errorf("slate: put: %w", err)
	}
	return nil
}

// Get returns the value for key, or backend.ErrNotFound if absent.
// SlateDB signals "not present" by returning a nil *[]byte from Get;
// we translate that into the backend's sentinel.
func (s *Slate) Get(key []byte) ([]byte, error) {
	if s.db == nil {
		return nil, backend.ErrClosed
	}
	raw, err := s.db.Get(key)
	if err != nil {
		return nil, fmt.Errorf("slate: get: %w", err)
	}
	if raw == nil {
		return nil, backend.ErrNotFound
	}
	return append([]byte(nil), (*raw)...), nil
}

// Delete removes key. Idempotent at the SlateDB level too.
func (s *Slate) Delete(key []byte) error {
	if s.db == nil {
		return backend.ErrClosed
	}
	if _, err := s.db.Delete(key); err != nil {
		return fmt.Errorf("slate: delete: %w", err)
	}
	return nil
}

// ScanPrefix returns an iterator over all keys starting with prefix,
// in key-ascending order. Caller MUST Close the iterator to release
// the underlying SlateDB cursor.
func (s *Slate) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if s.db == nil {
		return nil, backend.ErrClosed
	}
	it, err := s.db.ScanPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("slate: scan prefix: %w", err)
	}
	return &iterator{it: it}, nil
}

// Begin starts a SlateDB transaction at snapshot isolation. SlateDB's
// only supported level today is SnapshotIsolation; serializable-snapshot
// is rejected up-front so callers don't get silent downgrades.
func (s *Slate) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	if s.db == nil {
		return nil, backend.ErrClosed
	}
	if level != backend.SnapshotIsolation {
		return nil, fmt.Errorf("slate: isolation level %d not supported (only SnapshotIsolation)", level)
	}
	tx, err := s.db.Begin(slatedb.IsolationLevelSnapshot)
	if err != nil {
		return nil, fmt.Errorf("slate: begin: %w", err)
	}
	return &transaction{tx: tx}, nil
}

// Close shuts the SlateDB down (flushing pending writes) and destroys
// the underlying object store handle. Idempotent.
func (s *Slate) Close() error {
	if s.db == nil {
		return nil
	}
	db := s.db
	store := s.store
	s.db = nil
	s.store = nil
	if err := db.Shutdown(); err != nil {
		if store != nil {
			store.Destroy()
		}
		return fmt.Errorf("slate: shutdown: %w", err)
	}
	if store != nil {
		store.Destroy()
	}
	return nil
}

// -- iterator -------------------------------------------------------

// iterator wraps a *slatedb.DbIterator, translating its (KeyValue,
// nil-on-exhaust) protocol to the backend.Iterator contract.
type iterator struct {
	it *slatedb.DbIterator
}

func (i *iterator) Next() ([]byte, []byte, error) {
	if i.it == nil {
		return nil, nil, nil
	}
	kv, err := i.it.Next()
	if err != nil {
		return nil, nil, fmt.Errorf("slate: scan next: %w", err)
	}
	if kv == nil {
		return nil, nil, nil
	}
	// Copy out of SlateDB-owned memory so the caller can hold these
	// past the next Next()/Close() call.
	key := append([]byte(nil), kv.Key...)
	val := append([]byte(nil), kv.Value...)
	return key, val, nil
}

func (i *iterator) Close() error {
	if i.it == nil {
		return nil
	}
	i.it.Destroy()
	i.it = nil
	return nil
}

// -- transaction ----------------------------------------------------

// transaction wraps a *slatedb.DbTransaction. Commit and Rollback are
// terminal; subsequent calls return backend.ErrClosed.
type transaction struct {
	tx   *slatedb.DbTransaction
	done bool
}

func (t *transaction) Get(key []byte) ([]byte, error) {
	if t.done || t.tx == nil {
		return nil, backend.ErrClosed
	}
	raw, err := t.tx.Get(key)
	if err != nil {
		return nil, fmt.Errorf("slate: tx get: %w", err)
	}
	if raw == nil {
		return nil, backend.ErrNotFound
	}
	return append([]byte(nil), (*raw)...), nil
}

func (t *transaction) Put(key, value []byte) error {
	if t.done || t.tx == nil {
		return backend.ErrClosed
	}
	if err := t.tx.Put(key, value); err != nil {
		return fmt.Errorf("slate: tx put: %w", err)
	}
	return nil
}

func (t *transaction) Delete(key []byte) error {
	if t.done || t.tx == nil {
		return backend.ErrClosed
	}
	if err := t.tx.Delete(key); err != nil {
		return fmt.Errorf("slate: tx delete: %w", err)
	}
	return nil
}

func (t *transaction) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if t.done || t.tx == nil {
		return nil, backend.ErrClosed
	}
	it, err := t.tx.ScanPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("slate: tx scan prefix: %w", err)
	}
	return &iterator{it: it}, nil
}

func (t *transaction) Commit() error {
	if t.done || t.tx == nil {
		return backend.ErrClosed
	}
	t.done = true
	if _, err := t.tx.Commit(); err != nil {
		return fmt.Errorf("slate: tx commit: %w", err)
	}
	return nil
}

func (t *transaction) Rollback() error {
	if t.done || t.tx == nil {
		return backend.ErrClosed
	}
	t.done = true
	if err := t.tx.Rollback(); err != nil {
		return fmt.Errorf("slate: tx rollback: %w", err)
	}
	return nil
}
