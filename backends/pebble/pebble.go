// Package pebble implements backend.Backend on top of Pebble,
// CockroachDB's pure-Go LSM-tree key-value engine. Pebble is local-disk
// only (no object storage, no replication of its own); use this backend
// when each shale node has durable local storage and you want a fast,
// embedded, cgo-free durability story.
//
// Build/runtime requirements: pure Go, no build tags, no native libs.
// `go build ./...` always includes this package.
//
// # Transaction semantics
//
// Pebble itself ships a Batch (atomic write-only buffer) and a Snapshot
// (point-in-time read view) but not a single read-write transaction
// object. backend.Begin asks for SnapshotIsolation, so we compose the
// two:
//
//   - A pebble.Snapshot is taken at Begin time. All reads (Get,
//     ScanPrefix) on the transaction consult this snapshot, so the
//     transaction sees a stable view of the DB regardless of what
//     other writers commit in the interim.
//   - A pebble.Batch buffers Put/Delete operations. The transaction's
//     own reads consult the batch overlay first (read-your-own-writes)
//     and fall back to the snapshot for keys the batch hasn't touched.
//   - Commit applies the batch atomically to the DB.
//   - Rollback drops both the batch and the snapshot.
//
// Concurrent transactions are independent: each holds its own snapshot
// + batch, and Commit is last-writer-wins per key (no conflict
// detection). This is honest snapshot isolation, not serializable;
// callers that need write-skew detection should ask for
// SerializableSnapshot (which we reject up-front so they don't get a
// silent downgrade).
package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/Zamua/shale/pkg/backend"
)

// Config is the open-time configuration for a Pebble backend.
type Config struct {
	// Dir is the on-disk data directory. Pebble creates it (and any
	// missing parents) on first open and reuses it on subsequent
	// opens. Required.
	Dir string

	// Options is forwarded verbatim to pebble.Open. Nil = pebble
	// defaults (the same &pebble.Options{} pebble.New used pre-v0.5).
	//
	// The shale layer never reads, mutates, copies, or validates this
	// value: operators get pebble's full knob surface (cache sizing,
	// WAL configuration, compaction tuning, event listeners, ...) with
	// type-checked field access. Any combination pebble rejects
	// surfaces from pebble.Open before shale sees the backend.
	//
	// See https://pkg.go.dev/github.com/cockroachdb/pebble#Options for
	// the field reference.
	Options *pebbledb.Options
}

// Pebble is a Pebble-backed Backend. It owns the underlying *pebble.DB
// and releases it on Close.
type Pebble struct {
	db *pebbledb.DB
}

// New opens a Pebble database at cfg.Dir. The caller must Close the
// returned *Pebble to flush the WAL and release the directory lock.
//
// If cfg.Options is non-nil, it is forwarded to pebble.Open verbatim
// (pass-through, no merging with shale defaults). Nil falls back to
// pebble's own defaults via an empty &pebble.Options{}.
func New(cfg Config) (*Pebble, error) {
	if cfg.Dir == "" {
		return nil, errors.New("pebble: Dir required")
	}
	opts := cfg.Options
	if opts == nil {
		opts = &pebbledb.Options{}
	}
	db, err := pebbledb.Open(cfg.Dir, opts)
	if err != nil {
		return nil, fmt.Errorf("pebble: open %q: %w", cfg.Dir, err)
	}
	return &Pebble{db: db}, nil
}

// Put stores value under key.
func (p *Pebble) Put(key, value []byte) error {
	if p.db == nil {
		return backend.ErrClosed
	}
	if err := p.db.Set(key, value, pebbledb.NoSync); err != nil {
		return fmt.Errorf("pebble: put: %w", err)
	}
	return nil
}

// Get returns the value for key, or backend.ErrNotFound if absent.
// Pebble signals "not present" with its own pebble.ErrNotFound; we
// translate that into the backend's sentinel so callers can rely on
// errors.Is(err, backend.ErrNotFound).
func (p *Pebble) Get(key []byte) ([]byte, error) {
	if p.db == nil {
		return nil, backend.ErrClosed
	}
	val, closer, err := p.db.Get(key)
	if err != nil {
		if errors.Is(err, pebbledb.ErrNotFound) {
			return nil, backend.ErrNotFound
		}
		return nil, fmt.Errorf("pebble: get: %w", err)
	}
	// Pebble owns the returned slice's backing storage until closer
	// is closed; copy it out so the caller can hold it indefinitely.
	out := append([]byte(nil), val...)
	_ = closer.Close()
	return out, nil
}

// Delete removes key. Idempotent at the Pebble level too.
func (p *Pebble) Delete(key []byte) error {
	if p.db == nil {
		return backend.ErrClosed
	}
	if err := p.db.Delete(key, pebbledb.NoSync); err != nil {
		return fmt.Errorf("pebble: delete: %w", err)
	}
	return nil
}

// ScanPrefix returns an iterator over all keys starting with prefix, in
// key-ascending order. Pebble's prefix-scan idiom is an iterator with
// LowerBound=prefix and UpperBound=prefix++ (the smallest byte string
// strictly greater than prefix). Caller MUST Close the iterator.
func (p *Pebble) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if p.db == nil {
		return nil, backend.ErrClosed
	}
	it, err := p.db.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("pebble: scan prefix: %w", err)
	}
	it.First()
	return &iterator{it: it}, nil
}

// Begin starts a transaction at the given isolation level. Only
// SnapshotIsolation is supported; SerializableSnapshot is rejected
// up-front so callers don't get a silent downgrade.
func (p *Pebble) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	if p.db == nil {
		return nil, backend.ErrClosed
	}
	if level != backend.SnapshotIsolation {
		return nil, fmt.Errorf("pebble: isolation level %d not supported (only SnapshotIsolation)", level)
	}
	return &transaction{
		db:      p.db,
		snap:    p.db.NewSnapshot(),
		batch:   p.db.NewBatch(),
		writes:  make(map[string][]byte),
		deletes: make(map[string]struct{}),
	}, nil
}

// Close releases all resources. Pebble flushes the WAL on Close, so a
// graceful shutdown loses no committed writes. Idempotent.
func (p *Pebble) Close() error {
	if p.db == nil {
		return nil
	}
	db := p.db
	p.db = nil
	if err := db.Close(); err != nil {
		return fmt.Errorf("pebble: close: %w", err)
	}
	return nil
}

// prefixUpperBound returns the smallest byte string strictly greater
// than prefix, or nil if prefix is empty or all-0xFF (in which case
// "scan everything from prefix onward" is the correct semantics, which
// pebble represents as a nil upper bound).
func prefixUpperBound(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xff {
			out := make([]byte, i+1)
			copy(out, prefix[:i+1])
			out[i]++
			return out
		}
	}
	return nil
}

// -- iterator -------------------------------------------------------

// iterator wraps a *pebble.Iterator and translates its
// Valid/Next/Key/Value cursor protocol into the backend.Iterator
// "Next returns nil,nil on exhaustion" contract.
type iterator struct {
	it      *pebbledb.Iterator
	started bool
}

func (i *iterator) Next() ([]byte, []byte, error) {
	if i.it == nil {
		return nil, nil, nil
	}
	// The constructor calls First() once so the very first Next()
	// returns the first entry instead of advancing past it.
	if !i.started {
		i.started = true
	} else {
		i.it.Next()
	}
	if !i.it.Valid() {
		if err := i.it.Error(); err != nil {
			return nil, nil, fmt.Errorf("pebble: scan next: %w", err)
		}
		return nil, nil, nil
	}
	// Pebble's Key/Value slices are invalidated by the next cursor
	// move; copy so the caller can hold them across iteration steps.
	key := append([]byte(nil), i.it.Key()...)
	val := append([]byte(nil), i.it.Value()...)
	return key, val, nil
}

func (i *iterator) Close() error {
	if i.it == nil {
		return nil
	}
	it := i.it
	i.it = nil
	if err := it.Close(); err != nil {
		return fmt.Errorf("pebble: scan close: %w", err)
	}
	return nil
}

// -- transaction ----------------------------------------------------

// transaction is a Pebble-snapshot + write-batch pair. Reads consult
// the local write overlay first (read-your-own-writes), then fall
// back to the snapshot. Writes accumulate in the batch and are
// applied atomically by Commit.
type transaction struct {
	db      *pebbledb.DB
	snap    *pebbledb.Snapshot
	batch   *pebbledb.Batch
	writes  map[string][]byte
	deletes map[string]struct{}
	done    bool
}

func (t *transaction) Get(key []byte) ([]byte, error) {
	if t.done || t.snap == nil {
		return nil, backend.ErrClosed
	}
	sk := string(key)
	if _, deleted := t.deletes[sk]; deleted {
		return nil, backend.ErrNotFound
	}
	if v, ok := t.writes[sk]; ok {
		return append([]byte(nil), v...), nil
	}
	val, closer, err := t.snap.Get(key)
	if err != nil {
		if errors.Is(err, pebbledb.ErrNotFound) {
			return nil, backend.ErrNotFound
		}
		return nil, fmt.Errorf("pebble: tx get: %w", err)
	}
	out := append([]byte(nil), val...)
	_ = closer.Close()
	return out, nil
}

func (t *transaction) Put(key, value []byte) error {
	if t.done || t.batch == nil {
		return backend.ErrClosed
	}
	sk := string(key)
	delete(t.deletes, sk)
	t.writes[sk] = append([]byte(nil), value...)
	if err := t.batch.Set(key, value, nil); err != nil {
		return fmt.Errorf("pebble: tx put: %w", err)
	}
	return nil
}

func (t *transaction) Delete(key []byte) error {
	if t.done || t.batch == nil {
		return backend.ErrClosed
	}
	sk := string(key)
	delete(t.writes, sk)
	t.deletes[sk] = struct{}{}
	if err := t.batch.Delete(key, nil); err != nil {
		return fmt.Errorf("pebble: tx delete: %w", err)
	}
	return nil
}

func (t *transaction) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if t.done || t.snap == nil {
		return nil, backend.ErrClosed
	}
	// Snapshot the matching range from the underlying snapshot,
	// then overlay the transaction's pending writes/deletes. We
	// materialize the merged result up-front so the returned
	// iterator doesn't have to hold pebble resources across
	// arbitrary Next() pauses.
	upper := prefixUpperBound(prefix)
	it, err := t.snap.NewIter(&pebbledb.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("pebble: tx scan prefix: %w", err)
	}
	defer func() { _ = it.Close() }()

	pairs := make(map[string][]byte)
	for it.First(); it.Valid(); it.Next() {
		k := append([]byte(nil), it.Key()...)
		v := append([]byte(nil), it.Value()...)
		pairs[string(k)] = v
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("pebble: tx scan next: %w", err)
	}

	// Apply overlay: writes win, deletes drop.
	for sk := range t.deletes {
		if bytes.HasPrefix([]byte(sk), prefix) {
			delete(pairs, sk)
		}
	}
	for sk, v := range t.writes {
		if bytes.HasPrefix([]byte(sk), prefix) {
			pairs[sk] = append([]byte(nil), v...)
		}
	}

	return newMemIterator(pairs), nil
}

func (t *transaction) Commit() error {
	if t.done || t.batch == nil {
		return backend.ErrClosed
	}
	t.done = true
	batch := t.batch
	snap := t.snap
	t.batch = nil
	t.snap = nil

	commitErr := batch.Commit(pebbledb.NoSync)
	// Always release the snapshot, even if commit failed; otherwise
	// pebble holds on to sequence numbers indefinitely.
	if snap != nil {
		_ = snap.Close()
	}
	if commitErr != nil {
		return fmt.Errorf("pebble: tx commit: %w", commitErr)
	}
	return nil
}

func (t *transaction) Rollback() error {
	if t.done {
		return nil
	}
	t.done = true
	var firstErr error
	if t.batch != nil {
		if err := t.batch.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("pebble: tx rollback batch: %w", err)
		}
		t.batch = nil
	}
	if t.snap != nil {
		if err := t.snap.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("pebble: tx rollback snapshot: %w", err)
		}
		t.snap = nil
	}
	t.writes = nil
	t.deletes = nil
	return firstErr
}

// memIterator is a simple in-memory iterator backed by a sorted slice
// of (key, value) pairs. Used by the transaction's ScanPrefix after
// the merge with the write overlay.
type memIterator struct {
	keys []string
	vals [][]byte
	pos  int
}

func newMemIterator(pairs map[string][]byte) *memIterator {
	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	vals := make([][]byte, len(keys))
	for i, k := range keys {
		vals[i] = pairs[k]
	}
	return &memIterator{keys: keys, vals: vals}
}

func (m *memIterator) Next() ([]byte, []byte, error) {
	if m.pos >= len(m.keys) {
		return nil, nil, nil
	}
	k := []byte(m.keys[m.pos])
	v := m.vals[m.pos]
	m.pos++
	return k, v, nil
}

func (m *memIterator) Close() error {
	m.keys = nil
	m.vals = nil
	return nil
}

// Compile-time interface checks.
var (
	_ backend.Backend     = (*Pebble)(nil)
	_ backend.Iterator    = (*iterator)(nil)
	_ backend.Iterator    = (*memIterator)(nil)
	_ backend.Transaction = (*transaction)(nil)
	_ io.Closer           = (*Pebble)(nil)
)
