// Package memory provides an in-process map-backed Backend. Used by
// tests + dev tools. Not durable - every restart loses data. Not
// shardable on its own; the cluster layer handles sharding by
// instantiating one Memory backend per node.
package memory

import (
	"bytes"
	"sort"
	"sync"

	"github.com/Zamua/shale/pkg/backend"
)

// Memory is the simplest possible Backend. Stores everything in a
// single in-process map, guarded by a sync.RWMutex. Transactions are
// implemented via a copy-on-write overlay (cheap enough for the
// scales we test at).
type Memory struct {
	mu     sync.RWMutex
	data   map[string][]byte
	closed bool
}

// New returns an empty Memory backend.
func New() *Memory { return &Memory{data: make(map[string][]byte)} }

func (m *Memory) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return backend.ErrClosed
	}
	m.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (m *Memory) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, backend.ErrClosed
	}
	v, ok := m.data[string(key)]
	if !ok {
		return nil, backend.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (m *Memory) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return backend.ErrClosed
	}
	delete(m.data, string(key))
	return nil
}

func (m *Memory) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, backend.ErrClosed
	}
	// Snapshot matching keys + values under the read lock so the
	// iterator doesn't need to hold the lock for its lifetime.
	var pairs []kv
	for k, v := range m.data {
		if bytes.HasPrefix([]byte(k), prefix) {
			pairs = append(pairs, kv{
				k: []byte(k),
				v: append([]byte(nil), v...),
			})
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return bytes.Compare(pairs[i].k, pairs[j].k) < 0 })
	return &iterator{pairs: pairs}, nil
}

func (m *Memory) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, backend.ErrClosed
	}
	return &transaction{
		parent:  m,
		writes:  make(map[string][]byte),
		deletes: make(map[string]struct{}),
	}, nil
}

func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	m.data = nil
	return nil
}

// -- iterator -------------------------------------------------------

type kv struct{ k, v []byte }

type iterator struct {
	pairs []kv
	pos   int
}

func (it *iterator) Next() ([]byte, []byte, error) {
	if it.pos >= len(it.pairs) {
		return nil, nil, nil
	}
	p := it.pairs[it.pos]
	it.pos++
	return p.k, p.v, nil
}

func (it *iterator) Close() error { it.pairs = nil; return nil }

// -- transaction ----------------------------------------------------

// Transaction is a simple optimistic overlay: writes + deletes are
// buffered, Commit flushes them under a single Lock, Rollback drops
// them. Snapshot isolation is enforced by reading the parent's data
// at the time of each tx.Get (the parent is locked for that read).
type transaction struct {
	parent  *Memory
	writes  map[string][]byte
	deletes map[string]struct{}
	done    bool
}

func (t *transaction) Get(key []byte) ([]byte, error) {
	if t.done {
		return nil, backend.ErrClosed
	}
	k := string(key)
	if _, deleted := t.deletes[k]; deleted {
		return nil, backend.ErrNotFound
	}
	if v, ok := t.writes[k]; ok {
		return append([]byte(nil), v...), nil
	}
	return t.parent.Get(key)
}

func (t *transaction) Put(key, value []byte) error {
	if t.done {
		return backend.ErrClosed
	}
	k := string(key)
	delete(t.deletes, k)
	t.writes[k] = append([]byte(nil), value...)
	return nil
}

func (t *transaction) Delete(key []byte) error {
	if t.done {
		return backend.ErrClosed
	}
	k := string(key)
	delete(t.writes, k)
	t.deletes[k] = struct{}{}
	return nil
}

func (t *transaction) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if t.done {
		return nil, backend.ErrClosed
	}
	// Merge the parent's prefix scan with the transaction's overlay.
	base, err := t.parent.ScanPrefix(prefix)
	if err != nil {
		return nil, err
	}
	var pairs []kv
	for {
		k, v, err := base.Next()
		if err != nil {
			return nil, err
		}
		if k == nil {
			break
		}
		sk := string(k)
		if _, deleted := t.deletes[sk]; deleted {
			continue
		}
		if overlay, ok := t.writes[sk]; ok {
			pairs = append(pairs, kv{k: k, v: overlay})
			continue
		}
		pairs = append(pairs, kv{k: k, v: v})
	}
	_ = base.Close()
	// Pull in any writes the parent didn't have (new keys in this tx).
	for sk, v := range t.writes {
		if !bytes.HasPrefix([]byte(sk), prefix) {
			continue
		}
		seen := false
		for _, p := range pairs {
			if string(p.k) == sk {
				seen = true
				break
			}
		}
		if !seen {
			pairs = append(pairs, kv{k: []byte(sk), v: v})
		}
	}
	sort.Slice(pairs, func(i, j int) bool { return bytes.Compare(pairs[i].k, pairs[j].k) < 0 })
	return &iterator{pairs: pairs}, nil
}

func (t *transaction) Commit() error {
	if t.done {
		return backend.ErrClosed
	}
	t.done = true
	t.parent.mu.Lock()
	defer t.parent.mu.Unlock()
	if t.parent.closed {
		return backend.ErrClosed
	}
	for k, v := range t.writes {
		t.parent.data[k] = v
	}
	for k := range t.deletes {
		delete(t.parent.data, k)
	}
	return nil
}

func (t *transaction) Rollback() error {
	t.done = true
	t.writes = nil
	t.deletes = nil
	return nil
}
