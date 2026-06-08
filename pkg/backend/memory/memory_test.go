package memory_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
)

func TestPutGetDelete(t *testing.T) {
	m := memory.New()
	t.Cleanup(func() { _ = m.Close() })

	if err := m.Put([]byte("foo"), []byte("bar")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := m.Get([]byte("foo"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, []byte("bar")) {
		t.Fatalf("got %q want bar", got)
	}
	if err := m.Delete([]byte("foo")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := m.Get([]byte("foo")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestScanPrefix(t *testing.T) {
	m := memory.New()
	t.Cleanup(func() { _ = m.Close() })

	_ = m.Put([]byte("user:alice"), []byte("a"))
	_ = m.Put([]byte("user:bob"), []byte("b"))
	_ = m.Put([]byte("post:1"), []byte("p"))

	it, err := m.ScanPrefix([]byte("user:"))
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	got := map[string]string{}
	for {
		k, v, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if k == nil {
			break
		}
		got[string(k)] = string(v)
	}
	if len(got) != 2 || got["user:alice"] != "a" || got["user:bob"] != "b" {
		t.Fatalf("unexpected scan results: %v", got)
	}
}

func TestTransaction_CommitAndRollback(t *testing.T) {
	m := memory.New()
	t.Cleanup(func() { _ = m.Close() })
	_ = m.Put([]byte("a"), []byte("1"))

	// Commit path: tx writes "b", commits, both keys readable on parent.
	tx, _ := m.Begin(backend.SnapshotIsolation)
	_ = tx.Put([]byte("b"), []byte("2"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	v, _ := m.Get([]byte("b"))
	if !bytes.Equal(v, []byte("2")) {
		t.Fatalf("post-commit b should be visible, got %q", v)
	}

	// Rollback path: tx writes "c", rolls back, parent unchanged.
	tx2, _ := m.Begin(backend.SnapshotIsolation)
	_ = tx2.Put([]byte("c"), []byte("3"))
	_ = tx2.Rollback()
	if _, err := m.Get([]byte("c")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("rolled-back c should be absent, got err=%v", err)
	}
}

func TestTransaction_ReadOverlay(t *testing.T) {
	m := memory.New()
	t.Cleanup(func() { _ = m.Close() })
	_ = m.Put([]byte("k"), []byte("parent"))

	tx, _ := m.Begin(backend.SnapshotIsolation)
	_ = tx.Put([]byte("k"), []byte("tx-write"))

	// tx.Get sees tx's own overlay write.
	got, _ := tx.Get([]byte("k"))
	if string(got) != "tx-write" {
		t.Fatalf("tx.Get should see overlay, got %q", got)
	}
	// parent.Get unaffected until commit.
	parentVal, _ := m.Get([]byte("k"))
	if string(parentVal) != "parent" {
		t.Fatalf("parent should be unaffected pre-commit, got %q", parentVal)
	}
}

func TestClosed(t *testing.T) {
	m := memory.New()
	_ = m.Close()
	if err := m.Put([]byte("x"), []byte("y")); !errors.Is(err, backend.ErrClosed) {
		t.Fatalf("Put on closed should be ErrClosed, got %v", err)
	}
}
