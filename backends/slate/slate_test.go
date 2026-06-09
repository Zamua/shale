//go:build slatedb

package slate_test

import (
	"bytes"
	"errors"
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"

	"github.com/Zamua/shale/backends/slate"
	"github.com/Zamua/shale/pkg/backend"
)

// openTestSlate opens a Slate backed by an in-memory object store
// (SlateDB's "memory:///" backend, no S3/MinIO required). Cleans up
// the backend on test teardown.
func openTestSlate(t *testing.T) *slate.Slate {
	t.Helper()
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve memory store: %v", err)
	}
	s, err := slate.NewWithStore("shale-test", store)
	if err != nil {
		store.Destroy()
		t.Fatalf("open slate: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPutGetDelete(t *testing.T) {
	s := openTestSlate(t)

	if err := s.Put([]byte("foo"), []byte("bar")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := s.Get([]byte("foo"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, []byte("bar")) {
		t.Fatalf("got %q want bar", got)
	}

	if err := s.Delete([]byte("foo")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.Get([]byte("foo")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestGet_AbsentKeyIsNotFound(t *testing.T) {
	s := openTestSlate(t)

	if _, err := s.Get([]byte("missing")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("expected ErrNotFound on absent key, got %v", err)
	}
}

func TestScanPrefix(t *testing.T) {
	s := openTestSlate(t)

	_ = s.Put([]byte("user:alice"), []byte("a"))
	_ = s.Put([]byte("user:bob"), []byte("b"))
	_ = s.Put([]byte("post:1"), []byte("p"))

	it, err := s.ScanPrefix([]byte("user:"))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer it.Close()

	got := map[string]string{}
	for {
		k, v, err := it.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
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

func TestTransaction_Commit(t *testing.T) {
	s := openTestSlate(t)
	_ = s.Put([]byte("a"), []byte("1"))

	tx, err := s.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("tx put: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	v, err := s.Get([]byte("b"))
	if err != nil {
		t.Fatalf("post-commit get: %v", err)
	}
	if !bytes.Equal(v, []byte("2")) {
		t.Fatalf("post-commit b = %q, want 2", v)
	}
}

func TestTransaction_Rollback(t *testing.T) {
	s := openTestSlate(t)

	tx, err := s.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("tx put: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if _, err := s.Get([]byte("c")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("rolled-back c should be absent, got err=%v", err)
	}
}

func TestTransaction_GetSeesOwnWrites(t *testing.T) {
	s := openTestSlate(t)
	_ = s.Put([]byte("k"), []byte("parent"))

	tx, err := s.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := tx.Put([]byte("k"), []byte("tx-write")); err != nil {
		t.Fatalf("tx put: %v", err)
	}
	got, err := tx.Get([]byte("k"))
	if err != nil {
		t.Fatalf("tx get: %v", err)
	}
	if string(got) != "tx-write" {
		t.Fatalf("tx.Get should see own write, got %q", got)
	}
}

func TestClosed(t *testing.T) {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	s, err := slate.NewWithStore("shale-closed", store)
	if err != nil {
		store.Destroy()
		t.Fatalf("open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := s.Put([]byte("x"), []byte("y")); !errors.Is(err, backend.ErrClosed) {
		t.Fatalf("Put on closed should be ErrClosed, got %v", err)
	}
	// Close is idempotent.
	if err := s.Close(); err != nil {
		t.Fatalf("second Close should be nil, got %v", err)
	}
}
