package pebble_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	pebblebe "github.com/Zamua/shale/backends/pebble"
	"github.com/Zamua/shale/pkg/backend"
)

// openTemp opens a Pebble backend under t.TempDir and registers cleanup
// that both closes the DB and verifies the data directory has been
// removed (t.TempDir handles the rmdir; we just assert it actually
// happened so a regression in Close that leaks file handles surfaces).
func openTemp(t *testing.T) (*pebblebe.Pebble, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "pebble")
	p, err := pebblebe.New(pebblebe.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(func() {
		_ = p.Close()
		if _, err := os.Stat(dir); err == nil {
			// t.TempDir will rm the parent, but if the file lock
			// were still held this stat path would still exist
			// outside the temp tree. Belt-and-suspenders.
			_ = os.RemoveAll(dir)
		}
	})
	return p, dir
}

func TestPutGetDelete(t *testing.T) {
	p, _ := openTemp(t)

	if err := p.Put([]byte("foo"), []byte("bar")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := p.Get([]byte("foo"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, []byte("bar")) {
		t.Fatalf("got %q want bar", got)
	}
	if err := p.Delete([]byte("foo")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := p.Get([]byte("foo")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestDeleteAbsentKeyIsIdempotent(t *testing.T) {
	p, _ := openTemp(t)
	if err := p.Delete([]byte("never-existed")); err != nil {
		t.Fatalf("delete on absent key should be nil, got %v", err)
	}
}

func TestScanPrefix(t *testing.T) {
	p, _ := openTemp(t)

	_ = p.Put([]byte("user:alice"), []byte("a"))
	_ = p.Put([]byte("user:bob"), []byte("b"))
	_ = p.Put([]byte("user:carol"), []byte("c"))
	_ = p.Put([]byte("post:1"), []byte("p"))

	it, err := p.ScanPrefix([]byte("user:"))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer func() { _ = it.Close() }()

	var keys []string
	got := map[string]string{}
	for {
		k, v, err := it.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if k == nil {
			break
		}
		keys = append(keys, string(k))
		got[string(k)] = string(v)
	}
	if len(got) != 3 || got["user:alice"] != "a" || got["user:bob"] != "b" || got["user:carol"] != "c" {
		t.Fatalf("unexpected scan results: %v", got)
	}
	// Must be in key-ascending order.
	want := []string{"user:alice", "user:bob", "user:carol"}
	for i, k := range keys {
		if k != want[i] {
			t.Fatalf("scan order wrong at %d: got %q want %q (full %v)", i, k, want[i], keys)
		}
	}
}

func TestScanPrefixEmpty(t *testing.T) {
	p, _ := openTemp(t)
	it, err := p.ScanPrefix([]byte("nope:"))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer func() { _ = it.Close() }()
	k, _, err := it.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if k != nil {
		t.Fatalf("expected empty scan, got key %q", k)
	}
}

func TestTransaction_CommitAndRollback(t *testing.T) {
	p, _ := openTemp(t)
	_ = p.Put([]byte("a"), []byte("1"))

	// Commit path: tx writes "b", commits, both keys readable on parent.
	tx, err := p.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("tx.Put: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	v, err := p.Get([]byte("b"))
	if err != nil || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("post-commit b should be visible; got v=%q err=%v", v, err)
	}

	// Rollback path: tx writes "c", rolls back, parent unchanged.
	tx2, err := p.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx2.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("tx2.Put: %v", err)
	}
	if err := tx2.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := p.Get([]byte("c")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("rolled-back c should be absent, got err=%v", err)
	}
}

func TestTransaction_ReadYourOwnWrites(t *testing.T) {
	p, _ := openTemp(t)
	_ = p.Put([]byte("k"), []byte("parent"))

	tx, err := p.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := tx.Put([]byte("k"), []byte("tx-write")); err != nil {
		t.Fatalf("tx.Put: %v", err)
	}
	got, err := tx.Get([]byte("k"))
	if err != nil || string(got) != "tx-write" {
		t.Fatalf("tx.Get should see own write; got %q err=%v", got, err)
	}

	// Parent must NOT see the uncommitted write.
	parentVal, _ := p.Get([]byte("k"))
	if string(parentVal) != "parent" {
		t.Fatalf("parent should be unaffected pre-commit, got %q", parentVal)
	}
}

func TestTransaction_SnapshotIsolation(t *testing.T) {
	// Reads inside a transaction see the snapshot taken at Begin,
	// not concurrent commits from outside.
	p, _ := openTemp(t)
	_ = p.Put([]byte("k"), []byte("v0"))

	tx, err := p.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Commit an outside change AFTER the snapshot was taken.
	if err := p.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("outside put: %v", err)
	}

	got, err := tx.Get([]byte("k"))
	if err != nil {
		t.Fatalf("tx.Get: %v", err)
	}
	if string(got) != "v0" {
		t.Fatalf("snapshot isolation: tx should still see v0, got %q", got)
	}
}

func TestTransaction_DeleteOverlay(t *testing.T) {
	p, _ := openTemp(t)
	_ = p.Put([]byte("k"), []byte("v"))

	tx, _ := p.Begin(backend.SnapshotIsolation)
	if err := tx.Delete([]byte("k")); err != nil {
		t.Fatalf("tx.Delete: %v", err)
	}
	if _, err := tx.Get([]byte("k")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("deleted key should report ErrNotFound inside tx, got %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := p.Get([]byte("k")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("committed delete should hide key on parent, got %v", err)
	}
}

func TestTransaction_ScanPrefixMergesOverlay(t *testing.T) {
	p, _ := openTemp(t)
	_ = p.Put([]byte("user:alice"), []byte("a"))
	_ = p.Put([]byte("user:bob"), []byte("b"))

	tx, _ := p.Begin(backend.SnapshotIsolation)
	defer func() { _ = tx.Rollback() }()

	_ = tx.Put([]byte("user:carol"), []byte("c"))  // overlay new
	_ = tx.Put([]byte("user:alice"), []byte("a2")) // overlay update
	_ = tx.Delete([]byte("user:bob"))              // overlay delete

	it, err := tx.ScanPrefix([]byte("user:"))
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer func() { _ = it.Close() }()

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
	want := map[string]string{
		"user:alice": "a2",
		"user:carol": "c",
	}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("scan[%q]: want %q, got %q", k, v, got[k])
		}
	}
}

func TestRejectsSerializableSnapshot(t *testing.T) {
	p, _ := openTemp(t)
	if _, err := p.Begin(backend.SerializableSnapshot); err == nil {
		t.Fatalf("expected Begin(SerializableSnapshot) to fail")
	}
}

func TestClosedRejectsOps(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	p, err := pebblebe.New(pebblebe.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := p.Put([]byte("x"), []byte("y")); !errors.Is(err, backend.ErrClosed) {
		t.Fatalf("Put on closed should be ErrClosed, got %v", err)
	}
	if _, err := p.Get([]byte("x")); !errors.Is(err, backend.ErrClosed) {
		t.Fatalf("Get on closed should be ErrClosed, got %v", err)
	}
	// Close is idempotent.
	if err := p.Close(); err != nil {
		t.Fatalf("second Close should be nil, got %v", err)
	}
}

func TestReopenSeesPersistedData(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble")
	p1, err := pebblebe.New(pebblebe.Config{Dir: dir})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := p1.Put([]byte("durable"), []byte("yes")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := p1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	p2, err := pebblebe.New(pebblebe.Config{Dir: dir})
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	t.Cleanup(func() { _ = p2.Close() })

	got, err := p2.Get([]byte("durable"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("yes")) {
		t.Fatalf("durable value lost: got %q want yes", got)
	}
}

func TestNewRejectsMissingDir(t *testing.T) {
	if _, err := pebblebe.New(pebblebe.Config{}); err == nil {
		t.Fatalf("expected error for empty Dir")
	}
}
