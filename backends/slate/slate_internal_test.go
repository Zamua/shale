//go:build slatedb

package slate

// Same-package tests pinning constructor wiring that's not externally
// observable on a memory:/// store (where AwaitDurable=false has no
// measurable behavior difference from AwaitDurable=true).

import (
	"testing"

	slatedb "slatedb.io/slatedb-go/uniffi"

	"github.com/Zamua/shale/pkg/backend"
)

// TestNewWithStoreOpts_StoresWriteOptionsPointer pins that the
// constructor captures the operator-supplied *WriteOptions on the
// returned *Slate. The Put/Delete/Commit conditionals depend on this
// field being non-nil; if a future refactor regresses the
// constructor's storage, this test fails BEFORE any FFI call paths
// are exercised, giving a cleaner signal than the external behavior
// tests (which pass on memory:/// regardless of route).
func TestNewWithStoreOpts_StoresWriteOptionsPointer(t *testing.T) {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve memory store: %v", err)
	}

	wopts := &slatedb.WriteOptions{AwaitDurable: false}
	s, err := NewWithStoreOpts("shale-internal-wiring", store, nil, wopts)
	if err != nil {
		store.Destroy()
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if s.writeOpts == nil {
		t.Fatalf("Slate.writeOpts is nil after NewWithStoreOpts(..., non-nil wopts); Put would route through plain Db.Put")
	}
	if s.writeOpts != wopts {
		t.Fatalf("Slate.writeOpts pointer = %p, want operator-supplied %p (slate must not copy)", s.writeOpts, wopts)
	}
	if s.writeOpts.AwaitDurable != false {
		t.Fatalf("Slate.writeOpts.AwaitDurable = %v, want false", s.writeOpts.AwaitDurable)
	}
}

// TestNewWithStoreOpts_NilWriteOptionsLeavesFieldNil is the inverse:
// nil in, nil stored. Pins that no spurious default is materialized.
func TestNewWithStoreOpts_NilWriteOptionsLeavesFieldNil(t *testing.T) {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve memory store: %v", err)
	}
	s, err := NewWithStoreOpts("shale-internal-wiring-nil", store, nil, nil)
	if err != nil {
		store.Destroy()
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if s.writeOpts != nil {
		t.Fatalf("Slate.writeOpts = %+v, want nil (operator passed nil; Put must use plain Db.Put)", *s.writeOpts)
	}
}

// TestBegin_TransactionInheritsWriteOpts pins that Begin threads the
// backend's writeOpts into the per-transaction state so Commit can
// route through CommitWithOptions. Without this thread, a Slate
// opened with relaxed durability would silently downgrade its
// transactions to the default Commit path.
func TestBegin_TransactionInheritsWriteOpts(t *testing.T) {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve memory store: %v", err)
	}
	wopts := &slatedb.WriteOptions{AwaitDurable: false}
	s, err := NewWithStoreOpts("shale-internal-tx-wiring", store, nil, wopts)
	if err != nil {
		store.Destroy()
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	txAny, err := s.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	tx, ok := txAny.(*transaction)
	if !ok {
		t.Fatalf("Begin returned %T, want *transaction", txAny)
	}
	defer func() { _ = tx.Rollback() }()

	if tx.writeOpts == nil {
		t.Fatalf("transaction.writeOpts is nil after Begin from a Slate opened with non-nil writeOpts; Commit would use plain tx.Commit")
	}
	if tx.writeOpts != wopts {
		t.Fatalf("transaction.writeOpts = %p, want inherited %p", tx.writeOpts, wopts)
	}
}
