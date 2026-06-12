//go:build slatedb

package slate_test

import (
	"bytes"
	"errors"
	"fmt"
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

// TestConfigSettings_NilUsesSlatedbDefaults pins the "nil = defaults"
// half of the spec's Settings pass-through: NewWithStore without
// Settings produces a working backend (existing test coverage already
// exercises this path via openTestSlate, but we restate it explicitly
// here so a future refactor that breaks defaulting can't silently
// pass).
func TestConfigSettings_NilUsesSlatedbDefaults(t *testing.T) {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve memory store: %v", err)
	}
	s, err := slate.NewWithStore("shale-test-nil-settings", store)
	if err != nil {
		store.Destroy()
		t.Fatalf("open slate with nil Settings: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := s.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("got %q want v", got)
	}
}

// TestConfigSettings_PassThroughReachesEngine pins the pass-through
// contract: a non-nil Settings is forwarded verbatim to the
// DbBuilder. We verify two ways:
//
//  1. A Settings tweaked with a VALID dotted path (flush_interval)
//     opens cleanly + writes succeed. This proves the slate->slatedb
//     plumbing (DbBuilder.WithSettings) runs without dropping the
//     handle on the floor.
//  2. A Settings tweaked with an INVALID dotted path is rejected by
//     slatedb.Settings.Set BEFORE shale ever sees it; a control
//     confirming Settings.Set is the actual validation point (not
//     shale's job, per spec: "no shale-level validation").
//
// We can't directly observe "DbBuilder received this exact Settings
// handle" without a slatedb-side test hook, but (1) is enough: if
// shale silently dropped Settings, slatedb would still get its own
// defaults + the test would still pass. If shale CORRUPTED Settings
// (e.g. mutated the handle before forwarding), slatedb would reject
// it during Build.
//
// NOTE on AwaitDurable: the spec example shows
// `settings.AwaitDurable = false`, but in the current slatedb-go
// v0.13.1 binding AwaitDurable is a field on WriteOptions (per-write),
// NOT on Settings; Settings itself has no exported struct fields
// (it's an opaque uniffi handle mutated via Set(dottedPath, json)).
// The spec example is aspirational for the Rust API; the Go binding
// would need to surface per-field Settings accessors before that exact
// code can compile. See backends/slate/README.md "Custom slatedb
// settings" for the in-binding workflow.
func TestConfigSettings_PassThroughReachesEngine(t *testing.T) {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve memory store: %v", err)
	}

	settings := slatedb.SettingsDefault()
	// Apply a real, valid knob via the binding's only mutation API.
	if err := settings.Set("flush_interval", `"250ms"`); err != nil {
		t.Fatalf("settings.Set flush_interval: %v", err)
	}

	s, err := slate.NewWithStore("shale-test-custom-settings", store, settings)
	if err != nil {
		store.Destroy()
		t.Fatalf("open slate with custom Settings: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := s.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("got %q want v", got)
	}

	// Control: type errors on known fields are caught by Settings.Set
	// itself (slatedb-side), before shale ever sees the value. Per
	// spec: "no shale-level validation."
	//
	// NOTE: unknown dotted paths are NOT rejected (the binding's docs
	// say missing intermediate objects are auto-created), so we use a
	// type mismatch on a known field to exercise the validation gate.
	bogus := slatedb.SettingsDefault()
	if err := bogus.Set("flush_interval", `42`); err == nil { // int where duration string expected
		t.Fatalf("expected Settings.Set to reject type mismatch on flush_interval")
	}
}

// TestConfigWriteOptions_NilUsesDefaults pins the "nil = plain
// Put/Delete/Commit" half of the WriteOptions pass-through. Equivalent
// in spirit to TestConfigSettings_NilUsesSlatedbDefaults but for the
// per-write knob: a backend opened without WriteOptions still
// supports Put/Get/Delete + transaction Commit, behaviorally
// indistinguishable from the pre-pass-through slate backend.
func TestConfigWriteOptions_NilUsesDefaults(t *testing.T) {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve memory store: %v", err)
	}
	s, err := slate.NewWithStoreOpts("shale-test-nil-writeopts", store, nil, nil)
	if err != nil {
		store.Destroy()
		t.Fatalf("open slate with nil WriteOptions: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := s.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("got %q want v", got)
	}
	if err := s.Delete([]byte("k")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.Get([]byte("k")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}

	// Transaction Commit on the nil-writeOpts path takes the plain
	// tx.Commit() route. Pin the post-commit visibility.
	tx, err := s.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Put([]byte("tx-k"), []byte("tx-v")); err != nil {
		t.Fatalf("tx put: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx commit: %v", err)
	}
	got, err = s.Get([]byte("tx-k"))
	if err != nil {
		t.Fatalf("post-commit get: %v", err)
	}
	if !bytes.Equal(got, []byte("tx-v")) {
		t.Fatalf("post-commit tx-k = %q want tx-v", got)
	}
}

// TestConfigWriteOptions_PassThroughReachesEngine pins the pass-through
// contract for WriteOptions. We can't observe the per-write durability
// behavior directly from a memory:/// store (there's no S3 round-trip
// whose latency would change), so we exercise the binding's wiring
// instead:
//
//  1. AwaitDurable=false against memory:/// opens cleanly + every
//     Put/Get/Delete round-trips correctly. This proves slate routes
//     through PutWithOptions/DeleteWithOptions without dropping the
//     options on the floor: if it did, the test would still pass with
//     the plain Put/Delete path. But if it CORRUPTED the options
//     (e.g. passed a zero-valued PutOptions whose Ttl is the un-set
//     enum variant, not TtlDefault), slatedb would reject the call at
//     the FFI boundary.
//  2. AwaitDurable=true against memory:/// is the same wiring with the
//     "ack-after-flush" knob; pinning both branches exercises that the
//     flag is forwarded by value, not flattened to a default.
//  3. Transaction Commit at AwaitDurable=false routes through
//     CommitWithOptions and the committed write becomes visible.
//
// The honest part: we don't have a hook to capture "this exact
// WriteOptions struct reached the C boundary," and an in-memory store
// won't surface the latency delta. The wiring proof is "it doesn't
// crash AND the data round-trips," combined with TestNewWithStoreOpts_
// PreservesWriteOptionsValue below which pins the constructor's
// store/dereference semantics.
func TestConfigWriteOptions_PassThroughReachesEngine(t *testing.T) {
	cases := []struct {
		name         string
		awaitDurable bool
	}{
		{name: "AwaitDurable_false_fastAck", awaitDurable: false},
		{name: "AwaitDurable_true_explicit", awaitDurable: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, err := slatedb.ObjectStoreResolve("memory:///")
			if err != nil {
				t.Fatalf("resolve memory store: %v", err)
			}
			wopts := &slatedb.WriteOptions{AwaitDurable: tc.awaitDurable}
			s, err := slate.NewWithStoreOpts("shale-test-writeopts-"+tc.name, store, nil, wopts)
			if err != nil {
				store.Destroy()
				t.Fatalf("open slate with WriteOptions{AwaitDurable=%v}: %v", tc.awaitDurable, err)
			}
			t.Cleanup(func() { _ = s.Close() })

			if err := s.Put([]byte("k"), []byte("v")); err != nil {
				t.Fatalf("put: %v", err)
			}
			got, err := s.Get([]byte("k"))
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if !bytes.Equal(got, []byte("v")) {
				t.Fatalf("got %q want v", got)
			}
			if err := s.Delete([]byte("k")); err != nil {
				t.Fatalf("delete: %v", err)
			}
			if _, err := s.Get([]byte("k")); !errors.Is(err, backend.ErrNotFound) {
				t.Fatalf("expected ErrNotFound after delete, got %v", err)
			}

			// Transaction Commit routes via CommitWithOptions when
			// writeOpts is non-nil.
			tx, err := s.Begin(backend.SnapshotIsolation)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := tx.Put([]byte("tx-k"), []byte("tx-v")); err != nil {
				t.Fatalf("tx put: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("tx commit (AwaitDurable=%v): %v", tc.awaitDurable, err)
			}
			got, err = s.Get([]byte("tx-k"))
			if err != nil {
				t.Fatalf("post-commit get: %v", err)
			}
			if !bytes.Equal(got, []byte("tx-v")) {
				t.Fatalf("post-commit tx-k = %q want tx-v", got)
			}
		})
	}
}

// TestConfigWriteOptions_TransactionRollbackUnaffected pins that
// Rollback stays on the plain tx.Rollback() path regardless of
// WriteOptions: rollback discards the write set, there's no durability
// knob to honor.
func TestConfigWriteOptions_TransactionRollbackUnaffected(t *testing.T) {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve memory store: %v", err)
	}
	s, err := slate.NewWithStoreOpts("shale-test-writeopts-rollback", store, nil,
		&slatedb.WriteOptions{AwaitDurable: false})
	if err != nil {
		store.Destroy()
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	tx, err := s.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Put([]byte("rb"), []byte("v")); err != nil {
		t.Fatalf("tx put: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := s.Get([]byte("rb")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("rolled-back key should be absent, got err=%v", err)
	}
}

// TestIsFenced pins the typed fence sentinel (the P3 fix for the chaos
// harness's brittle substring classification). It constructs the REAL
// slatedb.ErrorClosed{Reason: CloseReasonFenced} the engine raises on a
// fence, wraps it the way the slate backend does (fmt.Errorf("...: %w")),
// and asserts IsFenced recognizes it - while NOT misclassifying a
// non-fence close (Clean) or an unrelated error. Because it asserts against
// the binding's OWN typed error, a slatedb binding change that altered the
// fence representation would FAIL this test instead of silently
// reclassifying a real fence as a real data loss downstream.
func TestIsFenced(t *testing.T) {
	fenced := slatedb.NewErrorClosed(slatedb.CloseReasonFenced, "detected newer DB client").AsError()
	if !slate.IsFenced(fenced) {
		t.Fatalf("IsFenced(fenced) = false, want true (err=%v)", fenced)
	}
	// Wrapped the way the backend wraps it (%w) must still be recognized.
	wrapped := fmt.Errorf("slate: put: %w", fenced)
	if !slate.IsFenced(wrapped) {
		t.Fatalf("IsFenced(wrapped fenced) = false, want true (err=%v)", wrapped)
	}
	// A CLEAN close is not a fence.
	clean := slatedb.NewErrorClosed(slatedb.CloseReasonClean, "closed cleanly").AsError()
	if slate.IsFenced(clean) {
		t.Fatalf("IsFenced(clean close) = true, want false")
	}
	// An unrelated error is not a fence.
	if slate.IsFenced(errors.New("boom")) {
		t.Fatalf("IsFenced(unrelated) = true, want false")
	}
	// nil is not a fence.
	if slate.IsFenced(nil) {
		t.Fatalf("IsFenced(nil) = true, want false")
	}
}
