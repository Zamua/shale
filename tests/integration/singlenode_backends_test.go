package integration

// Single-node mode is backend-agnostic, and this pins it.
//
// Config.Backend with an EMPTY ClusterToken is local-only embedding: no
// membership, no ring, every operation served by the supplied Backend.
// Nothing in that path inspects what the backend can do - it imposes no
// fencing requirement, no unit-open requirement, no replica addressing.
// Any Backend implementation therefore satisfies it.
//
// This test is the safety net for retiring the legacy MULTI-node fallback
// (Config.Backend WITH a bind address). That removal must not reach the
// single-node early return in cluster.Open, which precedes every piece of
// the machinery being deleted. Running the full public verb surface over
// two independent Backend implementations - one volatile map, one durable
// on-disk LSM - is what demonstrates the seam held.

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/Zamua/shale/backends/pebble"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
)

// singleNodeBackendCases enumerates the Backend implementations that
// single-node mode must keep accepting. Each entry constructs a fresh
// backend; the subtest owns it and the Cluster closes it.
//
// backends/slate is the third implementation. It is not in this table
// because it needs cgo plus the native slatedb library at link time, so a
// test importing it cannot build in the default configuration. It is
// covered by the slatedb-tagged sibling in this directory.
func singleNodeBackendCases(t *testing.T) []struct {
	name string
	open func(t *testing.T) backend.Backend
} {
	t.Helper()
	return []struct {
		name string
		open func(t *testing.T) backend.Backend
	}{
		{
			name: "memory",
			open: func(_ *testing.T) backend.Backend { return memory.New() },
		},
		{
			name: "pebble",
			open: func(t *testing.T) backend.Backend {
				be, err := pebble.New(pebble.Config{Dir: filepath.Join(t.TempDir(), "db")})
				if err != nil {
					t.Fatalf("open pebble: %v", err)
				}
				return be
			},
		},
	}
}

// TestSingleNode_EveryBackend_RoundTrip exercises the full local verb
// surface. A backend that satisfies pkg/backend.Backend is enough; the
// cluster layer asks nothing further of it in this mode.
func TestSingleNode_EveryBackend_RoundTrip(t *testing.T) {
	for _, tc := range singleNodeBackendCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			c, err := cluster.Open(cluster.Config{NodeID: "solo", Backend: tc.open(t)})
			if err != nil {
				t.Fatalf("Open single-node with %s backend: %v", tc.name, err)
			}
			t.Cleanup(func() { _ = c.Close() })

			if err := c.Put([]byte("alpha"), []byte("one")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			got, err := c.Get([]byte("alpha"))
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !bytes.Equal(got, []byte("one")) {
				t.Fatalf("Get returned %q, want %q", got, "one")
			}

			// Overwrite is last-write-wins on the local backend.
			if err := c.Put([]byte("alpha"), []byte("two")); err != nil {
				t.Fatalf("Put overwrite: %v", err)
			}
			got, err = c.Get([]byte("alpha"))
			if err != nil {
				t.Fatalf("Get after overwrite: %v", err)
			}
			if !bytes.Equal(got, []byte("two")) {
				t.Fatalf("Get after overwrite returned %q, want %q", got, "two")
			}

			// Scan sees every key under the prefix, in ascending order.
			for _, k := range []string{"p/b", "p/a", "p/c"} {
				if err := c.Put([]byte(k), []byte(k)); err != nil {
					t.Fatalf("Put %s: %v", k, err)
				}
			}
			it, err := c.ScanPrefix([]byte("p/"))
			if err != nil {
				t.Fatalf("ScanPrefix: %v", err)
			}
			var scanned []string
			for {
				k, _, err := it.Next()
				if err != nil {
					_ = it.Close()
					t.Fatalf("Iterator.Next: %v", err)
				}
				if k == nil {
					break
				}
				scanned = append(scanned, string(k))
			}
			if err := it.Close(); err != nil {
				t.Fatalf("Iterator.Close: %v", err)
			}
			want := []string{"p/a", "p/b", "p/c"}
			if len(scanned) != len(want) {
				t.Fatalf("ScanPrefix returned %v, want %v", scanned, want)
			}
			for i := range want {
				if scanned[i] != want[i] {
					t.Fatalf("ScanPrefix returned %v, want %v", scanned, want)
				}
			}

			// Delete is idempotent and leaves a not-found read behind.
			if err := c.Delete([]byte("alpha")); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if err := c.Delete([]byte("alpha")); err != nil {
				t.Fatalf("Delete is not idempotent: %v", err)
			}
			if _, err := c.Get([]byte("alpha")); !errors.Is(err, backend.ErrNotFound) {
				t.Fatalf("Get after Delete returned %v, want ErrNotFound", err)
			}
		})
	}
}

// TestSingleNode_EveryBackend_Transaction pins the local transaction
// proxy, which in single-node mode delegates straight to the backend
// with no cross-shard check to satisfy.
func TestSingleNode_EveryBackend_Transaction(t *testing.T) {
	for _, tc := range singleNodeBackendCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			c, err := cluster.Open(cluster.Config{NodeID: "solo", Backend: tc.open(t)})
			if err != nil {
				t.Fatalf("Open single-node with %s backend: %v", tc.name, err)
			}
			t.Cleanup(func() { _ = c.Close() })

			tx, err := c.Begin(backend.SnapshotIsolation)
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			if err := tx.Put([]byte("t/committed"), []byte("yes")); err != nil {
				t.Fatalf("tx.Put: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("tx.Commit: %v", err)
			}
			got, err := c.Get([]byte("t/committed"))
			if err != nil {
				t.Fatalf("Get after commit: %v", err)
			}
			if !bytes.Equal(got, []byte("yes")) {
				t.Fatalf("Get after commit returned %q, want %q", got, "yes")
			}

			tx, err = c.Begin(backend.SnapshotIsolation)
			if err != nil {
				t.Fatalf("Begin for rollback: %v", err)
			}
			if err := tx.Put([]byte("t/rolledback"), []byte("no")); err != nil {
				t.Fatalf("tx.Put for rollback: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("tx.Rollback: %v", err)
			}
			if _, err := c.Get([]byte("t/rolledback")); !errors.Is(err, backend.ErrNotFound) {
				t.Fatalf("rolled-back key readable: err = %v, want ErrNotFound", err)
			}
		})
	}
}

// TestSingleNode_EveryBackend_NoRing pins the property that makes the
// mode backend-agnostic: single-node builds no membership and no ring,
// so it never consults an adapter about ownership, fencing, or units.
// If a future change routes single-node through ring lookup, this fails.
func TestSingleNode_EveryBackend_NoRing(t *testing.T) {
	for _, tc := range singleNodeBackendCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			c, err := cluster.Open(cluster.Config{NodeID: "solo", Backend: tc.open(t)})
			if err != nil {
				t.Fatalf("Open single-node with %s backend: %v", tc.name, err)
			}
			t.Cleanup(func() { _ = c.Close() })

			// With no ring, Members synthesizes the single local node.
			// More than one member means a ring was built, which is the
			// regression this guards against.
			members := c.Members()
			if len(members) != 1 || members[0].ID != "solo" {
				t.Fatalf("single-node reported members %v, want exactly the local node", members)
			}
			// Every key is local: with no ring there is nowhere else for
			// one to live, so ownership is unconditionally true.
			if !c.OwnsKey([]byte("anything")) {
				t.Fatal("single-node does not own an arbitrary key")
			}
		})
	}
}
