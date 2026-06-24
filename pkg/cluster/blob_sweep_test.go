package cluster_test

// Tests for the phase-2 same-shard orphan-bytes sweep (design section 11.7) and
// the unit-token accessors (11.5). The sweep reclaims a pointer-less blob object
// only when its object-store ModTime is older than the grace (10.2), so a
// just-staged-not-yet-bound blob is never swept (the in-flight stage race).
//
// Most of these run in LEGACY (non-multi) mode: there is one keyspace,
// MountedUnits() returns the single "legacy" sentinel, and every blob is keyed
// blob/legacy/. That exercises the full sweep machinery (referenced-set scan,
// age-gate, delete) without standing up a multi-backend cluster. The multi-mode
// "sweep only touches MOUNTED units" P0 guard is pinned by
// TestSweepOrphans_OnlySweepsMountedUnits in blob_sweep_mounted_test.go.

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/blob/blobmem"
	"github.com/Zamua/shale/pkg/cluster"
)

// newBlobKV is the legacy-mode *BlobKV the sweep tests share.
func newBlobKV(t *testing.T, store *blobmem.Store) *cluster.BlobKV {
	t.Helper()
	bkv, err := cluster.NewBlobKV(cluster.Config{NodeID: "n1", Backend: memory.New(), BlobStore: store})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })
	return bkv
}

// TestSweepOrphans_KeepsReferenced pins that a blob with a live bound pointer is
// NEVER swept, even when it is old enough to be past the grace.
func TestSweepOrphans_KeepsReferenced(t *testing.T) {
	store := blobmem.New()
	bkv := newBlobKV(t, store)

	routeKey := []byte("slug-keep")
	body := []byte("referenced bytes")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error { return tx.BindBlob(ref) }); err != nil {
		t.Fatalf("Transact(bind): %v", err)
	}
	// Age the object well past the grace so age is NOT the thing keeping it.
	store.SetModTime(blob.FinalKey(ref.Unit, ref.BlobID), time.Unix(0, 0))

	if err := bkv.SweepOrphans(context.Background(), time.Now(), time.Hour); err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	if store.Len() != 1 {
		t.Fatalf("referenced blob was swept: store holds %d, want 1", store.Len())
	}
	// And it still resolves.
	rc, _, err := bkv.GetBlob(context.Background(), routeKey, ref.BlobID)
	if err != nil {
		t.Fatalf("GetBlob after sweep: %v", err)
	}
	_ = rc.Close()
}

// TestSweepOrphans_DecodesEnvelopedPointer pins the R>1 path. At R>1 a bound
// blob's bref is stored as an LWW Envelope (Stamp + payload) - the shape the
// replicated write produces and the live prod cluster holds. The orphan sweep's
// referenced-set scan reads brefs RAW via LocalScanPrefix, so it must Decode the
// envelope before DecodePointer. Without that strip it fed the envelope's binary
// header into the JSON decoder, fail-closed on the FIRST enveloped bref, and the
// GC never ran (the prod #427 bug: "decode pointer: invalid character").
//
// This test FAILS against the pre-fix code: SweepOrphans returns the decode
// error, so the aged orphan is never reclaimed.
func TestSweepOrphans_DecodesEnvelopedPointer(t *testing.T) {
	store := blobmem.New()
	mem := memory.New()
	bkv, err := cluster.NewBlobKV(cluster.Config{NodeID: "n1", Backend: mem, BlobStore: store})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })
	ctx := context.Background()

	// A referenced blob: stage + bind (BindBlob writes a RAW bref at R=1).
	ref, err := bkv.StageBlob(ctx, []byte("slug-ref"), bytes.NewReader([]byte("kept bytes")), 10)
	if err != nil {
		t.Fatalf("StageBlob ref: %v", err)
	}
	if err := bkv.Transact([]byte("slug-ref"), func(tx *cluster.BlobTx) error { return tx.BindBlob(ref) }); err != nil {
		t.Fatalf("bind: %v", err)
	}

	// Re-stamp every bref as an LWW Envelope, the shape a replicated R>1 write
	// produces. This is exactly what the referenced-set scan must strip.
	it, err := mem.ScanPrefix([]byte("bref/"))
	if err != nil {
		t.Fatalf("scan bref/: %v", err)
	}
	type kvp struct{ k, v []byte }
	var brefs []kvp
	for {
		k, v, ierr := it.Next()
		if ierr != nil {
			t.Fatalf("iter: %v", ierr)
		}
		if k == nil {
			break
		}
		brefs = append(brefs, kvp{append([]byte(nil), k...), append([]byte(nil), v...)})
	}
	_ = it.Close()
	if len(brefs) == 0 {
		t.Fatal("bind wrote no bref")
	}
	for _, b := range brefs {
		env := cluster.Encode(cluster.Envelope{Stamp: cluster.Stamp{TimestampNanos: 1, NodeID: "n1"}, Payload: b.v})
		if err := mem.Put(b.k, env); err != nil {
			t.Fatalf("re-stamp bref: %v", err)
		}
	}

	// An aged orphan (staged, never bound). The sweep reaches it ONLY if the
	// referenced-set scan did not fail-close on the enveloped bref above.
	orphan, err := bkv.StageBlob(ctx, []byte("slug-orphan"), bytes.NewReader([]byte("orphan bytes")), 12)
	if err != nil {
		t.Fatalf("StageBlob orphan: %v", err)
	}
	store.SetModTime(blob.FinalKey(orphan.Unit, orphan.BlobID), time.Unix(0, 0))
	store.SetModTime(blob.FinalKey(ref.Unit, ref.BlobID), time.Unix(0, 0)) // age the referenced too

	if err := bkv.SweepOrphans(ctx, time.Now(), time.Hour); err != nil {
		t.Fatalf("SweepOrphans fail-closed on an enveloped bref (the #427 bug): %v", err)
	}
	// Referenced object survives (its ObjKey was decoded from the envelope).
	if has, _ := store.Has(ctx, blob.FinalKey(ref.Unit, ref.BlobID)); !has {
		t.Fatal("referenced blob was wrongly swept - the enveloped bref's ObjKey was not decoded")
	}
	// Orphan is reclaimed (the GC actually ran).
	if has, _ := store.Has(ctx, blob.FinalKey(orphan.Unit, orphan.BlobID)); has {
		t.Fatal("aged orphan was not reclaimed - the referenced-set scan did not complete")
	}
}

// TestSweepOrphans_ReclaimsAgedOrphan is the crash-injection case: stage with NO
// bind (the process "died" before the binding commit), age the object past the
// grace, and assert the sweep reclaims it.
func TestSweepOrphans_ReclaimsAgedOrphan(t *testing.T) {
	store := blobmem.New()
	bkv := newBlobKV(t, store)

	routeKey := []byte("slug-orphan")
	body := []byte("crash orphan bytes")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	// No BindBlob: the bytes are an unreferenced orphan. Age them past the grace.
	store.SetModTime(blob.FinalKey(ref.Unit, ref.BlobID), time.Now().Add(-2*time.Hour))

	if err := bkv.SweepOrphans(context.Background(), time.Now(), time.Hour); err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}
	if store.Len() != 0 {
		t.Fatalf("aged orphan was NOT reclaimed: store holds %d, want 0", store.Len())
	}
}

// TestSweepOrphans_KeepsFreshOrphan is the in-flight-stage guard: an unreferenced
// blob YOUNGER than the grace (a stage whose bind has not committed yet) must NOT
// be swept, or the sweep would race a legitimate in-flight upload (design section
// 10.2). The same blob, once aged past the grace, IS reclaimed (the second
// assertion proves the gate is age, not a blanket keep).
func TestSweepOrphans_KeepsFreshOrphan(t *testing.T) {
	store := blobmem.New()
	bkv := newBlobKV(t, store)

	routeKey := []byte("slug-fresh")
	body := []byte("just staged")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	objkey := blob.FinalKey(ref.Unit, ref.BlobID)
	// Fresh: modtime is "now"; the bind just hasn't committed yet.
	store.SetModTime(objkey, time.Now())

	if err := bkv.SweepOrphans(context.Background(), time.Now(), time.Hour); err != nil {
		t.Fatalf("SweepOrphans (fresh): %v", err)
	}
	if store.Len() != 1 {
		t.Fatalf("fresh (in-flight) orphan was swept: store holds %d, want 1", store.Len())
	}

	// Age it past the grace -> now it is a genuine orphan and is reclaimed.
	store.SetModTime(objkey, time.Now().Add(-2*time.Hour))
	if err := bkv.SweepOrphans(context.Background(), time.Now(), time.Hour); err != nil {
		t.Fatalf("SweepOrphans (aged): %v", err)
	}
	if store.Len() != 0 {
		t.Fatalf("aged orphan was NOT reclaimed after grace: store holds %d, want 0", store.Len())
	}
}

// TestSweepOrphans_BoundaryAge pins the exact age-gate boundary: an object whose
// age equals the grace (ModTime == cutoff) is KEPT (the gate reclaims only
// objects STRICTLY older than the cutoff), and one a hair older is reclaimed.
func TestSweepOrphans_BoundaryAge(t *testing.T) {
	store := blobmem.New()
	bkv := newBlobKV(t, store)

	routeKey := []byte("slug-boundary")
	body := []byte("b")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	objkey := blob.FinalKey(ref.Unit, ref.BlobID)

	now := time.Now()
	grace := time.Hour
	// Exactly at the cutoff: kept (not strictly older).
	store.SetModTime(objkey, now.Add(-grace))
	if err := bkv.SweepOrphans(context.Background(), now, grace); err != nil {
		t.Fatalf("SweepOrphans (at boundary): %v", err)
	}
	if store.Len() != 1 {
		t.Fatalf("object exactly at the grace boundary was swept: store holds %d, want 1", store.Len())
	}
	// One nanosecond older than the cutoff: reclaimed.
	store.SetModTime(objkey, now.Add(-grace).Add(-time.Nanosecond))
	if err := bkv.SweepOrphans(context.Background(), now, grace); err != nil {
		t.Fatalf("SweepOrphans (past boundary): %v", err)
	}
	if store.Len() != 0 {
		t.Fatalf("object just past the grace boundary was NOT reclaimed: store holds %d, want 0", store.Len())
	}
}

// TestSweepOrphans_Idempotent pins that a second pass over an already-reclaimed
// orphan is harmless (Delete is idempotent on the store; the referenced-set scan
// finds nothing either way).
func TestSweepOrphans_Idempotent(t *testing.T) {
	store := blobmem.New()
	bkv := newBlobKV(t, store)

	routeKey := []byte("slug-idem")
	body := []byte("once")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	store.SetModTime(blob.FinalKey(ref.Unit, ref.BlobID), time.Now().Add(-2*time.Hour))

	for i := 0; i < 3; i++ {
		if err := bkv.SweepOrphans(context.Background(), time.Now(), time.Hour); err != nil {
			t.Fatalf("SweepOrphans pass %d: %v", i, err)
		}
	}
	if store.Len() != 0 {
		t.Fatalf("store holds %d after repeated sweeps, want 0", store.Len())
	}
}

// listErrStore wraps a blobmem.Store and yields an error from List, to pin the
// sweep's FAIL-CLOSED behavior: a listing error keeps the bytes (a missed sweep
// is a storage leak, never data loss).
type listErrStore struct {
	*blobmem.Store
	err error
}

func (s listErrStore) List(_ context.Context, _ string) iter.Seq2[blob.ObjectInfo, error] {
	return func(yield func(blob.ObjectInfo, error) bool) {
		yield(blob.ObjectInfo{}, s.err)
	}
}

// TestSweepOrphans_FailClosedOnListError pins that a List error aborts the pass
// with that error and reclaims nothing.
func TestSweepOrphans_FailClosedOnListError(t *testing.T) {
	inner := blobmem.New()
	bkv, err := cluster.NewBlobKV(cluster.Config{
		NodeID:    "n1",
		Backend:   memory.New(),
		BlobStore: listErrStore{Store: inner, err: errors.New("list boom")},
	})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })

	// Stage a blob (via the inner store directly: the wrapper only overrides
	// List). It is an aged orphan, so a working sweep WOULD reclaim it.
	routeKey := []byte("slug-failclosed")
	body := []byte("kept on error")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	inner.SetModTime(blob.FinalKey(ref.Unit, ref.BlobID), time.Now().Add(-2*time.Hour))

	err = bkv.SweepOrphans(context.Background(), time.Now(), time.Hour)
	if err == nil {
		t.Fatal("SweepOrphans should surface the List error (fail-closed)")
	}
	if inner.Len() != 1 {
		t.Fatalf("fail-closed sweep reclaimed bytes: store holds %d, want 1", inner.Len())
	}
}

// --- MountedUnits / RoutedUnitToken (design section 11.5) ---

// TestMountedUnits_LegacySentinel pins that a non-multi cluster reports the
// single "legacy" unit token (the one keyspace), and RoutedUnitToken for any
// route key is that same sentinel.
func TestMountedUnits_LegacySentinel(t *testing.T) {
	kv, err := cluster.New(cluster.Config{NodeID: "n1", Backend: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	c := kv.Cluster()

	units := c.MountedUnits()
	if len(units) != 1 || units[0] != "legacy" {
		t.Fatalf("MountedUnits (legacy) = %v, want [legacy]", units)
	}
	if tok := c.RoutedUnitToken([]byte("any-route-key")); tok != "legacy" {
		t.Fatalf("RoutedUnitToken (legacy) = %q, want legacy", tok)
	}
}

// TestRoutedUnitToken_StableForKey pins that RoutedUnitToken is a deterministic,
// pure function of the route key (the same key always yields the same token, so
// the object key, the pointer's ObjKey, and the sweep prefix agree).
func TestRoutedUnitToken_StableForKey(t *testing.T) {
	kv, err := cluster.New(cluster.Config{NodeID: "n1", Backend: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	c := kv.Cluster()

	a := c.RoutedUnitToken([]byte("slug-stable"))
	b := c.RoutedUnitToken([]byte("slug-stable"))
	if a != b {
		t.Fatalf("RoutedUnitToken not stable: %q vs %q", a, b)
	}
}
