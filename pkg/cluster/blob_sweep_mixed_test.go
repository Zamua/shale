package cluster_test

// Additional same-shard orphan-sweep tests (design section 11.7) rounding out
// guarantee 3:
//
//   - a sweep pass over a MIXED population (some referenced, some orphaned,
//     under the same unit) reclaims only the orphans and keeps every bound blob,
//     proving the referenced-set check is per-object, not all-or-nothing;
//   - the sweep's referenced-set scan FAILS CLOSED on a malformed pointer value
//     under bref/: an undecodable pointer aborts the pass with the error and
//     reclaims nothing (a missed sweep is a storage leak, never data loss).
//
// Legacy mode (one keyspace, the "legacy" unit), like the sibling sweep tests.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/blob/blobmem"
	"github.com/Zamua/shale/pkg/cluster"
)

// TestSweepOrphans_MixedPopulation pins that within ONE sweep pass over the same
// unit, an aged orphan is reclaimed while a bound (referenced) blob is kept -
// regardless of the bound blob's age. This is the per-object discrimination the
// brief's guarantee 3 requires ("a bound blob is NEVER swept regardless of age",
// alongside reclaiming the unbound aged one).
func TestSweepOrphans_MixedPopulation(t *testing.T) {
	store := blobmem.New()
	bkv, err := cluster.NewBlobKV(cluster.Config{NodeID: "n1", Backend: memory.New(), BlobStore: store})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })

	ctx := context.Background()

	// Bound blob: stage + bind, then age it to the epoch (oldest possible). It must
	// survive because it is referenced, not because it is young.
	boundRoute := []byte("slug-bound")
	boundBody := []byte("i am referenced")
	boundRef, err := bkv.StageBlob(ctx, boundRoute, bytes.NewReader(boundBody), int64(len(boundBody)))
	if err != nil {
		t.Fatalf("StageBlob(bound): %v", err)
	}
	if err := bkv.Transact(boundRoute, func(tx *cluster.BlobTx) error { return tx.BindBlob(boundRef) }); err != nil {
		t.Fatalf("Transact(bind bound): %v", err)
	}
	store.SetModTime(blob.FinalKey(boundRef.Unit, boundRef.BlobID), time.Unix(0, 0))

	// Orphan blob: stage with NO bind, aged past the grace -> reclaimable.
	orphanRoute := []byte("slug-orphan")
	orphanBody := []byte("i am a crash orphan")
	orphanRef, err := bkv.StageBlob(ctx, orphanRoute, bytes.NewReader(orphanBody), int64(len(orphanBody)))
	if err != nil {
		t.Fatalf("StageBlob(orphan): %v", err)
	}
	orphanKey := blob.FinalKey(orphanRef.Unit, orphanRef.BlobID)
	store.SetModTime(orphanKey, time.Now().Add(-2*time.Hour))

	// A fresh orphan too (unbound, recent) -> must be KEPT (in-flight stage guard).
	freshRoute := []byte("slug-fresh")
	freshBody := []byte("i am in flight")
	freshRef, err := bkv.StageBlob(ctx, freshRoute, bytes.NewReader(freshBody), int64(len(freshBody)))
	if err != nil {
		t.Fatalf("StageBlob(fresh): %v", err)
	}
	freshKey := blob.FinalKey(freshRef.Unit, freshRef.BlobID)
	store.SetModTime(freshKey, time.Now())

	if store.Len() != 3 {
		t.Fatalf("pre-sweep store holds %d objects, want 3", store.Len())
	}

	if err := bkv.SweepOrphans(ctx, time.Now(), time.Hour); err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	// Bound survives (referenced); fresh survives (young); aged orphan reclaimed.
	if store.Len() != 2 {
		t.Fatalf("post-sweep store holds %d objects, want 2 (bound + fresh kept, aged orphan reclaimed)", store.Len())
	}
	if has, _ := store.Has(ctx, orphanKey); has {
		t.Fatal("aged orphan was NOT reclaimed")
	}
	if has, _ := store.Has(ctx, blob.FinalKey(boundRef.Unit, boundRef.BlobID)); !has {
		t.Fatal("bound (referenced) blob was swept despite being referenced")
	}
	if has, _ := store.Has(ctx, freshKey); !has {
		t.Fatal("fresh (in-flight) orphan was swept despite being within grace")
	}
	// And the bound blob still resolves.
	rc, _, err := bkv.GetBlob(ctx, boundRoute, boundRef.BlobID)
	if err != nil {
		t.Fatalf("GetBlob(bound) after mixed sweep: %v", err)
	}
	_ = rc.Close()
}

// TestSweepOrphans_FailClosedOnMalformedPointer pins that a malformed pointer
// value sitting under bref/ aborts the sweep's referenced-set scan rather than
// being treated as "no reference" (which would let the sweep delete the object
// that malformed pointer was protecting). The whole pass fails closed: it
// returns the decode error and reclaims nothing.
//
// To plant a malformed pointer we write a hash-tagged bref-shaped key (so it
// lands under the bref/ prefix the scan walks) with a value that is NOT a valid
// encoded blob.Pointer.
func TestSweepOrphans_FailClosedOnMalformedPointer(t *testing.T) {
	store := blobmem.New()
	bkv, err := cluster.NewBlobKV(cluster.Config{NodeID: "n1", Backend: memory.New(), BlobStore: store})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })

	ctx := context.Background()

	// Stage an aged, unbound orphan a working sweep WOULD reclaim.
	routeKey := []byte("slug-malformed")
	body := []byte("kept because the scan fails closed")
	ref, err := bkv.StageBlob(ctx, routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	store.SetModTime(blob.FinalKey(ref.Unit, ref.BlobID), time.Now().Add(-2*time.Hour))

	// Plant a malformed value under a bref/ key. brefKey is pure over the ref, so
	// reuse a ref's key shape but write garbage bytes (not a Pointer envelope).
	badPtrRef := cluster.BlobRef{Unit: "legacy", RouteShard: []byte("slug-malformed"), BlobID: "garbage"}
	if err := bkv.Put(cluster.BrefKeyForTest(badPtrRef), []byte("not-a-valid-pointer-envelope")); err != nil {
		t.Fatalf("plant malformed pointer: %v", err)
	}

	err = bkv.SweepOrphans(ctx, time.Now(), time.Hour)
	if err == nil {
		t.Fatal("SweepOrphans should fail closed on a malformed pointer (decode error)")
	}
	// Nothing reclaimed: the orphan object is still present (the scan aborted
	// before the delete loop ran).
	if has, _ := store.Has(ctx, blob.FinalKey(ref.Unit, ref.BlobID)); !has {
		t.Fatal("fail-closed sweep reclaimed the orphan despite a malformed-pointer scan error")
	}
}
