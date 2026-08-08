package cluster_test

// Tests for the phase-2 capability-split surface (design section 11.1): the
// *KV / *BlobKV / *Tx / *BlobTx wrappers, the constructor gating, the
// embed-and-shadow Transact dispatch, the BlobRef shape, and the brefKey
// co-routing rule. These are FOUNDATION tests: they pin the type surface and the
// pure key rule. The blob op BODIES (StageBlob / GetBlob / UnstageBlob and the
// BindBlob / UnbindBlob persistence) land in the implementation phase; here
// BindBlob / UnbindBlob are stubs, so these tests do NOT exercise a bind commit.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/blob/blobmem"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
)

// --- constructor capability gating ---

// TestNew_RejectsConfiguredBlobStore pins that New (the metadata-only *KV
// constructor) refuses a non-nil Config.BlobStore: the *KV surface cannot reach
// the byte plane, so a configured store there is an unreachable-capability
// wiring mistake that must surface at construction.
func TestNew_RejectsConfiguredBlobStore(t *testing.T) {
	kv, err := cluster.New(cluster.Config{
		NodeID:    "n1",
		Backend:   memory.New(),
		BlobStore: blobmem.New(),
	})
	if err == nil {
		t.Fatal("New with a configured BlobStore should error (unreachable through *KV)")
	}
	if kv != nil {
		t.Fatalf("New error path returned a non-nil *KV: %v", kv)
	}
}

// TestNew_MetadataOnly pins that New with no blob store succeeds and the
// resulting *KV does the routed KV ops.
func TestNew_MetadataOnly(t *testing.T) {
	kv, err := cluster.New(cluster.Config{NodeID: "n1", Backend: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	if err := kv.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := kv.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("Get = %q, want %q", got, "v")
	}
	if err := kv.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

// TestNewBlobKV_RequiresBlobStore pins that NewBlobKV refuses a nil
// Config.BlobStore: the whole point of *BlobKV is the reachable byte plane, so a
// missing store is a constructor error (steering the caller to New).
func TestNewBlobKV_RequiresBlobStore(t *testing.T) {
	bkv, err := cluster.NewBlobKV(cluster.Config{NodeID: "n1", Backend: memory.New()})
	if err == nil {
		t.Fatal("NewBlobKV with a nil BlobStore should error")
	}
	if bkv != nil {
		t.Fatalf("NewBlobKV error path returned a non-nil *BlobKV: %v", bkv)
	}
}

// TestNewBlobKV_Succeeds pins that NewBlobKV with a configured store yields a
// *BlobKV that, because it embeds *KV, still does the metadata KV ops.
func TestNewBlobKV_Succeeds(t *testing.T) {
	bkv, err := cluster.NewBlobKV(cluster.Config{
		NodeID:    "n1",
		Backend:   memory.New(),
		BlobStore: blobmem.New(),
	})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })

	// Promoted-from-*KV metadata ops work on the superset type.
	if err := bkv.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := bkv.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("Get = %q, want %q", got, "v")
	}
}

// --- *KV.Transact (metadata-only) ---

// TestKV_Transact pins that *KV.Transact commits a multi-key write atomically
// and surfaces the values afterward.
func TestKV_Transact(t *testing.T) {
	kv, err := cluster.New(cluster.Config{NodeID: "n1", Backend: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	err = kv.Transact([]byte("pin"), func(tx *cluster.Tx) error {
		if err := tx.Put([]byte("a"), []byte("1")); err != nil {
			return err
		}
		return tx.Put([]byte("b"), []byte("2"))
	})
	if err != nil {
		t.Fatalf("Transact: %v", err)
	}
	for k, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := kv.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("Get %s = %q, want %q", k, got, want)
		}
	}
}

// TestKV_Transact_AbortOnError pins that fn returning an error rolls back (no
// keys written).
func TestKV_Transact_AbortOnError(t *testing.T) {
	kv, err := cluster.New(cluster.Config{NodeID: "n1", Backend: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	sentinel := errors.New("boom")
	err = kv.Transact([]byte("pin"), func(tx *cluster.Tx) error {
		if err := tx.Put([]byte("a"), []byte("1")); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Transact error = %v, want sentinel", err)
	}
	if _, err := kv.Get([]byte("a")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Get(a) after abort = %v, want ErrNotFound (rollback)", err)
	}
}

// --- *BlobKV.Transact shadow + dispatch ---

// TestBlobKV_Transact_YieldsBlobTx pins the embed-and-shadow dispatch: a
// *BlobKV's Transact hands the closure a *BlobTx (the richer type with
// BindBlob/UnbindBlob), NOT a plain *Tx. The metadata Put/Get inherited via the
// embedded *Tx still commit. The test exercises the shadowed method purely by
// the closure's parameter type compiling as *cluster.BlobTx.
func TestBlobKV_Transact_YieldsBlobTx(t *testing.T) {
	bkv, err := cluster.NewBlobKV(cluster.Config{
		NodeID:    "n1",
		Backend:   memory.New(),
		BlobStore: blobmem.New(),
	})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })

	var sawBlobTx bool
	err = bkv.Transact([]byte("pin"), func(tx *cluster.BlobTx) error {
		sawBlobTx = true // the closure parameter is *BlobTx by construction
		// Promoted-from-*Tx metadata op buffers into the same commit.
		return tx.Put([]byte("meta"), []byte("payload"))
	})
	if err != nil {
		t.Fatalf("Transact: %v", err)
	}
	if !sawBlobTx {
		t.Fatal("closure did not run")
	}
	got, err := bkv.Get([]byte("meta"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("Get = %q, want %q", got, "payload")
	}
}

// --- StageBlob / BindBlob / GetBlob (reader-atomic create) ---

// TestStageBlob_BytesDurableButUnreferenced pins that StageBlob streams the
// bytes to the final unit-keyed object key durably, yet they are UNREFERENCED
// until a bind commits: GetBlob is not-found before the bind (reader-atomic
// create, design section 11.2 / 11.3).
func TestStageBlob_BytesDurableButUnreferenced(t *testing.T) {
	store := blobmem.New()
	bkv, err := cluster.NewBlobKV(cluster.Config{NodeID: "n1", Backend: memory.New(), BlobStore: store})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })

	routeKey := []byte("slug-1")
	body := []byte("hello blob world")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	if ref.BlobID == "" {
		t.Fatal("StageBlob returned an empty BlobID")
	}
	if ref.Unit == "" {
		t.Fatal("StageBlob returned an empty Unit")
	}
	if string(ref.RouteShard) != "slug-1" {
		t.Fatalf("ref.RouteShard = %q, want the route shard key %q", ref.RouteShard, "slug-1")
	}
	if ref.Size != int64(len(body)) {
		t.Fatalf("ref.Size = %d, want %d", ref.Size, len(body))
	}
	// Bytes durable in the object store at the final key.
	if store.Len() != 1 {
		t.Fatalf("object store holds %d objects, want 1 (the staged blob)", store.Len())
	}
	// ... but UNREFERENCED: GetBlob cannot see it (no bound pointer yet).
	if _, _, err := bkv.GetBlob(context.Background(), routeKey, ref.BlobID); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("GetBlob before bind = %v, want blob.ErrNotFound (reader-atomic create)", err)
	}
}

// TestBindBlob_MakesGetBlobResolve pins the full reader-atomic create: stage,
// then bind the pointer co-committed with the app's metadata in one transaction,
// after which GetBlob streams the exact bytes back and the metadata is present.
func TestBindBlob_MakesGetBlobResolve(t *testing.T) {
	store := blobmem.New()
	bkv, err := cluster.NewBlobKV(cluster.Config{NodeID: "n1", Backend: memory.New(), BlobStore: store})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })

	routeKey := []byte("slug-7")
	body := []byte("the quick brown fox")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}

	// Bind the blob + write the app's metadata in ONE transaction (co-commit).
	// The metadata key is hash-tagged with the route key so it co-routes with the
	// bref onto the same shard (the cross-shard guard admits them into one tx).
	metaKey := []byte("{slug-7}/paste")
	err = bkv.Transact(routeKey, func(tx *cluster.BlobTx) error {
		if err := tx.BindBlob(ref); err != nil {
			return err
		}
		return tx.Put(metaKey, []byte("paste-metadata"))
	})
	if err != nil {
		t.Fatalf("Transact(bind+meta): %v", err)
	}

	// GetBlob now resolves and streams the exact bytes.
	rc, size, err := bkv.GetBlob(context.Background(), routeKey, ref.BlobID)
	if err != nil {
		t.Fatalf("GetBlob after bind: %v", err)
	}
	defer func() { _ = rc.Close() }()
	if size != int64(len(body)) {
		t.Fatalf("GetBlob size = %d, want %d", size, len(body))
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read blob stream: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("GetBlob bytes = %q, want %q", got, body)
	}
	// The metadata co-committed.
	meta, err := bkv.Get(metaKey)
	if err != nil {
		t.Fatalf("Get(metadata): %v", err)
	}
	if string(meta) != "paste-metadata" {
		t.Fatalf("metadata = %q, want %q", meta, "paste-metadata")
	}
}

// TestBindBlob_PointerCarriesContentHash pins that an app-supplied ContentHash on
// the ref is carried verbatim into the persisted pointer (the app's integrity /
// dedup hash slot, design section 11.6).
func TestBindBlob_PointerCarriesContentHash(t *testing.T) {
	store := blobmem.New()
	bkv, err := cluster.NewBlobKV(cluster.Config{NodeID: "n1", Backend: memory.New(), BlobStore: store})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })

	routeKey := []byte("slug-h")
	body := []byte("payload")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	ref.ContentHash = "sha256:deadbeef"
	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error { return tx.BindBlob(ref) }); err != nil {
		t.Fatalf("Transact(bind): %v", err)
	}
	// Read the raw pointer back through the routed Get on the bref key.
	enc, err := bkv.Get(cluster.BrefKeyForTest(ref))
	if err != nil {
		t.Fatalf("Get(brefKey): %v", err)
	}
	ptr, err := blob.DecodePointer(enc)
	if err != nil {
		t.Fatalf("DecodePointer: %v", err)
	}
	if ptr.ContentHash != "sha256:deadbeef" {
		t.Fatalf("pointer.ContentHash = %q, want %q", ptr.ContentHash, "sha256:deadbeef")
	}
	if ptr.ObjKey != blob.FinalKey(ref.Unit, ref.BlobID) {
		t.Fatalf("pointer.ObjKey = %q, want %q", ptr.ObjKey, blob.FinalKey(ref.Unit, ref.BlobID))
	}
}

// --- UnbindBlob (atomic delete) ---

// TestUnbindBlob_AtomicDelete pins that unbinding the pointer + deleting the
// app's metadata in ONE transaction makes GetBlob not-found again (the bytes are
// now unreferenced, to be reclaimed by the sweep). Atomic delete, section 11.3.
func TestUnbindBlob_AtomicDelete(t *testing.T) {
	store := blobmem.New()
	bkv, err := cluster.NewBlobKV(cluster.Config{NodeID: "n1", Backend: memory.New(), BlobStore: store})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })

	routeKey := []byte("slug-9")
	body := []byte("to be deleted")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	// Hash-tagged with the route key so it co-routes with the bref (one shard).
	metaKey := []byte("{slug-9}/paste")
	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error {
		if err := tx.BindBlob(ref); err != nil {
			return err
		}
		return tx.Put(metaKey, []byte("m"))
	}); err != nil {
		t.Fatalf("Transact(bind): %v", err)
	}

	// Unbind the pointer + delete the metadata in one transaction.
	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error {
		if err := tx.UnbindBlob(ref); err != nil {
			return err
		}
		return tx.Delete(metaKey)
	}); err != nil {
		t.Fatalf("Transact(unbind): %v", err)
	}

	// GetBlob is not-found again (pointer gone). The bytes still sit in the store
	// (unreferenced) until the sweep reclaims them.
	if _, _, err := bkv.GetBlob(context.Background(), routeKey, ref.BlobID); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("GetBlob after unbind = %v, want blob.ErrNotFound", err)
	}
	if store.Len() != 1 {
		t.Fatalf("object store holds %d objects, want 1 (now-orphaned bytes await the sweep)", store.Len())
	}
}

// TestTxDelete_BrefKeyEquivalentToUnbind pins that an ordinary tx.Delete of the
// bref key removes the pointer exactly as UnbindBlob does (UnbindBlob is a
// convenience over tx.Delete(brefKey), design section 11.3).
func TestTxDelete_BrefKeyEquivalentToUnbind(t *testing.T) {
	store := blobmem.New()
	bkv, err := cluster.NewBlobKV(cluster.Config{NodeID: "n1", Backend: memory.New(), BlobStore: store})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })

	routeKey := []byte("slug-eq")
	body := []byte("x")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error { return tx.BindBlob(ref) }); err != nil {
		t.Fatalf("Transact(bind): %v", err)
	}
	// Delete via the raw bref key (what an app's tx.Delete(blobKey) compiles to).
	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error {
		return tx.Delete(cluster.BrefKeyForTest(ref))
	}); err != nil {
		t.Fatalf("Transact(tx.Delete bref): %v", err)
	}
	if _, _, err := bkv.GetBlob(context.Background(), routeKey, ref.BlobID); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("GetBlob after raw bref delete = %v, want blob.ErrNotFound", err)
	}
}

// --- brefKey co-routing rule (design section 11.5) ---

// TestBrefKey_Shape pins the literal TOKEN-FREE bref key layout
// bref/{<routeShard>}/<blobid> (design section 12): the unit token is NOT a key
// segment, so the key is identical before and after a reshard (#471). Unit is
// set on the ref but must NOT appear in the key.
func TestBrefKey_Shape(t *testing.T) {
	ref := cluster.BlobRef{Unit: "0-13", RouteShard: []byte("slug-7"), BlobID: "deadbeef"}
	got := cluster.BrefKeyForTest(ref)
	want := []byte("bref/{slug-7}/deadbeef")
	if !bytes.Equal(got, want) {
		t.Fatalf("brefKey = %q, want %q", got, want)
	}
}

// TestBrefKey_TokenFree pins the load-bearing reshard-transparency property: two
// refs that differ ONLY in Unit (the routed token, which a reshard changes)
// produce the IDENTICAL bref key. This is what makes a post-reshard read
// reconstruct the same key the pre-reshard bind wrote (#471).
func TestBrefKey_TokenFree(t *testing.T) {
	gen0 := cluster.BlobRef{Unit: "0-13", RouteShard: []byte("slug-7"), BlobID: "deadbeef"}
	gen1 := cluster.BlobRef{Unit: "1-5", RouteShard: []byte("slug-7"), BlobID: "deadbeef"}
	if !bytes.Equal(cluster.BrefKeyForTest(gen0), cluster.BrefKeyForTest(gen1)) {
		t.Fatalf("brefKey is NOT token-free: gen0=%q gen1=%q (a reshard would make the read miss)",
			cluster.BrefKeyForTest(gen0), cluster.BrefKeyForTest(gen1))
	}
}

// TestBrefKey_CoRoutesWithMetadata is the load-bearing co-routing assertion: the
// DEFAULT ring.ShardKey (the cluster's shard-key extractor when ShardKeyFn is
// unset) applied to the bref key must return the EXACT route shard key the app's
// metadata shards on. That equality is what makes the pointer land on the same
// unit as the metadata, so the cross-shard guard admits them into one
// transaction (design section 11.5).
func TestBrefKey_CoRoutesWithMetadata(t *testing.T) {
	cases := []struct {
		name       string
		routeShard string
	}{
		{"simple slug", "slug-7"},
		{"id-like", "paste/abc123"},
		{"with dashes", "0-13-not-a-unit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ref := cluster.BlobRef{Unit: "0-13", RouteShard: []byte(tc.routeShard), BlobID: "id"}
			bref := cluster.BrefKeyForTest(ref)
			// ring.ShardKey extracts the hash-tagged segment - the route shard key.
			extracted := ring.ShardKey(bref)
			if string(extracted) != tc.routeShard {
				t.Fatalf("ring.ShardKey(%q) = %q, want the route shard key %q (pointer would NOT co-route with metadata)",
					bref, extracted, tc.routeShard)
			}
		})
	}
}

// TestBrefKey_PureFunctionOfRef pins that brefKey depends only on the BlobRef's
// RouteShard/Unit/BlobID, so it is reproducible from the ref alone (Bind, Unbind,
// and Get all reconstruct the same key without re-resolving routing).
func TestBrefKey_PureFunctionOfRef(t *testing.T) {
	ref := cluster.BlobRef{Unit: "0-2", RouteShard: []byte("s"), BlobID: "b"}
	a := cluster.BrefKeyForTest(ref)
	b := cluster.BrefKeyForTest(ref)
	if !bytes.Equal(a, b) {
		t.Fatalf("brefKey not deterministic: %q vs %q", a, b)
	}
}
