package cluster_test

// UnstageBlob (design section 13): exact-ref reclamation of staged bytes.
// Properties pinned:
//   - a staged, never-bound blob's bytes are deleted (13.2 step 2);
//   - a BOUND ref is refused with blob.ErrBound and the bytes survive (13.2
//     step 1, the guard);
//   - unstaging an already-deleted object is a no-op (Store.Delete idempotence,
//     so a recovery re-running a partial list converges);
//   - after UnbindBlob the ref is unstageable again (13.4, the scan-free
//     deletion path).
//
// Legacy mode (one node, no cluster stand-up), same fixture as the streaming
// tests.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/blob/blobmem"
	"github.com/Zamua/shale/pkg/cluster"
)

// stageOne stages a small payload and returns its ref, asserting the bytes
// landed in the store.
func stageOne(t *testing.T, bkv *cluster.BlobKV, store *blobmem.Store, routeKey []byte) cluster.BlobRef {
	t.Helper()
	body := []byte("unstage-me")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	has, err := store.Has(context.Background(), blob.FinalKey(ref.Unit, ref.BlobID))
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !has {
		t.Fatalf("staged object missing before the test body ran")
	}
	return ref
}

func TestUnstageBlob_DeletesStagedNeverBoundBytes(t *testing.T) {
	store := blobmem.New()
	bkv := newStreamingBlobKV(t, store)
	ref := stageOne(t, bkv, store, []byte("slug-unstage"))

	if err := bkv.UnstageBlob(context.Background(), ref); err != nil {
		t.Fatalf("UnstageBlob: %v", err)
	}
	has, err := store.Has(context.Background(), blob.FinalKey(ref.Unit, ref.BlobID))
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if has {
		t.Fatalf("object survived UnstageBlob")
	}
}

func TestUnstageBlob_RefusesBoundRef(t *testing.T) {
	store := blobmem.New()
	bkv := newStreamingBlobKV(t, store)
	routeKey := []byte("slug-bound")
	ref := stageOne(t, bkv, store, routeKey)

	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error { return tx.BindBlob(ref) }); err != nil {
		t.Fatalf("Transact(bind): %v", err)
	}

	err := bkv.UnstageBlob(context.Background(), ref)
	if !errors.Is(err, blob.ErrBound) {
		t.Fatalf("UnstageBlob on a bound ref: err = %v, want blob.ErrBound", err)
	}
	has, herr := store.Has(context.Background(), blob.FinalKey(ref.Unit, ref.BlobID))
	if herr != nil {
		t.Fatalf("Has: %v", herr)
	}
	if !has {
		t.Fatalf("bound blob's bytes were deleted despite the refusal")
	}
	// The refusal must leave the read path intact.
	rc, _, gerr := bkv.GetBlob(context.Background(), routeKey, ref.BlobID)
	if gerr != nil {
		t.Fatalf("GetBlob after refused unstage: %v", gerr)
	}
	got, cerr := io.ReadAll(rc)
	_ = rc.Close()
	if cerr != nil {
		t.Fatalf("read blob: %v", cerr)
	}
	if string(got) != "unstage-me" {
		t.Fatalf("blob content changed: %q", got)
	}
}

func TestUnstageBlob_IdempotentOnMissingObject(t *testing.T) {
	store := blobmem.New()
	bkv := newStreamingBlobKV(t, store)
	ref := stageOne(t, bkv, store, []byte("slug-idem"))

	if err := bkv.UnstageBlob(context.Background(), ref); err != nil {
		t.Fatalf("first UnstageBlob: %v", err)
	}
	if err := bkv.UnstageBlob(context.Background(), ref); err != nil {
		t.Fatalf("second UnstageBlob (object already gone): %v", err)
	}
}

func TestUnstageBlob_SucceedsAfterUnbind(t *testing.T) {
	store := blobmem.New()
	bkv := newStreamingBlobKV(t, store)
	routeKey := []byte("slug-unbind")
	ref := stageOne(t, bkv, store, routeKey)

	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error { return tx.BindBlob(ref) }); err != nil {
		t.Fatalf("Transact(bind): %v", err)
	}
	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error { return tx.UnbindBlob(ref) }); err != nil {
		t.Fatalf("Transact(unbind): %v", err)
	}

	if err := bkv.UnstageBlob(context.Background(), ref); err != nil {
		t.Fatalf("UnstageBlob after unbind: %v", err)
	}
	has, err := store.Has(context.Background(), blob.FinalKey(ref.Unit, ref.BlobID))
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if has {
		t.Fatalf("unbound blob's bytes survived UnstageBlob")
	}
}
