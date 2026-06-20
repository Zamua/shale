package cluster_test

// Phase-2 streaming + reader-atomicity tests that the foundation suite
// (kv_test.go) leaves to round out the guarantees in the brief:
//
//   - a >1 MB blob round-trips through StageBlob -> BindBlob -> GetBlob, byte for
//     byte, exercising the streaming path with a payload too large to be an
//     accident of a small fixed buffer (guarantee: streaming, design section 1);
//   - the reader-atomic-create guarantee holds under transaction ABORT: a bind
//     buffered in a transaction that then aborts leaves the blob unreachable (the
//     bref Put rolls back with the rest of the write-set), so a half-done create
//     never exposes the bytes (design section 11.3).
//
// These run in legacy (non-multi) mode: one node, one keyspace, the full
// StageBlob/Bind/Get path without standing up a cluster.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand"
	"testing"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/blob/blobmem"
	"github.com/Zamua/shale/pkg/cluster"
)

// newStreamingBlobKV builds the legacy-mode *BlobKV these tests share over the
// supplied store.
func newStreamingBlobKV(t *testing.T, store *blobmem.Store) *cluster.BlobKV {
	t.Helper()
	bkv, err := cluster.NewBlobKV(cluster.Config{NodeID: "n1", Backend: memory.New(), BlobStore: store})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })
	return bkv
}

// TestBlob_LargeRoundTrip streams a >1 MB random payload through the full blob
// path and asserts the bytes come back identical (by sha256 + length, so a
// mismatch is reported without dumping megabytes). A payload this size cannot
// fit any fixed small buffer accidentally, so an equal round-trip proves the
// stream is piped end to end on both write (StageBlob) and read (GetBlob).
func TestBlob_LargeRoundTrip(t *testing.T) {
	store := blobmem.New()
	bkv := newStreamingBlobKV(t, store)

	// 3 MiB of deterministic-but-non-trivial bytes (a fixed seed keeps the test
	// reproducible; the content is not all-zeros so a truncation is detectable).
	const size = 3 << 20
	body := make([]byte, size)
	rng := rand.New(rand.NewSource(0xB10B))
	if _, err := rng.Read(body); err != nil {
		t.Fatalf("seed payload: %v", err)
	}
	wantSum := sha256.Sum256(body)

	routeKey := []byte("slug-large")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	if ref.Size != int64(size) {
		t.Fatalf("ref.Size = %d, want %d", ref.Size, size)
	}
	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error { return tx.BindBlob(ref) }); err != nil {
		t.Fatalf("Transact(bind): %v", err)
	}

	rc, gotSize, err := bkv.GetBlob(context.Background(), routeKey, ref.BlobID)
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	defer func() { _ = rc.Close() }()
	if gotSize != int64(size) {
		t.Fatalf("GetBlob size = %d, want %d", gotSize, size)
	}
	// Stream through a hasher (do NOT buffer the whole blob a second time just to
	// compare) so the read side is exercised as a stream too.
	h := sha256.New()
	n, err := io.Copy(h, rc)
	if err != nil {
		t.Fatalf("stream blob: %v", err)
	}
	if n != int64(size) {
		t.Fatalf("streamed %d bytes, want %d (truncated stream)", n, size)
	}
	if got := h.Sum(nil); !bytes.Equal(got, wantSum[:]) {
		t.Fatalf("round-tripped %d-byte blob differs: sha256 got %x, want %x", size, got, wantSum)
	}
}

// TestBlob_LargeRoundTrip_SizeUnknown pins that the >1 MB round-trip also works
// when the caller does not know the length up front (blob.SizeUnknown), the path
// hostthis takes when it streams a compressed body whose final length is unknown
// until EOF (design section 11.2 / blob.SizeUnknown).
func TestBlob_LargeRoundTrip_SizeUnknown(t *testing.T) {
	store := blobmem.New()
	bkv := newStreamingBlobKV(t, store)

	const size = (2 << 20) + 12345 // not a round number, to catch off-by-N
	body := make([]byte, size)
	rng := rand.New(rand.NewSource(0xFEED))
	if _, err := rng.Read(body); err != nil {
		t.Fatalf("seed payload: %v", err)
	}
	wantSum := sha256.Sum256(body)

	routeKey := []byte("slug-large-unknown")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), blob.SizeUnknown)
	if err != nil {
		t.Fatalf("StageBlob(SizeUnknown): %v", err)
	}
	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error { return tx.BindBlob(ref) }); err != nil {
		t.Fatalf("Transact(bind): %v", err)
	}

	rc, _, err := bkv.GetBlob(context.Background(), routeKey, ref.BlobID)
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	defer func() { _ = rc.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, rc); err != nil {
		t.Fatalf("stream blob: %v", err)
	}
	if got := h.Sum(nil); !bytes.Equal(got, wantSum[:]) {
		t.Fatalf("round-tripped SizeUnknown blob differs: sha256 got %x, want %x", got, wantSum)
	}
}

// TestBindBlob_RollsBackWithTransaction pins reader-atomic create under ABORT:
// a BindBlob buffered into a transaction whose fn then returns an error must NOT
// take effect. The bref Put is an ordinary write-set entry, so it rolls back
// with the app's metadata; afterward GetBlob is still not-found (the half-done
// create never exposed the bytes). The bytes remain durable-but-unreferenced (a
// future stage can be retried / the sweep reclaims them), so the object store
// still holds the one object.
func TestBindBlob_RollsBackWithTransaction(t *testing.T) {
	store := blobmem.New()
	bkv := newStreamingBlobKV(t, store)

	routeKey := []byte("slug-abort")
	body := []byte("never bound")
	ref, err := bkv.StageBlob(context.Background(), routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}

	// Bind inside a transaction that then aborts. The bind buffers the bref Put;
	// the abort discards the whole write-set, bref included.
	metaKey := []byte("{slug-abort}/paste")
	sentinel := errors.New("app aborted the create")
	err = bkv.Transact(routeKey, func(tx *cluster.BlobTx) error {
		if err := tx.BindBlob(ref); err != nil {
			return err
		}
		if err := tx.Put(metaKey, []byte("m")); err != nil {
			return err
		}
		return sentinel // abort AFTER buffering both writes
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Transact error = %v, want the abort sentinel", err)
	}

	// The bind rolled back: GetBlob is not-found (the bytes were never referenced).
	if _, _, err := bkv.GetBlob(context.Background(), routeKey, ref.BlobID); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("GetBlob after aborted bind = %v, want blob.ErrNotFound (bind must roll back)", err)
	}
	// The metadata rolled back too (the co-committed Put).
	if _, err := bkv.Get(metaKey); err == nil {
		t.Fatal("metadata survived an aborted transaction, want not-found (rollback)")
	}
	// The staged bytes are still in the store, unreferenced (await the sweep).
	if store.Len() != 1 {
		t.Fatalf("object store holds %d objects, want 1 (the unreferenced staged bytes)", store.Len())
	}
}
