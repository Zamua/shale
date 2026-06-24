package integration

// A >1 MB blob round-tripped across a real 3-node in-process cluster, exercising
// the co-location/routing guarantee AND the streaming guarantee together:
//
//   - the blob is staged on a node that does NOT own the route key's shard, so
//     the small pointer commit forwards over gRPC to the owner (the pointer
//     routes), while
//   - the multi-megabyte byte plane stays OFF the RPC path - the bytes go
//     node -> shared object store directly on both stage and read, proven by
//     reading them back from a THIRD node and matching the sha256.
//
// The 92 KiB sibling test (blob_multinode_test.go) pins the routing mechanics in
// detail; this one specifically pushes a payload over 1 MB through the same
// cross-node path so the streaming requirement is exercised end to end across
// nodes, not just in-process (brief: round a >1 MB blob through at least one
// test). Reuses startBlobNode + findKeyNotOwnedBy + waitForClusterReady from the
// integration harness.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/blob/blobmem"
	"github.com/Zamua/shale/pkg/cluster"
)

func TestBlobMultiNode_LargeBlobStreamsCrossNode(t *testing.T) {
	store := blobmem.New() // one shared byte plane reachable from every node
	n1 := startBlobNode(t, "lbn1", "", store)
	n2 := startBlobNode(t, "lbn2", n1.BindAddr, store)
	n3 := startBlobNode(t, "lbn3", n1.BindAddr, store)

	clusters := []*cluster.Cluster{n1.Blob.Cluster(), n2.Blob.Cluster(), n3.Blob.Cluster()}
	waitForClusterReady(t, clusters, 15*time.Second)

	// Route key whose shard owner is NOT lbn1, so binding via lbn1 forwards the
	// pointer commit across gRPC.
	routeKey := []byte(findKeyNotOwnedBy(t, n1.Blob.Cluster(), "lbn1", 1000))

	// 4 MiB of deterministic random bytes (fixed seed -> reproducible; non-trivial
	// content so a truncation/corruption is detectable). Compared by sha256 so a
	// failure does not dump megabytes.
	const size = 4 << 20
	body := make([]byte, size)
	rng := rand.New(rand.NewSource(0xC0FFEE))
	if _, err := rng.Read(body); err != nil {
		t.Fatalf("seed payload: %v", err)
	}
	wantSum := sha256.Sum256(body)

	ctx := context.Background()
	// Stage on lbn1: the 4 MiB stream goes straight to the shared store, off the
	// RPC path (lbn1 may not even own the shard - the byte plane does not care).
	ref, err := n1.Blob.StageBlob(ctx, routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob via lbn1: %v", err)
	}
	if ref.Size != int64(size) {
		t.Fatalf("ref.Size = %d, want %d", ref.Size, size)
	}

	// Before the bind, the blob is unreferenced from every node (reader-atomic).
	if _, _, err := n3.Blob.GetBlob(ctx, routeKey, ref.BlobID); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("GetBlob via lbn3 before bind = %v, want blob.ErrNotFound", err)
	}

	// Bind the pointer (routes to the owner over gRPC). The metadata co-routes via
	// the same hash tag so the bind + metadata Put are one single-shard tx.
	metaKey := append(append([]byte("{"), routeKey...), []byte("}/paste")...)
	err = n1.Blob.Transact(routeKey, func(tx *cluster.BlobTx) error {
		if err := tx.BindBlob(ref); err != nil {
			return err
		}
		return tx.Put(metaKey, []byte("metadata"))
	})
	if err != nil {
		t.Fatalf("Transact(bind+meta) via lbn1: %v", err)
	}

	// Read the 4 MiB back from a THIRD node: pointer read routes to the owner,
	// then the bytes stream from the shared store directly. Stream through a
	// hasher so the read side is exercised as a stream, not buffered whole.
	rc, gotSize, err := n3.Blob.GetBlob(ctx, routeKey, ref.BlobID)
	if err != nil {
		t.Fatalf("GetBlob via lbn3 after bind: %v", err)
	}
	defer func() { _ = rc.Close() }()
	if gotSize != int64(size) {
		t.Fatalf("GetBlob size via lbn3 = %d, want %d", gotSize, size)
	}
	h := sha256.New()
	n, err := io.Copy(h, rc)
	if err != nil {
		t.Fatalf("stream blob via lbn3: %v", err)
	}
	if n != int64(size) {
		t.Fatalf("streamed %d bytes via lbn3, want %d (truncated cross-node stream)", n, size)
	}
	if got := h.Sum(nil); !bytes.Equal(got, wantSum[:]) {
		t.Fatalf("cross-node %d-byte blob differs: sha256 got %x, want %x", size, got, wantSum)
	}
}
