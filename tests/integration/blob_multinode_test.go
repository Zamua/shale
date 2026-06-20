package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/blob/blobmem"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rpc"
	"google.golang.org/grpc"
)

// blobNode is a cluster node fronted by the blob-capable *BlobKV surface, built
// through the production NewBlobKV constructor (cfg.BlobStore set) so the
// integration test exercises the real wiring, not a test-only wrapper.
type blobNode struct {
	ID       string
	Blob     *cluster.BlobKV
	BindAddr string

	stop func()
}

func (n *blobNode) Close() {
	if n.Blob != nil {
		_ = n.Blob.Close()
		n.Blob = nil
	}
	if n.stop != nil {
		n.stop()
		n.stop = nil
	}
}

// startBlobNode brings up one blob-capable node joined to seedAddr (empty = the
// first node). It mirrors startTestNodeWithReplication's gRPC + memberlist
// wiring, but builds the cluster via NewBlobKV with the SHARED object store so
// every node reaches the same byte plane. The gRPC server is registered against
// the underlying *Cluster so routed pointer ops forward between nodes.
func startBlobNode(t *testing.T, id, seedAddr string, store blob.Store) *blobNode {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startBlobNode %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:                  id,
		Backend:                 memory.New(),
		BlobStore:               store,
		GRPCAddr:                grpcAddr,
		LogOutput:               io.Discard,
		RebalanceSettleDelay:    500 * time.Millisecond,
		RebalanceGraceDuration:  3 * time.Second,
		RebalanceHandoffTimeout: 4 * time.Second,
		ReplicationFactor:       1,
	}
	if seedAddr != "" {
		cfg.Seeds = []string{seedAddr}
	}

	// NewBlobKV opens the cluster internally; retry the bind on the
	// release-rebind port race, mirroring openClusterRetryBind.
	var bkv *cluster.BlobKV
	var bindAddr string
	const maxAttempts = 8
	for range maxAttempts {
		bindAddr = hostPort(freePort(t))
		cfg.BindAddr = bindAddr
		b, oerr := cluster.NewBlobKV(cfg)
		if oerr == nil {
			bkv = b
			break
		}
		if !isBindConflict(oerr) {
			t.Fatalf("startBlobNode %s: NewBlobKV: %v", id, oerr)
		}
	}
	if bkv == nil {
		t.Fatalf("startBlobNode %s: all bind attempts hit a port conflict", id)
	}

	rpc.NewServer(bkv.Cluster()).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &blobNode{
		ID:       id,
		Blob:     bkv,
		BindAddr: bindAddr,
		stop: func() {
			grpcSrv.GracefulStop()
			<-serveDone
		},
	}
	t.Cleanup(n.Close)
	return n
}

// TestBlobMultiNode_PointerRoutesBytesShared is the phase-2 multi-node
// integration test (design section 11.10): it proves the value-separation
// property end to end across a real 3-node in-process cluster -
//
//   - the BYTE plane is SHARED object storage reachable from every node (one
//     blobmem.Store backs all three nodes), so StageBlob and the GetBlob byte
//     stream never cross the cluster's gRPC plane; and
//   - the POINTER routes through the ring exactly like metadata: BindBlob's
//     tx.Put of the bref key co-commits on the unit owner, and a GetBlob from a
//     DIFFERENT node resolves it via a routed Get before streaming the bytes
//     from the shared store.
//
// The blob is staged on one node, its pointer is bound through a node that does
// NOT own the route key's shard (forcing the pointer commit over the wire), and
// it is read back from a third node - so the pointer demonstrably crosses the
// RPC boundary while the bytes demonstrably do not.
func TestBlobMultiNode_PointerRoutesBytesShared(t *testing.T) {
	// ONE shared object store stands in for the shared bucket every node reaches
	// (the byte plane is shared storage, section 2.1). All three nodes point at
	// it; a blob staged via one node is byte-reachable from any node.
	store := blobmem.New()
	n1 := startBlobNode(t, "bn1", "", store)
	n2 := startBlobNode(t, "bn2", n1.BindAddr, store)
	n3 := startBlobNode(t, "bn3", n1.BindAddr, store)

	clusters := []*cluster.Cluster{n1.Blob.Cluster(), n2.Blob.Cluster(), n3.Blob.Cluster()}
	waitForClusterReady(t, clusters, 15*time.Second)

	// Choose a route key whose shard owner is NOT bn1, so binding its pointer via
	// bn1 forwards the commit across gRPC to the owner (the cross-node pointer
	// route is the thing under test).
	routeKey := []byte(findKeyNotOwnedBy(t, n1.Blob.Cluster(), "bn1", 1000))

	ctx := context.Background()
	body := bytes.Repeat([]byte("multinode-blob-payload!"), 4096) // ~92 KiB, streamed
	// Stage on bn1: bytes go straight to the shared store, off the RPC path.
	ref, err := n1.Blob.StageBlob(ctx, routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob via bn1: %v", err)
	}

	// Before the bind commits, the blob is UNREFERENCED: a GetBlob from ANY node
	// is not-found (reader-atomic create), even though the bytes are durable.
	for name, b := range map[string]*cluster.BlobKV{"bn1": n1.Blob, "bn2": n2.Blob, "bn3": n3.Blob} {
		if _, _, err := b.GetBlob(ctx, routeKey, ref.BlobID); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("GetBlob via %s before bind = %v, want blob.ErrNotFound", name, err)
		}
	}

	// Bind the pointer + the app metadata in one transaction, pinned on the route
	// key, issued via bn1 (a non-owner) so the commit routes to the owner.
	// The app's metadata key MUST co-route with the route key (and hence the
	// bref) onto the same shard, or the bind's tx.Put + the metadata tx.Put are a
	// cross-shard transaction the guard rejects. Real apps achieve this by
	// hash-tagging the metadata with the same shard key the blob routes on; here
	// the route key IS the shard key, so the metadata key carries it as a tag.
	metaKey := append(append([]byte("{"), routeKey...), []byte("}/paste")...)
	err = n1.Blob.Transact(routeKey, func(tx *cluster.BlobTx) error {
		if err := tx.BindBlob(ref); err != nil {
			return err
		}
		return tx.Put(metaKey, []byte("metadata"))
	})
	if err != nil {
		t.Fatalf("Transact(bind+meta) via bn1: %v", err)
	}

	// Read the blob back from bn3 (a third node): the pointer read routes to the
	// owner over gRPC, then the bytes stream from the shared store directly.
	rc, size, err := n3.Blob.GetBlob(ctx, routeKey, ref.BlobID)
	if err != nil {
		t.Fatalf("GetBlob via bn3 after bind: %v", err)
	}
	defer func() { _ = rc.Close() }()
	if size != int64(len(body)) {
		t.Fatalf("GetBlob size via bn3 = %d, want %d", size, len(body))
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("stream blob via bn3: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("GetBlob bytes via bn3 differ from staged payload (len got=%d want=%d)", len(got), len(body))
	}

	// The co-committed metadata is visible from bn2 as well (routed read).
	meta, err := n2.Blob.Get(metaKey)
	if err != nil {
		t.Fatalf("Get(metadata) via bn2: %v", err)
	}
	if string(meta) != "metadata" {
		t.Fatalf("metadata via bn2 = %q, want %q", meta, "metadata")
	}

	// Atomic delete from bn2: unbind the pointer + delete the metadata in one tx.
	// After it, GetBlob is not-found from every node (the bytes await the sweep).
	err = n2.Blob.Transact(routeKey, func(tx *cluster.BlobTx) error {
		if err := tx.UnbindBlob(ref); err != nil {
			return err
		}
		return tx.Delete(metaKey)
	})
	if err != nil {
		t.Fatalf("Transact(unbind+delete) via bn2: %v", err)
	}
	if _, _, err := n3.Blob.GetBlob(ctx, routeKey, ref.BlobID); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("GetBlob via bn3 after unbind = %v, want blob.ErrNotFound", err)
	}
}
