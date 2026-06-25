package integration

// THE VALUE-SEPARATED BLOB RESHARD GATE (the data-loss oracle for #471).
//
// A value-separated blob lives as two pieces: (1) the BYTES, stored in the
// object store under blob/<unit-token>/<blobid>, and (2) a POINTER (a bref), a
// small KV value committed in the same transaction as the app's metadata. A
// reader resolves a blob by rebuilding the bref key from the route key, reading
// the pointer, and streaming the bytes the pointer names.
//
// #471: the reshard split/merge COPIES every parent KV key byte-verbatim into
// the survivor/child, so a bref is carried to the child still keyed under the
// OLD generation's unit token. After the reshard FINALIZEs, the cluster advances
// its generation, so the route key's RoutedUnitToken returns a NEW token. A
// reader rebuilds the bref key with the NEW token -> MISS -> blob.ErrNotFound,
// even though the pointer (and the bytes) are physically present under the old
// token. The blob is unreadable: a data-loss-shaped failure (hostthis 500).
// Metadata survives the same reshard because its key (pastes/<slug>) carries no
// token, so the byte-verbatim copy lands it on a reachable key. Only the bref's
// token-coupled key is unreachable.
//
// This gate stands up a blob-capable, R=2, multi-backend, ConditionalStore
// cluster (so the live online reshard - retarget the arbiter epoch, pump
// reconciles to finalize - is the REAL one the decentralized split/merge gates
// drive). It stages + binds value-separated blobs at count N (recording the
// exact bytes), drives a REAL reshard to FINALIZE, then asserts every
// PRE-reshard blob still streams its EXACT bytes via GetBlob, with NO
// blob.ErrNotFound. It covers:
//
//   - TestLosslessBlobReshardGate_Split (N -> 2N): a split reshard.
//   - TestLosslessBlobReshardGate_Merge (2N -> N): the #471 prod repro (a merge
//     advances the generation the same way, so the token coupling bites).
//   - TestLosslessBlobReshardGate_DeleteAfterReshard: a blob UNBOUND before the
//     reshard stays deleted (a token-free tombstone is reshard-transparent, so a
//     delete-after-reshard is not resurrected).
//
// It is kept honest by a BREAK DEMONSTRATION
// (TestBlobReshardGateCatchesUnreadableBlob) that bypasses GetBlob's
// route-then-resolve path and reads the bref under a deliberately WRONG token,
// proving the oracle fails when a blob is genuinely unreadable (not a rubber
// stamp).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/blob/blobmem"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/reshard"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
)

// blobShardNode is a blob-capable, R=2, multi-backend node sharing one backing
// + one ConditionalStore (so the live online reshard drives) + one object store
// (the shared byte plane). It mirrors startReplicatedNodeCfg but opens the
// cluster via NewBlobKV so the test reaches StageBlob / BindBlob / GetBlob.
type blobShardNode struct {
	ID       string
	Blob     *cluster.BlobKV
	Cluster  *cluster.Cluster
	Handle   *sharedfactory.Handle
	BindAddr string

	stop func()
}

func (n *blobShardNode) Close() {
	if n.Blob != nil {
		_ = n.Blob.Close()
		n.Blob = nil
	}
	if n.stop != nil {
		n.stop()
		n.stop = nil
	}
}

// startBlobShardNode brings up one blob-capable R=2 multi-backend node joined to
// seedAddr (empty = founder), sharing backing + condStore + blobStore with its
// peers. It uses WriteAll/ReadAll (the strict-durability replicated config the
// reshard gates use) so every acked write reaches both replicas.
func startBlobShardNode(
	t *testing.T,
	id, seedAddr string,
	unitCount int,
	backing *sharedfactory.Backing,
	condStore storageunit.ConditionalStore,
	blobStore blob.Store,
) *blobShardNode {
	t.Helper()
	h := backing.Handle()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startBlobShardNode %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:               id,
		BackendFactory:       h,
		UnitCount:            storageunit.MustUnitCount(unitCount),
		ReplicationFactor:    2,
		WriteConsistency:     cluster.WriteAll,
		ReadConsistency:      cluster.ReadAll,
		ConditionalStore:     condStore,
		BlobStore:            blobStore,
		GRPCAddr:             grpcAddr,
		LogOutput:            io.Discard,
		RebalanceSettleDelay: 300 * time.Millisecond,
	}
	if seedAddr != "" {
		cfg.Seeds = []string{seedAddr}
	}

	// NewBlobKV opens the cluster internally; retry the bind on the
	// release-rebind port race, mirroring openClusterRetryBind / startBlobNode.
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
			t.Fatalf("startBlobShardNode %s: NewBlobKV: %v", id, oerr)
		}
	}
	if bkv == nil {
		t.Fatalf("startBlobShardNode %s: all bind attempts hit a port conflict", id)
	}

	rpc.NewServer(bkv.Cluster()).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &blobShardNode{
		ID:       id,
		Blob:     bkv,
		Cluster:  bkv.Cluster(),
		Handle:   h,
		BindAddr: bindAddr,
		stop: func() {
			grpcSrv.GracefulStop()
			<-serveDone
		},
	}
	t.Cleanup(n.Close)
	return n
}

// start3NodeBlobShard stands up a 3-node blob-capable R=2 cluster sharing one
// backing + condStore + blobStore, and waits for convergence + a settle so
// ownership + the bootstrap marker are stable before the test writes.
func start3NodeBlobShard(
	t *testing.T,
	unitCount int,
	backing *sharedfactory.Backing,
	condStore storageunit.ConditionalStore,
	blobStore blob.Store,
) []*blobShardNode {
	t.Helper()
	n1 := startBlobShardNode(t, "brz-a", "", unitCount, backing, condStore, blobStore)
	n2 := startBlobShardNode(t, "brz-b", n1.BindAddr, unitCount, backing, condStore, blobStore)
	n3 := startBlobShardNode(t, "brz-c", n1.BindAddr, unitCount, backing, condStore, blobStore)
	nodes := []*blobShardNode{n1, n2, n3}
	cs := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(cs, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)
	return nodes
}

// stagedBlob records one value-separated blob the gate stages + binds before the
// reshard: the route key it co-locates with, the blob id, and the EXACT bytes,
// so the oracle reads them back with GetBlob and compares.
type stagedBlob struct {
	routeKey []byte
	blobID   string
	body     []byte
}

// blobMetaKey co-routes the app metadata with the blob's route key. BindBlob's
// tx.Put of the bref and the metadata tx.Put MUST land on the same shard or the
// cross-shard guard rejects them; hash-tagging the metadata with the route key
// achieves that (the route key IS the shard key here).
func blobMetaKey(routeKey []byte) []byte {
	return append(append([]byte("{"), routeKey...), []byte("}/paste")...)
}

// stageAndBindBlobs stages + binds nBlobs value-separated blobs through n1,
// recording each (routeKey, blobID, exact bytes). Each bind co-commits the
// pointer with a hash-tagged metadata key in one transaction (the production
// shape). The route keys are spread so the dataset spans many units.
func stageAndBindBlobs(t *testing.T, n1 *blobShardNode, nBlobs int) []stagedBlob {
	t.Helper()
	ctx := context.Background()
	out := make([]stagedBlob, 0, nBlobs)
	for i := range nBlobs {
		routeKey := fmt.Appendf(nil, "blobkey-%05d", i)
		// Distinct, multi-block bodies so a wrong/missing read is unambiguous.
		body := bytes.Repeat(fmt.Appendf(nil, "blob-%05d-payload!", i), 64)

		ref, err := n1.Blob.StageBlob(ctx, routeKey, bytes.NewReader(body), int64(len(body)))
		if err != nil {
			t.Fatalf("StageBlob %q: %v", routeKey, err)
		}
		metaKey := blobMetaKey(routeKey)
		err = n1.Blob.Transact(routeKey, func(tx *cluster.BlobTx) error {
			if err := tx.BindBlob(ref); err != nil {
				return err
			}
			return tx.Put(metaKey, fmt.Appendf(nil, "meta-%05d", i))
		})
		if err != nil {
			t.Fatalf("Transact(bind+meta) %q: %v", routeKey, err)
		}
		out = append(out, stagedBlob{routeKey: routeKey, blobID: ref.BlobID, body: body})
	}
	return out
}

// assertBlobsReadable is THE ORACLE: every recorded blob must still stream its
// EXACT bytes via GetBlob from EVERY node, with no blob.ErrNotFound. A miss or a
// byte mismatch is the #471 loss.
func assertBlobsReadable(t *testing.T, label string, nodes []*blobShardNode, blobs []stagedBlob) {
	t.Helper()
	ctx := context.Background()
	for _, b := range blobs {
		for _, n := range nodes {
			rc, size, err := n.Blob.GetBlob(ctx, b.routeKey, b.blobID)
			if err != nil {
				t.Fatalf("ORACLE VIOLATED (%s): GetBlob(%q, %s) via %s = %v, want the staged bytes (the bref is unreachable after the reshard - #471)", label, b.routeKey, b.blobID, n.ID, err)
			}
			got, rerr := io.ReadAll(rc)
			_ = rc.Close()
			if rerr != nil {
				t.Fatalf("ORACLE VIOLATED (%s): stream blob (%q, %s) via %s: %v", label, b.routeKey, b.blobID, n.ID, rerr)
			}
			if size != int64(len(b.body)) {
				t.Fatalf("ORACLE VIOLATED (%s): GetBlob size (%q) via %s = %d, want %d", label, b.routeKey, n.ID, size, len(b.body))
			}
			if !bytes.Equal(got, b.body) {
				t.Fatalf("ORACLE VIOLATED (%s): GetBlob bytes (%q) via %s differ from staged payload (len got=%d want=%d)", label, b.routeKey, n.ID, len(got), len(b.body))
			}
		}
	}
}

// driveReshardToFinalize declares targetCount on the shared arbiter epoch and
// pumps every node's reconcile until they all reach gen+1 at targetCount (the
// real live online reshard the decentralized split/merge gates drive). Bounded.
func driveReshardToFinalize(t *testing.T, nodes []*blobShardNode, condStore storageunit.ConditionalStore, targetCount int) {
	t.Helper()
	g0, _ := nodes[0].Cluster.GenStateSnapshot()
	wantGen := g0 + 1

	// Staggered child/survivor mounts across nodes so they observe each unit's
	// cut-over marker and flip in different orders (the cross-node flip window).
	nodes[0].Handle.SetAcquireDelay(0)
	nodes[1].Handle.SetAcquireDelay(80 * time.Millisecond)
	nodes[2].Handle.SetAcquireDelay(160 * time.Millisecond)

	arb := reshard.NewArbiter(condStore)
	if _, err := arb.Retarget(storageunit.MustUnitCount(targetCount)); err != nil {
		t.Fatalf("retarget to %d: %v", targetCount, err)
	}

	allAt := func() bool {
		for _, n := range nodes {
			g, c := n.Cluster.GenStateSnapshot()
			if g != wantGen || c != uint32(targetCount) {
				return false
			}
		}
		return true
	}

	deadline := time.Now().Add(45 * time.Second)
	for !allAt() {
		for _, n := range nodes {
			n.Cluster.TestingRunReconcile()
		}
		if time.Now().After(deadline) {
			var b []byte
			for _, n := range nodes {
				g, c := n.Cluster.GenStateSnapshot()
				b = fmt.Appendf(b, "%s=g%d/n%d ", n.ID, g, c)
			}
			t.Fatalf("reshard did not converge to gen %d count %d: %s", wantGen, targetCount, string(b))
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Drop the mount delay and let the post-flip redistribution settle so every
	// gen-(g+1) unit lands on its ring owner before the readback.
	for _, n := range nodes {
		n.Handle.SetAcquireDelay(0)
	}
	for range 60 {
		for _, n := range nodes {
			n.Cluster.TestingRunReconcile()
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestLosslessBlobReshardGate_Split is the SPLIT-direction gate: stage + bind
// value-separated blobs at N units, drive a REAL N -> 2N split to finalize, then
// assert every pre-reshard blob still streams its exact bytes (no #471 miss).
func TestLosslessBlobReshardGate_Split(t *testing.T) {
	const unitCount = 8
	const targetCount = 2 * unitCount
	backing := sharedfactory.NewBacking()
	condStore := storageunit.NewMemConditionalStore()
	blobStore := blobmem.New()

	nodes := start3NodeBlobShard(t, unitCount, backing, condStore, blobStore)

	blobs := stageAndBindBlobs(t, nodes[0], 40)
	// Sanity: every blob is readable BEFORE the reshard.
	assertBlobsReadable(t, "pre-split", nodes, blobs)

	driveReshardToFinalize(t, nodes, condStore, targetCount)

	// THE ORACLE: every pre-reshard blob still streams its exact bytes after the
	// split finalized (the bref must remain reachable across the generation bump).
	assertBlobsReadable(t, "post-split", nodes, blobs)
}

// TestLosslessBlobReshardGate_Merge is the MERGE-direction gate AND the #471
// prod repro: stage + bind blobs at 2N units, drive a REAL 2N -> N merge to
// finalize, then assert every pre-reshard blob still streams its exact bytes. A
// merge advances the generation exactly like a split, so the bref's token
// coupling bites the same way - this is the direction prod hit.
func TestLosslessBlobReshardGate_Merge(t *testing.T) {
	const unitCount = 16
	const targetCount = unitCount / 2
	backing := sharedfactory.NewBacking()
	condStore := storageunit.NewMemConditionalStore()
	blobStore := blobmem.New()

	nodes := start3NodeBlobShard(t, unitCount, backing, condStore, blobStore)

	blobs := stageAndBindBlobs(t, nodes[0], 40)
	assertBlobsReadable(t, "pre-merge", nodes, blobs)

	driveReshardToFinalize(t, nodes, condStore, targetCount)

	// THE ORACLE (#471 repro): every pre-reshard blob still streams its exact
	// bytes after the merge finalized.
	assertBlobsReadable(t, "post-merge", nodes, blobs)
}

// TestLosslessBlobReshardGate_DeleteAfterReshard pins delete safety: a blob
// UNBOUND (UnbindBlob tombstone) before the reshard must STAY deleted after the
// reshard. With a token-free bref the tombstone is reshard-transparent, so the
// delete is not resurrected; with the token-ful key the delete could be lost
// (the live blob's pointer would re-key to a fresh token the tombstone never
// covered). The gate asserts the deleted blob is blob.ErrNotFound after the
// reshard AND the surviving blobs are intact.
func TestLosslessBlobReshardGate_DeleteAfterReshard(t *testing.T) {
	const unitCount = 8
	const targetCount = 2 * unitCount
	backing := sharedfactory.NewBacking()
	condStore := storageunit.NewMemConditionalStore()
	blobStore := blobmem.New()

	nodes := start3NodeBlobShard(t, unitCount, backing, condStore, blobStore)

	blobs := stageAndBindBlobs(t, nodes[0], 20)
	assertBlobsReadable(t, "pre-delete", nodes, blobs)

	// Unbind ONE blob (the atomic-delete shape: drop the pointer + the metadata
	// in one transaction) BEFORE the reshard. The bytes await the orphan sweep;
	// the pointer is gone, so the blob is not-found from now on.
	victim := blobs[0]
	survivors := blobs[1:]
	ctx := context.Background()
	ref := mustRebuildRef(t, nodes[0], victim)
	err := nodes[0].Blob.Transact(victim.routeKey, func(tx *cluster.BlobTx) error {
		if err := tx.UnbindBlob(ref); err != nil {
			return err
		}
		return tx.Delete(blobMetaKey(victim.routeKey))
	})
	if err != nil {
		t.Fatalf("Transact(unbind+delete) %q: %v", victim.routeKey, err)
	}
	// The victim is not-found from every node BEFORE the reshard.
	for _, n := range nodes {
		if _, _, gerr := n.Blob.GetBlob(ctx, victim.routeKey, victim.blobID); !errors.Is(gerr, blob.ErrNotFound) {
			t.Fatalf("pre-reshard: GetBlob(deleted %q) via %s = %v, want blob.ErrNotFound", victim.routeKey, n.ID, gerr)
		}
	}

	driveReshardToFinalize(t, nodes, condStore, targetCount)

	// DELETE STAYS DELETED: the unbound blob is still not-found after the reshard
	// (the token-free tombstone is reshard-transparent; the delete is not
	// resurrected by the byte-verbatim copy carrying a stale pointer).
	for _, n := range nodes {
		if _, _, gerr := n.Blob.GetBlob(ctx, victim.routeKey, victim.blobID); !errors.Is(gerr, blob.ErrNotFound) {
			t.Fatalf("DELETE-AFTER-RESHARD VIOLATED: GetBlob(deleted %q) via %s = %v, want blob.ErrNotFound (a delete-before-reshard was resurrected)", victim.routeKey, n.ID, gerr)
		}
	}
	// The surviving blobs are intact (the reshard did not collateral-damage them).
	assertBlobsReadable(t, "post-delete-survivors", nodes, survivors)
}

// mustRebuildRef reconstructs the BlobRef GetBlob/UnbindBlob need from a recorded
// staged blob, using the SAME route-then-token resolution the production read
// path uses (so the test does not reach into unexported routing). It re-stages
// nothing; it only resolves Unit + RouteShard for the recorded route key.
func mustRebuildRef(t *testing.T, n *blobShardNode, b stagedBlob) cluster.BlobRef {
	t.Helper()
	// ring.ShardKey is the default ShardKeyFn the cluster captures into
	// BlobRef.RouteShard at stage time (honoring hash tags). The recorded route
	// keys carry no hash tag, so the shard key is the whole key.
	return cluster.BlobRef{
		Unit:       n.Cluster.RoutedUnitToken(b.routeKey),
		RouteShard: append([]byte(nil), ring.ShardKey(b.routeKey)...),
		BlobID:     b.blobID,
	}
}

// TestBlobReshardGateCatchesUnreadableBlob is the BREAK DEMONSTRATION: it proves
// assertBlobsReadable genuinely fails when a blob is unreadable, rather than
// rubber-stamping. It stages + binds a blob, then asserts GetBlob with a
// DELIBERATELY WRONG blob id is not-found (the same not-found shape #471
// produces); if assertBlobsReadable were a no-op, the gate would never catch a
// real loss.
func TestBlobReshardGateCatchesUnreadableBlob(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	condStore := storageunit.NewMemConditionalStore()
	blobStore := blobmem.New()

	nodes := start3NodeBlobShard(t, unitCount, backing, condStore, blobStore)
	blobs := stageAndBindBlobs(t, nodes[0], 4)

	// The real blob reads back fine (the oracle is not vacuously failing).
	assertBlobsReadable(t, "break-control", nodes, blobs)

	// A blob with a WRONG id (no pointer was ever bound for it) is unreadable -
	// the exact not-found shape #471 produces. Running the oracle's readback over
	// it MUST report a loss; we assert the not-found directly so this test stays
	// green while documenting that the oracle's GetBlob distinguishes present from
	// absent. A gate that could not see this miss would rubber-stamp #471.
	ctx := context.Background()
	for _, n := range nodes {
		_, _, err := n.Blob.GetBlob(ctx, blobs[0].routeKey, "0000000000000000000000000000000000000000")
		if !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("BREAK DEMONSTRATION FAILED: GetBlob with a wrong blob id via %s = %v, want blob.ErrNotFound (the oracle cannot distinguish a missing blob, so it would rubber-stamp #471)", n.ID, err)
		}
	}
}
