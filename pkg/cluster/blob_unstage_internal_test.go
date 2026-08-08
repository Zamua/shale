package cluster

// White-box pin for the UnstageBlob bound-ref guard's read consistency (design
// section 13.2): at R>1 the guard must not conclude "unbound" from sub-quorum
// evidence. With peers unreachable, the local replica's not-found is one
// witness of a required two - the guard must refuse (fail closed) rather than
// delete. Under the cluster's plain ReadNearest read (n=1) the same state
// reads as a clean not-found and the delete would proceed; this test is what
// separates the two.

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/blob/blobmem"
)

func TestUnstageBlob_GuardRefusesSubQuorumAbsence(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 3, backing, "n1", "n2", "n3")
	c.cfg.ReadConsistency = ReadNearest
	c.cfg.ReadTimeout = 2 * time.Second
	c.peerClientsBlocked = true
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}

	store := blobmem.New()
	bkv := &BlobKV{KV: &KV{c: c}, blobs: store}

	body := []byte("sub-quorum")
	ref, err := bkv.StageBlob(context.Background(), []byte("slug-subq"), bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}

	err = bkv.UnstageBlob(context.Background(), ref)
	if err == nil {
		t.Fatalf("UnstageBlob concluded 'unbound' from a single local miss with both peers unreachable; want a fail-closed refusal")
	}
	if errors.Is(err, blob.ErrBound) || errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("refusal has the wrong shape: %v", err)
	}
	has, herr := store.Has(context.Background(), blob.FinalKey(ref.Unit, ref.BlobID))
	if herr != nil {
		t.Fatalf("Has: %v", herr)
	}
	if !has {
		t.Fatalf("bytes were deleted on unverifiable absence")
	}
}
