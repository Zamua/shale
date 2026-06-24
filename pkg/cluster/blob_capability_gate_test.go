package cluster_test

// Compile-time capability gate (design section 4 / 11.1, guarantee 5).
//
// The blob capability is meant to be visible IN THE TYPE: a metadata-only *KV
// has NO StageBlob / GetBlob / SweepOrphans, and its Transact yields a plain
// *Tx that has NO BindBlob / UnbindBlob. Reaching a blob op without having
// configured a blob store is therefore a COMPILE error, not a runtime nil
// dereference. There is no runtime assertion that can pin "this does not
// compile"; the demonstration is the commented-out call sites below, each of
// which the Go compiler would reject. Uncommenting any one breaks the build
// with the noted error - that IS the gate.
//
// To verify the gate by hand, uncomment a block and run `go vet ./pkg/cluster`;
// the build fails with the documented message. Left commented so the suite
// builds.

import (
	"bytes"
	"context"
	"testing"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/blob/blobmem"
	"github.com/Zamua/shale/pkg/cluster"
)

// TestCapabilityGate_PositiveSurface pins the POSITIVE half of the gate that
// CAN be asserted at compile+run time: a *BlobKV (and only a *BlobKV) exposes
// the blob ops, and because it embeds *KV it still satisfies any consumer that
// only needs the metadata surface. The compile-time NEGATIVE half (a *KV cannot
// reach a blob op) is demonstrated by the commented blocks below, which the
// compiler rejects.
//
// A consumer that needs only metadata accepts EITHER type; one that needs blobs
// accepts only *BlobKV. These two function values existing and being assignable
// is the in-type capability split, checked by the compiler when this file
// builds.
func TestCapabilityGate_PositiveSurface(t *testing.T) {
	bkv, err := cluster.NewBlobKV(cluster.Config{
		NodeID:    "n1",
		Backend:   memory.New(),
		BlobStore: blobmem.New(),
	})
	if err != nil {
		t.Fatalf("NewBlobKV: %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })

	kv, err := cluster.New(cluster.Config{NodeID: "n2", Backend: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	// metadataOnly accepts a *KV. A *BlobKV is assignable to it (embeds *KV), so
	// blob-capable code can be handed where only metadata is needed - the
	// superset relationship, checked by the compiler at the call sites below.
	metadataOnly := func(k *cluster.KV) error { return k.Put([]byte("k"), []byte("v")) }
	if err := metadataOnly(kv); err != nil {
		t.Fatalf("metadataOnly(*KV): %v", err)
	}
	if err := metadataOnly(bkv.KV); err != nil { // *BlobKV embeds *KV -> assignable
		t.Fatalf("metadataOnly(*BlobKV.KV): %v", err)
	}

	// blobRequiring accepts ONLY a *BlobKV. A plain *KV does NOT compile here
	// (the commented call in the negative block below proves it), so a consumer
	// that needs blobs cannot be wired with a metadata-only cluster. We assert
	// the wiring (a *BlobKV is accepted) and actually drive a blob op through it
	// so the surface is exercised, not just type-checked.
	blobRequiring := func(b *cluster.BlobKV) (cluster.BlobRef, error) {
		return b.StageBlob(context.Background(), []byte("rk"), bytes.NewReader([]byte("x")), 1)
	}
	if _, err := blobRequiring(bkv); err != nil {
		t.Fatalf("blobRequiring(*BlobKV): %v", err)
	}
}

// The NEGATIVE half of the gate. Each block is a call the Go compiler REJECTS
// because the type does not have the method. Uncommenting any one fails the
// build with the noted error; that compile failure is the capability gate. They
// are kept commented so the package builds.
//
// Setup the blocks assume:
//
//	kv, _ := cluster.New(cluster.Config{NodeID: "n", Backend: memory.New()})
//
//	// (A) *KV has no StageBlob:
//	//     kv.StageBlob(context.Background(), []byte("rk"), nil, 0)
//	// build error: kv.StageBlob undefined (type *cluster.KV has no field or method StageBlob)
//
//	// (B) *KV has no GetBlob:
//	//     kv.GetBlob(context.Background(), []byte("rk"), "id")
//	// build error: kv.GetBlob undefined (type *cluster.KV has no field or method GetBlob)
//
//	// (C) *KV has no SweepOrphans:
//	//     kv.SweepOrphans(context.Background(), time.Now(), time.Hour)
//	// build error: kv.SweepOrphans undefined (type *cluster.KV has no field or method SweepOrphans)
//
//	// (D) *KV.Transact yields a plain *Tx, which has NO BindBlob:
//	//     kv.Transact([]byte("pin"), func(tx *cluster.Tx) error { return tx.BindBlob(cluster.BlobRef{}) })
//	// build error: tx.BindBlob undefined (type *cluster.Tx has no field or method BindBlob)
//
//	// (E) *KV.Transact yields a plain *Tx, which has NO UnbindBlob:
//	//     kv.Transact([]byte("pin"), func(tx *cluster.Tx) error { return tx.UnbindBlob(cluster.BlobRef{}) })
//	// build error: tx.UnbindBlob undefined (type *cluster.Tx has no field or method UnbindBlob)
//
//	// (F) the *KV.Transact closure parameter is *Tx, NOT *BlobTx - a closure
//	//     typed for the blob tx does not match the metadata Transact signature:
//	//     kv.Transact([]byte("pin"), func(tx *cluster.BlobTx) error { return nil })
//	// build error: cannot use func(tx *cluster.BlobTx) error value as
//	//              func(*cluster.Tx) error value in argument to kv.Transact
