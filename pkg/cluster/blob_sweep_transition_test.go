package cluster_test

// THE TORN-PASS DATA-LOSS regression (docs/design/blob-values.md 11.7, the
// single-snapshot topology guard). SweepOrphans' referenced-object set and its
// unit enumeration were two reads of a MOVING mount view: a unit ACQUIRED
// between them (a reconcile acquire during a rollout, a fence-evict re-acquire,
// a boot mount completing mid-pass) was enumerated for objects with NONE of its
// brefs in the referenced set, so every BOUND blob under it older than the
// grace was deleted - committed-data loss, observed live as a >3h-old bound
// canary blob vanishing mid-rollout while its metadata and pointer stayed
// intact ("blob: object not found" on a paste whose record listed fine).
//
// The repro drives the exact interleaving deterministically via the Testing
// mid-pass hook: unit U's mount is cleared before the pass (its brefs are
// invisible to the referenced scan), and the hook re-mounts it (a reconcile
// pass) between the scan and the enumeration. Pre-guard, the pass listed U's
// objects against a referenced set that predates the mount and DELETED the
// bound blob. Post-guard, the pass aborts fail-closed and the blob survives;
// the next calm pass sweeps normally.
//
// TWO guards now stand between this interleaving and the loss, and the pass is
// refused by whichever one the timing reaches first:
//
//   - THE COVERAGE GUARD (scanCoverageErr), which subsumes this scenario
//     entirely. A referenced scan on a node holding an owned position UNMOUNTED
//     is refused outright, because such a scan silently omits that position's
//     brefs and returns a short set with a clean end-of-iteration. That is the
//     precise input that makes this bug possible, so the pass can no longer
//     BEGIN with the gap. It fires first here.
//   - THE TOPOLOGY GUARD (sameUnitTokenSet), still load-bearing as the backstop
//     for the case the up-front check cannot predict: a node that starts FULLY
//     mounted, whose view moves after the referenced scan has already run. The
//     second test below pins that one directly, since this one no longer
//     reaches it.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/blob/blobmem"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

func TestSweepOrphans_AbortsWhenMountViewChangesMidPass(t *testing.T) {
	store := blobmem.New()
	bkv, err := cluster.NewBlobKV(cluster.Config{
		NodeID:         "solo",
		BackendFactory: memfactory.New(),
		UnitCount:      storageunit.MustUnitCount(2),
		BlobStore:      store,
	})
	if err != nil {
		t.Fatalf("NewBlobKV (multi): %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })
	c := bkv.Cluster()
	ctx := context.Background()

	// Stage + BIND a blob (a live pointer references it), then age the object
	// far past the grace so ONLY the referenced set protects it.
	routeKey := []byte("slug-bound")
	body := []byte("bound bytes that must survive")
	ref, err := bkv.StageBlob(ctx, routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error { return tx.BindBlob(ref) }); err != nil {
		t.Fatalf("Transact(bind): %v", err)
	}
	objKey := blob.FinalKey(ref.Unit, ref.BlobID)
	store.SetModTime(objKey, time.Unix(0, 0))

	// Resolve which UnitID the blob landed in from the ref's unit token, then
	// CLEAR that unit's mount: its brefs become invisible to the referenced
	// scan, exactly a not-yet-(re)acquired unit at pass start.
	var gen uint64
	var id uint32
	if _, err := fmt.Sscanf(ref.Unit, "%d-%d", &gen, &id); err != nil {
		t.Fatalf("parse unit token %q: %v", ref.Unit, err)
	}
	c.TestingClearMount(storageunit.UnitID(id))

	// Mid-pass, the reconcile re-mounts it (the rollout-window acquire): the
	// unit is back in the mount view AFTER the referenced scan ran without it.
	bkv.TestingSetSweepMidPassHook(func() { c.TestingRunReconcile() })
	defer bkv.TestingSetSweepMidPassHook(nil)

	err = bkv.SweepOrphans(ctx, time.Now(), time.Hour)
	if err == nil {
		t.Fatalf("a pass whose mount view changed mid-scan must ABORT fail-closed, not complete")
	}
	t.Logf("torn pass aborted fail-closed: %v", err)

	// Restore the mount view before reading back. The pass is now refused at
	// the referenced scan (the coverage guard), which is EARLIER than the
	// mid-pass hook, so the hook's reconcile never ran and unit U is still
	// unmounted; a Get would be refused for that reason rather than because
	// anything was swept. Re-mounting isolates the assertion to what it is
	// about: the bytes must still be in the object store.
	c.TestingRunReconcile()

	// THE DATA-LOSS ASSERTION: the bound blob's bytes must still exist and the
	// pointer must still resolve.
	rc, _, gerr := bkv.GetBlob(ctx, routeKey, ref.BlobID)
	if gerr != nil {
		t.Fatalf("DATA LOSS: bound blob unreadable after a torn sweep pass: %v", gerr)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, body) {
		t.Fatalf("bound blob bytes corrupted: %q", got)
	}

	// A CALM follow-up pass (hook removed, view stable) completes without
	// touching the bound blob.
	bkv.TestingSetSweepMidPassHook(nil)
	if err := bkv.SweepOrphans(ctx, time.Now(), time.Hour); err != nil {
		t.Fatalf("calm pass: %v", err)
	}
	rc2, _, gerr2 := bkv.GetBlob(ctx, routeKey, ref.BlobID)
	if gerr2 != nil {
		t.Fatalf("bound blob lost by the calm pass: %v", gerr2)
	}
	_ = rc2.Close()
}

// TestSweepOrphans_TopologyGuardAbortsAViewChangeAfterTheScan pins the SECOND
// guard on its own, the one the coverage guard cannot stand in for.
//
// The node starts FULLY mounted, so the up-front coverage check passes and the
// referenced scan runs to completion over a sound view. Only THEN does the
// mount view move. Nothing about the node's state at pass start could have
// predicted that, so the topology guard is the only thing between a moved view
// and an enumeration measured against a stale referenced set. Without this
// test, the coverage guard firing earlier in the sibling test above would leave
// sameUnitTokenSet unexercised.
func TestSweepOrphans_TopologyGuardAbortsAViewChangeAfterTheScan(t *testing.T) {
	store := blobmem.New()
	bkv, err := cluster.NewBlobKV(cluster.Config{
		NodeID:         "solo",
		BackendFactory: memfactory.New(),
		UnitCount:      storageunit.MustUnitCount(2),
		BlobStore:      store,
	})
	if err != nil {
		t.Fatalf("NewBlobKV (multi): %v", err)
	}
	t.Cleanup(func() { _ = bkv.Close() })
	c := bkv.Cluster()
	ctx := context.Background()

	routeKey := []byte("slug-bound")
	body := []byte("bound bytes that must survive")
	ref, err := bkv.StageBlob(ctx, routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	if err := bkv.Transact(routeKey, func(tx *cluster.BlobTx) error { return tx.BindBlob(ref) }); err != nil {
		t.Fatalf("Transact(bind): %v", err)
	}
	store.SetModTime(blob.FinalKey(ref.Unit, ref.BlobID), time.Unix(0, 0))

	var gen uint64
	var id uint32
	if _, err := fmt.Sscanf(ref.Unit, "%d-%d", &gen, &id); err != nil {
		t.Fatalf("parse unit token %q: %v", ref.Unit, err)
	}

	// The view moves only AFTER the referenced scan has already run.
	bkv.TestingSetSweepMidPassHook(func() { c.TestingClearMount(storageunit.UnitID(id)) })
	defer bkv.TestingSetSweepMidPassHook(nil)

	err = bkv.SweepOrphans(ctx, time.Now(), time.Hour)
	if err == nil {
		t.Fatalf("a pass whose mount view moved AFTER the referenced scan must ABORT fail-closed, not complete")
	}
	t.Logf("view-change pass aborted fail-closed: %v", err)

	c.TestingRunReconcile()
	rc, _, gerr := bkv.GetBlob(ctx, routeKey, ref.BlobID)
	if gerr != nil {
		t.Fatalf("DATA LOSS: bound blob unreadable after an aborted pass: %v", gerr)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.Equal(got, body) {
		t.Fatalf("bound blob bytes corrupted: %q", got)
	}
}
