package cluster_test

// P0 regression: the phase-2 orphan sweep must touch ONLY units this node has
// MOUNTED. The bug (review P0): SweepOrphans iterated DESIRED ownership for its
// object-listing loop while referencedObjKeys scanned only MOUNTED brefs. During
// a cold boot / rebalance-acquire window a unit is DESIRED but not yet MOUNTED:
// its blob objects (in shared object storage) get listed, but its pointers are
// NOT in the local scan, so every BOUND blob under it looks unreferenced and -
// being old, past the age-gate - gets DELETED. That is committed-data loss.
//
// The fix makes the sweep enumerate MountedUnits, so the object-list loop and the
// referenced-pointer scan read the SAME ownership view. This test pins that an
// old, unreferenced object placed (directly, via the fake) under a unit token
// the cluster does NOT have mounted is NEVER swept, while an equally old,
// unreferenced object under a MOUNTED unit's prefix IS reclaimed.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/blob"
	"github.com/Zamua/shale/pkg/blob/blobmem"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestSweepOrphans_OnlySweepsMountedUnits is the P0 regression. A single-node
// multi-backend cluster mounts units 0-0 and 0-1 (the whole 2-unit space). We
// place TWO aged, unreferenced objects in the store:
//
//   - one under a MOUNTED unit's prefix (blob/0-0/...): a genuine crash-orphan
//     the sweep MUST reclaim.
//   - one under a NON-mounted, bogus unit token (blob/9-99/...): the analogue of
//     a desired-but-unmounted unit's bytes. The sweep MUST NOT touch it, even
//     though it is old + unreferenced, because the node cannot see that unit's
//     pointers in its local scan (mis-classifying them as orphans is the P0).
//
// Reverting SweepOrphans to enumerate all desired/owned tokens (the
// desired-from-ring set) instead of MountedUnits does NOT regress this test on its own
// (a single node mounts everything it desires); the loss only manifests when the
// listing view and the pointer-scan view diverge. To pin THAT divergence
// directly, the bogus token stands in for "a prefix the node lists but whose
// pointers it cannot see": under the mounted-only sweep it is invisible (never
// enumerated), so it survives. See the revert note at the end of this file.
func TestSweepOrphans_OnlySweepsMountedUnits(t *testing.T) {
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
	mounted := c.MountedUnits()
	if len(mounted) != 2 {
		t.Fatalf("expected 2 mounted units, got %v", mounted)
	}
	// Sanity: the bogus token is NOT one this node has mounted.
	const bogusUnit = "9-99"
	for _, u := range mounted {
		if u == bogusUnit {
			t.Fatalf("test precondition broken: %q is mounted", bogusUnit)
		}
	}

	ctx := context.Background()

	// (1) A genuine crash-orphan under a MOUNTED unit: stage with NO bind, age it
	// past the grace. The sweep MUST reclaim it.
	routeKey := []byte("slug-a") // routes to a mounted unit (0-0)
	body := []byte("mounted crash orphan")
	ref, err := bkv.StageBlob(ctx, routeKey, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("StageBlob: %v", err)
	}
	mountedOrphanKey := blob.FinalKey(ref.Unit, ref.BlobID)
	store.SetModTime(mountedOrphanKey, time.Now().Add(-2*time.Hour))

	// (2) An equally aged, unreferenced object placed DIRECTLY under a NON-mounted
	// unit's prefix (the desired-but-unmounted analogue). The sweep MUST NOT touch
	// it, because the node cannot see that unit's pointers in its local scan.
	survivorKey := blob.FinalKey(bogusUnit, "deadbeef")
	if err := store.PutStream(ctx, survivorKey, bytes.NewReader([]byte("must survive")), 12); err != nil {
		t.Fatalf("PutStream (bogus unit): %v", err)
	}
	store.SetModTime(survivorKey, time.Now().Add(-2*time.Hour))

	if store.Len() != 2 {
		t.Fatalf("setup: store holds %d, want 2", store.Len())
	}

	if err := bkv.SweepOrphans(ctx, time.Now(), time.Hour); err != nil {
		t.Fatalf("SweepOrphans: %v", err)
	}

	// The mounted-unit orphan is gone; the non-mounted-unit object survives.
	if has, _ := store.Has(ctx, mountedOrphanKey); has {
		t.Errorf("aged orphan under MOUNTED unit %q was NOT reclaimed", ref.Unit)
	}
	if has, _ := store.Has(ctx, survivorKey); !has {
		t.Errorf("object under NON-mounted unit %q was swept (P0: data loss)", bogusUnit)
	}
	if store.Len() != 1 {
		t.Fatalf("after sweep store holds %d, want 1 (only the non-mounted survivor)", store.Len())
	}
}
