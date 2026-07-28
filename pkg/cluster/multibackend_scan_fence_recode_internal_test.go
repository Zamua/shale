package cluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
)

// fenceScanBackend mimics REAL slatedb's closed-handle-on-read: a ScanPrefix on a
// SUPERSEDED mount returns backend.ErrFenced, because a higher-epoch owner opened
// the same per-(unit, replica) database and slatedb CLOSED this now-stale handle.
// The sharedfactory fencedReplicaBackend's OLD permissive default let fenced
// reads PASS - exactly the test-double / production divergence that hid the #443
// bug - and it now DEFAULTS to strict read fencing (reads-pass is the
// SetStrictReadFencing(false) opt-out). This wrapper is still needed because the
// test mounts a bare memory.Memory OUTSIDE the factory, so the fence is injected
// here, mirroring fenceAtBackend (the write-path fence-recode test's double).
// The wrap matches the slate backend's fenceTag shape so
// errors.Is(err, backend.ErrFenced) holds.
type fenceScanBackend struct {
	backend.Backend
}

func (f fenceScanBackend) ScanPrefix(_ []byte) (backend.Iterator, error) {
	return nil, fmt.Errorf("slate: scan prefix: %w", backend.ErrFenced)
}

// TestScan_FencedMount_RecodedToTransient_AndEvicted pins the #443 fix. The
// cross-shard scan (Aggregate / LocalScan) reads through this node's MOUNTED unit
// handles directly. During a membership change a mounted position can be
// SUPERSEDED (a higher-epoch owner opens the same db), and in production slatedb
// a read/scan through that stale handle fails with the writer-epoch fence
// (`detected newer DB client`). Without the fix the scan path returns that RAW
// fence, failing the WHOLE aggregate - and because a SCAN-ONLY workload (the blob
// migrate dry-run, the blob-GC sweeper) never issues the write that would
// otherwise evict the stale mount via the write path, the fence re-surfaces on
// every retry FOREVER (the observed prod symptom).
//
// The fix makes the scan path symmetric to the write path's fenceToTransient
// self-heal: a fenced mounted scan is recoded to the TRANSIENT acquiring-window
// error (so the caller retries) AND the stale mount is EVICTED (so the reconcile
// re-acquires a fresh handle and a later pass is clean). Both Aggregate scan
// paths are covered: the LOCAL-node localMountedSnapshot and the peer-facing
// localScanMounted (mountedIterator), which is the path the prod RPC error
// actually travelled.
func TestScan_FencedMount_RecodedToTransient_AndEvicted(t *testing.T) {
	target := ru(0, 0, 0)

	assertRecodedAndEvicted := func(t *testing.T, c *Cluster, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("a fenced mounted scan must surface an error, got nil")
		}
		if errors.Is(err, backend.ErrFenced) {
			t.Fatalf("a fenced scan leaked the RAW fence (this FAILS the whole aggregate); got %v", err)
		}
		if !isAcquiringErr(err) {
			t.Fatalf("a fenced scan must recode to the TRANSIENT acquiring-window error so the caller retries; got %v", err)
		}
		if !isTransientReplicaErr(err) {
			t.Fatalf("a recoded fenced scan must classify transient (so the inventory retry rides it out); got %v", err)
		}
		_, stillMounted := c.mounts.backendFor(target)
		if stillMounted {
			t.Fatalf("a fenced scan must EVICT the stale mount so the reconcile re-acquires it fresh; %v still mounted", target)
		}
	}

	// mountFenced builds a FULLY MOUNTED white-box R=2 cluster and then swaps ONE
	// position's handle for the fenced double. Mounting everything first matters
	// for two reasons. It is the realistic shape (a superseded handle among
	// otherwise-healthy mounts, which is what a membership change produces), and
	// it keeps this test on the path it is about: a node holding an owned
	// position UNMOUNTED is refused earlier by the scan coverage guard
	// (scanCoverageErr), which would short-circuit the scan before it ever
	// touched the fenced handle. The two guards are complementary - coverage
	// catches a position that is missing, this one catches a position that is
	// present but stale - and the fixture has to isolate the second.
	//
	// closed is set so the post-evict scheduleReconcile (not under test here)
	// no-ops rather than leaking a debounced AfterFunc; the scan + recode +
	// evict paths do not gate on closed, so the behavior under test is unchanged.
	mountFenced := func(t *testing.T) *Cluster {
		t.Helper()
		backing := sharedfactory.NewBacking()
		c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")
		if err := c.mountReplicaUnits(); err != nil {
			t.Fatalf("mountReplicaUnits: %v", err)
		}
		if r := c.MountReadiness(); r.PendingUnits != 0 {
			t.Fatalf("fixture must be fully mounted so the coverage guard does not pre-empt the fence path: %+v", r)
		}
		c.mounts.mountUndecorated(target, fenceScanBackend{Backend: memory.New()})
		c.closed.Store(true)
		return c
	}

	t.Run("localMountedSnapshot", func(t *testing.T) {
		c := mountFenced(t)
		_, err := c.localMountedSnapshot()
		assertRecodedAndEvicted(t, c, err)
	})

	t.Run("localScanMounted_iterator", func(t *testing.T) {
		c := mountFenced(t)
		it, err := c.localScanMounted(nil)
		if err != nil {
			t.Fatalf("localScanMounted setup returned an error: %v", err)
		}
		_, _, err = it.Next()
		_ = it.Close()
		assertRecodedAndEvicted(t, c, err)
	})
}
