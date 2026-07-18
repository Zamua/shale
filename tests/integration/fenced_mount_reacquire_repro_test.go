package integration

// REPRO (#433): a node's MOUNTED replica position that gets FENCED by a peer
// (the peer's open bumped the durable writer-epoch, then the peer did NOT keep
// it - the orphan a slow/contended real slatedb open produces on a mass restart)
// is never re-acquired, so writes to that unit wedge permanently while reads
// keep working.
//
// Why the existing tests miss it: the cluster's reconcile ACQUIRE half only acts
// on DESIRED-but-UNMOUNTED positions. A fenced position is still in mountMap
// (the open succeeded earlier; a later peer fenced it out-of-band), so reconcile
// treats it as healthy and never re-opens it at a fresh epoch. The in-memory
// test backing's clean open-faults can't surface this because they never fence a
// live holder; this test fences one directly (open+close at a higher epoch),
// exactly modeling "a peer's open fenced me, then the peer skipped the mount".
//
// EXPECTED before the fix: RED - writes to the fenced unit never recover.

import (
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

func TestFencedLiveMount_R2_ClusterReacquiresAndServesWrites(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	nodes := start3NodeR2(t, unitCount, backing)

	// A key whose unit we will fence. Seed it so the unit is mounted + serving.
	const key = "fence-victim-key"
	if err := putWithRetryUnavailable(t, nodes[0].Cluster, key, "v0", 10*time.Second); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	// Sanity: it is writable now.
	if err := putWithRetryUnavailable(t, nodes[0].Cluster, key, "v1", 10*time.Second); err != nil {
		t.Fatalf("pre-fence write should succeed: %v", err)
	}

	// Identify the key's unit + its primary replica position (gen 0).
	uc := storageunit.MustUnitCount(unitCount)
	u := storageunit.UnitForShardKey(ring.ShardKey([]byte(key)), uc)
	victim := storageunit.NewReplicaUnit(gu0(u), 0)
	owners := replicaSetOnRing(nodes[0].Cluster, key, unitCount, 2)
	t.Logf("repro: fencing %s (key %q unit %v), R2 owners=%v", victim, key, u, owners)

	// FENCE the live primary out-of-band: a fresh handle opens the position at a
	// far-higher epoch (bumping the durable writer-epoch, which fences whoever
	// holds it mounted) then closes WITHOUT keeping it. This is exactly a peer
	// whose open fenced the live holder and then errored/skipped the mount: the
	// holder's mount is now fenced, and no node holds the position cleanly.
	fencer := backing.Handle()
	if _, _, err := fencer.OpenReplicaUnit(victim, storageunit.Epoch(1_000_000)); err != nil {
		t.Fatalf("out-of-band fence open: %v", err)
	}
	if err := fencer.CloseReplicaUnit(victim); err != nil {
		t.Fatalf("out-of-band fence close: %v", err)
	}
	t.Logf("repro: %s fenced (durable epoch bumped, holder's mount now stale)", victim)

	// The cluster MUST detect the fenced mount + re-acquire it so writes recover.
	// Poll a write to the key across all entry nodes for a generous heal window.
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	recovered := false
	for i := 0; time.Now().Before(deadline); i++ {
		entry := nodes[i%len(nodes)]
		lastErr = entry.Cluster.Put([]byte(key), []byte(fmt.Sprintf("post-fence-%d", i)))
		if lastErr == nil {
			recovered = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !recovered {
		t.Fatalf("WEDGE: writes to the fenced unit never recovered in 30s (last err: %v) - "+
			"the fenced mount was not detected + re-acquired", lastErr)
	}
	t.Logf("repro: writes recovered after the fenced mount was re-acquired")
}

// TestFencedLiveMount_R2_GetWedges is the READ-path counterpart, and the one
// that reproduces the staging wedge: the keygate (and every request's first op)
// is a routed GET, and the replicated-Get local read (dispatchReplicaGetUnit)
// returns b.Get(key) RAW - it does NOT call fenceToTransient the way the write
// and scan paths do - so a fenced mount read via GET returns the raw fence error
// AND is never evicted, so reconcile never re-acquires it. The cluster's
// DebugState reports the position mounted+healthy while every GET to it fails
// forever. EXPECTED before the fix: RED (reads never recover).
func TestFencedLiveMount_R2_GetWedges(t *testing.T) {
	const unitCount = 8
	// The backing defaults to real slatedb's close-on-fence: a fenced handle
	// fails READS too, not just writes. The permissive reads-pass-through model
	// (the SetStrictReadFencing(false) opt-out) would mask the routed-GET wedge
	// this test reproduces.
	backing := sharedfactory.NewBacking()
	nodes := start3NodeR2(t, unitCount, backing)

	const key = "fence-read-victim"
	if err := putWithRetryUnavailable(t, nodes[0].Cluster, key, "val", 10*time.Second); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	// Confirm it reads now.
	if _, err := nodes[0].Cluster.Get([]byte(key)); err != nil {
		t.Fatalf("pre-fence read should succeed: %v", err)
	}

	uc := storageunit.MustUnitCount(unitCount)
	u := storageunit.UnitForShardKey(ring.ShardKey([]byte(key)), uc)
	t.Logf("repro: fencing BOTH replicas of %v then probing GET (key %q)", u, key)

	// Fence BOTH replica positions out-of-band (the mass-restart race fenced both
	// copies of the unit). With no healthy replica left, a routed GET cannot get a
	// quorum winner, so there is no read-repair to back-door the write-path
	// eviction: the only thing that can heal this is the GET path evicting its own
	// fenced local mount - which it currently does not do.
	fencer := backing.Handle()
	for _, r := range []uint8{0, 1} {
		victim := storageunit.NewReplicaUnit(gu0(u), r)
		if _, _, err := fencer.OpenReplicaUnit(victim, storageunit.Epoch(1_000_000)); err != nil {
			t.Fatalf("fence open %s: %v", victim, err)
		}
		if err := fencer.CloseReplicaUnit(victim); err != nil {
			t.Fatalf("fence close %s: %v", victim, err)
		}
	}

	// Reads MUST recover (the fenced mount detected + re-acquired). Poll GET
	// across all entry nodes for a generous heal window.
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	recovered := false
	for i := 0; time.Now().Before(deadline); i++ {
		entry := nodes[i%len(nodes)]
		_, lastErr = entry.Cluster.Get([]byte(key))
		if lastErr == nil {
			recovered = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !recovered {
		t.Fatalf("WEDGE: GET to the fenced unit never recovered in 30s (last err: %v) - "+
			"the replicated-Get local read does not evict a fenced mount", lastErr)
	}
	t.Logf("repro: reads recovered after the fenced mount was re-acquired")
}
