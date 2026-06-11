package cluster

// White-box tests for the v0.8 Phase 4 doubling resharder. These live in
// package cluster so they can drive the unexported reshard internals
// (genState, resolveGenUnit, Reshard) and inspect the mount map keyed by
// GenUnit directly. The cross-node reshard-under-membership is covered end to
// end in tests/integration.

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// openSingleNodeMultiN opens a single-node multi-backend cluster on a memfactory
// with unit count n and returns both. The cluster owns the whole unit space.
func openReshardCluster(t *testing.T, n int) (*Cluster, *memfactory.Factory) {
	t.Helper()
	fac := memfactory.New()
	c, err := Open(Config{
		NodeID:         "solo",
		BackendFactory: fac,
		UnitCount:      storageunit.MustUnitCount(n),
	})
	if err != nil {
		t.Fatalf("Open multi-backend: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, fac
}

// TestResolveGenUnit_SteadyAndMidReshard pins the routing decision. In steady
// state (empty cut-over set) every key resolves to GenUnit{gen, K}. With K in
// the cut-over set the same key resolves to GenUnit{gen+1, child}, where child
// is one of K's two doubling children.
func TestResolveGenUnit_SteadyAndMidReshard(t *testing.T) {
	count := storageunit.MustUnitCount(4)
	next := storageunit.MustUnitCount(8)
	gs := genState{gen: 3, count: count, nextCount: next, cutOver: map[storageunit.UnitID]struct{}{}}

	for h := storageunit.ShardHash(0); h < 64; h++ {
		k := storageunit.UnitForHash(h, count)
		got := gs.resolveGenUnit(h)
		want := storageunit.NewGenUnit(3, k)
		if got != want {
			t.Fatalf("steady resolveGenUnit(%d) = %s, want %s", h, got, want)
		}
	}

	// Cut unit 1 over: keys whose old unit is 1 now resolve to gen-4 children.
	gs.cutOver[1] = struct{}{}
	for h := storageunit.ShardHash(0); h < 64; h++ {
		oldK := storageunit.UnitForHash(h, count)
		got := gs.resolveGenUnit(h)
		if oldK == 1 {
			child := storageunit.UnitForHash(h, next)
			want := storageunit.NewGenUnit(4, child)
			if got != want {
				t.Fatalf("cut-over resolveGenUnit(%d) = %s, want %s", h, got, want)
			}
			// The child must be one of unit 1's two doubling children.
			low, high, _ := storageunit.ChildUnits(1, count)
			if child != low && child != high {
				t.Fatalf("hash %d on cut-over unit 1 landed on child %d, want %d or %d", h, child, low, high)
			}
		} else {
			want := storageunit.NewGenUnit(3, oldK)
			if got != want {
				t.Fatalf("non-cut-over resolveGenUnit(%d) = %s, want %s", h, got, want)
			}
		}
	}
}

// TestReshard_DoublesUnitsAndPreservesData is the core single-node reshard
// gate: write a spread of acked keys, Reshard, and assert (1) the generation
// advanced, (2) the unit count doubled, (3) EVERY key is still readable with
// its exact value, and (4) each key now physically lives in the correct
// gen-(g+1) child database (split by the new hash bit), not the retired old
// unit.
func TestReshard_DoublesUnitsAndPreservesData(t *testing.T) {
	const n = 8
	c, fac := openReshardCluster(t, n)
	oldCount := storageunit.MustUnitCount(n)
	newCount := storageunit.MustUnitCount(2 * n)

	want := make(map[string][]byte)
	const nKeys = 500
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("rec-%05d", i)
		v := []byte(fmt.Sprintf("val-%05d", i))
		if err := c.Put([]byte(k), v); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
		want[k] = v
	}

	if err := c.Reshard(); err != nil {
		t.Fatalf("Reshard: %v", err)
	}

	// Generation advanced 0 -> 1, count doubled.
	gs := c.genSnapshot()
	if gs.gen != 1 {
		t.Fatalf("generation = %d after reshard, want 1", gs.gen)
	}
	if gs.count.N() != 2*n {
		t.Fatalf("count = %d after reshard, want %d", gs.count.N(), 2*n)
	}
	if len(gs.cutOver) != 0 {
		t.Fatalf("cut-over set non-empty after reshard completion: %v", gs.cutOver)
	}

	// Every key still readable with its exact value.
	for k, v := range want {
		got, err := c.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %q after reshard: %v (NO ACKED WRITE LOST violated)", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("Get %q = %q, want %q after reshard", k, got, v)
		}
	}

	// Physical placement: each key lives in its gen-1 child database (the unit
	// it maps to at 2N), and NOT in the retired gen-0 unit.
	for k, v := range want {
		h := storageunit.HashShardKey(c.shardKey([]byte(k)))
		childID := storageunit.UnitForHash(h, newCount)
		childGU := storageunit.NewGenUnit(1, childID)
		be, ok := fac.UnitBackend(childGU)
		if !ok {
			t.Fatalf("gen-1 child unit %s has no store for key %q", childGU, k)
		}
		got, err := be.Get([]byte(k))
		if err != nil || !bytes.Equal(got, v) {
			t.Fatalf("key %q not in gen-1 child %s: got=%q err=%v", k, childGU, got, err)
		}
		// The old gen-0 unit must be retired (closed): it should not be in the
		// factory's open set anymore.
		oldID := storageunit.UnitForHash(h, oldCount)
		oldGU := storageunit.NewGenUnit(0, oldID)
		if _, open := fac.CurrentEpoch(oldGU); open {
			t.Fatalf("retired old gen-0 unit %s still open after reshard", oldGU)
		}
	}

	// The mounted set is exactly the 2N gen-1 units.
	open := fac.OpenUnits()
	if len(open) != 2*n {
		t.Fatalf("factory has %d units open after reshard, want %d", len(open), 2*n)
	}
	for _, gu := range open {
		if gu.Gen != 1 {
			t.Fatalf("unit %s still open at old generation after reshard", gu)
		}
	}
}

// TestReshard_TwiceQuadruples checks two doublings in a row: 4 -> 8 -> 16 at
// generations 0 -> 1 -> 2, with all data preserved across both.
func TestReshard_TwiceQuadruples(t *testing.T) {
	const n = 4
	c, _ := openReshardCluster(t, n)
	want := make(map[string][]byte)
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("q-%04d", i)
		v := []byte(fmt.Sprintf("qv-%04d", i))
		if err := c.Put([]byte(k), v); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
		want[k] = v
	}
	for step := 0; step < 2; step++ {
		if err := c.Reshard(); err != nil {
			t.Fatalf("Reshard step %d: %v", step, err)
		}
	}
	gs := c.genSnapshot()
	if gs.gen != 2 || gs.count.N() != 16 {
		t.Fatalf("after two reshards gen=%d count=%d, want gen=2 count=16", gs.gen, gs.count.N())
	}
	for k, v := range want {
		got, err := c.Get([]byte(k))
		if err != nil || !bytes.Equal(got, v) {
			t.Fatalf("key %q after two reshards: got=%q err=%v", k, got, v)
		}
	}
}

// TestReshard_ConcurrentWritesNotLost is THE no-acked-write-lost gate for the
// online bisect: writers hammer the cluster with ACKED writes WHILE a reshard
// runs. Every write that returned success must be readable after the reshard
// completes - none lost to the copy / catch-up / cut-over boundary. A write
// landing in an old unit between the copy and the cut-over is the exact hazard;
// the per-unit write-pause + catch-up drain must capture it.
func TestReshard_ConcurrentWritesNotLost(t *testing.T) {
	const n = 8
	c, _ := openReshardCluster(t, n)

	// Seed a baseline so every unit has data to bisect.
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("seed-%04d", i)
		if err := c.Put([]byte(k), []byte("seed")); err != nil {
			t.Fatalf("seed Put: %v", err)
		}
	}

	// Record every acked write in a concurrency-safe map. Writers run until the
	// reshard finishes, then a final barrier ensures all acked writes are in.
	var mu sync.Mutex
	acked := make(map[string][]byte)
	var stop atomic.Bool
	var wg sync.WaitGroup

	const writers = 6
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for !stop.Load() {
				k := fmt.Sprintf("hot-%d-%06d", w, i)
				v := []byte(fmt.Sprintf("hv-%d-%06d", w, i))
				if err := c.Put([]byte(k), v); err != nil {
					// A reshard never makes a single-node write fail (the write-
					// pause only blocks, the unit is always mounted locally).
					t.Errorf("concurrent Put %q during reshard: %v", k, err)
					return
				}
				// Only record AFTER the Put returned success: that is an ACKED
				// write the invariant must protect.
				mu.Lock()
				acked[k] = v
				mu.Unlock()
				i++
			}
		}(w)
	}

	// Run the reshard while writers hammer. Then stop the writers and join.
	if err := c.Reshard(); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("Reshard under concurrent writes: %v", err)
	}
	stop.Store(true)
	wg.Wait()

	// Generation advanced.
	if gs := c.genSnapshot(); gs.gen != 1 {
		t.Fatalf("generation = %d after concurrent reshard, want 1", gs.gen)
	}

	// EVERY acked write - including those that landed mid-reshard - must be
	// readable with its exact value. This is the no-acked-write-lost oracle.
	mu.Lock()
	defer mu.Unlock()
	if len(acked) < writers { // sanity: writers actually ran
		t.Fatalf("only %d acked writes recorded; writers did not run", len(acked))
	}
	for k, v := range acked {
		got, err := c.Get([]byte(k))
		if err != nil {
			t.Fatalf("acked-during-reshard key %q unreadable after reshard: %v (NO ACKED WRITE LOST violated)", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("acked-during-reshard key %q = %q, want %q", k, got, v)
		}
	}
}

// TestReshard_DeletesDuringReshardHonored checks that a Delete acked during the
// reshard is honored: the key must be absent after the reshard, not resurrected
// by the copy of the old unit. (A delete landing after the copy but before
// cut-over is the symmetric hazard to a lost Put.)
func TestReshard_DeletesDuringReshardHonored(t *testing.T) {
	const n = 4
	c, _ := openReshardCluster(t, n)

	// Seed keys, then we will delete half of them DURING the reshard.
	const nKeys = 100
	keys := make([]string, nKeys)
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("d-%04d", i)
		keys[i] = k
		if err := c.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("seed Put %q: %v", k, err)
		}
	}

	deleted := make(map[string]struct{})
	var mu sync.Mutex
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for !stop.Load() && i < nKeys/2 {
			k := keys[i]
			if err := c.Delete([]byte(k)); err != nil {
				t.Errorf("Delete %q during reshard: %v", k, err)
				return
			}
			mu.Lock()
			deleted[k] = struct{}{}
			mu.Unlock()
			i++
		}
	}()

	if err := c.Reshard(); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("Reshard: %v", err)
	}
	stop.Store(true)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	for _, k := range keys {
		_, err := c.Get([]byte(k))
		if _, wasDeleted := deleted[k]; wasDeleted {
			if err == nil {
				t.Fatalf("deleted-during-reshard key %q resurrected after reshard (got a value)", k)
			}
		} else {
			if err != nil {
				t.Fatalf("surviving key %q lost after reshard: %v", k, err)
			}
		}
	}
}

// TestReshard_RejectsLegacyMode confirms Reshard is a no-op error in legacy
// per-node mode (the legacy path is never resharded).
func TestReshard_RejectsLegacyMode(t *testing.T) {
	c, err := Open(Config{NodeID: "legacy", Backend: memory.New()})
	if err != nil {
		t.Fatalf("Open legacy: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Reshard(); err == nil {
		t.Fatal("Reshard in legacy mode should error")
	}
}

// TestReshard_RejectsAtMaxCount confirms a reshard that would overflow the
// unit-count ceiling refuses up front (no half-bisect).
func TestReshard_RejectsAtMaxCount(t *testing.T) {
	// Build a cluster whose count is the max, so Double() errors. We construct
	// genState directly to avoid mounting 2^30 units.
	c, _ := openReshardCluster(t, 2)
	maxCount := storageunit.MustUnitCount(storageunit.MaxUnitCount)
	c.commitGenState(genState{gen: 0, count: maxCount, cutOver: map[storageunit.UnitID]struct{}{}})
	if err := c.Reshard(); err == nil {
		t.Fatal("Reshard at max unit count should refuse (2N overflows the ceiling)")
	}
}
