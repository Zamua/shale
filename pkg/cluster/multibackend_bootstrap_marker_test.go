package cluster

// White-box tests for the homogeneous (runtime try-join-else-form) bootstrap.
// They drive bootstrapViaMarker directly against the in-process
// MemConditionalStore double - no membership / gRPC - to pin the two
// split-brain-critical properties in isolation: exactly one founder under
// concurrent first-boot, and "a present marker is adopted, never re-founded"
// (the seed-restart / #453 fix). The full cluster path is validated end-to-end
// on a real staging cluster.

import (
	"sync"
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

// TestBootstrapViaMarker_SingleFounderUnderConcurrency pins THE split-brain
// property: when N nodes bootstrap concurrently against one fresh shared
// ConditionalStore, EXACTLY ONE founds (the PutIfAbsent winner) and the rest
// adopt the founder's durable {gen, count}. Concurrent first-boot of a
// homogeneous fleet can therefore never split into multiple gen-0 rings.
func TestBootstrapViaMarker_SingleFounderUnderConcurrency(t *testing.T) {
	const n = 8
	cs := storageunit.NewMemConditionalStore()
	uc := storageunit.MustUnitCount(16)

	type result struct {
		gen     storageunit.Generation
		count   storageunit.UnitCount
		founded bool
		err     error
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := &Cluster{unitCount: uc}
			c.cfg.ConditionalStore = cs
			<-start // release every goroutine at once for maximum contention
			g, ct, f, e := c.bootstrapViaMarker()
			results[i] = result{gen: g, count: ct, founded: f, err: e}
		}(i)
	}
	close(start)
	wg.Wait()

	founders := 0
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("node %d bootstrapViaMarker: %v", i, r.err)
		}
		if r.founded {
			founders++
		}
		if r.gen != 0 {
			t.Fatalf("node %d: fresh cluster must be gen 0, got %d", i, r.gen)
		}
		if r.count.N() != uc.N() {
			t.Fatalf("node %d: want count N=%d, got N=%d", i, uc.N(), r.count.N())
		}
	}
	if founders != 1 {
		t.Fatalf("SPLIT-BRAIN: want exactly 1 founder across %d concurrent boots, got %d", n, founders)
	}
}

// TestBootstrapViaMarker_RestartAdoptsDurableGen pins the #453 fix: a node that
// bootstraps against a marker that ALREADY EXISTS adopts its durable {gen,
// count} and does NOT re-found - so a restart rejoins the live ring at the live
// generation instead of re-forming a fresh gen-0 1-node ring. It also proves the
// FULL-restart-resume property: the adopted count is the marker's live count,
// not this node's configured N.
func TestBootstrapViaMarker_RestartAdoptsDurableGen(t *testing.T) {
	cs := storageunit.NewMemConditionalStore()
	// Seed the marker as if a long-ago founder resharded: gen 5, N=64.
	payload, err := encodeClusterInit(clusterInitRecord{Gen: 5, Count: 64})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := cs.PutIfAbsent(clusterInitMarkerKey, payload); err != nil {
		t.Fatalf("seed marker: %v", err)
	}

	// Configured N (16) deliberately differs from the live N (64) to prove the
	// node adopts the DURABLE count, not its own config.
	c := &Cluster{unitCount: storageunit.MustUnitCount(16)}
	c.cfg.ConditionalStore = cs

	gen, count, founded, err := c.bootstrapViaMarker()
	if err != nil {
		t.Fatalf("bootstrapViaMarker: %v", err)
	}
	if founded {
		t.Fatalf("#453: a node seeing an existing marker must NOT re-found")
	}
	if gen != 5 {
		t.Fatalf("want adopted gen 5, got %d", gen)
	}
	if count.N() != 64 {
		t.Fatalf("want adopted live count N=64 (not the configured 16), got N=%d", count.N())
	}
}
