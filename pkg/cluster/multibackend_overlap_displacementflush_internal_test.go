package cluster

// White-box pins for the DISPLACEMENT FLUSH (docs/SPEC.md "Displacement
// flush", v0.8 Phase 2e): the Owned -> Draining edge (beginDrain) asks the
// displaced owner's backend to flush its in-memory write state EXACTLY ONCE
// per transition, best-effort and asynchronously, so the successor's fencing
// open replays a minimal WAL tail. The sharedfactory double counts Flush
// calls per replica position (Backing.ReplicaFlushCount); the real slate
// backend implements the same optional backend.Flusher capability with a
// memtable flush.

import (
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// waitReplicaFlushCount polls the backing until ru's flush count reaches want
// (the displacement flush runs in a background goroutine) or the deadline
// fires. It then holds the assertion through a short grace window so a LATE
// duplicate flush also fails the test rather than slipping past a one-shot
// read.
func waitReplicaFlushCount(t *testing.T, b *sharedfactory.Backing, ru storageunit.ReplicaUnit, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.ReplicaFlushCount(ru) == want {
			// Grace window: a buggy double-flush lands within milliseconds
			// (the goroutine is spawned synchronously on the edge); catch it.
			time.Sleep(30 * time.Millisecond)
			if got := b.ReplicaFlushCount(ru); got != want {
				t.Fatalf("flush count moved past %d to %d (duplicate displacement flush)", want, got)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("flush count = %d, want %d (displacement flush did not fire?)", b.ReplicaFlushCount(ru), want)
}

// TestOverlap_DisplacementFlush_FiresExactlyOncePerTransition pins the whole
// lifecycle on the REAL mount path (acquireReplicaUnitOverlapBlocking ->
// storeMount, so the mounted entry is the fencedSelfHealing decorator and the
// flush must reach the inner backend through the unwrap):
//
//  1. mounting alone never flushes;
//  2. the Owned -> Draining edge flushes exactly once;
//  3. re-entrant drain ticks (beginDrain while already Draining) and
//     drainCheck polls do NOT flush again;
//  4. a reclaim + re-drain is a NEW transition and flushes exactly once more.
func TestOverlap_DisplacementFlush_FiresExactlyOncePerTransition(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")
	target := ru(0, 0, 0)

	// 1) Mount through the real flip: storeMount wraps the factory backend in
	// the fencedSelfHealing decorator. No flush yet.
	c.acquireReplicaUnitOverlapBlocking(target)
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("precondition: target should be mounted via the real flip")
	}
	if got := backing.ReplicaFlushCount(target); got != 0 {
		t.Fatalf("mounting alone flushed %d times; the displacement flush must fire only on the drain edge", got)
	}

	// 2) The Owned -> Draining edge: exactly one flush, asynchronously.
	c.beginDrain(target)
	if st := c.handoffPhaseOf(target); st.Phase != storageunit.PhaseDraining {
		t.Fatalf("precondition: beginDrain should set Draining, got %v", st.Phase)
	}
	waitReplicaFlushCount(t, backing, target, 1)

	// 3) Re-entrant edges while already Draining are no-ops: beginDrain
	// returns on the phase guard before the flush; drainCheck (no successor
	// marker yet, so the position stays Draining) never flushes.
	c.beginDrain(target)
	c.beginDrain(target)
	c.drainCheck(target)
	c.loopWG.Wait() // settle any (buggy) stray flush goroutine before asserting
	if got := backing.ReplicaFlushCount(target); got != 1 {
		t.Fatalf("re-entrant drain ticks flushed again: count = %d, want 1", got)
	}

	// 4) Reclaim (the ring flip-flopped the position back) clears the drain;
	// a LATER re-displacement is a NEW Owned -> Draining transition and
	// flushes exactly once more.
	c.reclaimDrainingPosition(target)
	if st := c.handoffPhaseOf(target); st.Phase != 0 {
		t.Fatalf("precondition: reclaim should clear the phase, got %v", st.Phase)
	}
	c.beginDrain(target)
	waitReplicaFlushCount(t, backing, target, 2)
}

// TestOverlap_DisplacementFlush_SkipsBackendWithoutCapability pins the
// capability gate: a mounted backend that does NOT implement the optional
// backend.Flusher (here a plain memory backend installed by hand, standing in
// for any BYO backend without a flush surface) is skipped silently - the
// drain edge still arms normally and nothing panics or errors.
func TestOverlap_DisplacementFlush_SkipsBackendWithoutCapability(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")
	target := ru(0, 1, 0)

	c.mountMu.Lock()
	c.storeMount(target, memory.New()) // wrapped in the decorator, like production
	c.mountMu.Unlock()
	c.myOpenEpoch.Store(target, storageunit.Epoch(1))

	c.beginDrain(target)
	if st := c.handoffPhaseOf(target); st.Phase != storageunit.PhaseDraining {
		t.Fatalf("drain edge should arm normally for a non-Flusher backend, got %v", st.Phase)
	}
	c.loopWG.Wait()
	if got := backing.ReplicaFlushCount(target); got != 0 {
		t.Fatalf("a non-Flusher backend should record no flushes, got %d", got)
	}
}
