package cluster

import (
	"sync"

	"github.com/Zamua/shale/pkg/backend"
)

// flushGroup group-commits the under-W owner durability flush (docs/SPEC.md
// outcome (c)) per storage unit. It is the fix for a flush STORM: outcome (c)'s
// synchronous owner-flush is correct, but under a SAME-OWNER write burst under-W
// becomes the COMMON case (the fan-out cannot reach W under the contention), so
// a naive per-write flush makes every one of those durably-committed writes
// force a full-memtable Flush. A slate Flush() persists the ENTIRE memtable, so
// N concurrent under-W writes on ONE unit each forcing their own flush is
// massively redundant - the second flush already persisted the first write's
// data. They need exactly ONE flush BETWEEN them. flushGroup collapses that
// burst from O(writes) flushes to O(flush-windows) flushes with the SAME
// per-write durability guarantee (each write's success still waits for a flush
// that includes it).
//
// Keying. Coalescing is per UNIT: same-unit concurrent under-W writes must share
// a flush, different units must flush independently. The map is keyed by the
// resolved backend.Flusher identity, which is 1:1 with the unit's independent
// durable store (in multi-backend mode each mounted ReplicaUnit is an
// independent backend, so distinct units yield distinct Flusher pointers) and is
// the single shared store on the legacy per-node replicated path (one c.backend,
// so all its under-W flushes correctly coalesce onto one entry). The Flusher is
// always a pointer-typed backend (backends hold mutable state), so it is a
// comparable, stable map key. A live entry is reused across calls (no per-call
// leak); an entry is EVICTED (forget) when its unit unmounts, so the map does
// not grow with the node's cumulative mount count over its lifetime (a remount
// opens a fresh backend = a fresh key).
type flushGroup struct {
	// mu guards states (its creation + entry lookup/insert). Held only for the
	// O(1) map access, never across a Flush.
	mu     sync.Mutex
	states map[backend.Flusher]*flushGroupState

	// afterEnqueue, when non-nil, is invoked by flush AFTER a waiter has been
	// appended to its unit's pending list and the spawn decision made, but
	// BEFORE the caller blocks and BEFORE the runner is spawned. It is a
	// TEST-ONLY synchronization seam (nil in production): it lets a test
	// guarantee all N concurrent waiters have enqueued before the first flush
	// snapshots, so the coalescing batch is deterministic. Correctness does NOT
	// depend on it - the coalescing invariant holds with it nil.
	afterEnqueue func()
}

// flushGroupState is one storage unit's group-commit state: the set of waiters
// awaiting a flush (pending) and whether a flush-runner is currently active
// (flushing). Both fields are guarded by mu. pending holds one buffered result
// channel per waiter; the runner sends each waiter its flush's error.
type flushGroupState struct {
	mu       sync.Mutex
	flushing bool
	pending  []chan error
}

// flush group-commits a Flush of f and returns that flush's error to the
// caller. It appends the caller as a waiter on f's per-unit state and, if no
// flush is in flight, spawns the flush-runner; then it blocks until a flush that
// INCLUDES this waiter completes and returns that flush's result.
//
// The coalescing correctness invariant (docs/SPEC.md "Owner-flush coalescing"):
// a waiter is satisfied ONLY by a flush that STARTED AFTER the waiter enqueued.
// The runner snapshots-and-clears pending UNDER the state lock and THEN calls
// f.Flush(), so every waiter in a batch enqueued before that flush began; a
// waiter that enqueues while a flush is in flight is not in that flush's
// snapshot and waits for the NEXT flush, which the runner starts after it
// enqueued. Because the caller enqueues AFTER its owner-local commit (its write
// is already in the memtable), any flush that starts after the enqueue persists
// its write - which is exactly what makes a SHARED flush durable for every
// waiter in its batch.
func (g *flushGroup) flush(f backend.Flusher) error {
	g.mu.Lock()
	if g.states == nil {
		g.states = make(map[backend.Flusher]*flushGroupState)
	}
	st := g.states[f]
	if st == nil {
		st = &flushGroupState{}
		g.states[f] = st
	}
	g.mu.Unlock()

	// Buffered so the runner's send never blocks even if this caller has not
	// yet reached the receive below.
	ch := make(chan error, 1)

	st.mu.Lock()
	st.pending = append(st.pending, ch)
	spawn := !st.flushing
	if spawn {
		// Claim the runner slot BEFORE unlocking so no second waiter also
		// spawns; the flip false->true under the lock is the single-runner
		// gate.
		st.flushing = true
	}
	st.mu.Unlock()

	if g.afterEnqueue != nil {
		g.afterEnqueue()
	}

	if spawn {
		go g.runFlush(f, st)
	}
	return <-ch
}

// runFlush is the flush-runner loop for one unit's state. Each iteration
// snapshots the current waiter batch and clears pending (under the state lock),
// calls Flush ONCE for the whole batch (WITHOUT the lock - no lock is held
// across the flush I/O), then delivers that single result to every waiter in the
// batch. If new waiters arrived while the flush was in flight it loops (a fresh
// flush for them, which by construction started after they enqueued); otherwise
// it clears flushing and exits. Snapshot-and-clear happens STRICTLY BEFORE
// Flush so the invariant holds: this flush covers exactly the waiters that
// enqueued before it began, never one that arrived during it. A Flush error is
// delivered to every waiter in the batch identically (the coordinator is
// transparent to the error), and the loop continues normally afterward, so a
// failed flush never wedges the coordinator.
func (g *flushGroup) runFlush(f backend.Flusher, st *flushGroupState) {
	for {
		st.mu.Lock()
		batch := st.pending
		st.pending = nil
		st.mu.Unlock()

		err := f.Flush()

		for _, ch := range batch {
			ch <- err
		}

		st.mu.Lock()
		if len(st.pending) == 0 {
			st.flushing = false
			st.mu.Unlock()
			return
		}
		st.mu.Unlock()
	}
}

// forget drops the per-unit group-commit state for a backend that is unmounting,
// so states does not grow with the node's cumulative mount count. A flush-runner
// already in flight holds its own *flushGroupState pointer and is unaffected -
// its pending waiters are still served; this only removes the map entry so a
// future mount of the same unit (a fresh backend) starts clean. No-op if absent.
func (g *flushGroup) forget(f backend.Flusher) {
	if f == nil {
		return
	}
	g.mu.Lock()
	delete(g.states, f)
	g.mu.Unlock()
}
