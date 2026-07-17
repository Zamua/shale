package cluster

// White-box pins for the under-W owner-flush GROUP COMMIT (flushGroup): the
// coalescing coordinator that collapses a same-owner under-W write burst from
// O(writes) full-memtable flushes to O(flush-windows) flushes, with the SAME
// per-write durability guarantee. These drive the coordinator through the same
// seam the existing under-W flush tests use (c.flushOwnerUnderReplicated over a
// fake backend.Flusher) and pin, per docs/SPEC.md "Owner-flush coalescing":
//
//   (a) COALESCING: N concurrent under-W flushes on ONE unit collapse to a
//       single shared flush (all N satisfied by it).
//   (b) THE INVARIANT / durability: a waiter that enqueues while a flush is
//       in flight is NOT satisfied by that flush - it waits for the NEXT one,
//       which starts after it enqueued (so its memtable-resident write is
//       persisted).
//   (c) DIFFERENT-UNIT INDEPENDENCE: a flush on one unit never satisfies (or
//       blocks) a waiter on another - distinct units flush independently.
//   (d) FLUSH-ERROR FAN-OUT: a failing shared flush delivers the error to EVERY
//       waiter in its batch (each terminal, no false success) and does not wedge
//       the coordinator (a later write still flushes).

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
)

// gateFlusher is a fake backend.Flusher (over a nil embedded Backend - the
// coordinator only ever calls Flush) that counts Flush calls atomically and can
// optionally SIGNAL each call's entry and GATE its return, so a test can drive
// the batching deterministically. A nil entered / release channel means "do not
// signal / do not block". failErr, when set, is returned by Flush (used for the
// error-fan-out pin); it is guarded by its own mutex so a test can flip it
// between flushes without racing the runner goroutine.
type gateFlusher struct {
	backend.Backend
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}

	mu      sync.Mutex
	failErr error
}

func (f *gateFlusher) Flush() error {
	f.calls.Add(1)
	if f.entered != nil {
		f.entered <- struct{}{}
	}
	if f.release != nil {
		<-f.release
	}
	f.mu.Lock()
	err := f.failErr
	f.mu.Unlock()
	return err
}

func (f *gateFlusher) setErr(err error) {
	f.mu.Lock()
	f.failErr = err
	f.mu.Unlock()
}

// TestFlushGroup_CoalescesConcurrentSameUnit pins (a): N concurrent under-W
// flushes on ONE unit collapse to a SINGLE shared flush. The afterEnqueue
// barrier holds every waiter until all N have enqueued, so the first (and only)
// runner snapshots all N into one batch - one Flush satisfies them all. This is
// the flush-storm fix: 32 concurrent under-W writes flush ONCE, not 32 times.
func TestFlushGroup_CoalescesConcurrentSameUnit(t *testing.T) {
	const n = 32
	c := &Cluster{}
	f := &gateFlusher{} // non-blocking flush; the barrier drives the batching

	enqueued := make(chan struct{}, n)
	release := make(chan struct{})
	c.casFlushGroup.afterEnqueue = func() {
		enqueued <- struct{}{}
		<-release // hold every waiter until the test has seen all N enqueue
	}

	results := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = c.flushOwnerUnderReplicated(f)
		}(i)
	}

	for i := 0; i < n; i++ {
		<-enqueued // all N have appended to the unit's pending list
	}
	// No flush can have run yet: every waiter (including the spawner) is parked
	// in afterEnqueue before the runner is spawned.
	if got := f.calls.Load(); got != 0 {
		t.Fatalf("no flush must run before the enqueue barrier releases; got %d", got)
	}
	close(release) // spawner now spawns the runner, which snapshots all N
	wg.Wait()

	if got := f.calls.Load(); got != 1 {
		t.Fatalf("N=%d concurrent same-unit under-W flushes must coalesce to ONE flush; got %d", n, got)
	}
	for i, err := range results {
		if err != nil {
			t.Fatalf("every coalesced waiter must succeed on the shared flush; waiter %d got %v", i, err)
		}
	}
}

// TestFlushGroup_ArrivedDuringFlushWaitsForNext pins (b), the correctness
// invariant + durability: a waiter (B) that enqueues WHILE another's flush is in
// flight is NOT satisfied by that flush - it is satisfied by the NEXT flush,
// which starts AFTER B enqueued (so B's memtable-resident write is persisted).
// The flush count sequencing (still 1 while B waits behind A's in-flight flush,
// then 2 once B's own flush starts) is the observable proof.
func TestFlushGroup_ArrivedDuringFlushWaitsForNext(t *testing.T) {
	c := &Cluster{}
	f := &gateFlusher{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}

	enqueued := make(chan struct{}, 2)
	c.casFlushGroup.afterEnqueue = func() { enqueued <- struct{}{} }

	// Writer A: enqueues, spawns the runner, its flush #1 goes in-flight.
	aResult := make(chan error, 1)
	go func() { aResult <- c.flushOwnerUnderReplicated(f) }()
	<-enqueued  // A enqueued
	<-f.entered // A's flush #1 is in flight (blocked on release)
	if got := f.calls.Load(); got != 1 {
		t.Fatalf("A's flush must be the only one in flight; got %d", got)
	}

	// Writer B: enqueues WHILE flush #1 is in flight. It must not ride flush #1.
	bResult := make(chan error, 1)
	go func() { bResult <- c.flushOwnerUnderReplicated(f) }()
	<-enqueued // B enqueued (into pending, behind the in-flight flush)
	if got := f.calls.Load(); got != 1 {
		t.Fatalf("B enqueuing during flush #1 must NOT start a new flush yet; got %d", got)
	}

	// Let flush #1 finish. It satisfies A only; the runner then loops and starts
	// flush #2 for B (which began AFTER B enqueued - the invariant).
	f.release <- struct{}{} // release flush #1
	if err := <-aResult; err != nil {
		t.Fatalf("A must succeed on flush #1; got %v", err)
	}
	<-f.entered // flush #2 has STARTED (a distinct, later flush for B)
	if got := f.calls.Load(); got != 2 {
		t.Fatalf("B must be satisfied by a SECOND flush, not A's; flush count %d", got)
	}
	f.release <- struct{}{} // release flush #2
	if err := <-bResult; err != nil {
		t.Fatalf("B must succeed on flush #2 (its write persisted); got %v", err)
	}
}

// TestFlushGroup_DifferentUnitsIndependent pins (c): concurrent under-W flushes
// on DIFFERENT units (distinct Flusher identities) do NOT coalesce - each has
// its own coordinator entry and runner. A flush blocked on unit A must never
// stall a waiter on unit B, and A's flush must never satisfy B's waiter. Proof:
// unit A's flush is held in flight while unit B's writer runs to completion
// independently.
func TestFlushGroup_DifferentUnitsIndependent(t *testing.T) {
	c := &Cluster{}
	fA := &gateFlusher{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	fB := &gateFlusher{} // independent, non-blocking

	// Unit A's flush goes in-flight and stays blocked.
	aResult := make(chan error, 1)
	go func() { aResult <- c.flushOwnerUnderReplicated(fA) }()
	<-fA.entered // A's flush is in flight (blocked)

	// Unit B's writer must complete WITHOUT waiting on A's release: its own
	// coordinator entry + runner flush it independently.
	bResult := make(chan error, 1)
	go func() { bResult <- c.flushOwnerUnderReplicated(fB) }()
	select {
	case err := <-bResult:
		if err != nil {
			t.Fatalf("unit B's independent flush must succeed; got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unit B's flush blocked on unit A's in-flight flush: units wrongly coalesced")
	}
	if got := fB.calls.Load(); got != 1 {
		t.Fatalf("unit B must flush exactly once, independently; got %d", got)
	}
	if got := fA.calls.Load(); got != 1 {
		t.Fatalf("unit A must have exactly one (still-blocked) flush; got %d", got)
	}

	// Release A; it completes on its own flush, never having touched B.
	fA.release <- struct{}{}
	if err := <-aResult; err != nil {
		t.Fatalf("unit A's flush must succeed after release; got %v", err)
	}
}

// TestFlushGroup_FlushErrorFansOutAndDoesNotWedge pins (d): a FAILING shared
// flush delivers the SAME error to EVERY waiter in its batch (each returns it
// terminally - no false success for any), and the coordinator is NOT wedged - a
// subsequent write flushes normally and succeeds.
func TestFlushGroup_FlushErrorFansOutAndDoesNotWedge(t *testing.T) {
	const n = 8
	c := &Cluster{}
	boom := errors.New("under-W flush: object store unreachable")
	f := &gateFlusher{failErr: boom} // the shared flush fails

	// Barrier: hold all N waiters until they have enqueued, so the single shared
	// flush covers the whole batch - proving the ERROR fans out to all N.
	enqueued := make(chan struct{}, n)
	release := make(chan struct{})
	c.casFlushGroup.afterEnqueue = func() {
		enqueued <- struct{}{}
		<-release
	}

	results := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = c.flushOwnerUnderReplicated(f)
		}(i)
	}
	for i := 0; i < n; i++ {
		<-enqueued
	}
	close(release)
	wg.Wait()

	if got := f.calls.Load(); got != 1 {
		t.Fatalf("the failing batch must be ONE shared flush; got %d", got)
	}
	for i, err := range results {
		if !errors.Is(err, boom) {
			t.Fatalf("every waiter in a failed batch must get the terminal error; waiter %d got %v", i, err)
		}
	}

	// The coordinator must not be wedged: clear the barrier + the error and issue
	// a fresh write; it flushes and succeeds.
	c.casFlushGroup.afterEnqueue = nil
	f.setErr(nil)
	if err := c.flushOwnerUnderReplicated(f); err != nil {
		t.Fatalf("a subsequent write must still flush (coordinator not wedged); got %v", err)
	}
	if got := f.calls.Load(); got != 2 {
		t.Fatalf("the post-error write must have flushed once more; total flushes %d", got)
	}
}
