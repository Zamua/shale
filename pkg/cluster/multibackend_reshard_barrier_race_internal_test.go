package cluster

// White-box race regression for the v0.8 multi-node reshard freeze barrier
// (P1-A: the in-flight-write-vs-bisect race). The freeze flag is a bool FLIP,
// not a synchronous quiesce. A writer that read isFrozen()==false a moment
// BEFORE the flag flipped is already past the gate and may be mid-Put into the
// old unit, holding the per-old-unit write-pause READ side across b.Put (see
// localWriteBackendForKey). If the bisect copied past that key before the Put
// landed and FLIP then retired the old unit, the value would be ORPHANED -> LOST.
//
// The fix: bisectUnitStatic takes the per-old-unit write-pause WRITE side around
// its copy, which BLOCKS until every in-flight writer to K has finished its Put,
// so the copy is provably quiescent and captures the write.
//
// The existing TestBisectStatic_DrainsInFlightWriterViaWritePause proves the
// bisect BLOCKS by grabbing the pause read-lock directly. This test goes one
// further and exercises the PRODUCTION write path end to end: a real c.Put runs
// through the routed local-write path (passes the freeze gate, takes the pause
// read-lock, calls b.Put) and is suspended INSIDE b.Put - exactly mid-Put - by a
// gating backend, while FREEZE then BISECT then FLIP run on the cluster. The
// assertion is the safety invariant itself: the acked write is CAPTURED into the
// new gen-(g+1) units (readable at gen g+1), never acked-then-lost. The
// companion case proves the OTHER safe outcome: a write that has not yet passed
// the freeze gate when the freeze engages is REFUSED with the retryable
// Unavailable (never acked). Either way: never acked-then-lost.

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gateFactory wraps a memfactory.Factory and hands out gateBackends so a test
// can SUSPEND a Put inside the backend (modeling a writer mid-Put, holding the
// reshard write-pause READ side across b.Put). It delegates every other
// BackendFactory method to the inner memfactory verbatim, so epoch fencing +
// data retention across CloseUnit behave exactly as production tests expect.
type gateFactory struct {
	inner *memfactory.Factory

	mu       sync.Mutex
	gates    map[storageunit.GenUnit]*putGate
	backends map[storageunit.GenUnit]*gateBackend
}

func newGateFactory() *gateFactory {
	return &gateFactory{
		inner:    memfactory.New(),
		gates:    make(map[storageunit.GenUnit]*putGate),
		backends: make(map[storageunit.GenUnit]*gateBackend),
	}
}

// putGate is a one-shot suspend point for a single Put. arm() installs it; the
// first Put that matches blocks on entered<-released after signaling entered, so
// the test knows the write is suspended INSIDE b.Put with the pause read-lock
// held. release() unblocks it.
type putGate struct {
	target   []byte        // the key whose Put suspends; nil = any
	entered  chan struct{} // closed-style signal: the gated Put has entered
	released chan struct{} // the test closes this to let the Put proceed
	once     sync.Once     // entered is signaled at most once
	fired    atomic.Bool
}

func (g *gateFactory) OpenUnit(m storageunit.MountRef, epoch storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	be, opened, err := g.inner.OpenUnit(m, epoch)
	if err != nil {
		return nil, 0, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	wb := &gateBackend{Backend: be, gu: m.Unit(), fac: g}
	g.backends[m.Unit()] = wb
	return wb, opened, nil
}

func (g *gateFactory) CloseUnit(m storageunit.MountRef) error { return g.inner.CloseUnit(m) }
func (g *gateFactory) CurrentEpoch(m storageunit.MountRef) (storageunit.Epoch, bool) {
	return g.inner.CurrentEpoch(m)
}
func (g *gateFactory) OpenUnits() []storageunit.MountRef { return g.inner.OpenUnits() }
func (g *gateFactory) DurableEpoch(m storageunit.MountRef) (storageunit.Epoch, error) {
	return g.inner.DurableEpoch(m)
}
func (g *gateFactory) WriteServingMarker(m storageunit.MountRef, e storageunit.Epoch) error {
	return g.inner.WriteServingMarker(m, e)
}
func (g *gateFactory) ReadServingMarker(m storageunit.MountRef) (storageunit.Epoch, bool, error) {
	return g.inner.ReadServingMarker(m)
}

// armPut installs a gate on the gen-0 unit gu so the NEXT Put of key on it
// suspends inside b.Put. Returns the gate.
func (g *gateFactory) armPut(gu storageunit.GenUnit, key []byte) *putGate {
	gate := &putGate{
		target:   append([]byte(nil), key...),
		entered:  make(chan struct{}),
		released: make(chan struct{}),
	}
	g.mu.Lock()
	g.gates[gu] = gate
	g.mu.Unlock()
	return gate
}

func (g *gateFactory) gateFor(gu storageunit.GenUnit, key []byte) *putGate {
	g.mu.Lock()
	defer g.mu.Unlock()
	gate := g.gates[gu]
	if gate == nil {
		return nil
	}
	if gate.target != nil && !bytes.Equal(gate.target, key) {
		return nil
	}
	return gate
}

// gateBackend wraps a per-unit backend.Backend so a single Put can be suspended.
type gateBackend struct {
	backend.Backend
	gu  storageunit.GenUnit
	fac *gateFactory
}

func (b *gateBackend) Put(key, value []byte) error {
	if gate := b.fac.gateFor(b.gu, key); gate != nil && gate.fired.CompareAndSwap(false, true) {
		gate.once.Do(func() { close(gate.entered) })
		<-gate.released
	}
	return b.Backend.Put(key, value)
}

// openGateReshardCluster opens a single-node multi-backend cluster over a
// gateFactory (so the test can suspend a Put inside the backend). Single node:
// it owns the whole unit space, so every Put routes locally and exercises the
// local-write path's pause read-lock. The multi-node freeze mechanics are
// identical per node; the race is per-node, so a single node is the faithful +
// deterministic stage for it.
func openGateReshardCluster(t *testing.T, n int) (*Cluster, *gateFactory) {
	t.Helper()
	fac := newGateFactory()
	c, err := Open(Config{
		NodeID:         "gate-solo",
		BackendFactory: fac,
		UnitCount:      storageunit.MustUnitCount(n),
	})
	if err != nil {
		t.Fatalf("Open gate cluster: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, fac
}

// TestBarrier_InFlightWritePastFreezeIsCapturedNotLost is the P1-A end-to-end
// race regression. A real Put is suspended INSIDE the backend (mid-Put, holding
// the reshard write-pause read-lock) while FREEZE -> BISECT -> FLIP run. The
// bisect must BLOCK on the write-pause WRITE side until the Put lands, so the
// value is copied into the gen-(g+1) child and survives the FLIP that retires
// the gen-0 unit. Without the drain the bisect would snapshot the old unit
// before the Put landed, FLIP would retire it, and the acked write would be
// LOST. We assert the write is acked AND readable at gen g+1.
func TestBarrier_InFlightWritePastFreezeIsCapturedNotLost(t *testing.T) {
	const n = 4
	c, fac := openGateReshardCluster(t, n)
	oldCount := storageunit.MustUnitCount(n)

	// Seed so every unit has gen-0 data to bisect.
	for i := range 40 {
		if err := c.Put(fmt.Appendf(nil, "seed-%03d", i), []byte("s")); err != nil {
			t.Fatalf("seed Put: %v", err)
		}
	}

	// Pick a concrete key + its gen-0 unit. The write of this key will be
	// suspended mid-Put while the barrier runs.
	raceKey := []byte("in-flight-race-key")
	raceVal := []byte("must-survive-the-bisect")
	h := storageunit.HashShardKey(c.shardKey(raceKey))
	oldU := storageunit.UnitForHash(h, oldCount)
	oldGU := storageunit.NewGenUnit(0, oldU)

	// Arm the gate so the Put of raceKey suspends inside gen-0 unit's b.Put.
	gate := fac.armPut(oldGU, raceKey)

	// Start the writer. It passes the (not-yet-set) freeze gate, takes the pause
	// READ side in localWriteBackendForKey, and blocks INSIDE b.Put - exactly the
	// in-flight window the freeze flag flip alone cannot quiesce.
	var putErr atomic.Value // error or nil
	var writerDone sync.WaitGroup
	writerDone.Go(func() {
		err := c.Put(raceKey, raceVal)
		if err != nil {
			putErr.Store(err)
		}
	})

	// Wait until the Put is genuinely suspended inside the backend (pause
	// read-lock held).
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the gated Put never reached the backend; cannot stage the in-flight race")
	}

	// NOW freeze + bisect while the writer is mid-Put. The bisect for oldU must
	// block on the pause WRITE side until the suspended Put releases.
	if err := c.reshardFreeze(1); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	var bisectDone atomic.Bool
	var bisectWG sync.WaitGroup
	bisectWG.Go(func() {
		_ = c.reshardBisect(1)
		bisectDone.Store(true)
	})

	// The bisect must NOT complete while the in-flight writer still holds the
	// pause read-lock (suspended mid-Put). If it does, the WRITE-side drain is
	// missing and the write is about to be lost.
	time.Sleep(120 * time.Millisecond)
	if bisectDone.Load() {
		close(gate.released)
		writerDone.Wait()
		bisectWG.Wait()
		t.Fatal("P1-A VIOLATED: bisect completed while a real Put was suspended mid-Put on the unit; the in-flight write would be lost")
	}

	// Release the suspended Put: it now lands in the gen-0 unit, the bisect's
	// WRITE-lock is granted, and the copy captures the just-landed value.
	close(gate.released)
	writerDone.Wait()
	bisectWG.Wait()

	if e := putErr.Load(); e != nil {
		t.Fatalf("the in-flight Put should have acked (it passed the freeze gate before the freeze): %v", e)
	}
	if !bisectDone.Load() {
		t.Fatal("bisect did not complete after the in-flight writer released")
	}

	// FLIP retires the gen-0 unit. The captured write must survive at gen g+1.
	if err := c.reshardFlip(1); err != nil {
		t.Fatalf("flip: %v", err)
	}
	if err := c.reshardResume(); err != nil {
		t.Fatalf("resume: %v", err)
	}

	got, err := c.Get(raceKey)
	if err != nil {
		t.Fatalf("P1-A VIOLATED: in-flight acked write %q unreadable at gen g+1: %v (acked-then-lost)", raceKey, err)
	}
	if !bytes.Equal(got, raceVal) {
		t.Fatalf("P1-A VIOLATED: in-flight acked write %q = %q, want %q at gen g+1", raceKey, got, raceVal)
	}
	if c.genSnapshot().gen != 1 {
		t.Fatalf("cluster should be at gen 1 after flip, got %d", c.genSnapshot().gen)
	}
}

// TestBarrier_WriteThatHitsFreezeIsRefusedNotLost is the complementary safe
// outcome: a write that has NOT passed the freeze gate when the freeze engages
// is REFUSED with the retryable codes.Unavailable, never acked. Combined with
// the capture case above, this is the full P1-A invariant: an in-flight write is
// either captured into the new units OR refused-retryable, NEVER acked-then-lost.
func TestBarrier_WriteThatHitsFreezeIsRefusedNotLost(t *testing.T) {
	const n = 4
	c, _ := openGateReshardCluster(t, n)

	// Freeze BEFORE the write is issued: the freeze gate refuses it.
	if err := c.reshardFreeze(1); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	err := c.Put([]byte("frozen-window-key"), []byte("must-not-ack"))
	if err == nil {
		t.Fatal("P1-A VIOLATED: a Put issued during the freeze acked; it must be refused (never acked)")
	}
	if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
		t.Fatalf("P1-A VIOLATED: frozen Put code = %v, want codes.Unavailable (retryable; the client retries after RESUME)", st.Code())
	}
	_ = c.reshardAbort(1)
}

// TestLocalWriteBackend_FreezeRecheckRefusesLateWriter pins the RESIDUAL P1-A
// window the WRITE-side drain does NOT close on its own: a writer can pass the
// caller's isFrozen() gate (Put/Delete/CAS), then be descheduled before taking
// the per-old-unit pause RLock - invisible to bisectUnitStatic's drain (which
// only blocks RLocks already held). localWriteBackendForKey re-checks isFrozen()
// AFTER taking that RLock and refuses such a late writer (ok=false -> the
// retryable acquiring-window error). Without the re-check it would resolve at
// gen g (FLIP runs after BISECT, so genState is still g), land its Put in the
// old unit, ack, and be LOST when FLIP retires old-K.
func TestLocalWriteBackend_FreezeRecheckRefusesLateWriter(t *testing.T) {
	const n = 4
	c, _ := openGateReshardCluster(t, n)

	// Not frozen: a write resolves to a mounted, writable backend.
	_, _, unlock, ok := c.localWriteBackendForKey([]byte("late-writer-key"))
	unlock()
	if !ok {
		t.Fatal("precondition: localWriteBackendForKey should resolve a mounted unit when not frozen")
	}

	// Freeze, then call localWriteBackendForKey directly: this models the late
	// writer that already passed the caller's gate and only NOW takes the pause
	// RLock. The post-RLock re-check must refuse it.
	if err := c.reshardFreeze(1); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	_, _, unlock2, ok2 := c.localWriteBackendForKey([]byte("late-writer-key"))
	unlock2()
	if ok2 {
		t.Fatal("P1-A RESIDUAL VIOLATED: localWriteBackendForKey resolved a writable backend while frozen. A writer that passed the isFrozen() gate, then took the pause RLock after the bisect drained, would land in the doomed old unit and be LOST at FLIP. The post-RLock freeze re-check must return ok=false.")
	}
	_ = c.reshardAbort(1)
}
