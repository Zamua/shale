package storageunit_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// stubBackend is a no-op backend.Backend so the factory test can return a
// real (interface-satisfying) value without any storage. The unit-model
// domain never calls these methods; it only hands the Backend back to the
// caller. Methods return ErrNotFound / nil so the value is harmless if used.
type stubBackend struct{ closed bool }

func (s *stubBackend) Put(_, _ []byte) error { return nil }
func (s *stubBackend) Get(_ []byte) ([]byte, error) {
	return nil, backend.ErrNotFound
}
func (s *stubBackend) Delete(_ []byte) error { return nil }
func (s *stubBackend) ScanPrefix(_ []byte) (backend.Iterator, error) {
	return nil, nil
}
func (s *stubBackend) Begin(_ backend.IsolationLevel) (backend.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) Close() error { s.closed = true; return nil }

// fakeFactory is an in-memory stand-in for a real (slatedb) BackendFactory.
// It tracks which units are open and at what epoch so the test can drive a
// full lease handoff: open at E, close, re-open at E+1 fences the prior
// writer. It is a TEST DOUBLE, not a shipped implementation - the point is
// to prove BackendFactory is shaped to express the handoff, per the design.
type fakeFactory struct {
	open map[storageunit.UnitID]storageunit.Epoch
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{open: make(map[storageunit.UnitID]storageunit.Epoch)}
}

func (f *fakeFactory) OpenUnit(u storageunit.UnitID, epoch storageunit.Epoch) (backend.Backend, error) {
	if cur, ok := f.open[u]; ok && epoch <= cur {
		return nil, fmt.Errorf("fakeFactory: unit %d already open at epoch %d, refusing open at %d", u, cur, epoch)
	}
	f.open[u] = epoch
	return &stubBackend{}, nil
}

func (f *fakeFactory) CloseUnit(u storageunit.UnitID) error {
	delete(f.open, u)
	return nil
}

func (f *fakeFactory) CurrentEpoch(u storageunit.UnitID) (storageunit.Epoch, bool) {
	e, ok := f.open[u]
	return e, ok
}

// Compile-time assertion: fakeFactory satisfies BackendFactory. If the
// interface shape drifts, this fails to build.
var _ storageunit.BackendFactory = (*fakeFactory)(nil)

func TestBackendFactory_OpenReturnsUsableBackend(t *testing.T) {
	f := newFakeFactory()
	be, err := f.OpenUnit(3, 1)
	if err != nil {
		t.Fatalf("OpenUnit: %v", err)
	}
	if be == nil {
		t.Fatalf("OpenUnit returned nil backend")
	}
	// The returned value really is a backend.Backend (use it).
	if _, err := be.Get([]byte("k")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("stub backend Get should be ErrNotFound, got %v", err)
	}
}

// TestBackendFactory_LeaseHandoffShape walks the handoff the design doc
// describes and proves the interface can express each step:
//
//  1. owner A opens unit U at epoch 1.
//  2. A releases U (CloseUnit).
//  3. owner B opens U at epoch 2 (epoch+1), which would fence a stale A.
//
// The fencing semantics themselves are an infrastructure concern; here we
// only assert the CONTRACT is shaped so a later phase can build them: the
// epoch threads through open, close releases without affecting other units,
// and CurrentEpoch reports the live epoch for choosing the next one.
func TestBackendFactory_LeaseHandoffShape(t *testing.T) {
	a := newFakeFactory() // node A's factory
	b := newFakeFactory() // node B's factory
	const u storageunit.UnitID = 7

	// 1. A opens U at epoch 1, plus an unrelated unit it keeps throughout.
	if _, err := a.OpenUnit(u, 1); err != nil {
		t.Fatalf("A.OpenUnit(%d, 1): %v", u, err)
	}
	if _, err := a.OpenUnit(99, 1); err != nil {
		t.Fatalf("A.OpenUnit(99, 1): %v", err)
	}
	priorEpoch, ok := a.CurrentEpoch(u)
	if !ok || priorEpoch != 1 {
		t.Fatalf("A.CurrentEpoch(%d) = (%d, %v), want (1, true)", u, priorEpoch, ok)
	}

	// 2. A releases U. Its other unit (99) must stay open: a handoff moves
	// ONE unit, not the whole node.
	if err := a.CloseUnit(u); err != nil {
		t.Fatalf("A.CloseUnit(%d): %v", u, err)
	}
	if _, stillOpen := a.CurrentEpoch(u); stillOpen {
		t.Fatalf("A.CurrentEpoch(%d) still open after CloseUnit", u)
	}
	if _, stillOpen := a.CurrentEpoch(99); !stillOpen {
		t.Fatalf("CloseUnit(%d) must not affect unit 99", u)
	}

	// 3. B opens U at priorEpoch+1, fencing the (now released) A writer.
	if _, err := b.OpenUnit(u, priorEpoch+1); err != nil {
		t.Fatalf("B.OpenUnit(%d, %d): %v", u, priorEpoch+1, err)
	}
	if e, ok := b.CurrentEpoch(u); !ok || e != 2 {
		t.Fatalf("B.CurrentEpoch(%d) = (%d, %v), want (2, true)", u, e, ok)
	}
}

func TestBackendFactory_RejectsStaleEpochOpen(t *testing.T) {
	f := newFakeFactory()
	if _, err := f.OpenUnit(1, 5); err != nil {
		t.Fatalf("OpenUnit(1, 5): %v", err)
	}
	// Re-opening at the same or a lower epoch is a stale acquire - the
	// contract lets the implementation reject it (a unit has one writer).
	if _, err := f.OpenUnit(1, 5); err == nil {
		t.Fatalf("OpenUnit at same epoch should be rejected")
	}
	if _, err := f.OpenUnit(1, 4); err == nil {
		t.Fatalf("OpenUnit at lower epoch should be rejected")
	}
	// A strictly higher epoch (a real handoff) is accepted.
	if _, err := f.OpenUnit(1, 6); err != nil {
		t.Fatalf("OpenUnit at higher epoch should succeed: %v", err)
	}
}

func TestBackendFactory_CloseUnitIdempotent(t *testing.T) {
	f := newFakeFactory()
	// Closing a never-opened unit is a no-op (nil error).
	if err := f.CloseUnit(42); err != nil {
		t.Fatalf("CloseUnit on unopened unit should be nil, got %v", err)
	}
}
