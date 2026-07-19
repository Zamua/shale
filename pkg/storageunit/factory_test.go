package storageunit_test

import (
	"errors"
	"fmt"
	"slices"
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
// It tracks which MOUNTS are open and at what epoch so the test can drive a
// full lease handoff: open at E, close, re-open at E+1 fences the prior
// writer. It is a TEST DOUBLE, not a shipped implementation - the point is
// to prove BackendFactory is shaped to express the handoff, per the design.
//
// The store is keyed by the WHOLE MountRef, so every coordinate of the
// identity separates entries: generation, replica position, AND the layout
// selector. Keying it by anything narrower would let two distinct mounts alias.
type fakeFactory struct {
	open   map[storageunit.MountRef]storageunit.Epoch
	marker map[storageunit.MountRef]storageunit.Epoch
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{
		open:   make(map[storageunit.MountRef]storageunit.Epoch),
		marker: make(map[storageunit.MountRef]storageunit.Epoch),
	}
}

func (f *fakeFactory) OpenUnit(m storageunit.MountRef, epoch storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	if cur, ok := f.open[m]; ok && epoch <= cur {
		return nil, 0, fmt.Errorf("fakeFactory: %s already open at epoch %d, refusing open at %d", m, cur, epoch)
	}
	f.open[m] = epoch
	return &stubBackend{}, epoch, nil
}

func (f *fakeFactory) CloseUnit(m storageunit.MountRef) error {
	delete(f.open, m)
	return nil
}

func (f *fakeFactory) CurrentEpoch(m storageunit.MountRef) (storageunit.Epoch, bool) {
	e, ok := f.open[m]
	return e, ok
}

func (f *fakeFactory) OpenUnits() []storageunit.MountRef {
	ms := make([]storageunit.MountRef, 0, len(f.open))
	for m := range f.open {
		ms = append(ms, m)
	}
	slices.SortFunc(ms, storageunit.CompareMountRefs)
	return ms
}

func (f *fakeFactory) DurableEpoch(m storageunit.MountRef) (storageunit.Epoch, error) {
	return f.open[m], nil
}

func (f *fakeFactory) WriteServingMarker(m storageunit.MountRef, epoch storageunit.Epoch) error {
	if cur, ok := f.marker[m]; ok && cur >= epoch {
		return nil
	}
	f.marker[m] = epoch
	return nil
}

func (f *fakeFactory) ReadServingMarker(m storageunit.MountRef) (storageunit.Epoch, bool, error) {
	e, ok := f.marker[m]
	return e, ok, nil
}

// gu0 builds a generation-0 GenUnit for the bare-id test cases.
func gu0(id storageunit.UnitID) storageunit.GenUnit {
	return storageunit.NewGenUnit(0, id)
}

// sole is the sole-mount ref of a generation-0 unit, the identity the R=1
// paths open.
func sole(id storageunit.UnitID) storageunit.MountRef {
	return storageunit.SoleMount(gu0(id))
}

// Compile-time assertion: fakeFactory satisfies BackendFactory. If the
// interface shape drifts, this fails to build.
var _ storageunit.BackendFactory = (*fakeFactory)(nil)

func TestBackendFactory_OpenReturnsUsableBackend(t *testing.T) {
	f := newFakeFactory()
	be, _, err := f.OpenUnit(sole(3), 1)
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

// TestBackendFactory_OpenUnitsTracksMountedSet pins the enumerator the
// anti-entropy reconcile diffs its desired set against: OpenUnits returns exactly
// the currently-mounted units, ascending, and CloseUnit removes one without
// disturbing the rest.
func TestBackendFactory_OpenUnitsTracksMountedSet(t *testing.T) {
	f := newFakeFactory()
	if got := f.OpenUnits(); len(got) != 0 {
		t.Fatalf("fresh factory OpenUnits = %v, want empty", got)
	}
	for _, u := range []storageunit.UnitID{5, 1, 9} {
		if _, _, err := f.OpenUnit(sole(u), 1); err != nil {
			t.Fatalf("OpenUnit(%d): %v", u, err)
		}
	}
	if got, want := f.OpenUnits(), []storageunit.MountRef{sole(1), sole(5), sole(9)}; !slices.Equal(got, want) {
		t.Fatalf("OpenUnits = %v, want %v (ascending mounted set)", got, want)
	}
	if err := f.CloseUnit(sole(5)); err != nil {
		t.Fatalf("CloseUnit(5): %v", err)
	}
	if got, want := f.OpenUnits(), []storageunit.MountRef{sole(1), sole(9)}; !slices.Equal(got, want) {
		t.Fatalf("OpenUnits after CloseUnit(5) = %v, want %v", got, want)
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
	u := gu0(7)

	// 1. A opens U at epoch 1, plus an unrelated unit it keeps throughout.
	if _, _, err := a.OpenUnit(storageunit.SoleMount(u), 1); err != nil {
		t.Fatalf("A.OpenUnit(%s, 1): %v", u, err)
	}
	if _, _, err := a.OpenUnit(sole(99), 1); err != nil {
		t.Fatalf("A.OpenUnit(99, 1): %v", err)
	}
	priorEpoch, ok := a.CurrentEpoch(storageunit.SoleMount(u))
	if !ok || priorEpoch != 1 {
		t.Fatalf("A.CurrentEpoch(%s) = (%d, %v), want (1, true)", u, priorEpoch, ok)
	}

	// 2. A releases U. Its other unit (99) must stay open: a handoff moves
	// ONE unit, not the whole node.
	if err := a.CloseUnit(storageunit.SoleMount(u)); err != nil {
		t.Fatalf("A.CloseUnit(%s): %v", u, err)
	}
	if _, stillOpen := a.CurrentEpoch(storageunit.SoleMount(u)); stillOpen {
		t.Fatalf("A.CurrentEpoch(%s) still open after CloseUnit", u)
	}
	if _, stillOpen := a.CurrentEpoch(sole(99)); !stillOpen {
		t.Fatalf("CloseUnit(%s) must not affect unit 99", u)
	}

	// 3. B opens U at priorEpoch+1, fencing the (now released) A writer.
	if _, _, err := b.OpenUnit(storageunit.SoleMount(u), priorEpoch+1); err != nil {
		t.Fatalf("B.OpenUnit(%s, %d): %v", u, priorEpoch+1, err)
	}
	if e, ok := b.CurrentEpoch(storageunit.SoleMount(u)); !ok || e != 2 {
		t.Fatalf("B.CurrentEpoch(%s) = (%d, %v), want (2, true)", u, e, ok)
	}
}

func TestBackendFactory_RejectsStaleEpochOpen(t *testing.T) {
	f := newFakeFactory()
	if _, _, err := f.OpenUnit(sole(1), 5); err != nil {
		t.Fatalf("OpenUnit(1, 5): %v", err)
	}
	// Re-opening at the same or a lower epoch is a stale acquire - the
	// contract lets the implementation reject it (a unit has one writer).
	if _, _, err := f.OpenUnit(sole(1), 5); err == nil {
		t.Fatalf("OpenUnit at same epoch should be rejected")
	}
	if _, _, err := f.OpenUnit(sole(1), 4); err == nil {
		t.Fatalf("OpenUnit at lower epoch should be rejected")
	}
	// A strictly higher epoch (a real handoff) is accepted.
	if _, _, err := f.OpenUnit(sole(1), 6); err != nil {
		t.Fatalf("OpenUnit at higher epoch should succeed: %v", err)
	}
}

// TestBackendFactory_GenerationsAreDistinctDatabases pins the Phase 4
// property the whole reshard relies on: two GenUnits that share a UnitID but
// differ in Generation are INDEPENDENT. Opening gen-1 unit K does not fence
// or disturb gen-0 unit K (the online bisect keeps the old unit serving while
// the new one fills), and each tracks its own epoch.
func TestBackendFactory_GenerationsAreDistinctDatabases(t *testing.T) {
	f := newFakeFactory()
	oldK := storageunit.NewGenUnit(0, 2)
	newK := storageunit.NewGenUnit(1, 2)
	if _, _, err := f.OpenUnit(storageunit.SoleMount(oldK), 1); err != nil {
		t.Fatalf("OpenUnit(%s): %v", oldK, err)
	}
	// Opening the SAME UnitID at the next generation must NOT be a double-open
	// error - it is a different database. Open at a low epoch to prove it does
	// not consult the old unit's epoch.
	if _, _, err := f.OpenUnit(storageunit.SoleMount(newK), 1); err != nil {
		t.Fatalf("OpenUnit(%s) should succeed independently of %s: %v", newK, oldK, err)
	}
	if _, ok := f.CurrentEpoch(storageunit.SoleMount(oldK)); !ok {
		t.Fatalf("gen-0 unit 2 must stay open while gen-1 unit 2 is opened")
	}
	if _, ok := f.CurrentEpoch(storageunit.SoleMount(newK)); !ok {
		t.Fatalf("gen-1 unit 2 must be open")
	}
	// Closing the new generation must not touch the old one.
	if err := f.CloseUnit(storageunit.SoleMount(newK)); err != nil {
		t.Fatalf("CloseUnit(%s): %v", newK, err)
	}
	if _, ok := f.CurrentEpoch(storageunit.SoleMount(oldK)); !ok {
		t.Fatalf("closing gen-1 unit 2 must not retire gen-0 unit 2")
	}
}

func TestBackendFactory_CloseUnitIdempotent(t *testing.T) {
	f := newFakeFactory()
	// Closing a never-opened unit is a no-op (nil error).
	if err := f.CloseUnit(sole(42)); err != nil {
		t.Fatalf("CloseUnit on unopened unit should be nil, got %v", err)
	}
}
