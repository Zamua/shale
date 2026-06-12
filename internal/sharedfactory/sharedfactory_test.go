package sharedfactory

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// gu0 builds a generation-0 GenUnit for the lease-handoff tests (handoff is
// a generation-stable operation; only the doubling reshard advances the gen).
func gu0(id storageunit.UnitID) storageunit.GenUnit {
	return storageunit.NewGenUnit(0, id)
}

// TestCopyFreeHandoff is the core property: when unit U's lease moves from
// node A's handle to node B's handle, B opens the SAME underlying bytes A
// wrote - zero copy. B must see every key A persisted before the handoff.
func TestCopyFreeHandoff(t *testing.T) {
	backing := NewBacking()
	a := backing.Handle()
	b := backing.Handle()
	u := gu0(3)

	// A acquires U at epoch 1 and writes some keys (these are "acked" - they
	// returned success, so they are durable).
	ba, err := a.OpenUnit(u, 1)
	if err != nil {
		t.Fatalf("A.OpenUnit: %v", err)
	}
	for i := range 5 {
		k := []byte{byte('k'), byte('0' + i)}
		if err := ba.Put(k, []byte("acked")); err != nil {
			t.Fatalf("A.Put %q: %v", k, err)
		}
	}

	// Handoff: A releases (CloseUnit, flush), B acquires at a higher epoch.
	if err := a.CloseUnit(u); err != nil {
		t.Fatalf("A.CloseUnit: %v", err)
	}
	bb, err := b.OpenUnit(u, 2)
	if err != nil {
		t.Fatalf("B.OpenUnit at higher epoch: %v", err)
	}

	// B sees all 5 acked keys WITHOUT any copy - it opened the same store.
	for i := range 5 {
		k := []byte{byte('k'), byte('0' + i)}
		got, err := bb.Get(k)
		if err != nil {
			t.Fatalf("B.Get %q after handoff: %v (acked write lost!)", k, err)
		}
		if !bytes.Equal(got, []byte("acked")) {
			t.Fatalf("B.Get %q = %q, want %q", k, got, "acked")
		}
	}
}

// TestFenceLocksOutPriorOwner pins the single-writer guarantee: once B
// acquires U at a higher epoch, A's still-open backend is FENCED - its
// further writes fail with ErrFenced. This is the safety core: the prior
// owner cannot keep writing to a unit whose lease has moved.
func TestFenceLocksOutPriorOwner(t *testing.T) {
	backing := NewBacking()
	a := backing.Handle()
	b := backing.Handle()
	u := gu0(0)

	ba, err := a.OpenUnit(u, 1)
	if err != nil {
		t.Fatalf("A.OpenUnit: %v", err)
	}
	// A can write before the handoff.
	if err := ba.Put([]byte("pre"), []byte("v")); err != nil {
		t.Fatalf("A.Put pre-handoff: %v", err)
	}

	// B acquires at a higher epoch (the fence point) - note A has NOT closed
	// yet, modeling A not having observed the membership change.
	if _, err := b.OpenUnit(u, 2); err != nil {
		t.Fatalf("B.OpenUnit at higher epoch: %v", err)
	}

	// A is now fenced: any further write fails.
	if err := ba.Put([]byte("post"), []byte("v")); !errors.Is(err, ErrFenced) {
		t.Fatalf("A.Put after B fenced it: got %v, want ErrFenced", err)
	}
	if err := ba.Delete([]byte("pre")); !errors.Is(err, ErrFenced) {
		t.Fatalf("A.Delete after fence: got %v, want ErrFenced", err)
	}
	if _, err := ba.Begin(backend.SnapshotIsolation); !errors.Is(err, ErrFenced) {
		t.Fatalf("A.Begin after fence: got %v, want ErrFenced", err)
	}

	// The fenced A.Put("post") must NOT have landed: B (the live owner)
	// must not see it.
	bb, _ := backing.UnitStore(u)
	if _, err := bb.Get([]byte("post")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("fenced write leaked into shared store: got %v, want ErrNotFound", err)
	}
	// And A's pre-handoff write IS visible to B (no acked write lost).
	if got, err := bb.Get([]byte("pre")); err != nil || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("acked pre-handoff write lost: got=%q err=%v", got, err)
	}
}

// TestColdAcquireFencesAboveDurable pins the cross-node fence: a handle that
// does NOT hold a unit acquires it by fencing ABOVE the durable epoch, using
// the intended epoch only as a floor. A low / stale intended epoch (the
// common case: a new owner's CurrentEpoch is ok=false, so the cluster passes
// the base floor) STILL fences correctly because the durable manifest, not
// the in-process hint, governs. This is the property a real slatedb factory
// has and the per-factory memfactory cannot model.
func TestColdAcquireFencesAboveDurable(t *testing.T) {
	backing := NewBacking()
	a := backing.Handle()
	b := backing.Handle()
	u := gu0(7)

	if _, err := a.OpenUnit(u, 5); err != nil {
		t.Fatalf("A.OpenUnit at 5: %v", err)
	}
	if backing.DurableEpoch(u) != 5 {
		t.Fatalf("durable epoch = %d, want 5", backing.DurableEpoch(u))
	}

	// B acquires with a LOW intended epoch (1) - it never held u, so its
	// local CurrentEpoch is ok=false and the cluster would pass the base
	// floor. The backing must still fence ABOVE the durable 5, landing at 6.
	if _, err := b.OpenUnit(u, 1); err != nil {
		t.Fatalf("B.OpenUnit cold-acquire with low floor: %v (a new owner must always be able to take the lease)", err)
	}
	if backing.DurableEpoch(u) != 6 {
		t.Fatalf("cold acquire landed at durable epoch %d, want 6 (fence above prior 5)", backing.DurableEpoch(u))
	}
}

// TestDoubleOpenSameHandleRejected pins the OTHER half of the contract: a
// handle re-opening a unit IT ALREADY HOLDS at an equal-or-lower epoch is a
// double-open programming error (one live writer per handle). A
// strictly-higher re-open by the same handle is allowed.
func TestDoubleOpenSameHandleRejected(t *testing.T) {
	backing := NewBacking()
	a := backing.Handle()
	u := gu0(2)

	if _, err := a.OpenUnit(u, 3); err != nil {
		t.Fatalf("A.OpenUnit at 3: %v", err)
	}
	// Re-open at equal / lower on the SAME handle: rejected.
	for _, e := range []storageunit.Epoch{3, 2} {
		if _, err := a.OpenUnit(u, e); err == nil {
			t.Fatalf("A re-open at %d (already holds at 3): expected error, got nil", e)
		}
	}
	// Strictly-higher re-open on the same handle is fine.
	if _, err := a.OpenUnit(u, 4); err != nil {
		t.Fatalf("A re-open at 4: %v", err)
	}
}

// TestHandlesIndependentOpenSets confirms each handle tracks its OWN open
// set (OpenUnits / CurrentEpoch are the local in-process view), while the
// durable epoch is shared. Two handles can have different units open.
func TestHandlesIndependentOpenSets(t *testing.T) {
	backing := NewBacking()
	a := backing.Handle()
	b := backing.Handle()

	if _, err := a.OpenUnit(gu0(0), 1); err != nil {
		t.Fatalf("A.OpenUnit 0: %v", err)
	}
	if _, err := a.OpenUnit(gu0(1), 1); err != nil {
		t.Fatalf("A.OpenUnit 1: %v", err)
	}
	if _, err := b.OpenUnit(gu0(2), 1); err != nil {
		t.Fatalf("B.OpenUnit 2: %v", err)
	}

	if got := a.OpenUnits(); len(got) != 2 || got[0] != gu0(0) || got[1] != gu0(1) {
		t.Fatalf("A.OpenUnits = %v, want [g0/u0 g0/u1]", got)
	}
	if got := b.OpenUnits(); len(got) != 1 || got[0] != gu0(2) {
		t.Fatalf("B.OpenUnits = %v, want [g0/u2]", got)
	}
	// CurrentEpoch is local: A holds 0 at epoch 1, B does not hold 0.
	if e, ok := a.CurrentEpoch(gu0(0)); !ok || e != 1 {
		t.Fatalf("A.CurrentEpoch(0) = (%d,%v), want (1,true)", e, ok)
	}
	if _, ok := b.CurrentEpoch(gu0(0)); ok {
		t.Fatalf("B.CurrentEpoch(0) ok=true, want false (B never opened unit 0)")
	}
}
