package sharedfactory

import (
	"bytes"
	"errors"
	"testing"
	"time"

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

// ru0 builds a generation-0 ReplicaUnit for the serving-marker tests.
func ru0(id storageunit.UnitID, replica uint8) storageunit.ReplicaUnit {
	return storageunit.NewReplicaUnit(gu0(id), replica)
}

// TestServingMarkerCrossNodeReadWrite pins the v0.8 Phase 2e POLL-ONLY release
// signal: the new owner's WriteServingMarker on its handle is observable via
// ReadServingMarker on the OLD owner's DISTINCT handle (the marker lives in the
// shared Backing, exactly like the durable object the slate factory writes).
// Before any write the marker is absent (ok == false); after the new owner
// writes it at its open epoch the old owner reads that exact epoch.
func TestServingMarkerCrossNodeReadWrite(t *testing.T) {
	backing := NewBacking()
	oldOwner := backing.Handle()
	newOwner := backing.Handle()
	ru := ru0(7, 1)

	// No live owner has reached Ready yet: the old owner sees no marker and must
	// keep serving (Draining).
	if e, ok, err := oldOwner.ReadServingMarker(ru); err != nil || ok || e != 0 {
		t.Fatalf("ReadServingMarker before write = (%d,%v,%v), want (0,false,nil)", e, ok, err)
	}

	// New owner mounts, flips, and writes the marker at its open epoch (5).
	if err := newOwner.WriteServingMarker(ru, 5); err != nil {
		t.Fatalf("WriteServingMarker: %v", err)
	}

	// The OLD owner's handle now observes the marker at epoch 5 (cross-node).
	if e, ok, err := oldOwner.ReadServingMarker(ru); err != nil || !ok || e != 5 {
		t.Fatalf("ReadServingMarker after write = (%d,%v,%v), want (5,true,nil)", e, ok, err)
	}
}

// TestServingMarkerMonotonic pins that the marker NEVER rolls back: a stale
// write from a fenced prior owner (at a lower epoch) must not lower a live
// higher-epoch owner's recorded value. A same-or-higher re-write is idempotent.
func TestServingMarkerMonotonic(t *testing.T) {
	backing := NewBacking()
	h := backing.Handle()
	ru := ru0(3, 0)

	if err := h.WriteServingMarker(ru, 9); err != nil {
		t.Fatalf("WriteServingMarker 9: %v", err)
	}
	// A stale lower-epoch write must be ignored (no rollback).
	if err := h.WriteServingMarker(ru, 4); err != nil {
		t.Fatalf("WriteServingMarker 4: %v", err)
	}
	if e, ok, err := h.ReadServingMarker(ru); err != nil || !ok || e != 9 {
		t.Fatalf("after stale write = (%d,%v,%v), want (9,true,nil)", e, ok, err)
	}
	// A higher-epoch write advances it.
	if err := h.WriteServingMarker(ru, 12); err != nil {
		t.Fatalf("WriteServingMarker 12: %v", err)
	}
	if e, _, _ := h.ReadServingMarker(ru); e != 12 {
		t.Fatalf("after higher write = %d, want 12", e)
	}
}

// TestDefaultStrictReadFencing pins the factory's DEFAULT read-fence semantics
// to production shape: with NO toggle calls, a fenced replica handle fails
// READS (Get + ScanPrefix) with ErrFenced too, not just writes - real slatedb
// CLOSES a fenced handle, so every op errors. The permissive reads-pass-through
// model is reachable only via the explicit SetStrictReadFencing(false) opt-out,
// which the test also pins.
func TestDefaultStrictReadFencing(t *testing.T) {
	backing := NewBacking()
	a := backing.Handle()
	b := backing.Handle()
	ru := ru0(4, 0)

	ba, _, err := a.OpenReplicaUnit(ru, 1)
	if err != nil {
		t.Fatalf("A.OpenReplicaUnit: %v", err)
	}
	if err := ba.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("A.Put pre-fence: %v", err)
	}
	// B fences A by acquiring the same replica position at a higher epoch.
	if _, _, err := b.OpenReplicaUnit(ru, 2); err != nil {
		t.Fatalf("B.OpenReplicaUnit: %v", err)
	}

	if _, err := ba.Get([]byte("k")); !errors.Is(err, ErrFenced) {
		t.Fatalf("default fenced Get = %v, want ErrFenced (strict read fencing must be the default)", err)
	}
	if _, err := ba.ScanPrefix([]byte("k")); !errors.Is(err, ErrFenced) {
		t.Fatalf("default fenced ScanPrefix = %v, want ErrFenced (strict read fencing must be the default)", err)
	}

	// The explicit opt-out restores the permissive model: reads pass through
	// the fenced handle (the durable bytes are shared and real).
	backing.SetStrictReadFencing(false)
	if got, err := ba.Get([]byte("k")); err != nil || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("opt-out fenced Get = (%q, %v), want (v, nil)", got, err)
	}
}

// TestDefaultEagerFenceAtOpenStart pins the factory's DEFAULT fence timing to
// production shape: with NO toggle calls, OpenReplicaUnit bumps the durable
// writer-epoch (fencing the prior owner) at open-START, before the modeled
// mount latency - real slatedb's DbBuilder.Build timing. Under the permissive
// fence-at-completion opt-out the epoch would not advance until the
// acquireDelay elapsed, so observing the fence well inside the delay window is
// the discriminator.
func TestDefaultEagerFenceAtOpenStart(t *testing.T) {
	backing := NewBacking()
	a := backing.Handle()
	b := backing.Handle()
	ru := ru0(5, 0)

	ba, _, err := a.OpenReplicaUnit(ru, 1)
	if err != nil {
		t.Fatalf("A.OpenReplicaUnit: %v", err)
	}

	const mountDelay = 1 * time.Second
	b.SetAcquireDelay(mountDelay)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, _, err := b.OpenReplicaUnit(ru, 2); err != nil {
			t.Errorf("B.OpenReplicaUnit: %v", err)
		}
	}()

	// The durable epoch must advance within the FIRST HALF of B's modeled
	// mount: eager fencing bumps it in milliseconds, fence-at-completion not
	// until the full delay has elapsed.
	deadline := time.Now().Add(mountDelay / 2)
	for {
		e, err := a.DurableEpochReplica(ru)
		if err != nil {
			t.Fatalf("DurableEpochReplica: %v", err)
		}
		if e >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("durable epoch still %d halfway through B's mount delay: fence is deferred to open COMPLETION, want eager fence at open-START by default", e)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// A is fenced while B is still mid-mount.
	if err := ba.Put([]byte("mid"), []byte("v")); !errors.Is(err, ErrFenced) {
		t.Fatalf("A.Put mid-mount = %v, want ErrFenced (prior owner fenced at open-START)", err)
	}
	<-done
}

// TestServingMarkerIndependentPositions pins that the marker is per-ReplicaUnit:
// writing replica 0's marker never affects replica 1's (independent positions).
func TestServingMarkerIndependentPositions(t *testing.T) {
	backing := NewBacking()
	h := backing.Handle()

	if err := h.WriteServingMarker(ru0(1, 0), 2); err != nil {
		t.Fatalf("WriteServingMarker r0: %v", err)
	}
	if _, ok, _ := h.ReadServingMarker(ru0(1, 1)); ok {
		t.Fatalf("replica 1 marker ok=true, want false (only replica 0 written)")
	}
	if e, ok, _ := h.ReadServingMarker(ru0(1, 0)); !ok || e != 2 {
		t.Fatalf("replica 0 marker = (%d,%v), want (2,true)", e, ok)
	}
}
