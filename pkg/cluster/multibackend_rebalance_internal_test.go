package cluster

// White-box tests for the v0.8 Phase 3 lease-handoff reconcile. These live
// in package cluster so they can drive the unexported reconcile
// (reconcileUnits, acquireUnit, releaseUnit) and inspect the mount map
// directly, with a controllable owner lookup standing in for the ring. The
// wired-together cross-node copy-free handoff is covered end to end in
// tests/integration/lease_handoff_test.go.

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// fakeOwnership is a mutable OwnerLookup a test can flip to simulate the
// ring re-assigning a unit. owner[u] = node that owns u; absent means
// no owner (skipped).
type fakeOwnership map[storageunit.UnitID]storageunit.NodeID

func (f fakeOwnership) OwnerOf(u storageunit.UnitID) (storageunit.NodeID, bool) {
	n, ok := f[u]
	return n, ok
}

// newReconcileCluster builds a minimal multi-backend Cluster wired to a
// shared-backing factory handle + a mutable owner lookup, WITHOUT membership
// / gRPC, so a test can drive reconcileUnits against a changing ownership
// map. self is this node's id; n is the unit count.
func newReconcileCluster(t *testing.T, self string, n int, backing *sharedfactory.Backing, owners fakeOwnership) *Cluster {
	t.Helper()
	h := backing.Handle()
	c := &Cluster{
		cfg:         Config{NodeID: self},
		multi:       true,
		factory:     h,
		unitCount:   storageunit.MustUnitCount(n),
		ownerLookup: owners,
		mountMap:    make(map[storageunit.UnitID]backend.Backend),
		closeCh:     make(chan struct{}),
	}
	return c
}

// mountedUnits returns this node's mounted unit ids (sorted by the factory).
func mountedUnits(c *Cluster) []storageunit.UnitID {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	out := make([]storageunit.UnitID, 0, len(c.mountMap))
	for u := range c.mountMap {
		out = append(out, u)
	}
	// insertion-sort for determinism
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func hasUnit(c *Cluster, u storageunit.UnitID) bool {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	_, ok := c.mountMap[u]
	return ok
}

// TestReconcileAcquiresNewlyOwned: a node that owns no units, then becomes
// the owner of two, acquires exactly those two on reconcile.
func TestReconcileAcquiresNewlyOwned(t *testing.T) {
	backing := sharedfactory.NewBacking()
	owners := fakeOwnership{0: "B", 1: "B", 2: "B", 3: "B"}
	c := newReconcileCluster(t, "A", 4, backing, owners)

	// Initially A owns nothing.
	c.reconcileUnits()
	if got := mountedUnits(c); len(got) != 0 {
		t.Fatalf("A mounted %v, want none", got)
	}

	// Ring re-assigns units 1 and 3 to A.
	owners[1] = "A"
	owners[3] = "A"
	c.reconcileUnits()
	got := mountedUnits(c)
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("A mounted %v, want [1 3]", got)
	}
}

// TestReconcileReleasesNoLongerOwned: a node that owns a unit, then loses
// it, releases (CloseUnit) it on reconcile.
func TestReconcileReleasesNoLongerOwned(t *testing.T) {
	backing := sharedfactory.NewBacking()
	owners := fakeOwnership{0: "A", 1: "A"}
	c := newReconcileCluster(t, "A", 2, backing, owners)

	c.reconcileUnits()
	if got := mountedUnits(c); len(got) != 2 {
		t.Fatalf("A mounted %v, want [0 1]", got)
	}

	// Ring moves unit 1 off A.
	owners[1] = "B"
	c.reconcileUnits()
	if got := mountedUnits(c); len(got) != 1 || got[0] != 0 {
		t.Fatalf("after release, A mounted %v, want [0]", got)
	}
	// The factory handle no longer holds unit 1 either.
	if _, ok := c.factory.CurrentEpoch(1); ok {
		t.Fatal("factory still holds unit 1 after release")
	}
}

// TestReconcileIdempotent: running reconcile repeatedly with no ownership
// change does no further mount/unmount work (steady state is a no-op).
func TestReconcileIdempotent(t *testing.T) {
	backing := sharedfactory.NewBacking()
	owners := fakeOwnership{0: "A", 1: "A", 2: "B", 3: "B"}
	c := newReconcileCluster(t, "A", 4, backing, owners)

	c.reconcileUnits()
	first := mountedUnits(c)
	// Capture the epoch each owned unit was opened at; a second reconcile
	// must NOT re-open (which would bump the epoch).
	e0, _ := c.factory.CurrentEpoch(0)
	for i := 0; i < 5; i++ {
		c.reconcileUnits()
	}
	if got := mountedUnits(c); len(got) != len(first) || got[0] != first[0] || got[1] != first[1] {
		t.Fatalf("idempotent reconcile changed mounted set: %v -> %v", first, got)
	}
	if e, _ := c.factory.CurrentEpoch(0); e != e0 {
		t.Fatalf("idempotent reconcile re-opened unit 0: epoch %d -> %d", e0, e)
	}
}

// TestReconcileSelfHealsLostMount: a node that should own U but lost its
// mount (e.g. a transient OpenUnit failure on a prior pass) re-acquires it
// on the next reconcile.
func TestReconcileSelfHealsLostMount(t *testing.T) {
	backing := sharedfactory.NewBacking()
	owners := fakeOwnership{0: "A", 1: "A"}
	c := newReconcileCluster(t, "A", 2, backing, owners)

	c.reconcileUnits()
	if !hasUnit(c, 1) {
		t.Fatal("precondition: A should own + mount unit 1")
	}

	// Simulate a lost mount: drop unit 1 from the map (and from the handle)
	// without changing ownership.
	c.mountMu.Lock()
	delete(c.mountMap, 1)
	c.mountMu.Unlock()
	_ = c.factory.CloseUnit(1)

	// Reconcile re-acquires it (still owned, not mounted -> acquire).
	c.reconcileUnits()
	if !hasUnit(c, 1) {
		t.Fatal("reconcile did not self-heal the lost mount of unit 1")
	}
}

// TestAcquireFencesPriorOwner: the safety core. Node A owns + writes unit U,
// then the ring hands U to node B. B's reconcile ACQUIRES U at a higher
// epoch, which FENCES A: A's backend (still held) can no longer write, and B
// sees A's acked write copy-free.
func TestAcquireFencesPriorOwner(t *testing.T) {
	backing := sharedfactory.NewBacking()
	const u storageunit.UnitID = 1
	ownersA := fakeOwnership{0: "A", 1: "A"}
	ownersB := fakeOwnership{0: "A", 1: "A"}
	a := newReconcileCluster(t, "A", 2, backing, ownersA)
	b := newReconcileCluster(t, "B", 2, backing, ownersB)

	// A mounts its units and writes an ACKED key directly to U's backend.
	a.reconcileUnits()
	abk := a.mountMap[u]
	if abk == nil {
		t.Fatal("A did not mount unit 1")
	}
	if err := abk.Put([]byte("acked"), []byte("v")); err != nil {
		t.Fatalf("A write to U: %v", err)
	}

	// Ring hands U to B. B reconciles -> acquires U at a higher epoch.
	ownersB[1] = "B"
	b.reconcileUnits()
	bbk := b.mountMap[u]
	if bbk == nil {
		t.Fatal("B did not acquire unit 1 on reconcile")
	}

	// B sees A's acked write copy-free (NO ACKED WRITE LOST).
	got, err := bbk.Get([]byte("acked"))
	if err != nil || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("B.Get acked after handoff: got=%q err=%v (acked write lost)", got, err)
	}

	// A is fenced: A's still-held backend cannot write U anymore.
	if err := abk.Put([]byte("stale"), []byte("x")); !errors.Is(err, sharedfactory.ErrFenced) {
		t.Fatalf("A write after B acquired: got %v, want ErrFenced", err)
	}

	// Now A reconciles (it lost U): it releases U cleanly.
	ownersA[1] = "B"
	a.reconcileUnits()
	if hasUnit(a, u) {
		t.Fatal("A still mounts U after it lost ownership")
	}
}

// TestAcquireOpensAtHigherEpoch: acquireUnit must open at an epoch strictly
// above the unit's durable epoch, so the durable epoch advances on every
// handoff (the monotonic fence).
func TestAcquireOpensAtHigherEpoch(t *testing.T) {
	backing := sharedfactory.NewBacking()
	const u storageunit.UnitID = 0
	ownersA := fakeOwnership{0: "A"}
	ownersB := fakeOwnership{0: "A"}
	a := newReconcileCluster(t, "A", 1, backing, ownersA)
	b := newReconcileCluster(t, "B", 1, backing, ownersB)

	a.reconcileUnits()
	epochAfterA := backing.DurableEpoch(u)
	if epochAfterA == 0 {
		t.Fatalf("durable epoch still 0 after A acquired")
	}

	// Hand off to B.
	ownersB[0] = "B"
	b.reconcileUnits()
	epochAfterB := backing.DurableEpoch(u)
	if epochAfterB <= epochAfterA {
		t.Fatalf("durable epoch did not advance on handoff: A=%d B=%d (no fence)", epochAfterA, epochAfterB)
	}
}
