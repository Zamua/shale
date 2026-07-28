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
	"sync"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// fakeOwnership is a mutable owner map a test can flip to simulate the ring
// re-assigning a unit. owner[u] = node that owns unit u (at generation 0;
// these Phase 3 reconcile tests do not reshard); absent means no owner
// (skipped). It is adapted to the cluster's generation-aware genOwner via
// OwnerOfGen, which keys on the GenUnit's UnitID.
type fakeOwnership map[storageunit.UnitID]storageunit.NodeID

// OwnerOfGen answers ownership for a GenUnit by its UnitID, ignoring the
// generation (the reconcile tests stay at gen 0). This lets a UnitID-keyed
// fake drive the GenUnit-shaped genOwner hook.
func (f fakeOwnership) OwnerOfGen(gu storageunit.GenUnit) (storageunit.NodeID, bool) {
	n, ok := f[gu.ID]
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
		cfg:        Config{NodeID: self},
		multi:      true,
		factory:    h,
		unitCount:  storageunit.MustUnitCount(n),
		genOwner:   owners.OwnerOfGen,
		pauseUnits: make(map[storageunit.UnitID]*sync.RWMutex),
		closeCh:    make(chan struct{}),
	}
	c.mounts.init(c)
	c.initGenState()
	return c
}

// gu0 builds a generation-0 GenUnit (these reconcile tests stay at gen 0).
func gu0(id storageunit.UnitID) storageunit.GenUnit {
	return storageunit.NewGenUnit(0, id)
}

// mountedUnits returns this node's mounted unit ids (gen-0, ascending). The
// reconcile tests do not reshard, so every mounted GenUnit is at generation 0.
func mountedUnits(c *Cluster) []storageunit.UnitID {
	mounted := c.mounts.mountedList()
	out := make([]storageunit.UnitID, 0, len(mounted))
	for _, ru := range mounted {
		out = append(out, ru.Unit.ID)
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
	_, ok := c.mounts.backendFor(replica0(gu0(u)))
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
	if _, ok := c.factory.CurrentEpoch(storageunit.SoleMount(gu0(1))); ok {
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
	e0, _ := c.factory.CurrentEpoch(storageunit.SoleMount(gu0(0)))
	for range 5 {
		c.reconcileUnits()
	}
	if got := mountedUnits(c); len(got) != len(first) || got[0] != first[0] || got[1] != first[1] {
		t.Fatalf("idempotent reconcile changed mounted set: %v -> %v", first, got)
	}
	if e, _ := c.factory.CurrentEpoch(storageunit.SoleMount(gu0(0))); e != e0 {
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
	c.mounts.unmount(replica0(gu0(1)))
	_ = c.factory.CloseUnit(storageunit.SoleMount(gu0(1)))

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
	abk, _ := a.mounts.backendFor(replica0(gu0(u)))
	if abk == nil {
		t.Fatal("A did not mount unit 1")
	}
	if err := abk.Put([]byte("acked"), []byte("v")); err != nil {
		t.Fatalf("A write to U: %v", err)
	}

	// Ring hands U to B. B reconciles -> acquires U at a higher epoch.
	ownersB[1] = "B"
	b.reconcileUnits()
	bbk, _ := b.mounts.backendFor(replica0(gu0(u)))
	if bbk == nil {
		t.Fatal("B did not acquire unit 1 on reconcile")
	}

	// B sees A's acked write copy-free (NO ACKED WRITE LOST).
	got, err := bbk.Get([]byte("acked"))
	if err != nil || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("B.Get acked after handoff: got=%q err=%v (acked write lost)", got, err)
	}

	// A is fenced: B acquired U at a higher epoch, so A's stale mount cannot
	// write U anymore. Via the fence-self-healing decorator (the mount seam) the
	// fenced write does NOT surface the raw fence - it recodes to the TRANSIENT
	// acquiring-window error AND evicts A's stale mount, so a retry re-routes to
	// the new owner instead of wedging on the dead handle (#433).
	if err := abk.Put([]byte("stale"), []byte("x")); !errors.Is(err, errAcquiringSentinel) {
		t.Fatalf("A write after B acquired: got %v, want the transient acquiring error", err)
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
	epochAfterA := backing.DurableEpoch(gu0(u))
	if epochAfterA == 0 {
		t.Fatalf("durable epoch still 0 after A acquired")
	}

	// Hand off to B.
	ownersB[0] = "B"
	b.reconcileUnits()
	epochAfterB := backing.DurableEpoch(gu0(u))
	if epochAfterB <= epochAfterA {
		t.Fatalf("durable epoch did not advance on handoff: A=%d B=%d (no fence)", epochAfterA, epochAfterB)
	}
}
