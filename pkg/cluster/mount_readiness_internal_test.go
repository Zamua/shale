package cluster

// White-box pins for the mount-readiness surface (docs/SPEC.md "Mount
// readiness"): the per-node mount-state counts (MountReadiness) and the
// Ready(minMountedFraction) predicate. These live in package cluster so they
// can drive the same fixture the Phase 2b/2f white-box tests use
// (newReplicatedCluster + the sharedfactory open-fault seam) without standing
// up membership / gRPC. The wire exposure of the same counts is pinned in
// pkg/rpc/server_test.go.

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestMountReadiness_AllMounted: a clean boot mounts every desired position;
// the counts say so and the node is ready at the strictest fraction.
func TestMountReadiness_AllMounted(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")

	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}

	want := len(c.desiredReplicaUnits())
	if want == 0 {
		t.Fatalf("fixture must desire at least one position")
	}
	r := c.MountReadiness()
	if r.DesiredUnits != want {
		t.Fatalf("DesiredUnits = %d, want %d", r.DesiredUnits, want)
	}
	if r.MountedUnits != want {
		t.Fatalf("MountedUnits = %d, want %d (all mounted)", r.MountedUnits, want)
	}
	if r.PendingUnits != 0 || r.FailedOpenUnits != 0 {
		t.Fatalf("pending/failed = %d/%d, want 0/0", r.PendingUnits, r.FailedOpenUnits)
	}
	if r.LastAcquireError != "" {
		t.Fatalf("LastAcquireError = %q, want empty", r.LastAcquireError)
	}
	if r.MountedUnits+r.PendingUnits != r.DesiredUnits {
		t.Fatalf("invariant Mounted+Pending==Desired violated: %+v", r)
	}
	if !r.Ready(1.0) {
		t.Fatalf("fully-mounted node must be Ready(1.0)")
	}
	if !c.Ready(1.0) {
		t.Fatalf("Cluster.Ready(1.0) must delegate to the same predicate")
	}
}

// TestMountReadiness_NoneMounted: a node that has mounted NOTHING (the
// incident shape: every position deferred/failed at boot) is NOT ready at any
// fraction > 0. This is the pin that makes "0 mounted counts as Ready"
// impossible to reintroduce silently.
func TestMountReadiness_NoneMounted(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	// Deliberately NO mountReplicaUnits: desired positions exist, zero mounted.

	want := len(c.desiredReplicaUnits())
	r := c.MountReadiness()
	if r.DesiredUnits != want || r.MountedUnits != 0 {
		t.Fatalf("desired/mounted = %d/%d, want %d/0", r.DesiredUnits, r.MountedUnits, want)
	}
	if r.PendingUnits != want {
		t.Fatalf("PendingUnits = %d, want %d (everything pending)", r.PendingUnits, want)
	}
	if r.FailedOpenUnits != 0 {
		t.Fatalf("FailedOpenUnits = %d, want 0 (no acquire was attempted)", r.FailedOpenUnits)
	}
	for _, f := range []float64{0.001, 0.5, 1.0} {
		if r.Ready(f) {
			t.Fatalf("0-mounted node must NOT be Ready(%v)", f)
		}
	}
	// Fraction 0 = no floor requested: vacuously ready by the clamp contract.
	if !r.Ready(0) {
		t.Fatalf("Ready(0) must be true (floor of zero positions)")
	}
}

// TestMountReadiness_FailedOpenCounts: an open that ERRORS leaves the position
// pending AND counted in FailedOpenUnits with its error surfaced; repairing +
// re-acquiring clears both. Pins that the counts read the SAME lastAcquireErr
// record the degraded-boot path maintains.
func TestMountReadiness_FailedOpenCounts(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")

	desired := c.desiredReplicaUnits()
	if len(desired) < 2 {
		t.Fatalf("test needs n1 to own >=2 replica positions, got %d", len(desired))
	}
	bad := desired[0]
	injected := errors.New("Data error: empty SSTable (injected)")
	backing.SetOpenReplicaFault(bad, injected)

	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits (degraded boot): %v", err)
	}

	k := len(desired)
	r := c.MountReadiness()
	if r.DesiredUnits != k || r.MountedUnits != k-1 {
		t.Fatalf("desired/mounted = %d/%d, want %d/%d", r.DesiredUnits, r.MountedUnits, k, k-1)
	}
	if r.PendingUnits != 1 {
		t.Fatalf("PendingUnits = %d, want 1", r.PendingUnits)
	}
	if r.FailedOpenUnits != 1 {
		t.Fatalf("FailedOpenUnits = %d, want 1 (the poisoned open errored)", r.FailedOpenUnits)
	}
	if !strings.Contains(r.LastAcquireError, "empty SSTable") {
		t.Fatalf("LastAcquireError = %q, want the injected open error", r.LastAcquireError)
	}
	// Partial thresholds: ready exactly when mounted/desired covers the floor.
	if r.Ready(1.0) {
		t.Fatalf("node with a failed position must NOT be Ready(1.0)")
	}
	if !r.Ready(float64(k-1) / float64(k)) {
		t.Fatalf("node must be Ready at fraction (k-1)/k with k-1 of k mounted")
	}

	// SELF-HEAL: repair + re-acquire clears the failed count and the error.
	backing.SetOpenReplicaFault(bad, nil)
	c.reconcileMu.Lock()
	c.acquireReplicaUnit(bad)
	c.reconcileMu.Unlock()
	r = c.MountReadiness()
	if r.MountedUnits != k || r.PendingUnits != 0 || r.FailedOpenUnits != 0 {
		t.Fatalf("after repair: %+v, want all %d mounted, 0 pending/failed", r, k)
	}
	if r.LastAcquireError != "" {
		t.Fatalf("after repair: LastAcquireError = %q, want empty", r.LastAcquireError)
	}
	if !r.Ready(1.0) {
		t.Fatalf("after repair the node must be Ready(1.0)")
	}
}

// TestMountReadiness_LegacyModeVacuouslyReady: legacy single-backend mode has
// no per-unit mounts; the surface reports zero counts and the predicate is
// vacuously ready (desired == 0).
func TestMountReadiness_LegacyModeVacuouslyReady(t *testing.T) {
	c := &Cluster{} // multi == false: the legacy shape
	r := c.MountReadiness()
	if r != (MountReadiness{}) {
		t.Fatalf("legacy MountReadiness = %+v, want zero value", r)
	}
	for _, f := range []float64{0, 0.5, 1.0} {
		if !c.Ready(f) {
			t.Fatalf("legacy node must be Ready(%v) (nothing to mount)", f)
		}
	}
}

// TestMountReadinessPredicate_Thresholds pins the pure ceil arithmetic:
// ready iff mounted >= ceil(fraction * desired).
func TestMountReadinessPredicate_Thresholds(t *testing.T) {
	cases := []struct {
		desired, mounted int
		fraction         float64
		want             bool
	}{
		{5, 5, 1.0, true},    // all mounted, strict
		{5, 4, 1.0, false},   // one short of strict
		{5, 0, 0.001, false}, // none mounted, any fraction > 0
		{2, 1, 0.5, true},    // exactly at the floor: ceil(1.0) = 1
		{3, 1, 0.5, false},   // under the floor: ceil(1.5) = 2
		{3, 2, 0.5, true},    // over the floor
		{4, 2, 0.5, true},    // exactly half
		{4, 2, 0.51, false},  // ceil(2.04) = 3
		{4, 1, 0.25, true},   // ceil(1.0) = 1
		{0, 0, 1.0, true},    // desired == 0: vacuously ready
		{0, 0, 0.5, true},
		{0, 0, 0.0, true},
	}
	for _, tc := range cases {
		r := MountReadiness{DesiredUnits: tc.desired, MountedUnits: tc.mounted}
		if got := r.Ready(tc.fraction); got != tc.want {
			t.Errorf("Ready(desired=%d mounted=%d f=%v) = %v, want %v",
				tc.desired, tc.mounted, tc.fraction, got, tc.want)
		}
	}
}

// TestMountReadiness_BootDeferredCountsAndClearsAtMountSeam: a position the
// boot mount DEFERS because a peer holds its serving marker (the real Phase 2f
// deferral path) counts in FailedOpenUnits with the boot-deferred message, and
// the record clears when the position mounts THROUGH THE MOUNT SEAM
// (storeMount) alone - no acquire-path per-site Delete involved. The final
// release step is the regression pin: without the seam clear, the stale
// boot-deferred record would resurface as a phantom failed acquire the moment
// the position unmounts again.
func TestMountReadiness_BootDeferredCountsAndClearsAtMountSeam(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	desired := c.desiredReplicaUnits()
	if len(desired) < 2 {
		t.Fatalf("test needs n1 to own >=2 replica positions, got %d", len(desired))
	}
	served := desired[0]

	// A PEER serves the position: opened it and wrote its serving marker, so
	// the boot mount reads the marker and defers via the real deferral path.
	peer := backing.Handle()
	_, peerEpoch, err := peer.OpenReplicaUnit(served, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("peer open: %v", err)
	}
	if err := peer.WriteServingMarker(served, peerEpoch); err != nil {
		t.Fatalf("peer write serving marker: %v", err)
	}

	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}

	k := len(desired)
	r := c.MountReadiness()
	if r.MountedUnits != k-1 || r.PendingUnits != 1 {
		t.Fatalf("mounted/pending = %d/%d, want %d/1", r.MountedUnits, r.PendingUnits, k-1)
	}
	if r.FailedOpenUnits != 1 {
		t.Fatalf("FailedOpenUnits = %d, want 1 (the deferral is a recorded non-mount)", r.FailedOpenUnits)
	}
	if !strings.Contains(r.LastAcquireError, "boot-deferred") {
		t.Fatalf("LastAcquireError = %q, want the boot-deferred message", r.LastAcquireError)
	}

	// The position later mounts THROUGH THE SEAM ONLY (storeMount), the way
	// the reshard split mounts and any future mount site do - deliberately NOT
	// via an acquire path with its own per-site Delete. The seam itself must
	// clear the record.
	b, _, err := c.replicaFactory.OpenReplicaUnit(served, acquireBaseEpoch)
	if err != nil {
		t.Fatalf("open for mount: %v", err)
	}
	c.mountMu.Lock()
	c.storeMount(served, b)
	c.mountMu.Unlock()

	r = c.MountReadiness()
	if r.MountedUnits != k || r.PendingUnits != 0 || r.FailedOpenUnits != 0 || r.LastAcquireError != "" {
		t.Fatalf("after seam mount: %+v, want all %d mounted, no pending/failed", r, k)
	}

	// THE PIN: unmount again (a release never touches lastAcquireErr). A
	// stale record would resurface here; the seam clear means none exists.
	c.reconcileMu.Lock()
	c.releaseReplicaUnit(served)
	c.reconcileMu.Unlock()
	r = c.MountReadiness()
	if r.PendingUnits != 1 {
		t.Fatalf("after release: PendingUnits = %d, want 1", r.PendingUnits)
	}
	if r.FailedOpenUnits != 0 || r.LastAcquireError != "" {
		t.Fatalf("after release: failed=%d err=%q, want 0/empty (the mount seam must have cleared the stale record)", r.FailedOpenUnits, r.LastAcquireError)
	}
}

// TestMountReadiness_LastAcquireErrorPicksMinPosition: with TWO failed
// positions, the one representative LastAcquireError is the failed position
// FIRST IN POSITION ORDER (min ru.String()), stable across repeated calls at
// unchanged state.
func TestMountReadiness_LastAcquireErrorPicksMinPosition(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	desired := c.desiredReplicaUnits()
	if len(desired) < 3 {
		t.Fatalf("test needs n1 to own >=3 replica positions, got %d", len(desired))
	}
	badA, badB := desired[0], desired[1]
	errA := errors.New("injected fault on " + badA.String())
	errB := errors.New("injected fault on " + badB.String())
	backing.SetOpenReplicaFault(badA, errA)
	backing.SetOpenReplicaFault(badB, errB)

	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}

	want := errA.Error()
	if badB.String() < badA.String() {
		want = errB.Error()
	}
	for i := 0; i < 3; i++ {
		r := c.MountReadiness()
		if r.FailedOpenUnits != 2 || r.PendingUnits != 2 {
			t.Fatalf("call %d: failed/pending = %d/%d, want 2/2", i, r.FailedOpenUnits, r.PendingUnits)
		}
		if r.LastAcquireError != want {
			t.Fatalf("call %d: LastAcquireError = %q, want the min-position error %q", i, r.LastAcquireError, want)
		}
	}
}

// TestMountReadiness_NotDesiredCountsNowhere: a position present in mountMap
// (or carrying a stale acquire-error record) that this node does NOT desire -
// the mid-drain loser shape - counts NOWHERE: not mounted, not pending, not
// failed. Readiness is scoped to the desired set (what the node owes).
func TestMountReadiness_NotDesiredCountsNowhere(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}
	desired := c.desiredReplicaUnits()
	k := len(desired)
	if k == 0 {
		t.Fatalf("fixture must desire at least one position")
	}

	// A position this node does NOT desire: the OTHER replica position of a
	// unit it holds (a node appears at most once in a unit's replica set, so
	// that position belongs to a peer). Mount it directly.
	foreign := storageunit.NewReplicaUnit(desired[0].Unit, 1-desired[0].Replica)
	for _, ru := range desired {
		if ru == foreign {
			t.Fatalf("fixture broke the one-position-per-unit assumption: %s is desired", foreign)
		}
	}
	fb, _, err := c.replicaFactory.OpenReplicaUnit(foreign, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("open foreign: %v", err)
	}
	c.mountMu.Lock()
	c.storeMount(foreign, fb)
	c.mountMu.Unlock()
	// And a stale acquire-error record for ANOTHER non-desired position: the
	// desired-set scoping must keep it out of the failed count too.
	last := desired[len(desired)-1]
	foreignPending := storageunit.NewReplicaUnit(last.Unit, 1-last.Replica)
	c.lastAcquireErr.Store(foreignPending, "stale record for a position this node does not desire")

	r := c.MountReadiness()
	if r.DesiredUnits != k || r.MountedUnits != k || r.PendingUnits != 0 || r.FailedOpenUnits != 0 {
		t.Fatalf("counts = %+v, want exactly the clean full mount over %d desired (non-desired positions count nowhere)", r, k)
	}
	if r.LastAcquireError != "" {
		t.Fatalf("LastAcquireError = %q, want empty", r.LastAcquireError)
	}
	if r.MountedUnits+r.PendingUnits != r.DesiredUnits {
		t.Fatalf("invariant Mounted+Pending==Desired violated: %+v", r)
	}
}

// TestMountReadinessPredicate_Clamping pins the fraction clamp to [0, 1]:
// below-range behaves as 0 (no floor), above-range as 1 (full mount), and NaN
// clamps to the conservative end (1) so garbage input cannot disable the gate.
func TestMountReadinessPredicate_Clamping(t *testing.T) {
	partial := MountReadiness{DesiredUnits: 4, MountedUnits: 2}
	full := MountReadiness{DesiredUnits: 4, MountedUnits: 4}

	if !partial.Ready(-3) {
		t.Fatalf("f < 0 must clamp to 0 (always ready)")
	}
	if partial.Ready(7.5) {
		t.Fatalf("f > 1 must clamp to 1 (full mount required); 2/4 is not full")
	}
	if !full.Ready(7.5) {
		t.Fatalf("f > 1 clamps to 1; a fully-mounted node is ready")
	}
	if partial.Ready(math.NaN()) {
		t.Fatalf("NaN must clamp to 1 (conservative); 2/4 is not full")
	}
	if !full.Ready(math.NaN()) {
		t.Fatalf("NaN clamps to 1; a fully-mounted node is ready")
	}
}
