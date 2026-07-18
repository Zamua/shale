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
