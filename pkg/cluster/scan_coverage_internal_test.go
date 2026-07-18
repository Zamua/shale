package cluster

// White-box pins for the LOCAL-SCAN COVERAGE GUARD (scanCoverageErr).
//
// Every other refusal in this package is an error that EXISTS and has to keep
// its identity across a node boundary or a fan-out collapse. This one is
// different in kind: without the guard there is no error at all. A cross-shard
// scan walks the mount map, so a position the node OWNS but has not mounted is
// not refused, it is ABSENT, and the scan ends cleanly having returned less
// than it owed. A short answer is indistinguishable from a correct one, which
// is why the fix has to MINT a refusal rather than preserve one.
//
// These tests use the same fixture the mount-readiness pins use, so the guard
// and the readiness probe are demonstrably keyed on one diff: a node cannot
// report itself fully mounted while refusing to scan, or vice versa.

import (
	"errors"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestScanCoverage_FullyMountedNodeScans: the guard must be invisible in the
// steady state. A node holding every position it owns has nothing to refuse,
// and a guard that fired here would take the cross-shard surface down for the
// 99% case it is not about.
func TestScanCoverage_FullyMountedNodeScans(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}
	if r := c.MountReadiness(); r.PendingUnits != 0 {
		t.Fatalf("fixture is not fully mounted: %+v", r)
	}

	if err := c.scanCoverageErr("LocalScan"); err != nil {
		t.Fatalf("fully-mounted node refused its own local scan: %v", err)
	}
	if _, err := c.localScanMounted(nil); err != nil {
		t.Fatalf("fully-mounted node refused localScanMounted: %v", err)
	}
	if _, err := c.localMountedSnapshot(); err != nil {
		t.Fatalf("fully-mounted node refused localMountedSnapshot: %v", err)
	}
}

// TestScanCoverage_UnmountedOwnedPositionRefusesBothScanPaths is the guard
// itself. A node owing positions it has not mounted must refuse, on BOTH local
// scan entry points, with the transient acquiring identity: the peer-facing
// LocalScan path (which the Aggregate fan-out and the blob sweep reach) and the
// in-process snapshot path (Aggregate's local leg).
//
// Refusing is the whole point. Returning the keys it DOES hold would be a
// partial that ends cleanly and reads as complete, and a caller that acts on
// what is absent from a scan - a referenced-blob set driving GC - would treat
// the gap as an authoritative absence and delete live data.
func TestScanCoverage_UnmountedOwnedPositionRefusesBothScanPaths(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	// Deliberately NO mountReplicaUnits: positions are desired, none mounted -
	// exactly a node whose acquires are still in flight.

	if r := c.MountReadiness(); r.PendingUnits == 0 {
		t.Fatalf("fixture must leave positions unmounted: %+v", r)
	}

	for _, tc := range []struct {
		name string
		err  func() error
	}{
		{"scanCoverageErr", func() error { return c.scanCoverageErr("LocalScan") }},
		{"localScanMounted", func() error { _, err := c.localScanMounted(nil); return err }},
		{"localMountedSnapshot", func() error { _, err := c.localMountedSnapshot(); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.err()
			if err == nil {
				t.Fatal("a node with unmounted owned positions returned a scan instead of refusing; " +
					"that scan is a silent partial")
			}
			if !errors.Is(err, ErrAcquiring) {
				t.Fatalf("refusal does not match ErrAcquiring, so a consumer cannot retry it: %v (%T)", err, err)
			}
			if code := status.Code(err); code != codes.Unavailable {
				t.Fatalf("refusal code = %v, want Unavailable (the code the retry shapes read). err=%v", code, err)
			}
		})
	}
}

// TestScanCoverage_TracksMountReadiness pins the guard and the readiness probe
// to ONE diff. They are separate surfaces a consumer can observe (a readiness
// endpoint and a scan result), and the pair is only coherent if they cannot
// disagree: a node that says it is fully mounted must scan, and a node that
// refuses to scan must not claim readiness.
func TestScanCoverage_TracksMountReadiness(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")

	if c.MountReadiness().PendingUnits == 0 {
		t.Fatal("fixture must start with pending positions")
	}
	if err := c.scanCoverageErr("LocalScan"); err == nil {
		t.Fatal("pending positions but the scan guard is quiet: the two surfaces disagree")
	}

	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}

	if c.MountReadiness().PendingUnits != 0 {
		t.Fatal("fixture must end fully mounted")
	}
	if err := c.scanCoverageErr("LocalScan"); err != nil {
		t.Fatalf("no pending positions but the scan guard refused: the two surfaces disagree: %v", err)
	}
}

// TestScanCoverage_LegacyModeUnaffected: single-backend mode has no per-unit
// mount map, so there is no coverage question to ask and the guard must not
// invent one. The legacy path is byte-for-byte unchanged by this work.
func TestScanCoverage_LegacyModeUnaffected(t *testing.T) {
	c := &Cluster{multi: false}
	if err := c.scanCoverageErr("LocalScan"); err != nil {
		t.Fatalf("legacy single-backend cluster refused a scan: %v", err)
	}
}
