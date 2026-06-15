package shaled

import (
	"bytes"
	"io"
	"log"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestClusterConfig_SingleBackendMapping pins the pure RunConfig ->
// cluster.Config mapping for the LEGACY single-backend path: the Backend is
// carried through, ReplicationFactor is threaded (the fix this milestone makes
// - it used to be dropped), and UnitCount stays ZERO so cluster's
// validateBackendMode accepts the legacy mode. BackendFactory must be nil.
func TestClusterConfig_SingleBackendMapping(t *testing.T) {
	be := memory.New()
	cc := clusterConfig(RunConfig{
		Std: StdConfig{
			NodeID:            "n1",
			BindAddr:          ":7946",
			Seeds:             []string{"a:1"},
			ReplicationFactor: 3,
			UnitCount:         storageunit.MustUnitCount(8), // must be ignored in single-backend mode
		},
		Backend: be,
	}, "1.2.3.4:9999")

	if cc.Backend == nil {
		t.Fatal("single-backend mapping: Backend must be carried through, got nil")
	}
	if cc.BackendFactory != nil {
		t.Fatal("single-backend mapping: BackendFactory must be nil")
	}
	if cc.ReplicationFactor != 3 {
		t.Fatalf("single-backend mapping: ReplicationFactor want 3 (threaded), got %d", cc.ReplicationFactor)
	}
	if !cc.UnitCount.IsZero() {
		t.Fatalf("single-backend mapping: UnitCount must be zero in legacy mode, got N=%d", cc.UnitCount.N())
	}
	if cc.NodeID != "n1" || cc.BindAddr != ":7946" || cc.GRPCAddr != "1.2.3.4:9999" {
		t.Fatalf("single-backend mapping: common fields not threaded: %+v", cc)
	}
}

// TestClusterConfig_MultiBackendMapping pins the mapping for MULTI-BACKEND
// mode: the BackendFactory is carried, UnitCount comes from Std, R is threaded,
// and Backend stays nil (validateBackendMode requires that).
func TestClusterConfig_MultiBackendMapping(t *testing.T) {
	h := sharedfactory.NewBacking().Handle()
	cc := clusterConfig(RunConfig{
		Std: StdConfig{
			NodeID:            "n1",
			ReplicationFactor: 2,
			UnitCount:         storageunit.MustUnitCount(4),
		},
		BackendFactory: h,
	}, "5.6.7.8:1234")

	if cc.Backend != nil {
		t.Fatal("multi-backend mapping: Backend must be nil")
	}
	if cc.BackendFactory == nil {
		t.Fatal("multi-backend mapping: BackendFactory must be carried through, got nil")
	}
	if cc.UnitCount.IsZero() || cc.UnitCount.N() != 4 {
		t.Fatalf("multi-backend mapping: UnitCount want N=4 from Std, got %+v", cc.UnitCount)
	}
	if cc.ReplicationFactor != 2 {
		t.Fatalf("multi-backend mapping: ReplicationFactor want 2, got %d", cc.ReplicationFactor)
	}
}

// TestBuildCluster_RejectsBadBackendXOR proves buildCluster fails fast on the
// XOR violations BEFORE touching cluster.Open (both set / neither set).
func TestBuildCluster_RejectsBadBackendXOR(t *testing.T) {
	_, err := buildCluster(RunConfig{
		Std:            StdConfig{NodeID: "n1"},
		Backend:        memory.New(),
		BackendFactory: sharedfactory.NewBacking().Handle(),
	}, "127.0.0.1:0")
	if err == nil || !contains(err.Error(), "not both") {
		t.Fatalf("both-set: want XOR error, got %v", err)
	}

	_, err = buildCluster(RunConfig{Std: StdConfig{NodeID: "n1"}}, "127.0.0.1:0")
	if err == nil || !contains(err.Error(), "required") {
		t.Fatalf("neither-set: want XOR error, got %v", err)
	}
}

// TestBuildCluster_MultiBackendR2_WriteReadBack is THE KEY TEST for the
// deployable multi-backend path. It drives the EXACT cluster-construction Run
// uses (buildCluster) in MULTI-BACKEND mode at ReplicationFactor=2 with an
// IN-PROCESS sharedfactory.Handle (the same test double the R=2 loss-oracle
// uses, which implements storageunit.ReplicaBackendFactory) so it needs NO
// -tags slatedb and NO MinIO.
//
// It asserts three things:
//
//  1. THE DEPLOYABLE WIRING WORKS: the cluster built from a RunConfig with
//     BackendFactory + UnitCount + ReplicationFactor=2 ACCEPTS a Put and READS
//     the same bytes back. A single-node cluster owns every replica position,
//     so R=2 here means the write fans to the node's own R replica copies; a
//     mis-threaded UnitCount or a dropped ReplicationFactor would make
//     cluster.Open fail validateBackendMode (R>1 multi-backend requires a
//     ReplicaBackendFactory + a non-zero UnitCount) - so a clean Open + a
//     correct read-back is itself proof the multi-backend R=2 path was taken,
//     not the legacy single-backend path.
//
//  2. THE MULTI-BACKEND PATH IS PHYSICALLY TAKEN: the written value lands in a
//     PER-UNIT REPLICA STORE on the shared Backing (not in any single Backend).
//     We locate the key's unit deterministically and assert at least one
//     replica copy of that unit physically carries the payload. A legacy
//     single-backend cluster has no replica stores at all, so this can only
//     pass if the factory-driven per-unit routing actually ran.
//
//  3. R=2 REPLICATION MODE ENGAGED, NOT THE LEGACY PATH: cluster.Open only
//     accepts ReplicationFactor=2 in multi-backend mode when the factory is a
//     ReplicaBackendFactory and UnitCount is non-zero (validateBackendMode
//     rejects R>1 multi-backend otherwise). A single-node cluster clamps the
//     replica set to the one available node, so the write lands on replica
//     POSITION 0 of the key's unit (a second distinct node would hold position
//     1; multi-node R=2 fan-out across distinct nodes is the loss-oracle's job
//     in tests/integration). We assert position 0 physically carries the
//     payload - which a legacy single-backend cluster (no per-replica stores at
//     all) could never produce.
func TestBuildCluster_MultiBackendR2_WriteReadBack(t *testing.T) {
	const unitCount, r = 8, 2
	backing := sharedfactory.NewBacking()

	c, err := buildCluster(RunConfig{
		Std: StdConfig{
			NodeID: "key-test-node",
			// BindAddr empty => single-node (no memberlist); the founder owns
			// every replica position, so a single node satisfies R=2 against its
			// own per-replica stores. No port is dialed.
			ReplicationFactor: r,
			UnitCount:         storageunit.MustUnitCount(unitCount),
		},
		BackendLabel:   "sharedfactory",
		BackendFactory: backing.Handle(),
		Logger:         log.New(io.Discard, "", 0),
		// Consistency is not a Std flag, so the helper builds the cluster with
		// the default WriteQuorum / ReadNearest. On a single node the replica
		// set clamps to one node, so the write satisfies quorum against replica
		// position 0; the read-back below proves the routed read finds it.
	}, "127.0.0.1:1")
	if err != nil {
		t.Fatalf("buildCluster multi-backend R=2: %v (a dropped R or UnitCount would fail validateBackendMode here)", err)
	}
	defer func() { _ = c.Close() }()

	const key, val = "deploy-key", "deploy-value-payload"

	// (1) The deployable wiring accepts a write and reads it back.
	if err := c.Put([]byte(key), []byte(val)); err != nil {
		t.Fatalf("Put on multi-backend R=2 cluster: %v", err)
	}
	got, err := c.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get on multi-backend R=2 cluster: %v", err)
	}
	if !bytes.Equal(got, []byte(val)) {
		t.Fatalf("read-back: want %q, got %q", val, got)
	}

	// (2)+(3) The value physically landed on a PER-UNIT REPLICA STORE of the
	// key's unit - proof the factory-driven multi-backend replica routing ran
	// (a legacy single-backend cluster has no per-replica stores). On a single
	// node the replica set clamps to position 0; we assert that copy carries
	// the LWW-envelope payload.
	u := storageunit.UnitForShardKey(shardKeyOf(key), storageunit.MustUnitCount(unitCount))
	gu := storageunit.NewGenUnit(0, u)
	ru := storageunit.NewReplicaUnit(gu, 0)
	be, ok := backing.ReplicaStore(ru)
	if !ok {
		t.Fatalf("unit %d replica 0: no replica store (multi-backend replica path did not run)", u)
	}
	stored, gerr := be.Get([]byte(key))
	if gerr != nil {
		t.Fatalf("unit %d replica 0: Get on replica store: %v", u, gerr)
	}
	// At R>1 the stored bytes are an LWW envelope carrying the payload.
	if !bytes.Contains(stored, []byte(val)) {
		t.Fatalf("unit %d replica 0: stored envelope %q does not carry payload %q", u, stored, val)
	}
}

// shardKeyOf mirrors the cluster's default shard-key extraction (ring.ShardKey,
// honoring {tag} hash tags) so the test names the same unit the cluster routed
// the key to. Kept local to avoid widening the ring import surface of the
// package's other tests.
func shardKeyOf(key string) []byte {
	return ring.ShardKey([]byte(key))
}

// contains is a tiny strings.Contains shim kept test-local so this file does
// not pull strings into a build that already has it via runtime_test.go's
// helpers; using a shared helper keeps the assertion messages uniform.
func contains(haystack, needle string) bool {
	return bytes.Contains([]byte(haystack), []byte(needle))
}
