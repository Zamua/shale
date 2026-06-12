package cluster_test

// v0.4 replication tests: R>1 fan-out for writes, WriteConsistency.
// Read-side tests (LWW, ReadConsistency, read-repair) live in
// replicate_read_test.go.
//
// These exercise the public Cluster surface end-to-end through
// in-process 3-node clusters with memory backends + real gRPC
// wiring (same harness shape as TestTwoNode_PutRoutesToOwner). The
// pure unit-level coverage of the LWW comparator + envelope encode/
// decode lives in envelope_test.go; the fanout primitive's accounting
// rules live in fanout_internal_test.go. Here we pin the cluster-layer
// integration: Put fans out, Delete writes a tombstone, the consistency
// knob actually gates the per-call decision.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestReplication_R3_PutFansOutToAllReplicas pins the v0.4 baseline:
// a Put with ReplicationFactor=3 + WriteAll lands envelope bytes on
// every node's backend. Verified by reading each backend directly
// and decoding the envelope.
func TestReplication_R3_PutFansOutToAllReplicas(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	if err := nodes[0].cluster.Put([]byte("rep-key"), []byte("rep-val")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	for i, n := range nodes {
		raw, err := n.mem.Get([]byte("rep-key"))
		if err != nil {
			t.Fatalf("node %d backend missing key: %v", i, err)
		}
		env, err := cluster.Decode(raw)
		if err != nil {
			t.Fatalf("node %d Decode: %v", i, err)
		}
		if !bytes.Equal(env.Payload, []byte("rep-val")) {
			t.Errorf("node %d payload: got %q want rep-val", i, env.Payload)
		}
		if env.Stamp.TimestampNanos == 0 {
			t.Errorf("node %d stamp not set: %+v", i, env.Stamp)
		}
	}
}

// TestReplication_R3_WriteQuorum_ToleratesOneDown pins the quorum
// write contract: with R=3 + WriteQuorum (W=2), a Put still succeeds
// after one replica's gRPC has been stopped (the local + one remote
// ack covers the quorum).
func TestReplication_R3_WriteQuorum_ToleratesOneDown(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteQuorum, cluster.ReadNearest)

	// Stop node[2]'s gRPC so any fan-out to it fails. Local +
	// node[1] must still satisfy quorum.
	nodes[2].stopGRPC()

	if err := nodes[0].cluster.Put([]byte("q-key"), []byte("q-val")); err != nil {
		t.Fatalf("Put with one node down should succeed at WriteQuorum: %v", err)
	}

	// node[0] + node[1] should have the value (we don't assert which
	// specific two; with R=3 + 3 members, all three are replicas).
	gotCount := 0
	for i := range 2 {
		raw, err := nodes[i].mem.Get([]byte("q-key"))
		if err != nil {
			continue
		}
		env, _ := cluster.Decode(raw)
		if bytes.Equal(env.Payload, []byte("q-val")) {
			gotCount++
		}
	}
	if gotCount < 2 {
		t.Errorf("expected at least 2 replicas to hold the value, got %d", gotCount)
	}
}

// TestReplication_R3_WriteAll_FailsWhenOneDown pins that WriteAll is
// strict: any down replica fails the write.
func TestReplication_R3_WriteAll_FailsWhenOneDown(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	nodes[2].stopGRPC()

	err := nodes[0].cluster.Put([]byte("all-key"), []byte("all-val"))
	if err == nil {
		t.Fatalf("Put with WriteAll + one node down should fail")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("want Unavailable, got %v", err)
	}
}

// TestReplication_R3_DeleteTombstone pins that Delete writes a
// tombstone envelope to every replica + subsequent direct backend
// reads show the empty-payload-with-stamp shape. Surfacing the
// tombstone via Get-returns-NotFound is covered in replicate_read_test.go.
func TestReplication_R3_DeleteTombstone(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	if err := nodes[0].cluster.Put([]byte("d"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := nodes[0].cluster.Delete([]byte("d")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Every node's backend must hold a tombstone envelope (empty
	// payload, real stamp).
	for i, n := range nodes {
		raw, err := n.mem.Get([]byte("d"))
		if err != nil {
			t.Fatalf("node %d: expected tombstone present, got NotFound", i)
		}
		env, err := cluster.Decode(raw)
		if err != nil {
			t.Fatalf("node %d Decode: %v", i, err)
		}
		if len(env.Payload) != 0 {
			t.Errorf("node %d tombstone should have empty payload, got %q", i, env.Payload)
		}
		if env.Stamp.TimestampNanos == 0 {
			t.Errorf("node %d tombstone should have real stamp, got %+v", i, env.Stamp)
		}
	}
}

// -- harness ----------------------------------------------------------

// replicatedNode is one of the three nodes in the test cluster.
type replicatedNode struct {
	cluster   *cluster.Cluster
	mem       *memory.Memory
	stopGRPC  func()
	stopOther func()
}

// startThreeNodeReplicatedCluster brings up 3 in-process nodes wired
// over gRPC + memberlist, then waits for membership convergence +
// rebalance idle. Returns the nodes; t.Cleanup tears them down.
func startThreeNodeReplicatedCluster(t *testing.T, replicationFactor int, wc cluster.WriteConsistency, rc cluster.ReadConsistency) []*replicatedNode {
	t.Helper()

	mems := []*memory.Memory{memory.New(), memory.New(), memory.New()}
	bindPorts := []int{freePort(t), freePort(t), freePort(t)}
	grpcHarnesses := make([]*grpcHarness, 3)
	grpcStops := make([]func(), 3)
	for i := range 3 {
		grpcHarnesses[i], grpcStops[i] = startGRPC(t)
	}

	clusters := make([]*cluster.Cluster, 3)
	for i := range 3 {
		cfg := cluster.Config{
			NodeID:               fmt.Sprintf("rep-n%d", i+1),
			Backend:              mems[i],
			BindAddr:             hostPort(bindPorts[i]),
			GRPCAddr:             grpcHarnesses[i].addr,
			LogOutput:            io.Discard,
			ReplicationFactor:    replicationFactor,
			WriteConsistency:     wc,
			ReadConsistency:      rc,
			RebalanceSettleDelay: 500 * time.Millisecond,
		}
		if i > 0 {
			cfg.Seeds = []string{hostPort(bindPorts[0])}
		}
		c, err := cluster.Open(cfg)
		if err != nil {
			for _, stop := range grpcStops {
				stop()
			}
			t.Fatalf("open node %d: %v", i, err)
		}
		clusters[i] = c
		grpcHarnesses[i].register(c)
	}

	// Wait for all rings to converge.
	for i, c := range clusters {
		if err := waitForRingSize(c, 3, 5*time.Second); err != nil {
			t.Fatalf("ring on node %d: %v", i, err)
		}
	}
	// Settle rebalance (bootstrap pulls from prior owners; we don't
	// care about the data movement since each backend is empty at
	// startup, but the StateReceiving guard would reject our test
	// Puts until it clears).
	for i, c := range clusters {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := c.WaitForRebalanceIdle(ctx); err != nil {
			cancel()
			t.Fatalf("node %d rebalance idle: %v", i, err)
		}
		cancel()
	}

	// Final gate: prove the routed forwarding path actually round-trips
	// at the configured WriteConsistency before the test issues its seed
	// Put. Ring convergence + rebalance-idle prove membership + ownership
	// stability, but NOT that every peer's gRPC server goroutine is
	// servicing connections yet - the gap that produces the CI
	// "write needed N acks, got M" flake. The retry here is scoped to
	// setup only; once this returns the cluster is write-ready and any
	// later Unavailable in a test body is a real failure.
	waitForWriteReady(t, clusters[0], 15*time.Second)

	out := make([]*replicatedNode, 3)
	for i := range 3 {
		out[i] = &replicatedNode{
			cluster: clusters[i],
			mem:     mems[i],
			stopGRPC: func() {
				grpcHarnesses[i].server.Stop()
				<-grpcHarnesses[i].done
				// Brief settle so subsequent dials see a definite refused.
				time.Sleep(50 * time.Millisecond)
			},
			stopOther: grpcStops[i],
		}
	}

	t.Cleanup(func() {
		for _, n := range out {
			_ = n.cluster.Close()
		}
		for _, stop := range grpcStops {
			stop()
		}
	})

	return out
}
