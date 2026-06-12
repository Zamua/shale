package cluster_test

// Regression tests for the v0.4.1 fix bundle (public-API surface).
// White-box variants (hung-peer goroutine-leak, read-repair lifecycle
// tied to Close) live in replicate_fixes_internal_test.go.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestFanout_UnavailableShortCircuits_NotTransient pins issue 2:
// codes.Unavailable from a dead peer is NOT classified as transient.
// With R=3 + WriteQuorum (W=2) + TWO peer gRPCs stopped, the Put
// MUST fail with codes.Unavailable in bounded wall-clock -- NOT wait
// for the WriteTimeout to fire on the never-arriving third reply.
//
// The "isTransientReplicaErr too broad" regression would mark the
// Unavailables as transient and the call would block on the failure-
// budget logic forever (until WriteTimeout) instead of short-
// circuiting once 2 failures land (one above the budget).
func TestFanout_UnavailableShortCircuits_NotTransient(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteQuorum, cluster.ReadNearest)
	nodes[1].stopGRPC()
	nodes[2].stopGRPC()

	start := time.Now()
	err := nodes[0].cluster.Put([]byte("sc-key"), []byte("v"))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("Put should fail with 2 of 3 peers down + WriteQuorum")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("Put failure: want codes.Unavailable, got %v", err)
	}
	// 2s budget: short-circuit returns sub-second once Unavailables
	// land. Without short-circuit the call would wait for the per-
	// dispatch WriteTimeout default (5s) before giving up.
	if elapsed > 2*time.Second {
		t.Errorf("Put took %v with 2 peers killed; failure budget should short-circuit (< 2s)", elapsed)
	}
}

// TestGet_ReadNearest_FailsOverWhenPrimaryGRPCDown pins issue 3:
// ReadNearest dispatches to all R replicas + returns on first success
// so a down primary falls back to a successor. R=2, both replicas
// hold the value, kill the primary's gRPC, the successor's local
// fast-path serves the value.
func TestGet_ReadNearest_FailsOverWhenPrimaryGRPCDown(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 2, cluster.WriteAll, cluster.ReadNearest)

	const key = "rn-fallback-key"
	if err := nodes[0].cluster.Put([]byte(key), []byte("v")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	primary, successor := primaryAndSuccessor(t, nodes, key, 2)
	primary.stopGRPC()

	got, err := successor.cluster.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get via successor with primary's gRPC down: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Errorf("Get returned %q, want v", got)
	}
}

// TestGet_ReadQuorum_TolerablePrimaryDown pins issue 3 for Quorum:
// R=3 / N=2 tolerates (R - N) = 1 replica failure. We kill the
// primary's gRPC + ReadQuorum from a survivor still returns the
// value (two surviving replicas satisfy N=2).
func TestGet_ReadQuorum_TolerablePrimaryDown(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	const key = "rq-fallback-key"
	if err := nodes[0].cluster.Put([]byte(key), []byte("v")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	primary, _ := primaryAndSuccessor(t, nodes, key, 3)
	primary.stopGRPC()

	var reader *replicatedNode
	for _, n := range nodes {
		if n != primary {
			reader = n
			break
		}
	}
	got, err := reader.cluster.Get([]byte(key))
	if err != nil {
		t.Fatalf("ReadQuorum Get with primary down: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Errorf("ReadQuorum got %q want v", got)
	}
}

// TestWriteQuorum_LiveClampedR5_On2Nodes pins issue 5: a Cluster
// configured with R=5 but running on 2 live members computes Quorum
// against R_live (2), not the configured R (5). LocateKeyN returns
// the available subset; W = floor(R_live/2)+1 = 2 ack majority of
// the 2 live nodes. The Put succeeds. With a configured-R semantic
// (W = floor(5/2)+1 = 3) the call would never satisfy the quorum
// because only 2 nodes can possibly ack.
//
// This is the spec's documented "live-clamped quorum" behavior
// (docs/SPEC.md "Configuration knobs"). The test comment names the
// alternative semantic so a reviewer doesn't change the math
// inadvertently.
func TestWriteQuorum_LiveClampedR5_On2Nodes(t *testing.T) {
	mems := []*memory.Memory{memory.New(), memory.New()}
	bindPorts := []int{freePort(t), freePort(t)}
	harnesses := make([]*grpcHarness, 2)
	stops := make([]func(), 2)
	for i := range 2 {
		harnesses[i], stops[i] = startGRPC(t)
	}
	defer func() {
		for _, s := range stops {
			s()
		}
	}()
	clusters := make([]*cluster.Cluster, 2)
	for i := range 2 {
		cfg := cluster.Config{
			NodeID:               fmt.Sprintf("lc-n%d", i+1),
			Backend:              mems[i],
			BindAddr:             hostPort(bindPorts[i]),
			GRPCAddr:             harnesses[i].addr,
			LogOutput:            io.Discard,
			ReplicationFactor:    5, // configured > live
			WriteConsistency:     cluster.WriteQuorum,
			ReadConsistency:      cluster.ReadNearest,
			RebalanceSettleDelay: 500 * time.Millisecond,
		}
		if i > 0 {
			cfg.Seeds = []string{hostPort(bindPorts[0])}
		}
		c, err := cluster.Open(cfg)
		if err != nil {
			t.Fatalf("open lc-n%d: %v", i+1, err)
		}
		t.Cleanup(func() { _ = c.Close() })
		clusters[i] = c
		harnesses[i].register(c)
	}

	for i, c := range clusters {
		if err := waitForRingSize(c, 2, 5*time.Second); err != nil {
			t.Fatalf("ring on lc-n%d: %v", i+1, err)
		}
	}
	for _, c := range clusters {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_ = c.WaitForRebalanceIdle(ctx)
		cancel()
	}

	if err := clusters[0].Put([]byte("lc-k"), []byte("v")); err != nil {
		t.Fatalf("Put with R=5-configured / R_live=2 / Quorum: should live-clamp to W=2 and succeed; got %v", err)
	}
}

// -- helpers ---------------------------------------------------------

// primaryAndSuccessor builds a real ring from the cluster's Members
// view (consistent-hash deterministic so this matches what
// LocateKeyN inside the cluster would return) and returns the
// primary + first successor for the given key. Asserts both are
// present in nodes (so the caller can stopGRPC on the primary).
func primaryAndSuccessor(t *testing.T, nodes []*replicatedNode, key string, r int) (*replicatedNode, *replicatedNode) {
	t.Helper()
	members := nodes[0].cluster.Members()
	if len(members) < r {
		t.Fatalf("ring has %d members, need at least %d", len(members), r)
	}
	rg := ring.New()
	for _, m := range members {
		rg.Add(m)
	}
	owners := rg.LocateKeyN([]byte(key), r)
	if len(owners) < 2 {
		t.Fatalf("LocateKeyN returned %d owners for %q (need >= 2)", len(owners), key)
	}
	byID := map[string]*replicatedNode{}
	for _, n := range nodes {
		byID[n.cluster.NodeID()] = n
	}
	pri, ok := byID[owners[0].ID]
	if !ok {
		t.Fatalf("primary %q not in nodes", owners[0].ID)
	}
	succ, ok := byID[owners[1].ID]
	if !ok {
		t.Fatalf("successor %q not in nodes", owners[1].ID)
	}
	return pri, succ
}
