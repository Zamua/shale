package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/rpc"
)

// TestThreeNode_AggregateFansOutPerNode writes a known set of keys
// across a 3-node cluster (routed naturally by the ring), then runs
// an Aggregate that counts keys per peer. Each AggregateResult.Value
// must equal that peer's actual per-backend key count - the sum
// across peers equals the total written, and no peer is counted
// twice. This pins the regression where Aggregate snapshotted peers
// via ScanPrefix(nil), which routed the empty prefix through ownerOf
// and made every peer hand back the SAME single-shard slice.
func TestThreeNode_AggregateFansOutPerNode(t *testing.T) {
	f := newThreeNodeFixture(t)

	const n = 300
	written := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("agg-%04d", i)
		written[k] = true
		if err := f.N1.Cluster.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	// Per-peer ground truth: what does each node's local backend
	// actually hold?
	groundTruth := make(map[string]int, 3)
	for _, node := range f.Nodes() {
		it, err := node.Backend.ScanPrefix(nil)
		if err != nil {
			t.Fatalf("backend scan on %s: %v", node.ID, err)
		}
		count := 0
		for {
			k, _, err := it.Next()
			if err != nil {
				t.Fatalf("backend scan next on %s: %v", node.ID, err)
			}
			if k == nil {
				break
			}
			count++
		}
		_ = it.Close()
		groundTruth[node.ID] = count
	}

	// Distribution sanity: keys should be spread across 2+ nodes,
	// otherwise the test below could pass even if Aggregate were
	// broken. If this fails the ring is degenerate, not Aggregate.
	if len(groundTruth) < 2 {
		t.Fatalf("expected keys spread across at least 2 nodes, got distribution=%v", groundTruth)
	}

	// Aggregate must report a per-peer count that matches ground
	// truth - one entry per peer, none duplicated.
	results := f.N1.Cluster.Aggregate(func(b backend.Backend) any {
		it, err := b.ScanPrefix(nil)
		if err != nil {
			return err
		}
		defer it.Close()
		count := 0
		for {
			k, _, err := it.Next()
			if err != nil {
				return err
			}
			if k == nil {
				break
			}
			count++
		}
		return count
	})

	if len(results) != 3 {
		t.Fatalf("Aggregate returned %d results, want 3 (one per node)", len(results))
	}

	sum := 0
	got := make([]int, len(results))
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("aggregate result[%d].Err: %v", i, r.Err)
		}
		c, ok := r.Value.(int)
		if !ok {
			t.Fatalf("aggregate result[%d].Value not int: %T %v", i, r.Value, r.Value)
		}
		got[i] = c
		sum += c
	}

	if sum != n {
		t.Fatalf("aggregate sum=%d want=%d (per-peer=%v, ground truth=%v)", sum, n, got, groundTruth)
	}

	// Pin the regression sharply: at least one peer must have a
	// DIFFERENT count from another. If snapshotPeer were still using
	// the routed ScanPrefix(nil), every peer would hand back the
	// same single owner-shard slice + every count would be equal.
	allEqual := true
	for i := 1; i < len(got); i++ {
		if got[i] != got[0] {
			allEqual = false
			break
		}
	}
	if allEqual && len(groundTruth) > 1 {
		t.Fatalf("aggregate per-peer counts all equal (%v) - snapshotPeer is routing through ownerOf instead of using LocalScan; ground truth shows real spread %v", got, groundTruth)
	}
}

// TestThreeNode_StatsKeysHeldIsPerNode verifies that the gRPC
// Stats.keys_held counter on each node reports that node's REAL
// per-backend key count, not the count routed-via-ownerOf (which
// would be either 0 or "total" for whichever node owns the empty
// prefix).
func TestThreeNode_StatsKeysHeldIsPerNode(t *testing.T) {
	f := newThreeNodeFixture(t)

	const n = 300
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("stats-%04d", i)
		if err := f.N1.Cluster.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	// Ground truth per node.
	truth := make(map[string]uint64, 3)
	for _, node := range f.Nodes() {
		it, err := node.Backend.ScanPrefix(nil)
		if err != nil {
			t.Fatalf("backend scan on %s: %v", node.ID, err)
		}
		var c uint64
		for {
			k, _, err := it.Next()
			if err != nil {
				t.Fatalf("backend next: %v", err)
			}
			if k == nil {
				break
			}
			c++
		}
		_ = it.Close()
		truth[node.ID] = c
	}
	if len(truth) < 2 {
		t.Fatalf("expected distribution across >=2 nodes, got %v", truth)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Each node's Stats reports its own keys_held.
	for _, node := range f.Nodes() {
		cli, err := rpc.NewClient(node.GRPCAddr)
		if err != nil {
			t.Fatalf("rpc.NewClient %s: %v", node.ID, err)
		}
		stats, err := cli.Stats(ctx)
		_ = cli.Close()
		if err != nil {
			t.Fatalf("Stats on %s: %v", node.ID, err)
		}
		if stats.GetKeysHeld() != truth[node.ID] {
			t.Fatalf("%s Stats.keys_held = %d, want %d (full truth=%v)",
				node.ID, stats.GetKeysHeld(), truth[node.ID], truth)
		}
	}
}
