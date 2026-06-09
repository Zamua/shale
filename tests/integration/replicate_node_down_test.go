package integration

// v0.4 replication: tolerating a down replica.
//
// Scenario: 3-node cluster at R=2, default WriteQuorum (=1 ack on R=2)
// + ReadQuorum. Put 100 keys; close N2; verify every key is still
// Gettable through one of the surviving cluster handles. A fresh Put
// with N2 down must still succeed (quorum on R=2 only needs 1 ack).
//
// ReadQuorum (not ReadNearest) on the post-leave read path is required
// to surface this test's contract: v0.4 ships without
// rebalance-with-replication, so when N2 leaves the 3-node ring the
// new 2-node ring's R=2 lookup re-routes some keys to (N1, N3) when
// the original placement was (N2, N3). ReadNearest hops only to the
// new primary (N1), which never held the original write -- it returns
// NotFound even though the surviving replica (N3) still has the data.
// ReadQuorum queries both replicas and the LWW winner picks the
// surviving copy. This is the documented v0.4 limitation per
// docs/SPEC.md "Out of scope for v0.4" + "Rebalance-with-replication".
// We can't readily probe "ReadAll triggers read-repair when N2
// returns" inside this test because closing a node in the integration
// harness shuts down its cluster handle for good; that asymmetry is
// covered by the in-package ReadRepair test using a poisoned backend.

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
)

// TestReplicate_R2_TolerateOneNodeDown pins the
// (R - W) failure budget: with R=2 + WriteQuorum (W=1) one node down
// MUST NOT block reads or writes that have a surviving replica.
func TestReplicate_R2_TolerateOneNodeDown(t *testing.T) {
	nodes := startReplicatedCluster(t, 3, 2, cluster.WriteQuorum, cluster.ReadQuorum)

	const n = 100
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("nd-%04d", i))
		val := []byte(fmt.Sprintf("v-%04d", i))
		if err := nodes[0].Cluster.Put(key, val); err != nil {
			t.Fatalf("seed Put %s: %v", key, err)
		}
	}

	// Close N2 cleanly. Graceful Leave broadcasts so survivors see the
	// 2-member ring; subsequent fan-outs only dispatch to the surviving
	// replicas.
	nodes[1].Close()

	// Wait for survivors to observe the 2-member ring.
	survivors := []*cluster.Cluster{nodes[0].Cluster, nodes[2].Cluster}
	if err := waitForMembersAll(survivors, 2, 10*time.Second); err != nil {
		t.Fatalf("post-leave convergence: %v", err)
	}

	// Every seed key remains readable through N1 or N3.
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("nd-%04d", i))
		want := []byte(fmt.Sprintf("v-%04d", i))
		// Read from N1 first; if that fails, fall back to N3. With R=2
		// and one survivor still holding each key (since every key was
		// replicated on TWO of the three nodes, and only one is gone),
		// at least one of these two reads must succeed.
		got, err1 := nodes[0].Cluster.Get(key)
		if err1 == nil {
			if !bytes.Equal(got, want) {
				t.Errorf("N1.Get %s: got %q want %q", key, got, want)
			}
			continue
		}
		got, err3 := nodes[2].Cluster.Get(key)
		if err3 != nil {
			t.Errorf("key %s unreadable post-N2-down: N1=%v N3=%v", key, err1, err3)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("N3.Get %s: got %q want %q", key, got, want)
		}
	}

	// Fresh writes with N2 down must still succeed (WriteQuorum on R=2
	// needs 1 ack; we have 2 reachable replicas, of which any single
	// one suffices).
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("post-down-%04d", i))
		val := []byte(fmt.Sprintf("pd-%04d", i))
		if err := nodes[0].Cluster.Put(key, val); err != nil {
			t.Fatalf("post-down Put %s: %v", key, err)
		}
		got, err := nodes[2].Cluster.Get(key)
		if errors.Is(err, backend.ErrNotFound) {
			// Could happen if the only replica is N1 (local) and N3 has
			// to forward to N1; N1 should serve. Retry via N1 to confirm.
			got, err = nodes[0].Cluster.Get(key)
		}
		if err != nil {
			t.Fatalf("post-down Get %s: %v", key, err)
		}
		if !bytes.Equal(got, val) {
			t.Errorf("post-down Get %s: got %q want %q", key, got, val)
		}
	}
}
