package integration

// Basic v0.4 replication integration test.
//
// Three nodes, R=2, default consistency (WriteQuorum + ReadNearest).
// Writes 100 keys via N1; verifies each key physically lands on the
// two replicas LocateKeyN picks (primary + first successor) by
// peering directly at each node's in-memory backend. Reads each key
// back via N3 (which only owns roughly 1/3 of the primaries) to
// exercise the cross-node replica-fetch path.

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
)

// TestReplicate_R2_KeyLandsOnBothReplicas pins the v0.4 fan-out
// contract: with R=2 every Put places the LWW envelope on TWO nodes
// (primary + immediate successor on the ring). We compute the
// expected pair from a test-side ring built off the public Members()
// snapshot, then peer directly at each backend.
func TestReplicate_R2_KeyLandsOnBothReplicas(t *testing.T) {
	nodes := startReplicatedCluster(t, 3, 2, cluster.WriteQuorum, cluster.ReadNearest)

	const n = 100
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("rep2-%04d", i))
		val := []byte(fmt.Sprintf("v-%04d", i))
		if err := nodes[0].Cluster.Put(key, val); err != nil {
			t.Fatalf("Put %s via N1: %v", key, err)
		}
	}

	// Build the expected primary + successor pair from the same Members
	// snapshot every node sees. ring.LocateKeyN is deterministic so the
	// test-side ring agrees with the cluster's internal ring.
	r := ring.New()
	for _, m := range nodes[0].Cluster.Members() {
		r.Add(m)
	}

	// Map node ID -> testNode for direct backend peering.
	byID := make(map[string]*testNode, len(nodes))
	for _, nd := range nodes {
		byID[nd.ID] = nd
	}

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("rep2-%04d", i))
		want := []byte(fmt.Sprintf("v-%04d", i))
		replicas := r.LocateKeyN(key, 2)
		if len(replicas) != 2 {
			t.Fatalf("LocateKeyN(%s, 2) returned %d members", key, len(replicas))
		}
		// Every replica's backend must hold the envelope for this key.
		for _, rep := range replicas {
			nd, ok := byID[rep.ID]
			if !ok {
				t.Fatalf("unknown replica node %s", rep.ID)
			}
			raw, err := nd.Backend.Get(key)
			if err != nil {
				t.Fatalf("replica %s missing key %s: %v", rep.ID, key, err)
			}
			env, err := cluster.Decode(raw)
			if err != nil {
				t.Fatalf("replica %s decode %s: %v", rep.ID, key, err)
			}
			if !bytes.Equal(env.Payload, want) {
				t.Errorf("replica %s key %s: payload=%q want %q", rep.ID, key, env.Payload, want)
			}
			if env.Stamp.TimestampNanos == 0 {
				t.Errorf("replica %s key %s: zero stamp", rep.ID, key)
			}
		}
		// The third (non-replica) node must NOT hold the key.
		for _, nd := range nodes {
			if nd.ID == replicas[0].ID || nd.ID == replicas[1].ID {
				continue
			}
			if _, err := nd.Backend.Get(key); err == nil {
				t.Errorf("non-replica %s unexpectedly holds key %s", nd.ID, key)
			}
		}
	}

	// All 100 keys round-trip through N3 regardless of which two
	// replicas own them. ReadNearest hops to the primary; even when N3
	// is the primary itself the local-replica fast-path serves the
	// envelope.
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("rep2-%04d", i))
		want := []byte(fmt.Sprintf("v-%04d", i))
		got, err := nodes[2].Cluster.Get(key)
		if err != nil {
			t.Fatalf("N3.Get %s: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("N3.Get %s: got %q want %q", key, got, want)
		}
	}
}
