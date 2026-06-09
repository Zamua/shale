package integration

// v0.4 LWW conflict resolution -- explicit per-stamp test.
//
// Per docs/SPEC.md "LWW comparator":
//
//	1. higher TimestampNanos wins, OR
//	2. timestamps equal AND higher (lex) NodeID wins.
//
// The cleanest way to pin the math without faking the system clock is
// to plant the two competing envelopes directly into two replicas'
// backends (one with t=100, one with t=50) and then issue a cluster
// Get on ReadAll. The LWW comparator runs in getReplicated and MUST
// pick the t=100 envelope. Direct-backend planting bypasses Put's
// re-stamping (which would always use time.Now()), so we exercise the
// comparator in isolation. The partition-simulation alternative (one
// side writes t=100, partition heals, other side writes t=50) is far
// harder to stage in-process; the task brief says it's fine to skip
// it.

import (
	"bytes"
	"testing"

	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
)

// TestReplicate_LWW_HigherTimestampWins seeds two replicas of the same
// key with hand-crafted envelopes (one early, one late). A ReadAll
// fans out to both + the LWW comparator must pick the late-timestamp
// payload regardless of which physical node it landed on.
func TestReplicate_LWW_HigherTimestampWins(t *testing.T) {
	// Two-node R=2 cluster so every key has the same primary +
	// successor pair (both nodes), making the direct-backend plant
	// straightforward.
	nodes := startReplicatedCluster(t, 2, 2, cluster.WriteAll, cluster.ReadAll)

	key := []byte("lww-key")

	// Identify the primary + successor for this key so the assertion
	// is precise about WHICH backend holds the early vs late envelope.
	r := ring.New()
	for _, m := range nodes[0].Cluster.Members() {
		r.Add(m)
	}
	replicas := r.LocateKeyN(key, 2)
	if len(replicas) != 2 {
		t.Fatalf("LocateKeyN(2) returned %d replicas", len(replicas))
	}

	byID := map[string]*testNode{}
	for _, n := range nodes {
		byID[n.ID] = n
	}

	earlyNode := byID[replicas[0].ID]
	lateNode := byID[replicas[1].ID]
	if earlyNode == nil || lateNode == nil {
		t.Fatalf("could not resolve replica nodes: %v", replicas)
	}

	earlyEnv := cluster.Encode(cluster.Envelope{
		Stamp: cluster.Stamp{
			TimestampNanos: 50,
			NodeID:   "writerA",
		},
		Payload: []byte("older"),
	})
	lateEnv := cluster.Encode(cluster.Envelope{
		Stamp: cluster.Stamp{
			TimestampNanos: 100,
			NodeID:   "writerB",
		},
		Payload: []byte("newer"),
	})

	if err := earlyNode.Backend.Put(key, earlyEnv); err != nil {
		t.Fatalf("plant earlyEnv on %s: %v", earlyNode.ID, err)
	}
	if err := lateNode.Backend.Put(key, lateEnv); err != nil {
		t.Fatalf("plant lateEnv on %s: %v", lateNode.ID, err)
	}

	// ReadAll fans out to both replicas; the LWW comparator picks t=100.
	for i, n := range nodes {
		got, err := n.Cluster.Get(key)
		if err != nil {
			t.Fatalf("Get via node[%d] %s: %v", i, n.ID, err)
		}
		if !bytes.Equal(got, []byte("newer")) {
			t.Errorf("Get via node[%d] %s: got %q want newer (LWW with t=100 must win over t=50)",
				i, n.ID, got)
		}
	}
}

// TestReplicate_LWW_NodeIDTiebreak pins the tiebreak rule: when two
// envelopes carry IDENTICAL timestamps, the lexicographically greater
// NodeID wins. Same plant-the-envelopes pattern as above.
func TestReplicate_LWW_NodeIDTiebreak(t *testing.T) {
	nodes := startReplicatedCluster(t, 2, 2, cluster.WriteAll, cluster.ReadAll)

	key := []byte("lww-tie")
	r := ring.New()
	for _, m := range nodes[0].Cluster.Members() {
		r.Add(m)
	}
	replicas := r.LocateKeyN(key, 2)
	byID := map[string]*testNode{}
	for _, n := range nodes {
		byID[n.ID] = n
	}

	// Plant two envelopes with identical TimestampNanos but different
	// NodeIDs. Lex tiebreak: "z" > "a", so "z" wins.
	aEnv := cluster.Encode(cluster.Envelope{
		Stamp:   cluster.Stamp{TimestampNanos: 42, NodeID: "a"},
		Payload: []byte("from-a"),
	})
	zEnv := cluster.Encode(cluster.Envelope{
		Stamp:   cluster.Stamp{TimestampNanos: 42, NodeID: "z"},
		Payload: []byte("from-z"),
	})

	if err := byID[replicas[0].ID].Backend.Put(key, aEnv); err != nil {
		t.Fatalf("plant aEnv: %v", err)
	}
	if err := byID[replicas[1].ID].Backend.Put(key, zEnv); err != nil {
		t.Fatalf("plant zEnv: %v", err)
	}

	got, err := nodes[0].Cluster.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("from-z")) {
		t.Errorf("LWW tiebreak: got %q want from-z (lex tiebreak: z > a)", got)
	}
}
