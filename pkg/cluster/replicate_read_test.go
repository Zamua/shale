package cluster_test

// v0.4 replication read-side tests: ReadConsistency fan-out, LWW
// winner picking, async read-repair on Quorum/All, tombstone surfacing
// as NotFound on Get. The harness + write-side tests live in
// replicate_test.go; this file imports the same harness helpers.

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
)

// TestReplication_R3_GetReturnsWinner pins the read path: after a
// Put + a separate (later) Put with the same key, Get returns the
// newer payload regardless of which replica the read landed on.
func TestReplication_R3_GetReturnsWinner(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	if err := nodes[0].cluster.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	// Sleep 2ms so the wall-clock stamp advances strictly; the LWW
	// comparator is nanosecond-resolution + the test memory backend
	// is fast enough that two consecutive Puts could collide. The
	// NodeID tiebreak would still resolve them but the assertion
	// below wants "v2 wins because it was later," not "v2 wins
	// because nodeID."
	time.Sleep(2 * time.Millisecond)
	if err := nodes[1].cluster.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	for i, n := range nodes {
		got, err := n.cluster.Get([]byte("k"))
		if err != nil {
			t.Fatalf("Get from node %d: %v", i, err)
		}
		if !bytes.Equal(got, []byte("v2")) {
			t.Errorf("Get from node %d: got %q want v2", i, got)
		}
	}
}

// TestReplication_R3_LWW_OlderWriteLoses pins the LWW comparator
// on the read path. We do this by:
//  1. Put via node[0] (timestamps with node[0]'s clock)
//  2. Wait ~5ms
//  3. Put via node[2] (timestamps with node[2]'s clock, strictly later)
//  4. Get from every node returns the newer payload.
func TestReplication_R3_LWW_OlderWriteLoses(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadAll)

	if err := nodes[0].cluster.Put([]byte("lww"), []byte("older")); err != nil {
		t.Fatalf("Put older: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := nodes[2].cluster.Put([]byte("lww"), []byte("newer")); err != nil {
		t.Fatalf("Put newer: %v", err)
	}

	for i, n := range nodes {
		got, err := n.cluster.Get([]byte("lww"))
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if !bytes.Equal(got, []byte("newer")) {
			t.Errorf("node %d Get: got %q want newer", i, got)
		}
	}
}

// TestReplication_R3_LWW_PlantedEnvelopes_NodeIDTiebreak pins clause (2)
// of the LWW comparator on the READ path: when two replicas hold envelopes
// with IDENTICAL TimestampNanos, the lexicographically greater NodeID wins
// the fan-out compare.
//
// TestReplication_R3_LWW_OlderWriteLoses above covers clause (1) (higher
// timestamp wins) but cannot reach clause (2): real Puts draw their stamp
// from the monotone stamp clock, so two writes never collide on the same
// nanosecond by construction. The only way to stage an equal-stamp race
// without faking the clock is to PLANT hand-crafted envelopes directly on
// each replica's backend, bypassing Put's re-stamping, and then read
// through the cluster so getReplicated's comparator runs on them.
//
// Stamp.Greater's tiebreak arithmetic is unit-tested in envelope_test.go
// (TestStamp_Greater_NodeIDTiebreak); what this test adds is that the READ
// fan-out actually consults that clause. The planted NodeIDs are ordered so
// the winner sits on the LAST ring replica, which is neither the primary nor
// the node the read is issued from: a "first response wins" or "primary
// wins" regression in getReplicated returns "from-a" here and fails, where a
// winner planted on the primary would pass such a regression by accident.
func TestReplication_R3_LWW_PlantedEnvelopes_NodeIDTiebreak(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadAll)

	key := []byte("lww-tie")

	// Resolve the ring's replica ORDER for this key so the plant is precise
	// about which physical backend gets which NodeID. At R=3 on a 3-member
	// ring every node is a replica; only the order differs per key.
	r := ring.New()
	for _, m := range nodes[0].cluster.Members() {
		r.Add(m)
	}
	replicas := r.LocateKeyN(key, 3)
	if len(replicas) != 3 {
		t.Fatalf("LocateKeyN(3) returned %d replicas, want 3", len(replicas))
	}
	byID := map[string]*replicatedNode{}
	for _, n := range nodes {
		byID[n.cluster.NodeID()] = n
	}

	// Identical TimestampNanos on all three; distinct NodeIDs. Lex order is
	// "a" < "m" < "z", so the "z" envelope on the LAST replica must win.
	stampNodeIDs := []string{"a", "m", "z"}
	payloads := []string{"from-a", "from-m", "from-z"}
	for i, rep := range replicas {
		n := byID[rep.ID]
		if n == nil {
			t.Fatalf("replica %q not in harness", rep.ID)
		}
		env := cluster.Encode(cluster.Envelope{
			Stamp:   cluster.Stamp{TimestampNanos: 42, NodeID: stampNodeIDs[i]},
			Payload: []byte(payloads[i]),
		})
		if err := n.mem.Put(key, env); err != nil {
			t.Fatalf("plant %q on replica[%d] %s: %v", stampNodeIDs[i], i, rep.ID, err)
		}
	}

	// ReadAll fans out to every replica; the comparator must break the
	// three-way timestamp tie on NodeID and return the "z" payload, from
	// whichever node the read is issued.
	for i, n := range nodes {
		got, err := n.cluster.Get(key)
		if err != nil {
			t.Fatalf("Get via node %d: %v", i, err)
		}
		if !bytes.Equal(got, []byte("from-z")) {
			t.Errorf("Get via node %d: got %q want from-z (equal stamps must break on lex-greatest NodeID)", i, got)
		}
	}
}

// TestReplication_R3_ReadRepair pushes the winner to lagging
// replicas on a Quorum read. Setup: write v1 via R=3 (all three get
// v1), then directly poison one node's local backend with a stale
// envelope (zero stamp, different payload), then Get with R=3 +
// ReadQuorum. The read should return v1 + the poisoned node should
// eventually catch up to v1 via read-repair.
func TestReplication_R3_ReadRepair(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	if err := nodes[0].cluster.Put([]byte("rr"), []byte("fresh")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	// Poison node[2]: overwrite its envelope with a v0.3-compat
	// bare-bytes value (zero stamp -> loses every comparison).
	if err := nodes[2].mem.Put([]byte("rr"), []byte("stale-bare")); err != nil {
		t.Fatalf("poison: %v", err)
	}

	// Quorum read MUST return "fresh" (zero-stamp envelope loses).
	got, err := nodes[1].cluster.Get([]byte("rr"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("fresh")) {
		t.Fatalf("Get returned stale: got %q want fresh", got)
	}

	// Read-repair is async + best-effort. Poll up to 2s for node[2]'s
	// backend to carry the freshly-stamped envelope.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := nodes[2].mem.Get([]byte("rr"))
		if err == nil {
			env, _ := cluster.Decode(raw)
			if bytes.Equal(env.Payload, []byte("fresh")) && env.Stamp.TimestampNanos > 0 {
				return // repaired
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	raw, _ := nodes[2].mem.Get([]byte("rr"))
	env, _ := cluster.Decode(raw)
	t.Fatalf("node[2] never received read-repair within 2s: payload=%q stamp=%+v", env.Payload, env.Stamp)
}

// TestReplication_R3_ReadNearest_NoReadRepair pins that ReadNearest
// skips read-repair: a poisoned replica that ISN'T the nearest does
// NOT get rewritten. We can't fully test "the nearest also doesn't
// trigger read-repair on itself" (it has only itself to compare
// against) so this just checks that the asymmetry survives: after a
// Nearest read against a poisoned-but-non-nearest replica, the
// poisoned bytes are still there.
func TestReplication_R3_ReadNearest_NoReadRepair(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	if err := nodes[0].cluster.Put([]byte("nr"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Poison node[2]'s envelope on this key.
	if err := nodes[2].mem.Put([]byte("nr"), []byte("stale-bare")); err != nil {
		t.Fatalf("poison: %v", err)
	}

	// A Nearest read from any node touches only one replica. Run a
	// few + then verify node[2]'s poisoned bytes are still present
	// (no read-repair fired).
	for i := range 5 {
		_, _ = nodes[i%3].cluster.Get([]byte("nr"))
	}
	time.Sleep(100 * time.Millisecond)

	raw, err := nodes[2].mem.Get([]byte("nr"))
	if err != nil {
		t.Fatalf("node[2] backend lost the poisoned value: %v", err)
	}
	if !bytes.Equal(raw, []byte("stale-bare")) {
		t.Errorf("ReadNearest unexpectedly read-repaired node[2]: got %q want stale-bare", raw)
	}
}

// TestReplication_R3_GetAfterDelete_NotFound pins the tombstone read
// path: after Delete fans out a tombstone envelope, Get surfaces
// backend.ErrNotFound (NOT an empty-bytes success). The tombstone-
// landed-on-replica shape is covered in replicate_test.go; this
// pins the cluster's surface contract.
func TestReplication_R3_GetAfterDelete_NotFound(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	if err := nodes[0].cluster.Put([]byte("d"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := nodes[0].cluster.Delete([]byte("d")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	for i, n := range nodes {
		_, err := n.cluster.Get([]byte("d"))
		if !errors.Is(err, backend.ErrNotFound) {
			t.Errorf("Get after Delete from node %d: want ErrNotFound, got %v", i, err)
		}
	}
}
