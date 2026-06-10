package cluster_test

// v0.6.x CAS write-set replication: integration coverage that composes the
// 3-node replicated harness (startThreeNodeReplicatedCluster from
// replicate_test.go) with the CAS surface (Begin / Transact / Commit). The
// envelope-shape + apply-only contract is pinned in cas_replicate_test.go;
// this file pins the OPERATOR-facing properties the v0.6.x feature exists to
// deliver:
//
//   - a committed CAS write-set lands the LWW envelope on ALL R replicas
//     (Put + Delete), readable back through the cluster after the commit;
//   - the durability payoff: a CAS write survives loss of the owning node
//     (read it from a surviving replica), which is the whole point of
//     replicating the write-set;
//   - the WriteConsistency knob gates the CAS commit the same way it gates a
//     single-key Put (Quorum tolerates one replica down; All does not);
//   - LWW resolves a CAS commit against a later single-key Put by stamp;
//   - replication does NOT break OCC: the no-lost-update invariant still
//     holds under contention at R=3.
//
// Helpers decodeReplica / seedConverged / eachReplicaEventually live in
// cas_replicate_test.go (same cluster_test package); reused here.

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ownerIndex returns the index of the node that owns key, or -1 if no node
// claims ownership (should never happen on a converged ring). With R=3 and 3
// members every node is a replica, but exactly one is the ring OWNER (the
// CAS commit's validate-and-apply routes there).
func ownerIndex(nodes []*replicatedNode, key []byte) int {
	for i, n := range nodes {
		if n.cluster.OwnsKey(key) {
			return i
		}
	}
	return -1
}

// transactPut runs a read-then-Put CAS commit for key=value, routed through
// whichever node owns key (so the owner is local + the validate-and-apply
// runs without a FailedPrecondition forward-reject). Returns the commit
// error verbatim so callers can assert on under-W / conflict / success.
func transactPut(nodes []*replicatedNode, key, value []byte) error {
	idx := ownerIndex(nodes, key)
	if idx < 0 {
		return fmt.Errorf("no owner for key %q", key)
	}
	return nodes[idx].cluster.Transact(key, func(tx backend.Transaction) error {
		if _, err := tx.Get(key); err != nil && !errors.Is(err, backend.ErrNotFound) {
			return err
		}
		return tx.Put(key, value)
	})
}

// TestCASReplicate_R3_CommitLandsOnAllReplicas_PutAndDelete pins
// requirement (1): a CAS commit at R=3 + WriteAll lands the LWW envelope on
// ALL three replica backends, for BOTH a Put write-op and a Delete write-op
// (tombstone). Asserted by decoding each replica's physical backend.
func TestCASReplicate_R3_CommitLandsOnAllReplicas_PutAndDelete(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	// Put write-op: a CAS commit of k=v1 must land a value envelope on all 3.
	if err := transactPut(nodes, []byte("k"), []byte("v1")); err != nil {
		t.Fatalf("CAS Put commit: %v", err)
	}
	eachReplicaEventually(t, nodes, []byte("k"), func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, []byte("v1")) && env.Stamp.TimestampNanos != 0
	})
	// Explicit "all three" assertion on the raw backends (not just the poll).
	for i, n := range nodes {
		env, ok := decodeReplica(t, n, []byte("k"))
		if !ok {
			t.Fatalf("node %d: CAS-committed value envelope missing", i)
		}
		if !bytes.Equal(env.Payload, []byte("v1")) {
			t.Errorf("node %d: payload %q want v1", i, env.Payload)
		}
	}

	// Delete write-op: a CAS commit that deletes k must land a STAMPED
	// tombstone (empty payload) on all 3, not a bare key removal.
	idx := ownerIndex(nodes, []byte("k"))
	if err := nodes[idx].cluster.Transact([]byte("k"), func(tx backend.Transaction) error {
		if _, err := tx.Get([]byte("k")); err != nil {
			return err
		}
		return tx.Delete([]byte("k"))
	}); err != nil {
		t.Fatalf("CAS Delete commit: %v", err)
	}
	eachReplicaEventually(t, nodes, []byte("k"), func(env cluster.Envelope, present bool) bool {
		return present && len(env.Payload) == 0 && env.Stamp.TimestampNanos != 0
	})
	for i, n := range nodes {
		env, ok := decodeReplica(t, n, []byte("k"))
		if !ok {
			t.Fatalf("node %d: tombstone envelope missing (bare removal leaked?)", i)
		}
		if len(env.Payload) != 0 {
			t.Errorf("node %d: tombstone payload %q want empty", i, env.Payload)
		}
	}
}

// TestCASReplicate_R3_WriteQuorum_ToleratesOneReplicaDown pins
// requirement (2): R=3 + WriteQuorum (W=2). With one replica's gRPC down a
// CAS commit still succeeds, because the owner's local commit (1 ack) plus
// one reachable replica (1 ack) meets the quorum of 2.
func TestCASReplicate_R3_WriteQuorum_ToleratesOneReplicaDown(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteQuorum, cluster.ReadQuorum)

	key := []byte("q-cas")
	owner := ownerIndex(nodes, key)
	if owner < 0 {
		t.Fatalf("no owner for %q", key)
	}
	// Stop the gRPC of a NON-owner replica so the owner stays reachable as
	// the commit target but one fan-out destination is unreachable.
	down := (owner + 1) % 3
	nodes[down].stopGRPC()

	if err := nodes[owner].cluster.Transact(key, func(tx backend.Transaction) error {
		if _, gerr := tx.Get(key); gerr != nil && !errors.Is(gerr, backend.ErrNotFound) {
			return gerr
		}
		return tx.Put(key, []byte("qv"))
	}); err != nil {
		t.Fatalf("WriteQuorum CAS commit with one replica down should succeed: %v", err)
	}

	// The owner + the one reachable replica must hold the value (2 of 3).
	gotCount := 0
	for i, n := range nodes {
		if i == down {
			continue
		}
		env, ok := decodeReplica(t, n, key)
		if ok && bytes.Equal(env.Payload, []byte("qv")) {
			gotCount++
		}
	}
	if gotCount < 2 {
		t.Errorf("want >=2 reachable replicas holding the CAS value, got %d", gotCount)
	}
}

// TestCASReplicate_R3_WriteAll_FailsWhenOneReplicaDown pins
// requirement (3): R=3 + WriteAll (W=3). With one replica down the CAS
// commit cannot collect 3 acks and returns an under-W error (NOT a
// conflict). The owner's LOCAL commit has ALREADY landed (replication is
// best-effort-to-W after the local tx commits), so we assert the value IS
// present on the owner's backend even though the commit reported an error.
func TestCASReplicate_R3_WriteAll_FailsWhenOneReplicaDown(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	key := []byte("all-cas")
	owner := ownerIndex(nodes, key)
	if owner < 0 {
		t.Fatalf("no owner for %q", key)
	}
	down := (owner + 1) % 3
	nodes[down].stopGRPC()

	err := nodes[owner].cluster.Transact(key, func(tx backend.Transaction) error {
		if _, gerr := tx.Get(key); gerr != nil && !errors.Is(gerr, backend.ErrNotFound) {
			return gerr
		}
		return tx.Put(key, []byte("av"))
	})
	if err == nil {
		t.Fatalf("WriteAll CAS commit with one replica down should fail under-W")
	}
	if errors.Is(err, backend.ErrCASConflict) {
		t.Fatalf("under-W must NOT surface as a conflict: %v", err)
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unavailable {
		t.Errorf("want codes.Unavailable for under-W, got %v", st.Code())
	}

	// Local commit landed despite the under-W fan-out: the owner's backend
	// holds the stamped envelope. (Document the assertion: the write is
	// durable on the owner, just not on W replicas.)
	env, ok := decodeReplica(t, nodes[owner], key)
	if !ok {
		t.Fatalf("owner backend should hold the locally-committed envelope after under-W")
	}
	if !bytes.Equal(env.Payload, []byte("av")) {
		t.Errorf("owner envelope payload %q want av", env.Payload)
	}
}

// TestCASReplicate_R3_ReadAfterCommit_SurvivesOwnerLoss pins
// requirement (4): the durability payoff. After a CAS commit on a 3-node
// R=3 cluster, a cluster Get returns the committed value. Then we close the
// OWNING node (graceful leave -> the survivors converge on a 2-member
// ring); a Get from a surviving replica STILL returns the value, because
// the write-set was replicated off the owner.
func TestCASReplicate_R3_ReadAfterCommit_SurvivesOwnerLoss(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	key := []byte("durable-cas")
	if err := transactPut(nodes, key, []byte("survives")); err != nil {
		t.Fatalf("CAS commit: %v", err)
	}

	// Sanity: readable through the cluster immediately after the commit.
	owner := ownerIndex(nodes, key)
	if got, err := nodes[owner].cluster.Get(key); err != nil || !bytes.Equal(got, []byte("survives")) {
		t.Fatalf("post-commit Get: got %q err %v want survives", got, err)
	}

	// Identify the two survivors BEFORE the owner leaves.
	survivors := make([]*replicatedNode, 0, 2)
	for i, n := range nodes {
		if i != owner {
			survivors = append(survivors, n)
		}
	}

	// Kill the owner via a graceful Close (broadcasts leave). The survivors
	// re-converge on a 2-member ring.
	if err := nodes[owner].cluster.Close(); err != nil {
		t.Fatalf("close owner: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		ok := true
		for _, s := range survivors {
			if len(s.cluster.Members()) != 2 {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("survivors never converged on a 2-member ring after owner loss")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The value must still be readable from a surviving replica. With R=3 +
	// WriteAll the original commit landed on every node, so both survivors
	// physically hold the envelope; a ReadQuorum Get over the new 2-member
	// ring resolves it. Try each survivor; at least one must serve it.
	var lastErr error
	for _, s := range survivors {
		got, err := s.cluster.Get(key)
		if err == nil && bytes.Equal(got, []byte("survives")) {
			return // durable across owner loss
		}
		lastErr = err
	}
	t.Fatalf("CAS value not readable from any survivor after owner loss (last err %v)", lastErr)
}

// TestCASReplicate_R3_LWW_LaterPutWinsOverCASCommit pins requirement (5):
// LWW consistency across the CAS and single-key write paths. A CAS commit
// writes k, then a strictly-later single-key Put on the same key writes a
// new value; a cluster Get resolves to the later (Put) value by stamp.
func TestCASReplicate_R3_LWW_LaterPutWinsOverCASCommit(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	key := []byte("lww-cas")
	if err := transactPut(nodes, key, []byte("from-cas")); err != nil {
		t.Fatalf("CAS commit: %v", err)
	}

	// Strictly-later wall-clock so the LWW comparator resolves on timestamp,
	// not the NodeID tiebreak.
	time.Sleep(3 * time.Millisecond)
	if err := nodes[0].cluster.Put(key, []byte("from-put")); err != nil {
		t.Fatalf("later Put: %v", err)
	}

	// Every node's cluster Get must resolve to the later Put value.
	for i, n := range nodes {
		got, err := n.cluster.Get(key)
		if err != nil {
			t.Fatalf("node %d Get: %v", i, err)
		}
		if !bytes.Equal(got, []byte("from-put")) {
			t.Errorf("node %d Get: got %q want from-put (later Put must win LWW)", i, got)
		}
	}
}

// TestCASReplicate_R3_LWW_LaterCASCommitWinsOverPut is the mirror of the
// above: a single-key Put then a strictly-later CAS commit; the CAS value
// wins on read. Together the two pin that the CAS path and the Put path
// share ONE LWW timeline (no separate ordering domain).
func TestCASReplicate_R3_LWW_LaterCASCommitWinsOverPut(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	key := []byte("lww-cas2")
	seedConverged(t, nodes, 0, key, []byte("from-put"))

	time.Sleep(3 * time.Millisecond)
	if err := transactPut(nodes, key, []byte("from-cas")); err != nil {
		t.Fatalf("later CAS commit: %v", err)
	}

	for i, n := range nodes {
		got, err := n.cluster.Get(key)
		if err != nil {
			t.Fatalf("node %d Get: %v", i, err)
		}
		if !bytes.Equal(got, []byte("from-cas")) {
			t.Errorf("node %d Get: got %q want from-cas (later CAS must win LWW)", i, got)
		}
	}
}

// TestCASReplicate_R1_StoresRawNotEnvelope pins requirement (6): the R=1
// path is unchanged. A single-node CAS commit stores the RAW value bytes,
// NOT an envelope. A regression to envelope-wrapping at R=1 would corrupt
// every existing single-node deploy. (Complements R1_StaysRaw in
// cas_replicate_test.go with an explicit no-magic-prefix check.)
func TestCASReplicate_R1_StoresRawNotEnvelope(t *testing.T) {
	c := newSingleNode(t)

	if err := c.Transact([]byte("k"), func(tx backend.Transaction) error {
		if _, err := tx.Get([]byte("k")); err != nil && !errors.Is(err, backend.ErrNotFound) {
			return err
		}
		return tx.Put([]byte("k"), []byte("rawval"))
	}); err != nil {
		t.Fatalf("Transact: %v", err)
	}

	raw, err := c.LocalGet([]byte("k"))
	if err != nil {
		t.Fatalf("LocalGet: %v", err)
	}
	// The stored bytes are EXACTLY the raw value (no envelope framing).
	if !bytes.Equal(raw, []byte("rawval")) {
		t.Fatalf("R=1 stored bytes %q want raw rawval (envelope leaked into R=1)", raw)
	}
	// And the stored bytes must NOT decode as a valid envelope carrying the
	// value as a payload (an envelope of "rawval" would be longer + framed).
	// A raw 6-byte value is too short to be a valid framed envelope, so a
	// successful Decode whose payload equals the raw value would be the
	// regression signal. We assert the bytes equal the raw value exactly
	// (above) which already excludes framing; this is the explicit length
	// guard that the value was not wrapped.
	if len(raw) != len("rawval") {
		t.Fatalf("R=1 stored %d bytes, want %d (no framing)", len(raw), len("rawval"))
	}
}

// TestCASReplicate_R3_NoLostUpdate pins requirement (7): the write-set
// replication step must NOT break OCC. N goroutines each increment a shared
// counter via Transact on a 3-node R=3 cluster; the final value must equal
// N exactly (no lost update), AND the committed total must be durable on all
// replicas. This is the no-lost-update invariant from the v0.6 suite, re-run
// with the R>1 write-set fan-out engaged (WriteAll -> every increment fans
// its envelope to all 3 replicas before the Transact returns).
//
// Read mode is ReadNearest BY DESIGN. The OCC read-modify-write reads the
// counter through the cluster read path; under ReadQuorum / ReadAll that
// read schedules an ASYNC read-repair which writes the read's winning
// envelope back to replicas verbatim. A repair scheduled with value V can
// fire AFTER a later CAS commit has landed V+1 on the owner-local backend,
// transiently clobbering it back to V at the storage layer (LWW re-resolves
// on the NEXT quorum read, but the CAS validate-and-apply reads the owner's
// LOCAL copy, so a clobbered owner copy can let a stale-based increment
// commit). That read-repair-vs-storage-layer race is the SAME one
// documented on seedConverged; it is orthogonal to write-set replication
// (it is a property of read-repair clobbering, present since v0.4) and is
// not what this requirement is about. ReadNearest skips read-repair, so the
// owner-local copy every commit validates against is never clobbered by a
// stale repair, isolating the OCC + fan-out behavior under test. The write
// path is still full R=3 replication; only the orthogonal repair is off.
func TestCASReplicate_R3_NoLostUpdate(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	key := []byte("counter")
	// Route the seed + the increments through the OWNER so every commit is
	// owner-local (a non-owner Transact would FailedPrecondition; the CAS
	// surface validates on the owner). All increments target the same key,
	// so they all share one owner.
	owner := ownerIndex(nodes, key)
	if owner < 0 {
		t.Fatalf("no owner for %q", key)
	}
	oc := nodes[owner].cluster
	if err := transactPut(nodes, key, []byte("0")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			err := oc.Transact(key, func(tx backend.Transaction) error {
				cur, err := tx.Get(key)
				if err != nil {
					return err
				}
				var n int
				_, _ = fmt.Sscanf(string(cur), "%d", &n)
				return tx.Put(key, []byte(fmt.Sprintf("%d", n+1)))
			})
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Transact increment: %v", err)
	}

	// Final value through the cluster must be exactly the worker count.
	got, err := oc.Get(key)
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	if string(got) != fmt.Sprintf("%d", workers) {
		t.Fatalf("counter: got %q want %d (lost update under R=3 replication)", got, workers)
	}

	// Durability: the final total must (eventually) land on every replica as
	// a stamped envelope carrying the counter value.
	want := []byte(fmt.Sprintf("%d", workers))
	eachReplicaEventually(t, nodes, key, func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, want)
	})
}
