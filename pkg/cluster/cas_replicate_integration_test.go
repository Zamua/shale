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
//     single-key Put (Quorum tolerates one replica down);
//   - LWW resolves a CAS commit against a later single-key Put by stamp;
//   - replication does NOT break OCC: the no-lost-update invariant still
//     holds under contention at R=3.
//
// Two neighbouring properties are pinned elsewhere and deliberately NOT
// duplicated here:
//
//   - the under-W (WriteAll with a replica down) commit outcome lives in
//     cas_replicate_test.go's TestCASReplicate_WriteAll_UnderReplicated-
//     CommitSucceeds, which selects the owner through the CAS-designated-
//     owner predicate (OwnsCASPin) rather than the plain ring owner;
//   - the no-lost-update invariant under ReadQuorum (where read-repair is
//     in play) lives in lww_on_write_test.go's TestLWWOnWrite_NoLostUpdate_
//     ReadQuorum, alongside its ReadAll sibling. TestCASReplicate_R3_
//     NoLostUpdate below is the ReadNearest (no read-repair) variant.
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
	// Seed the key so its partition is PHYSICALLY HELD on every node before
	// the commit below. This is load-bearing for determinism, not setup
	// convenience: WaitForRebalanceIdle only proves the coordinator is idle
	// at one instant, and the periodic sweep tick re-runs reconcile with
	// retryEmpty=true on every tick regardless of ring generation. An owned
	// partition that is merely EMPTY is not treated as settled on that path
	// (it burns a bounded empty-retry budget instead), so on a fresh cluster
	// with an empty keyspace essentially every owned partition gets re-
	// registered StateReceiving for several ticks after setup. While a
	// partition is Receiving, the replica-side migration guard REJECTS the
	// CAS write-set batch for keys in it. That rejection is classified
	// transient, so it counts as neither an ack nor a failure: the fan-out
	// silently lands under W and the commit still returns nil, because an
	// under-replicated CAS commit is a SUCCESS by design (outcome (c)).
	// Seeding makes the partition physically present, and reconcile skips
	// any partition it already holds, so the guard cannot reopen for this
	// key and the fan-out below is measured on a genuinely quiet cluster.
	//
	// This must run while all three nodes are UP so the seed reaches every
	// backend.
	seedConverged(t, nodes, owner, key, []byte("seed"))

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

// TestCASReplicate_R3_NoLostUpdate pins requirement (7): the write-set
// replication step must NOT break OCC. N goroutines each increment a shared
// counter via Transact on a 3-node R=3 cluster; the final value must equal
// N exactly (no lost update), AND the committed total must be durable on all
// replicas. This is the no-lost-update invariant from the v0.6 suite, re-run
// with the R>1 write-set fan-out engaged (WriteAll -> every increment fans
// its envelope to all 3 replicas before the Transact returns).
//
// Read mode is ReadNearest here to isolate the OCC + fan-out behavior
// under test, but the apply-if-newer write rule (v0.7+) means the test
// would now ALSO pass under ReadQuorum / ReadAll: a stale async read-
// repair can no longer clobber a newer owner-local value, because the
// replica-receiving write paths reject any envelope carrying an older-or-
// equal stamp. lww_on_write_test.go's TestLWWOnWrite_NoLostUpdate_ReadQuorum
// and _ReadAll pin that stronger guarantee directly, on the same workload.
// Keeping this variant on ReadNearest is
// purely for isolation (no repair traffic in the trace) and determinism;
// it is no longer REQUIRED for correctness the way it was pre-v0.7. The
// write path is full R=3 replication in both variants.
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
	for range workers {
		go func() {
			defer wg.Done()
			err := oc.Transact(key, func(tx backend.Transaction) error {
				cur, err := tx.Get(key)
				if err != nil {
					return err
				}
				var n int
				_, _ = fmt.Sscanf(string(cur), "%d", &n)
				return tx.Put(key, fmt.Appendf(nil, "%d", n+1))
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
	want := fmt.Appendf(nil, "%d", workers)
	eachReplicaEventually(t, nodes, key, func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, want)
	})
}
