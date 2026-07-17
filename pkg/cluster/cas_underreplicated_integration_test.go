package cluster_test

// Integration pins for the ROOT fix: a CAS commit whose owner-local apply
// durably committed but whose replica fan-out missed W is a SUCCESS with
// degraded replication (outcome (c)), NOT a retryable error. Transact must NOT
// re-run fn for such a commit - re-running re-commits an already-durable write
// (amplification) and can make a retried insert observe its own committed write
// as a false conflict / already-exists. These reuse the 3-node replicated
// harness (startThreeNodeReplicatedCluster) with real gRPC so the fan-out +
// forwarded-owner CommitCAS RPC are exercised end-to-end, and the co-replica
// helpers ownsCASPinIndex / stopACoReplica from cas_replicate_test.go.
//
// See docs/SPEC.md "The four commit outcomes".

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
)

// setUnderReplicatedFastFail shrinks the retryable window + zeroes the CAS
// backoff for one test, so a REGRESSION (under-W surfacing retryable again)
// fails fast with a re-run count rather than spinning for the production 30s.
// Restores both on cleanup.
func setUnderReplicatedFastFail(t *testing.T) {
	t.Helper()
	prevTO := cluster.SetTransactUnavailableTimeout(500 * time.Millisecond)
	prevBK := cluster.SetCASBaseBackoffZero()
	t.Cleanup(func() {
		cluster.SetTransactUnavailableTimeout(prevTO)
		cluster.RestoreCASBaseBackoff(prevBK)
	})
}

// TestCASReplicate_UnderReplicated_InsertNoFalseAlreadyExists pins outcome (e):
// an insert-if-absent (ExpectAbsent) Transact under an induced under-W commit
// completes exactly ONCE and never surfaces a false already-exists on its own
// durably-committed write. Under the OLD code the first commit applied the
// insert on the owner but missed W -> retryable Unavailable -> Transact re-ran
// fn -> the re-run's tx.Get saw the key present (its own committed write) ->
// fn returned already-exists, a spurious failure on a durable write.
func TestCASReplicate_UnderReplicated_InsertNoFalseAlreadyExists(t *testing.T) {
	setUnderReplicatedFastFail(t)

	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)
	key := []byte("slug:new")
	owner := ownsCASPinIndex(t, nodes, key)
	stopACoReplica(nodes, owner)

	errAlreadyExists := errors.New("slug taken")
	fnRuns := 0
	err := nodes[owner].cluster.Transact(key, func(tx backend.Transaction) error {
		fnRuns++
		_, gerr := tx.Get(key)
		if gerr == nil {
			// Key present: an insert-if-absent rejects. Reaching this on a re-run
			// means fn observed its OWN durably-committed write - the false
			// already-exists the fix eliminates.
			return errAlreadyExists
		}
		if !errors.Is(gerr, backend.ErrNotFound) {
			return gerr
		}
		return tx.Put(key, []byte("v"))
	})
	if errors.Is(err, errAlreadyExists) {
		t.Fatalf("under-W insert observed its own committed write as already-existing (false conflict); fnRuns=%d", fnRuns)
	}
	if err != nil {
		t.Fatalf("under-W insert-if-absent must SUCCEED once; got %v", err)
	}
	if fnRuns != 1 {
		t.Fatalf("insert-if-absent under under-W must run fn exactly once; ran %d", fnRuns)
	}
	// The inserted value is durable on the owner.
	env, ok := decodeReplica(t, nodes[owner], key)
	if !ok || !bytes.Equal(env.Payload, []byte("v")) {
		t.Fatalf("inserted value must be durable on the owner; present=%v payload=%q", ok, env.Payload)
	}
}

// TestCASReplicate_UnderReplicated_RemoteOwnerForwarded pins outcome (f): the
// SAME success-under-replication semantics when the commit is FORWARDED to a
// remote owner. The forwarding node routes to the owner over gRPC; the owner
// commits locally and its fan-out misses W; the CommitCASResponse carries
// committed=true + under_replicated=true across the wire, so the forwarding
// node returns nil and runs fn exactly once. If the wire mapping instead
// surfaced the under-W codes.Unavailable, the forwarding node would classify it
// retryable and re-run fn.
func TestCASReplicate_UnderReplicated_RemoteOwnerForwarded(t *testing.T) {
	setUnderReplicatedFastFail(t)

	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)
	key := []byte("fwd-durable")
	owner := ownsCASPinIndex(t, nodes, key)

	// The two non-owners: one routes the commit (stays UP so the owner's fan-out
	// can ack it), the other is the down victim that keeps the fan-out under W.
	router, victim := -1, -1
	for i := range nodes {
		if i == owner {
			continue
		}
		if router == -1 {
			router = i
		} else {
			victim = i
		}
	}
	nodes[victim].stopGRPC()

	fnRuns := 0
	err := nodes[router].cluster.Transact(key, func(tx backend.Transaction) error {
		fnRuns++
		if _, gerr := tx.Get(key); gerr != nil && !errors.Is(gerr, backend.ErrNotFound) {
			return gerr
		}
		return tx.Put(key, []byte("fwd"))
	})
	if err != nil {
		t.Fatalf("forwarded under-W commit must SUCCEED via the remote owner; got %v (fnRuns %d)", err, fnRuns)
	}
	if fnRuns != 1 {
		t.Fatalf("a forwarded committed-under-replicated commit must run fn exactly once; ran %d", fnRuns)
	}
	// Durable on the (remote) owner's local backend.
	env, ok := decodeReplica(t, nodes[owner], key)
	if !ok || !bytes.Equal(env.Payload, []byte("fwd")) {
		t.Fatalf("forwarded under-replicated write must be durable on the owner; present=%v payload=%q", ok, env.Payload)
	}
}
