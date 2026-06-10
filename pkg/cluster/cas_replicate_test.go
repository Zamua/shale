package cluster_test

// v0.6.x CAS write-set replication tests: at R>1 a committed CAS write-set
// fans out to the shard's R replicas as LWW envelopes (apply-only via
// ApplyBatch), validation decodes the stored envelope before comparing,
// and Delete replicates as a stamped tombstone. The R=1 path stays raw
// (covered by the existing cas_test.go single-node suite).
//
// These reuse the 3-node replicated harness from replicate_test.go
// (startThreeNodeReplicatedCluster), which brings up real gRPC + member-
// list wiring so the ApplyBatch RPC is exercised end-to-end.

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// decodeReplica reads key directly from node i's backend and decodes the
// stored envelope. A missing key returns ok=false.
func decodeReplica(t *testing.T, n *replicatedNode, key []byte) (cluster.Envelope, bool) {
	t.Helper()
	raw, err := n.mem.Get(key)
	if err != nil {
		return cluster.Envelope{}, false
	}
	env, err := cluster.Decode(raw)
	if err != nil {
		t.Fatalf("decode replica envelope: %v", err)
	}
	return env, true
}

// seedConverged Puts key=value via the cluster and then waits until EVERY
// replica's backend holds an envelope with that payload, polling the RAW
// backends directly (NOT cluster Gets). The single-key Put fan-out is
// surplus-in-background past W, and replica-side writes are verbatim (LWW
// is resolved at READ time, not write time), so a seed Put can still be in
// flight to a lagging replica when the next op runs. Waiting for the seed
// to land on every backend first makes the subsequent stamped-envelope
// assertions on raw backends deterministic.
//
// Polling raw backends (rather than cluster Gets) is deliberate: a cluster
// Get under ReadAll / ReadQuorum schedules read-repair, which pushes the
// seed envelope to replicas asynchronously and could re-fire AFTER a later
// mutation, clobbering the new stamped value at the storage layer (benign
// in production: the next quorum read resolves LWW and re-repairs toward
// the winner, but it would race a raw-backend test assertion). By reading
// backends directly we never schedule a repair on the seed.
func seedConverged(t *testing.T, nodes []*replicatedNode, from int, key, value []byte) {
	t.Helper()
	if err := nodes[from].cluster.Put(key, value); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		all := true
		for _, n := range nodes {
			env, ok := decodeReplica(t, n, key)
			if !ok || !bytes.Equal(env.Payload, value) {
				all = false
				break
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("seed %q=%q did not converge on all replicas", key, value)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// eachReplicaEventually polls every replica's RAW backend until check
// holds on the decoded envelope for key on all of them, or the deadline
// passes. It exists because a CAS commit's write-set is the LWW winner but
// can be transiently clobbered on a single replica at the storage layer by
// an async read-repair of the pre-commit value (read-repair writes are
// verbatim, LWW resolves on the NEXT read, see seedConverged's note). The
// committed envelope re-converges as repairs settle; this poll pins the
// eventual state without flaking on the transient window. The check on the
// owner-local copy is immediate, but the remote replicas may lag a beat.
func eachReplicaEventually(t *testing.T, nodes []*replicatedNode, key []byte, check func(env cluster.Envelope, present bool) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		all := true
		for _, n := range nodes {
			env, ok := decodeReplica(t, n, key)
			if !check(env, ok) {
				all = false
				break
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			// Surface the per-node state for diagnosis.
			for i, n := range nodes {
				env, ok := decodeReplica(t, n, key)
				t.Logf("node %d: present=%v payload=%q stamp=%+v", i, ok, env.Payload, env.Stamp)
			}
			t.Fatalf("replicas never satisfied the check for key %q", key)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestCASReplicate_WriteSetFansOutToAllReplicas: with R=3 + WriteAll, a
// CAS Transact that Puts two keys lands stamped envelopes on every
// replica's backend, all under ONE shared commit stamp.
func TestCASReplicate_WriteSetFansOutToAllReplicas(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	err := nodes[0].cluster.Transact([]byte("acct:a"), func(tx backend.Transaction) error {
		if _, err := tx.Get([]byte("acct:a")); err != nil && !errors.Is(err, backend.ErrNotFound) {
			return err
		}
		if err := tx.Put([]byte("acct:a"), []byte("100")); err != nil {
			return err
		}
		return tx.Put([]byte("{acct:a}:log"), []byte("opened"))
	})
	if err != nil {
		t.Fatalf("Transact: %v", err)
	}

	// Every replica must hold both keys as stamped envelopes (the commit
	// wrote a fresh key under WriteAll, so all R replicas applied it).
	eachReplicaEventually(t, nodes, []byte("acct:a"), func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, []byte("100"))
	})
	eachReplicaEventually(t, nodes, []byte("{acct:a}:log"), func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, []byte("opened"))
	})

	// The two write-set keys must carry the SAME stamp on each node (the
	// shared commit stamp): the whole write-set replicates + LWW-resolves
	// as a unit.
	for i, n := range nodes {
		envA, okA := decodeReplica(t, n, []byte("acct:a"))
		envLog, okLog := decodeReplica(t, n, []byte("{acct:a}:log"))
		if !okA || !okLog {
			t.Fatalf("node %d missing a committed key (a=%v log=%v)", i, okA, okLog)
		}
		if envA.Stamp != envLog.Stamp {
			t.Errorf("node %d: write-set keys carry different stamps %+v vs %+v (must share)", i, envA.Stamp, envLog.Stamp)
		}
		if envA.Stamp.TimestampNanos == 0 {
			t.Errorf("node %d: stamp not set: %+v", i, envA.Stamp)
		}
	}
}

// TestCASReplicate_ValidationDecodesEnvelope: a CAS commit's read-check
// must compare the client-observed (decoded) payload against the decoded
// stored envelope, not the raw envelope bytes. Seed a value via a single-
// key Put (stored as an envelope), then a Transact that reads it and
// conditionally writes must NOT spuriously conflict.
func TestCASReplicate_ValidationDecodesEnvelope(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	seedConverged(t, nodes, 0, []byte("k"), []byte("v1"))

	// A read-modify-write that reads v1 (decoded) and writes v2. If
	// validation compared the raw envelope to the decoded ExpectedVal it
	// would always conflict; it must succeed.
	err := nodes[1].cluster.Transact([]byte("k"), func(tx backend.Transaction) error {
		got, err := tx.Get([]byte("k"))
		if err != nil {
			return err
		}
		if !bytes.Equal(got, []byte("v1")) {
			t.Errorf("tx read: got %q want v1 (decoded payload, not envelope bytes)", got)
		}
		return tx.Put([]byte("k"), []byte("v2"))
	})
	if err != nil {
		t.Fatalf("Transact read-modify-write: %v", err)
	}

	// The new value must be readable through the cluster (decoded, LWW-
	// resolved) and re-converge as a stamped envelope on every replica.
	got, err := nodes[2].cluster.Get([]byte("k"))
	if err != nil {
		t.Fatalf("post-commit Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("post-commit Get: got %q want v2", got)
	}
	eachReplicaEventually(t, nodes, []byte("k"), func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, []byte("v2"))
	})
}

// TestCASReplicate_StaleReadConflicts: a CAS commit whose read-set no
// longer matches the owner's decoded copy conflicts (the decode path must
// preserve conflict detection, not mask it). The conflicting mutation is
// itself a CAS commit so it is GUARANTEED to land on the validating owner-
// local copy (a CAS commit always writes the owner's local tx first),
// avoiding the surplus-in-background nondeterminism a single-key Put fan-
// out would introduce into this assertion.
func TestCASReplicate_StaleReadConflicts(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	seedConverged(t, nodes, 0, []byte("k"), []byte("v1"))

	// Stale tx: reads v1, buffers a write, does NOT commit yet.
	tx, err := nodes[0].cluster.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if got, err := tx.Get([]byte("k")); err != nil || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("tx Get: got %q err %v", got, err)
	}
	if err := tx.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("tx Put: %v", err)
	}

	// Conflicting writer commits k=v9 via its own CAS Transact, which
	// updates the owner-local copy the stale tx will validate against.
	if err := nodes[0].cluster.Transact([]byte("k"), func(itx backend.Transaction) error {
		if _, gerr := itx.Get([]byte("k")); gerr != nil {
			return gerr
		}
		return itx.Put([]byte("k"), []byte("v9"))
	}); err != nil {
		t.Fatalf("conflicting Transact: %v", err)
	}

	if err := tx.Commit(); !errors.Is(err, backend.ErrCASConflict) {
		t.Fatalf("Commit: want ErrCASConflict, got %v", err)
	}
	// The conflicting value must survive (the stale tx did not apply).
	got, err := nodes[0].cluster.Get([]byte("k"))
	if err != nil {
		t.Fatalf("post Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v9")) {
		t.Fatalf("post Get: got %q want v9 (stale tx must not have applied)", got)
	}
}

// TestCASReplicate_DeleteReplicatesAsTombstone: a CAS Delete writes a
// stamped empty-payload tombstone envelope (NOT a bare key removal) to
// every replica, and a subsequent cluster Get surfaces NotFound.
func TestCASReplicate_DeleteReplicatesAsTombstone(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	seedConverged(t, nodes, 0, []byte("k"), []byte("v1"))
	err := nodes[0].cluster.Transact([]byte("k"), func(tx backend.Transaction) error {
		if _, err := tx.Get([]byte("k")); err != nil {
			return err
		}
		return tx.Delete([]byte("k"))
	})
	if err != nil {
		t.Fatalf("Transact delete: %v", err)
	}

	// Every replica must (eventually) hold a STAMPED tombstone (envelope
	// present, empty payload), not an absent key: a bare removal would
	// lose LWW to a stale stamped value on another replica.
	eachReplicaEventually(t, nodes, []byte("k"), func(env cluster.Envelope, present bool) bool {
		return present && len(env.Payload) == 0 && env.Stamp.TimestampNanos != 0
	})
	if _, err := nodes[1].cluster.Get([]byte("k")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("post-delete Get: want ErrNotFound, got %v", err)
	}
}

// TestCASReplicate_ExpectAbsentTreatsTombstoneAsAbsent: a CAS commit that
// expects a key to be absent must SUCCEED when the stored value is a
// tombstone envelope (the decoded payload is empty => not-found).
func TestCASReplicate_ExpectAbsentTreatsTombstoneAsAbsent(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	// Put then Delete leaves a tombstone envelope on every replica.
	if err := nodes[0].cluster.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	if err := nodes[0].cluster.Delete([]byte("k")); err != nil {
		t.Fatalf("seed Delete: %v", err)
	}

	// A Transact that reads k (sees absent via the tombstone) and writes a
	// fresh value must commit without a conflict.
	err := nodes[0].cluster.Transact([]byte("k"), func(tx backend.Transaction) error {
		if _, err := tx.Get([]byte("k")); !errors.Is(err, backend.ErrNotFound) {
			t.Errorf("tx Get over tombstone: want ErrNotFound, got %v", err)
		}
		return tx.Put([]byte("k"), []byte("reborn"))
	})
	if err != nil {
		t.Fatalf("Transact over tombstone: %v", err)
	}
	got, err := nodes[0].cluster.Get([]byte("k"))
	if err != nil || !bytes.Equal(got, []byte("reborn")) {
		t.Fatalf("post Get: got %q err %v want reborn", got, err)
	}
}

// TestCASReplicate_WriteOne_SucceedsWithOwnerOnly: with WriteOne (W=1),
// the owner's local commit alone satisfies W, so a CAS commit succeeds
// even if every other replica's gRPC is down (the fan-out is best-effort
// at W=1).
func TestCASReplicate_WriteOne_SucceedsWithOwnerOnly(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteOne, cluster.ReadNearest)

	nodes[1].stopGRPC()
	nodes[2].stopGRPC()

	// Route the commit to whichever node owns "solo" and run it there so
	// the owner is local + the other (down) replicas are remote.
	var err error
	for _, n := range nodes {
		err = n.cluster.Transact([]byte("solo"), func(tx backend.Transaction) error {
			if _, gerr := tx.Get([]byte("solo")); gerr != nil && !errors.Is(gerr, backend.ErrNotFound) {
				return gerr
			}
			return tx.Put([]byte("solo"), []byte("ok"))
		})
		// FailedPrecondition means this node isn't the owner; try the next.
		if st, ok := status.FromError(err); ok && st.Code() == codes.FailedPrecondition {
			continue
		}
		break
	}
	if err != nil {
		t.Fatalf("WriteOne Transact with peers down should succeed: %v", err)
	}
}

// TestCASReplicate_WriteAll_FailsWhenReplicaDown: with WriteAll (W=3), a
// CAS commit returns an error (under-W) when a replica's gRPC is down,
// even though the owner's local commit already happened. The error is the
// best-effort-to-W signal, NOT a conflict.
func TestCASReplicate_WriteAll_FailsWhenReplicaDown(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	nodes[2].stopGRPC()

	var commitErr error
	for _, n := range nodes {
		commitErr = n.cluster.Transact([]byte("durable"), func(tx backend.Transaction) error {
			if _, err := tx.Get([]byte("durable")); err != nil && !errors.Is(err, backend.ErrNotFound) {
				return err
			}
			return tx.Put([]byte("durable"), []byte("x"))
		})
		if st, ok := status.FromError(commitErr); ok && st.Code() == codes.FailedPrecondition {
			continue
		}
		break
	}
	if commitErr == nil {
		t.Fatalf("WriteAll Transact with a replica down should fail under-W")
	}
	if errors.Is(commitErr, backend.ErrCASConflict) {
		t.Fatalf("under-W must NOT surface as a conflict: %v", commitErr)
	}
}

// TestCASReplicate_R1_StaysRaw pins the R=1 invariant: a single-node CAS
// commit stores RAW values (no envelope magic byte), exactly as v0.6 did.
// A regression to envelope-wrapping here would corrupt every existing
// single-node deploy's reads.
func TestCASReplicate_R1_StaysRaw(t *testing.T) {
	c := newSingleNode(t)

	err := c.Transact([]byte("k"), func(tx backend.Transaction) error {
		if _, err := tx.Get([]byte("k")); err != nil && !errors.Is(err, backend.ErrNotFound) {
			return err
		}
		return tx.Put([]byte("k"), []byte("rawval"))
	})
	if err != nil {
		t.Fatalf("Transact: %v", err)
	}

	// LocalGet returns exactly what the backend stored. At R=1 it must be
	// the raw value, not an envelope (no 0xE0 magic prefix).
	raw, err := c.LocalGet([]byte("k"))
	if err != nil {
		t.Fatalf("LocalGet: %v", err)
	}
	if !bytes.Equal(raw, []byte("rawval")) {
		t.Fatalf("R=1 stored bytes: got %q want raw rawval (envelope leaked into R=1 path?)", raw)
	}
}
