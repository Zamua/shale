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
// surplus-in-background past W, so a seed Put can still be in flight to a
// lagging replica when the next op runs. Waiting for the seed to land on
// every backend first makes the subsequent stamped-envelope assertions on
// raw backends deterministic.
//
// Polling raw backends (rather than cluster Gets) avoids scheduling read-
// repair on the seed: a cluster Get under ReadAll / ReadQuorum schedules a
// repair, and while apply-if-newer (v0.7+) means a stale repair can no
// longer CLOBBER a newer value, the repair traffic would still add non-
// determinism to a raw-backend timing assertion. Reading backends directly
// keeps the trace clean.
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
// passes. It exists because a single-key Put fan-out is surplus-in-
// background past W, so a remote replica may lag the owner-local commit by
// a beat before the committed envelope lands everywhere. This poll pins
// the eventual state without flaking on that propagation window. (Apply-
// if-newer, v0.7+, removed the older flake source where a stale read-
// repair could transiently clobber the committed envelope on a replica;
// the only remaining lag is plain fan-out propagation.)
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

// TestCASReplicate_WriteAll_UnderReplicatedCommitSucceeds: with WriteAll
// (W=3) and a co-replica's gRPC down, a CAS commit that misses W is a SUCCESS
// with degraded replication (outcome (c)), NOT an error. The owner-local
// commit is durable, so the write is readable on the owner immediately and
// Transact returns nil. This is the ROOT-fix re-contract of the old "under-W
// fails" behavior: the owner-local commit already applied, so a fan-out
// shortfall cannot fail it. See docs/SPEC.md "The four commit outcomes".
func TestCASReplicate_WriteAll_UnderReplicatedCommitSucceeds(t *testing.T) {
	// Shrink the retryable window + zero the backoff so that IF this fix ever
	// regresses (the under-W commit surfaces retryable again), the test fails
	// FAST with a re-run count instead of spinning for the production 30s.
	prevTO := cluster.SetTransactUnavailableTimeout(500 * time.Millisecond)
	defer cluster.SetTransactUnavailableTimeout(prevTO)
	prevBK := cluster.SetCASBaseBackoffZero()
	defer cluster.RestoreCASBaseBackoff(prevBK)

	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	key := []byte("durable")
	owner := ownsCASPinIndex(t, nodes, key)
	// Stop the gRPC of a co-replica (NOT the owner) so the owner commits
	// locally but the fan-out cannot reach W=3.
	stopACoReplica(nodes, owner)

	fnRuns := 0
	err := nodes[owner].cluster.Transact(key, func(tx backend.Transaction) error {
		fnRuns++
		if _, gerr := tx.Get(key); gerr != nil && !errors.Is(gerr, backend.ErrNotFound) {
			return gerr
		}
		return tx.Put(key, []byte("x"))
	})
	if err != nil {
		t.Fatalf("under-W WriteAll commit must SUCCEED (durable on owner); got %v", err)
	}
	if fnRuns != 1 {
		t.Fatalf("a committed-under-replicated commit must run fn exactly once (no re-run); ran %d", fnRuns)
	}
	// The write is durable on the owner's local backend immediately (no data
	// loss), stored as a stamped envelope.
	env, ok := decodeReplica(t, nodes[owner], key)
	if !ok || !bytes.Equal(env.Payload, []byte("x")) {
		t.Fatalf("under-replicated write must be durable on the owner; present=%v payload=%q", ok, env.Payload)
	}
}

// ownsCASPinIndex returns the index of the node that is the designated CAS
// owner of pinKey (steady state, after convergence). Fails if no single node
// claims it.
func ownsCASPinIndex(t *testing.T, nodes []*replicatedNode, pinKey []byte) int {
	t.Helper()
	owner := -1
	for i, n := range nodes {
		if n.cluster.OwnsCASPin(pinKey) {
			if owner != -1 {
				t.Fatalf("two nodes claim CAS ownership of %q (n%d and n%d)", pinKey, owner, i)
			}
			owner = i
		}
	}
	if owner == -1 {
		t.Fatalf("no node owns CAS pin %q", pinKey)
	}
	return owner
}

// stopACoReplica stops the gRPC server of the FIRST node that is not owner, so
// the owner's fan-out loses one ack while the owner itself stays up.
func stopACoReplica(nodes []*replicatedNode, owner int) {
	for i, n := range nodes {
		if i != owner {
			n.stopGRPC()
			return
		}
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
	// Spelled-out no-framing guard. The byte-equality above already implies
	// it, but stating the two framing signals explicitly keeps the intent
	// legible: an Encode'd envelope leads with the magic byte and is longer
	// than its payload by the header (magic + timestamp + nodeID length +
	// nodeID). Either one appearing here is the regression.
	if len(raw) > 0 && raw[0] == 0xE0 {
		t.Fatalf("R=1 stored bytes lead with the envelope magic byte 0xE0: %q", raw)
	}
	if len(raw) != len("rawval") {
		t.Fatalf("R=1 stored %d bytes, want %d (no envelope framing)", len(raw), len("rawval"))
	}
}

// TestCASReplicate_CommitSurvivesFutureStampedReadSet is the observed-
// stamp-ratchet regression test: a validated CAS commit must never lose
// the LWW compare to the very value it validated against and replaced,
// even when that stored value carries a stamp from a clock far AHEAD of
// the owner's.
//
// Setup: plant the SAME encoded envelope, stamped ~10 minutes in the
// future by a fictitious node, directly on EVERY replica's backend. Then
// run a CAS read-modify-write of that key through the cluster. Without
// the monotone stamp source + ratchet, the owner's commit stamp (raw
// wall clock) lands BELOW the planted stamp: the owner validates,
// commits locally (authoritative), acks - but every replica's apply-if-
// newer rejects the batch, a quorum read resurrects the OLD value, and
// read-repair pushes it back over the owner's committed write. The
// acked, validated commit is silently un-committed. With the ratchet,
// the validate decode Observes the planted stamp before the commit
// stamp is issued, so the commit stamp strictly exceeds it and the
// write survives everywhere.
func TestCASReplicate_CommitSurvivesFutureStampedReadSet(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	key := []byte("ratchet-k")
	future := uint64(time.Now().Add(10 * time.Minute).UnixNano())
	planted := cluster.Encode(cluster.Envelope{
		Stamp:   cluster.Stamp{TimestampNanos: future, NodeID: "future-node"},
		Payload: []byte("old"),
	})
	// Identical bytes on every replica: no laggards, so no read-repair
	// noise from the setup itself.
	for i, n := range nodes {
		if err := n.mem.Put(key, planted); err != nil {
			t.Fatalf("plant node %d: %v", i, err)
		}
	}

	// CAS read-modify-write through the cluster: read "old" (recording
	// it in the read-set), write "new".
	err := nodes[0].cluster.Transact(key, func(tx backend.Transaction) error {
		got, err := tx.Get(key)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, []byte("old")) {
			t.Errorf("tx read: got %q want old", got)
		}
		return tx.Put(key, []byte("new"))
	})
	if err != nil {
		t.Fatalf("Transact: %v", err)
	}

	// The validated commit must survive a quorum read. Pre-ratchet this
	// returns "old": the commit stamp lost LWW to the planted future
	// stamp on every non-owner replica, so the quorum winner is the old
	// value (and the read schedules the repair that un-commits the
	// owner's copy).
	got, err := nodes[1].cluster.Get(key)
	if err != nil {
		t.Fatalf("post-commit Get: %v", err)
	}
	if !bytes.Equal(got, []byte("new")) {
		t.Fatalf("post-commit Get: got %q want new (validated CAS commit lost LWW to the stamp it validated against)", got)
	}

	// And after replication + any read-repair settle, EVERY replica must
	// hold the new payload under a stamp strictly above the planted one.
	eachReplicaEventually(t, nodes, key, func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, []byte("new")) && env.Stamp.TimestampNanos > future
	})
}

// TestCASReplicate_CommitSurvivesFutureStampedBlindWrite is the BLIND-WRITE
// variant of the observed-stamp-ratchet regression test above: the commit-
// stamp floor must cover the whole WRITE-SET, not just the validated
// read-set. A WriteOp on a key the fn never Gets (a normal API shape) is
// tx.Put without decoding the stored envelope, so the validate loop never
// Observes its stored stamp. Without the write-set floor, planting a
// future-stamped envelope on such a key makes the owner draw a commit
// stamp BELOW the stored one: the owner commits and acks, every replica's
// apply-if-newer rejects that entry, a quorum read resurrects the old
// value, and read-repair pushes it back over the owner's copy - the acked
// commit is silently un-committed. The fix Observes the stored stamp of
// every blind write-set key inside the owner-local tx BEFORE the commit
// stamp is drawn, so the commit survives.
func TestCASReplicate_CommitSurvivesFutureStampedBlindWrite(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	key := []byte("blind-ratchet-k")
	future := uint64(time.Now().Add(10 * time.Minute).UnixNano())
	planted := cluster.Encode(cluster.Envelope{
		Stamp:   cluster.Stamp{TimestampNanos: future, NodeID: "future-node"},
		Payload: []byte("old"),
	})
	// Identical bytes on every replica: no laggards, so no read-repair
	// noise from the setup itself.
	for i, n := range nodes {
		if err := n.mem.Put(key, planted); err != nil {
			t.Fatalf("plant node %d: %v", i, err)
		}
	}

	// BLIND write through the cluster: the fn never Gets the key, so the
	// read-set is empty and the validate loop Observes nothing.
	err := nodes[0].cluster.Transact(key, func(tx backend.Transaction) error {
		return tx.Put(key, []byte("new"))
	})
	if err != nil {
		t.Fatalf("Transact: %v", err)
	}

	// The acked blind commit must survive a quorum read. Without the
	// write-set floor this returns "old": the commit stamp lost LWW to
	// the planted future stamp on every non-owner replica.
	got, err := nodes[1].cluster.Get(key)
	if err != nil {
		t.Fatalf("post-commit Get: %v", err)
	}
	if !bytes.Equal(got, []byte("new")) {
		t.Fatalf("post-commit Get: got %q want new (acked blind CAS commit lost LWW to the stored stamp it replaced)", got)
	}

	// And after replication + any read-repair settle, EVERY replica must
	// hold the new payload under a stamp strictly above the planted one.
	eachReplicaEventually(t, nodes, key, func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, []byte("new")) && env.Stamp.TimestampNanos > future
	})
}
