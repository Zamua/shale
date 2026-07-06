package cluster_test

// v0.7 LWW-on-write (apply-if-newer) tests. The READ side already resolved
// LWW (getReplicated picks the max-stamp winner; read-repair pushes it to
// laggards). v0.7 adds the WRITE-side rule: every replica-receiving write at
// R>1 is apply-if-newer - it persists an incoming envelope ONLY IF its stamp
// strictly beats the stored stamp (or there is no stored value). An older-or-
// equal incoming write is a silent no-op.
//
// These tests pin the behaviors that rule exists to deliver, in increasing
// order of directness:
//
//   - no lost update under ReadQuorum AND ReadAll (the headline bug: a stale
//     async read-repair clobbered the owner-local copy, so a later CAS
//     validate-and-apply read the stale value and missed a conflict);
//   - read-repair does not clobber a value a newer write already landed;
//   - apply-if-newer directly: an older incoming envelope is rejected, a
//     newer one wins, equal stamps are a no-op;
//   - reordered CAS fan-outs converge to the higher-stamp value on every
//     replica;
//   - R=1 takes none of this path (raw values, no envelope).
//
// The 3-node replicated harness (startThreeNodeReplicatedCluster) + the
// decode/seed/poll helpers (decodeReplica / seedConverged /
// eachReplicaEventually) live in replicate_test.go + cas_replicate_test.go
// (same cluster_test package); reused here. The CAS-lock-released-before-
// fan-out behavioral check is a white-box internal test in
// lww_on_write_internal_test.go (it injects a hung peer into the ring).

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryMigrationGuard runs op, retrying ONLY the bootstrap-rebalance
// migration-guard transient (codes.ResourceExhausted, "key is migrating
// out; retry after Nms"). A late-arriving gossip round can reopen a
// StateReceiving window on the legacy R>1 harness AFTER the fixture settled,
// and production replica-write callers treat that refusal as a transient
// (the fan-out counts it toward neither budget and the originator retries).
// These tests drive LocalReplicaPut / ApplyBatchLocal DIRECTLY, so they must
// retry the same class themselves or the reopened window fails them
// spuriously (the standing LWW flake). Semantics match the guard's own hint:
// sleep the retry-after the error carries (fallback 50ms, the package
// default), a small bounded attempt budget, and FAIL HARD on any other error
// or on exhaustion - the retry can never mask a real failure class.
func retryMigrationGuard(t *testing.T, op func() error) error {
	t.Helper()
	const maxAttempts = 20
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err = op()
		if err == nil {
			return nil
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.ResourceExhausted || !strings.Contains(st.Message(), "migrating") {
			return err // anything but the migration guard: surface immediately.
		}
		retryAfter := 50 * time.Millisecond
		if idx := strings.Index(st.Message(), "retry after"); idx >= 0 {
			var ms int
			if _, serr := fmt.Sscanf(st.Message()[idx:], "retry after %dms", &ms); serr == nil && ms > 0 {
				retryAfter = time.Duration(ms) * time.Millisecond
			}
		}
		time.Sleep(retryAfter)
	}
	return err // budget exhausted mid-migration: surface the guard error.
}

// runConcurrentIncrements seeds key=0 through the owner and fires `workers`
// concurrent CAS read-modify-write increments, all routed through the owner
// (every increment targets the same key so they share one owner). It returns
// the owner's cluster handle so callers can read the final value. The whole
// point is OCC under contention: if no update is lost, the final value is
// exactly `workers`. This is the workload that failed pre-v0.7 under
// ReadQuorum / ReadAll (a stale read-repair clobbered the owner-local copy,
// so a later validate-and-apply missed a conflict). It is shared by the
// ReadQuorum + ReadAll variants below so the two read modes run the exact
// same workload.
func runConcurrentIncrements(t *testing.T, nodes []*replicatedNode, key []byte, workers int) *cluster.Cluster {
	t.Helper()
	// Every increment targets ONE key, so all `workers` writers serialize on
	// that key's CAS. Under the race detector the contention window widens
	// enough that the default CASMaxAttempts (10) exhausts before a contended
	// writer wins - the increments ERROR OUT (caught at line ~85), they are
	// not silently lost, so this is a test-environment artifact, not a lost
	// update. Give the budget generous headroom and drop the inter-attempt
	// backoff so the retries resolve quickly. Restored at test end.
	oldMax, oldBackoff := cluster.CASMaxAttempts, cluster.SetCASBaseBackoffZero()
	cluster.CASMaxAttempts = 500
	t.Cleanup(func() { cluster.CASMaxAttempts = oldMax; cluster.RestoreCASBaseBackoff(oldBackoff) })
	owner := ownerIndex(nodes, key)
	if owner < 0 {
		t.Fatalf("no owner for %q", key)
	}
	oc := nodes[owner].cluster
	if err := transactPut(nodes, key, []byte("0")); err != nil {
		t.Fatalf("seed: %v", err)
	}

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
	return oc
}

// assertCounterConverged asserts the owner reads exactly `want` AND every
// replica eventually holds an envelope carrying that value (durability).
func assertCounterConverged(t *testing.T, nodes []*replicatedNode, oc *cluster.Cluster, key []byte, want int) {
	t.Helper()
	got, err := oc.Get(key)
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	if string(got) != fmt.Sprintf("%d", want) {
		t.Fatalf("counter: got %q want %d (lost update: a stale read-repair clobbered the owner-local copy and a CAS commit missed the conflict)", got, want)
	}
	wantBytes := fmt.Appendf(nil, "%d", want)
	eachReplicaEventually(t, nodes, key, func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, wantBytes)
	})
}

// TestLWWOnWrite_NoLostUpdate_ReadQuorum is the headline regression for the
// apply-if-newer fix. N concurrent CAS increments of one counter at R=3 with
// ReadConsistency=ReadQuorum must reach EXACTLY N on the owner and on every
// replica. Under ReadQuorum the OCC read schedules an async read-repair on
// every Get; pre-v0.7 a repair scheduled with the counter at V could fire
// AFTER a later commit landed V+1 on the owner-local backend, clobbering it
// back to V, so the next validate-and-apply read the stale V on the owner's
// LOCAL copy and MISSED the conflict (observed: 17-19 instead of 20). Apply-
// if-newer rejects the stale repair (older stamp), so the count is exactly N.
func TestLWWOnWrite_NoLostUpdate_ReadQuorum(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)
	const workers = 20
	key := []byte("counter")
	oc := runConcurrentIncrements(t, nodes, key, workers)
	assertCounterConverged(t, nodes, oc, key, workers)
}

// TestLWWOnWrite_NoLostUpdate_ReadAll runs the identical workload under
// ReadAll. ReadAll waits for every live replica and ALSO seeds read-repair on
// disagreement, so the same stale-repair-clobbers hazard exists; apply-if-
// newer must close it here too. Pinning both ReadQuorum and ReadAll proves
// the fix is not specific to one read mode - any read mode that schedules
// read-repair is covered.
func TestLWWOnWrite_NoLostUpdate_ReadAll(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadAll)
	const workers = 20
	key := []byte("counter")
	oc := runConcurrentIncrements(t, nodes, key, workers)
	assertCounterConverged(t, nodes, oc, key, workers)
}

// TestLWWOnWrite_ReadRepairDoesNotClobber constructs the stale-read-then-
// newer-write ordering the apply-if-newer rule was built to defuse, and
// asserts the newer value survives on every replica even when the stale
// repair fires LATE (after the newer write landed).
//
// Read-repair dispatches the winner envelope it computed AT READ TIME through
// the replica-receiving write path (scheduleReadRepair -> dispatchReplicaPut
// -> applyEnvelopeIfNewer). LocalReplicaPut hits that exact path in-process,
// so we drive a deterministic "the repair fires late" by:
//
//  1. Seed k=v1 on every replica; capture v1's stamped envelope bytes (this
//     is precisely what a read-repair scheduled while the cluster held v1
//     would carry as the "winner").
//  2. Write a strictly-later k=v2 (stamp s2 > s1) so every replica now holds
//     v2.
//  3. Replay the captured v1 envelope as a LATE read-repair against EVERY
//     replica (LocalReplicaPut, the read-repair dispatch path).
//  4. Assert every replica still holds v2: apply-if-newer rejected the stale
//     repair (s1 < s2 loses the compare), so the newer write is never
//     clobbered. Pre-v0.7 the verbatim replica write would have reverted each
//     replica to v1.
//
// We capture v1's bytes by reading them off node[0]'s raw backend after the
// seed (the seed left the v1 envelope on every replica), which is exactly the
// envelope a real read-repair would have built from a v1-era quorum read.
func TestLWWOnWrite_ReadRepairDoesNotClobber(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	key := []byte("rr-clobber")
	seedConverged(t, nodes, 0, key, []byte("v1"))

	// Capture the v1 envelope bytes (the winner a v1-era read-repair carries).
	v1Bytes, err := nodes[0].mem.Get(key)
	if err != nil {
		t.Fatalf("capture v1 envelope: %v", err)
	}
	v1Env, derr := cluster.Decode(v1Bytes)
	if derr != nil || !bytes.Equal(v1Env.Payload, []byte("v1")) {
		t.Fatalf("captured envelope not v1: %+v %q err %v", v1Env.Stamp, v1Env.Payload, derr)
	}

	// Write a strictly-later v2 (s2 > s1). The 3ms sleep guarantees the wall-
	// clock stamp advances (nanosecond-resolution comparator; the memory
	// backend is fast enough that two writes could otherwise collide on the
	// same nanosecond and resolve on the NodeID tiebreak). v2 fans out to all
	// replicas under WriteAll.
	time.Sleep(3 * time.Millisecond)
	if err := nodes[0].cluster.Put(key, []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	// Wait for v2 to land on every replica before replaying the stale repair,
	// so the repair is unambiguously LATE.
	eachReplicaEventually(t, nodes, key, func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, []byte("v2"))
	})

	// Replay the captured v1 envelope as a LATE read-repair against EVERY
	// replica. This is the exact path scheduleReadRepair uses (dispatchReplica-
	// Put -> applyEnvelopeIfNewer). Each must be a committed no-op: the stale
	// stamp loses the apply-if-newer compare.
	for i, n := range nodes {
		if err := n.cluster.LocalReplicaPut(key, v1Bytes); err != nil {
			t.Fatalf("node %d late repair replay (should be a no-op, not an error): %v", i, err)
		}
	}

	// Every replica must STILL hold v2: the stale repair was rejected.
	for i, n := range nodes {
		env, ok := decodeReplica(t, n, key)
		if !ok {
			t.Fatalf("node %d: key vanished", i)
		}
		if !bytes.Equal(env.Payload, []byte("v2")) {
			t.Fatalf("node %d: payload %q want v2 (stale late read-repair clobbered the newer write)", i, env.Payload)
		}
	}

	// And the cluster Get must resolve to v2 from every node.
	for i, n := range nodes {
		v, err := n.cluster.Get(key)
		if err != nil {
			t.Fatalf("node %d post Get: %v", i, err)
		}
		if !bytes.Equal(v, []byte("v2")) {
			t.Errorf("node %d Get: got %q want v2", i, v)
		}
	}
}

// TestLWWOnWrite_ApplyIfNewer_OlderRejectedNewerWins exercises the apply-if-
// newer rule directly through the replica-receiving single-key Put path
// (LocalReplicaPut, which routes through applyEnvelopeIfNewer at R>1). It
// builds envelopes with hand-chosen stamps so the ordering is exact, with no
// dependence on wall-clock timing:
//
//	s2 (newer) lands first  -> stored.
//	s1 (older) applied next -> REJECTED (no-op); stored stays s2.
//	s3 (newest) applied     -> WINS; stored becomes s3.
//	s3 re-applied (equal)   -> no-op (Greater is strict).
//
// This is the core invariant in isolation: the comparator gates the write,
// arrival order is irrelevant.
func TestLWWOnWrite_ApplyIfNewer_OlderRejectedNewerWins(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	// Pick a key this node is a replica of so LocalReplicaPut's OwnsReplica-
	// gated path is the legitimate receiving path. With R=3 + 3 members every
	// node is a replica of every key, so node[0] qualifies for any key.
	key := []byte("aifn")
	n := nodes[0]
	if !n.cluster.OwnsReplica(key) {
		t.Fatalf("node[0] should be a replica of %q at R=3/3-members", key)
	}

	mkEnv := func(ts uint64, payload string) []byte {
		return cluster.Encode(cluster.Envelope{
			Stamp:   cluster.Stamp{TimestampNanos: ts, NodeID: "writer"},
			Payload: []byte(payload),
		})
	}
	s1, s2, s3 := uint64(1000), uint64(2000), uint64(3000)

	// s2 lands first (no stored value -> apply).
	if err := retryMigrationGuard(t, func() error { return n.cluster.LocalReplicaPut(key, mkEnv(s2, "val-s2")) }); err != nil {
		t.Fatalf("apply s2: %v", err)
	}
	if env, ok := decodeReplica(t, n, key); !ok || env.Stamp.TimestampNanos != s2 || !bytes.Equal(env.Payload, []byte("val-s2")) {
		t.Fatalf("after s2: stored %+v / %q want stamp %d payload val-s2", env.Stamp, env.Payload, s2)
	}

	// s1 < s2: REJECTED. Stored must stay s2.
	if err := retryMigrationGuard(t, func() error { return n.cluster.LocalReplicaPut(key, mkEnv(s1, "val-s1-stale")) }); err != nil {
		t.Fatalf("apply s1 (should be a committed no-op, not an error): %v", err)
	}
	if env, ok := decodeReplica(t, n, key); !ok || env.Stamp.TimestampNanos != s2 || !bytes.Equal(env.Payload, []byte("val-s2")) {
		t.Fatalf("after stale s1: stored %+v / %q want UNCHANGED stamp %d payload val-s2 (older write must be a no-op)", env.Stamp, env.Payload, s2)
	}

	// s3 > s2: WINS.
	if err := retryMigrationGuard(t, func() error { return n.cluster.LocalReplicaPut(key, mkEnv(s3, "val-s3")) }); err != nil {
		t.Fatalf("apply s3: %v", err)
	}
	if env, ok := decodeReplica(t, n, key); !ok || env.Stamp.TimestampNanos != s3 || !bytes.Equal(env.Payload, []byte("val-s3")) {
		t.Fatalf("after s3: stored %+v / %q want stamp %d payload val-s3 (newer write must win)", env.Stamp, env.Payload, s3)
	}

	// s3 re-applied (EQUAL stamp): no-op (A.Greater(A) == false). A re-
	// delivered identical envelope leaves the stored value intact.
	if err := retryMigrationGuard(t, func() error { return n.cluster.LocalReplicaPut(key, mkEnv(s3, "val-s3-redelivered")) }); err != nil {
		t.Fatalf("re-apply equal s3: %v", err)
	}
	if env, ok := decodeReplica(t, n, key); !ok || env.Stamp.TimestampNanos != s3 || !bytes.Equal(env.Payload, []byte("val-s3")) {
		t.Fatalf("after equal-stamp re-deliver: stored %+v / %q want UNCHANGED val-s3 (equal stamp is a no-op)", env.Stamp, env.Payload)
	}
}

// TestLWWOnWrite_ApplyIfNewer_ViaApplyBatch mirrors the directness test above
// but through the CAS write-set fan-out path (ApplyBatchLocal): an older batch
// must self-resolve to a no-op per key, a newer batch must win. This is the
// path CommitCASApply's post-commit fan-out uses, and the property
// (reordered older batch is a no-op) is exactly what lets the owner release
// casCommitMu before fanning out.
func TestLWWOnWrite_ApplyIfNewer_ViaApplyBatch(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadNearest)

	key := []byte("aifn-batch")
	n := nodes[0]
	mkEnv := func(ts uint64, payload string) []byte {
		return cluster.Encode(cluster.Envelope{
			Stamp:   cluster.Stamp{TimestampNanos: ts, NodeID: "writer"},
			Payload: []byte(payload),
		})
	}
	s1, s2 := uint64(5000), uint64(9000)

	// Newer batch (s2) first.
	if err := retryMigrationGuard(t, func() error {
		return n.cluster.ApplyBatchLocal([]cluster.EnvelopeWrite{{Key: key, Envelope: mkEnv(s2, "batch-s2")}})
	}); err != nil {
		t.Fatalf("ApplyBatch s2: %v", err)
	}
	if env, _ := decodeReplica(t, n, key); env.Stamp.TimestampNanos != s2 {
		t.Fatalf("after batch s2: stamp %d want %d", env.Stamp.TimestampNanos, s2)
	}

	// Older batch (s1) arrives reordered: REJECTED per key, committed no-op.
	if err := retryMigrationGuard(t, func() error {
		return n.cluster.ApplyBatchLocal([]cluster.EnvelopeWrite{{Key: key, Envelope: mkEnv(s1, "batch-s1-stale")}})
	}); err != nil {
		t.Fatalf("ApplyBatch s1 (should be a no-op, not an error): %v", err)
	}
	if env, _ := decodeReplica(t, n, key); env.Stamp.TimestampNanos != s2 || !bytes.Equal(env.Payload, []byte("batch-s2")) {
		t.Fatalf("after reordered older batch: stored stamp %d payload %q want UNCHANGED s2/batch-s2", env.Stamp.TimestampNanos, env.Payload)
	}
}

// TestLWWOnWrite_ReorderedCASFanOut_Converges pins requirement (4): two CAS
// commits to the SAME key whose fan-outs arrive OUT OF ORDER at a replica
// still converge to the higher-stamp value on every replica. Because the
// owner releases casCommitMu before fanning out (v0.7), the earlier commit's
// fan-out can in principle land at a replica AFTER the later commit's. We
// force that reordering deterministically:
//
//  1. CAS-commit k=first (shared stamp sA); capture first's envelope batch
//     off a replica backend (exactly what first's fan-out carries).
//  2. CAS-commit k=second (shared stamp sB > sA); now every replica holds
//     second.
//  3. Replay first's captured batch LATE via ApplyBatchLocal on every replica
//     (simulating first's fan-out arriving after second's).
//  4. Assert every replica still holds second: apply-if-newer rejected the
//     reordered older batch per key (sA < sB loses). Pre-v0.7 the verbatim
//     batch write would have reverted every replica to first.
func TestLWWOnWrite_ReorderedCASFanOut_Converges(t *testing.T) {
	nodes := startThreeNodeReplicatedCluster(t, 3, cluster.WriteAll, cluster.ReadQuorum)

	key := []byte("reorder-cas")

	// First CAS commit: k=first (stamp sA). Capture its envelope bytes.
	if err := transactPut(nodes, key, []byte("first")); err != nil {
		t.Fatalf("CAS commit first: %v", err)
	}
	eachReplicaEventually(t, nodes, key, func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, []byte("first"))
	})
	firstBytes, err := nodes[0].mem.Get(key)
	if err != nil {
		t.Fatalf("capture first envelope: %v", err)
	}

	// Strictly-later wall clock so the second commit's shared stamp sB > sA.
	time.Sleep(3 * time.Millisecond)
	// Second CAS commit: k=second (stamp sB > sA).
	if err := transactPut(nodes, key, []byte("second")); err != nil {
		t.Fatalf("CAS commit second: %v", err)
	}
	eachReplicaEventually(t, nodes, key, func(env cluster.Envelope, present bool) bool {
		return present && bytes.Equal(env.Payload, []byte("second"))
	})

	// Replay first's batch LATE on every replica (the reordered fan-out). This
	// is the path replicateCASBatch dispatches through (ApplyBatch ->
	// ApplyBatchLocal). Each must be a committed no-op per key.
	reorderedBatch := []cluster.EnvelopeWrite{{Key: key, Envelope: firstBytes}}
	for i, n := range nodes {
		if err := retryMigrationGuard(t, func() error { return n.cluster.ApplyBatchLocal(reorderedBatch) }); err != nil {
			t.Fatalf("node %d reordered batch replay (should be a no-op, not an error): %v", i, err)
		}
	}

	// Every replica must STILL hold the higher-stamp value "second".
	for i, n := range nodes {
		env, ok := decodeReplica(t, n, key)
		if !ok {
			t.Fatalf("node %d: key vanished", i)
		}
		if !bytes.Equal(env.Payload, []byte("second")) {
			t.Fatalf("node %d: payload %q want second (reordered earlier fan-out clobbered the later commit)", i, env.Payload)
		}
	}

	// Cluster Get resolves to the later value on every node.
	for i, n := range nodes {
		got, err := n.cluster.Get(key)
		if err != nil {
			t.Fatalf("node %d Get: %v", i, err)
		}
		if !bytes.Equal(got, []byte("second")) {
			t.Errorf("node %d Get: got %q want second", i, got)
		}
	}
}

// TestLWWOnWrite_R1_NoEnvelopeNoApplyIfNewer pins requirement (6): at R=1 the
// apply-if-newer path is NOT taken. A single-node forwarded replica write
// stores RAW bytes (no envelope framing), exactly as the pre-v0.7 R=1 path
// did, and a second write with "older" semantics still overwrites verbatim
// (there is no stamp comparison at R=1). A regression that wrapped R=1 writes
// in envelopes or gated them on a stamp would corrupt every single-node
// deploy.
func TestLWWOnWrite_R1_NoEnvelopeNoApplyIfNewer(t *testing.T) {
	c := newSingleNode(t)

	// A forwarded replica write at R=1 stores raw bytes.
	if err := c.LocalReplicaPut([]byte("k"), []byte("raw1")); err != nil {
		t.Fatalf("LocalReplicaPut raw1: %v", err)
	}
	raw, err := c.LocalGet([]byte("k"))
	if err != nil {
		t.Fatalf("LocalGet: %v", err)
	}
	if !bytes.Equal(raw, []byte("raw1")) {
		t.Fatalf("R=1 stored %q want raw raw1 (envelope leaked into R=1 path?)", raw)
	}
	if len(raw) != len("raw1") {
		t.Fatalf("R=1 stored %d bytes want %d (no envelope framing)", len(raw), len("raw1"))
	}

	// A second raw write overwrites verbatim: there is no stamp, so no apply-
	// if-newer gate could reject it. (At R>1 an "older" write would be a no-
	// op; at R=1 every write lands.)
	if err := c.LocalReplicaPut([]byte("k"), []byte("raw2")); err != nil {
		t.Fatalf("LocalReplicaPut raw2: %v", err)
	}
	raw2, err := c.LocalGet([]byte("k"))
	if err != nil {
		t.Fatalf("LocalGet 2: %v", err)
	}
	if !bytes.Equal(raw2, []byte("raw2")) {
		t.Fatalf("R=1 second write: stored %q want raw2 (R=1 must overwrite verbatim, no stamp gate)", raw2)
	}
}
