package cluster

// White-box test for the CAS fence-recode (the graceful-leave scale-down gap for
// the optimistic-concurrency write path). It mirrors
// multibackend_fence_recode_internal_test.go's fenceAtBackend / fenceAtTx doubles
// (Begin succeeds; a chosen tx op - Get / Put / Commit - returns a %w-wrapped
// backend.ErrFenced, exactly as real slatedb surfaces a fence on a now-stale
// writer AFTER Begin), but drives the fence through CommitCASApply (the owner-side
// CAS validate-and-apply) instead of the single-key Put fan-out.
//
// The contract: a FENCED owner-local CAS apply must NOT hard-fail the client. It
// recodes to the TRANSIENT acquiring-window error (codes.Unavailable) which the
// CAS retry classifier (commitRetryable) treats as retryable, so Transact re-runs
// fn against the re-resolved owner (the successor, once it serves). Left raw, the
// fence is a hard failure: commitRetryable returns false for a multi-%w-wrapped
// backend.ErrFenced (status.FromError ok == false on it), so Transact gives up.

import (
	"context"
	"errors"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
)

// keyResolvingToOwnedMount brute-forces a key whose write resolves, on this node,
// to an OWNED + MOUNTED replica position (the pin unit a CAS commit applies
// against). It returns the key and the resolved position. Fails the test if no
// such key is found in a generous probe budget (the ring fixture is broken).
func keyResolvingToOwnedMount(t *testing.T, c *Cluster) []byte {
	t.Helper()
	for i := 0; i < 4096; i++ {
		key := []byte{byte(i), byte(i >> 8), 'k'}
		if !c.OwnsKey(key) {
			continue
		}
		_, _, unlock, ok := c.localWriteBackendForKey(key)
		unlock()
		if ok {
			return key
		}
	}
	t.Fatalf("no key resolved to an owned+mounted position; ring/mount fixture broken")
	return nil
}

// TestCommitCASApply_FencedAtAnyOp_RecodedToTransient pins the CAS scale-down
// gap fix: a CAS owner-local commit whose read-set Get, write-set Put, OR Commit
// is FENCED (real slatedb surfaces the fence at whichever op runs first against a
// stale writer, AFTER a successful Begin) must surface a RETRYABLE result, not the
// raw fence. The raw fence is NOT retryable to the CAS classifier, so Transact
// would give up and hard-fail the client; the recoded acquiring-window error
// (codes.Unavailable) makes commitRetryable true so Transact re-runs the closure.
//
// The in-memory fence double (fencedReplicaBackend, used elsewhere) fences at
// Begin and cannot reach these surfaces, so this reuses the post-Begin fenceAtTx
// double from multibackend_fence_recode_internal_test.go.
func TestCommitCASApply_FencedAtAnyOp_RecodedToTransient(t *testing.T) {
	for _, at := range []string{"get", "put", "commit"} {
		t.Run(at, func(t *testing.T) {
			backing := sharedfactory.NewBacking()
			c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3", "n4")

			// Seed every position self owns with a FENCED mount: Begin succeeds, the
			// chosen tx op returns a %w-wrapped backend.ErrFenced. Whichever owned
			// position the pin key resolves to is therefore a fenced mount.
			desired := c.desiredReplicaUnits()
			if len(desired) == 0 {
				t.Fatalf("self desires no positions; ring fixture broken")
			}
			for _, target := range desired {
				c.mountMap[target] = fenceAtBackend{Backend: memory.New(), at: at}
			}

			pinKey := keyResolvingToOwnedMount(t, c)

			// A read-then-write CAS commit: the read-set Get exercises the "get"
			// fence; the write-set Put exercises "put"; the final flush exercises
			// "commit". ExpectAbsent on the read so a missing/decoded value is not a
			// conflict (the fence, not a conflict, is what we are pinning).
			reads := []backend.ReadCheck{{Key: pinKey, ExpectAbsent: true}}
			writes := []backend.WriteOp{{Key: pinKey, Value: []byte("v")}}

			res := c.CommitCASApply(context.Background(), backend.SnapshotIsolation, pinKey, reads, writes)

			if res.Committed {
				t.Fatalf("fence at %s: commit reported Committed, but a fenced apply did NOT durably commit", at)
			}
			if res.Conflict {
				t.Fatalf("fence at %s: commit reported Conflict, but a fence is a transient retry, not an OCC conflict", at)
			}
			if res.Err == nil {
				t.Fatalf("fence at %s: a fenced CAS apply must surface an error, got nil", at)
			}
			if errors.Is(res.Err, backend.ErrFenced) {
				t.Fatalf("fence at %s: leaked the RAW fence to the client (a HARD, non-retryable failure); got %v", at, res.Err)
			}
			if !isAcquiringErr(res.Err) {
				t.Fatalf("fence at %s: must recode to the TRANSIENT acquiring-window error so Transact retries; got %v", at, res.Err)
			}
			if !commitRetryable(res.Err) {
				t.Fatalf("fence at %s: the recoded error must be classified RETRYABLE by the CAS retry loop; got %v", at, res.Err)
			}
		})
	}
}

// TestCommitRetryable_RawFence_IsRetryable pins the belt-and-suspenders guard in
// commitRetryable: a multi-%w-wrapped backend.ErrFenced that reaches the
// classifier WITHOUT a gRPC status (status.FromError ok == false on it) is still
// classified retryable. This is the second net behind the owner-local recode, so
// a raw fence that ever bypasses casFenceToTransient still does not hard-fail the
// client.
func TestCommitRetryable_RawFence_IsRetryable(t *testing.T) {
	raw := errors.New("outer: middle: " + backend.ErrFenced.Error())
	// Build a genuine multi-%w wrap so errors.Is resolves through it.
	wrapped := errors.Join(errors.New("CAS owner-local apply"), backend.ErrFenced)
	if !commitRetryable(wrapped) {
		t.Fatalf("a multi-wrapped backend.ErrFenced must be retryable; got not-retryable")
	}
	// Sanity: a plain non-fence, non-status error is NOT retryable.
	if commitRetryable(raw) {
		t.Fatalf("a plain error string (no fence sentinel, no status) must NOT be retryable")
	}
}
