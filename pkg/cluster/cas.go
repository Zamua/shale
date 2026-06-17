package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// casReplicated reports whether this cluster stores CAS write-sets as LWW
// envelopes and fans them out (R>1), versus the raw-value single-owner
// path (R=1). It is the SAME condition Put / Get / Delete use to choose
// the replicated path (replicationFactor > 1 with a populated ring), so a
// CAS commit and a single-key write agree on the storage format for the
// same shard. At R=1 the whole envelope / fan-out machinery is bypassed
// and CommitCASApply behaves exactly as v0.6 did.
func (c *Cluster) casReplicated() bool {
	return c.replicationFactor() > 1 && c.ring != nil && !c.ring.Empty()
}

// CASMaxAttempts bounds how many times Transact re-runs its closure on a
// CAS conflict before giving up. A converging workload finishes in 1-2
// attempts; a key under sustained contention may need a handful. Exposed
// as a var (not const) so tests can shrink / grow it. Past this budget
// Transact returns backend.ErrCASConflict (the caller cannot make
// progress either way; the sentinel is the same).
var CASMaxAttempts = 10

// casBaseBackoff is the first inter-attempt sleep on a conflict; each
// retry sleeps a randomized multiple of it (jitter avoids a herd of
// contending clients all re-reading + re-committing in lockstep). var so
// tests can zero it for speed.
var casBaseBackoff = 2 * time.Millisecond

// transactUnavailableTimeout bounds how long Transact keeps re-running its
// closure through a retryable codes.Unavailable commit (the cluster is briefly
// write-frozen for a reshard, or the pin unit's lease is mid-handoff) before
// surfacing the Unavailable to the caller. Generous enough to ride out a
// cluster-wide freeze (bounded by the reshard's per-phase timeout). var so
// tests can shrink it.
var transactUnavailableTimeout = 30 * time.Second

// CommitCASApply is the owner-side validate-and-apply: the heart of the
// CAS protocol. It runs fully synchronously and owner-local, against a
// single short backend.Transaction that opens and commits without any
// network round-trip inside it.
//
// Contract (see docs/SPEC.md "Owner-side validate-and-apply"):
//  1. Ownership: the local node must own the pin key. The gRPC handler
//     and the in-process fast-path both check this BEFORE calling;
//     CommitCASApply trusts the gate (the same way LocalReplicaPut trusts
//     OwnsReplica) and does NOT re-check ownership per read/write key.
//     The client's cross-shard guard already ensured every key shards to
//     the same owner as the pin key.
//  2. Open ONE local transaction at the requested isolation level. A
//     deferred Rollback is armed immediately, guarded by a committed flag
//     so a successful Commit does not double-finalize and a failed Commit
//     (which is not guaranteed to have finalized the tx) still rolls back.
//  3. Validate every read-check against the tx. ExpectAbsent: a found
//     value is a conflict. Otherwise: a not-found OR a value that does
//     not match ExpectedVal byte-for-byte is a conflict. On the first
//     conflict, stop, let the deferred Rollback run, return {Conflict}.
//  4. Apply every write-op in order (Put / Delete). A backend error here
//     returns {Err} (deferred Rollback runs).
//  5. Commit. On commit error, do NOT mark committed (deferred Rollback
//     runs) and return {Err}. On success, mark committed (suppressing the
//     deferred Rollback) and return {Committed}.
//
// A cancelled ctx (client disconnect, deadline) propagates: the deferred
// Rollback runs and the transaction did not happen. The validate-and-
// apply is one goroutine against one (non-goroutine-safe)
// backend.Transaction, so there is no concurrency to coordinate.
//
// Replication (v0.6.x, R>1). At ReplicationFactor > 1 the Backend stores
// LWW Envelope bytes, not raw values (the same split single-key Put / Get
// use). CommitCASApply then differs from the R=1 path two ways: it
// DECODES the stored envelope before each read-check compare (the
// client's ExpectedVal is a decoded payload; a tombstone counts as not-
// found), and it ENCODES each write-op into an Envelope under ONE shared
// Stamp{now, owner NodeID} before tx.Put (Delete becomes an empty-payload
// tombstone Put, NOT tx.Delete, so the LWW comparator on a replica sees a
// stamped removal). After the local commit succeeds it fans the SAME
// encoded envelopes out to the R-1 other replicas via ApplyBatch, waiting
// for W total acks (the owner's local commit is one of them). An under-W
// fan-out returns {Err: codes.Unavailable} but the write is ALREADY
// durable on the owner + any replica that acked: same best-effort-to-W
// shape single-key Put has. See docs/SPEC.md "Write-set replication".
//
// pinKey anchors the replica-set lookup; every key in the commit co-
// shards with it (the client's cross-shard guard guarantees this), so one
// replica set covers the whole write-set. It is unused at R=1.
func (c *Cluster) CommitCASApply(ctx context.Context, level backend.IsolationLevel, pinKey []byte, reads []backend.ReadCheck, writes []backend.WriteOp) backend.CASResult {
	if c.notReady() {
		return backend.CASResult{Err: backend.ErrClosed}
	}

	replicated := c.casReplicated()

	// Serialize the validate-and-apply against other CAS commits on this
	// node so the read-set check + write-set apply are atomic (no lost
	// update; see Cluster.casCommitMu). The lock covers ONLY validate +
	// the owner-local Commit, NOT the post-commit fan-out at R>1: the
	// owner-local commit establishes OCC order, and once a replica apply
	// is apply-if-newer (ApplyBatchLocal), a reordered fan-out
	// self-resolves on each replica (an older batch is a no-op). So the
	// fan-out no longer needs the lock to keep competing commits' write-
	// sets in order. Releasing before replicateCASBatch restores the "no
	// lock held across the network" property CAS was chosen for. We
	// unlock explicitly right after the local commit rather than via
	// defer so the network fan-out runs lock-free.
	c.casCommitMu.Lock()
	casMuHeld := true
	defer func() {
		if casMuHeld {
			c.casCommitMu.Unlock()
		}
	}()

	tx, err := c.localBeginForKey(pinKey, level)
	if err != nil {
		return backend.CASResult{Err: err}
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Bail out early if the caller's context is already done (disconnect
	// before we touch the backend). The deferred Rollback handles the
	// just-opened tx.
	if err := ctx.Err(); err != nil {
		return backend.CASResult{Err: err}
	}

	// Validate the read-set. At R>1 the stored value is an Envelope; the
	// client's ExpectedVal is the DECODED payload it saw via getReplicated,
	// so we decode before comparing and treat a tombstone (empty payload)
	// as not-found. At R=1 readPayload returns the raw bytes unchanged, so
	// this loop is byte-for-byte the v0.6 behavior.
	for _, r := range reads {
		got, found, err := c.casReadPayload(tx, r.Key, replicated)
		if r.ExpectAbsent {
			if err != nil {
				return backend.CASResult{Err: c.casFenceToTransient(tx, "CommitCASApply", err)}
			}
			if found {
				return backend.CASResult{Conflict: true}
			}
			continue
		}
		if err != nil {
			return backend.CASResult{Err: c.casFenceToTransient(tx, "CommitCASApply", err)}
		}
		if !found {
			// Client observed a value; key is now absent (or a tombstone):
			// conflict.
			return backend.CASResult{Conflict: true}
		}
		if !bytesEqual(got, r.ExpectedVal) {
			return backend.CASResult{Conflict: true}
		}
	}

	// Apply the write-set in order. At R>1 each op is encoded into an
	// Envelope under ONE shared commit stamp and written as a tx.Put (a
	// Delete becomes an empty-payload tombstone Put). At R=1 the raw
	// tx.Put / tx.Delete path is unchanged. encodedWrites is the batch
	// fanned out after the commit (nil at R=1).
	var stamp Stamp
	var encodedWrites []EnvelopeWrite
	if replicated {
		stamp = Stamp{
			TimestampNanos: uint64(time.Now().UnixNano()),
			NodeID:         c.cfg.NodeID,
		}
		encodedWrites = make([]EnvelopeWrite, 0, len(writes))
	}
	for _, w := range writes {
		if !replicated {
			if w.Del {
				if err := tx.Delete(w.Key); err != nil {
					return backend.CASResult{Err: c.casFenceToTransient(tx, "CommitCASApply", err)}
				}
				continue
			}
			if err := tx.Put(w.Key, w.Value); err != nil {
				return backend.CASResult{Err: c.casFenceToTransient(tx, "CommitCASApply", err)}
			}
			continue
		}
		// R>1: encode an envelope (Put => payload=value, Delete => empty-
		// payload tombstone) under the shared stamp and Put it. A Delete is
		// NOT tx.Delete: a bare key-removal would lose LWW to a stale
		// stamped value on another replica.
		var payload []byte
		if !w.Del {
			payload = append([]byte(nil), w.Value...)
		}
		envBytes := Encode(Envelope{Stamp: stamp, Payload: payload})
		if err := tx.Put(w.Key, envBytes); err != nil {
			return backend.CASResult{Err: c.casFenceToTransient(tx, "CommitCASApply", err)}
		}
		encodedWrites = append(encodedWrites, EnvelopeWrite{
			Key:      append([]byte(nil), w.Key...),
			Envelope: envBytes,
		})
	}

	if err := tx.Commit(); err != nil {
		return backend.CASResult{Err: c.casFenceToTransient(tx, "CommitCASApply", err)}
	}
	committed = true

	// The owner-local commit is done and has established this commit's OCC
	// order under casCommitMu. Release the lock NOW, before the network
	// fan-out: the fan-out is order-independent because each replica's
	// ApplyBatchLocal is apply-if-newer (a reordered older batch is a
	// no-op there), so holding casCommitMu across the network is no longer
	// needed. This restores the "no lock held across the network"
	// property. Concurrent CAS commits on this node can now validate +
	// commit locally while this one's fan-out is in flight.
	c.casCommitMu.Unlock()
	casMuHeld = false

	// At R>1, the write-set is now durable on the owner. Fan the SAME
	// encoded envelopes out to the R-1 other replicas, waiting for W total
	// acks (owner's local commit pre-counted as 1). An under-W result is
	// {Err: codes.Unavailable}: the write is already durable on the owner
	// + any replica that acked, the same best-effort-to-W model single-key
	// Put has. A read-only commit (no writes) has nothing to replicate.
	if replicated && len(encodedWrites) > 0 {
		if err := c.replicateCASBatch(pinKey, encodedWrites); err != nil {
			return backend.CASResult{Err: err}
		}
	}
	return backend.CASResult{Committed: true}
}

// casReadPayload reads key from the owner-local tx for a CAS read-check.
// At R>1 it decodes the stored Envelope and returns the payload, with
// found=false for both a missing key AND a winning tombstone (empty
// payload), so a tombstone satisfies ExpectAbsent and conflicts a value-
// match check. At R=1 it returns the raw stored bytes unchanged (found is
// simply "the key exists"), preserving the v0.6 byte-for-byte semantics.
// A corrupt envelope at R>1 surfaces as a backend error (the commit
// reports {Err}, not a silent conflict).
func (c *Cluster) casReadPayload(tx backend.Transaction, key []byte, replicated bool) (payload []byte, found bool, err error) {
	got, err := tx.Get(key)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !replicated {
		return got, true, nil
	}
	env, derr := Decode(got)
	if derr != nil {
		return nil, false, derr
	}
	if len(env.Payload) == 0 {
		// Winning tombstone: the key the client saw is gone. Treat as
		// not-found for both ExpectAbsent and value-match checks.
		return nil, false, nil
	}
	return env.Payload, true, nil
}

// casFenceToTransient recodes a FENCED owner-local CAS apply error to the
// TRANSIENT acquiring-window error, mirroring the single-key Put fan-out's
// fenceToTransient. When a graceful leave is in flight, a successor opens the
// shared slatedb at a higher epoch and FENCES this leaver's owner-local CAS
// writer; the fence surfaces as backend.ErrFenced on a tx Get / Put / Commit
// (AFTER a successful Begin, on real slatedb). Left raw, that fence is a HARD
// failure the CAS retry classifier (commitRetryable) does not treat as
// retryable, so Transact gives up and the CAS write hard-fails the client. This
// recode evicts the stale mount and returns errUnitAcquiring (codes.Unavailable)
// instead, which commitRetryable DOES treat as retryable, so Transact re-runs fn
// (re-validating the read-set on the re-resolved owner, the successor once it
// serves). A fenced commit did NOT durably apply, so recoding it retryable (not
// counting it applied) is correct; the OCC re-read makes the retry idempotent
// (no double-apply, no lost update).
//
// ru + b come from the pin tx's *pausedTx wrapper (localBeginForKey, multi mode).
// On the R=1 / non-multi path the tx is a bare backend.Transaction (no pausedTx,
// no mounted-unit epoch handoff), so the assert fails and the raw error passes
// through unchanged - the v0.6 behavior. A non-fence error also passes through
// unchanged (it stays a hard failure).
func (c *Cluster) casFenceToTransient(tx backend.Transaction, op string, err error) error {
	pt, ok := tx.(*pausedTx)
	if !ok {
		return err
	}
	return c.fenceToTransient(pt.ru, pt.b, op, err)
}

// commitRetryable reports whether a CommitCAS error is a TRANSIENT
// reshard-window signal that Transact should ride out by re-running fn
// (re-resolving the owner from the live ring each attempt), rather than a
// terminal failure. Two gRPC status codes qualify:
//
//   - codes.Unavailable: the cluster-wide write-freeze window, or the pin
//     unit's lease mid-handoff. The owner refuses the commit retryably.
//   - codes.FailedPrecondition: a forwarded CommitCAS reached the node that
//     just lost ownership of pin_key across the reshard FLIP/redistribution
//     cutover. The owner refuses WITHOUT applying ("re-pin against the current
//     ring", the ring-refresh loop-guard). The next attempt re-resolves the
//     owner and lands on the new one. This is the SAME retryable window the
//     SPEC's read path already retries across the staggered-generation FLIP
//     (docs/SPEC.md "READ AVAILABILITY ... retryable across FLIP"); the write
//     path must match it so a Transact spanning a reshard commits exactly once
//     at the new generation instead of surfacing the bare re-pin error.
//
// A genuine cross-shard guard violation is NOT a FailedPrecondition here: it
// fires inside fn as backend.ErrCrossShard and aborts before any commit, so it
// never reaches this classifier.
func commitRetryable(err error) bool {
	// Belt-and-suspenders: a FENCED owner-local CAS apply (a graceful-leave
	// successor fenced this leaver mid-commit) is a transient retry, the same
	// shape the single-key Put path treats it. The owner-local recode in
	// CommitCASApply (casFenceToTransient) already converts the fence to the
	// codes.Unavailable acquiring-window error that the status switch below
	// catches; this guard is a second net for any raw multi-%w-wrapped
	// backend.ErrFenced that reaches the classifier WITHOUT a gRPC status (a
	// multi-wrapped fence has errors.Is == true but status.FromError ok == false,
	// so the status switch alone would mis-class it NOT-retryable -> a hard
	// client failure). A fenced commit did not durably apply, so retrying it
	// (with the OCC read-set re-validation) is safe + idempotent.
	if errors.Is(err, backend.ErrFenced) {
		return true
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.FailedPrecondition:
		return true
	default:
		return false
	}
}

// bytesEqual reports byte-for-byte equality, treating nil and empty as
// equal (a read-check's expected value and the backend's stored value can
// each be either). Same semantics as bytes.Equal.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Transact is the ergonomic primary OCC surface: a retry closure that
// hides the conflict loop. It opens a CAS-buffered transaction pinned to
// pinKey, runs fn (which issues Gets + buffered Puts/Deletes against tx),
// commits via the CAS validate-and-apply, and on backend.ErrCASConflict
// RE-RUNS fn from scratch (fresh reads, fresh buffer) up to CASMaxAttempts
// with a small randomized backoff. If it never converges within the
// budget it returns backend.ErrCASConflict. A NON-conflict error from fn
// or from Commit aborts immediately with that error and is NOT retried.
//
// fn MUST be re-runnable and side-effect-free outside tx: Transact may
// invoke it multiple times. Mutating external state inside fn (bumping a
// process-local counter, sending a notification) is a bug, because a
// conflict re-runs the whole closure. The canonical pattern is "read
// current value(s) via tx, compute the new value(s) purely, buffer the
// writes, return nil"; the retry then re-reads the now-changed value and
// recomputes.
//
// pinKey should be a key in (or sharding to the same shard as) the
// transaction's key-set; it fixes the shard the OCC commit validates
// against. The first tx.Get / tx.Put would pin the same shard anyway, so
// passing the natural anchor key (e.g. the counter the transaction
// updates) is the convention.
//
// A commit that fails with a RETRYABLE status is NOT a conflict and NOT a hard
// failure: Transact re-runs fn after a randomized backoff WITHOUT consuming the
// conflict budget, up to transactUnavailableTimeout, so a brief reshard window
// is transparent to the caller. Two status codes are retryable here (see
// commitRetryable + docs/SPEC.md "The retry closure"):
//   - codes.Unavailable: the cluster is briefly write-frozen for a reshard, or
//     the pin unit's lease is mid-handoff.
//   - codes.FailedPrecondition: a forwarded CommitCAS landed on the node that
//     JUST lost ownership of pinKey across the FLIP/redistribution cutover; the
//     owner refuses with the "re-pin against the current ring" loop-guard. Since
//     commitCAS re-resolves the owner from the LIVE ring on every attempt, the
//     re-run lands on the NEW owner and commits. A genuine cross-shard violation
//     does NOT reach here: it fires inside fn as backend.ErrCrossShard and aborts
//     immediately, before any commit.
//
// Past transactUnavailableTimeout either code surfaces as-is (still a retryable
// status the caller may handle).
func (c *Cluster) Transact(pinKey []byte, fn func(tx backend.Transaction) error) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	var conflicted bool
	unavailableDeadline := time.Now().Add(transactUnavailableTimeout)
	for attempt := 0; attempt < CASMaxAttempts; attempt++ {
		tx := c.newCASTx(backend.SnapshotIsolation)
		tx.pin(pinKey)
		err := fn(tx)
		if err != nil {
			// A non-Commit error from fn aborts immediately. fn does not
			// produce ErrCASConflict itself (only Commit does); a cross-
			// shard guard error from a buffered op is a hard error here,
			// not a retry. Roll back the buffer (purely local) for
			// hygiene before returning.
			_ = tx.Rollback()
			return err
		}
		err = tx.Commit()
		if err == nil {
			return nil
		}
		// A retryable commit status (freeze / handoff Unavailable, or the
		// reshard-cutover re-pin FailedPrecondition) is transient: back off and
		// re-run fn from scratch without spending a conflict attempt, until the
		// deadline. This makes a reshard barrier invisible to Transact callers -
		// the commit re-resolves the owner from the live ring each attempt, so a
		// re-run after a cutover lands on the new owner (the commit's gRPC status
		// survives the wire per the CAS server's status-preserving path).
		if commitRetryable(err) {
			if time.Now().After(unavailableDeadline) {
				return err
			}
			if casBaseBackoff > 0 {
				jitter := time.Duration(rand.Int63n(int64(casBaseBackoff) + 1))
				time.Sleep(casBaseBackoff + jitter)
			}
			attempt-- // do not consume the conflict budget for a transient freeze
			continue
		}
		if !errors.Is(err, backend.ErrCASConflict) {
			return err
		}
		conflicted = true
		// Randomized backoff before the next attempt. Skip the sleep on
		// the final iteration (we are about to return).
		if attempt < CASMaxAttempts-1 && casBaseBackoff > 0 {
			jitter := time.Duration(rand.Int63n(int64(casBaseBackoff) * int64(attempt+1)))
			time.Sleep(casBaseBackoff + jitter)
		}
	}
	if conflicted {
		return fmt.Errorf("cluster: Transact exhausted %d attempts: %w", CASMaxAttempts, backend.ErrCASConflict)
	}
	return backend.ErrCASConflict
}
