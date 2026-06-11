package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/Zamua/shale/pkg/backend"
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
				return backend.CASResult{Err: err}
			}
			if found {
				return backend.CASResult{Conflict: true}
			}
			continue
		}
		if err != nil {
			return backend.CASResult{Err: err}
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
					return backend.CASResult{Err: err}
				}
				continue
			}
			if err := tx.Put(w.Key, w.Value); err != nil {
				return backend.CASResult{Err: err}
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
			return backend.CASResult{Err: err}
		}
		encodedWrites = append(encodedWrites, EnvelopeWrite{
			Key:      append([]byte(nil), w.Key...),
			Envelope: envBytes,
		})
	}

	if err := tx.Commit(); err != nil {
		return backend.CASResult{Err: err}
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
func (c *Cluster) Transact(pinKey []byte, fn func(tx backend.Transaction) error) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	var conflicted bool
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
