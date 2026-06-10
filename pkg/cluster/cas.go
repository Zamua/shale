package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/Zamua/shale/pkg/backend"
)

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
// Rollback runs and the transaction did not happen. The whole sequence
// is one goroutine against one (non-goroutine-safe) backend.Transaction,
// so there is no concurrency to coordinate.
func (c *Cluster) CommitCASApply(ctx context.Context, level backend.IsolationLevel, reads []backend.ReadCheck, writes []backend.WriteOp) backend.CASResult {
	if c.closed.Load() || c.backend == nil {
		return backend.CASResult{Err: backend.ErrClosed}
	}

	// Serialize the validate-and-apply against other CAS commits on this
	// node so the read-set check + write-set apply are atomic (no lost
	// update; see Cluster.casCommitMu). Held for the whole sequence: the
	// window is one local tx with no network round-trip inside it.
	c.casCommitMu.Lock()
	defer c.casCommitMu.Unlock()

	tx, err := c.LocalBegin(level)
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

	// Validate the read-set.
	for _, r := range reads {
		got, err := tx.Get(r.Key)
		if r.ExpectAbsent {
			if err == nil {
				return backend.CASResult{Conflict: true}
			}
			if !errors.Is(err, backend.ErrNotFound) {
				return backend.CASResult{Err: err}
			}
			continue
		}
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				// Client observed a value; key is now absent: conflict.
				return backend.CASResult{Conflict: true}
			}
			return backend.CASResult{Err: err}
		}
		if !bytesEqual(got, r.ExpectedVal) {
			return backend.CASResult{Conflict: true}
		}
	}

	// Apply the write-set in order.
	for _, w := range writes {
		if w.Del {
			if err := tx.Delete(w.Key); err != nil {
				return backend.CASResult{Err: err}
			}
			continue
		}
		if err := tx.Put(w.Key, w.Value); err != nil {
			return backend.CASResult{Err: err}
		}
	}

	if err := tx.Commit(); err != nil {
		return backend.CASResult{Err: err}
	}
	committed = true
	return backend.CASResult{Committed: true}
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
	if c.closed.Load() || c.backend == nil {
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
