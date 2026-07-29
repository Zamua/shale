package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
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
	return c.replicationFactor() > 1 && c.coord != nil && c.coord.Populated()
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

// transactRetryableBackoffCap bounds the exponential backoff the retryable-
// status branch of Transact grows into: each consecutive retryable commit
// failure doubles the backoff from casBaseBackoff up to this cap, and the
// sleep before each re-run is full-jitter, a uniform random duration in
// (0, current-backoff]. var so tests can tune it.
//
// WHY Transact DOES NOT USE THE SHARED retryWait (see retry_wait.go). Its two
// backoffs differ from the handoff / re-drive family in KIND, not in constants:
//
//   - JITTER SHAPE. This branch is FULL jitter, uniform in (0, backoff]. The
//     conflict branch below is neither that nor the shared [0.5, 1.0) shape: it
//     sleeps casBaseBackoff + uniform[0, casBaseBackoff*(attempt+1)), i.e. an
//     ADDITIVE, linearly-growing spread rather than a doubling one. Two distinct
//     shapes in one loop is exactly what a single parameterized wait cannot
//     express without becoming a bag of flags.
//   - BUDGET SHAPE. The bound here is not one wall clock. It is a wall clock
//     (transactUnavailableTimeout) AND a re-run cap (transactRetryableMaxRuns)
//     AND the conflict budget (CASMaxAttempts) - and the retryable branch
//     deliberately DECREMENTS the loop counter so a transient freeze does not
//     consume a conflict attempt. The shared wait assumes one monotonic
//     schedule; this loop's attempt counter is not monotonic.
//   - INJECTION POINT. The retryable branch sleeps through the transactSleep
//     var so unit tests can capture the requested durations without really
//     sleeping. The shared wait sleeps for real.
//
// Folding this in would mean carrying three extra knobs to serve one caller, so
// it stays hand-rolled on purpose.
var transactRetryableBackoffCap = 500 * time.Millisecond

// transactRetryableMaxRuns caps how many times Transact re-runs fn through a
// retryable commit status before giving up with ErrTransactRetriesExhausted.
// The retryable statuses that reach this loop are outcome (d) only: a PRE-
// commit refusal that applied nothing (a cluster-wide freeze / lease mid-
// handoff codes.Unavailable, a reshard-cutover codes.FailedPrecondition, a
// graceful-leave fence recoded to codes.Unavailable). A committed-but-under-
// replicated commit (outcome (c)) is NO LONGER one of them: it is classified as
// success-under-replication at the source (CommitCASApply / casResultToError),
// so it never enters this loop. The cap therefore bounds how many times one
// Transact re-runs fn while riding out a genuine transient window; an unbounded
// loop on an aggressive flat backoff re-runs every few milliseconds for the
// whole transactUnavailableTimeout, and a herd of callers behind one freeze
// hammers the (unfreezing) owner. transactUnavailableTimeout remains the outer
// wall-clock bound; whichever trips first ends the loop. var so tests can
// shrink it.
var transactRetryableMaxRuns = 24

// ErrTransactRetriesExhausted marks a Transact that gave up after
// transactRetryableMaxRuns re-runs through a retryable commit status
// (codes.Unavailable / codes.FailedPrecondition). It is returned WRAPPED
// around the last commit error, so status.FromError still resolves the
// retryable gRPC status through the wrap (callers classifying by code keep
// working) while errors.Is against this sentinel lets a caller distinguish
// "gave up retrying transient unavailability" from a hard failure.
var ErrTransactRetriesExhausted = errors.New("cluster: Transact retryable-commit re-run budget exhausted")

// transactSleep is the sleeper the retryable-status branch of Transact uses
// between re-runs. var so unit tests can capture the requested backoff
// durations instead of really sleeping.
var transactSleep = time.Sleep

// transactCommit commits a CAS-buffered transaction on behalf of Transact.
// var so unit tests can inject retryable commit outcomes without standing up
// a multi-node cluster; production behavior is exactly tx.Commit().
var transactCommit = func(tx *clusterTx) error { return tx.Commit() }

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
// Stamp{stamps.Next(), owner NodeID} before tx.Put (Delete becomes an empty-payload
// tombstone Put, NOT tx.Delete, so the LWW comparator on a replica sees a
// stamped removal). After the local commit succeeds it fans the SAME
// encoded envelopes out to the R-1 other replicas via ApplyBatch, waiting
// for W total acks (the owner's local commit is one of them). An under-W
// fan-out returns {Committed: true, UnderReplicated: true, Err: <under-W
// error>} - a SUCCESS with degraded replication, NOT a failure: the write
// is ALREADY durable on the owner + any replica that acked (same best-
// effort-to-W shape single-key Put has), so the commit must never be
// retried away. committed==true is the sound discriminator because the
// owner-local Commit already returned before the fan-out ran. Because the
// pods run relaxed (slate AwaitDurable=false, ack from the memtable), the
// under-W branch first SYNCHRONOUSLY FLUSHES the owner's local backend to
// the object store (flushOwnerUnderReplicated), so the lone owner copy is
// bucket-durable before success returns; a flush failure is the one under-W
// case that returns a terminal {Err} instead of success. See docs/SPEC.md
// "The four commit outcomes" + "Write-set replication".
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
		// The commit stamp is drawn AFTER (a) the read-set validation above
		// Observed every stored stamp it decoded (casReadPayload) and (b)
		// the stored stamp of every BLIND write-set key (a WriteOp key with
		// no matching read-check, which the validate loop never decodes)
		// has been Observed here, one header-only parse per blind key
		// against the already-open tx. So the commit stamp strictly
		// exceeds every stored stamp the commit replaces - read-set AND
		// write-set: the committed write-set can never lose the LWW
		// compare to the values it replaced (a raw wall clock behind a
		// stored stamp would let replicas reject the commit and
		// read-repair un-commit it; see stampclock.go).
		if err := c.observeBlindWriteStamps(tx, reads, writes); err != nil {
			return backend.CASResult{Err: c.casFenceToTransient(tx, "CommitCASApply", err)}
		}
		stamp = Stamp{
			TimestampNanos: c.stamps.Next(),
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
	// acks (owner's local commit pre-counted as 1). An under-W result is a
	// committed-under-replication SUCCESS (see the return below), NOT an
	// error: the write is already durable on the owner + any replica that
	// acked, the same best-effort-to-W model single-key Put has. A read-only
	// commit (no writes) has nothing to replicate.
	if replicated && len(encodedWrites) > 0 {
		// Thread the position the owner-local commit applied to (the pausedTx's
		// resolved ru, multi mode) into the fan-out, so the owner's OTHER mounted
		// position of the pin unit (the index-shuffling survivor mid-transition)
		// receives the batch too and counts as a durable ack - the CAS mirror of
		// the put path's dual-position routing. Zero-valued on the legacy per-node
		// path (no pausedTx), where the attempt ignores it. ownerBackend is the
		// SAME mounted backend the owner-local commit applied against: pt.b in
		// multi mode, c.backend on the legacy per-node replicated path (no
		// pausedTx). It is the flush target on the under-W branch below.
		var committedRU storageunit.ReplicaUnit
		ownerBackend := c.backend
		if pt, ok := tx.(*pausedTx); ok {
			committedRU = pt.ru
			ownerBackend = pt.b
		}
		if err := c.replicateCASBatch(pinKey, committedRU, encodedWrites); err != nil {
			// POST-commit failure. committed==true is the sound discriminator:
			// everything after the owner-local tx.Commit() above succeeded, and
			// the ONLY thing that can fail here is replicateCASBatch, whose every
			// error return is a W-shortfall on the fan-out (a codes.Unavailable;
			// see replicateCASBatchAttempt / classifyWriteAttempt). So this is
			// definitionally committed-but-under-replicated, NOT not-committed:
			// the write is durable on the owner (+ any replica that acked).
			//
			// DURABILITY CLOSING ADDITION (relaxed-durability seal). shale runs the
			// pods RELAXED (slate AwaitDurable=false, ack from the memtable) and
			// buys durability from REPLICATION: an ack to W replicas is W
			// independent in-RAM copies that survive a single pod loss. The NORMAL
			// W-reached path above deliberately does NOT flush (that is the whole
			// performance design). But on THIS under-W branch the replication
			// safety net MISSED: the write's only copy is in the owner's memtable
			// (RAM), so acking it as durable would be unsound (an owner-pod crash
			// in the sub-second pre-flush window is silent loss). So before
			// returning success, synchronously FLUSH the owner's local backend to
			// the object store, converting that lone copy from RAM-only to bucket-
			// durable. The lock is already released (casMuHeld==false) so the flush
			// I/O holds no lock; a backend that is already synchronously durable
			// (memory/pebble, or slate at AwaitDurable=true) does not implement
			// backend.Flusher and is skipped (no RAM-loss window to close).
			if ferr := c.flushOwnerUnderReplicated(ownerBackend); ferr != nil {
				// The flush FAILED: the lone owner copy could not be persisted.
				// That is a genuine durability failure (committed to the memtable,
				// could NOT be made bucket-durable), so we do NOT report success -
				// we surface the flush error as a not-committed error the caller
				// sees (committed-but-couldnt-persist is real; the caller must know).
				// This does NOT amplify: a flush I/O failure is a TERMINAL error,
				// NOT the retryable under-W codes.Unavailable, so casResultToError
				// returns it and Transact does NOT re-run fn (the write-set applied
				// at most once). The one flush error that DOES classify retryable is
				// a fence (a successor opened the db at a higher epoch in the post-
				// commit window); re-running fn there is safe, not amplifying,
				// because a fenced memtable write was never persisted, so the OCC
				// re-commit on the successor is a clean single apply. committed is
				// already true, so the deferred Rollback stays suppressed - we do
				// not un-apply the owner-local write, we only report that its
				// durability could not be confirmed.
				return backend.CASResult{Err: ferr}
			}
			// Owner copy is now bucket-durable. Return committed-under-replication
			// SUCCESS so Transact does NOT re-run fn and re-commit an already-
			// durable write (the amplification + false-conflict bug). Err is
			// preserved for logging/observability only; the caller maps
			// Committed==true to nil. The missing replica COUNT heals via apply-if-
			// newer read-repair on the next quorum Get (NOT via the fan-out's own
			// WriteTimeout-bounded retry - that is already exhausted here, which is
			// exactly what produced this under-W outcome). See docs/SPEC.md "The
			// four commit outcomes".
			return backend.CASResult{Committed: true, UnderReplicated: true, Err: err}
		}
	}
	return backend.CASResult{Committed: true}
}

// flushOwnerUnderReplicated makes the owner's lone durable copy of an under-
// replicated CAS commit bucket-durable, on the RARE under-W branch of
// CommitCASApply only. ownerBackend is the mounted backend the owner-local
// commit applied against (pt.b in multi mode, c.backend on the legacy per-node
// replicated path). The flush is OPTIONAL by capability: a backend that is
// already synchronously durable (memory/pebble, or slate at AwaitDurable=true)
// does not implement backend.Flusher, so there is no RAM-loss window and the
// flush is skipped (returns nil). flushableBackend unwraps the fencedSelfHealing
// decorator the mount seam wraps a mounted backend in, so a real slate mount at
// AwaitDurable=false is found and Flush() forces its memtable durable to the
// object store. A non-nil return is a genuine durability failure the under-W
// branch surfaces terminally (see the call site); a nil return means the lone
// copy is now bucket-durable (or the backend never had a loss window).
//
// The Flush is GROUP-COMMITTED per storage unit through c.casFlushGroup, NOT
// issued directly: under a same-owner write burst under-W is the common case,
// so a per-write full-memtable flush would be a flush storm; the coordinator
// coalesces concurrent under-W flushes on the SAME unit into a shared flush
// (one Flush persists all their memtable-resident writes) while keeping
// different units independent. The coordinator keys by the resolved
// backend.Flusher identity - which is 1:1 with the mounted unit's independent
// durable store (multi mode) and the single shared store on the legacy per-node
// path - so "same unit" and "same flush target" coincide. It is transparent to
// the returned error: a shared flush's error (including a %w-wrapped
// backend.ErrFenced) reaches every waiter in the batch unchanged, so the
// existing terminal-vs-retryable-fence classification at the call site is
// preserved. See flushGroup + docs/SPEC.md "Owner-flush coalescing".
func (c *Cluster) flushOwnerUnderReplicated(ownerBackend backend.Backend) error {
	if ownerBackend == nil {
		return nil
	}
	fl, ok := flushableBackend(ownerBackend)
	if !ok {
		return nil
	}
	return c.casFlushGroup.flush(fl)
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
	// Ratchet the stamp clock with the stored stamp BEFORE the commit
	// stamp is issued (the validate loop runs first in CommitCASApply),
	// so the commit stamp strictly exceeds every stamp in the validated
	// read-set. Without this, a stored stamp from a faster clock would
	// beat the commit stamp and read-repair could un-commit the
	// validated write (see stampclock.go).
	c.stamps.Observe(env.Stamp.TimestampNanos)
	if len(env.Payload) == 0 {
		// Winning tombstone: the key the client saw is gone. Treat as
		// not-found for both ExpectAbsent and value-match checks.
		return nil, false, nil
	}
	return env.Payload, true, nil
}

// observeBlindWriteStamps ratchets the owner's stamp clock with the stored
// stamp of every BLIND write-set key: a WriteOp whose key has no matching
// read-check, so the validate loop never decoded (and never Observed) its
// stored envelope. Called by CommitCASApply at R>1, against the already-open
// owner-local tx, BEFORE the shared commit stamp is drawn - the write-set
// half of the commit-stamp floor (casReadPayload is the read-set half).
// Without it a blind tx.Put could draw a commit stamp below the stored
// value's (a faster peer clock wrote it), and the acked commit would be
// silently un-committed by apply-if-newer rejection + read-repair.
//
// One tx.Get + header-only decodeStamp per distinct blind key. A missing key
// contributes nothing; a stored value without the envelope magic byte (a
// legacy raw value) decodes to the zero stamp, a no-op Observe. A truncated
// or overrun envelope header is surfaced as the error it is (the commit
// reports {Err}, the same policy casReadPayload applies to the read-set).
func (c *Cluster) observeBlindWriteStamps(tx backend.Transaction, reads []backend.ReadCheck, writes []backend.WriteOp) error {
	var readKeys map[string]struct{}
	if len(reads) > 0 {
		readKeys = make(map[string]struct{}, len(reads))
		for _, r := range reads {
			readKeys[string(r.Key)] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(writes))
	for _, w := range writes {
		k := string(w.Key)
		if _, ok := readKeys[k]; ok {
			continue // read-set key: casReadPayload already Observed it
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		got, err := tx.Get(w.Key)
		if err != nil {
			if errors.Is(err, backend.ErrNotFound) {
				continue
			}
			return err
		}
		stored, _, derr := decodeStamp(got)
		if derr != nil {
			return derr
		}
		c.stamps.Observe(stored.TimestampNanos)
	}
	return nil
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
// failure: Transact re-runs fn after a backoff WITHOUT consuming the conflict
// budget, so a brief reshard window is transparent to the caller. Only outcome
// (d) - a PRE-commit refusal that applied nothing - reaches this branch; a
// committed-but-under-replicated commit (outcome (c)) is classified as success
// at the source (CommitCASApply returns Committed==true and the tx-commit-to-
// error conversion maps that to nil), so it never re-runs fn. The backoff is
// exponential with full jitter (a uniform random sleep in (0, backoff],
// doubling per consecutive retryable failure from casBaseBackoff up to
// transactRetryableBackoffCap), and the re-runs are bounded BOTH by
// transactUnavailableTimeout and by transactRetryableMaxRuns, whichever trips
// first. The bounds keep the loop polite: a herd of callers riding out one
// multi-second freeze must not hammer the (unfreezing) owner with a flat few-
// millisecond re-run rate. Every (d) re-run applied nothing, so it is a fresh
// single-apply attempt, never a second durable commit of the same write-set.
// Two status codes are retryable here (see commitRetryable + docs/SPEC.md "The
// four commit outcomes"):
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
// status the caller may handle); past transactRetryableMaxRuns the error is
// wrapped in ErrTransactRetriesExhausted (errors.Is-distinguishable, and still
// resolving to the retryable status via status.FromError).
func (c *Cluster) Transact(pinKey []byte, fn func(tx backend.Transaction) error) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	var conflicted bool
	retryableRuns := 0
	retryableBackoff := casBaseBackoff
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
		err = transactCommit(tx)
		if err == nil {
			return nil
		}
		// A retryable commit status (freeze / handoff Unavailable, or the
		// reshard-cutover re-pin FailedPrecondition) is transient: back off and
		// re-run fn from scratch without spending a conflict attempt, until the
		// deadline or the re-run cap, whichever trips first. This makes a
		// reshard barrier invisible to Transact callers - the commit
		// re-resolves the owner from the live ring each attempt, so a re-run
		// after a cutover lands on the new owner (the commit's gRPC status
		// survives the wire per the CAS server's status-preserving path).
		//
		// Only outcome (d) reaches here - a PRE-commit refusal (freeze /
		// handoff / reshard-cutover / recoded fence) that applied nothing.
		// Outcome (c), a committed-but-under-replicated commit, is classified as
		// success at the source (CommitCASApply -> casResultToError / the wire
		// mapping return nil), so it never enters this loop and fn is never re-
		// run for an already-durable write. The loop stays POLITE anyway (see
		// docs/SPEC.md "The four commit outcomes"): a herd behind one multi-
		// second freeze would otherwise hammer the owner. The backoff grows
		// exponentially with full jitter (uniform in (0, backoff], doubling per
		// consecutive retryable failure up to transactRetryableBackoffCap) and
		// the re-runs are capped at transactRetryableMaxRuns, past which the
		// wrapped ErrTransactRetriesExhausted surfaces (still carrying the
		// retryable status for callers that classify by code).
		if commitRetryable(err) {
			if time.Now().After(unavailableDeadline) {
				return err
			}
			if retryableRuns >= transactRetryableMaxRuns {
				return fmt.Errorf("%w after %d re-runs: last commit error: %w",
					ErrTransactRetriesExhausted, retryableRuns, err)
			}
			retryableRuns++
			if retryableBackoff > 0 {
				// Full jitter: a uniform random sleep in (0, retryableBackoff].
				transactSleep(time.Duration(rand.Int63n(int64(retryableBackoff)) + 1))
			}
			retryableBackoff *= 2
			if retryableBackoff > transactRetryableBackoffCap {
				retryableBackoff = transactRetryableBackoffCap
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
