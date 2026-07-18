// LWW on write (apply-if-newer, v0.7+).
//
// At ReplicationFactor > 1 every Backend value is an LWW Envelope
// (Stamp{TimestampNanos, NodeID} + payload). The READ side already
// resolves LWW (getReplicated picks the max-stamp winner across
// replicas; read-repair pushes that winner to laggards). This file adds
// the complementary WRITE-side rule: a replica-receiving write applies
// an incoming envelope ONLY IF its stamp strictly beats the stored
// stamp (or there is no stored value). An older incoming write is a
// silent no-op (the replica already holds an equal-or-newer value).
//
// Why this matters: under ReadQuorum / ReadAll an async read-repair
// scheduled from an OLD read can fire AFTER a newer write landed. With
// verbatim replica writes (pre-v0.7) that stale repair overwrites the
// newer value, and a later CAS validate-and-apply then reads the stale
// value on the owner-local copy and MISSES a conflict, losing an
// update. Apply-if-newer rejects the stale repair (its stamp is older),
// so read-repair stops clobbering for free once it rides this path.
//
// A second payoff: because replica applies are now order-independent
// (an out-of-order older write self-resolves to a no-op), CommitCASApply
// no longer needs to hold casCommitMu across the network fan-out. See
// docs/SPEC.md "LWW on write (apply-if-newer)".
//
// Boundary: this is REPLICA-RECEIVING-write only, R>1 only. The owner's
// own CAS local commit (CommitCASApply's LocalBegin validate-and-apply,
// under casCommitMu) is authoritative by construction and writes the
// freshest stamp directly, NOT through this path. At R=1 there are no
// envelopes; the raw-value paths bypass this entirely.

package cluster

import "github.com/Zamua/shale/pkg/backend"

// applyEnvelopeIfNewer persists incomingEnvBytes for key on the local
// backend ONLY IF the incoming envelope's Stamp strictly beats the
// stored value's Stamp, or there is no stored value. An older-or-equal
// incoming write is a committed no-op that leaves the newer stored value
// intact.
//
// The get-compare-put is serialized node-wide under c.applyMu and run in
// one local backend transaction so it is atomic per key: two concurrent
// applies on the same key cannot both read the old stamp and race (the
// memory backend's tx has snapshot-isolation reads but no write-write
// conflict detection, so the lock - not the tx - is what makes this
// correct). The window holds no network round-trip, so the coarse lock
// is cheap.
//
// Stamp semantics (see docs/SPEC.md):
//   - Absent stored value: apply (no stored stamp to lose to).
//   - Legacy raw / v0.3 stored value: Decode yields a zero Stamp, which
//     any real incoming stamp beats, so it gets replaced.
//   - Tombstone (empty payload, still stamped): stamp-vs-stamp only,
//     payload irrelevant. A newer tombstone applies over an older value;
//     a newer value applies over an older tombstone.
//   - Equal stamps: do NOT apply (Greater is strict, A.Greater(A)==false).
//     A re-delivered identical envelope is a no-op.
//
// This is only ever called on the R>1 replica-receiving write paths;
// callers pass already-Encode()d envelope bytes.
func (c *Cluster) applyEnvelopeIfNewer(key, incomingEnvBytes []byte) error {
	_, err := c.applyEnvelopeIfNewerReport(key, incomingEnvBytes)
	return err
}

// applyEnvelopeIfNewerReport is applyEnvelopeIfNewer with an extra
// return value: applied reports whether the incoming envelope actually
// beat the stored stamp and was written (true) or lost the compare and
// was a committed no-op (false). The migration receive path needs this
// to keep its rollback set honest: a stream that fails its terminal
// CRC32 / count check rolls back by DELETING the keys it landed, so it
// must record ONLY the keys it actually wrote. A no-op apply-if-newer
// left a NEWER concurrent value in place; deleting that key on rollback
// would clobber the newer write, the exact LWW violation apply-if-newer
// exists to prevent. So FetchRange appends to its rollback set only when
// applied is true.
func (c *Cluster) applyEnvelopeIfNewerReport(key, incomingEnvBytes []byte) (applied bool, err error) {
	if c.notReady() {
		return false, backend.ErrClosed
	}
	if _, _, err := decodeStamp(incomingEnvBytes); err != nil {
		// Corrupt incoming envelope: refuse the write rather than store
		// garbage. The originator Encode()d this, so a decode failure is
		// a real bug, not v0.3 compat data.
		return false, err
	}
	// Legacy single-backend site: raw errors on both surfaces (c.backend
	// is not a mounted unit, so there is no fence recode and no mount to
	// evict).
	return c.applyEnvelopesTx(c.backend, []EnvelopeWrite{{Key: key, Envelope: incomingEnvBytes}}, rawApplyErr, rawApplyErr)
}

// rawApplyErr is the identity error mapper of the two legacy single-
// backend apply sites (applyEnvelopeIfNewerReport and ApplyBatchLocal's
// legacy tail): no fence recode, no mount eviction - the raw backend
// error is those sites' contract.
func rawApplyErr(err error) error { return err }

// applyEnvelopesTx is the ONE apply-if-newer transaction body every
// replica-receiving apply path shares (the single-key envelope applies
// and the CAS write-set batches; a single key is a batch of one). It owns
// the c.applyMu critical section and the transaction lifecycle: ONE
// transaction on b at SnapshotIsolation, APPLY-IF-NEWER per entry
// (txApplyIfNewer: write the incoming envelope verbatim only when its
// stamp strictly beats the stored one, or there is no stored value;
// an older-or-equal entry is a committed no-op that leaves the stored
// value intact), then commit the whole batch together, rolling back on
// any error. The lock - not the tx - is what serializes concurrent
// appliers on the same key (the memory backend's tx has snapshot-
// isolation reads but no write-write conflict detection), and it is held
// across the WHOLE batch. On the all-no-op path the empty tx is still
// committed; commit-without-put keeps the backend's tx lifecycle clean
// (a Rollback would be equally correct since nothing was written).
//
// The error surface differs per call site, so the caller passes two
// mappers: mapBeginErr recodes a Begin failure (the mounted-unit sites
// evict the stale mount and report the transient acquiring-window
// refusal; the legacy sites return it raw) and mapTxErr recodes an
// in-transaction failure from txApplyIfNewer, Put, or Commit (the
// mounted-unit sites fence-recode via fenceToTransient; the legacy sites
// return it raw). A corrupt entry envelope is always returned raw,
// bypassing both mappers: the originator Encode()d it, so a parse
// failure is a real bug, not a condition to recode.
//
// applied reports whether ANY entry won its stamp compare and was
// written; for a batch of one that is exactly the single key's decision
// (the report the migration receive and the online split copy consume).
// On error applied is false and nothing is committed.
func (c *Cluster) applyEnvelopesTx(b backend.Backend, writes []EnvelopeWrite, mapBeginErr, mapTxErr func(error) error) (applied bool, err error) {
	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	tx, berr := b.Begin(backend.SnapshotIsolation)
	if berr != nil {
		return false, mapBeginErr(berr)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	for _, w := range writes {
		stamp, _, derr := decodeStamp(w.Envelope)
		if derr != nil {
			return false, derr
		}
		apply, aerr := txApplyIfNewer(tx, w.Key, stamp)
		if aerr != nil {
			return false, mapTxErr(aerr)
		}
		if !apply {
			continue
		}
		if perr := tx.Put(w.Key, w.Envelope); perr != nil {
			return false, mapTxErr(perr)
		}
		applied = true
	}
	if cerr := tx.Commit(); cerr != nil {
		return false, mapTxErr(cerr)
	}
	committed = true
	return applied, nil
}
