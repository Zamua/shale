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
	if c.closed.Load() || c.backend == nil {
		return backend.ErrClosed
	}

	incoming, err := Decode(incomingEnvBytes)
	if err != nil {
		// Corrupt incoming envelope: refuse the write rather than store
		// garbage. The originator Encode()d this, so a decode failure is
		// a real bug, not v0.3 compat data.
		return err
	}

	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	tx, err := c.backend.Begin(backend.SnapshotIsolation)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	apply, aerr := txApplyIfNewer(tx, key, incoming.Stamp)
	if aerr != nil {
		return aerr
	}
	if apply {
		if err := tx.Put(key, incomingEnvBytes); err != nil {
			return err
		}
	}
	// On the no-apply path we commit the empty tx (no Put) so the newer
	// stored value is left intact; commit-without-put keeps the backend's
	// tx lifecycle clean. A Rollback would be equally correct since
	// nothing was written.
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}
