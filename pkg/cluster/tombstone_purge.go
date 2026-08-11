package cluster

// Mount-time tombstone purge (docs/SPEC.md "Tombstone purge (RF>1)").
//
// At R>1 a delete is an LWW envelope with an empty payload - live KV to the
// storage engine, so nothing engine-side ever removes it. This file is the
// reclamation step: when a replica position completes its mount, one
// background pass scans that position's LOCAL backend and converts every
// expired shale tombstone into a NATIVE backend delete, which the engine's own
// compaction then drops physically (the standard last-run rule).
//
// The pass is a mount-LIFECYCLE step, invoked from the same single seam that
// publishes the serving marker (mountTable.mountServing), so every mount path
// inherits it and none can forget it. There is deliberately no periodic
// driver: restart is the de facto purge trigger.

import (
	"errors"
	"time"

	"github.com/Zamua/shale/internal/decide"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// startTombstonePurge is the mount-seam hook: decide eligibility from the
// configuration, and either spawn one background pass for ru or refuse. A
// refusal with the feature DISABLED (zero grace, the default) is silent; a
// refusal with the feature ENABLED but the configuration unsafe (write ack
// bar below R) logs loudly - the operator asked for purging and must learn
// why it will not run.
func (c *Cluster) startTombstonePurge(ru storageunit.ReplicaUnit) {
	grace := c.cfg.TombstoneGracePeriod
	v := decide.TombstonePurge(grace, c.cfg.ReplicationFactor,
		requiredWriteAcks(c.cfg.WriteConsistency, c.cfg.ReplicationFactor))
	if !v.Eligible {
		if grace > 0 {
			c.logf("shale: tombstone purge REFUSED for %s: %s", ru, v.Reason)
		}
		return
	}
	c.loopWG.Add(1)
	go func() {
		defer c.loopWG.Done()
		c.runTombstonePurge(ru, grace)
	}()
}

// TestingRunTombstonePurge runs one SYNCHRONOUS purge pass over every
// position this node currently has mounted, under the same eligibility gate
// as the mount-time hook. Test-only, following the Testing* white-box
// convention: integration tests trigger a deterministic purge instead of
// remounting a position and waiting for the background pass.
func (c *Cluster) TestingRunTombstonePurge() {
	grace := c.cfg.TombstoneGracePeriod
	v := decide.TombstonePurge(grace, c.cfg.ReplicationFactor,
		requiredWriteAcks(c.cfg.WriteConsistency, c.cfg.ReplicationFactor))
	if !v.Eligible {
		return
	}
	for _, ru := range c.mounts.mountedList() {
		c.runTombstonePurge(ru, grace)
	}
}

// runTombstonePurge is one purge pass over ru's local backend: collect the
// keys whose value is an expired tombstone envelope, then delete each through
// a per-key transaction that re-checks the key first. Fail-closed throughout:
// anything unreadable, undecodable, unexpired, or changed-underneath is KEPT.
func (c *Cluster) runTombstonePurge(ru storageunit.ReplicaUnit, grace time.Duration) {
	b, ok := c.mounts.backendFor(ru)
	if !ok {
		return // released before the pass started; the next mount purges.
	}
	candidates, scanned, err := collectExpiredTombstones(b, grace, uint64(time.Now().UnixNano()))
	if err != nil {
		c.logf("shale: tombstone purge for %s aborted during scan (kept everything): %v", ru, err)
		return
	}
	if len(candidates) == 0 {
		return
	}

	purged, skipped := 0, 0
	for _, key := range candidates {
		if c.closed.Load() {
			c.logf("shale: tombstone purge for %s stopped by shutdown: purged %d of %d", ru, purged, len(candidates))
			return
		}
		switch err := c.purgeOneTombstone(ru, b, key, grace); {
		case err == nil:
			purged++
		case errors.Is(err, backend.ErrCASConflict):
			skipped++ // the key changed underneath (re-created / re-deleted); keep whatever it is now.
		case errors.Is(err, backend.ErrFenced), errors.Is(err, backend.ErrClosed):
			// Superseded or shutting down: the successor's own mount purges.
			c.logf("shale: tombstone purge for %s stopped (%v): purged %d of %d", ru, err, purged, len(candidates))
			return
		default:
			c.logf("shale: tombstone purge for %s aborted on %q: %v (purged %d of %d)", ru, key, err, purged, len(candidates))
			return
		}
	}
	c.logf("shale: tombstone purge for %s: scanned %d, purged %d, skipped %d (of %d expired)",
		ru, scanned, purged, skipped, len(candidates))
}

// collectExpiredTombstones scans b once and returns copies of every key whose
// value decodes to a tombstone envelope (empty payload) with a stamp older
// than grace. Values that fail to decode are kept without failing the pass:
// the pass deletes only what it AFFIRMATIVELY recognizes as an expired
// tombstone, and an undecodable value is not that.
func collectExpiredTombstones(b backend.Backend, grace time.Duration, nowNanos uint64) (keys [][]byte, scanned int, err error) {
	it, err := b.ScanPrefix(nil)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = it.Close() }()
	for {
		k, v, err := it.Next()
		if err != nil {
			return nil, scanned, err
		}
		if k == nil {
			return keys, scanned, nil
		}
		scanned++
		env, derr := Decode(v)
		if derr != nil || len(env.Payload) != 0 {
			continue
		}
		if !decide.TombstoneExpired(env.Stamp.TimestampNanos, nowNanos, grace) {
			continue
		}
		keys = append(keys, append([]byte(nil), k...))
	}
}

// purgeOneTombstone deletes key from b iff it still holds an expired
// tombstone, atomically: the re-read and the delete share one transaction, so
// a write racing the pass surfaces as a commit conflict (backend.ErrCASConflict)
// instead of being clobbered - the guard and its subject come from one read at
// one instant. The delete is applied under the unit's write-pause read-lock,
// the same lock local writes take, so a reshard cut-over quiesces the purge
// exactly as it quiesces writes.
func (c *Cluster) purgeOneTombstone(ru storageunit.ReplicaUnit, b backend.Backend, key []byte, grace time.Duration) error {
	pause := c.pauseLockFor(ru.Unit.ID)
	pause.RLock()
	defer pause.RUnlock()

	tx, err := b.Begin(backend.SnapshotIsolation)
	if err != nil {
		return err
	}
	v, err := tx.Get(key)
	if err != nil {
		_ = tx.Rollback()
		if errors.Is(err, backend.ErrNotFound) {
			return backend.ErrCASConflict // already gone; nothing to purge.
		}
		return err
	}
	env, derr := Decode(v)
	if derr != nil || len(env.Payload) != 0 ||
		!decide.TombstoneExpired(env.Stamp.TimestampNanos, uint64(time.Now().UnixNano()), grace) {
		_ = tx.Rollback()
		return backend.ErrCASConflict // no longer an expired tombstone; keep it.
	}
	if err := tx.Delete(key); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
