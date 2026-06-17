// Position-addressed replica apply (v0.8 Phase 2e, pending ranges). This is the
// WIRE side of the union DUAL-WRITE: a routed write to a union member carries the
// EXPLICIT ReplicaUnit on the wire (the *AtReplica peer client + the Ru field) so
// the receiving replica resolves mountMap[ru] DIRECTLY, rather than re-deriving
// the position from the key against its own current-set ring index. This is
// required because a PENDING owner (a node added to the routed union by the
// draining-excluded split) does NOT appear at the position in its current-set
// ring index - it physically holds the slot it acquired, addressable only by the
// explicit ru. A CURRENT owner is addressed at its current index identically.
//
// There is NO per-position forward to a predecessor: the union routes writes +
// reads DIRECTLY to both the current and pending owners. A pending owner still
// mid-mount has no mounted entry for ru and returns the transient acquiring
// error, which the union fan-out tolerates (another union member - the
// still-mounted current owner - answers). See docs/design/overlap-handoff.md and
// docs/SPEC.md "v0.8 Phase 2e".

package cluster

import (
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// replicaUnitFromWire reconstructs a storageunit.ReplicaUnit from the wire
// fields of a position-addressed forwarded op (the pb.ReplicaUnitRef). It keeps
// the proto<->domain conversion in pkg/cluster so pkg/rpc's server handlers can
// pass the raw ints without importing storageunit.
func replicaUnitFromWire(gen uint64, unit, replica uint32) storageunit.ReplicaUnit {
	return storageunit.NewReplicaUnit(
		storageunit.NewGenUnit(storageunit.Generation(gen), storageunit.UnitID(unit)),
		uint8(replica),
	)
}

// LocalReplicaPutAtWire is the rpc.Server entry point for a position-addressed
// forwarded Put: it reconstructs the ru from the wire ints and applies it via
// LocalReplicaPutAt. Exposed so the server handler need not import storageunit.
func (c *Cluster) LocalReplicaPutAtWire(gen uint64, unit, replica uint32, key, value []byte) error {
	return c.LocalReplicaPutAt(replicaUnitFromWire(gen, unit, replica), key, value)
}

// LocalReplicaGetAtWire is the rpc.Server entry point for a position-addressed
// forwarded Get.
func (c *Cluster) LocalReplicaGetAtWire(gen uint64, unit, replica uint32, key []byte) ([]byte, error) {
	return c.LocalReplicaGetAt(replicaUnitFromWire(gen, unit, replica), key)
}

// LocalReplicaDeleteAtWire is the rpc.Server entry point for a position-addressed
// forwarded Delete.
func (c *Cluster) LocalReplicaDeleteAtWire(gen uint64, unit, replica uint32, key, value []byte) error {
	return c.LocalReplicaDeleteAt(replicaUnitFromWire(gen, unit, replica), key, value)
}

// -- Receiver side: serve a position-addressed union op ------------------------

// LocalReplicaPutAt applies a position-addressed union write DIRECTLY against the
// explicit ru's mounted backend, bypassing the live-ring resolver (which on a
// PENDING owner does not yield the position - a pending owner is absent from its
// own current-set ring index). It is the receiver's handler for a union
// dual-write. The caller (rpc.Server.Put, the ru-set branch) has already
// confirmed nothing about ring ownership - the union deliberately routes a
// position to a node not at that current-set index - so the only gate is whether
// this node still has ru mounted. An unmounted ru (a pending owner still
// mid-mount, or a released drain) returns the retryable acquiring-window error so
// the originator's union fan-out tolerates it (another union member answers).
func (c *Cluster) LocalReplicaPutAt(ru storageunit.ReplicaUnit, key, bytesToWrite []byte) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	if c.isFrozen() {
		return errWriteFrozen("Put")
	}
	// resolveAndApplyReplicaPut quiesces a parent-slot write around the v0.9
	// finalize retire (pause read side) so a forwarded dual-write cannot land on a
	// retiring parent and be lost; outside a split it is the plain resolve + apply.
	return recodeForwardedReplicaErr(c.resolveAndApplyReplicaPut(ru, key, bytesToWrite))
}

// LocalReplicaGetAt serves a position-addressed union read DIRECTLY from the
// explicit ru's mounted backend. Mirrors LocalReplicaPutAt: an unmounted ru
// returns ErrNotFound-or-acquiring so the originator's read fan-out skips this
// leg. backend.ErrNotFound is surfaced as not-found.
func (c *Cluster) LocalReplicaGetAt(ru storageunit.ReplicaUnit, key []byte) ([]byte, error) {
	if c.notReady() {
		return nil, backend.ErrClosed
	}
	b, ok := c.localBackendForReplicaUnit(ru)
	if !ok {
		return nil, errUnitAcquiring("Get")
	}
	return b.Get(key)
}

// LocalReplicaDeleteAt clears key DIRECTLY from ru's mounted backend. At R>1 a
// Delete is normally a tombstone ENVELOPE routed through the Put path (so the
// union delete rides the position-addressed Put, not this); this method covers
// the rare direct position-addressed Delete shape and does a raw backend delete,
// mirroring LocalReplicaDelete's R=1 behavior. An unmounted ru (already released)
// returns the retryable acquiring error so the originator retries.
func (c *Cluster) LocalReplicaDeleteAt(ru storageunit.ReplicaUnit, key, _ []byte) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	if c.isFrozen() {
		return errWriteFrozen("Delete")
	}
	b, ok := c.localBackendForReplicaUnit(ru)
	if !ok {
		return recodeForwardedReplicaErr(errUnitAcquiring("Delete"))
	}
	if err := b.Delete(key); err != nil {
		c.evictStaleMount(ru, b)
		return recodeForwardedReplicaErr(errUnitAcquiring("Delete"))
	}
	return nil
}

// applyEnvelopeIfNewerToBackend is applyEnvelopeIfNewerToUnit against an EXPLICIT
// backend (the position-addressed union target) instead of the ring-resolved one.
// It runs the same never-clobber apply-if-newer in one transaction under
// c.applyMu, so a union dual-write and a direct write on the same node cannot
// race. A stale mount (the entry was evicted mid-apply) is evicted + reported as
// acquiring so the originator retries.
func (c *Cluster) applyEnvelopeIfNewerToBackend(b backend.Backend, ru storageunit.ReplicaUnit, key, incomingEnvBytes []byte) error {
	_, err := c.applyEnvelopeIfNewerToBackendReport(b, ru, key, incomingEnvBytes)
	return err
}

// applyEnvelopeIfNewerToBackendReport is applyEnvelopeIfNewerToBackend that also
// reports whether the incoming envelope was actually APPLIED (its stamp strictly
// beat the stored one / there was no stored value). The v0.9 online split copy
// uses the bool to detect a CLEAN re-scan pass (applied nothing == the child
// already holds everything the parent holds at this instant).
func (c *Cluster) applyEnvelopeIfNewerToBackendReport(b backend.Backend, ru storageunit.ReplicaUnit, key, incomingEnvBytes []byte) (applied bool, err error) {
	incoming, err := Decode(incomingEnvBytes)
	if err != nil {
		return false, err
	}

	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	tx, err := b.Begin(backend.SnapshotIsolation)
	if err != nil {
		c.evictStaleMount(ru, b)
		return false, errUnitAcquiring("Put")
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	apply, aerr := txApplyIfNewer(tx, key, incoming.Stamp)
	if aerr != nil {
		return false, c.fenceToTransient(ru, b, "Put", aerr)
	}
	if apply {
		if err := tx.Put(key, incomingEnvBytes); err != nil {
			return false, c.fenceToTransient(ru, b, "Put", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, c.fenceToTransient(ru, b, "Put", err)
	}
	committed = true
	return apply, nil
}
