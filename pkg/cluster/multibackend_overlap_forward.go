// Position-addressed forward (v0.8 Phase 2e, Option B overlap handoff). This is
// the WIRE side of the overlap: while a ReplicaUnit is in PhaseAcquiring on the
// NEW (gaining) owner, a routed op for that position is NOT answered with
// errUnitAcquiring - it is FORWARDED to the recorded predecessor (the OLD owner,
// still serving the Draining entry) over RPC, carrying the EXPLICIT ReplicaUnit
// on the wire so the predecessor resolves mountMap[ru] DIRECTLY (it no longer
// holds the position at its own live ring index). The forward stops the instant
// the new owner reaches Ready (its mount entry is inserted), after which it
// serves locally.
//
// Read AND write are transparent: the forwarded op reuses the existing clientFor
// plumbing and the predecessor's local-apply / local-read code against the
// Draining mountMap[ru] entry. A forwarded write is applied AwaitDurable on the
// predecessor's authoritative database below the fence epoch, so a forwarded ack
// means the write is durable and the new owner reads it on open.
//
// Single-hop fallback: if the forward RPC fails (predecessor down / dial error /
// timeout) OR no predecessor was recorded (ambiguous / multi-hop / pure-new-mount),
// the new owner does NOT guess - it falls through to errUnitAcquiring, the
// Option-A retry belt. See docs/design/overlap-handoff.md "Predecessor
// forwarding" + "Predecessor unreachable OR ambiguous".

package cluster

import (
	"context"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// acquiringForwardTarget reports, for a key whose unit this node is GAINING, the
// in-flight ReplicaUnit + the recorded predecessor address to forward to, IF the
// position is currently PhaseAcquiring WITH an identifiable single-hop
// predecessor. ok=false means there is no overlap forward to do for this key:
// either the position is not in Acquiring on this node, or it is Acquiring with
// NO predecessor (the single-hop fallback) - in both cases the caller proceeds
// with its normal local resolution (which yields errUnitAcquiring when the mount
// is absent, the Option-A belt).
//
// The position is resolved from this node's LIVE ring index for the key's unit
// (the same resolver normal routed ops use); the overlap forward only fires for
// the position the ring currently assigns this node, which is exactly the one
// being acquired.
func (c *Cluster) acquiringForwardTarget(key []byte) (ru storageunit.ReplicaUnit, predAddr string, ok bool) {
	gu := c.genUnitForKey(key)
	pos, hasPos := c.localReplicaPos(gu)
	if !hasPos {
		return storageunit.ReplicaUnit{}, "", false
	}
	ru = storageunit.NewReplicaUnit(gu, pos)

	state := c.handoffPhaseOf(ru)
	if state.Phase != storageunit.PhaseAcquiring || !state.HasPredecessor() {
		return storageunit.ReplicaUnit{}, "", false
	}
	addr, resolved := c.addrForNodeID(state.Predecessor)
	if !resolved {
		// The recorded predecessor is no longer in the ring (it left). Treat as
		// unreachable -> Option-A fallback.
		return storageunit.ReplicaUnit{}, "", false
	}
	return ru, addr, true
}

// addrForNodeID resolves a storageunit.NodeID to its gRPC dial address via the
// live ring. The pure HandoffState records the predecessor by NodeID (I/O-free);
// the dial address is a transport concern resolved here at forward time. ok=false
// when the node is not in the ring (it left), which the caller treats as an
// unreachable predecessor (Option-A fallback).
func (c *Cluster) addrForNodeID(id storageunit.NodeID) (string, bool) {
	if c.ring == nil {
		return "", false
	}
	for _, m := range c.ring.Members() {
		if storageunit.NodeID(m.ID) == id {
			return m.Addr, true
		}
	}
	return "", false
}

// forwardPutToPredecessor forwards a write (the pre-stamped envelope bytes) to
// the predecessor of an Acquiring position, position-addressed by the explicit
// ru. It returns (handled=true, err) when the overlap forward applied (or the
// predecessor refused); (handled=false, nil) when there is no overlap forward to
// do (the caller proceeds with normal local resolution). On a forward RPC
// failure it returns handled=true with the transient errUnitAcquiring so the
// fan-out tolerates it and the Option-A retry rides it out (NOT a hard failure
// that papers over the handoff).
//
// The fanout ATTEMPT ctx (derived from WriteTimeout) is threaded to
// PutAtReplica so a HUNG predecessor is cancelled at the write deadline rather
// than leaking a goroutine + gRPC stream until the OS TCP timeout.
func (c *Cluster) forwardPutToPredecessor(ctx context.Context, key, envBytes []byte) (handled bool, err error) {
	ru, addr, ok := c.acquiringForwardTarget(key)
	if !ok {
		return false, nil
	}
	cli, derr := c.clientFor(addr)
	if derr != nil {
		// Predecessor unreachable: single-hop fallback to Option A.
		return true, errUnitAcquiring("Put")
	}
	if perr := cli.PutAtReplica(ctx, ru, key, envBytes); perr != nil {
		return true, errUnitAcquiring("Put")
	}
	return true, nil
}

// forwardGetToPredecessor forwards a read to the predecessor of an Acquiring
// position, position-addressed by the explicit ru. Same handled/fallback shape
// as forwardPutToPredecessor: handled=false means no overlap forward (caller
// reads locally); a forward failure returns handled=true + errUnitAcquiring so
// the read fan-out skips this leg and the Option-A path covers it.
//
// The fanout ATTEMPT ctx (derived from ReadTimeout) is threaded to
// GetAtReplica so a HUNG predecessor is cancelled at the read deadline rather
// than leaking a goroutine + gRPC stream until the OS TCP timeout.
func (c *Cluster) forwardGetToPredecessor(ctx context.Context, key []byte) (handled bool, value []byte, err error) {
	ru, addr, ok := c.acquiringForwardTarget(key)
	if !ok {
		return false, nil, nil
	}
	cli, derr := c.clientFor(addr)
	if derr != nil {
		return true, nil, errUnitAcquiring("Get")
	}
	v, found, gerr := cli.GetAtReplica(ctx, ru, key)
	if gerr != nil {
		return true, nil, errUnitAcquiring("Get")
	}
	if !found {
		return true, nil, backend.ErrNotFound
	}
	return true, v, nil
}

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

// -- Predecessor (Draining) side: serve a position-addressed forwarded op ------

// LocalReplicaPutAt applies a position-addressed forwarded write DIRECTLY against
// the explicit ru's mounted backend (the Draining entry), bypassing the live-ring
// resolver (which on the predecessor no longer yields the moving position). It is
// the predecessor's handler for the overlap forward. The caller (rpc.Server.Put,
// the ru-set branch) has already confirmed nothing about ring ownership - the
// position MOVED away from this node's ring index, which is the whole point - so
// the only gate is whether this node still has ru mounted (a Draining entry). An
// unmounted ru (already released) returns the retryable acquiring-window error so
// the forwarder falls through to Option A.
func (c *Cluster) LocalReplicaPutAt(ru storageunit.ReplicaUnit, key, bytesToWrite []byte) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	if c.isFrozen() {
		return errWriteFrozen("Put")
	}
	b, ok := c.localBackendForReplicaUnit(ru)
	if !ok {
		return recodeForwardedReplicaErr(errUnitAcquiring("Put"))
	}
	return recodeForwardedReplicaErr(c.applyEnvelopeIfNewerToBackend(b, ru, key, bytesToWrite))
}

// LocalReplicaGetAt serves a position-addressed forwarded read DIRECTLY from the
// explicit ru's mounted backend (the Draining entry). Mirrors LocalReplicaPutAt:
// an unmounted ru returns ErrNotFound-or-acquiring so the forwarder degrades to
// Option A. backend.ErrNotFound is surfaced as not-found.
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

// LocalReplicaDeleteAt clears key DIRECTLY from ru's Draining mount. At R>1 a
// Delete is normally a tombstone ENVELOPE routed through the Put path (so the
// overlap delete forward rides forwardPutToPredecessor, not this); this method
// covers the rare direct-forward shape and does a raw backend delete, mirroring
// LocalReplicaDelete's R=1 behavior. An unmounted ru (already released) returns
// the retryable acquiring error so the forwarder degrades to Option A.
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
// backend (the position-addressed Draining entry) instead of the ring-resolved
// one. It runs the same never-clobber apply-if-newer in one transaction under
// c.applyMu, so a forwarded overlap write and a direct write on the predecessor
// cannot race. A stale mount (the entry was evicted mid-apply) is evicted +
// reported as acquiring so the forwarder retries.
func (c *Cluster) applyEnvelopeIfNewerToBackend(b backend.Backend, ru storageunit.ReplicaUnit, key, incomingEnvBytes []byte) error {
	incoming, err := Decode(incomingEnvBytes)
	if err != nil {
		return err
	}

	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	tx, err := b.Begin(backend.SnapshotIsolation)
	if err != nil {
		c.evictStaleMount(ru, b)
		return errUnitAcquiring("Put")
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
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}
