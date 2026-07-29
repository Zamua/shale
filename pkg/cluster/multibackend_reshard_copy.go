// Online split copy for the decentralized reshard (v0.9).
//
// During a split, the node holding parent slot s of unit K copies its parent
// slot's data into the two co-located gen-(g+1) child slots (low = K, high =
// K+N), LOCALLY (the children share the parent's slot s on this node). The copy
// is envelope-aware APPLY-IF-NEWER, never a bare Put: the stored bytes are LWW
// envelopes, so a stale copied value loses to a newer concurrent dual-write and
// vice versa - the copy is idempotent and order-independent against the dual
// writes flowing into the child throughout. A re-scan that applies NOTHING is a
// CLEAN pass: the child slot holds everything the parent slot holds at that
// instant. The child is NEVER cleared (it is dual-receiving); apply-if-newer
// idempotence makes a re-scan a stamp no-op for already-delivered keys.

package cluster

import (
	"context"
	"time"

	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/storageunit"
)

// defaultReshardForwardTimeout bounds ONE cross-node merge forward when no
// WriteTimeout is configured. Per-key (not per-pass) so a slow/down survivor leg
// defers the parent's finalize after one RTT rather than holding the parent's
// write-pause for a whole-pass timeout - the finalize re-tries on the next tick.
const defaultReshardForwardTimeout = 5 * time.Second

// reshardForwardTimeout is the per-forward deadline for the merge copy: the
// configured WriteTimeout (so a finalize forward under the parent pause is bounded
// like any write), falling back to the default.
func (c *Cluster) reshardForwardTimeout() time.Duration {
	if c.cfg.WriteTimeout > 0 {
		return c.cfg.WriteTimeout
	}
	return defaultReshardForwardTimeout
}

// copyMaxPasses bounds the re-scan loop in copyParentUntilCaughtUp so continuous
// write load (which keeps leaving a few un-delivered stragglers for the next
// pass to catch) cannot spin forever. Reaching the cap is not a correctness
// problem: the child holds all but a handful of in-flight stragglers, the dual
// writes keep delivering them, and the ordered-removal gate's final re-scan is
// the strict backstop before the parent is ever retired.
const copyMaxPasses = 8

// copyParentIntoChildren runs ONE copy pass: it scans parent slot ru's backend
// and applies each stored envelope APPLY-IF-NEWER into the co-located child slot
// it hashes into. It returns the number of keys newly applied this pass (0 == a
// clean pass). The parent + both children must be mounted on this node (they are,
// by the cut-over-aware desired set); a missing mount yields the transient
// acquiring error so the caller retries on the next reconcile.
func (c *Cluster) copyParentIntoChildren(ru storageunit.ReplicaUnit, gs genState) (applied int, err error) {
	src, ok := c.localBackendForReplicaUnit(ru)
	if !ok {
		return 0, errUnitAcquiring("copy")
	}
	low, high, err := storageunit.ChildUnits(ru.Unit.ID, gs.count)
	if err != nil {
		return 0, err
	}
	slot := ru.Replica
	lowRU := storageunit.NewReplicaUnit(storageunit.NewGenUnit(gs.gen+1, low), slot)
	highRU := storageunit.NewReplicaUnit(storageunit.NewGenUnit(gs.gen+1, high), slot)
	lowB, ok1 := c.localBackendForReplicaUnit(lowRU)
	highB, ok2 := c.localBackendForReplicaUnit(highRU)
	if !ok1 || !ok2 {
		return 0, errUnitAcquiring("copy")
	}

	it, err := src.ScanPrefix(nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = it.Close() }()
	for {
		key, envBytes, nerr := it.Next()
		if nerr != nil {
			return applied, nerr
		}
		if key == nil {
			return applied, nil
		}
		h := storageunit.HashShardKey(c.shardKey(key))
		dstRU, dstB := highRU, highB
		if storageunit.UnitForHash(h, gs.nextCount) == low {
			dstRU, dstB = lowRU, lowB
		}
		did, aerr := c.applyEnvelopeIfNewerToBackendReport(dstB, dstRU, key, envBytes)
		if aerr != nil {
			return applied, aerr
		}
		if did {
			applied++
		}
	}
}

// copyParentIntoSurvivor forwards every key in parent slot ru into its
// gen-(g+1) SURVIVOR's replicas APPLY-IF-NEWER. The merge is the cross-node
// case: the survivor lives at its OWN ring home (two parents K and K+N collapse
// into survivor K), so the copy forwards each key to the survivor's R replica
// legs through the normal dispatch (local-self for a co-resident replica, gRPC
// otherwise), requiring a WRITE QUORUM of acks per key. A stale forwarded value
// loses to a newer dual-write (LWW); a key that cannot reach quorum returns an
// error so the caller defers (the finalize copy must be strict before the parent
// retires). Idempotent: re-running re-delivers only what a leg has not applied.
func (c *Cluster) copyParentIntoSurvivor(ru storageunit.ReplicaUnit, gs genState) error {
	src, ok := c.localBackendForReplicaUnit(ru)
	if !ok {
		return errUnitAcquiring("merge-copy")
	}
	survivorID, err := storageunit.ParentUnit(ru.Unit.ID, gs.count)
	if err != nil {
		return err
	}
	survivorGU := storageunit.NewGenUnit(gs.gen+1, survivorID)
	legs := legsAt(survivorGU, c.unitReplicas(survivorGU))
	if len(legs) == 0 {
		return errUnitAcquiring("merge-copy")
	}

	it, err := src.ScanPrefix(nil)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()

	for {
		key, envBytes, nerr := it.Next()
		if nerr != nil {
			return nerr
		}
		if key == nil {
			return nil
		}
		// Per-key deadline: a down survivor leg fails THIS forward after one
		// WriteTimeout and aborts the pass, so finalize (which runs the copy under
		// the parent's write-pause WLock) holds the pause for at most one RTT, not a
		// whole-pass timeout. The next reconcile tick retries.
		kctx, kcancel := context.WithTimeout(context.Background(), c.reshardForwardTimeout())
		err := c.forwardKeyQuorum(kctx, legs, key, envBytes)
		kcancel()
		if err != nil {
			return err
		}
	}
}

// forwardKeyQuorum applies one envelope APPLY-IF-NEWER to a unit's replica legs,
// requiring the configured write quorum of acks (over the leg count). It reuses
// the replicated write fan-out, so a transient/acquiring leg is tolerated and a
// quorum shortfall returns the retryable error.
func (c *Cluster) forwardKeyQuorum(ctx context.Context, legs []routedReplica, key, envBytes []byte) error {
	w := c.writeAckBar(len(legs))
	acks, errs, transient, ch := fanout(ctx, membersOf(legs), w,
		func(opCtx context.Context, idx int, replica coord.Node) ([]byte, error) {
			return nil, c.dispatchReplicaPutUnit(opCtx, replica, legs[idx].ru, key, envBytes)
		})
	go drainResults(ch)
	return classifyWriteAttempt(acks, w, errs, transient).err
}

// copyParentUntilCaughtUp re-scans parent slot ru into its children until a pass
// applies nothing (caught up) or copyMaxPasses is reached. It returns true when a
// clean pass was observed (the strict caught-up signal). Each pass is idempotent
// (apply-if-newer), so re-scanning under concurrent dual-writes only re-delivers
// the few keys a best-effort dual-write has not yet landed.
func (c *Cluster) copyParentUntilCaughtUp(ru storageunit.ReplicaUnit, gs genState) (clean bool, err error) {
	for pass := 0; pass < copyMaxPasses; pass++ {
		applied, err := c.copyParentIntoChildren(ru, gs)
		if err != nil {
			return false, err
		}
		if applied == 0 {
			return true, nil
		}
	}
	return false, nil
}
