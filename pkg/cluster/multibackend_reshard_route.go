// Two-generation dual-write routing for the decentralized online split (v0.9).
//
// While a split is in flight, a key's old unit K is being copied into the
// gen-(g+1) child it hashes into. Every write DUAL-WRITES to both the parent
// GenUnit{g,K} and that one child GenUnit{g+1, child}, CO-LOCATED at the same R
// parent slots (the child is mounted alongside its parent during the split,
// desiredReplicaUnits / splitChildrenVia). The ack bar is counted over the
// AUTHORITATIVE legs only:
//
//   - BEFORE this node observes K's durable cut-over marker (hasCutOver(K)
//     false): authoritative = the PARENT legs. Every acked write has a durable
//     home on the parent set throughout the slow copy; the child legs are
//     supplementary (best-effort), keeping the child current so its caught-up
//     re-scan converges fast.
//   - AFTER the marker is observed (hasCutOver(K) true): authoritative = the
//     CHILD legs (now caught-up). The parent legs are supplementary so a LAGGING
//     node still reading the parent sees the write until the parent retires.
//
// The single cluster-ordered cut point is the durable cut-over marker (C5/C6),
// observed identically by every node; this routing is what makes the staggered
// per-node flip lossless without a cluster-wide freeze. Supplementary legs are
// best-effort on purpose: the no-acked-write-loss guarantee rests on the
// authoritative ack bar plus the child's caught-up re-scan backstop (C5), NOT on
// every supplementary leg landing.

package cluster

import (
	"context"

	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// reshardLegs is the dual-write routing for a key whose unit is mid-split.
type reshardLegs struct {
	auth    []routedReplica // ack bar counts these (parent pre-flip, child post-flip)
	supp    []routedReplica // the other generation; best-effort, never gates
	stableR int             // R; the ack bar is requiredWriteAcks over this
}

// routedReplicasForReshard resolves a key during an in-flight split to its
// dual-write legs. ok is false when no split is in flight (nextCount zero), so
// the caller falls back to the single-generation routedReplicas* path.
//
// Parent and child legs share the parent member set (co-location), so the same R
// nodes are touched, each at two ReplicaUnits (parent gen and child gen). Which
// set is authoritative follows this node's local cut-over view for K.
func (c *Cluster) routedReplicasForReshard(key []byte) (reshardLegs, bool) {
	gs := c.genSnapshot()
	if gs.nextCount.IsZero() {
		return reshardLegs{}, false
	}
	h := storageunit.HashShardKey(c.shardKey(key))
	k := storageunit.UnitForHash(h, gs.count)
	parentGU := storageunit.NewGenUnit(gs.gen, k)
	childGU := storageunit.NewGenUnit(gs.gen+1, storageunit.UnitForHash(h, gs.nextCount))

	members := c.unitReplicas(parentGU) // co-location: child shares these slots
	if len(members) == 0 {
		return reshardLegs{}, false
	}
	parentLegs := legsAt(parentGU, members)
	childLegs := legsAt(childGU, members)

	legs := reshardLegs{stableR: len(members)}
	if gs.hasCutOver(k) {
		legs.auth, legs.supp = childLegs, parentLegs
	} else {
		legs.auth, legs.supp = parentLegs, childLegs
	}
	return legs, true
}

// legsAt pairs each member (by slot index) with the ReplicaUnit it holds gu at.
// For a split's child legs the members are the PARENT's replica set (the child
// is co-located at the parent's slots), so the child slot index matches the
// parent slot index it was copied from.
func legsAt(gu storageunit.GenUnit, members []ring.Member) []routedReplica {
	out := make([]routedReplica, len(members))
	for i, m := range members {
		out[i] = routedReplica{member: m, ru: storageunit.NewReplicaUnit(gu, uint8(i))}
	}
	return out
}

// putReshardDualWrite fans a pre-stamped write out to the authoritative legs
// (ack-gated at writeAckBar over stableR) and the supplementary legs (fired
// best-effort, drained in the background, never gating the ack). It returns the
// authoritative writeAttempt the retry wrapper classifies, exactly like the
// single-generation putReplicatedUnitAttempt.
func (c *Cluster) putReshardDualWrite(ctx context.Context, legs reshardLegs, key, envBytes []byte) writeAttempt {
	c.fireSupplementary(ctx, legs.supp, key, envBytes)

	authMembers := membersOf(legs.auth)
	if len(authMembers) == 0 {
		return writeAttempt{err: errUnitAcquiring("Put")}
	}
	w := c.writeAckBar(legs.stableR)
	acks, errs, ch := fanout(ctx, authMembers, w,
		func(opCtx context.Context, idx int, replica ring.Member) ([]byte, error) {
			return nil, c.dispatchReplicaPutUnit(opCtx, replica, legs.auth[idx].ru, key, envBytes)
		})
	go drainResults(ch)
	return classifyWriteAttempt(acks, w, errs)
}

// fireSupplementary dispatches the supplementary legs and drains their results
// in the background. Their acks/errs are intentionally discarded: the
// authoritative generation carries durability, and the child's caught-up
// re-scan (C5) is the backstop that guarantees the child holds every parent
// write before cut-over, so a dropped supplementary leg cannot lose an acked
// write. A supplementary leg that hits a mid-mount unit returns the transient
// errUnitAcquiring, harmlessly drained here.
func (c *Cluster) fireSupplementary(ctx context.Context, legs []routedReplica, key, envBytes []byte) {
	members := membersOf(legs)
	if len(members) == 0 {
		return
	}
	_, _, ch := fanout(ctx, members, 1, // requiredAcks clamped to 1 by fanout; ignored
		func(opCtx context.Context, idx int, replica ring.Member) ([]byte, error) {
			return nil, c.dispatchReplicaPutUnit(opCtx, replica, legs[idx].ru, key, envBytes)
		})
	go drainResults(ch)
}

// parentSlotMidSplit reports whether ru is a gen-g (PARENT) unit while a split is
// in flight on this node - the writes that must be quiesced around finalize.
func (c *Cluster) parentSlotMidSplit(ru storageunit.ReplicaUnit) bool {
	gs := c.genSnapshot()
	return !gs.nextCount.IsZero() && ru.Unit.Gen == gs.gen
}

// resolveAndApplyReplicaPut resolves ru's mounted backend and applies the
// envelope apply-if-newer. When ru is a parent slot mid-split it takes the unit's
// write-pause READ side around the RESOLVE + apply, so finalizeSplit's retire
// (which holds the WRITE side around its final copy) is atomic w.r.t. this write:
// a write racing the retire blocks, then resolves the now-retired mount as !ok and
// returns the transient acquiring error - re-routed to the child, never a lost
// ack. Outside a split / for child units it is the plain resolve + apply. This is
// the multi-backend analogue of localWriteBackendForKey's pause discipline, on
// the position-addressed (union/dual-write) apply path.
func (c *Cluster) resolveAndApplyReplicaPut(ru storageunit.ReplicaUnit, key, envBytes []byte) error {
	if c.parentSlotMidSplit(ru) {
		pause := c.pauseLockFor(ru.Unit.ID)
		pause.RLock()
		defer pause.RUnlock()
	}
	b, ok := c.localBackendForReplicaUnit(ru)
	if !ok {
		return errUnitAcquiring("Put")
	}
	return c.applyEnvelopeIfNewerToBackend(b, ru, key, envBytes)
}

// membersOf projects the routed legs onto their member slice for fanout.
func membersOf(legs []routedReplica) []ring.Member {
	out := make([]ring.Member, len(legs))
	for i, l := range legs {
		out[i] = l.member
	}
	return out
}

// drainResults consumes a fanout result channel so the accounting goroutine and
// op goroutines never block on an unread channel.
func drainResults(ch <-chan replicaResult) {
	//nolint:revive // empty-block: idiomatic channel drain.
	for range ch {
	}
}
