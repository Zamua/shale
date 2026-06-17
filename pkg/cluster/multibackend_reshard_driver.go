// The decentralized online split driver (v0.9 C6): the per-reconcile-tick
// orchestration that turns the agreed reshard epoch into a live split, with no
// coordinator and no cluster-wide freeze.
//
// observeReshard runs on every R>1 reconcile tick (the periodic self-heal loop +
// every membership event), under reconcileMu + reshardMu, so it is serialized
// with the overlap reconcile and any imperative Reshard. It is a no-op unless an
// Arbiter is wired (opt-in via Config.ConditionalStore). Each tick advances the
// split by one observable step:
//
//  1. ENTER: reflect the agreed State into genState (set nextCount) when the
//     declared count is ahead - the split begins; the cut-over-aware desired set
//     then mounts this node's gen-(g+1) children alongside its parents.
//  2. COPY + MARK: copy each owned parent slot into its co-located children until
//     a clean pass, publish that slot's caught-up marker, and (once every slot of
//     the unit is caught up) publish the unit's durable cut-over marker.
//  3. FLIP: observe the durable cut-over markers and set local cutOver, which
//     flips routing for those units to the children (ack bar moves to the child
//     legs in the dual-write router).
//  4. FINALIZE: once EVERY old unit has cut over, a final clean copy of every
//     owned parent (the strict no-loss backstop), then advance the live
//     generation and retire the now-obsolete parent mounts. The overlap reconcile
//     then redistributes the gen-(g+1) children to their ring homes (zero-copy).
//  5. ADVANCE: when settled at the agreed count and the declared target wants
//     more, race to Advance the agreed epoch one generation (the CAS lets exactly
//     one node perform it).
//
// The copy is SYNCHRONOUS under reconcileMu here (correct + simple; the in-memory
// oracle proves it). Backgrounding the copy for slow object stores is a hardening
// follow-up; the markers + apply-if-newer idempotence already make a re-run safe.

package cluster

import (
	"time"

	"github.com/Zamua/shale/pkg/storageunit"
)

// observeReshard drives this node's share of any in-flight decentralized split.
// No-op when no Arbiter is wired. Caller holds reconcileMu + reshardMu
// (reconcileUnits, via runReconcile).
func (c *Cluster) observeReshard() {
	if c.arbiter == nil {
		return
	}
	s, _, err := c.arbiter.Read()
	if err != nil {
		return // transient store read; the next tick retries
	}

	gs := c.genSnapshot()
	if gs.nextCount.IsZero() {
		// Steady: enter a split (set nextCount) if the agreed count is ahead.
		if next, changed := reshardGenStep(gs, s); changed {
			c.commitGenState(next)
			gs = next
		}
	}

	if !gs.nextCount.IsZero() {
		if gs.nextCount.N() > gs.count.N() {
			c.driveSplitCopies(gs)
		} else {
			c.driveMergeCopies(gs)
		}
		c.observeCutoverMarkers(gs)
		gs = c.genSnapshot()
		if allCutOver(gs) {
			c.finalizeReshard(gs)
			gs = c.genSnapshot()
		}
	}

	if shouldAdvanceArbiter(gs, s) {
		_, _, _ = c.arbiter.Advance()
	}
}

// ownedParentSlots returns the gen-g parent ReplicaUnits this node currently has
// mounted (the slots it copies into children). Read under mountMu.
func (c *Cluster) ownedParentSlots(gs genState) []storageunit.ReplicaUnit {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	var out []storageunit.ReplicaUnit
	for ru := range c.mountMap {
		if ru.Unit.Gen == gs.gen {
			out = append(out, ru)
		}
	}
	return out
}

// driveSplitCopies copies each owned, not-yet-cut-over parent slot into its
// children until caught up, then publishes that slot's caught-up marker and -
// once every replica slot of the unit is caught up - the unit's durable cut-over
// marker. A slot that cannot reach a clean pass this tick is simply retried next
// tick (the copy is idempotent).
func (c *Cluster) driveSplitCopies(gs genState) {
	r := c.replicationFactor()
	for _, ru := range c.ownedParentSlots(gs) {
		k := ru.Unit.ID
		if gs.hasCutOver(k) {
			continue // already flipped; its copy is done
		}
		if !c.cfg.TestingForceUncleanReshard {
			clean, err := c.copyParentUntilCaughtUp(ru, gs)
			if err != nil || !clean {
				continue
			}
		}
		// BREAK-DEMO (TestingForceUncleanReshard): the copy above is skipped, so
		// the markers below publish over children that never received the parent's
		// data - the oracle must catch the resulting acked-write loss.
		if err := c.publishCaughtupMarker(gs.gen, k, ru.Replica); err != nil {
			continue
		}
		if all, err := c.allSlotsCaughtUp(gs.gen, k, r); err == nil && all {
			_ = c.publishCutoverMarker(gs.gen, k)
		}
	}
}

// driveMergeCopies is the merge analogue of driveSplitCopies. Each owned parent
// slot is copied into its gen-(g+1) SURVIVOR (cross-node, quorum-acked) and
// publishes its slot caught-up marker. Because TWO parents (K and K+N) collapse
// into one survivor, a survivor is only caught up once BOTH its parents are, so
// this publishes the cut-over markers for BOTH parents together, gated on both
// parents' slots all being caught up.
func (c *Cluster) driveMergeCopies(gs genState) {
	r := c.replicationFactor()
	for _, ru := range c.ownedParentSlots(gs) {
		k := ru.Unit.ID
		if gs.hasCutOver(k) {
			continue // already flipped; its copy is done
		}
		if !c.cfg.TestingForceUncleanReshard {
			if err := c.copyParentIntoSurvivor(ru, gs); err != nil {
				continue
			}
		}
		if err := c.publishCaughtupMarker(gs.gen, k, ru.Replica); err != nil {
			continue
		}
		// The two parents of this survivor are ChildUnits(survivorID, nextCount):
		// publish BOTH cut-over markers once both parents' slots are all caught up.
		survivorID, err := storageunit.ParentUnit(k, gs.count)
		if err != nil {
			continue
		}
		low, high, err := storageunit.ChildUnits(survivorID, gs.nextCount)
		if err != nil {
			continue
		}
		lowDone, e1 := c.allSlotsCaughtUp(gs.gen, low, r)
		highDone, e2 := c.allSlotsCaughtUp(gs.gen, high, r)
		if e1 == nil && e2 == nil && lowDone && highDone {
			_ = c.publishCutoverMarker(gs.gen, low)
			_ = c.publishCutoverMarker(gs.gen, high)
		}
	}
}

// observeCutoverMarkers polls every old unit's durable cut-over marker and sets
// local cutOver for any newly-flipped unit. The durable marker - not node-local
// state - is the authority, so every node flips a unit in cluster-agreed order
// (whenever it observes the marker), the staggered per-node timing the design
// tolerates.
func (c *Cluster) observeCutoverMarkers(gs genState) {
	var flipped []storageunit.UnitID
	for _, k := range gs.count.IDs() {
		if gs.hasCutOver(k) {
			continue
		}
		if present, err := c.cutoverMarkerPresent(gs.gen, k); err == nil && present {
			flipped = append(flipped, k)
		}
	}
	if len(flipped) == 0 {
		return
	}
	next := c.genSnapshot().clone()
	for _, k := range flipped {
		next.cutOver[k] = struct{}{}
	}
	c.commitGenState(next)
}

// finalizeReshard completes the generation once EVERY old unit has cut over. For
// each owned parent slot it takes that unit's write-PAUSE (the WRITE side), so
// the final copy + retire is ATOMIC w.r.t. parent-leg writes (which take the read
// side in resolveAndApplyReplicaPut): the final copy provably captures every
// write that landed before the pause, and a write that arrives during the pause
// blocks, then fails on the retired parent (transient, never acked) and is
// re-routed to the child/survivor. Without this quiesce a lagging-node parent
// write could land between the final scan and the retire and be lost (the P0 the
// single-node bisect's pause also prevents). Once every owned parent is retired
// the live generation advances; the overlap reconcile then redistributes a
// split's children (a merge's survivors are already at their ring home). A unit
// whose final copy is not strict this tick defers finalize to the next.
func (c *Cluster) finalizeReshard(gs genState) {
	for _, ru := range c.ownedParentSlots(gs) {
		if !c.finalizeParent(ru, gs) {
			return // not safe to retire this unit yet; retry next tick
		}
	}
	c.commitGenState(genState{
		gen:     gs.gen + 1,
		count:   gs.nextCount,
		cutOver: make(map[storageunit.UnitID]struct{}),
	})
}

// finalizeParent quiesces parent slot ru (its unit's write-pause WRITE side),
// runs the STRICT final copy under the pause, and retires the parent. It reports
// whether the parent was retired (false = defer; the copy was not strict). The
// gen advance stays with the caller (it is cluster-generation-level, after every
// owned parent has retired).
func (c *Cluster) finalizeParent(ru storageunit.ReplicaUnit, gs genState) bool {
	pause := c.pauseLockFor(ru.Unit.ID)
	pause.Lock()
	defer pause.Unlock()

	if !c.cfg.TestingForceUncleanReshard && !c.finalCopyUnderPause(ru, gs) {
		return false
	}
	if d := c.cfg.TestingFinalizeRetireDelay; d > 0 {
		time.Sleep(d) // widen the post-final-copy / pre-retire window (test-only)
	}
	c.releaseReplicaUnit(ru)
	return true
}

// finalCopyUnderPause runs the strict final copy for one parent slot (caller
// holds its write-pause). A SPLIT copies into the co-located children and
// requires a clean re-scan; a MERGE forwards into the survivor with a quorum ack
// per key. Returns whether it succeeded (false defers the retire).
func (c *Cluster) finalCopyUnderPause(ru storageunit.ReplicaUnit, gs genState) bool {
	if gs.nextCount.N() > gs.count.N() { // SPLIT
		clean, err := c.copyParentUntilCaughtUp(ru, gs)
		return err == nil && clean
	}
	return c.copyParentIntoSurvivor(ru, gs) == nil // MERGE
}
