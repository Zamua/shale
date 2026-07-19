// Decentralized reshard agreement wiring for multi-backend mode (v0.9).
//
// This file is the cluster-side seam between the agreement layer (pkg/reshard's
// Arbiter, a CAS-guarded epoch object in shared storage) and the per-unit
// routing substrate (genState). It owns ONLY the construction + seeding of the
// Arbiter here; the routing, online copy, and per-unit cut-over machinery the
// Arbiter drives land in sibling files as later phases.
//
// Scope: the decentralized online reshard is R>1 multi-backend ONLY. The R=1
// multi-node path keeps the coordinated freeze barrier
// (multibackend_reshard_barrier.go); the single-node path keeps the inline
// online bisect (multibackend_reshard.go). So the Arbiter is constructed only
// when cfg.ConditionalStore is set AND replicaFactory != nil (R>1).

package cluster

import (
	"fmt"

	"github.com/Zamua/shale/pkg/reshard"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// initReshardArbiter constructs and seeds the decentralized reshard Arbiter when
// this cluster opts into the declarative path: a ConditionalStore is configured
// AND this is an R>1 multi-backend cluster (replicaFactory wired). It is a no-op
// (leaves c.arbiter nil) otherwise, so a cluster without a ConditionalStore, or
// at R=1 / legacy, stays byte-for-byte on the existing reshard paths.
//
// Called from initMultiBackend AFTER initReplicatedFactory (which sets
// replicaFactory) and BEFORE the unit mount, so the agreed epoch object exists
// from the moment the node starts serving. Seeding is idempotent across the
// cluster: exactly one node's create-if-absent wins and every other node (and a
// later joiner, even one configured at a different base count) adopts the
// already-seeded State, keeping the single durable object the sole authority.
//
// Fails closed: a ConditionalStore that cannot be reached fails Open rather than
// leaving a node that opted into the decentralized path without its agreement
// object.
func (c *Cluster) initReshardArbiter() error {
	if c.cfg.ConditionalStore == nil || !c.replicaLayout() {
		return nil
	}
	a := reshard.NewArbiter(c.cfg.ConditionalStore)
	if _, err := a.Seed(c.unitCount); err != nil {
		return fmt.Errorf("cluster: seed reshard arbiter: %w", err)
	}
	c.arbiter = a
	return nil
}

// reshardGenStep computes the next genState given the local routing state and
// the cluster's agreed reshard State S, stepping the LIVE generation ONE step at
// a time toward S.Count. It is pure (no I/O, no locks) so the whole state
// machine is table-testable in isolation; the live reconcile reads S, calls
// this, and commits the result.
//
// Two transitions, in priority order:
//
//  1. FINALIZE the in-flight generation. While a split is in flight
//     (nextCount != 0) the per-unit machinery adds each old unit to cutOver as
//     its durable caught-up marker is observed. Once EVERY old unit has cut over
//     (allCutOver), the live generation advances: gen -> gen+1, count ->
//     nextCount, the reshard is no longer in flight, cutOver clears. This is the
//     same final-commit the imperative bisect does, but gated on the durable
//     per-unit markers rather than a coordinator.
//
//  2. ENTER IN-FLIGHT, one generation at a time. When steady (nextCount == 0)
//     and the agreed count differs from ours, begin the immediate next step
//     toward it: a split sets nextCount = count.Double(), a merge sets
//     nextCount = count.Halve(). Crucially it steps to OUR next generation's
//     count (count*2 or count/2), NOT S.Count, and never reads S.Epoch - so a
//     node that was asleep while the agreed count moved several steps still
//     performs each intermediate generation in order (the non-contiguous-epoch
//     invariant: never skip a generation's work).
//
// changed reports whether next differs from local (whether the caller should
// commitGenState).
func reshardGenStep(local genState, S reshard.State) (next genState, changed bool) {
	if !local.nextCount.IsZero() {
		if allCutOver(local) {
			return genState{
				gen:     local.gen + 1,
				count:   local.nextCount,
				cutOver: make(map[storageunit.UnitID]struct{}),
			}, true
		}
		return local, false // mid-split: the per-unit machinery is still working
	}
	switch {
	case S.Count.N() > local.count.N(): // SPLIT one generation toward the agreed count
		nc, err := local.count.Double()
		if err != nil {
			return local, false // already at MaxUnitCount; cannot split further
		}
		next = local.clone()
		next.nextCount = nc
		return next, true
	case S.Count.N() < local.count.N(): // MERGE one generation toward the agreed count
		nc, err := local.count.Halve()
		if err != nil {
			return local, false // already at MinUnitCount; cannot merge further
		}
		next = local.clone()
		next.nextCount = nc
		return next, true
	}
	// S.Count == local.count: steady at the agreed count, no change.
	return local, false
}

// splitChildrenVia returns the gen-(g+1) child ReplicaUnits this node should
// hold during an in-flight split (genState.nextCount != 0). Each child is
// CO-LOCATED with its gen-g parent: this node desires child unit C at the same
// replica SLOT it holds C's parent, using the SAME replica resolver (replicaAt)
// the gen-g pass uses (so the pending/current views stay in lockstep).
//
// Co-location is what makes the online copy + dual-write LOCAL: the node holding
// parent slot s copies its parent bytes (gen-g .../r<s>) into the child's slot-s
// database (gen-(g+1) .../r<s>) and dual-writes there, so every one of the
// child's R slot-databases is populated by the corresponding parent slot. After
// cut-over the children redistribute from these parent-co-located slots to their
// gen-(g+1) ring homes by the normal zero-copy lease handoff (the slot's durable
// bytes have a fixed prefix; only which node holds the slot changes) - design
// §3.5. The whole key-space doubles, so EVERY child of EVERY parent this node
// holds is desired up-front (not just cut-over ones); the cutOver set governs
// ROUTING (which generation serves a key), not which children are mounted.
func (c *Cluster) splitChildrenVia(gs genState, self storageunit.NodeID, replicaAt func(gu storageunit.GenUnit) []ring.Member) []storageunit.ReplicaUnit {
	out := make([]storageunit.ReplicaUnit, 0, gs.nextCount.N())
	for _, child := range gs.nextCount.IDs() {
		parent, err := storageunit.ParentUnit(child, gs.nextCount)
		if err != nil {
			continue
		}
		for slot, m := range replicaAt(storageunit.NewGenUnit(gs.gen, parent)) {
			if storageunit.NodeID(m.ID) == self {
				out = append(out, storageunit.NewReplicaUnit(storageunit.NewGenUnit(gs.gen+1, child), uint8(slot)))
				break
			}
		}
	}
	return out
}

// allCutOver reports whether every old (gen-g) unit in an in-flight split has
// cut over to its gen-(g+1) children. The cutOver set is keyed by old unit id
// 0..count-1, so a full set (len == count) means the whole key-space has flipped
// and the live generation can advance.
func allCutOver(g genState) bool {
	return !g.nextCount.IsZero() && len(g.cutOver) == int(g.count.N())
}

// shouldAdvanceArbiter reports whether this node should race to Advance the
// agreed reshard epoch one generation toward its Target. It advances ONLY when
// this node is steady (no split in flight) AND has fully applied the currently
// agreed count (local.count == S.Count) AND the Target still wants a different
// count. The "fully applied" gate is the non-contiguous-epoch safeguard: a node
// reaches local.count == S.Count only after finalizing the current generation,
// which requires every old unit's durable cut-over marker (the current
// generation's reshard is cluster-wide complete), so advancing cannot outrun an
// unfinished generation. The CAS race in Arbiter.Advance then lets exactly one
// node perform the step; the rest adopt it.
func shouldAdvanceArbiter(local genState, S reshard.State) bool {
	return local.nextCount.IsZero() &&
		local.count.N() == S.Count.N() &&
		S.Count.N() != S.Target.N()
}
