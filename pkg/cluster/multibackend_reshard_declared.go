// Declarative reshard: the unit count is config, declared and advertised
// through the coordinator (coord.Params.DeclaredUnitCount).
//
// The v0.9 decentralized reshard driver (multibackend_reshard_driver.go) is
// DRIVEN by the arbiter's agreed `target`, but nothing yet SETS that target
// from operator config after founding. This file closes that gap WITHOUT an
// imperative trigger (no RPC, no CLI command): the operator declares the shard
// count in the deployment (`SHALE_UNIT_COUNT`, the same value that seeds the
// arbiter at founding) exactly the way a Deployment declares `replicas`, and a
// running cluster splits or merges to match.
//
// The hard part is the rolling deploy. The arbiter `target` lives in ONE
// durable object so the cluster plans one direction; if each node independently
// CAS'd the target to ITS OWN declared value, a rolling deploy - during which
// old pods declare the OLD count and new pods declare the NEW count for ~a
// roll's duration - would have nodes fighting the target back and forth (split,
// then merge, then split). The fix is to gate the retarget on AGREEMENT: each
// node advertises its standing declared count through the coordination port
// (coord.Member.DeclaredUnitCount), and a node retargets the arbiter ONLY
// when the cluster is steady AND every live member advertises the SAME known
// declared count (UNANIMITY). During a roll the live set is a mix, so no node
// retargets until the roll completes; the moment the last node rolls, the set
// is unanimous and exactly one retarget fires (the arbiter CAS picks the
// winner). A not-yet-rolled pod is a VETO, never a vote for the stale value, so
// even a reshard that finishes faster than the roll cannot be dragged back.
// See docs/SPEC.md "Declarative reshard: the unit count is config".

package cluster

import (
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/storageunit"
)

// observeDeclaredReshardTarget reconciles the arbiter's agreed `target` toward
// the cluster-wide DECLARED unit count, when (and only when) the cluster is
// steady and every live member agrees on that count. It is a no-op unless an
// Arbiter is wired (R>1 multi-backend with a ConditionalStore - the homogeneous
// deployment). Caller holds reconcileMu (reconcileUnits does); it runs just
// before observeReshard so a freshly-declared target is picked up the SAME
// reconcile tick.
func (c *Cluster) observeDeclaredReshardTarget() {
	if !c.cfg.DeclarativeReshard {
		return // opt-in: the homogeneous deployment drives the target from the
		// advertised declared count; everything else (tests driving the arbiter
		// imperatively, legacy) leaves the target externally set.
	}
	if c.arbiter == nil || c.coord == nil {
		return // declarative reshard requires a wired arbiter + coordinator
	}
	// Only act when STEADY: no split/merge in flight on this node...
	if !c.genSnapshot().nextCount.IsZero() {
		return
	}
	s, _, err := c.arbiter.Read()
	if err != nil {
		return // transient store read; the next tick retries
	}
	// ...AND the arbiter itself has converged to its current target (it is not
	// mid-way through an earlier multi-generation retarget).
	if s.Count.N() != s.Target.N() {
		return
	}
	// Unanimity over the LIVE membership (self included): every member must
	// advertise the SAME known declared count. A member with an unknown (0)
	// count - an older image, or a node still mid-roll - breaks unanimity and
	// DEFERS the retarget. This is the flap guard.
	d, ok := unanimousDeclaredCount(c.coord.View().Members)
	if !ok {
		return
	}
	if d == s.Target.N() {
		return // already the declared target; nothing to do
	}
	target, err := storageunit.NewUnitCount(int(d))
	if err != nil {
		return // a non-power-of-two declared count (rejected at flag time on
		// every node, so this is defensive); ignore rather than churn.
	}
	// CAS the agreed target to the declared count. The arbiter's CAS makes
	// exactly one racing node win; the rest observe the new target and no-op.
	// observeReshard (same tick) then steps count one generation toward it.
	_, _ = c.arbiter.Retarget(target)
}

// unanimousDeclaredCount returns (D, true) iff the member set is non-empty and
// EVERY member advertises the same nonzero declared unit count D. Any member
// with an unknown (0) count, any disagreement, or an empty set returns
// (_, false) - the fail-safe that DEFERS a declarative retarget until the whole
// live cluster agrees. Pure (no I/O, no locks) so the unanimity logic is
// testable in isolation.
func unanimousDeclaredCount(members []coord.Member) (uint32, bool) {
	var d uint32
	for _, m := range members {
		if m.DeclaredUnitCount == 0 {
			return 0, false // unknown - defer
		}
		switch {
		case d == 0:
			d = m.DeclaredUnitCount
		case m.DeclaredUnitCount != d:
			return 0, false // disagreement (mid-roll)
		}
	}
	if d == 0 {
		return 0, false // empty member set
	}
	return d, true
}
