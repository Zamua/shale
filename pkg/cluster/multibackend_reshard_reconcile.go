// Declarative-resharding reconcile loop (v0.8).
//
// THE TRIGGER IS CONFIG, NOT AN RPC. --unit-count is the DESIRED unit count.
// An operator bumps it (2 -> 4) and rolls the deploy; the cluster reshards
// ITSELF to match, reusing the validated freeze barrier unchanged. The
// decision lives here: a DETERMINISTIC coordinator runs reconcileReshard, which
// drives ONE doubling toward the desired count when four guards hold.
//
// WHERE IT RUNS. reconcileReshard is invoked at the TAIL of the settled unit
// reconcile (after reconcileUnits in runReconcile) AND on the periodic
// reconcile-loop tick (runReconcileLoop). The settle-pass hook fires once a
// rolling deploy settles (membership stops churning, every node reports the new
// desired); the periodic tick is the backstop for a coordinator handoff that
// arrives WITHOUT a fresh membership event on the survivor (the dead
// coordinator left, the next-lowest id is now coordinator, but its ring already
// settled). The periodic tick goes through reconcileReshardIfStable (which
// enforces guard 1 before calling reconcileReshard); the settle pass calls
// reconcileReshard directly. Both reach the same idempotent decision; running it
// twice is a safe no-op.
//
// THE COORDINATOR (exactly one). A reshard must be kicked off by exactly ONE
// node - two nodes both starting a barrier toward g+1 would each freeze the
// cluster and race. The coordinator is the lowest live node-id among the
// current members, read off the SAME membership.Snapshot() the desired-count
// guard reads (sorted by id; coordinator is element 0). Every node runs the
// same selection against the same snapshot, so they agree without an election.
// A single-node multi cluster (no membership) is trivially its own coordinator,
// but it has no reconcile loop running (Open returns early before the loops in
// single-node mode), so it never auto-reshards from the loop.
//
// THE FOUR GUARDS (all must hold). See docs/SPEC.md "Declarative resharding":
//
//	(1) membership STABLE (debounced) - enforced on both caller paths: the settle
//	    pass holds it structurally via the debounce (the pass only fires after
//	    membership has been unchanged for the settle window), and the periodic tick
//	    holds it explicitly via reconcileReshardIfStable / membershipStableFor. So
//	    a mid-roll cluster reporting MIXED desired counts never triggers, and a
//	    periodic tick mid-roll is skipped until the ring shape has settled.
//	(2) all live members report the SAME desired count - unanimity. A 0/unknown
//	    desired (legacy or not-yet-decoded peer) fails this; a half-rolled deploy
//	    with mixed counts fails this.
//	(3) desired > current (genState.count) - desired == current is an idempotent
//	    no-op; desired < current is ignored (shale never SHRINKS).
//	(4) desired is a valid DOUBLING target - a power of two strictly above
//	    current and reachable by repeated doubling. One pass advances by exactly
//	    one doubling; 2 -> 8 takes two settled passes (2 -> 4, then 4 -> 8).
//
// When all four hold, the coordinator runs the barrier ONCE via reshardLocked
// (NON-BLOCKING: a TryLock so a still-in-flight reshard is skipped, never
// queued behind - queuing would double a SECOND time on top of the first). The
// barrier's own safety properties are inherited unchanged (the up-front
// count.Double() ceiling check, the member-identity re-check before BISECT/FLIP
// with ABORT on drift, the NO-ACKED-WRITE-LOST freeze). An aborted reshard
// leaves the generation untouched, so the NEXT settled pass simply re-attempts:
// the declarative loop self-heals a transient abort with no operator action.

package cluster

import (
	"log"
	"os"
	"time"

	"github.com/Zamua/shale/pkg/membership"
)

// reshardDebugEnabled gates the declarative-reshard diagnostic logging (off unless
// SHALE_RESHARD_DEBUG is set). Read once at package init.
var reshardDebugEnabled = os.Getenv("SHALE_RESHARD_DEBUG") != ""

// lowestIDIsCoordinator is the PURE coordinator-selection rule: among the
// members snapshot (sorted ascending by id, as membership.Snapshot guarantees),
// the coordinator is the lowest-id member, so selfID is the coordinator iff it
// equals the first element's id. An empty snapshot has no coordinator (false):
// without a member view we cannot establish identity. Pure (no cluster state)
// so the rule is unit-testable against a constructed member list.
func lowestIDIsCoordinator(snap []membership.Member, selfID string) bool {
	if len(snap) == 0 {
		return false
	}
	return snap[0].ID == selfID
}

// unanimousDesired is the PURE guard-2 rule: it returns the single desired unit
// count every member reports, and ok=true, ONLY when there is unanimity AND that
// count is non-zero. It returns ok=false the instant any member reports a
// DIFFERENT count, or reports 0 ("unknown" - a legacy single-backend node, or a
// peer whose Meta has not yet decoded a count segment). An empty snapshot is not
// agreement (false). Pure so the rule is unit-testable.
func unanimousDesired(snap []membership.Member) (count uint32, ok bool) {
	if len(snap) == 0 {
		return 0, false
	}
	want := snap[0].DesiredCount
	if want == 0 {
		return 0, false
	}
	for _, m := range snap[1:] {
		if m.DesiredCount != want {
			return 0, false
		}
	}
	return want, true
}

// isReshardCoordinator reports whether THIS node is the deterministic reshard
// coordinator: the lowest live node-id among the current members. It reads the
// membership snapshot and applies lowestIDIsCoordinator. A cluster with no
// membership (single-node mode) reports true (it is trivially its own
// coordinator), though single-node mode never starts the reconcile loop so this
// is moot there.
//
// Recomputed on every call (not cached) so a coordinator handoff after the prior
// coordinator leaves is observed on the next reconcile pass without an explicit
// election: the survivors all see the same shrunk snapshot and agree on the new
// lowest-id coordinator.
func (c *Cluster) isReshardCoordinator() bool {
	if c.membership == nil {
		return true
	}
	return lowestIDIsCoordinator(c.membership.Snapshot(), c.cfg.NodeID)
}

// unanimousDesiredCount returns the single desired unit count every live member
// reports, and ok=true, ONLY when there is unanimity AND that count is non-zero
// (guard 2). It reads the membership snapshot and applies unanimousDesired. A
// half-rolled deploy with some nodes at the old desired and some at the new
// fails this, which is what makes the trigger safe across a partial roll: the
// cluster never reshards until every node agrees on the target.
//
// A nil / empty membership returns ok=false: with no member view there is no
// agreement to read.
func (c *Cluster) unanimousDesiredCount() (count uint32, ok bool) {
	if c.membership == nil {
		return 0, false
	}
	return unanimousDesired(c.membership.Snapshot())
}

// validDoublingTarget reports whether advancing from current toward desired by
// exactly ONE doubling is allowed (guard 4 + the desired>current half of guard
// 3). It is true iff desired is a power of two STRICTLY above current and
// reachable by repeated doubling from current (so current must itself divide
// desired as a power-of-two factor). The single next step is always 2*current;
// this function only gates whether that step moves TOWARD a valid desired.
//
// Concretely, with current and desired both powers of two (current is, by the
// UnitCount invariant; desired is checked here):
//   - desired <= current: false (no up-reshard needed; never shrink).
//   - desired not a power of two: false.
//   - desired a power of two strictly greater than current: true (repeated
//     doubling from a power of two reaches every higher power of two, so any
//     higher power-of-two desired is reachable one doubling at a time).
func validDoublingTarget(current uint32, desired uint32) bool {
	if desired <= current {
		return false
	}
	// desired must be a power of two. current is guaranteed a power of two by
	// the UnitCount constructor, so desired > current && desired is a power of
	// two implies desired is reachable by repeated doubling from current.
	return desired&(desired-1) == 0
}

// reconcileReshard is the declarative-resharding DECISION, run by the
// coordinator on each settled reconcile pass and each periodic tick. It drives
// at most ONE doubling per call when all four guards hold; it is otherwise a
// no-op. Idempotent + safe to call repeatedly (the periodic tick + the settle
// pass both call it).
//
// Lock discipline: this method does NOT hold reshardMu / reconcileMu when
// called (the callers invoke it AFTER runReconcile has released those locks),
// and it acquires reshardMu NON-BLOCKING via reshardLocked-with-TryLock through
// triggerReshardStep below, so a reconcile pass that overlaps an in-flight
// reshard (its own from a prior pass, or a barrier this node is participating
// in) skips cleanly rather than blocking the reconcile loop.
func (c *Cluster) reconcileReshard() {
	if c.notReady() || !c.multi {
		return
	}
	// Read the membership snapshot ONCE and base the whole decision on that
	// single local view. Reading it again per-guard would let the
	// coordinator-check pass against one snapshot while the unanimity read sees
	// a different one (a membership flux mid-decision), so the decision could
	// straddle two snapshots and act on neither consistently. A nil membership
	// (single-node multi) yields an empty snapshot: that node is trivially its
	// own coordinator (no early-return below) and unanimousDesired(nil) returns
	// ok=false, so it is a clean no-op - identical to the prior behavior.
	var snap []membership.Member
	if c.membership != nil {
		snap = c.membership.Snapshot()
		// Guard: only the coordinator decides. A non-coordinator is a passive
		// barrier participant; it must not also kick off a reshard. (For nil
		// membership we skip this check: that node IS its own coordinator.)
		if !lowestIDIsCoordinator(snap, c.cfg.NodeID) {
			reshardDebugf("reconcileReshard: not coordinator (self=%s snap=%v)", c.cfg.NodeID, snap)
			return
		}
	}
	// Guard 2: unanimous, non-zero desired across all live members, read off the
	// SAME snapshot as the coordinator check. (Guard 1, membership stability, is
	// enforced by the caller: the settle pass via its debounce, the periodic tick
	// via reconcileReshardIfStable.)
	desired, ok := unanimousDesired(snap)
	if !ok {
		reshardDebugf("reconcileReshard: NOT unanimous (self=%s snap=%v)", c.cfg.NodeID, snap)
		return
	}
	// Guards 3 + 4: desired must be strictly above the live count AND a valid
	// doubling target. genSnapshot().count is the live count the cluster routes
	// at (which a founder may have derived from durable state, distinct from
	// the desired --unit-count).
	current := c.genSnapshot().count.N()
	if !validDoublingTarget(current, desired) {
		// desired == current (already at target, idempotent no-op), desired <
		// current (never shrink), or a non-power-of-two desired: refuse without
		// touching the generation.
		reshardDebugf("reconcileReshard: not a valid doubling (current=%d desired=%d)", current, desired)
		return
	}
	// All guards hold: advance ONE doubling toward desired. A multi-step desired
	// (2 -> 8) is reached by successive settled passes, each doubling once.
	reshardDebugf("reconcileReshard: ALL GUARDS HOLD, triggering %d -> %d", current, desired)
	c.triggerReshardStep()
}

// membershipStableFor reports whether the ring shape has been unchanged for at
// least d. It reads lastRingChangeNanos (stamped by bumpRingGen on every ring-
// shape change). A zero value means the ring has never changed, which reads as
// stable (a never-changed ring trivially satisfies any stability window).
func (c *Cluster) membershipStableFor(d time.Duration) bool {
	last := c.lastRingChangeNanos.Load()
	if last == 0 {
		return true
	}
	return time.Since(time.Unix(0, last)) >= d
}

// reconcileReshardIfStable is the PERIODIC-tick entry to the declarative-reshard
// decision: it enforces guard 1 (membership stability) explicitly before
// delegating to reconcileReshard. The periodic tick fires unconditionally every
// reconcileInterval, so unlike the settle pass it does NOT inherit stability from
// a debounce; without this gate a tick could evaluate a reshard mid-roll. The
// gate correctly DELAYS a coordinator-handoff reshard by settleDelay after the
// triggering membership change (the prior coordinator leaving stamps
// lastRingChangeNanos), which is desirable - membership should settle first.
func (c *Cluster) reconcileReshardIfStable() {
	if !c.membershipStableFor(c.settleDelay()) {
		reshardDebugf("reconcileReshardIfStable: membership not stable for %s, skipping", c.settleDelay())
		return
	}
	c.reconcileReshard()
}

// reshardDebugf logs a declarative-reshard diagnostic line when SHALE_RESHARD_DEBUG
// is set in the environment. Off (one cheap env read) in every normal run; used to
// trace which guard blocks a declarative reshard when validating the harness.
func reshardDebugf(format string, args ...any) {
	if reshardDebugEnabled {
		log.Printf("[reshard-debug] "+format, args...)
	}
}

// triggerReshardStep runs the barrier ONCE toward the next doubling, NON-
// BLOCKING. It acquires reshardMu via TryLock so a reshard already in flight
// (this coordinator's own from an earlier pass that has not finished, or a
// barrier this node is otherwise driving) causes a clean skip rather than a
// queued second doubling. On a successful acquire it runs reshardLocked, which
// routes to the coordinated multi-node barrier (or the single-node bisect) and
// advances the generation by one doubling; any error path (ceiling refuse,
// membership-drift ABORT) leaves the generation untouched, so the next settled
// pass re-attempts.
//
// The {from,to} transition is intentionally not reported anywhere: the trigger
// is config, not an operator call awaiting a result. genState advancement is
// the observable effect; a caller that wants to watch progress reads
// genSnapshot() / Topology.
func (c *Cluster) triggerReshardStep() {
	if !c.reshardMu.TryLock() {
		// A reshard is already in flight; skip this pass. The next settled pass
		// (or periodic tick) re-evaluates against the post-reshard generation.
		return
	}
	defer c.reshardMu.Unlock()
	if err := c.reshardLocked(); err != nil {
		reshardDebugf("triggerReshardStep: reshardLocked error: %v", err)
	}
}

// TestingIsReshardCoordinator exposes the deterministic-coordinator decision to
// out-of-package tests (tests/integration), which build real multi-node
// clusters over gRPC + memberlist and need to assert WHICH node drives a
// declarative reshard. Not part of the app-facing surface.
func (c *Cluster) TestingIsReshardCoordinator() bool { return c.isReshardCoordinator() }

// TestingUnanimousDesiredCount exposes the guard-2 read to out-of-package tests
// so they can wait for the cluster to reach unanimity on a bumped desired before
// asserting on the reshard outcome. Not part of the app-facing surface.
func (c *Cluster) TestingUnanimousDesiredCount() (count uint32, ok bool) {
	return c.unanimousDesiredCount()
}

// TestingReconcileReshard runs ONE declarative-resharding decision pass (the
// same reconcileReshard the settle pass + periodic tick call). Exposed so an
// out-of-package test can drive the decision deterministically rather than
// waiting on the loop's cadence. A no-op on a non-coordinator or when any guard
// fails. Not part of the app-facing surface.
func (c *Cluster) TestingReconcileReshard() { c.reconcileReshard() }
