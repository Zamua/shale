// Cluster-wide-freeze doubling reshard for multi-backend mode (v0.8 multi-node).
//
// This file holds the COORDINATED multi-node doubling reshard. Phase 4's
// single-node bisect (multibackend_reshard.go) leans on a node-LOCAL
// write-pause, which cannot order writes across nodes: with each node
// advancing its generation independently, a concurrent write during a
// cross-node bisect could route at the old generation on one node while
// another has already flipped, and be lost. This file removes that hazard at
// the root with a brief cluster-wide WRITE-FREEZE: pause ALL writes
// cluster-wide for the (short) reshard, so NO NEW write enters during the
// reshard. The freeze FLAG is a bool flip (not a synchronous quiesce), so a
// writer that passed the gate a moment before the flip may still be landing a
// Put; the per-node bisect closes that window by taking the per-old-unit
// write-pause WRITE side (draining those in-flight writers) before its copy.
// With no new writers and the in-flight ones drained, the copy is quiescent -
// the catch-up window that is the single-node bisect's hard part disappears.
//
// THE INVARIANT THIS FILE IS BUILT TO:
//
//	NO ACKED WRITE IS LOST.
//
// Acked writes (before the freeze) are in the gen-g units, copied into the
// gen-(g+1) units during the (drained) bisect, and visible after the FLIP.
// Writes attempted DURING the freeze get a RETRYABLE error (never acked) and
// succeed on client retry after RESUME, at gen g+1. No write is ever routed to a
// unit being retired: retirement happens at FLIP, under the freeze, AFTER the
// bisect has captured every gen-g write.
//
// THE PROTOCOL (coordinator-driven 4-phase barrier). The node where Reshard()
// is called on a multi-node cluster is the COORDINATOR. It reaches every node
// (including itself) by DIRECT peer RPC (ReshardControl), NOT gossip - a
// barrier needs a synchronous ack from every node, which an eventually-
// consistent broadcast cannot give. Targeting targetGen = g+1:
//
//  1. FREEZE  - coordinator RPCs every node ReshardFreeze(g+1). Each node
//     enters the write-freeze (Put / Delete / the CAS commit write path return
//     codes.Unavailable; reads continue) and acks. Coordinator WAITS for ALL
//     acks (the freeze barrier). Any failure / timeout -> ABORT.
//  2. BISECT  - each node bisects the gen-g units IT owns into fresh gen-(g+1)
//     units K and K+N in the SHARED backing (so the later redistribution is
//     copy-free). Frozen + the per-old-unit write-pause WRITE side drains any
//     in-flight writer => one quiesced copy pass, NO catch-up. Each node reports
//     done; coordinator WAITS for all.
//  3. FLIP    - coordinator RPCs all ReshardFlip(g+1). Each node ATOMICALLY
//     advances its genState to gen g+1 and RETIRES its old gen-g units
//     (CloseUnit). Each acks; coordinator WAITS for all. No node flips until
//     every node has bisected; the freeze still holds.
//  4. RESUME  - coordinator RPCs all ReshardResume, RETRYING any node that has
//     not yet acked until all do (bounded). Each node unfreezes; writes resume
//     at gen g+1. The 2N units then redistribute by the REUSED Phase 3 lease
//     handoff (the ring re-keys onto the 2N generation-qualified ids; reconcile
//     acquires/releases; copy-free). A node the coordinator never reaches with
//     RESUME is NOT stranded frozen forever: the self-heal loop auto-unfreezes a
//     node that flipped but stayed frozen past the RESUME-retry budget.
//
// FAIL-SAFE ABORT (bounded): if ANY node fails to ack a FREEZE / BISECT phase
// (error or timeout), or membership IDENTITY changes mid-reshard (any add /
// remove / count-preserving swap), the coordinator RPCs all ReshardAbort: every
// node UNFREEZES, discards any half-built gen-(g+1) units (harmless - never
// routed), and STAYS at gen g. The cluster never half-reshards. A node that
// crashed mid-reshard before its FLIP restarts at gen g (it never flipped) -
// consistent with the survivors. FLIP applies SEQUENTIALLY, so a failure after
// an earlier node already advanced leaves a documented STRADDLE (some nodes g,
// some g+1) ABORT cannot un-flip; the coordinator surfaces ErrReshardStraddle so
// recovery is known to be MANUAL, not a clean retry.
//
// Scope: multi-backend mode ONLY. The legacy per-node path and the single-node
// Reshard (Phase 4) are UNCHANGED - a single-node cluster skips this protocol
// entirely. Stable membership for the reshard's duration; a membership change
// mid-reshard ABORTS. R=1 only. Serialized by reshardMu (one reshard at a time).

package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Zamua/shale/pkg/ring"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrReshardStraddle is returned by Reshard when the FLIP barrier fails PARTWAY
// through: at least one node already advanced to gen g+1 before a later node
// failed to flip. ABORT cannot un-flip the node(s) that already advanced (un-
// flipping a committed generation is itself the inconsistency ABORT exists to
// prevent), so the cluster is left STRADDLING two generations. This is an out-
// of-scope residual (the same class as a concurrent membership change mid-
// reshard); recovery is MANUAL - re-drive the reshard once every node is
// reachable, not a clean automatic retry. The distinct sentinel lets the
// operator (and a test) tell a clean FLIP failure (nobody flipped, the cluster
// is cleanly at gen g) from a straddle.
var ErrReshardStraddle = errors.New("cluster: Reshard FLIP straddled generations: some nodes advanced to gen g+1 before a peer failed to flip; manual recovery required")

// reshardPhaseTimeout bounds how long the coordinator waits for any single
// node's ack of any single barrier phase. Past it, that node is treated as
// failed and the coordinator ABORTS. It is a var (not const) so tests can
// shrink it. Generous by default: the BISECT phase copies a node's units
// (bounded by their size), which dominates; FREEZE / FLIP / RESUME / ABORT are
// near-instant flag flips.
var reshardPhaseTimeout = 30 * time.Second

// resumeRetryTimeout bounds how long the coordinator keeps re-issuing RESUME to
// the nodes that have not yet acked before giving up (a node still unreachable
// past it relies on the node-side stale-freeze self-heal). resumeRetryBackoff is
// the sleep between RESUME retry rounds. Both var so tests can shrink them. The
// RESUME retry exists because a node left frozen after FLIP would otherwise
// reject every write FOREVER: the self-heal reconcile short-circuits while
// frozen, and a later reshard toward g+2 is rejected by an already-flipped node
// frozen toward g+1.
var (
	resumeRetryTimeout = 15 * time.Second
	resumeRetryBackoff = 200 * time.Millisecond
)

// staleFreezeGrace is how long a node must sit flipped-but-frozen (advanced to
// its freeze target but never RESUMEd) before the self-heal loop auto-unfreezes
// it. Strictly longer than resumeRetryTimeout so the self-heal never races a
// coordinator still retrying RESUME: only a genuinely stranded node (its
// coordinator died, or the node missed every retry) ages past it. var so tests
// can shrink it.
var staleFreezeGrace = 25 * time.Second

// -- The per-node write-freeze flag ----------------------------------------

// isFrozen reports whether this node is currently write-frozen by an in-flight
// cluster-wide reshard. The WRITE paths consult it (Put / Delete / Begin / the
// CAS commit write path) and refuse with the retryable acquiring-window error
// while true; READS never consult it. Cheap: one short locked bool read, taken
// only on the multi-backend write path (legacy mode never freezes).
func (c *Cluster) isFrozen() bool {
	c.freezeMu.Lock()
	defer c.freezeMu.Unlock()
	return c.frozen
}

// errWriteFrozen is the FREEZE refusal: this node's writes are paused for the
// (brief) duration of a cluster-wide reshard. It REUSES the handoff-window
// shape (codes.Unavailable) so the existing client / forwarding retry logic
// treats it as transient backoff-and-retry - the same family as the Phase 3
// acquiring-window and the v0.3 migration-guard retries. The write was NEVER
// acked, so the client retries and succeeds after RESUME at the new generation;
// NO ACKED WRITE IS LOST because a frozen write never returns success.
func errWriteFrozen(op string) error {
	return status.Errorf(codes.Unavailable,
		"cluster: %s: writes are frozen for a cluster-wide reshard; retry shortly", op)
}

// -- The idempotent per-node phase handlers --------------------------------
//
// Each handler is the receiving side of one ReshardControl phase. They are
// idempotent (a repeated FREEZE while already frozen for the same target is a
// no-op, etc.) so a coordinator retry or a duplicated RPC cannot corrupt state.
// They are called from rpc.Server.ReshardControl (which delegates here) and,
// for the coordinator's own node, directly from the coordinator loop (the
// coordinator is also a participant).

// reshardFreeze enters the write-freeze targeting generation targetGen. Writes
// begin returning errWriteFrozen; reads continue. Idempotent: re-freezing for
// the same target is a no-op. Rejects a target that does not equal this node's
// current generation + 1 (a crossed / stale reshard) so a frozen node can only
// be driven toward the immediately-next generation. Multi-backend mode only.
//
// TOLERATES A STRANDED FREEZE: a node frozen toward a target it has ALREADY
// flipped past (freezeTargetGen <= curGen) is stranded - it flipped but a
// dropped RESUME left it frozen. Because the freeze gate is keyed by curGen+1,
// such a node has freezeTargetGen < targetGen, which the same-target idempotency
// check below would otherwise reject, deadlocking every future reshard. So a new
// FREEZE toward curGen+1 is allowed to TAKE OVER the stranded freeze (re-stamp
// the target + entry time) rather than refuse. The only freeze we refuse is one
// toward a DIFFERENT not-yet-flipped target (freezeTargetGen == curGen+1 but !=
// targetGen, which cannot happen given the curGen+1 gate, kept defensively).
//
// BARRIER POINT 1 (FREEZE): once this returns nil, this node acks no new write
// until RESUME / ABORT. The coordinator collecting every node's ack establishes
// the cluster-wide freeze barrier: from that instant there are no concurrent
// writes anywhere, so the subsequent bisect is a static copy.
func (c *Cluster) reshardFreeze(targetGen storageunit.Generation) error {
	if !c.multi {
		return fmt.Errorf("cluster: ReshardFreeze is only valid in multi-backend mode")
	}
	if c.notReady() {
		return fmt.Errorf("cluster: ReshardFreeze on a closed cluster")
	}
	curGen := c.genSnapshot().gen
	if targetGen != curGen+1 {
		return fmt.Errorf("cluster: ReshardFreeze target gen %d does not match current gen %d + 1", targetGen, curGen)
	}
	c.freezeMu.Lock()
	defer c.freezeMu.Unlock()
	if c.frozen && c.freezeTargetGen != targetGen && c.freezeTargetGen > curGen {
		// Frozen toward a DIFFERENT target the node has NOT flipped past: a genuine
		// crossed reshard, refuse. (A stranded freeze has freezeTargetGen <= curGen
		// and falls through to take over.)
		return fmt.Errorf("cluster: ReshardFreeze already frozen toward gen %d, refusing %d", c.freezeTargetGen, targetGen)
	}
	if !c.frozen || c.freezeTargetGen != targetGen {
		// Stamp the freeze-entry time when newly freezing OR when taking over a
		// stranded freeze toward a different (already-flipped-past) target. A
		// re-freeze for the SAME target keeps the original timestamp so the
		// stale-freeze age reflects how long the node has actually been frozen.
		c.frozenAt = time.Now()
	}
	c.frozen = true
	c.freezeTargetGen = targetGen
	return nil
}

// reshardBisect runs the local per-node bisect under the freeze: for each
// gen-g unit this node currently mounts, create its two fresh gen-(g+1)
// children in the shared backing and copy K's keys into them, split by the new
// hash bit (ChildUnit). It REUSES the Phase 4 bisect mechanics (open children,
// copyUnitInto) WITHOUT the catch-up / cut-over: with writes frozen cluster-wide
// the source is logically static, so one DRAINED copy pass captures every
// (already-acked) write and there is nothing to catch up. The drain still takes
// the per-old-unit write-pause WRITE side around the copy (bisectUnitStatic) to
// quiesce any writer that slipped past the freeze flag before it flipped - the
// freeze stops new writers, the WRITE-lock waits out the in-flight ones, so the
// copy is provably quiescent. The old gen-g units stay mounted (still serving
// reads) until FLIP retires them.
//
// Refuses if not frozen for targetGen (a bisect must run under the freeze, or
// concurrent writes could be lost). Idempotent enough for a coordinator retry:
// re-creating a child that already exists in the shared backing CLEARS it then
// re-copies the same drained bytes (a deterministic image of old-K, deletes
// included); the children are not routed until FLIP, so a partial-then-retried
// bisect is harmless. Multi-backend mode only.
//
// BARRIER POINT 2 (BISECT): the coordinator waits for every node's bisect-done
// ack before issuing FLIP, so no node advances its generation until all gen-g
// data has been copied into the gen-(g+1) units everywhere.
func (c *Cluster) reshardBisect(targetGen storageunit.Generation) error {
	if !c.multi {
		return fmt.Errorf("cluster: ReshardBisect is only valid in multi-backend mode")
	}
	if c.notReady() {
		return fmt.Errorf("cluster: ReshardBisect on a closed cluster")
	}
	if !c.frozenFor(targetGen) {
		return fmt.Errorf("cluster: ReshardBisect requires the node be frozen toward gen %d", targetGen)
	}
	start := c.genSnapshot()
	if targetGen != start.gen+1 {
		return fmt.Errorf("cluster: ReshardBisect target gen %d does not match current gen %d + 1", targetGen, start.gen)
	}
	nextCount, err := start.count.Double()
	if err != nil {
		return fmt.Errorf("cluster: ReshardBisect cannot double %s: %w", start.count, err)
	}

	// Bisect exactly the gen-g units this node currently mounts. Each node only
	// bisects the units it locally owns; the new children land on their ring
	// owners via the post-RESUME reconcile. Stable membership for the reshard's
	// duration means this set does not move under us.
	for _, k := range c.mountedOldUnits(start.gen) {
		if err := c.bisectUnitStatic(start.gen, k, start.count, nextCount); err != nil {
			return fmt.Errorf("cluster: ReshardBisect unit %d: %w", k, err)
		}
	}
	return nil
}

// reshardFlip atomically advances this node to the target generation and
// retires its old gen-g units. After this returns, routing on this node
// resolves the 2N gen-(g+1) units directly (the steady-state gen-(g+1) map, no
// cut-over indirection) and the gen-g databases are closed. The freeze STILL
// HOLDS (RESUME clears it), so no write lands during the flip.
//
// Refuses if not frozen for targetGen (the flip must happen under the freeze)
// or if the bisect did not run (the gen-(g+1) children must exist before
// routing resolves them). Idempotent: a node already at targetGen is a no-op.
// Multi-backend mode only.
//
// BARRIER POINT 3 (FLIP): the coordinator issues FLIP only after EVERY node
// has bisected, so no node routes at the new generation while another still
// holds un-bisected gen-g data. Retirement (CloseUnit of the gen-g units)
// happens here, under the freeze, after the bisect captured every gen-g write -
// so no acked write is ever routed to a unit being retired.
func (c *Cluster) reshardFlip(targetGen storageunit.Generation) error {
	if !c.multi {
		return fmt.Errorf("cluster: ReshardFlip is only valid in multi-backend mode")
	}
	if c.notReady() {
		return fmt.Errorf("cluster: ReshardFlip on a closed cluster")
	}
	start := c.genSnapshot()
	if start.gen == targetGen {
		// Already flipped (idempotent retry). Nothing to do.
		return nil
	}
	if !c.frozenFor(targetGen) {
		return fmt.Errorf("cluster: ReshardFlip requires the node be frozen toward gen %d", targetGen)
	}
	if targetGen != start.gen+1 {
		return fmt.Errorf("cluster: ReshardFlip target gen %d does not match current gen %d + 1", targetGen, start.gen)
	}
	nextCount, err := start.count.Double()
	if err != nil {
		return fmt.Errorf("cluster: ReshardFlip cannot double %s: %w", start.count, err)
	}

	// Snapshot the gen-g units to retire BEFORE advancing the generation: once
	// the genState is at gen+1, mountedOldUnits(start.gen) would no longer be
	// the routing-relevant set. The children created by the bisect are already
	// in the mount map keyed by their gen-(g+1) GenUnit, so advancing the
	// generation makes routing resolve them immediately.
	oldUnits := c.mountedOldUnits(start.gen)

	// ATOMIC FLIP: publish the gen-(g+1) routing state in one store. From this
	// instant resolveGenUnit returns GenUnit{gen+1, UnitForHash(h, 2N)} for
	// every key (cutOver empty: the whole space is now at the new generation),
	// and the mount-map lookup finds the gen-(g+1) child the bisect created.
	c.commitGenState(genState{
		gen:     targetGen,
		count:   nextCount,
		cutOver: make(map[storageunit.UnitID]struct{}),
	})

	// Retire the old gen-g units: their key-space now resolves to the gen-(g+1)
	// children, so close + free the gen-g databases. CloseUnit at the OLD
	// generation does not touch the distinct gen-(g+1) children.
	for _, k := range oldUnits {
		oldGU := storageunit.NewGenUnit(start.gen, k)
		c.mountMu.Lock()
		delete(c.mountMap, oldGU)
		c.mountMu.Unlock()
		_ = c.factory.CloseUnit(oldGU)
	}
	return nil
}

// reshardResume clears the write-freeze and re-keys the ring onto the 2N
// gen-(g+1) ids so the Phase 3 reconcile redistributes the new units across
// nodes by lease handoff (copy-free - the children already live in the shared
// backing). Writes resume at the new generation the instant the freeze clears.
// Idempotent: clearing an already-clear freeze is a no-op. Multi-backend only.
//
// BARRIER POINT 4 (RESUME): the coordinator issues RESUME only after every
// node has flipped, so when writes resume every node is at gen g+1 and routes
// to the 2N units consistently. A write that was refused during the freeze
// now succeeds on the client's retry, at the new generation.
func (c *Cluster) reshardResume() error {
	if !c.multi {
		return fmt.Errorf("cluster: ReshardResume is only valid in multi-backend mode")
	}
	c.unfreeze()
	// Re-key the ring onto the 2N gen-(g+1) ids: the Phase 3 reconcile
	// acquires/releases so the new units redistribute by lease handoff. In
	// single-node mode this is a no-op (self owns everything), but a multi-node
	// resume schedules a debounced reconcile on every node. Cheap + idempotent.
	if c.ring != nil {
		c.bumpRingGen()
	}
	return nil
}

// reshardAbort is the fail-safe: unfreeze + discard any half-built gen-(g+1)
// units this node created during a bisect that did not reach FLIP, and STAY at
// the current generation. A node that has NOT flipped (still at gen g) discards
// children that were never routed (routing only resolves them after FLIP
// advances the generation), so dropping them is harmless. Idempotent.
// Multi-backend mode only.
//
// ABORT keeps a node consistent with itself: it stays at whatever generation it
// has actually committed. For a node that aborts BEFORE its FLIP that is gen g,
// so the cluster does not half-reshard. ABORT is reachable on a FLIP-phase error
// too (broadcastPhase applies FLIP sequentially and returns on the first failed
// node), and in THAT case the abort can find an EARLIER node already flipped to
// gen g+1: discardGenUnits keys on the node's OWN current generation, so a
// flipped node discards its now-stale gen-g units (not its live gen-(g+1) ones)
// and stays at g+1 - it does NOT un-flip. A partial-FLIP therefore leaves the
// cluster STRADDLING two generations (some nodes g, some g+1). That is an
// out-of-scope residual (the same class as a mid-reshard membership change): the
// coordinator surfaces a DISTINCT error so the operator knows recovery is manual
// (re-drive the reshard once the unreachable node is back), not a clean retry. A
// two-phase prepare/commit FLIP that would make FLIP atomic across nodes is a
// longer-term option, deliberately out of scope here.
func (c *Cluster) reshardAbort(targetGen storageunit.Generation) error {
	if !c.multi {
		return fmt.Errorf("cluster: ReshardAbort is only valid in multi-backend mode")
	}
	c.unfreeze()
	// Discard any gen-(g+1) children this node built during the bisect. They
	// are not routed (we stay at gen g), so closing + dropping them only frees
	// resources. The gen-g units this node still mounts are untouched, so the
	// cluster keeps serving at gen g.
	cur := c.genSnapshot().gen
	c.discardGenUnits(targetGen, cur)
	return nil
}

// TestingIsFrozen reports whether this node is currently write-frozen for an
// in-flight cluster-wide reshard. Test-only white-box hook (the production
// freeze check isFrozen is unexported); it lets an integration test in another
// package assert the read-availability + abort-path freeze semantics. Follows
// the Testing* convention (see TestingClearMount / TestingDropAllPeerClients).
func (c *Cluster) TestingIsFrozen() bool {
	c.freezeMu.Lock()
	defer c.freezeMu.Unlock()
	return c.frozen
}

// TestingUnfreeze clears the write-freeze WITHOUT advancing the generation or
// touching units. Test-only white-box hook used by the multi-node reshard gate's
// break-demonstration to model a node that fails to honor the freeze (lets a
// write through mid-barrier). NO production path unfreezes this way; RESUME /
// ABORT are the real un-freeze. Follows the Testing* convention.
func (c *Cluster) TestingUnfreeze() {
	c.unfreeze()
}

// TestingRunReconcile runs ONE reconcile pass (the same runReconcile the periodic
// self-heal loop ticks), synchronously. Test-only white-box hook: it lets a test
// drive the stale-freeze self-heal (a node left frozen by a dropped RESUME RPC
// auto-unfreezes once it has aged past staleFreezeGrace) on demand instead of
// waiting out the production reconcile interval. Follows the Testing* convention
// (see TestingClearMount / TestingDropAllPeerClients).
func (c *Cluster) TestingRunReconcile() {
	c.runReconcile()
}

// TestingSetStaleFreezeGrace overrides the stale-freeze grace window (how long a
// node must sit flipped-but-frozen before the self-heal auto-unfreezes it) and
// returns a restore func the caller MUST defer. Test-only white-box hook so an
// integration test in another package can shrink the grace from its 25s default
// to a few milliseconds and exercise the self-heal without a long sleep. The
// production default is unchanged for every non-test path. Follows the Testing*
// convention.
func TestingSetStaleFreezeGrace(d time.Duration) (restore func()) {
	prev := staleFreezeGrace
	staleFreezeGrace = d
	return func() { staleFreezeGrace = prev }
}

// TestingSetAfterFreezeHook installs fn (or clears it with nil) as the coordinator
// post-FREEZE / pre-membership-re-check seam reshardCoordinated invokes. Test-only
// white-box hook: it lets a test inject a membership change across the freeze
// barrier deterministically and assert the coordinator ABORTS, rather than racing
// a real node join/leave against the phase timing. Follows the Testing*
// convention.
func (c *Cluster) TestingSetAfterFreezeHook(fn func()) {
	c.testingAfterFreezeHook = fn
}

// TestingSetRingMembers replaces the local node's ring membership with the given
// members (used to model a count-preserving membership swap mid-reshard: the
// coordinator's Members() reads the ring, so swapping a member here makes the
// barrier's membershipChanged re-check observe an identity change). Test-only
// white-box hook; multi-node mode only (no-op if the ring is nil). Follows the
// Testing* convention.
func (c *Cluster) TestingSetRingMembers(members []ring.Member) {
	if c.ring == nil {
		return
	}
	r := ring.New()
	for _, m := range members {
		r.Add(m)
	}
	c.ring = r
}

// frozenFor reports whether this node is frozen AND its freeze target is exactly
// targetGen. The phase handlers gate on this so a bisect / flip can only run
// under the freeze the coordinator established for this very reshard.
func (c *Cluster) frozenFor(targetGen storageunit.Generation) bool {
	c.freezeMu.Lock()
	defer c.freezeMu.Unlock()
	return c.frozen && c.freezeTargetGen == targetGen
}

// unfreeze clears the write-freeze. RESUME and ABORT both call it. Idempotent.
func (c *Cluster) unfreeze() {
	c.freezeMu.Lock()
	c.frozen = false
	c.freezeTargetGen = 0
	c.frozenAt = time.Time{}
	c.freezeMu.Unlock()
}

// clearStaleFreeze auto-unfreezes a node STRANDED frozen after FLIP: the node
// already advanced to (or past) its freeze target generation but never received
// RESUME (a dropped RESUME RPC, or a coordinator that died mid-protocol), so it
// would reject every write forever. It clears such a freeze ONLY when:
//
//   - the node is frozen, AND
//   - it has already FLIPPED to (or past) the freeze target (curGen >=
//     freezeTargetGen), so no bisect is in flight - the data is committed at the
//     new generation and writes are safe to resume, AND
//   - it has been in that flipped-but-frozen state longer than staleFreezeGrace
//     (longer than the coordinator's RESUME retry budget), so a normal in-flight
//     RESUME is never raced.
//
// A node still AT gen g frozen toward g+1 (mid-FREEZE / mid-BISECT) has curGen <
// freezeTargetGen and is left alone - clearing it would break the barrier. It
// returns whether it cleared a stale freeze (the caller then proceeds to the
// post-flip reconcile + ring re-key the missed RESUME would have triggered).
func (c *Cluster) clearStaleFreeze() bool {
	curGen := c.genSnapshot().gen
	c.freezeMu.Lock()
	stale := c.frozen &&
		curGen >= c.freezeTargetGen &&
		!c.frozenAt.IsZero() &&
		time.Since(c.frozenAt) > staleFreezeGrace
	if stale {
		c.frozen = false
		c.freezeTargetGen = 0
		c.frozenAt = time.Time{}
	}
	c.freezeMu.Unlock()
	return stale
}

// discardGenUnits closes + drops every mounted unit at generation gen (used by
// ABORT to throw away half-built gen-(g+1) children). It only touches units at
// the named generation, so an ABORT before FLIP (which discards the gen-(g+1)
// children) never touches the still-live gen-g units the cluster keeps serving.
func (c *Cluster) discardGenUnits(gen, keepGen storageunit.Generation) {
	if gen == keepGen {
		// Defensive: never discard the generation the node is staying at.
		return
	}
	c.mountMu.Lock()
	toDrop := make([]storageunit.GenUnit, 0)
	for gu := range c.mountMap {
		if gu.Gen == gen {
			toDrop = append(toDrop, gu)
		}
	}
	for _, gu := range toDrop {
		delete(c.mountMap, gu)
	}
	c.mountMu.Unlock()
	for _, gu := range toDrop {
		_ = c.factory.CloseUnit(gu)
	}
}

// bisectUnitStatic creates the two gen-(g+1) children of old unit K and copies
// K's keys into them under the cluster-wide freeze. It is the STATIC-copy form
// of bisectUnit (Phase 4): because writes are frozen cluster-wide, the source
// is (logically) not moving, so there is NO catch-up drain and NO cut-over -
// one copy pass captures every acked write. It REUSES the Phase 4 copyUnitInto
// split-by-ChildUnit mechanics verbatim. The old unit is NOT retired here (FLIP
// does that for all units at once); it stays mounted serving reads until the
// flip.
//
// THE IN-FLIGHT-WRITER DRAIN (closes the freeze-vs-bisect race): the freeze flag
// is a bool flip, not a synchronous quiesce. A writer that read isFrozen()==false
// a moment BEFORE the flag flipped is already past the gate and may be mid-Put
// into old-K (the routed write path holds the per-old-unit write-pause READ side
// across b.Put - see localWriteBackendForKey). If the copy scanned past that
// key before the Put landed, the Put would ack, FLIP would retire old-K, and the
// value would be orphaned -> LOST. So this takes the per-old-unit write-pause
// WRITE side around the copy, exactly as single-node bisectUnit does: acquiring
// the WRITE side blocks until every in-flight writer to K's key-space has
// released its READ side, so the source is PROVABLY quiescent for the copy pass.
// Once the freeze flag is set, no NEW writer can pass the gate, so the WRITE-lock
// acquisition is bounded by the in-flight set draining.
//
// CLEAR-BEFORE-COPY (no deleted-key resurrection on abort+retry): the gen-(g+1)
// children may already hold bytes from an ABORTED earlier reshard run that
// CloseUnit'd (but did not wipe) them; re-OpenUnit re-attaches the SAME durable
// bytes. copyUnitInto only Puts present keys, so a key deleted between the
// aborted run and this retry would survive in the child and be resurrected at
// FLIP. Clear both children first (match single-node bisectUnit's catch-up
// clear+copy) so the child is an EXACT image of old-K, deletes included.
func (c *Cluster) bisectUnitStatic(gen storageunit.Generation, k storageunit.UnitID, oldCount, newCount storageunit.UnitCount) error {
	oldGU := storageunit.NewGenUnit(gen, k)
	low, high, err := storageunit.ChildUnits(k, oldCount)
	if err != nil {
		return fmt.Errorf("child units of %d: %w", k, err)
	}
	lowGU := storageunit.NewGenUnit(gen+1, low)
	highGU := storageunit.NewGenUnit(gen+1, high)

	// CREATE the two fresh gen-(g+1) children in the SHARED backing (epoch base:
	// brand-new databases, no prior writer to fence). Mount them so the copy can
	// write into them AND so routing resolves them once FLIP advances the
	// generation. Creating in the shared backing is what makes the later
	// redistribution copy-free: a child this node built is the SAME durable
	// bytes the eventual ring owner will open.
	lowBE, err := c.factory.OpenUnit(lowGU, epochAtOpen)
	if err != nil {
		return fmt.Errorf("open child %s: %w", lowGU, err)
	}
	highBE, err := c.factory.OpenUnit(highGU, epochAtOpen)
	if err != nil {
		_ = c.factory.CloseUnit(lowGU)
		return fmt.Errorf("open child %s: %w", highGU, err)
	}
	c.mountMu.Lock()
	c.mountMap[lowGU] = lowBE
	c.mountMap[highGU] = highBE
	c.mountMu.Unlock()

	// The source backend (old gen-g unit K), which keeps serving reads.
	c.mountMu.RLock()
	src, ok := c.mountMap[oldGU]
	c.mountMu.RUnlock()
	if !ok {
		return fmt.Errorf("old unit %s not mounted", oldGU)
	}

	// DRAIN in-flight writers to K's key-space: take old-K's write-pause WRITE
	// side. The routed write path holds the READ side across b.Put, so the WRITE
	// side cannot be granted until every writer that slipped past the freeze flag
	// has finished its Put. With the freeze set, no new writer enters; once we
	// hold the WRITE lock the source is quiescent for the whole copy below.
	pause := c.pauseLockFor(k)
	pause.Lock()
	defer pause.Unlock()

	// CLEAR the children before the copy so a retry after an aborted run starts
	// from an empty child (a key deleted since the aborted run does not survive),
	// matching single-node bisectUnit's clear+copy. Cheap on a fresh child.
	if err := clearBackend(lowBE); err != nil {
		return fmt.Errorf("clear child %s: %w", lowGU, err)
	}
	if err := clearBackend(highBE); err != nil {
		return fmt.Errorf("clear child %s: %w", highGU, err)
	}

	// STATIC COPY: split old-K's keys into the two children by the new hash bit.
	// With the write-pause held the source cannot change under us: this ONE pass
	// captures every acked write. NO catch-up drain is needed beyond the WRITE-
	// lock quiesce (that is the point of the freeze - the single-node bisect's
	// background-copy-then-catch-up dance collapses to one drained pass).
	if err := c.copyUnitInto(src, lowBE, highBE, oldCount, newCount); err != nil {
		return fmt.Errorf("copy %s: %w", oldGU, err)
	}
	return nil
}

// -- The coordinator -------------------------------------------------------

// reshardCoordinated drives the cluster-wide-freeze barrier across every node
// (including self) when Reshard() is called on a MULTI-NODE cluster. It is the
// COORDINATOR. The caller (Reshard) already holds reshardMu, so two reshards
// cannot overlap. The flow targets targetGen = g+1:
//
//	FREEZE-barrier -> wait all acks -> BISECT-barrier -> wait all acks
//	-> FLIP-barrier -> wait all acks -> RESUME
//
// ABORT on any failure / timeout / membership change BEFORE the FLIP barrier
// completes. Once every node has flipped, the coordinator is committed to
// RESUME (there is nothing left that could lose a write, and un-flipping a node
// that already advanced its generation would itself be the inconsistency ABORT
// exists to prevent).
//
// Membership stability: the participant set is snapshotted once at the start.
// Before each barrier the coordinator re-checks that membership has not changed
// (a join / leave mid-reshard); if it has, it ABORTS (retry later) rather than
// freeze/flip a set that no longer matches the ring.
func (c *Cluster) reshardCoordinated() error {
	start := c.genSnapshot()
	// Up-front ceiling check: a reshard either fully proceeds or fully refuses
	// before any node freezes; it never half-reshards and then discovers it
	// cannot double.
	if _, err := start.count.Double(); err != nil {
		return fmt.Errorf("cluster: Reshard cannot double %s: %w", start.count, err)
	}
	targetGen := start.gen + 1

	// Snapshot the participant set (every member, including self). Stable
	// membership for the reshard's duration is a scope assumption; we still
	// re-check before each barrier and ABORT on any drift.
	members := c.reshardMembers()
	if len(members) < 2 {
		// Defensive: the single-node path handles len < 2 directly (Reshard
		// routes here only for len > 1). Treat a shrink-to-one as a membership
		// change and refuse rather than run a degenerate barrier.
		return fmt.Errorf("cluster: Reshard coordinated flow needs >1 member, saw %d", len(members))
	}
	// Snapshot the member IDENTITY set, not just the count. A count-only check
	// misses a count-preserving swap (member X leaves, member Y joins): the
	// coordinator would drive the STALE set, Y would never reshard, and the
	// cluster would straddle. Comparing the id set before each barrier aborts on
	// ANY add/remove, even at equal count.
	memberSet := make(map[string]struct{}, len(members))
	for _, m := range members {
		memberSet[m.id] = struct{}{}
	}

	// PHASE 1 - FREEZE barrier. Freeze every node; wait for ALL acks. Any
	// failure -> ABORT (unfreeze everyone; the cluster stays at gen g).
	if err := c.broadcastPhase(members, pb.ReshardPhase_RESHARD_PHASE_FREEZE, targetGen); err != nil {
		c.abortReshard(members, targetGen)
		return fmt.Errorf("cluster: Reshard FREEZE barrier failed: %w", err)
	}

	// Test-only seam: a hook fires here (post-FREEZE, pre-membership-re-check) so
	// a test can deterministically inject a membership change across the barrier
	// and prove the coordinator ABORTS on it. Nil in production.
	if c.testingAfterFreezeHook != nil {
		c.testingAfterFreezeHook()
	}

	// Membership must be stable across the bisect: re-check before BISECT.
	if changed, err := c.membershipChanged(memberSet); changed {
		c.abortReshard(members, targetGen)
		return fmt.Errorf("cluster: Reshard aborted, membership changed after FREEZE: %w", err)
	}

	// PHASE 2 - BISECT barrier. Each node statically copies its gen-g units into
	// fresh gen-(g+1) children. Wait for ALL bisect-done acks before any flip.
	if err := c.broadcastPhase(members, pb.ReshardPhase_RESHARD_PHASE_BISECT, targetGen); err != nil {
		c.abortReshard(members, targetGen)
		return fmt.Errorf("cluster: Reshard BISECT barrier failed: %w", err)
	}

	if changed, err := c.membershipChanged(memberSet); changed {
		c.abortReshard(members, targetGen)
		return fmt.Errorf("cluster: Reshard aborted, membership changed after BISECT: %w", err)
	}

	// PHASE 3 - FLIP barrier. Every node atomically advances to gen g+1 and
	// retires its gen-g units. Still under the freeze; no write lands. A FLIP
	// failure triggers ABORT (the documented fail-safe). No ACKED WRITE is lost
	// either way: the bisect already copied every gen-g write into the gen-(g+1)
	// children in the SHARED backing, and the freeze still holds (no new writes).
	//
	// FLIP applies SEQUENTIALLY and returns on the first failed node (acked is the
	// count that flipped before the failure). Two failure shapes:
	//   - acked == 0: the FIRST node failed to flip, so NO node advanced. ABORT
	//     leaves every node cleanly at gen g; a plain retry is safe.
	//   - acked  > 0: an EARLIER node already advanced to gen g+1 before a later
	//     node failed. ABORT cannot un-flip the advanced node (un-flipping a
	//     committed generation is the very inconsistency ABORT prevents), so the
	//     cluster STRADDLES two generations. We still ABORT (the un-flipped nodes
	//     discard their gen-(g+1) children and stay at gen g; the flipped node
	//     keeps its gen-(g+1) units, whose durable bytes persist in the shared
	//     backing), but we surface the DISTINCT ErrReshardStraddle so the operator
	//     knows recovery is manual (re-drive once the failed node is reachable),
	//     not a clean retry. This is the same out-of-scope class as a concurrent
	//     membership change mid-reshard.
	if acked, err := c.broadcastFlip(members, targetGen); err != nil {
		c.abortReshard(members, targetGen)
		if acked > 0 {
			return fmt.Errorf("cluster: Reshard FLIP barrier failed after %d node(s) flipped: %w: %w", acked, ErrReshardStraddle, err)
		}
		return fmt.Errorf("cluster: Reshard FLIP barrier failed (no node flipped; cluster stays at gen %d): %w", start.gen, err)
	}

	// PHASE 4 - RESUME. Every node flipped; unfreeze everywhere so writes resume
	// at gen g+1, then the 2N units redistribute via the Phase 3 lease handoff.
	// RESUME is RETRIED until every node acks (bounded by resumeRetryTimeout): a
	// node left frozen would return Unavailable on every write FOREVER (the self-
	// heal reconcile short-circuits while frozen and a later reshard toward g+2
	// would be rejected by an already-flipped freeze), so "self-heal" is not a
	// claim we can make on the RESUME RPC alone. RESUME is idempotent (clearing an
	// already-clear freeze is a no-op), so retrying only the still-frozen nodes is
	// safe. The node-side self-heal (auto-unfreeze of a stale freeze on a flipped
	// node) is the SECOND line of defense for a node the coordinator never reaches
	// at all (it crashed and restarted past the retry budget); together they make
	// the stuck-frozen window bounded, not permanent.
	if err := c.resumeUntilAcked(members, targetGen); err != nil {
		return fmt.Errorf("cluster: Reshard RESUME incomplete (cluster is at gen %d; remaining frozen node(s) auto-unfreeze via self-heal): %w", targetGen, err)
	}
	return nil
}

// resumeUntilAcked drives RESUME to every node, RETRYING the nodes that have not
// yet acked until all do or resumeRetryTimeout elapses. Unlike a one-shot
// broadcast, a transient blip on one node's RESUME does not strand it frozen:
// the coordinator keeps re-issuing RESUME (idempotent) until it sticks. Returns
// the last error if some node is still unreachable past the budget (that node
// then relies on the node-side stale-freeze self-heal).
func (c *Cluster) resumeUntilAcked(members []memberRPC, targetGen storageunit.Generation) error {
	deadline := time.Now().Add(resumeRetryTimeout)
	pending := append([]memberRPC(nil), members...)
	var lastErr error
	for {
		var stillPending []memberRPC
		lastErr = nil
		for _, m := range pending {
			if err := c.sendPhase(m, pb.ReshardPhase_RESHARD_PHASE_RESUME, targetGen); err != nil {
				lastErr = fmt.Errorf("node %s RESUME: %w", m.id, err)
				stillPending = append(stillPending, m)
			}
		}
		if len(stillPending) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		pending = stillPending
		time.Sleep(resumeRetryBackoff)
	}
}

// abortReshard RPCs ABORT to every node (best-effort: an unreachable node is
// the reason we are aborting, and its own restart leaves it at gen g). It is
// the bounded fail-safe: every reachable node unfreezes, discards any half-
// built gen-(g+1) children, and stays at gen g.
func (c *Cluster) abortReshard(members []memberRPC, targetGen storageunit.Generation) {
	// Best-effort: ignore per-node errors. A node we cannot reach to abort is
	// already failed; on restart it is at gen g (it never flipped), consistent.
	_ = c.broadcastPhaseBestEffort(members, pb.ReshardPhase_RESHARD_PHASE_ABORT, targetGen)
}

// broadcastPhase sends phase to every member and waits for ALL acks. It returns
// the first error (or timeout) any node reports; nil only if EVERY node acked
// success. This is the barrier: the coordinator does not proceed to the next
// phase until every node has applied this one.
func (c *Cluster) broadcastPhase(members []memberRPC, phase pb.ReshardPhase, targetGen storageunit.Generation) error {
	for _, m := range members {
		if err := c.sendPhase(m, phase, targetGen); err != nil {
			return fmt.Errorf("node %s phase %s: %w", m.id, phase, err)
		}
	}
	return nil
}

// broadcastFlip is broadcastPhase specialized for the FLIP barrier: it reports
// how many nodes ACKED the flip before any failure (acked), so the coordinator
// can tell a clean FLIP failure (acked == 0, nobody advanced) from a partial-
// FLIP straddle (acked > 0, some node already advanced and ABORT cannot un-flip
// it). On full success it returns (len(members), nil).
func (c *Cluster) broadcastFlip(members []memberRPC, targetGen storageunit.Generation) (acked int, err error) {
	for _, m := range members {
		if e := c.sendPhase(m, pb.ReshardPhase_RESHARD_PHASE_FLIP, targetGen); e != nil {
			return acked, fmt.Errorf("node %s phase %s: %w", m.id, pb.ReshardPhase_RESHARD_PHASE_FLIP, e)
		}
		acked++
	}
	return acked, nil
}

// broadcastPhaseBestEffort sends phase to every member, ignoring per-node
// errors, and returns the first error seen (for logging) without short-
// circuiting. Used by ABORT, where we want to reach every reachable node even
// if one is down.
func (c *Cluster) broadcastPhaseBestEffort(members []memberRPC, phase pb.ReshardPhase, targetGen storageunit.Generation) error {
	var firstErr error
	for _, m := range members {
		if err := c.sendPhase(m, phase, targetGen); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// sendPhase applies one barrier phase to one node. For the coordinator's OWN
// node it calls the local handler directly (no RPC, no self-dial); for a peer
// it dials the ReshardControl RPC. Either way a phase timeout bounds the wait.
func (c *Cluster) sendPhase(m memberRPC, phase pb.ReshardPhase, targetGen storageunit.Generation) error {
	if m.id == c.cfg.NodeID {
		// Local participant: apply directly. The coordinator is also a node
		// being driven through the barrier.
		return c.applyReshardPhase(phase, targetGen)
	}
	cli, err := c.clientFor(m.addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), reshardPhaseTimeout)
	defer cancel()
	resp, err := cli.ReshardControl(ctx, phase, uint64(targetGen))
	if err != nil {
		return err
	}
	if e := resp.GetError(); e != "" {
		return fmt.Errorf("%s", e)
	}
	return nil
}

// applyReshardPhase dispatches one barrier phase to its idempotent handler. It
// is the single entry point both the local coordinator path (sendPhase) and the
// gRPC server (rpc.Server.ReshardControl -> Cluster.ApplyReshardPhase) funnel
// through, so the phase semantics live in exactly one place.
func (c *Cluster) applyReshardPhase(phase pb.ReshardPhase, targetGen storageunit.Generation) error {
	switch phase {
	case pb.ReshardPhase_RESHARD_PHASE_FREEZE:
		return c.reshardFreeze(targetGen)
	case pb.ReshardPhase_RESHARD_PHASE_BISECT:
		return c.reshardBisect(targetGen)
	case pb.ReshardPhase_RESHARD_PHASE_FLIP:
		return c.reshardFlip(targetGen)
	case pb.ReshardPhase_RESHARD_PHASE_RESUME:
		return c.reshardResume()
	case pb.ReshardPhase_RESHARD_PHASE_ABORT:
		return c.reshardAbort(targetGen)
	default:
		return fmt.Errorf("cluster: unknown reshard phase %v", phase)
	}
}

// ApplyReshardPhase is the exported entry point the gRPC server delegates a
// ReshardControl RPC to. It validates the phase enum then runs the idempotent
// handler. Exported (capitalized) because pkg/rpc lives in a different package;
// it is cluster-internal in intent (only the peer coordinator calls it over the
// wire), never part of the app-facing surface.
func (c *Cluster) ApplyReshardPhase(phase pb.ReshardPhase, targetGen uint64) error {
	if phase == pb.ReshardPhase_RESHARD_PHASE_UNSPECIFIED {
		return fmt.Errorf("cluster: ReshardControl phase unspecified")
	}
	return c.applyReshardPhase(phase, storageunit.Generation(targetGen))
}

// memberRPC is the minimal coordinator view of a participant: its node id (to
// detect self) + its gRPC address (to dial). Built from c.Members() once at the
// start of the coordinated reshard.
type memberRPC struct {
	id   string
	addr string
}

// reshardMembers returns the coordinator's participant view as memberRPC
// values (id + addr). A thin wrapper over the public Members so the barrier
// code reads in terms of memberRPC; addr is the ring member's gRPC Addr.
func (c *Cluster) reshardMembers() []memberRPC {
	ms := c.Members()
	out := make([]memberRPC, 0, len(ms))
	for _, m := range ms {
		out = append(out, memberRPC{id: m.ID, addr: m.Addr})
	}
	return out
}

// membershipChanged reports whether the current member id SET differs from want
// (the identity set snapshotted at the start of the reshard). A join, a leave,
// OR a count-preserving swap (X leaves, Y joins) mid-reshard changes the set, so
// the coordinator ABORTS (concurrent membership-change + reshard is out of
// scope). Comparing identity, not just count, closes the swap gap a count-only
// check missed: a leave+join that keeps the count constant would otherwise pass
// and let the coordinator drive a stale set, leaving the joiner un-resharded and
// the cluster straddling. It returns a descriptive error alongside the bool for
// the abort message.
func (c *Cluster) membershipChanged(want map[string]struct{}) (bool, error) {
	now := c.Members()
	if len(now) != len(want) {
		return true, fmt.Errorf("member count changed %d -> %d", len(want), len(now))
	}
	for _, m := range now {
		if _, ok := want[m.ID]; !ok {
			return true, fmt.Errorf("member set changed: %s joined (or replaced a departed member) mid-reshard", m.ID)
		}
	}
	return false, nil
}
