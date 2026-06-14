// Lease-handoff rebalance for multi-backend mode (v0.8 Phase 3).
//
// This file holds the anti-entropy RECONCILE that makes a multi-backend
// node ACT on membership changes. When the ring re-assigns units, a unit
// whose owner changed hands off COPY-FREE: the old owner closes it (flush +
// release the lease), the new owner opens it at a strictly higher epoch
// (fencing the old). The bytes never move - they live in shared object
// storage - only the writer lease moves.
//
// THIS IS THE DATA-LOSS-SENSITIVE PHASE. It is built to one invariant:
//
//	NO ACKED WRITE MAY BE LOST.
//
// How the invariant holds (R=1 + AwaitDurable=true in multi mode):
//
//   - Every ACKED write is durable in object storage BEFORE its ack returns
//     (durable-before-ack). So at the instant a handoff begins, every write
//     the client got success for is already persisted in the unit's shared
//     backing store.
//   - The old owner's CloseUnit FLUSHES (durable) then releases, so any
//     write that was acked but still in a memtable is forced down first.
//   - The new owner's OpenUnit opens the SAME shared backing store, so it
//     observes every durable (hence every acked) write.
//   - In-flight (un-acked) writes may be fenced or lost, but the client
//     never got success for those and retries. That is allowed; losing an
//     ACKED write is not.
//
// Scope: multi-backend mode ONLY. The legacy per-node path and its v0.3
// Coordinator rebalance are UNTOUCHED. R=1 only (multi mode rejects R>1 per
// Phase 2); per-unit replication is a later phase.

package cluster

import (
	"time"

	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scheduleReconcile (re)arms the settle timer to run the unit reconcile in
// multi-backend mode. It is the multi-mode counterpart of scheduleEvaluate
// (the legacy v0.3 Coordinator path): bumpRingGen calls this instead when
// c.multi is true. Like scheduleEvaluate it debounces a burst of membership
// changes (rolling restart, several joins within a second) into one
// reconcile pass at settleDelay from the last change, rather than thrashing
// the mount map through intermediate ring shapes.
//
// Holds settleMu only long enough to swap the timer reference (the same
// lock + timer scheduleEvaluate uses, so the two never both arm a live
// timer - a cluster is in exactly one mode).
func (c *Cluster) scheduleReconcile() {
	if c.closed.Load() {
		return
	}
	c.settleMu.Lock()
	defer c.settleMu.Unlock()
	if c.settleTimer != nil {
		// Re-arm: a still-live (or already-firing) timer already owns a
		// pending obligation; the replacement inherits it. Do NOT
		// double-count. Mirrors scheduleEvaluate.
		c.settleTimer.Stop()
	} else {
		// Fresh arm: this reconcile is pending until the timer callback
		// below releases it, so WaitForRebalanceIdle blocks through it.
		c.settlePending.Add(1)
	}
	c.settleTimer = time.AfterFunc(c.settleDelay(), c.runScheduledReconcile)
}

// runScheduledReconcile is the settle-timer callback for multi-backend
// mode. It captures-and-clears the timer reference (so a concurrent
// scheduleReconcile whose re-arm Stop() raced this firing treats itself
// as a FRESH arm with its own pending increment), runs ONE reconcile
// pass, then releases the pending obligation this firing owned. The
// decrement lives here, NOT in runReconcile, because runReconcile is
// also invoked directly (the self-heal loop, TestingRunReconcile) by
// callers that never armed a pending obligation. By the time we
// decrement, reconcileUnits has synchronously applied the acquire/
// release set, so the settlePending -> applied-mounts handoff is
// seamless for a WaitForRebalanceIdle poller.
func (c *Cluster) runScheduledReconcile() {
	defer c.settlePending.Add(-1)
	c.settleMu.Lock()
	if c.settleTimer != nil {
		c.settleTimer = nil
	}
	c.settleMu.Unlock()
	c.runReconcile()
}

// runReconcile fires when the settle timer elapses in multi-backend mode (and
// from the slow self-heal loop). It runs one reconcile pass against the
// CURRENT ring + generation. Serialized by reconcileMu so two membership
// changes whose timers fire close together cannot interleave mounts (only one
// reconcile mutates the mount map at a time); a change that arrives mid-pass
// re-arms the timer and runs after.
//
// It also takes reshardMu (BEFORE reconcileMu) so a reconcile never runs
// concurrently with an in-flight Reshard: the resharder transiently mounts the
// gen-(g+1) children and holds the gen-g old units until cut-over, and a
// reconcile diffing against the live generation mid-bisect could release them.
// The resharder itself, already holding reshardMu, calls reconcileUnits
// directly (not runReconcile) after the generation has advanced. Lock order
// is reshardMu -> reconcileMu everywhere to avoid deadlock.
func (c *Cluster) runReconcile() {
	if c.closed.Load() {
		return
	}
	c.reshardMu.Lock()
	defer c.reshardMu.Unlock()
	// FREEZE guard (v0.8 multi-node reshard): on a NON-coordinator node the
	// barrier phase handlers (FREEZE / BISECT / FLIP) run via RPC WITHOUT
	// holding this node's reshardMu, so reshardMu alone does not exclude the
	// self-heal reconcile here from the in-flight bisect. While frozen, the
	// bisect owns the mount map (it transiently mounts the gen-(g+1) children
	// the live-generation reconcile would otherwise see as mounted-but-not-
	// desired and RELEASE). Skip the reconcile until RESUME / ABORT unfreezes;
	// RESUME then bumps the ring generation, which schedules a fresh reconcile
	// that does the post-flip redistribution against the advanced generation.
	//
	// STALE-FREEZE SELF-HEAL: first try to clear a freeze that is STRANDED - a
	// node that FLIPPED to its target generation but never got RESUME (a dropped
	// RESUME RPC) would otherwise reject every write forever. clearStaleFreeze
	// only clears a freeze the node has already flipped past AND that has aged
	// beyond the coordinator's RESUME retry budget, so it never races a normal
	// in-flight reshard. When it clears one, do the ring re-key the missed RESUME
	// would have done (bumpRingGen) and fall through to the post-flip reconcile.
	if c.isFrozen() {
		if !c.clearStaleFreeze() {
			return
		}
		if c.ring != nil {
			c.bumpRingGen()
		}
	}
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()
	c.reconcileUnits()
}

// reconcileUnits is the anti-entropy heart of Phase 3. It makes this node's
// MOUNTED unit set match the generation-qualified unit set the CURRENT ring +
// CURRENT generation assign it:
//
//   - desired = desiredGenUnits() against the live ring at the live
//     generation (who SHOULD this node own right now).
//   - mounted = the mount-map keys (what this node ACTUALLY has open).
//   - desired-but-not-mounted -> ACQUIRE: OpenUnit at a strictly higher
//     epoch (fences the prior owner; see acquireUnit).
//   - mounted-but-not-desired -> RELEASE: CloseUnit (flush + release the
//     lease; see releaseUnit).
//
// It is idempotent (a node already holding exactly its desired set does no
// work) and self-healing (a node that should own U but lost its mount
// re-acquires it), so it is safe to run on every membership event.
//
// RESHARD INTERACTION (Phase 4): a unit being actively bisected by a local
// Reshard is a mountMap entry the reshard owns for the duration of the
// bisect. To avoid the reconcile fighting the resharder (releasing a
// mid-bisect old unit, or releasing a freshly-created child before the
// generation advances), the reconcile takes reshardMu for the snapshot +
// diff so it cannot run concurrently with a Reshard. desiredGenUnits is
// computed against the live generation, so once a reshard has advanced the
// generation the reconcile naturally releases the retired old-gen units and
// acquires the new-gen units the ring assigns this node.
//
// ORDERING - release first, then acquire - is deliberate. On the node
// LOSING a unit, the release half runs here; on the node GAINING it, the
// acquire half runs (on that other node). The gaining node's OpenUnit
// fences against the unit's DURABLE epoch, not the losing node's in-process
// state, so the two halves do not need a global order for safety: even if
// the gaining node acquires before the losing node has released, the fence
// makes the losing node's further writes fail, and the losing node's
// CloseUnit flushed every acked write durably before that point (see the
// file-header invariant). Within THIS node, releasing first frees resources
// before we acquire, keeping the mounted set from transiently exceeding the
// desired set.
//
// Caller MUST hold reconcileMu (runReconcile does) AND must exclude an
// in-flight Reshard - either by holding reshardMu (the resharder's own
// post-bisect call) or because runReconcile took reshardMu before reconcileMu.
// This mutual exclusion matters because a bisect transiently mounts the
// gen-(g+1) children and holds the gen-g old unit until cut-over; a reconcile
// running concurrently could release those mid-bisect. mountMap mutations
// inside acquireUnit / releaseUnit take mountMu.
func (c *Cluster) reconcileUnits() {
	desired := c.desiredGenUnits()
	desiredSet := make(map[storageunit.GenUnit]struct{}, len(desired))
	for _, gu := range desired {
		desiredSet[gu] = struct{}{}
	}

	// Snapshot the currently-mounted set under the read lock so we diff
	// against a coherent view. acquireUnit / releaseUnit re-take mountMu
	// (write) to mutate, so a concurrent reader of the mount map never sees
	// a half-applied reconcile.
	c.mountMu.RLock()
	mounted := make(map[storageunit.GenUnit]struct{}, len(c.mountMap))
	for gu := range c.mountMap {
		mounted[gu] = struct{}{}
	}
	c.mountMu.RUnlock()

	// RELEASE first: units this node no longer owns. The old owner of a
	// handed-off unit runs this half. CloseUnit flushes (durable) then
	// releases the lease so the new owner, opening at a higher epoch, sees
	// every acked write.
	for gu := range mounted {
		if _, want := desiredSet[gu]; !want {
			c.releaseUnit(gu)
		}
	}

	// ACQUIRE: units this node now owns but has not mounted. The new owner
	// of a handed-off unit runs this half, opening at a strictly higher
	// epoch to FENCE the prior owner.
	for _, gu := range desired {
		if _, have := mounted[gu]; !have {
			c.acquireUnit(gu)
		}
	}
}

// acquireUnit mounts the generation-qualified unit gu, opening it at a
// strictly higher epoch than the unit's current durable lease epoch. THIS IS
// THE FENCE POINT: opening at a higher epoch makes the prior owner's further
// writes to gu FAIL (slatedb's single-writer writer-epoch fencing; the test
// factory mirrors it via a shared epoch registry). The new owner therefore
// acquires the lease and the old owner is locked out, even if the old owner
// has not yet observed the membership change.
//
// The intended epoch passed to OpenUnit is the cluster's best-effort next
// epoch (durable epoch + 1). The factory is AUTHORITATIVE: it reads the
// unit's durable manifest writer-epoch and fences strictly above it, so a
// stale intended epoch cannot under-fence. The cross-node source of truth
// is the durable lease state, NOT this in-process value.
//
// On OpenUnit error the unit is left unmounted: an op routed here for gu then
// hits the handoff-window guard (errUnitAcquiring, a retryable
// codes.Unavailable) and the next membership tick / reconcile retries the
// acquire. We never serve gu from a wrong engine and never lose a write by
// failing to mount.
//
// Caller MUST hold reconcileMu. mountMap mutation takes mountMu.
func (c *Cluster) acquireUnit(gu storageunit.GenUnit) {
	epoch := c.nextEpochFor(gu)
	b, err := c.factory.OpenUnit(gu, epoch)
	if err != nil {
		// Leaving gu unmounted is safe: routed ops get the retryable
		// acquiring-window error and the next reconcile retries. Do not
		// mount a half-open unit.
		return
	}
	c.mountMu.Lock()
	if c.closed.Load() {
		// Close raced us between OpenUnit and the mount. Close already ran
		// closeMountedUnits over the mountMap, so inserting now would leak
		// this freshly-opened backend past shutdown (and risk a write after
		// Close). Release it instead of mounting.
		c.mountMu.Unlock()
		_ = c.factory.CloseUnit(gu)
		return
	}
	c.mountMap[gu] = b
	c.mountMu.Unlock()
}

// releaseUnit unmounts the generation-qualified unit gu via CloseUnit, which
// FLUSHES anything durable then releases the lease. Flush-before-release is
// what upholds the NO-ACKED-WRITE-LOST invariant: every acked write is forced
// down to the unit's shared backing store before the lease moves, so the new
// owner sees it. After CloseUnit the unit may be re-opened (here or on another
// node) at a higher epoch.
//
// The mount-map entry is removed BEFORE CloseUnit so that, the instant the
// lease is being released, a routed op no longer resolves the local backend
// for gu (it falls through to the handoff-window guard and retries). Removing
// after the close would leave a brief window where the local map still
// points at a backend whose lease is being torn down.
//
// Caller MUST hold reconcileMu. mountMap mutation takes mountMu.
func (c *Cluster) releaseUnit(gu storageunit.GenUnit) {
	c.mountMu.Lock()
	delete(c.mountMap, gu)
	c.mountMu.Unlock()
	// CloseUnit is idempotent + best-effort: a close error does not change
	// ownership (the ring already moved gu off this node), and the new
	// owner's higher-epoch open fences any writer this close failed to stop.
	_ = c.factory.CloseUnit(gu)
}

// nextEpochFor computes the INTENDED epoch to open gu at: one above the
// epoch this node currently holds gu at, or the base acquire epoch if this
// node does not currently hold gu (the common handoff case - the new owner
// never had gu open). This is a best-effort hint only; the factory fences
// authoritatively against the unit's DURABLE writer-epoch (see acquireUnit).
// CurrentEpoch is the LOCAL in-process view and is NOT the cross-node source
// of truth, which is exactly why the factory - not this function - performs
// the real fence.
func (c *Cluster) nextEpochFor(gu storageunit.GenUnit) storageunit.Epoch {
	if cur, ok := c.factory.CurrentEpoch(gu); ok {
		return cur + 1
	}
	return acquireBaseEpoch
}

// acquireBaseEpoch is the intended epoch a node passes to OpenUnit when it
// has no local epoch for the unit (it never held it). It is intentionally 1,
// strictly above the Phase 2 static epochAtOpen (0), so a Phase 3 acquire of
// a unit that was statically opened at epoch 0 still names a higher intended
// epoch. The factory re-derives the true fence from the durable manifest
// regardless, so the exact value here only matters as a floor.
const acquireBaseEpoch storageunit.Epoch = 1

// TestingClearMount removes unit u (at the cluster's CURRENT generation) from
// the mount map WITHOUT releasing the factory lease, putting this node into
// the owner-but-unmounted state (the instantaneous handoff window)
// DETERMINISTICALLY for a test. A routed op for u's keys then hits the
// retryable acquiring-window guard (errUnitAcquiring). Test-only; follows the
// Testing* white-box-hook convention (see TestingDropAllPeerClients). No-op in
// legacy mode.
func (c *Cluster) TestingClearMount(u storageunit.UnitID) {
	if !c.multi {
		return
	}
	gu := storageunit.NewGenUnit(c.genSnapshot().gen, u)
	c.mountMu.Lock()
	delete(c.mountMap, gu)
	c.mountMu.Unlock()
}

// errUnitAcquiring is the HANDOFF-WINDOW refusal: this node IS the ring
// owner of the key's unit but has NOT yet mounted it (the acquire is in
// flight, or the prior reconcile failed to open it and the next will
// retry). Unlike errUnitNotMounted (the node is NOT the owner - a stale
// routing view that the client must refresh), this says "you reached the
// right node, the unit is momentarily unavailable, retry."
//
// It carries codes.Unavailable, which the client / forwarding retry shape
// treats as a transient backoff-and-retry (the same family as the v0.3
// cutover / migration-guard retries). The originator retries and succeeds
// once the new owner has acquired. We NEVER serve a wrong or stale result
// and NEVER lose a write during this window.
func errUnitAcquiring(op string) error {
	return status.Errorf(codes.Unavailable,
		"cluster: %s: unit for key is handing off to this node; retry shortly", op)
}
