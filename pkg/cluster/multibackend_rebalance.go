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
	"errors"
	"time"

	"github.com/Zamua/shale/internal/decide"
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
	c.scheduleReconcileIn(c.settleDelay())
}

// scheduleReconcileIn is scheduleReconcile with an explicit delay. The one
// caller that passes anything but the settle debounce is the boot-defer path
// (mountReplicaUnits): a node that booted missing owned positions has KNOWN,
// non-churn work whose delay directly extends a write-quorum gap, so it arms
// the pass at zero. Everything else debounces as before.
//
// The arming RULES - what a debounced arm may do to a pending immediate one,
// and which arm owns the pending obligation - live in decide.SettleArm. This is
// the shell: snapshot the state under settleMu, ask, perform.
//
// Stop is the only way to learn whether the pending timer is still live, and it
// consumes a live one, so the probe runs BEFORE the decision and every arm
// installs a replacement timer.
func (c *Cluster) scheduleReconcileIn(d time.Duration) {
	if c.closed.Load() {
		return
	}
	c.settleMu.Lock()
	defer c.settleMu.Unlock()
	cur := decide.SettleState{Armed: c.settleTimer != nil, Immediate: c.settleImmediate}
	if cur.Armed {
		cur.Fired = !c.settleTimer.Stop()
	}
	dec := decide.SettleArm(cur, d)
	if dec.NewObligation {
		// The reconcile is pending until this arm's callback releases it, so
		// WaitForRebalanceIdle blocks through it.
		c.settlePending.Add(1)
	}
	c.settleImmediate = dec.Immediate
	c.settleTimer = time.AfterFunc(dec.Delay, c.runScheduledReconcile)
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
	c.settleImmediate = false
	c.settleMu.Unlock()
	// Stall visibility: a pass that blocks mid-run holds the pending
	// obligation, and every idle-waiter with it. Log once past a generous
	// bound so a wedged pass names itself instead of presenting as a bare
	// deadline-exceeded in whoever was waiting.
	c.reconcileRunning.Store(true)
	stall := time.AfterFunc(5*time.Second, func() {
		c.logf("shale: reconcile pass running >5s - an idle-waiter blocked on it sees settlePending>0; see /debug/shale/state for acquire-in-flight")
	})
	defer func() {
		stall.Stop()
		c.reconcileRunning.Store(false)
	}()
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
// Reshard is a mount-table entry the reshard owns for the duration of the
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
// running concurrently could release those mid-bisect. The mount mutations
// inside acquireUnit / releaseUnit go through the mount table.
func (c *Cluster) reconcileUnits() {
	// Decentralized online reshard (v0.9, extended to R=1 multi-node): when an
	// Arbiter is wired (opt-in via ConditionalStore), drive this node's share of
	// any in-flight split FIRST - enter/finalize the generation, copy owned
	// parents into their children, publish/observe the durable markers, flip
	// routing, retire finalized parents. No-op without an Arbiter / split. Runs
	// on BOTH the R>1 and R=1 branches (the driver picks the per-R copy
	// mechanics), before the mount reconcile so a just-entered split's children
	// are in the desired set this same pass, and a just-finalized generation's
	// redistribution runs against the advanced generation.
	//
	// First, reconcile the agreed target toward the cluster-wide DECLARED
	// unit count (the operator's SHALE_UNIT_COUNT, advertised through the
	// coordinator): when the cluster is steady AND every live member agrees on
	// the same declared count, CAS the arbiter target to it. observeReshard
	// below then picks the new target up THIS SAME tick and enters the
	// split/merge. No-op without an arbiter, or until the live set is
	// unanimous (the rolling-deploy flap guard). See
	// multibackend_reshard_declared.go.
	c.observeDeclaredReshardTarget()
	c.observeReshard()
	if c.replicaLayout() {
		// R>1 (replicated multi-backend, v0.8 Phase 2b): the desired set is the
		// replica-set membership, not single ownership. This runs on the INITIAL
		// multi-node convergence (each node mounting the units it replicates as
		// the ring fills with peers); the topology is otherwise STATIC (no
		// lease handoff at R>1 yet - a later phase). It diffs desired-vs-mounted
		// the same way, opening each newly-desired unit at its replica POSITION
		// (an independent durable database) and releasing ones no longer desired.
		//
		// v0.8 Phase 2e (Option B): the position-MOVE branch is the overlap
		// handoff controller (acquire-then-release, sequenced by the per-
		// ReplicaUnit FSM), and every Draining position is polled for release on
		// this same cadence (drainCheck reads the durable serving marker). The
		// initial-convergence pure-new-mount case takes the same background
		// bounded acquire (no drain half); plain drop-out stays the clean-cut
		// release. See multibackend_overlap.go.
		if c.cfg.TestingForceCleanCut {
			// Break-demo only (docs/SPEC.md "v0.8 Phase 2e" gate): the pre-2e
			// clean-cut RELEASE-then-ACQUIRE, no overlap, no forwarding. A
			// position moving away is released eagerly and the new owner refuses
			// routed ops (errUnitAcquiring) for the whole mount. Used to prove the
			// overlap forward is what holds availability.
			c.reconcileReplicaUnitsCleanCut()
			return
		}
		c.reconcileReplicaUnitsOverlap()
		c.runDrainChecks()
		// JOIN write-transparency (entry side): clear this node's Joining bit once
		// it has mounted every position it owns (a no-op unless it is warming).
		c.maintainJoiningState()
		return
	}
	desired := c.desiredGenUnits()
	desiredSet := make(map[storageunit.GenUnit]struct{}, len(desired))
	for _, gu := range desired {
		desiredSet[gu] = struct{}{}
	}

	// Snapshot the currently-mounted set so we diff against a coherent view.
	// acquireUnit / releaseUnit re-enter the table to mutate, so a concurrent
	// reader of the mount map never sees a half-applied reconcile.
	snapshot := c.mounts.mountedList()
	mounted := make(map[storageunit.GenUnit]struct{}, len(snapshot))
	for _, ru := range snapshot {
		// R=1 lease-handoff path: a node holds each unit at position 0, so the
		// GenUnit view is ru.Unit.
		mounted[ru.Unit] = struct{}{}
	}

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

	// ORPHAN SWEEP: close factory holds that are neither mounted nor desired.
	// A FENCED stale mount is EVICTED from the mount table only (evictStaleMount
	// keeps the factory handle open, since a close by MountRef could tear down a
	// concurrently re-acquired healthy handle); when this node then never
	// re-acquires the unit - its ownership moved away, e.g. a co-located reshard
	// child redistributing to its gen-(g+1) ring home fenced this node's copy -
	// nothing else ever closes that handle, leaking the open (and its lease
	// record) for the life of the process. Under reconcileMu nothing else opens
	// units, so a ref that is absent from BOTH the (post-diff) mount table and
	// the desired set is provably orphaned: release it. This is the
	// held-but-not-owned close the storage port's OpenUnits contract names.
	desiredNow := make(map[storageunit.GenUnit]struct{}, len(desired))
	for _, gu := range desired {
		desiredNow[gu] = struct{}{}
	}
	tableNow := make(map[storageunit.GenUnit]struct{})
	for _, ru := range c.mounts.mountedList() {
		tableNow[ru.Unit] = struct{}{}
	}
	for _, m := range c.factory.OpenUnits() {
		if m.Replicated() {
			continue // defensive: this is the R=1 sole-mount branch
		}
		gu := m.Unit()
		if _, ok := tableNow[gu]; ok {
			continue
		}
		if _, ok := desiredNow[gu]; ok {
			continue // an evicted-but-desired unit re-acquires instead
		}
		_ = c.factory.CloseUnit(m)
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
// Caller MUST hold reconcileMu. The mount mutation goes through the mount table.
func (c *Cluster) acquireUnit(gu storageunit.GenUnit) {
	epoch := c.nextEpochFor(gu)
	b, _, err := c.factory.OpenUnit(storageunit.SoleMount(gu), epoch)
	if err != nil {
		// Leaving gu unmounted is safe: routed ops get the retryable
		// acquiring-window error and the next reconcile retries. Do not
		// mount a half-open unit. Record the failure so the readiness
		// counts (FailedOpenUnits/LastAcquireError) see it, keyed by the
		// R=1 mount position exactly as acquireReplicaUnit records at R>1.
		c.mounts.recordAcquireErr(replica0(gu), err.Error())
		return
	}
	c.mounts.clearAcquireErr(replica0(gu))
	if !c.mounts.mountUnlessClosed(replica0(gu), b) {
		// Close raced us between OpenUnit and the mount. Close already drained
		// the mount table, so inserting now would leak this freshly-opened
		// backend past shutdown (and risk a write after Close). Release it
		// instead of mounting.
		_ = c.factory.CloseUnit(storageunit.SoleMount(gu))
		return
	}
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
// Caller MUST hold reconcileMu. The mount mutation goes through the mount table.
func (c *Cluster) releaseUnit(gu storageunit.GenUnit) {
	c.mounts.unmount(replica0(gu))
	// CloseUnit is idempotent + best-effort: a close error does not change
	// ownership (the ring already moved gu off this node), and the new
	// owner's higher-epoch open fences any writer this close failed to stop.
	_ = c.factory.CloseUnit(storageunit.SoleMount(gu))
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
	if cur, ok := c.factory.CurrentEpoch(storageunit.SoleMount(gu)); ok {
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
	c.mounts.unmount(replica0(storageunit.NewGenUnit(c.genSnapshot().gen, u)))
}

// errAcquiringSentinel is the package-private sentinel that tags every
// errUnitAcquiring value (v0.8 Phase 2d). The fan-out's classifier
// (isTransientReplicaErr) recognizes it via errors.Is so a mid-acquire
// replica counts toward NEITHER the ack nor the failure budget, rather
// than being miscounted as a down-peer failure (the Phase 2b gap that
// dropped the membership-change ack rate to ~52-64%).
//
// The sentinel survives the in-process local-self fan-out dispatch path
// verbatim (it is a plain Go error, not re-serialized). It does NOT cross
// gRPC: only the status code travels the wire, so the cross-node forwarded
// replica leg re-codes the acquiring refusal to codes.ResourceExhausted
// (recodeForwardedReplicaErr) which isTransientReplicaErr already skips.
//
// It UNWRAPS to the exported ErrAcquiring (see reason.go), which is what makes
// the acquiring window matchable by a library consumer. The wrapping direction
// matters: this tag keeps its OWN identity, so errors.Is against it still means
// exactly what it has always meant - a LOCAL in-process mid-acquire replica -
// and no classifier's behavior moves. A wire-decoded acquiring refusal wraps
// ErrAcquiring WITHOUT wrapping this tag, so the fan-out and read-leg budgets
// classify remote errors exactly as they did before reasons existed.
var errAcquiringSentinel error = &localReason{
	msg:      "cluster: unit acquiring (handoff window)",
	sentinel: ErrAcquiring,
}

// acquiringError is the HANDOFF-WINDOW refusal value: it carries
// codes.Unavailable ON THE WIRE (via GRPCStatus, so the client / forwarding
// retry shape treats it as a transient backoff-and-retry, the same family
// as the v0.3 cutover / migration-guard retries) AND wraps
// errAcquiringSentinel so the in-process fan-out classifier can distinguish
// "this replica is mid-acquire, skip it and wait for the budget to refill"
// from "this replica is down, count it."
type acquiringError struct {
	st *status.Status
}

func (e *acquiringError) Error() string              { return e.st.Message() }
func (e *acquiringError) GRPCStatus() *status.Status { return e.st }

// Unwrap exposes the sentinel to errors.Is without flattening the gRPC
// status: status.FromError still reads codes.Unavailable off GRPCStatus,
// and errors.Is(err, errAcquiringSentinel) returns true.
func (e *acquiringError) Unwrap() error { return errAcquiringSentinel }

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
// and NEVER lose a write during this window. The returned value also wraps
// errAcquiringSentinel (Phase 2d) so the fan-out's isTransientReplicaErr
// treats a local-self acquiring replica as transient, not as a failure.
//
// The status additionally carries the ReasonAcquiring ErrorInfo detail
// (reason.go). That detail is what lets the identity SURVIVE gRPC: the peer
// client decodes it back into a value wrapping ErrAcquiring, so a consumer's
// errors.Is(err, cluster.ErrAcquiring) holds on the forwarded path exactly as
// it does here. The code stays codes.Unavailable; the detail is purely
// additive, so nothing that reads the code is affected.
func errUnitAcquiring(op string) error {
	return errUnitAcquiringBecause(op, "handing off to this node")
}

// errUnitAcquiringBecause is errUnitAcquiring with the CAUSE named. Same
// sentinel, same code, same wire detail - only the message differs.
//
// It exists because several structurally different conditions used to produce a
// byte-identical string, which made the message actively misleading rather than
// merely vague. A cross-shard scan can be refused because this node's mount map
// does not cover its owned positions (scanCoverageErr, keyed on
// MountReadiness().PendingUnits) or because a mounted handle turned out to be
// FENCED (fenceToTransient, keyed on backend.ErrFenced). Those have different
// causes, different fixes, and - critically - different observable state: the
// first raises PendingUnits, the second does not. An operator who reads the
// shared message, checks the documented PendingUnits relationship, finds zero,
// and concludes the invariant is violated has done everything right and still
// been sent the wrong way. That happened in production, and cost hours.
//
// The cause is prose in the MESSAGE only. Nothing programmatic keys on it: the
// sentinel, the gRPC code, and the ReasonAcquiring detail are identical across
// every cause, so errors.Is and every consumer match are unaffected.
func errUnitAcquiringBecause(op, cause string) error {
	st := status.Newf(codes.Unavailable,
		"cluster: %s: unit for key is %s; retry shortly", op, cause)
	return &acquiringError{st: withReason(st, ReasonAcquiring)}
}

// isAcquiringErr reports whether err is (or wraps) the Phase 2d acquiring-
// window sentinel - i.e. a replica that is mid-acquire (handoff window),
// NOT a genuinely-down peer. Used by layer 2's retry classifier to decide
// whether an under-W fan-out outcome is a retryable handoff blip.
func isAcquiringErr(err error) bool {
	return errors.Is(err, errAcquiringSentinel)
}

// recodeForwardedReplicaErr converts a local acquiring refusal into the
// transient codes.ResourceExhausted that crosses gRPC as a fan-out
// transient (the SAME code the v0.3 migration guard uses), for the
// FORWARDED replica leg ONLY (LocalReplicaPut / ApplyBatchLocal). The
// errAcquiringSentinel does not survive the wire (gRPC carries only the
// status code), so without this re-code a cross-node mid-acquire replica
// would arrive at the originator as a bare codes.Unavailable and be
// miscounted as a down peer. The CLIENT-facing top-level refusal stays
// codes.Unavailable; only this internal replica-to-replica leg re-codes.
// A non-acquiring error passes through unchanged.
//
// The re-coded status still carries the ReasonAcquiring ErrorInfo detail
// (reason.go). The CODE is what the fan-out's budget classifier reads, and that
// is unchanged (ResourceExhausted, transient); the detail is additive identity
// that lets the ORIGINATOR still tell "this leg was mid-acquire" apart from "this
// leg hit the v0.3 migration guard", which both travel as ResourceExhausted.
// Without it a remote mid-acquire replica is indistinguishable from any other
// transient at the originator, and a write shortfall caused by it could not
// honestly report ReasonAcquiring. The originator's peer client decodes the
// detail into the EXPORTED ErrAcquiring only, never the private tag, so
// isAcquiringErr / isTransientReplicaErr / isUnreachableLegErr all see exactly
// what they saw before.
func recodeForwardedReplicaErr(err error) error {
	if err == nil || !isAcquiringErr(err) {
		return err
	}
	st := status.New(codes.ResourceExhausted,
		"cluster: forwarded replica write: unit mid-acquire (handoff window); retry shortly")
	return withReason(st, ReasonAcquiring).Err()
}
