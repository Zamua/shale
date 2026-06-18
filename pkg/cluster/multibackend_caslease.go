// CAS-guarded serving-marker LEASE (v0.8 Phase 2g) acquire helpers for the R>1
// multi-backend replicated cluster. They turn "open the slatedb writer to claim
// a position" into "win a compare-and-swap on the durable lease object, THEN
// open" - so a full-cluster (all-pods-at-once) restart re-forms ownership
// instead of wedging, and two nodes can never fence-fight a position.
//
// The protocol (see docs/SPEC.md "v0.8 Phase 2g"):
//
//   1. READ the lease object + its ETag (ReadServingLease).
//   2. If a lease is PRESENT and LIVE (owner in the live gossip member set AND
//      heartbeat fresh), DEFER - a live owner holds it; do not open, do not fence.
//   3. If absent, or present-but-STALE, CAS the lease ({me, intended epoch, now}):
//      If-None-Match: * to claim an absent lease, If-Match: <etag> to steal a
//      present-stale one. Exactly ONE node wins per contended round.
//   4. ONLY the CAS winner opens the writer (the fence is then uncontested).
//   5. A CAS LOSER backs off WITHOUT opening (the winner's fresh live lease makes
//      it defer next tick).
//
// Convergence gate: a node only treats owner-not-in-live-set as grounds to
// CAS-steal once gossip has SETTLED (the member set unchanged for a settle
// window); until then it conservatively defers to a present lease. The lease
// heartbeat is the backstop. The R=1 path and the legacy per-node path are
// untouched (this lives behind multiReplicated(), like the rest of Phase 2e/f).

package cluster

import (
	"sync"
	"time"

	"github.com/Zamua/shale/pkg/storageunit"
)

// leaseWindow is how recently a serving owner must have refreshed its lease
// heartbeat for the lease to count as LIVE (predicate B's heartbeat backstop). A
// small multiple of reconcileInterval so a single missed refresh does not expire
// a live owner. A var (not const) so tests can shrink it alongside
// reconcileInterval. Zero is treated as "no heartbeat-age check" by the
// predicate (owner-in-live-set is then the only liveness signal).
var leaseWindow = 4 * reconcileInterval

// convergenceSettleWindow is how long the live gossip member set must be UNCHANGED
// before this node will CAS-steal a stale-looking lease whose owner is not in the
// set (the "decide off a settled ring" gate). Until the set has been stable this
// long, a node defers to a present lease rather than risk stealing from a live
// peer whose gossip has not yet arrived. A var so tests can shrink it.
var convergenceSettleWindow = 3 * reconcileInterval

// nowUnixMs is the wall-clock source for lease heartbeats (Unix millis), a var so
// a test can pin time deterministically. The DEFAULT is monotonicUnixMs, which
// guarantees a STRICTLY-ADVANCING value per process (see below); a test that
// overrides this var takes full control of the returned values.
var nowUnixMs = monotonicUnixMs

// lastHeartbeatMs and lastHeartbeatMu back monotonicUnixMs's strictly-advancing
// guarantee for the lease heartbeat field.
var (
	lastHeartbeatMu sync.Mutex
	lastHeartbeatMs int64
)

// monotonicUnixMs returns the current wall clock in Unix millis, but NEVER returns
// the same value (or a lower one) twice in a process: if the wall clock has not
// advanced since the last call it returns lastHeartbeatMs + 1.
//
// CLEANUP #2 (content-hash-ETag ABA guard): real object stores derive an ETag
// from the object's CONTENT, so two BYTE-IDENTICAL lease payloads yield the SAME
// ETag - an ABA hazard a CAS cannot detect. Every lease write stamps the
// HeartbeatUnixMs field, and most writes (refreshOwnedLeases especially) keep the
// SAME owner + epoch, so the heartbeat is the only field that distinguishes two
// consecutive writes by this node. If two such writes landed in the same
// millisecond the payloads would be identical. Forcing the heartbeat to strictly
// advance makes every lease this node writes byte-distinct from its predecessor,
// so no two consecutive CAS writes can collide on a content-hash ETag. The cost is
// at most a sub-millisecond skew of the recorded heartbeat under a burst, which is
// far below leaseWindow and harmless to the staleness predicate.
func monotonicUnixMs() int64 {
	lastHeartbeatMu.Lock()
	defer lastHeartbeatMu.Unlock()
	now := time.Now().UnixMilli()
	if now <= lastHeartbeatMs {
		now = lastHeartbeatMs + 1
	}
	lastHeartbeatMs = now
	return now
}

// convergenceClock is the time source the convergence-settle tracker measures the
// settle window against. A separate var from nowUnixMs because the tracker needs a
// time.Time it can subtract (Sub) rather than a Unix-millis int; a test pins it to
// advance the settle window deterministically without sleeping. Defaults to the
// real wall clock.
var convergenceClock = func() time.Time { return time.Now() }

// liveMemberSet returns the set of CURRENTLY-ALIVE member ids from the gossip
// snapshot (predicate B's "owner in the live member set" input). A nil membership
// (white-box tests without a memberlist) yields just this node - so in such a
// test only this node's own leases read as live, which is the safe default
// (every other lease is stale and CAS-claimable). The set always includes this
// node's own id so a node never treats its OWN fresh lease as a ghost.
func (c *Cluster) liveMemberSet() map[string]bool {
	live := make(map[string]bool, 4)
	live[c.cfg.NodeID] = true
	if c.membership == nil {
		return live
	}
	for _, m := range c.membership.Snapshot() {
		live[m.ID] = true
	}
	return live
}

// noteMembershipForConvergence records the current live member set and updates the
// convergence state (the settle-window tracker for the CAS-steal gate). Called
// once per reconcile pass BEFORE the acquire diff. It compares the live set to the
// previously-observed one: if it CHANGED, the settle clock restarts (not yet
// converged); if it is UNCHANGED and has been stable for convergenceSettleWindow,
// convergence is READY (a node may CAS-steal a stale-owner lease). Cheap; runs
// under convergenceMu.
func (c *Cluster) noteMembershipForConvergence(live map[string]bool) {
	c.convergenceMu.Lock()
	defer c.convergenceMu.Unlock()
	now := convergenceClock()
	if !sameStringSet(c.lastMemberSet, live) {
		c.lastMemberSet = live
		c.convergedSince = now
		c.convergenceReady = false
		return
	}
	if !c.convergenceReady && now.Sub(c.convergedSince) >= convergenceSettleWindow {
		c.convergenceReady = true
	}
}

// convergedEnoughToSteal reports whether gossip has SETTLED long enough that this
// node may CAS-steal a stale-looking lease whose owner is simply not in the live
// set (predicate B's owner-not-in-set branch). Until then the acquire defers to a
// present lease. The heartbeat-age branch of staleness is NOT gated on this (a
// heartbeat that has aged out of the lease window is reclaimable regardless of
// convergence - that is the backstop for a lingering-but-dead owner id).
func (c *Cluster) convergedEnoughToSteal() bool {
	c.convergenceMu.Lock()
	defer c.convergenceMu.Unlock()
	return c.convergenceReady
}

// sameStringSet reports whether two live-member sets are identical.
func sameStringSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if !b[id] {
			return false
		}
	}
	return true
}

// leaseWindowMs returns the lease window expressed in the milliseconds that the
// pure ServingMarker predicates speak (HeartbeatUnixMs is Unix millis). A
// non-positive result (leaseWindow == 0) disables the heartbeat-age backstop in
// those predicates (owner-liveness alone then decides), matching the documented
// "zero = no heartbeat-age check" contract on leaseWindow.
func leaseWindowMs() int64 { return int64(leaseWindow / time.Millisecond) }

// TestingSetCASLeaseTiming shrinks the CAS-lease timing windows and overrides the
// two clocks for a deterministic restart test, returning a restore func the caller
// MUST defer. It sets the heartbeat lease window, the convergence-settle window,
// the heartbeat clock (nowUnixMs, Unix-millis), and the convergence-settle clock
// (convergenceClock, a time.Time source). All four are package-level vars the
// production path reads; an integration test in another package cannot reach them
// directly, so this exported white-box hook is the seam. Pass nil for either clock
// to leave that clock at its production default. Test-only; the production defaults
// are unchanged for every non-test path. Follows the Testing* convention.
//
// The two clocks are SEPARATE on purpose: nowUnixMs feeds the lease heartbeat
// (Unix-millis int that the pure ServingMarker predicates compare), and
// convergenceClock feeds the settle-window tracker (a time.Time it subtracts). A
// fast-restart test pins nowUnixMs so heartbeats stay FRESH (younger than
// leaseWindow) across the restart, forcing recovery through the convergence seed
// rather than the heartbeat-age backstop.
func TestingSetCASLeaseTiming(
	lease, settle time.Duration,
	nowMs func() int64,
	convClock func() time.Time,
) (restore func()) {
	prevLease, prevSettle := leaseWindow, convergenceSettleWindow
	prevNow, prevConv := nowUnixMs, convergenceClock
	leaseWindow = lease
	convergenceSettleWindow = settle
	if nowMs != nil {
		nowUnixMs = nowMs
	}
	if convClock != nil {
		convergenceClock = convClock
	}
	return func() {
		leaseWindow = prevLease
		convergenceSettleWindow = prevSettle
		nowUnixMs = prevNow
		convergenceClock = prevConv
	}
}

// TestingConvergedEnoughToSteal exposes the CAS-steal convergence gate for the
// fast-restart regression test. It reports whether the settle window has elapsed
// since the membership last changed, which is exactly the signal the boot
// convergence-seed exists to start EARLY (from boot, not from the first reconcile
// pass). A test asserts it is TRUE shortly after boot WITHOUT having run a
// reconcile - true only because mountReplicaUnits seeded the tracker; reverting
// that seed makes this stay false until a reconcile runs. Test-only white-box hook
// following the Testing* convention.
func (c *Cluster) TestingConvergedEnoughToSteal() bool { return c.convergedEnoughToSteal() }

// leaseIsLiveDeferred reports whether a present lease should be DEFERRED to (a
// live owner holds it). It defers entirely to the pure staleness predicate
// ServingMarker.IsStale for the owner/heartbeat half (the SINGLE source of truth)
// and layers ONLY the convergence gate on top:
//
//   - lease NOT stale (known owner in the live set, heartbeat fresh): LIVE, DEFER.
//   - lease stale BECAUSE the heartbeat aged out: a DEAD owner; do NOT defer
//     (the backstop - reclaimable regardless of ring convergence).
//   - lease stale for any OTHER reason (owner "" or owner-not-in-live-set) but
//     the heartbeat is still fresh: the owner might be a live peer whose gossip
//     has not arrived yet, so DEFER until gossip has SETTLED. Only a converged
//     ring lets this node treat owner-not-in-set as grounds to steal.
//
// now is the caller's wall clock (millis). The heartbeat-age comparison and the
// owner-liveness comparison both live in ServingMarker now; this function adds no
// staleness logic of its own, only the convergence policy.
func (c *Cluster) leaseIsLiveDeferred(lease storageunit.ServingMarker, live map[string]bool, now int64) bool {
	winMs := leaseWindowMs()
	// Owner + heartbeat half: a non-stale lease is a live owner; always defer.
	if !lease.IsStale(now, winMs, live) {
		return true
	}
	// Stale. The heartbeat-age backstop overrides the convergence gate: a KNOWN
	// owner that stopped refreshing is dead regardless of whether its id lingers
	// in gossip or whether the ring has converged, so do NOT defer. (An empty
	// owner has no heartbeat to trust as a death signal, so it falls through to
	// the convergence gate, matching the legacy bare-epoch handling.)
	if lease.Owner != "" && lease.HeartbeatAgedOut(now, winMs) {
		return false
	}
	// Stale only because the owner is unknown / not in the live set (and, for a
	// known owner, its heartbeat is still fresh). Before gossip has settled, DEFER
	// conservatively (the owner may be a live peer we have not learned yet).
	return !c.convergedEnoughToSteal()
}

// claimReplicaLease runs the Phase 2g single-winner CAS claim for position ru at
// the intended floor epoch. It returns:
//
//   - won == true: this node WON the lease CAS (or already holds a live lease it
//     refreshed). The caller - and ONLY the caller, as the sole winner - may now
//     open the writer; the fence is uncontested. wonETag is the lease version the
//     winner holds (pass it to recordOpenEpochInLease after the open to record the
//     ACTUAL open epoch).
//   - won == false, deferLive == true: a LIVE owner holds the lease; defer (do
//     not open, do not fence).
//   - won == false, deferLive == false: this node LOST the CAS race to another
//     claimer (or hit transient I/O); back off and retry next reconcile (by then
//     the winner's fresh lease makes this a deferLive).
//
// It never opens the writer itself - that stays the caller's responsibility,
// gated on won == true - so the "open only if you won the CAS" invariant is
// explicit at the call site.
func (c *Cluster) claimReplicaLease(ru storageunit.ReplicaUnit, intended storageunit.Epoch) (won bool, wonETag string, deferLive bool) {
	live := c.liveMemberSet()
	now := nowUnixMs()

	// BREAK-DEMO (TestingForceLeaseClaimWin): reproduce the PRE-2g behavior where
	// claiming a position was "just open the writer" - no live-owner defer, no CAS
	// single-winner. We still WRITE a bare-epoch-style lease so the marker exists,
	// but we report won == true REGARDLESS of whether the CAS succeeded, so every
	// booting node proceeds to open the SAME stale-marked position in parallel and
	// the opens fence-fight. The wonETag is best-effort (the post-open rewrite CAS
	// is allowed to lose); the point is that `won` is unconditional.
	if c.cfg.TestingForceLeaseClaimWin {
		_, etag, present, _ := c.replicaFactory.ReadServingLease(ru)
		expected := ""
		if present {
			expected = etag
		}
		next := storageunit.ServingMarker{Epoch: intended, Owner: c.cfg.NodeID, HeartbeatUnixMs: now}
		newETag, _, _ := c.replicaFactory.CasServingLease(ru, expected, next)
		return true, newETag, false
	}

	lease, etag, present, err := c.replicaFactory.ReadServingLease(ru)
	if err != nil {
		// Transient read failure: do not open (could fence a live peer). Treat as a
		// lost round; retry next tick.
		return false, "", false
	}
	// A lease THIS node already owns is never deferred to: it is re-claimable by
	// self (the self-heal path - a prior claim won the CAS but the open then failed,
	// leaving a self-owned lease that must not block this node's own retry). Only a
	// lease owned by a DIFFERENT live peer is a defer.
	if present && lease.Owner != c.cfg.NodeID && c.leaseIsLiveDeferred(lease, live, now) {
		return false, "", true
	}

	// Absent or stale: attempt the CAS to claim it. expectedETag "" = create-only
	// (absent), the observed etag = swap-only (steal a present-stale lease).
	expected := ""
	if present {
		expected = etag
	}
	next := storageunit.ServingMarker{Epoch: intended, Owner: c.cfg.NodeID, HeartbeatUnixMs: now}
	newETag, ok, cerr := c.replicaFactory.CasServingLease(ru, expected, next)
	if cerr != nil || !ok {
		// Lost the race (or transient I/O): back off, do NOT open.
		return false, "", false
	}
	return true, newETag, false
}

// claimReplicaLeaseForHandoff is the overlap-gainer variant of claimReplicaLease:
// it CASes the lease to THIS node WITHOUT the live-owner defer. The overlap
// handoff is the LEGITIMATE ownership-change path: the ring has assigned this node
// as the pending owner of a position a DRAINING predecessor still serves, so the
// predecessor's lease is LIVE (it is in the member set, heartbeat fresh) - a
// live-defer would wedge the handoff. There is exactly ONE pending owner per
// position (the ring), so there is no multi-node race to arbitrate here; the CAS
// still serializes against a concurrent claimer and records ownership so the
// predecessor's drainCheck (strict-`>` epoch gate) releases once this node opens
// at a higher epoch. It returns won + the won etag (for the post-open epoch
// rewrite); a lost CAS (a concurrent claimer) backs the gainer off to retry.
func (c *Cluster) claimReplicaLeaseForHandoff(ru storageunit.ReplicaUnit, intended storageunit.Epoch) (won bool, wonETag string) {
	_, etag, present, err := c.replicaFactory.ReadServingLease(ru)
	if err != nil {
		return false, ""
	}
	expected := ""
	if present {
		expected = etag
	}
	next := storageunit.ServingMarker{Epoch: intended, Owner: c.cfg.NodeID, HeartbeatUnixMs: nowUnixMs()}
	newETag, ok, cerr := c.replicaFactory.CasServingLease(ru, expected, next)
	if cerr != nil || !ok {
		return false, ""
	}
	return true, newETag
}

// recordOpenEpochInLease rewrites this node's just-won lease to carry the ACTUAL
// open epoch (the claim wrote the INTENDED floor; the post-open rewrite records
// the exact factory-returned epoch) and a fresh heartbeat. It CASes If-Match the
// just-won etag so it never clobbers a newer writer. Best-effort: a lost CAS here
// means some other change moved the lease (rare; the next heartbeat refresh
// reconverges it), so the error is swallowed - the writer is already open and the
// epoch field is only consulted by the strict-`>` release gate, which a
// monotonic-floor intended epoch still satisfies until the rewrite lands.
func (c *Cluster) recordOpenEpochInLease(ru storageunit.ReplicaUnit, wonETag string, openEpoch storageunit.Epoch) {
	next := storageunit.ServingMarker{Epoch: openEpoch, Owner: c.cfg.NodeID, HeartbeatUnixMs: nowUnixMs()}
	_, _, _ = c.replicaFactory.CasServingLease(ru, wonETag, next)
}

// recordServingLease writes THIS node's lease for an already-mounted position at
// the given open epoch WITHOUT a pre-won etag (the recovery path: a stuck flip
// whose mount is already present but whose lease was never written). It reads the
// current lease etag and CASes If-Match (or create-only if absent), so it does not
// clobber a newer writer. Best-effort: a lost CAS means the lease already moved
// (someone advanced it), which is fine - the mount is the authoritative durable
// owner and the next heartbeat refresh reconverges the lease.
func (c *Cluster) recordServingLease(ru storageunit.ReplicaUnit, openEpoch storageunit.Epoch) {
	_, etag, present, err := c.replicaFactory.ReadServingLease(ru)
	if err != nil {
		return
	}
	expected := ""
	if present {
		expected = etag
	}
	next := storageunit.ServingMarker{Epoch: openEpoch, Owner: c.cfg.NodeID, HeartbeatUnixMs: nowUnixMs()}
	_, _, _ = c.replicaFactory.CasServingLease(ru, expected, next)
}

// refreshOwnedLeases re-stamps the heartbeat (and re-asserts ownership) on every
// position THIS node currently serves, on the reconcile cadence (part A's
// periodic refresh + the heartbeat backstop for predicate B). For each mounted
// position it CASes the lease If-Match its current etag to {me, my open epoch,
// now}, but ONLY when the lease already records THIS node as owner at THIS node's
// open epoch - so a refresh never overwrites a successor that has legitimately
// taken the position over (the successor opened at a higher epoch and its lease
// carries that higher epoch; this node's stale refresh would lose the strict-`>`
// comparison anyway, but gating on owner+epoch avoids even attempting it). A lost
// CAS is benign (someone else advanced the lease; this node is being handed off).
// Caller holds reconcileMu.
func (c *Cluster) refreshOwnedLeases() {
	c.mountMu.RLock()
	mounted := make([]storageunit.ReplicaUnit, 0, len(c.mountMap))
	for ru := range c.mountMap {
		// Do not refresh a position mid-transition: a Draining loser must let its
		// own marker stand (the successor's higher-epoch marker is what releases it),
		// and a still-Acquiring gainer has not written its marker yet.
		if st := c.handoffPhase[ru]; st.Phase != 0 {
			continue
		}
		mounted = append(mounted, ru)
	}
	c.mountMu.RUnlock()

	for _, ru := range mounted {
		myEpoch := c.ownOpenEpoch(ru)
		lease, etag, present, err := c.replicaFactory.ReadServingLease(ru)
		if err != nil || !present {
			continue
		}
		// Only refresh a lease that still records THIS node serving at THIS node's
		// open epoch. If the recorded owner/epoch differ, the position has moved (or
		// a successor is taking over): leave it alone.
		if lease.Owner != c.cfg.NodeID || lease.Epoch != myEpoch {
			continue
		}
		next := storageunit.ServingMarker{Epoch: myEpoch, Owner: c.cfg.NodeID, HeartbeatUnixMs: nowUnixMs()}
		_, _, _ = c.replicaFactory.CasServingLease(ru, etag, next)
	}
}
