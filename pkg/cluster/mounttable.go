package cluster

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// mountTable owns the complete answer to "which storage-unit positions does
// this node currently have open, at what epoch, in what handoff phase" - the
// mount map, the per-position open epoch, the in-flight handoff phase, the
// in-flight acquire set, the per-position acquire diagnostic, the node-wide
// open-permit gate and its holder bookkeeping. Every one of those used to be a
// separate Cluster field guarded (by convention) by a shared cluster-level
// RWMutex that ~150 call sites took by hand; folding them behind one type with
// a PRIVATE mutex makes the lock discipline a property of the code instead of a
// convention.
//
// LOCK DISCIPLINE. mu is private and is never exported, so no caller can hold
// it across arbitrary cluster code. Every multi-step read-modify-write that the
// old call sites performed inside ONE shared-mutex hold is a single method here (see
// armDrain, completeAcquireFlip, releaseDrained, evictIfSame, finishStuckGainer,
// clearOrphanedDrain, dropGeneration, takeAll), so those critical sections keep
// their atomicity without exposing the lock.
//
// NO RE-ENTRANCY. No method calls back into cluster code while holding mu. The
// only outward reference taken under the lock is wrap (below), a pure
// constructor. Everything else called under mu is either a map/slice operation
// or a pure function of the storageunit domain package (NextOnDrain,
// NextOnReady, NextOnRelease, replica0, sortReplicaUnits) - none of which can
// reach back into the table. Slow work (factory opens/closes, serving-marker
// I/O, flushes) stays OUTSIDE: the methods hand the caller what it needs
// (usually the backend handle that was just removed) and the caller does the
// I/O after the method returns.
//
// The sync.Map-backed members (openEpochs, acquireErr, permits) keep their
// pre-existing lock-free discipline: they are read both inside and outside mu
// exactly where they were before, so folding them in adds no ordering.
//
// ZERO VALUE IS READ-SAFE, exactly like the nil maps it replaces: the mutex is
// zero-value ready and every read ranges/indexes a nil map, so a Cluster built
// without init (legacy mode, a bare white-box test fixture) reports "nothing
// mounted" instead of panicking. Mount sites still require init, and panic on
// the nil map if they run without it - the same failure the raw map assignment
// had. It is therefore held BY VALUE on the Cluster, like casFlushGroup.
type mountTable struct {
	mu sync.RWMutex
	// mounts holds the positions this node has open, keyed by ReplicaUnit so a
	// gen-g unit K and a gen-(g+1) unit K (a doubling's old + new) coexist, and
	// so a node can hold the OLD (draining) and NEW (acquiring) position of the
	// SAME unit at once during an overlap handoff. The R=1 paths key via
	// replica0(gu).
	mounts map[storageunit.ReplicaUnit]backend.Backend
	// phases holds, per IN-FLIGHT position, the pure ownership-transition state
	// of an overlap handoff. Absence means steady state: Owned if the position
	// is in mounts, Absent otherwise. Guarded by the SAME mu as mounts so the
	// phase value and the mount entry move together.
	phases map[storageunit.ReplicaUnit]storageunit.HandoffState
	// inFlight tracks positions whose overlap mount (the slow OpenUnit) is
	// running in a background goroutine. It is the idempotency guard: a
	// reconcile that re-drives an already-in-flight acquire must not spawn a
	// second open.
	inFlight map[storageunit.ReplicaUnit]struct{}
	// openSem is the node-wide permit gate bounding how many replica-unit opens
	// run CONCURRENTLY on this node's factory. Created LAZILY under mu on first
	// use so its size reflects the config at first acquire.
	openSem chan struct{}

	// openEpochs records the EXACT epoch THIS node opened each mounted position
	// at, captured from the factory's RETURN VALUE (not a re-read of the shared,
	// climbing durable epoch). It is the STABLE drain-release gate AND the epoch
	// the serving marker carries. Recorded just before the mount at every open
	// site; cleared on release. sync.Map: read without mu (see openEpochOf's
	// callers), so it needs no ordering against the mount map.
	openEpochs sync.Map // storageunit.ReplicaUnit -> storageunit.Epoch
	// acquireErr records, per position, why a DESIRED position is not mounted:
	// the most recent open failure during an acquire, or the non-error
	// "boot-deferred:" note. WRITE-ONLY OBSERVABILITY - the mount paths write
	// it, only the reporting surfaces (DebugState, MountReadiness) read it, and
	// no control flow may branch on it (it is a single map shared by every mount
	// site, so a retry loop reading it back would let an unrelated path's write
	// decide its branch).
	acquireErr sync.Map // storageunit.ReplicaUnit -> string
	// permits tracks which positions currently hold a node-wide open permit and
	// since when. Observability only: the per-tick acquire-queue summary names
	// the oldest holder, exactly the datum a wedged-open pipeline stall hides.
	permits sync.Map // storageunit.ReplicaUnit -> time.Time

	// wrap decorates a backend at the mount seam (the fence-self-healing
	// decorator). It is a PURE CONSTRUCTOR (nil check, type assert, struct
	// alloc); it never touches this table, so calling it under mu cannot
	// re-enter. Holding it here rather than at the call sites keeps the
	// decorator a property of "being mounted": no mount site can omit it.
	wrap func(storageunit.ReplicaUnit, backend.Backend) backend.Backend
	// closed points at the Cluster's closed flag. Read under mu by
	// mountUnlessClosed / startAcquire so the "is the cluster shutting down"
	// check pairs atomically with the mutation, exactly as the old call sites
	// read c.closed inside their mount-lock hold. A pointer, never a callback.
	closed *atomic.Bool
}

// init makes the table ready to hold mounts for c. It takes the two narrow
// dependencies the table needs from the cluster (the mount decorator and the
// closed flag) as data, NOT as a general cluster handle, so the set of things a
// table method can reach is auditable at this one line.
func (t *mountTable) init(c *Cluster) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mounts = make(map[storageunit.ReplicaUnit]backend.Backend)
	t.phases = make(map[storageunit.ReplicaUnit]storageunit.HandoffState)
	t.inFlight = make(map[storageunit.ReplicaUnit]struct{})
	t.wrap = c.newFencedSelfHealing
	t.closed = &c.closed
}

// isClosed reports the cluster's shutdown flag. Callers hold mu.
func (t *mountTable) isClosed() bool {
	return t.closed != nil && t.closed.Load()
}

// decorate applies the mount-seam decorator, or passes the backend through when
// the table was built without one (a bare white-box test fixture).
func (t *mountTable) decorate(ru storageunit.ReplicaUnit, b backend.Backend) backend.Backend {
	if t.wrap == nil {
		return b
	}
	return t.wrap(ru, b)
}

// ---------------------------------------------------------------- resolution

// backendFor returns the backend mounted at the EXACT position ru.
func (t *mountTable) backendFor(ru storageunit.ReplicaUnit) (backend.Backend, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	b, ok := t.mounts[ru]
	return b, ok
}

// backendForOrLowest resolves ru exactly, FALLING BACK to the LOWEST mounted
// position of the SAME unit when the exact position is not mounted (returning
// the position actually resolved, so a fence recode evicts the right mount).
func (t *mountTable) backendForOrLowest(ru storageunit.ReplicaUnit) (backend.Backend, storageunit.ReplicaUnit, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if b, ok := t.mounts[ru]; ok {
		return b, ru, true
	}
	return t.lowestLocked(ru.Unit)
}

// lowestForUnit returns the LOWEST mounted position of gu and its backend.
func (t *mountTable) lowestForUnit(gu storageunit.GenUnit) (backend.Backend, storageunit.ReplicaUnit, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lowestLocked(gu)
}

// lowestLocked is the shared lowest-mounted-position scan. Caller holds mu.
func (t *mountTable) lowestLocked(gu storageunit.GenUnit) (backend.Backend, storageunit.ReplicaUnit, bool) {
	var (
		be    backend.Backend
		got   storageunit.ReplicaUnit
		found bool
	)
	for cand, b := range t.mounts {
		if cand.Unit != gu {
			continue
		}
		if !found || cand.Replica < got.Replica {
			got, be, found = cand, b, true
		}
	}
	return be, got, found
}

// anyForUnit returns SOME mounted position of gu (any replica index). Used by
// the physical "we hold this unit's bytes even though the ring moved it off us"
// read resolver, which does not care which copy answers.
func (t *mountTable) anyForUnit(gu storageunit.GenUnit) (backend.Backend, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for ru, b := range t.mounts {
		if ru.Unit == gu {
			return b, true
		}
	}
	return nil, false
}

// ------------------------------------------------------------------ queries

// allMounted reports whether EVERY position in rus is mounted, evaluated
// against ONE coherent view of the table.
func (t *mountTable) allMounted(rus []storageunit.ReplicaUnit) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, ru := range rus {
		if _, ok := t.mounts[ru]; !ok {
			return false
		}
	}
	return true
}

// mountedList snapshots the mounted positions.
func (t *mountTable) mountedList() []storageunit.ReplicaUnit {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]storageunit.ReplicaUnit, 0, len(t.mounts))
	for ru := range t.mounts {
		out = append(out, ru)
	}
	return out
}

// mountedAtGen snapshots the mounted positions at generation gen.
func (t *mountTable) mountedAtGen(gen storageunit.Generation) []storageunit.ReplicaUnit {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []storageunit.ReplicaUnit
	for ru := range t.mounts {
		if ru.Unit.Gen == gen {
			out = append(out, ru)
		}
	}
	return out
}

// mountedCount is how many positions this node currently has mounted.
func (t *mountTable) mountedCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.mounts)
}

// mountedPairs snapshots every mount as (position, backend) in ascending
// (generation, unit, replica) order.
func (t *mountTable) mountedPairs() []mountedUnit {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ids := make([]storageunit.ReplicaUnit, 0, len(t.mounts))
	for ru := range t.mounts {
		ids = append(ids, ru)
	}
	sortReplicaUnits(ids)
	out := make([]mountedUnit, 0, len(ids))
	for _, ru := range ids {
		out = append(out, mountedUnit{ru: ru, b: t.mounts[ru]})
	}
	return out
}

// -------------------------------------------------------------------- phases

// phaseOf returns the in-flight HandoffState for ru (the zero value, with
// Phase 0, when ru is not in flight).
func (t *mountTable) phaseOf(ru storageunit.ReplicaUnit) storageunit.HandoffState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.phases[ru]
}

// hasLoserPhase reports whether any position is in a loser (Draining) phase.
func (t *mountTable) hasLoserPhase() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, st := range t.phases {
		if st.Phase.IsLoser() {
			return true
		}
	}
	return false
}

// drainingPositions snapshots every position currently in PhaseDraining.
func (t *mountTable) drainingPositions() []storageunit.ReplicaUnit {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]storageunit.ReplicaUnit, 0)
	for ru, st := range t.phases {
		if st.Phase == storageunit.PhaseDraining {
			out = append(out, ru)
		}
	}
	return out
}

// setPhase records an in-flight handoff state for ru. The gainer side sets
// PhaseAcquiring BEFORE its mount starts, so there is no instant where routing
// targets this node, the mount is incomplete, and no phase entry exists. The
// loser side goes through armDrain instead (it has to see the mount).
func (t *mountTable) setPhase(ru storageunit.ReplicaUnit, st storageunit.HandoffState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.phases[ru] = st
}

// armDrain performs the Owned -> Draining edge for ru at this node's open
// epoch, returning the still-mounted backend so the caller can flush it. It
// reports false (and changes nothing) when ru is not mounted, is already
// mid-transition, or the FSM rejects the edge - so the caller's displacement
// flush fires EXACTLY ONCE per displacement, on the edge alone.
func (t *mountTable) armDrain(ru storageunit.ReplicaUnit, open storageunit.Epoch) (backend.Backend, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	b, mounted := t.mounts[ru]
	if !mounted {
		return nil, false // lost the mount under us (a concurrent evict).
	}
	if t.phases[ru].Phase != 0 {
		return nil, false
	}
	next, err := storageunit.NextOnDrain(storageunit.HandoffState{})
	if err != nil {
		return nil, false
	}
	next.OpenEpoch = open
	t.phases[ru] = next
	return b, true
}

// reclaimDrain aborts an in-flight drain for a position this node owns again:
// it clears the loser-phase entry so the position converges back to Owned. A
// no-op unless the position is still mounted AND in a loser phase. The mount is
// never touched, so the reclaim has no availability gap.
func (t *mountTable) reclaimDrain(ru storageunit.ReplicaUnit) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, mounted := t.mounts[ru]; !mounted {
		return
	}
	if cur, ok := t.phases[ru]; ok && cur.Phase.IsLoser() {
		delete(t.phases, ru)
	}
}

// finishStuckGainer completes a half-done flip: a MOUNTED position in a GAINER
// phase with NO acquire goroutine in flight never dropped its phase, so it never
// reads as serving. Dropping the phase converges it to Owned. Reports whether it
// did, so the caller writes the serving marker outside the lock.
func (t *mountTable) finishStuckGainer(ru storageunit.ReplicaUnit) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, running := t.inFlight[ru]; running {
		return false // a flip goroutine is running; let it complete.
	}
	st, ok := t.phases[ru]
	if !ok || !st.Phase.IsGainer() {
		return false // Owned (no phase) or a loser phase: nothing to finish.
	}
	if _, mounted := t.mounts[ru]; !mounted {
		return false // not mounted; the acquire half re-drives the mount.
	}
	delete(t.phases, ru) // mounted + no phase = Owned (serving locally).
	return true
}

// clearOrphanedDrain converges a Draining position whose mount is already gone
// straight to Absent, reporting whether it did. Without it, such a phase would
// loop forever in the readiness poll while the acquire half's loser-phase skip
// kept the position from re-mounting.
func (t *mountTable) clearOrphanedDrain(ru storageunit.ReplicaUnit) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur, ok := t.phases[ru]
	if !ok || cur.Phase != storageunit.PhaseDraining {
		return false
	}
	if _, mounted := t.mounts[ru]; mounted {
		return false
	}
	delete(t.phases, ru)
	return true
}

// flipResult is the outcome of completeAcquireFlip.
type flipResult int

const (
	// flipSuperseded: Close ran before the flip; nothing was mounted and the
	// caller must release the freshly-opened backend.
	flipSuperseded flipResult = iota
	// flipMountedResolved: the mount is installed, but the phase entry was
	// already resolved elsewhere (or the FSM edge was illegal), so this was not
	// the clean Acquiring -> Ready -> Owned advance.
	flipMountedResolved
	// flipMountedReady: the clean Acquiring -> Ready -> Owned advance.
	flipMountedReady
)

// completeAcquireFlip is THE MOUNT FLIP: it installs the mount entry and
// resolves the handoff phase to Owned under ONE lock hold, so a routed op never
// sees the mount present without the phase resolved (and vice versa). Every
// mounted outcome leaves the same steady state (mounted, no phase entry); the
// result only tells the caller which log line and cleanup to run OUTSIDE the
// lock.
func (t *mountTable) completeAcquireFlip(ru storageunit.ReplicaUnit, b backend.Backend, opened storageunit.Epoch) flipResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.isClosed() {
		return flipSuperseded
	}
	res := flipMountedReady
	cur := t.phases[ru]
	switch {
	case cur.Phase != storageunit.PhaseAcquiring:
		// The phase entry is gone or not Acquiring (a concurrent reconcile
		// already flipped or dropped it). The mount is still installed below (it
		// is the authoritative durable owner) and any stale phase entry dropped.
		res = flipMountedResolved
	default:
		if _, err := storageunit.NextOnReady(cur, opened); err != nil {
			// Illegal edge should not happen (cur is Acquiring); mount and drop
			// the phase to converge to Owned regardless.
			res = flipMountedResolved
		}
	}
	t.mountLocked(ru, b)
	// Ready is transient: once the mount entry is present the node serves
	// locally, so the steady state is Owned (no phase entry). Drop the entry
	// rather than parking in Ready - Owned = mounted + no phase, per the FSM's
	// steady-state poles.
	delete(t.phases, ru)
	return res
}

// releaseDrained is the drain release: it re-checks the Draining phase and
// removes the mount entry in ONE critical section, returning the removed backend
// so the caller can drop its flush state and close it OUTSIDE the lock. Only
// this hold can take THIS entry, which is the exactly-once guard however many
// pollers observed the successor's marker.
func (t *mountTable) releaseDrained(ru storageunit.ReplicaUnit) (backend.Backend, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur, inFlight := t.phases[ru]
	if !inFlight || cur.Phase != storageunit.PhaseDraining {
		return nil, false // another tick already advanced it.
	}
	b, mounted := t.mounts[ru]
	if !mounted || b == nil {
		// Already evicted; just drop the phase entry to converge to Absent.
		delete(t.phases, ru)
		return nil, false
	}
	if _, err := storageunit.NextOnRelease(cur); err != nil {
		return nil, false
	}
	delete(t.mounts, ru)
	t.openEpochs.Delete(ru) // released; a re-acquire records a fresh open epoch.
	// Drop the phase entry: Releasing is transient, the steady state after the
	// release is Absent (no mount, no phase).
	delete(t.phases, ru)
	return b, true
}

// ----------------------------------------------------------------- in flight

// startAcquire claims the in-flight slot for ru, reporting whether the caller
// owns it. False means the cluster is closing or an open for this position is
// already running: do NOT spawn a second open.
func (t *mountTable) startAcquire(ru storageunit.ReplicaUnit) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.isClosed() {
		return false
	}
	if _, running := t.inFlight[ru]; running {
		return false
	}
	t.inFlight[ru] = struct{}{}
	return true
}

// finishAcquire releases the in-flight slot claimed by startAcquire.
func (t *mountTable) finishAcquire(ru storageunit.ReplicaUnit) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inFlight, ru)
}

// acquireInFlight reports whether an open for ru is currently running.
func (t *mountTable) acquireInFlight(ru storageunit.ReplicaUnit) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, running := t.inFlight[ru]
	return running
}

// ------------------------------------------------------------------ mutation

// mountLocked installs ru -> b, decorated. Caller holds mu.
//
// A successful mount clears any recorded acquire error BY CONSTRUCTION: doing
// it at this single choke point means no mount site (boot, reconcile acquire,
// overlap handoff, reshard split, or a future one) can leave a stale record
// behind for FailedOpenUnits/LastAcquireError to resurface once the position
// later unmounts.
func (t *mountTable) mountLocked(ru storageunit.ReplicaUnit, b backend.Backend) {
	t.acquireErr.Delete(ru)
	t.mounts[ru] = t.decorate(ru, b)
}

// mount installs ru -> b, decorated.
func (t *mountTable) mount(ru storageunit.ReplicaUnit, b backend.Backend) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mountLocked(ru, b)
}

// mountUnlessClosed installs ru -> b unless the cluster is shutting down,
// reporting whether it mounted. The closed check pairs atomically with the
// insert: Close sets the flag BEFORE draining the table, so observing false
// here proves the drain has not started and this mount will be closed by it.
// Mounting after the drain would leak the backend past shutdown.
func (t *mountTable) mountUnlessClosed(ru storageunit.ReplicaUnit, b backend.Backend) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.isClosed() {
		return false
	}
	t.mountLocked(ru, b)
	return true
}

// mountUndecorated installs ru -> b WITHOUT the mount-seam decorator and
// without clearing the acquire record. It is the white-box test seam that
// mirrors a raw mount-map assignment: tests that inject a fenced/failing handle
// and then assert the exact handle is evicted need the value they stored to be
// the value the table holds. Production mount sites use mount /
// mountUnlessClosed / completeAcquireFlip.
func (t *mountTable) mountUndecorated(ru storageunit.ReplicaUnit, b backend.Backend) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mounts[ru] = b
}

// unmount removes ru's mount entry. The entry goes BEFORE the factory close so
// a routed op stops resolving the local backend immediately.
func (t *mountTable) unmount(ru storageunit.ReplicaUnit) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.mounts, ru)
}

// evictIfSame drops ru's mount if the entry still points at the SAME backend
// that just failed, reporting whether it evicted. The compare-by-pointer guard
// avoids evicting a healthy mount a concurrent reconcile already swapped in.
//
// A position mid-DRAIN is NEVER evicted: a fenced/failed write there is the
// EXPECTED successor-fence signal, not the stale-handle desync this path exists
// for, and evicting would drop the leaver's mount before its successor proves it
// is serving.
func (t *mountTable) evictIfSame(ru storageunit.ReplicaUnit, failed backend.Backend) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st, ok := t.phases[ru]; ok && st.Phase.IsLoser() {
		return false
	}
	if cur, ok := t.mounts[ru]; ok && cur == failed {
		delete(t.mounts, ru)
		t.openEpochs.Delete(ru) // re-acquire records a fresh open epoch.
		return true
	}
	return false
}

// dropGeneration removes every mount at generation gen and returns the GenUnits
// it dropped, so the caller closes them OUTSIDE the lock. Collect-and-delete is
// one critical section so a concurrent mount cannot slip a position past the
// sweep between the two halves.
func (t *mountTable) dropGeneration(gen storageunit.Generation) []storageunit.GenUnit {
	t.mu.Lock()
	defer t.mu.Unlock()
	dropped := make([]storageunit.GenUnit, 0)
	for ru := range t.mounts {
		if ru.Unit.Gen == gen {
			dropped = append(dropped, ru.Unit)
		}
	}
	for _, gu := range dropped {
		delete(t.mounts, replica0(gu))
	}
	return dropped
}

// takeAll empties the table and returns every position it held, so the caller
// releases them via the factory OUTSIDE the lock. Used by shutdown (and the
// Open-time rollback): the factory close is backing-store I/O and must not run
// under a lock routed ops take.
func (t *mountTable) takeAll() []storageunit.ReplicaUnit {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]storageunit.ReplicaUnit, 0, len(t.mounts))
	for ru := range t.mounts {
		out = append(out, ru)
		delete(t.mounts, ru)
	}
	return out
}

// -------------------------------------------------------------------- epochs

// openEpochOf returns the exact epoch this node opened ru at, if recorded.
// Read WITHOUT mu, deliberately: the drain path reads it before taking the lock
// so its defensive durable-epoch fallback can never do shared-storage I/O under
// a lock routed ops need.
func (t *mountTable) openEpochOf(ru storageunit.ReplicaUnit) (storageunit.Epoch, bool) {
	v, ok := t.openEpochs.Load(ru)
	if !ok {
		return 0, false
	}
	e, ok := v.(storageunit.Epoch)
	return e, ok
}

// recordOpenEpoch stores this node's exact open epoch for ru. Every open site
// records it just BEFORE the mount, so a drain that sees the mount also sees the
// epoch.
func (t *mountTable) recordOpenEpoch(ru storageunit.ReplicaUnit, e storageunit.Epoch) {
	t.openEpochs.Store(ru, e)
}

// forgetOpenEpoch drops the recorded open epoch for ru; a re-acquire records a
// fresh one.
func (t *mountTable) forgetOpenEpoch(ru storageunit.ReplicaUnit) {
	t.openEpochs.Delete(ru)
}

// ------------------------------------------------------------ acquire errors

// recordAcquireErr records why a desired position is not mounted.
func (t *mountTable) recordAcquireErr(ru storageunit.ReplicaUnit, msg string) {
	t.acquireErr.Store(ru, msg)
}

// clearAcquireErr drops ru's acquire record.
func (t *mountTable) clearAcquireErr(ru storageunit.ReplicaUnit) {
	t.acquireErr.Delete(ru)
}

// acquireErrOf returns ru's recorded acquire error, if any. PRESENCE decides
// the bool (the readiness tally counts a recorded position as failed whatever
// the record holds); the string cast is best-effort because only strings are
// ever recorded.
func (t *mountTable) acquireErrOf(ru storageunit.ReplicaUnit) (string, bool) {
	v, ok := t.acquireErr.Load(ru)
	if !ok {
		return "", false
	}
	s, _ := v.(string)
	return s, true
}

// ------------------------------------------------------------------- permits

// openPermit blocks until a node-wide open permit is available and returns the
// release func. The gate is created lazily on first use, sized by the caller's
// current concurrency setting at that moment. The BLOCKING send happens after
// the lock is dropped: waiting for a permit under mu would stall every routed
// op's mount lookup behind a slow open.
func (t *mountTable) openPermit(size int) (release func()) {
	t.mu.Lock()
	if t.openSem == nil {
		t.openSem = make(chan struct{}, size)
	}
	sem := t.openSem
	t.mu.Unlock()
	sem <- struct{}{}
	return func() { <-sem }
}

// holdPermit records that ru holds an open permit as of now.
func (t *mountTable) holdPermit(ru storageunit.ReplicaUnit) {
	t.permits.Store(ru, time.Now())
}

// dropPermit clears ru's permit-holder record.
func (t *mountTable) dropPermit(ru storageunit.ReplicaUnit) {
	t.permits.Delete(ru)
}

// permitSummary reports how many positions hold an open permit plus the oldest
// holder and when it took the permit (the zero position/time when none do).
func (t *mountTable) permitSummary() (holders int, oldestRU storageunit.ReplicaUnit, oldest time.Time) {
	t.permits.Range(func(k, v any) bool {
		holders++
		if ts, ok := v.(time.Time); ok && (oldest.IsZero() || ts.Before(oldest)) {
			oldest = ts
			if ru, ok := k.(storageunit.ReplicaUnit); ok {
				oldestRU = ru
			}
		}
		return true
	})
	return holders, oldestRU, oldest
}

// ----------------------------------------------------------------- reporting

// mountCounts is the mount-state tally over a desired set.
type mountCounts struct {
	Mounted    int
	Pending    int
	Failed     int
	FirstError string
}

// readiness tallies the desired set against the table under ONE read hold.
//
// COUNTING IS BY MOUNT PRESENCE ONLY: a position present in the mount map counts
// as Mounted regardless of its handoff phase (a fenced-but-unevicted mount still
// counts). Positions NOT in desired count nowhere, so a mounted-but-no-longer-
// desired position (a loser mid-drain) is invisible here - readiness is about
// what the node owes. FirstError is the record of the first failed position in
// position-string order, which makes it deterministic across calls at unchanged
// state.
func (t *mountTable) readiness(desired []storageunit.ReplicaUnit) mountCounts {
	var (
		counts      mountCounts
		firstFailed string // ru.String() of the failed position picked so far
	)
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, ru := range desired {
		if _, ok := t.mounts[ru]; ok {
			counts.Mounted++
			continue
		}
		counts.Pending++
		msg, ok := t.acquireErrOf(ru)
		if !ok {
			continue
		}
		counts.Failed++
		if key := ru.String(); firstFailed == "" || key < firstFailed {
			firstFailed = key
			counts.FirstError = msg
		}
	}
	return counts
}

// queueStats reports the mounted count, the number of positions in a gainer
// phase, and the number of opens in flight, from ONE coherent view.
func (t *mountTable) queueStats() (mounted, gainers, inFlight int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, st := range t.phases {
		if st.Phase.IsGainer() {
			gainers++
		}
	}
	return len(t.mounts), gainers, len(t.inFlight)
}

// mountSnapshot is a coherent copy of the table's per-position state, for the
// diagnostic dump.
type mountSnapshot struct {
	mounted  map[storageunit.ReplicaUnit]bool
	phases   map[storageunit.ReplicaUnit]storageunit.HandoffState
	inFlight []storageunit.ReplicaUnit
}

// snapshot copies the mounted set, the handoff phases and the in-flight acquires
// under ONE read hold, so the dump cannot show a mount and a phase from
// different instants.
func (t *mountTable) snapshot() mountSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	snap := mountSnapshot{
		mounted:  make(map[storageunit.ReplicaUnit]bool, len(t.mounts)),
		phases:   make(map[storageunit.ReplicaUnit]storageunit.HandoffState, len(t.phases)),
		inFlight: make([]storageunit.ReplicaUnit, 0, len(t.inFlight)),
	}
	for ru := range t.mounts {
		snap.mounted[ru] = true
	}
	for ru, st := range t.phases {
		snap.phases[ru] = st
	}
	for ru := range t.inFlight {
		snap.inFlight = append(snap.inFlight, ru)
	}
	return snap
}
