// Doubling resharder for multi-backend mode (v0.8 Phase 4).
//
// This file holds the ONLINE per-unit BISECT that grows the cluster by
// DOUBLING the unit count: N -> 2N at a new cluster generation. Each old
// gen-g unit K bisects into EXACTLY the two new gen-(g+1) units K and K+N by
// one additional hash bit (storageunit.ChildUnit). The old unit KEEPS SERVING
// reads + writes while its data is copied into the two children; then a brief
// per-key-space write-pause drains the last writes and an atomic cut-over
// flips routing for that unit's key-space. March through all N old units one
// at a time and the cluster is uniformly at gen g+1 (2N units).
//
// THE INVARIANT THIS FILE IS BUILT TO:
//
//	NO ACKED WRITE MAY BE LOST DURING THE BISECT.
//
// The hazard is a write to old-unit-K that arrives DURING the background copy
// or the catch-up. How the copy-then-catch-up-then-atomic-cut-over pattern
// (the same shape as the validated Phase 3 lease handoff) upholds it:
//
//   - BEFORE cut-over, every routed write for K's key-space resolves to the
//     OLD gen-g unit K (resolveGenUnit returns GenUnit{g, K} while K is not in
//     the cut-over set). So an acked write either (a) landed before the copy
//     scan reached its key - the copy picks it up - or (b) landed after the
//     copy passed its key - the CATCH-UP drain (a second copy pass taken under
//     the write-pause, after writes are quiesced) picks it up.
//   - THE CUT-OVER BOUNDARY: the resharder takes the per-unit write-pause lock
//     (pauseUnit) BEFORE the catch-up drain and adds K to the cut-over set
//     while still holding it. Routed writes for K's key-space block on that
//     lock for the brief pause. So no write can slip BETWEEN "catch-up drain
//     finished reading old-K" and "cut-over flag set": any write that was
//     about to land is blocked until the flag is up, and then re-resolves to
//     the NEW gen-(g+1) child. The boundary is clean: every write is captured
//     either by the drain (it landed in old-K before the pause) or by the new
//     unit (it lands after the flag flips).
//   - AFTER cut-over, routed writes for K's key-space resolve to the gen-(g+1)
//     child (resolveGenUnit returns GenUnit{g+1, child} once K is in the
//     cut-over set), so they go straight to the new database.
//
// With R=1 + durable-before-ack, every acked write is durable in the resolved
// unit's backing store before its ack, so after the reshard every acked write
// is visible. The phase is built to this invariant.
//
// Scope: multi-backend mode ONLY (legacy per-node path untouched). R=1 only.
// Assumes STABLE membership for the duration of a reshard (concurrent
// membership-change + reshard is out of scope - a later concern).

package cluster

import (
	"fmt"
	"sync"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// genState is the generation-aware routing state. It is a plain value held
// behind c.genMu; routing reads a snapshot, the resharder mutates it under
// the write lock. See the field doc on Cluster.genState.
type genState struct {
	// gen is the cluster's CURRENT generation. Steady state: every key
	// resolves to GenUnit{gen, UnitForHash(h, count)}.
	gen storageunit.Generation
	// count is the unit count N at the current generation.
	count storageunit.UnitCount
	// nextCount is the doubled count 2N at gen+1. Zero (IsZero) when no
	// reshard is in flight; set for the duration of a reshard so mid-reshard
	// routing can map a cut-over key into the gen-(g+1) space.
	nextCount storageunit.UnitCount
	// cutOver is the set of OLD (gen-g) unit ids that have already bisected +
	// cut over. A key whose old unit is in this set routes to its gen-(g+1)
	// child; a key whose old unit is NOT in it routes to the old gen-g unit.
	// Empty in steady state (the common, no-reshard path).
	cutOver map[storageunit.UnitID]struct{}
}

// hasCutOver reports whether old unit u has cut over (its key-space now lives
// in the gen-(g+1) children).
func (g genState) hasCutOver(u storageunit.UnitID) bool {
	if g.cutOver == nil {
		return false
	}
	_, ok := g.cutOver[u]
	return ok
}

// clone returns a deep copy of g (cutOver set duplicated) so the resharder
// can build the next state without mutating the snapshot routed ops may be
// reading. genState is otherwise value-copied; only the map needs care.
func (g genState) clone() genState {
	cp := g
	cp.cutOver = make(map[storageunit.UnitID]struct{}, len(g.cutOver))
	for u := range g.cutOver {
		cp.cutOver[u] = struct{}{}
	}
	return cp
}

// initGenState seeds the routing state at Open for a multi-backend cluster:
// generation 0, count = the configured UnitCount, no reshard in flight. Called
// from initMultiBackend before any unit is mounted.
func (c *Cluster) initGenState() {
	c.genMu.Lock()
	c.genState = genState{
		gen:     0,
		count:   c.unitCount,
		cutOver: make(map[storageunit.UnitID]struct{}),
	}
	c.genMu.Unlock()
	c.pauseMu.Lock()
	c.pauseUnits = make(map[storageunit.UnitID]*sync.RWMutex)
	c.pauseMu.Unlock()
}

// genSnapshot returns a read-locked copy of the current routing state. The
// returned value's cutOver map is the LIVE map (not copied) but is only read
// by callers; the resharder never mutates the live map in place - it swaps in
// a freshly-cloned genState under the write lock (see commitGenState), so a
// reader holding this snapshot value sees a coherent, immutable set.
func (c *Cluster) genSnapshot() genState {
	c.genMu.RLock()
	defer c.genMu.RUnlock()
	return c.genState
}

// commitGenState swaps in a new routing state under the write lock. The
// resharder builds the next state from a clone (so in-flight readers keep
// their old snapshot), then publishes it here in one atomic store.
func (c *Cluster) commitGenState(next genState) {
	c.genMu.Lock()
	c.genState = next
	c.genMu.Unlock()
}

// resolveGenUnit maps a key's shard hash to the generation-qualified unit it
// currently belongs to. This is THE routing decision (used by both ownership
// and local-backend lookup), generation-aware:
//
//   - Compute the OLD unit K = UnitForHash(h, count) at the current gen.
//   - If K has cut over -> the key now lives in its gen-(g+1) child:
//     GenUnit{gen+1, UnitForHash(h, nextCount)}. (UnitForHash at 2N yields
//     exactly one of K's two children, per the doubling math.)
//   - Else -> the old unit: GenUnit{gen, K}.
//
// In steady state (cutOver empty) every key resolves to GenUnit{gen, K}, so
// the routing is identical to Phase 2 / Phase 3 with a constant generation
// prefix. Deterministic; the same (hash, genState) always yields the same
// GenUnit, so every node agrees.
func (g genState) resolveGenUnit(h storageunit.ShardHash) storageunit.GenUnit {
	k := storageunit.UnitForHash(h, g.count)
	if g.hasCutOver(k) {
		child := storageunit.UnitForHash(h, g.nextCount)
		return storageunit.NewGenUnit(g.gen+1, child)
	}
	return storageunit.NewGenUnit(g.gen, k)
}

// genUnitForKey resolves the generation-qualified unit for a raw key, using
// the SAME shard-key extraction the ring uses (so co-located keys share a unit
// and a transaction stays in one engine). Multi-backend mode only.
func (c *Cluster) genUnitForKey(key []byte) storageunit.GenUnit {
	h := storageunit.HashShardKey(c.shardKey(key))
	return c.genSnapshot().resolveGenUnit(h)
}

// pauseLockFor returns the per-OLD-unit write-pause RWMutex for unit u,
// creating it on first touch. The routed write path takes the READ side
// briefly (so many writers proceed concurrently in steady state); the
// resharder takes the WRITE side during the catch-up + cut-over so all writes
// to u's key-space are quiesced across the boundary. Lazily allocated so
// steady-state ops that never reshard pay nothing.
func (c *Cluster) pauseLockFor(u storageunit.UnitID) *sync.RWMutex {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	m, ok := c.pauseUnits[u]
	if !ok {
		m = &sync.RWMutex{}
		c.pauseUnits[u] = m
	}
	return m
}

// localWriteBackendForKey resolves the mounted backend a LOCAL WRITE on key
// must apply against, holding the per-old-unit write-pause across the resolve
// so the cut-over boundary is clean. It returns the backend, an unlock
// function the caller MUST defer (releases the pause read-lock), and ok.
//
// THE CUT-OVER BOUNDARY (NO ACKED WRITE LOST): the pause is keyed by the OLD
// unit id K = UnitForHash(h, count). Taking its READ side here, then resolving
// the GenUnit + looking up the backend WHILE STILL HOLDING IT, means:
//
//   - If the resharder is mid-cut-over for K, it holds the pause WRITE side,
//     so this writer blocks until cut-over completes. When it wakes, the
//     genState already has K in the cut-over set, so resolveGenUnit returns the
//     gen-(g+1) child and the write lands in the new database - never in the
//     just-retired old-K.
//   - If no reshard is touching K, the read-lock is uncontended (many writers
//     proceed concurrently) and the resolve returns the steady-state GenUnit.
//
// ok=false means the resolved unit is not mounted here (owner-but-unmounted
// handoff window, or never owned): the caller returns the retryable
// acquiring-window error. The pause is still released via unlock in that case.
// Multi-backend mode only.
func (c *Cluster) localWriteBackendForKey(key []byte) (be backend.Backend, gu storageunit.GenUnit, unlock func(), ok bool) {
	h := storageunit.HashShardKey(c.shardKey(key))
	// Key the pause by the OLD unit at the CURRENT count. resolveGenUnit (under
	// genMu) decides old-vs-new; the pause must be on the same K the resharder
	// pauses, which is the old-count unit id.
	//
	// TODO(reshard-toctou): the pause key here and resolveGenUnit below read the
	// genState via two separate genSnapshot() calls. They can only straddle the
	// reshard's FINAL count flip (N->2N); a single-node reshard at the final
	// commit could pause on the gen-N K while the resolve picks the gen-(g+1)
	// unit. In that window the write still lands in the RESOLVED (correct) unit
	// and the stale pause is harmless (no cut-over is in flight post-reshard),
	// which is why the 252k-write gate sees no loss; but the two snapshots
	// should be unified (derive oldK + the resolve from ONE snapshot taken with
	// the pause held) so the invariant is structural, not incidental.
	oldK := storageunit.UnitForHash(h, c.genSnapshot().count)
	pause := c.pauseLockFor(oldK)
	pause.RLock()
	gu = c.genSnapshot().resolveGenUnit(h)
	c.mountMu.RLock()
	be, ok = c.mountMap[gu]
	c.mountMu.RUnlock()
	return be, gu, pause.RUnlock, ok
}

// evictStaleMount drops gu from the mount map if the entry still points at the
// SAME backend that just failed a write. A local write that fails on an
// owned+mounted unit means the mount is STALE: the unit's lease moved (it was
// re-acquired at a higher epoch by a concurrent reconcile - the cross-node
// reshard redistribution window where one node pre-acquired a gen-(g+1) child
// another node then re-created with data at a higher epoch, fencing the first
// mount). Evicting the stale entry lets the next reconcile (self-heal loop or
// the bumpRingGen-scheduled pass) RE-ACQUIRE the unit fresh at the durable-max
// epoch, with the other node's data visible copy-free. The compare-by-pointer
// guard avoids evicting a healthy mount a concurrent reconcile already swapped
// in. The routed op returns the retryable acquiring-window error so the
// originator retries and lands on the freshly re-acquired (or re-routed) mount.
// This is the Phase 3 self-heal philosophy applied to a fenced mount; it keeps
// the NO-ACKED-WRITE-LOST invariant by never acking a write that did not land.
func (c *Cluster) evictStaleMount(gu storageunit.GenUnit, failed backend.Backend) {
	c.mountMu.Lock()
	evicted := false
	if cur, ok := c.mountMap[gu]; ok && cur == failed {
		delete(c.mountMap, gu)
		evicted = true
	}
	c.mountMu.Unlock()
	if evicted && c.ring != nil {
		// Schedule a (debounced) reconcile so the unit is RE-ACQUIRED fresh at
		// the durable-max epoch promptly, rather than waiting for the slow
		// self-heal loop tick. Safe to call from the KV path: scheduleReconcile
		// only swaps the settle timer.
		c.scheduleReconcile()
	}
}

// Reshard performs ONE doubling reshard: it advances the cluster from the
// current generation g (N units) to g+1 (2N units) by bisecting every old
// gen-g unit this node currently mounts into its two gen-(g+1) children,
// online and per-unit. It is the explicit operator trigger.
//
// For each old unit K this node mounts at gen g:
//
//  1. CREATE the two new gen-(g+1) units K and K+N (fresh databases).
//  2. BACKGROUND-COPY: scan old-K's keys, routing each to new-K or new-(K+N)
//     by ChildUnit (the new hash bit). Old-K KEEPS SERVING throughout.
//  3. CATCH-UP + ATOMIC CUT-OVER: take old-K's write-pause, drain the last
//     writes (a second copy pass under the pause), flip the cut-over flag for
//     K's key-space, release the pause. Retire old-K (CloseUnit at gen g).
//
// After all locally-mounted old units bisect, advance the cluster's live
// generation to g+1 and clear the cut-over set (every key now resolves under
// the gen-(g+1) map directly), then bump the ring generation so the Phase 3
// reconcile redistributes the 2N gen-(g+1) units across nodes by lease
// handoff (the ring re-keys onto the new generation-qualified unit ids).
//
// Serialized by reshardMu: two concurrent Reshard calls cannot interleave. In
// legacy mode it is a no-op error (resharding is multi-backend only).
//
// NO ACKED WRITE LOST: see this file's header. The copy-then-catch-up-then-
// atomic-cut-over pattern makes the cut-over boundary clean.
func (c *Cluster) Reshard() error {
	if c.notReady() {
		return fmt.Errorf("cluster: Reshard on a closed cluster")
	}
	if !c.multi {
		return fmt.Errorf("cluster: Reshard is only valid in multi-backend mode")
	}
	// v0.8 Phase 4 supports the SINGLE-NODE online reshard, which is gate-
	// validated lossless under concurrent writes. A multi-node doubling needs
	// a cluster-wide generation BARRIER: every node must route at one
	// generation and the write-pause must span the logical key-space, not one
	// node's local view. Reshard is a purely local trigger with no such
	// coordination, so a multi-node reshard under concurrent writes can lose
	// acked writes. Refuse it rather than ship that footgun; the cluster-wide
	// barrier is a follow-up (see docs/SPEC.md, v0.8 Phase 4 scope).
	if c.ring != nil && len(c.ring.Members()) > 1 {
		return fmt.Errorf("cluster: multi-node Reshard is not yet supported (a cluster-wide generation barrier is required for concurrent-write safety); reshard requires a single-node cluster")
	}
	c.reshardMu.Lock()
	defer c.reshardMu.Unlock()

	start := c.genSnapshot()
	// Reject if a doubling would exceed the unit-count ceiling. Checking up
	// front means a reshard either fully proceeds or fully refuses; it never
	// half-bisects and then discovers it cannot double.
	nextCount, err := start.count.Double()
	if err != nil {
		return fmt.Errorf("cluster: Reshard cannot double %s: %w", start.count, err)
	}

	// Publish nextCount so mid-reshard routing can map cut-over keys into the
	// gen-(g+1) space. cutOver starts empty (nothing has bisected yet).
	mid := start.clone()
	mid.nextCount = nextCount
	c.commitGenState(mid)

	// Snapshot the OLD gen-g units this node currently mounts. Stable
	// membership for the reshard's duration (a scope assumption) means this
	// set does not move out from under us mid-reshard.
	oldUnits := c.mountedOldUnits(start.gen)

	for _, k := range oldUnits {
		if err := c.bisectUnit(start.gen, k, start.count, nextCount); err != nil {
			return fmt.Errorf("cluster: Reshard bisect unit %d: %w", k, err)
		}
	}

	// All locally-mounted old units have bisected. Advance the live generation
	// to g+1 and clear the cut-over set: from here every key resolves directly
	// under the gen-(g+1) map (count 2N), no cut-over indirection needed.
	final := genState{
		gen:     start.gen + 1,
		count:   nextCount,
		cutOver: make(map[storageunit.UnitID]struct{}),
	}
	c.commitGenState(final)

	// Re-key the ring onto the 2N generation-qualified unit ids: the Phase 3
	// reconcile acquires/releases so the new units redistribute across nodes
	// by lease handoff. In single-node mode this is a cheap no-op (self owns
	// everything regardless of generation). bumpRingGen schedules the
	// debounced reconcile; in multi mode (c.multi) that is scheduleReconcile.
	if c.ring != nil {
		c.bumpRingGen()
	}
	// Run the reconcile directly (we already hold reshardMu, so we must NOT
	// call runReconcile - it would re-take reshardMu and deadlock). The live
	// generation just advanced, so this mounts any gen-(g+1) unit this node
	// owns that the bisect did not create locally and releases any retired
	// gen-g unit still lingering. Idempotent + cheap. In multi-node mode the
	// bumpRingGen above ALSO schedules a debounced reconcile on every node so
	// the new units redistribute by lease handoff; this direct call just makes
	// the local view consistent immediately.
	c.reconcileMu.Lock()
	c.reconcileUnits()
	c.reconcileMu.Unlock()
	return nil
}

// mountedOldUnits returns the UnitIDs of the gen-g units this node currently
// has mounted, ascending. The resharder bisects exactly these (each node only
// bisects the units it locally owns; the new children land on their ring
// owners via reconcile afterward).
func (c *Cluster) mountedOldUnits(gen storageunit.Generation) []storageunit.UnitID {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	out := make([]storageunit.UnitID, 0, len(c.mountMap))
	for gu := range c.mountMap {
		if gu.Gen == gen {
			out = append(out, gu.ID)
		}
	}
	// ascending for a deterministic march
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// bisectUnit performs the online per-unit bisect of one old gen-g unit K into
// its two gen-(g+1) children K and K+N. See Reshard for the three-step shape
// and this file's header for the NO-ACKED-WRITE-LOST cut-over reasoning.
func (c *Cluster) bisectUnit(gen storageunit.Generation, k storageunit.UnitID, oldCount, newCount storageunit.UnitCount) error {
	oldGU := storageunit.NewGenUnit(gen, k)
	low, high, err := storageunit.ChildUnits(k, oldCount)
	if err != nil {
		return fmt.Errorf("child units of %d: %w", k, err)
	}
	lowGU := storageunit.NewGenUnit(gen+1, low)
	highGU := storageunit.NewGenUnit(gen+1, high)

	// 1. CREATE the two fresh gen-(g+1) children (open at the base epoch; they
	// are brand-new databases with no prior writer to fence). Mount them so
	// the copy can write into them AND so the local routing fast-path resolves
	// them once the cut-over flag flips (resolveGenUnit -> mountMap lookup).
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

	// The source backend (old gen-g unit K), which keeps serving throughout.
	c.mountMu.RLock()
	src, ok := c.mountMap[oldGU]
	c.mountMu.RUnlock()
	if !ok {
		return fmt.Errorf("old unit %s not mounted", oldGU)
	}

	// 2. BACKGROUND-COPY: split old-K's keys into the two children by the new
	// hash bit. Old-K keeps serving reads + writes; a write that lands here
	// AFTER the copy passes its key is captured by the catch-up drain below.
	if err := c.copyUnitInto(src, lowBE, highBE, oldCount, newCount); err != nil {
		return fmt.Errorf("copy %s: %w", oldGU, err)
	}

	// 3. CATCH-UP + ATOMIC CUT-OVER. Take old-K's write-pause (WRITE side):
	// every routed write for K's key-space blocks here for the brief pause, so
	// no write can land in old-K while we drain + flip the flag. This is THE
	// cut-over boundary that makes NO-ACKED-WRITE-LOST hold.
	pause := c.pauseLockFor(k)
	pause.Lock()
	// Catch-up drain: with writes quiesced, old-K is now FROZEN, so make the
	// two children EXACTLY MATCH old-K's current contents. Clear them first,
	// then full-copy: this captures every write AND every DELETE that landed in
	// old-K after the bulk copy (step 2). A delete is a key present in a child
	// from step 2 but absent from frozen old-K; clearing the children before
	// the re-copy removes that stale value (an incremental re-copy alone would
	// leave a deleted key resurrected in the child). The clear+copy is bounded
	// by old-K's size, the same cost as the bulk pass; it runs only during the
	// brief write-pause.
	if err := clearBackend(lowBE); err != nil {
		pause.Unlock()
		return fmt.Errorf("catch-up clear %s: %w", lowGU, err)
	}
	if err := clearBackend(highBE); err != nil {
		pause.Unlock()
		return fmt.Errorf("catch-up clear %s: %w", highGU, err)
	}
	if err := c.copyUnitInto(src, lowBE, highBE, oldCount, newCount); err != nil {
		pause.Unlock()
		return fmt.Errorf("catch-up %s: %w", oldGU, err)
	}
	// ATOMIC CUT-OVER: add K to the cut-over set while still holding the pause.
	// The instant this commits, resolveGenUnit routes K's key-space to the
	// gen-(g+1) children. Writers blocked on the pause wake up AFTER this and
	// re-resolve to the new child - they never write to the retired old-K.
	cur := c.genSnapshot().clone()
	cur.cutOver[k] = struct{}{}
	c.commitGenState(cur)
	pause.Unlock()

	// Retire old-K: its key-space has cut over, so release the lease + free
	// the gen-g database. CloseUnit at the OLD generation does not touch the
	// gen-(g+1) children (distinct identities).
	c.mountMu.Lock()
	delete(c.mountMap, oldGU)
	c.mountMu.Unlock()
	_ = c.factory.CloseUnit(oldGU)
	return nil
}

// copyUnitInto scans every key in src (one old gen-g unit) and writes each
// into the correct gen-(g+1) child by the doubling hash bit. For each key:
// compute its old unit's low child (ChildUnits) and the child its hash lands
// on at 2N (UnitForHash); if they match the key goes to the LOW backend, else
// the HIGH backend. Idempotent re-writes (the catch-up pass re-copies keys the
// first pass already moved) are harmless: a Put of the same (key, value) is a
// no-op overwrite. Reads from src are always valid (old-K keeps serving); the
// children are the freshly-opened destinations.
func (c *Cluster) copyUnitInto(src, lowBE, highBE backend.Backend, oldCount, newCount storageunit.UnitCount) error {
	it, err := src.ScanPrefix(nil)
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for {
		key, value, err := it.Next()
		if err != nil {
			return err
		}
		if key == nil {
			return nil
		}
		// Route the key by its shard hash through the new bit. The child is one
		// of the old unit's two children; pick the destination backend by which
		// child UnitForHash(h, 2N) lands on.
		h := storageunit.HashShardKey(c.shardKey(key))
		child := storageunit.UnitForHash(h, newCount)
		oldK := storageunit.UnitForHash(h, oldCount)
		lowChild, _, cerr := storageunit.ChildUnits(oldK, oldCount)
		if cerr != nil {
			return cerr
		}
		dst := highBE
		if child == lowChild {
			dst = lowBE
		}
		if err := dst.Put(key, value); err != nil {
			return err
		}
	}
}

// clearBackend deletes every key in b. Used by the catch-up drain to reset a
// child before the authoritative re-copy from the frozen old unit, so a key
// deleted in the old unit after the bulk copy does not survive in the child.
// It materializes the key list first (do not delete while iterating the same
// backend's open iterator). Bounded by the child's current size.
func clearBackend(b backend.Backend) error {
	it, err := b.ScanPrefix(nil)
	if err != nil {
		return err
	}
	var keys [][]byte
	for {
		k, _, err := it.Next()
		if err != nil {
			_ = it.Close()
			return err
		}
		if k == nil {
			break
		}
		kc := make([]byte, len(k))
		copy(kc, k)
		keys = append(keys, kc)
	}
	_ = it.Close()
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}
