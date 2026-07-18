// Multi-backend node (v0.8): mount-map plumbing + per-unit routing.
//
// This file holds the multi-backend MODE of the cluster: instead of one
// Config.Backend per node, the node mounts a SET of per-unit backends and
// routes each key to the unit that owns it (key -> shardKey -> unit ->
// owner). It is additive + mutually exclusive with the legacy per-node
// path: a cluster runs in exactly one mode, chosen at Open (validated by
// the factory+unitcount XOR backend check). When this mode is off
// (c.multi == false) NOTHING here runs and the legacy path is byte-for-
// byte unchanged.
//
// Initial mount happens at Open (initMultiBackend, below). Membership
// changes mid-run are acted on by the Phase 3 lease-handoff RECONCILE in
// multibackend_rebalance.go: a unit whose owner moved is released by the
// old owner (CloseUnit, flush) and acquired by the new owner (OpenUnit at a
// strictly higher epoch, fencing the old) - copy-free, the bytes stay in
// shared object storage. That reconcile is wired through bumpRingGen ->
// scheduleReconcile (multi branch). THIS file is the mount-map plumbing +
// routing; the handoff / fencing / reconcile logic lives next door in
// multibackend_rebalance.go.

package cluster

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// notReady reports whether the cluster cannot serve an op: it is closed,
// or (legacy mode) has no backend. In multi-backend mode c.backend is
// intentionally nil, so the legacy "c.backend == nil" guard must NOT be
// used directly on the KV path; this predicate replaces it everywhere a
// KV method gates entry. A multi-backend cluster is "ready" once Open
// returns regardless of how many units it mounted (a node owning zero
// units is still a valid member that forwards every op).
func (c *Cluster) notReady() bool {
	if c.closed.Load() {
		return true
	}
	if c.multi {
		return false
	}
	return c.backend == nil
}

// epochAtOpen is the epoch a unit is opened at during the INITIAL mount in
// initMultiBackend (cold start). 0 is the base: there is no prior owner to
// fence at first start, the durable manifest (if any) governs, and the
// factory fences against it regardless. The Phase 3 lease HANDOFF acquires
// at a strictly higher epoch instead (acquireUnit / nextEpochFor in
// multibackend_rebalance.go), which is the path that fences a prior owner.
const epochAtOpen storageunit.Epoch = 0

// defaultOpenConcurrency is the boot-mount worker-pool size when
// Config.OpenConcurrency is unset. It defaults to 1 = SEQUENTIAL, which is the
// only mount mode proven safe in production.
//
// CONCURRENCY IS OPT-IN AND CURRENTLY KNOWN-UNSAFE with the slatedb-go binding we
// ship (v0.13.1). Opening MULTIPLE units CONCURRENTLY, when those units have REAL
// durable data to read (WAL replay / SST reads), corrupts the reads and slatedb
// reports "empty SSTable" - INDEPENDENT of object-store CPU (it reproduced on
// prod against an UNCAPPED minio with zero throttling, 2026-06-19). The earlier
// "store CPU saturation" theory was a correlation, not the cause: the concurrent
// FFI opens themselves are the trigger. Empty/fresh units do NOT reproduce it
// (no real objects to read), which is why a fresh-data staging cluster passed
// twice while prod failed.
//
// The bounded-concurrency worker pool (Phase 2g) and the per-open transient
// retry remain in the code, gated behind Config.OpenConcurrency > 1, for if/when
// the slatedb-go FFI is made concurrency-safe - but a value > 1 MUST be
// re-validated against PROD-SHAPED DATA before use. Until then the default is 1.
const defaultOpenConcurrency = 1

// openConcurrency returns the node-wide bound on concurrent replica-unit
// opens: Config.OpenConcurrency normalized (zero/negative -> the safe
// defaultOpenConcurrency). BOTH open paths consult it - the boot mount pool
// (mountReplicaUnits) and the background overlap acquires (via
// acquireOpenPermit) - so every real-data FFI open on a node respects ONE
// knob, and raising it is one deliberate, revalidated act (see the
// defaultOpenConcurrency doc for why the default stays 1).
func (c *Cluster) openConcurrency() int {
	conc := c.cfg.OpenConcurrency
	if conc <= 0 {
		conc = defaultOpenConcurrency
	}
	return conc
}

// defaultOpenPermitTimeout is the permit watchdog bound when
// Config.OpenPermitTimeout is unset: how long one overlap acquire may hold
// the node-wide open permit before the permit is released to unstarve the
// queue (the open itself keeps running). Roughly 2x a worst-case clean open
// against a loaded object store.
const defaultOpenPermitTimeout = 60 * time.Second

// openPermitTimeout returns the normalized permit watchdog bound.
func (c *Cluster) openPermitTimeout() time.Duration {
	if d := c.cfg.OpenPermitTimeout; d > 0 {
		return d
	}
	return defaultOpenPermitTimeout
}

// acquireOpenPermit blocks until a node-wide open permit is available and
// returns the release func. The permit gate (openSem) is created lazily
// under mountMu on first use, sized by openConcurrency() at that moment.
// Used by the background overlap acquires; the boot mount pool carries the
// same bound via its own errgroup limit (boot and reconcile do not overlap:
// mountReplicaUnits completes before Open returns and starts the loops).
func (c *Cluster) acquireOpenPermit() (release func()) {
	c.mountMu.Lock()
	if c.openSem == nil {
		c.openSem = make(chan struct{}, c.openConcurrency())
	}
	sem := c.openSem
	c.mountMu.Unlock()
	sem <- struct{}{}
	return func() { <-sem }
}

// genUnitBytes encodes a GenUnit (the generation-qualified storage identity)
// as 12 fixed-width big-endian bytes: 8 bytes of Generation followed by 4
// bytes of UnitID. This is the stable encoding fed to the ring (LocateKey
// hashes it whole, since a generation-qualified id carries no "{...}" hash
// tag) so a unit's owner is the same on every node. The generation is part
// of the hash input, so the gen-g and gen-(g+1) ids of the SAME UnitID hash
// to potentially DIFFERENT ring positions (hence potentially different
// owners) - which is what lets a doubling reshard land the new generation's
// units wherever the ring places them. The encoding MUST be identical
// everywhere a unit owner is computed; centralizing it here is what
// guarantees that.
func genUnitBytes(gu storageunit.GenUnit) []byte {
	var b [12]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(gu.Gen))
	binary.BigEndian.PutUint32(b[8:12], uint32(gu.ID))
	return b[:]
}

// genUnitOwner returns the node that owns the generation-qualified unit gu,
// hashing genUnitBytes(gu) through the SAME ring the cluster routes keys with
// (no second ring), so unit ownership and key routing agree by construction.
// With an empty / nil ring (single-node multi-backend, before any peer is
// known) it returns the local node (ok=true): with no ring to place units on,
// this node owns them all.
func (c *Cluster) genUnitOwner(gu storageunit.GenUnit) (storageunit.NodeID, bool) {
	if c.ring == nil || c.ring.Empty() {
		return storageunit.NodeID(c.cfg.NodeID), true
	}
	m := c.ring.LocateKey(genUnitBytes(gu))
	if m.ID == "" {
		return "", false
	}
	return storageunit.NodeID(m.ID), true
}

// desiredGenUnits returns the generation-qualified units this node SHOULD
// have mounted right now: for the cluster's CURRENT generation + count, every
// unit whose GenUnit the ring assigns to self. Mid-reshard a unit that has
// already cut over is resolved at the NEW generation (its gen-(g+1) children),
// so the desired set is the union of the not-yet-cut-over gen-g units and the
// cut-over units' gen-(g+1) children that this node owns. It is the
// generation-aware, ring-backed derivation of the desired mount set (at R=1;
// desiredReplicaUnits is its R>1 sibling); the Phase 3 reconcile diffs it
// against the factory's OpenUnits() to decide acquire / release.
func (c *Cluster) desiredGenUnits() []storageunit.GenUnit {
	gs := c.genSnapshot()
	want := make(map[storageunit.GenUnit]struct{})
	self := storageunit.NodeID(c.cfg.NodeID)
	ownerOf := c.genOwner // ring-backed by default; test-overridable

	// Old-generation units that have NOT cut over are still live at gen g.
	for _, u := range gs.count.IDs() {
		if gs.hasCutOver(u) {
			// This old unit has been retired; its key-space now lives in the
			// gen-(g+1) children, handled by the new-count pass below.
			continue
		}
		gu := storageunit.NewGenUnit(gs.gen, u)
		if owner, ok := ownerOf(gu); ok && owner == self {
			want[gu] = struct{}{}
		}
	}

	// Cut-over units contribute their two gen-(g+1) children. nextCount is the
	// doubled count; with no reshard in flight the cut-over set is empty and
	// this loop adds nothing (steady state is the old-gen pass only).
	if !gs.nextCount.IsZero() {
		for u := range gs.cutOver {
			low, high, err := storageunit.ChildUnits(u, gs.count)
			if err != nil {
				continue
			}
			for _, child := range []storageunit.UnitID{low, high} {
				gu := storageunit.NewGenUnit(gs.gen+1, child)
				if owner, ok := ownerOf(gu); ok && owner == self {
					want[gu] = struct{}{}
				}
			}
		}
	}

	out := make([]storageunit.GenUnit, 0, len(want))
	for gu := range want {
		out = append(out, gu)
	}
	sortGenUnits(out)
	return out
}

// sortGenUnits sorts in place ascending by (Generation, UnitID). A small
// insertion sort keeps the dependency surface minimal (the sets are tiny:
// at most N entries).
func sortGenUnits(g []storageunit.GenUnit) {
	for i := 1; i < len(g); i++ {
		for j := i; j > 0 && genUnitLess(g[j], g[j-1]); j-- {
			g[j-1], g[j] = g[j], g[j-1]
		}
	}
}

// genUnitLess orders by Generation then UnitID.
func genUnitLess(a, b storageunit.GenUnit) bool {
	if a.Gen != b.Gen {
		return a.Gen < b.Gen
	}
	return a.ID < b.ID
}

// replicaUnitLess orders by (Generation, UnitID, replica position), extending
// genUnitLess so the admin scan reports mounted positions in a deterministic
// (gen, unit, position) order. Phase 2e re-keying.
func replicaUnitLess(a, b storageunit.ReplicaUnit) bool {
	if a.Unit != b.Unit {
		return genUnitLess(a.Unit, b.Unit)
	}
	return a.Replica < b.Replica
}

// sortReplicaUnits orders a slice of ReplicaUnits in place by replicaUnitLess.
func sortReplicaUnits(r []storageunit.ReplicaUnit) {
	for i := 1; i < len(r); i++ {
		for j := i; j > 0 && replicaUnitLess(r[j], r[j-1]); j-- {
			r[j-1], r[j] = r[j], r[j-1]
		}
	}
}

// initMultiBackend wires up multi-backend mode at Open: seed the
// generation-aware routing state (generation 0, count N), derive the
// generation-qualified units this node owns, and mount each via the factory
// into the mount map. Called only when c.multi is true. On any OpenUnit error
// it rolls back (closes everything it already opened) so Open fails cleanly
// with nothing half-mounted.
func (c *Cluster) initMultiBackend() error {
	if c.genOwner == nil {
		c.genOwner = c.genUnitOwner
	}
	c.initGenState()

	// Bootstrap the generation BEFORE deriving or mounting any unit below, or
	// this node routes / owns keys at gen 0 and orphans acked writes after the
	// cluster has resharded.
	//
	// When a shared ConditionalStore is configured, the __cluster/init marker is
	// the authority (homogeneous runtime try-join-else-form; see
	// multibackend_bootstrap_marker.go): every node runs the SAME path, the
	// single PutIfAbsent winner founds at gen 0, and everyone else adopts the
	// marker's durable {gen, count}. This is what lets a restart REJOIN at the
	// live generation instead of re-forming gen 0, and a full-cluster restart
	// resume the live generation with no live peer to ask.
	//
	// Without a ConditionalStore, fall back to the legacy config-driven path: a
	// JOINER (Open WITH seeds) learns the live generation from a seed RPC, while
	// the founder / single-node paths keep initGenState's gen-0 default. Fail
	// closed in both: a marker read / seed query that cannot complete fails Open
	// rather than leaving this node at the wrong generation.
	if c.cfg.ConditionalStore != nil {
		gen, count, founded, err := c.bootstrapViaMarker()
		if err != nil {
			return err
		}
		if !founded {
			// Adopt the marker's durable {gen, count}. (A founder wrote
			// {gen:0, count:N}; initGenState already seeded that, nothing to do.)
			c.commitGenState(genState{
				gen:     gen,
				count:   count,
				cutOver: make(map[storageunit.UnitID]struct{}),
			})
		}
	} else if len(c.cfg.Seeds) > 0 {
		if err := c.learnGenerationFromSeed(); err != nil {
			return err
		}
	}

	c.mountMap = make(map[storageunit.ReplicaUnit]backend.Backend)
	// handoffPhase (v0.8 Phase 2e, pending ranges) tracks in-flight handoff
	// transitions per ReplicaUnit, guarded by mountMu alongside mountMap. Empty at
	// Open (no transition in flight); populated by reconcileReplicaUnitsOverlap
	// when a draining-excluded split moves a position (Acquiring on the pending
	// owner, Draining on the leaver).
	c.handoffPhase = make(map[storageunit.ReplicaUnit]storageunit.HandoffState)
	c.acquireInFlight = make(map[storageunit.ReplicaUnit]struct{})

	// R>1 (replicated multi-backend, v0.8 Phase 2b): mount each owned unit at
	// its replica POSITION (an independent durable database) via the per-replica
	// factory, then return. The R=1 single-mount loop below is bypassed; the
	// replicated write/read paths key off the unit's replica set instead of a
	// single owner. STATIC topology: the replica set is fixed at Open (no
	// membership-change handoff at R>1 yet - a later phase).
	// Debug/repro hook: simulate a slow cold-start unit mount (a loaded object
	// store). Because the caller starts this node's gRPC server only AFTER Open
	// returns, delaying the mount also delays when this node SERVES gRPC - so a
	// founder with this set reproduces the window where a joiner's GenState dial
	// times out, exercising the patient learnGenerationFromSeed. No-op unless set.
	if d := c.cfg.TestingMountDelay; d > 0 {
		time.Sleep(d)
	}
	c.initReplicatedFactory()
	// Decentralized reshard agreement (v0.9): construct + seed the Arbiter when
	// opted in (ConditionalStore set) on an R>1 cluster. No-op otherwise. Done
	// after initReplicatedFactory (replicaFactory gates it) and before the mount
	// so the agreed epoch object exists from node start.
	if err := c.initReshardArbiter(); err != nil {
		_ = c.closeMountedUnits()
		return err
	}
	if c.replicaFactory != nil {
		return c.mountReplicaUnits()
	}

	for _, gu := range c.desiredGenUnits() {
		// Cold-start mount at the base epoch. The factory fences against
		// the unit's durable manifest if one already exists. A later
		// membership change drives the Phase 3 handoff (acquireUnit) which
		// opens at a strictly higher epoch to fence a prior owner.
		b, err := c.factory.OpenUnit(gu, epochAtOpen)
		if err != nil {
			// Rollback is best-effort: we are already failing Open, so a
			// secondary close error does not change the outcome. The
			// primary OpenUnit error is what the caller acts on.
			_ = c.closeMountedUnits()
			return fmt.Errorf("cluster: open unit %s: %w", gu, err)
		}
		c.storeMount(replica0(gu), b)
	}
	return nil
}

// replica0 is the ReplicaUnit at position 0 for a GenUnit. The R=1 single-mount
// paths (legacy multi-backend Phase 2/3, reshard Phase 4) hold each unit at one
// position (0), so they key the ReplicaUnit-keyed mountMap via replica0(gu). The
// R>1 replicated paths key by the unit's ACTUAL replica position instead. Phase
// 2e re-keying helper.
func replica0(gu storageunit.GenUnit) storageunit.ReplicaUnit {
	return storageunit.NewReplicaUnit(gu, 0)
}

// localReplicaPos returns the replica POSITION this node holds key's unit gu at,
// for resolving the ReplicaUnit-keyed mountMap on a normal ring-routed local op.
// At R>1 it is this node's index in the unit's live replica set (unitReplicas);
// the node appears at most once, so the position is unique. At R=1 (and in the
// legacy / reshard paths) there is one position (0). ok is false at R>1 when this
// node is not in the unit's replica set (it should not be serving the key
// locally), in which case the caller treats the unit as unmounted.
func (c *Cluster) localReplicaPos(gu storageunit.GenUnit) (pos uint8, ok bool) {
	if !c.multiReplicated() {
		return 0, true
	}
	for i, m := range c.unitReplicas(gu) {
		if m.ID == c.cfg.NodeID {
			return uint8(i), true
		}
	}
	return 0, false
}

// closeMountedUnits releases every unit this node has mounted, via the
// factory's CloseUnit. Called from Close (and from initMultiBackend's
// rollback path). Best-effort: it attempts every unit and returns the first
// error so a single stubborn unit does not strand the rest. After this the
// mount map is cleared.
func (c *Cluster) closeMountedUnits() error {
	c.mountMu.Lock()
	defer c.mountMu.Unlock()
	var firstErr error
	for ru := range c.mountMap {
		// At R>1 each unit is an independent durable database mounted at a
		// replica POSITION, so release the right replica copy via the per-
		// replica factory; at R=1 (and legacy) release the GenUnit-keyed unit
		// (ru.Replica is 0 there).
		if c.replicaFactory != nil {
			if err := c.replicaFactory.CloseReplicaUnit(ru); err != nil && firstErr == nil {
				firstErr = err
			}
		} else if err := c.factory.CloseUnit(ru.Unit); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.mountMap, ru)
	}
	return firstErr
}

// unitOwnerOf returns the ring member that owns key's generation-qualified
// unit + whether the local node is that owner. Multi-backend analogue of
// ownerOf: the ownership question goes through the unit (resolved
// generation-aware), not the raw key. With an empty / nil ring the local node
// owns everything (single-node multi-backend).
func (c *Cluster) unitOwnerOf(key []byte) (owner ring.Member, isLocal bool) {
	gu := c.genUnitForKey(key)
	if c.ring == nil || c.ring.Empty() {
		self := ring.Member{ID: c.cfg.NodeID, Addr: c.cfg.GRPCAddr}
		return self, true
	}
	owner = c.ring.LocateKey(genUnitBytes(gu))
	return owner, owner.ID == c.cfg.NodeID
}

// localBackendForKey resolves the mounted backend a LOCAL operation on key
// must apply against. ok is false when this node does not have key's
// (generation-qualified) unit mounted - either it never owned the unit, the
// lease moved (Phase 3), or a reshard is mid-flight for the unit (Phase 4).
// Callers that have already established local ownership (the forwarding
// guard) treat ok=false as "this node does not own the unit": refuse, do
// NOT re-forward. Multi-backend mode only.
func (c *Cluster) localBackendForKey(key []byte) (backend.Backend, bool) {
	gu := c.genUnitForKey(key)
	pos, ok := c.localReplicaPos(gu)
	if !ok {
		return nil, false
	}
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	b, ok := c.mountMap[storageunit.NewReplicaUnit(gu, pos)]
	return b, ok
}

// localBackendForReplicaUnit resolves the mounted backend for an EXPLICIT
// ReplicaUnit, bypassing the live-ring-index resolver (localReplicaPos). It is
// the POSITION-ADDRESSED path the overlap-handoff predecessor's forwarded-op
// handler uses (v0.8 Phase 2e): the moving position is no longer this node's
// own ring index (the ring rotated it away), so localBackendForKey would not
// find it - but the position is still mounted in a Draining handoffPhase entry,
// reachable only by its explicit ru. ok=false means this node does not have ru
// mounted (it already released, or never held it), in which case the caller
// refuses with the loop-guard rather than re-forwarding. Multi-backend mode only.
func (c *Cluster) localBackendForReplicaUnit(ru storageunit.ReplicaUnit) (backend.Backend, bool) {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	b, ok := c.mountMap[ru]
	return b, ok
}

// localReadBackendForReplicaUnit resolves the mounted backend a READ leg
// addressed at ru serves from: mountMap[ru] when the exact position is
// mounted, FALLING BACK to the LOWEST mounted position of the SAME unit when
// it is not (returning the ru actually resolved, so a fence recode evicts the
// right mount). This is the READ sibling of localWriteBackendForKey's
// transition fallback (docs/SPEC.md "Union reads", mounted-position
// fallback): a ring change can shuffle a member's index within a unit's
// replica set, so during the post-change reconcile window the member
// physically holds the unit's bytes at its OLD position while the routed set
// addresses it at the NEW one. A replica copy at another index of the same
// unit is just another replica of the same data, so serving a READ from it is
// the union model working as intended ("whoever physically holds the unit
// serves"). READ-ONLY by design: write legs never fall back (a write applied
// only to a soon-released old-index copy could be lost); steady state never
// reaches the fallback (the exact resolve hits). ok=false means this node has
// NO position of ru's unit mounted (a mid-acquire pending owner): the caller
// returns the transient acquiring error and the union covers the read.
func (c *Cluster) localReadBackendForReplicaUnit(ru storageunit.ReplicaUnit) (backend.Backend, storageunit.ReplicaUnit, bool) {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	if b, ok := c.mountMap[ru]; ok {
		return b, ru, true
	}
	var (
		be    backend.Backend
		got   storageunit.ReplicaUnit
		found bool
	)
	for cand, b := range c.mountMap {
		if cand.Unit != ru.Unit {
			continue
		}
		if !found || cand.Replica < got.Replica {
			got, be, found = cand, b, true
		}
	}
	return be, got, found
}

// localMountedBackendForKey resolves the key's unit against this node's PHYSICAL
// mountMap (ANY replica index), NOT the live-ring index. It backs LocalGet (the
// "we physically hold this key even though the ring moved it off us" read
// forwarder). The ring-index resolver (localBackendForKey) disclaims a position
// this node is no longer the ring owner of - which is exactly a DRAINING node:
// it is excluded from the ownership ring (graceful scale-down) yet still holds
// the unit MOUNTED while it hands off. Scanning the mountMap by GenUnit lets the
// draining node keep serving routed reads of the data it physically holds during
// the drain window, instead of refusing them via the loop-guard. Returns the
// first mounted backend whose ReplicaUnit is for the key's GenUnit.
func (c *Cluster) localMountedBackendForKey(key []byte) (backend.Backend, bool) {
	gu := c.genUnitForKey(key)
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	for ru, b := range c.mountMap {
		if ru.Unit == gu {
			return b, true
		}
	}
	return nil, false
}

// mountedBackends returns a snapshot of every backend this node currently
// has mounted, in ascending (generation, unit) order. Used by the LOCAL admin
// scan (LocalScanPrefix with the unit-spanning behavior keysHeld + Aggregate
// need) so a multi-backend node reports the union of its units, not a single
// engine. During a reshard this transiently spans BOTH the old gen-g unit and
// its gen-(g+1) children (until the old unit is retired); the admin scan
// counting a key in both is acceptable for the non-routed keysHeld view.
// Multi-backend mode only.
// mountedUnit pairs a mounted ReplicaUnit with its live backend handle. The
// scan paths carry the ru alongside the backend so a scan that hits a FENCED
// handle can evict the RIGHT mount (the cross-shard scan fence self-heal needs
// the ru, which a bare []backend.Backend loses).
type mountedUnit struct {
	ru storageunit.ReplicaUnit
	b  backend.Backend
}

func (c *Cluster) mountedUnits() []mountedUnit {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	ids := make([]storageunit.ReplicaUnit, 0, len(c.mountMap))
	for ru := range c.mountMap {
		ids = append(ids, ru)
	}
	sortReplicaUnits(ids)
	out := make([]mountedUnit, 0, len(ids))
	for _, ru := range ids {
		out = append(out, mountedUnit{ru: ru, b: c.mountMap[ru]})
	}
	return out
}

// localScanMounted returns an iterator over the union of every mounted
// unit's keys with the given prefix. It is the multi-backend form of the
// local admin scan (LocalScanPrefix): keysHeld counts the node's whole
// physical keyspace, and a node holds keys across many units. The units
// are scanned in ascending unit order and their iterators chained; each
// open iterator is closed when its turn ends (or on the chain's Close).
// Multi-backend mode only.
func (c *Cluster) localScanMounted(prefix []byte) (backend.Iterator, error) {
	return &mountedIterator{c: c, units: c.mountedUnits(), prefix: prefix}, nil
}

// mountedIterator chains the per-unit iterators of a multi-backend node
// into one backend.Iterator. It opens each unit's iterator lazily as the
// prior one exhausts, so at most one unit iterator is live at a time.
//
// A FENCED mounted handle (a higher-epoch owner superseded this node's mount
// during a membership change) makes the underlying ScanPrefix / Next fail with
// backend.ErrFenced in production. The iterator recodes that exactly as the
// write path does (fenceToTransient: EVICT the stale mount + surface the
// retryable acquiring-window error) so the cross-shard scan that drives this
// (Aggregate / LocalScan) re-runs and heals once the reconcile re-acquires,
// instead of failing the whole aggregate with the raw fence. A non-fence error
// passes through unchanged. Carries (c, ru) per unit so the eviction hits the
// right mount. The caller MUST stop on a non-nil Next error (every in-repo
// caller does: the LocalScan handler + localMountedSnapshot both return on the
// first error); the iterator does not resume cleanly past a fence (the fenced
// unit has already been consumed from its work list).
type mountedIterator struct {
	c      *Cluster
	units  []mountedUnit
	prefix []byte
	idx    int
	cur    backend.Iterator
	curRU  storageunit.ReplicaUnit // the unit `cur` belongs to (for fence eviction)
	curB   backend.Backend
}

func (it *mountedIterator) Next() (key, value []byte, err error) {
	for {
		if it.cur == nil {
			if it.idx >= len(it.units) {
				return nil, nil, nil
			}
			mu := it.units[it.idx]
			it.idx++
			cur, err := mu.b.ScanPrefix(it.prefix)
			if err != nil {
				return nil, nil, it.c.fenceToTransient(mu.ru, mu.b, "LocalScan", err)
			}
			it.cur = cur
			it.curRU = mu.ru
			it.curB = mu.b
		}
		k, v, err := it.cur.Next()
		if err != nil {
			_ = it.cur.Close()
			it.cur = nil
			return nil, nil, it.c.fenceToTransient(it.curRU, it.curB, "LocalScan", err)
		}
		if k == nil {
			// This unit exhausted; move to the next.
			_ = it.cur.Close()
			it.cur = nil
			continue
		}
		return k, v, nil
	}
}

func (it *mountedIterator) Close() error {
	if it.cur != nil {
		err := it.cur.Close()
		it.cur = nil
		return err
	}
	return nil
}

// localMountedSnapshot materializes a read-only in-process view spanning
// every mounted unit, for Aggregate's local fn invocation (multi-backend
// mode has no single c.backend to hand fn). It is the LOCAL analogue of
// snapshotPeer: load every mounted unit's keys into a transient
// snapshotBackend. Multi-backend mode only.
func (c *Cluster) localMountedSnapshot() (backend.Backend, error) {
	snap := newSnapshotBackend()
	for _, mu := range c.mountedUnits() {
		it, err := mu.b.ScanPrefix(nil)
		if err != nil {
			// A FENCED mount (superseded by a higher-epoch owner mid-convergence)
			// is recoded EXACTLY as the write path does: evict the stale mount +
			// surface the retryable acquiring-window error, so Aggregate's caller
			// re-runs the scan and heals once the reconcile re-acquires, instead of
			// failing the aggregate with the raw fence. Non-fence errors pass through.
			return nil, c.fenceToTransient(mu.ru, mu.b, "aggregate scan", err)
		}
		for {
			k, v, err := it.Next()
			if err != nil {
				_ = it.Close()
				return nil, c.fenceToTransient(mu.ru, mu.b, "aggregate scan", err)
			}
			if k == nil {
				break
			}
			_ = snap.Put(k, v)
		}
		_ = it.Close()
	}
	return snap, nil
}
