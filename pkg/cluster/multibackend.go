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
// cut-over units' gen-(g+1) children that this node owns. This is the
// generation-aware analogue of storageunit.OwnedUnits; the Phase 3 reconcile
// diffs it against the factory's OpenUnits() to decide acquire / release.
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

	// Generation propagation (join-after-reshard fix): a JOINER (multi-backend
	// Open WITH seeds) must learn the cluster's LIVE {generation, unit-count}
	// BEFORE it derives or mounts any unit below, or it routes / owns keys at
	// gen 0 and orphans acked writes after the cluster has resharded. Query a
	// seed and commit the live generation here, ahead of the mount loop, so
	// there is NO window in which this node serves at gen 0. The founder /
	// single-node / legacy paths have no seeds and keep initGenState's gen-0
	// default. Fail closed: a seed that cannot be reached fails Open rather
	// than leaving the joiner at the wrong generation.
	if len(c.cfg.Seeds) > 0 {
		if err := c.learnGenerationFromSeed(); err != nil {
			return err
		}
	}

	c.mountMap = make(map[storageunit.ReplicaUnit]backend.Backend)

	// R>1 (replicated multi-backend, v0.8 Phase 2b): mount each owned unit at
	// its replica POSITION (an independent durable database) via the per-replica
	// factory, then return. The R=1 single-mount loop below is bypassed; the
	// replicated write/read paths key off the unit's replica set instead of a
	// single owner. STATIC topology: the replica set is fixed at Open (no
	// membership-change handoff at R>1 yet - a later phase).
	c.initReplicatedFactory()
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
		c.mountMap[replica0(gu)] = b
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

// mountedBackends returns a snapshot of every backend this node currently
// has mounted, in ascending (generation, unit) order. Used by the LOCAL admin
// scan (LocalScanPrefix with the unit-spanning behavior keysHeld + Aggregate
// need) so a multi-backend node reports the union of its units, not a single
// engine. During a reshard this transiently spans BOTH the old gen-g unit and
// its gen-(g+1) children (until the old unit is retired); the admin scan
// counting a key in both is acceptable for the non-routed keysHeld view.
// Multi-backend mode only.
func (c *Cluster) mountedBackends() []backend.Backend {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	ids := make([]storageunit.ReplicaUnit, 0, len(c.mountMap))
	for ru := range c.mountMap {
		ids = append(ids, ru)
	}
	sortReplicaUnits(ids)
	out := make([]backend.Backend, 0, len(ids))
	for _, ru := range ids {
		out = append(out, c.mountMap[ru])
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
	return &mountedIterator{backends: c.mountedBackends(), prefix: prefix}, nil
}

// mountedIterator chains the per-unit iterators of a multi-backend node
// into one backend.Iterator. It opens each unit's iterator lazily as the
// prior one exhausts, so at most one unit iterator is live at a time.
type mountedIterator struct {
	backends []backend.Backend
	prefix   []byte
	idx      int
	cur      backend.Iterator
}

func (it *mountedIterator) Next() (key, value []byte, err error) {
	for {
		if it.cur == nil {
			if it.idx >= len(it.backends) {
				return nil, nil, nil
			}
			cur, err := it.backends[it.idx].ScanPrefix(it.prefix)
			if err != nil {
				return nil, nil, err
			}
			it.cur = cur
			it.idx++
		}
		k, v, err := it.cur.Next()
		if err != nil {
			_ = it.cur.Close()
			it.cur = nil
			return nil, nil, err
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
	for _, b := range c.mountedBackends() {
		it, err := b.ScanPrefix(nil)
		if err != nil {
			return nil, err
		}
		for {
			k, v, err := it.Next()
			if err != nil {
				_ = it.Close()
				return nil, err
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
