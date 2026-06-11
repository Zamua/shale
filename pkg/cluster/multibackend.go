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

// unitIDBytes encodes a UnitID as 4 fixed-width big-endian bytes. This is
// the stable encoding fed to the ring (LocateKey hashes it whole, since a
// bare unit id carries no "{...}" hash tag) so a unit's owner is the same
// on every node. The encoding MUST be identical everywhere a unit owner is
// computed; centralizing it here is what guarantees that.
func unitIDBytes(u storageunit.UnitID) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(u))
	return b[:]
}

// ringOwnerLookup adapts the cluster's ring into a storageunit.OwnerLookup.
// OwnerOf(u) hashes unitIDBytes(u) through the SAME ring the cluster routes
// keys with (no second ring), so unit ownership and key routing agree by
// construction. An empty ring (single-node multi-backend, before any peer
// is known) reports the local node as owner of every unit: with no ring to
// place units on, this node owns them all.
type ringOwnerLookup struct {
	c *Cluster
}

// OwnerOf returns the node that owns unit u. With an empty / nil ring it
// returns the local node (ok=true) so a single-node multi-backend cluster
// mounts the whole unit space locally. Otherwise it locates the unit id on
// the ring.
func (l ringOwnerLookup) OwnerOf(u storageunit.UnitID) (storageunit.NodeID, bool) {
	if l.c.ring == nil || l.c.ring.Empty() {
		return storageunit.NodeID(l.c.cfg.NodeID), true
	}
	m := l.c.ring.LocateKey(unitIDBytes(u))
	if m.ID == "" {
		return "", false
	}
	return storageunit.NodeID(m.ID), true
}

// initMultiBackend wires up multi-backend mode at Open: build the owner
// lookup from the ring, derive the units this node owns, and mount each via
// the factory into the mount map. Called only when c.multi is true. On any
// OpenUnit error it rolls back (closes everything it already opened) so Open
// fails cleanly with nothing half-mounted.
func (c *Cluster) initMultiBackend() error {
	c.ownerLookup = ringOwnerLookup{c: c}
	c.mountMap = make(map[storageunit.UnitID]backend.Backend)

	owned := storageunit.OwnedUnits(storageunit.NodeID(c.cfg.NodeID), c.unitCount, c.ownerLookup)
	for _, u := range owned {
		// Cold-start mount at the base epoch. The factory fences against
		// the unit's durable manifest if one already exists. A later
		// membership change drives the Phase 3 handoff (acquireUnit) which
		// opens at a strictly higher epoch to fence a prior owner.
		b, err := c.factory.OpenUnit(u, epochAtOpen)
		if err != nil {
			// Rollback is best-effort: we are already failing Open, so a
			// secondary close error does not change the outcome. The
			// primary OpenUnit error is what the caller acts on.
			_ = c.closeMountedUnits()
			return fmt.Errorf("cluster: open unit %d: %w", u, err)
		}
		c.mountMap[u] = b
	}
	return nil
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
	for u := range c.mountMap {
		if err := c.factory.CloseUnit(u); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.mountMap, u)
	}
	return firstErr
}

// unitForKey computes the storage unit a key belongs to, using the SAME
// shard-key extraction the ring uses (so co-located keys share a unit and a
// transaction stays in one engine). Multi-backend mode only.
func (c *Cluster) unitForKey(key []byte) storageunit.UnitID {
	return storageunit.UnitForShardKey(c.shardKey(key), c.unitCount)
}

// unitOwnerOf returns the ring member that owns key's unit + whether the
// local node is that owner. Multi-backend analogue of ownerOf: the ownership
// question goes through the unit, not the raw key. With an empty / nil ring
// the local node owns everything (single-node multi-backend).
func (c *Cluster) unitOwnerOf(key []byte) (owner ring.Member, isLocal bool) {
	u := c.unitForKey(key)
	if c.ring == nil || c.ring.Empty() {
		self := ring.Member{ID: c.cfg.NodeID, Addr: c.cfg.GRPCAddr}
		return self, true
	}
	owner = c.ring.LocateKey(unitIDBytes(u))
	return owner, owner.ID == c.cfg.NodeID
}

// localBackendForKey resolves the mounted backend a LOCAL operation on key
// must apply against. ok is false when this node does not have key's unit
// mounted - either it never owned the unit, or (Phase 3) the lease moved.
// Callers that have already established local ownership (the forwarding
// guard) treat ok=false as "this node does not own the unit": refuse, do
// NOT re-forward. Multi-backend mode only.
func (c *Cluster) localBackendForKey(key []byte) (backend.Backend, bool) {
	u := c.unitForKey(key)
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	b, ok := c.mountMap[u]
	return b, ok
}

// mountedBackends returns a snapshot of every backend this node currently
// has mounted, in ascending unit order. Used by the LOCAL admin scan
// (LocalScanPrefix with the unit-spanning behavior keysHeld + Aggregate
// need) so a multi-backend node reports the union of its units, not a
// single engine. Multi-backend mode only.
func (c *Cluster) mountedBackends() []backend.Backend {
	c.mountMu.RLock()
	defer c.mountMu.RUnlock()
	ids := make([]storageunit.UnitID, 0, len(c.mountMap))
	for u := range c.mountMap {
		ids = append(ids, u)
	}
	// Ascending unit order for a stable, deterministic scan sequence.
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
	out := make([]backend.Backend, 0, len(ids))
	for _, u := range ids {
		out = append(out, c.mountMap[u])
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
