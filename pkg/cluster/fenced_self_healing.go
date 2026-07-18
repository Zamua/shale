package cluster

import (
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// fencedSelfHealing decorates a mounted backend so that a FENCED owner-local op
// self-heals instead of returning the raw fence forever. A higher-epoch owner
// that superseded this mount fences it (production slatedb CLOSES the stale
// handle, so EVERY op - read or write - errors with backend.ErrFenced); the
// decorator recodes that fence to the transient acquiring-window error AND evicts
// this stale mount, so the reconcile re-acquires the position fresh.
//
// THE POINT of the decorator is to make the fence-recode-evict an INTRINSIC
// property of the mounted handle - applied once, at the mount seam (storeMount) -
// rather than a step every owner-local op site must remember to call
// (fenceToTransient). Forgetting it is exactly what wedged the routed GET path
// forever (#433): a fenced GET returned the raw fence, and the mount - still in
// mountMap - was never re-acquired, so reconcile treated it as healthy. With the
// decorator a NEW owner-local op path cannot reintroduce that bug: it just calls
// a method on a handle that already heals.
//
// POINT ops (Get/Put/Delete) self-heal here. ScanPrefix + Begin DELEGATE to the
// inner backend: the streaming scan (mountedIterator) and the transactional CAS
// (CommitCASApply) carry their own mid-stream / mid-transaction fence handling,
// and because the value they resolve from mountMap is THIS decorator, their
// fenceToTransient evicts the right mount. fenceToTransient -> evictStaleMount
// keeps its Draining-mount guard, so a fenced op on a mount mid-handoff recodes
// to transient WITHOUT eviction - the overlap handoff is unaffected.
type fencedSelfHealing struct {
	c     *Cluster
	ru    storageunit.ReplicaUnit
	inner backend.Backend
}

// newFencedSelfHealing wraps inner, idempotently (an already-wrapped or nil
// backend is returned unchanged) so a re-store or double-wrap never nests.
func (c *Cluster) newFencedSelfHealing(ru storageunit.ReplicaUnit, inner backend.Backend) backend.Backend {
	if inner == nil {
		return nil
	}
	if _, already := inner.(*fencedSelfHealing); already {
		return inner
	}
	return &fencedSelfHealing{c: c, ru: ru, inner: inner}
}

// storeMount records ru -> inner in the mount map, wrapping inner in the
// fence-self-healing decorator. It is the SINGLE choke point every mount site
// goes through, so the decorator is applied uniformly and no mount site can omit
// it. Caller MUST hold mountMu (mountMap is mountMu-guarded), exactly as the raw
// `c.mountMap[ru] = b` assignments it replaces required.
func (c *Cluster) storeMount(ru storageunit.ReplicaUnit, inner backend.Backend) {
	// A successful mount clears any recorded acquire error BY CONSTRUCTION:
	// doing it at this single choke point means no mount site (boot, reconcile
	// acquire, overlap handoff, reshard split, or a future one) can leave a
	// stale record behind for FailedOpenUnits/LastAcquireError to resurface
	// once the position later unmounts. The per-site Deletes at the acquire
	// paths remain as harmless belt and suspenders.
	c.lastAcquireErr.Delete(ru)
	c.mountMap[ru] = c.newFencedSelfHealing(ru, inner)
}

func (f *fencedSelfHealing) Get(key []byte) ([]byte, error) {
	v, err := f.inner.Get(key)
	if err != nil {
		return nil, f.c.fenceToTransient(f.ru, f, "Get", err)
	}
	return v, nil
}

func (f *fencedSelfHealing) Put(key, value []byte) error {
	if err := f.inner.Put(key, value); err != nil {
		return f.c.fenceToTransient(f.ru, f, "Put", err)
	}
	return nil
}

func (f *fencedSelfHealing) Delete(key []byte) error {
	if err := f.inner.Delete(key); err != nil {
		return f.c.fenceToTransient(f.ru, f, "Delete", err)
	}
	return nil
}

// ScanPrefix delegates: the scan path (mountedIterator) recodes + evicts on a
// mid-stream fence, resolving THIS decorator from mountMap.
func (f *fencedSelfHealing) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	return f.inner.ScanPrefix(prefix)
}

// Begin delegates: the transactional CAS path (CommitCASApply) recodes + evicts
// on a mid-transaction fence at every owner-local op site, resolving THIS
// decorator from mountMap.
func (f *fencedSelfHealing) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	return f.inner.Begin(level)
}

func (f *fencedSelfHealing) Close() error { return f.inner.Close() }
