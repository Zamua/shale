// Package memfactory is an in-memory storageunit.BackendFactory for tests
// and local dev: it hands out one pkg/backend/memory.Memory per storage
// mount. A real (slatedb) factory mounts a durable database per mount; this
// one mounts an in-process map per mount so the multi-backend cluster paths
// can be exercised without object storage.
//
// It lives under internal/ so it is reusable across the core module's test
// trees (pkg/cluster tests + tests/integration) without widening the
// public API. It is NOT a shipped backend; the slatedb per-unit factory is
// the production implementation.
//
// SCOPE: this factory is PER-PROCESS. Its durable state, including the epochs
// and the serving markers, is held in this Factory value, so two Factory values
// share nothing. It therefore models a SINGLE node's storage honestly and does
// NOT model the cross-node shared backing a real handoff fences through; a test
// that needs several nodes over one backing wants internal/sharedfactory
// instead.
//
// Concurrency: every method is safe to call concurrently (guarded by a mutex).
// The returned memory.Memory backends are themselves goroutine-safe per the
// backend.Backend contract.
package memfactory

import (
	"fmt"
	"slices"
	"sync"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// Factory mounts an in-memory backend per storageunit.MountRef. It honors the
// epoch fencing contract (open at a higher epoch fences any prior writer; a
// double-open of a mount it already holds is an error) so it can drive a lease
// handoff in tests, but the per-mount data is held in a map keyed by the
// MountRef so a close-then-reopen at a higher epoch RETAINS the data (the
// durable-database analogue: the bytes survive an unmount; only the writer
// lease moves). That retention is what lets a handoff test assert "the new
// owner sees the prior owner's keys."
//
// Keying by MountRef means every coordinate of the identity separates stores:
// a gen-g unit K and a gen-(g+1) unit K are DISTINCT (the doubling reshard
// fills the new generation's units while the old generation's unit keeps
// serving), two replica positions of one unit are DISTINCT (that is what
// replication buys), and a sole mount is DISTINCT from replica 0 of the same
// unit (matching the two different on-disk layouts a real adapter derives).
type Factory struct {
	mu     sync.Mutex
	open   map[storageunit.MountRef]storageunit.Epoch // currently-held mounts -> epoch
	store  map[storageunit.MountRef]*memory.Memory    // persistent per-mount data (survives CloseUnit)
	fenced map[storageunit.MountRef]storageunit.Epoch // highest epoch ever opened per mount
	marker map[storageunit.MountRef]storageunit.Epoch // serving markers
}

// New returns an empty Factory with no mounts open.
func New() *Factory {
	return &Factory{
		open:   make(map[storageunit.MountRef]storageunit.Epoch),
		store:  make(map[storageunit.MountRef]*memory.Memory),
		fenced: make(map[storageunit.MountRef]storageunit.Epoch),
		marker: make(map[storageunit.MountRef]storageunit.Epoch),
	}
}

// OpenUnit mounts m at epoch and returns the epoch it landed at. It returns an
// error if m is already open at an equal-or-higher epoch (a double-open /
// stale acquire). The mount's data persists across a CloseUnit (see Factory
// doc), so re-opening returns the SAME underlying store with whatever was
// written before.
//
// This factory derives the open epoch EXACTLY as slate does:
// opened = max(intended, durable+1), where the durable floor is the highest
// epoch this mount has ever been opened at. It used to take the caller's
// intended epoch VERBATIM ("tests drive the epoch arithmetic explicitly") -
// which meant every epoch assertion against this double asserted its own
// input, and a stale writer could re-open BELOW the historical fence after a
// close, a state the real backing store makes impossible. That fidelity gap
// is part of why the serving-marker wedge reached production: the bug class
// was structurally invisible to memfactory-based tests. The double must be
// allowed to disagree with the caller, because the real store does.
func (f *Factory) OpenUnit(m storageunit.MountRef, epoch storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	opened := epoch
	if floor := f.fenced[m]; opened <= floor {
		opened = floor + 1
	}
	if cur, ok := f.open[m]; ok && opened <= cur {
		return nil, 0, fmt.Errorf("memfactory: %s already open at epoch %d, refusing open at %d (intended %d)", m, cur, opened, epoch)
	}
	b, ok := f.store[m]
	if !ok {
		b = memory.New()
		f.store[m] = b
	}
	f.open[m] = opened
	if opened > f.fenced[m] {
		f.fenced[m] = opened
	}
	return b, opened, nil
}

// CloseUnit unmounts m. Idempotent: closing a mount that is not open is a
// no-op returning nil. The mount's data is RETAINED (only the writer lease is
// released) so a later OpenUnit at a higher epoch sees it again.
func (f *Factory) CloseUnit(m storageunit.MountRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.open, m)
	return nil
}

// CurrentEpoch reports the epoch m is currently held at, and ok=false if m is
// not held. Local in-process view only.
func (f *Factory) CurrentEpoch(m storageunit.MountRef) (storageunit.Epoch, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.open[m]
	return e, ok
}

// DurableEpoch reports the highest epoch m has ever been opened at, read
// without mounting. A mount never opened reports 0. Never errors: there is no
// I/O to fail.
func (f *Factory) DurableEpoch(m storageunit.MountRef) (storageunit.Epoch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fenced[m], nil
}

// WriteServingMarker records m's serving marker at epoch. Monotonic: a write
// at a lower epoch than the one already recorded is ignored, so a stale write
// from a fenced prior owner cannot roll the marker back.
func (f *Factory) WriteServingMarker(m storageunit.MountRef, epoch storageunit.Epoch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := f.marker[m]; ok && cur >= epoch {
		return nil
	}
	f.marker[m] = epoch
	return nil
}

// ReadServingMarker reads m's serving marker without mounting. A never-written
// marker is (0, false, nil), not an error.
func (f *Factory) ReadServingMarker(m storageunit.MountRef) (storageunit.Epoch, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.marker[m]
	return e, ok, nil
}

// UnitBackend returns the per-unit backend this factory holds for the SOLE
// mount of gu, and ok=false if that mount has never been opened (so no store
// exists yet). It exposes the mount -> backend map to tests so they can assert
// PHYSICAL placement at the unit granularity: a key must land in exactly its
// own unit's backend and nowhere else. The store survives CloseUnit (see the
// Factory doc), so this returns the backend whether the mount is currently held
// or not. Inspection only; it does not mount, fence, or mutate epoch.
func (f *Factory) UnitBackend(gu storageunit.GenUnit) (backend.Backend, bool) {
	return f.MountBackend(storageunit.SoleMount(gu))
}

// MountBackend is UnitBackend for an arbitrary mount identity, so a test can
// inspect a replica position's store as well as a sole unit's.
func (f *Factory) MountBackend(m storageunit.MountRef) (backend.Backend, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.store[m]
	return b, ok
}

// OpenUnits returns the currently-held mounts in CompareMountRefs order. The
// returned slice is a fresh copy the caller may retain.
func (f *Factory) OpenUnits() []storageunit.MountRef {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]storageunit.MountRef, 0, len(f.open))
	for m := range f.open {
		out = append(out, m)
	}
	slices.SortFunc(out, storageunit.CompareMountRefs)
	return out
}

// compile-time assertion that Factory satisfies the one storage port.
var _ storageunit.BackendFactory = (*Factory)(nil)
