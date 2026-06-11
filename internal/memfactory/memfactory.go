// Package memfactory is an in-memory storageunit.BackendFactory for tests
// and local dev: it hands out one pkg/backend/memory.Memory per storage
// unit. It is the multi-backend analogue of using memory.New() for the
// legacy single-backend path - a real (slatedb) factory mounts a durable
// database per unit; this one mounts an in-process map per unit so the
// multi-backend cluster paths can be exercised without object storage.
//
// It lives under internal/ so it is reusable across the core module's test
// trees (pkg/cluster tests + tests/integration) without widening the
// public API. It is NOT a shipped backend; the slatedb/pebble per-unit
// factories are the production implementations (later phases).
//
// Concurrency: OpenUnit / CloseUnit / the query methods are safe to call
// concurrently (guarded by a mutex). The returned memory.Memory backends
// are themselves goroutine-safe per the backend.Backend contract.
package memfactory

import (
	"fmt"
	"sort"
	"sync"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// Factory mounts an in-memory backend per GenUnit. It honors the epoch
// fencing contract (open at a higher epoch fences any prior writer; open
// at an equal-or-lower epoch on an already-open unit is an error) so it
// can drive a lease handoff in tests, but the per-unit data is held in a
// map keyed by the generation-qualified GenUnit so a close-then-reopen at a
// higher epoch RETAINS the data (the durable-database analogue: the bytes
// survive an unmount; only the writer lease moves). That retention is what
// lets a Phase 3 handoff test assert "the new owner sees the prior owner's
// keys."
//
// Keying by GenUnit (v0.8 Phase 4) means a gen-g unit K and a gen-(g+1) unit
// K are DISTINCT stores - the doubling reshard fills the new generation's
// units while the old generation's unit keeps serving, and they must not
// collide. A bare UnitID key would alias them onto one store.
type Factory struct {
	mu    sync.Mutex
	open  map[storageunit.GenUnit]storageunit.Epoch // currently-mounted units -> epoch
	store map[storageunit.GenUnit]*memory.Memory    // persistent per-unit data (survives CloseUnit)
}

// New returns an empty Factory with no units open.
func New() *Factory {
	return &Factory{
		open:  make(map[storageunit.GenUnit]storageunit.Epoch),
		store: make(map[storageunit.GenUnit]*memory.Memory),
	}
}

// OpenUnit mounts the generation-qualified unit gu at epoch. It returns an
// error if gu is already open at an equal-or-higher epoch (a unit has at
// most one live writer; re-opening at a not-strictly-higher epoch is a
// double-open / stale acquire, exactly the BackendFactory contract). The
// unit's data persists across a CloseUnit (see Factory doc), so re-opening
// returns the SAME underlying store with whatever was written before. A
// different generation of the same UnitID is a different store and is opened
// independently.
func (f *Factory) OpenUnit(gu storageunit.GenUnit, epoch storageunit.Epoch) (backend.Backend, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := f.open[gu]; ok && epoch <= cur {
		return nil, fmt.Errorf("memfactory: unit %s already open at epoch %d, refusing open at %d", gu, cur, epoch)
	}
	b, ok := f.store[gu]
	if !ok {
		b = memory.New()
		f.store[gu] = b
	}
	f.open[gu] = epoch
	return b, nil
}

// CloseUnit unmounts gu. Idempotent: closing a unit that is not open is a
// no-op returning nil. The unit's data is RETAINED (only the writer lease is
// released) so a later OpenUnit at a higher epoch sees it again.
func (f *Factory) CloseUnit(gu storageunit.GenUnit) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.open, gu)
	return nil
}

// CurrentEpoch reports the epoch gu is currently mounted at, and ok=false
// if gu is not mounted. Local in-process view only.
func (f *Factory) CurrentEpoch(gu storageunit.GenUnit) (storageunit.Epoch, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.open[gu]
	return e, ok
}

// UnitBackend returns the per-unit backend this factory holds for gu, and
// ok=false if the factory has never opened gu (so no store exists yet). It
// exposes the unit -> backend map to tests so they can assert PHYSICAL
// placement at the unit granularity: a key must land in exactly its own
// unit's backend and nowhere else. The store survives CloseUnit (see the
// Factory doc), so this returns the backend whether gu is currently mounted
// or not. Inspection only; it does not mount, fence, or mutate epoch.
func (f *Factory) UnitBackend(gu storageunit.GenUnit) (backend.Backend, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.store[gu]
	return b, ok
}

// OpenUnits returns the currently-mounted units in ascending
// (Generation, UnitID) order. The returned slice is a fresh copy the caller
// may retain.
func (f *Factory) OpenUnits() []storageunit.GenUnit {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]storageunit.GenUnit, 0, len(f.open))
	for gu := range f.open {
		out = append(out, gu)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Gen != out[j].Gen {
			return out[i].Gen < out[j].Gen
		}
		return out[i].ID < out[j].ID
	})
	return out
}
