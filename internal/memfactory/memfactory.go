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

// Factory mounts an in-memory backend per unit. It honors the epoch
// fencing contract (open at a higher epoch fences any prior writer; open
// at an equal-or-lower epoch on an already-open unit is an error) so it
// can drive a lease handoff in tests, but the per-unit data is held in a
// map keyed by unit so a close-then-reopen at a higher epoch RETAINS the
// data (the durable-database analogue: the bytes survive an unmount; only
// the writer lease moves). That retention is what lets a Phase 3 handoff
// test assert "the new owner sees the prior owner's keys."
type Factory struct {
	mu    sync.Mutex
	open  map[storageunit.UnitID]storageunit.Epoch // currently-mounted units -> epoch
	store map[storageunit.UnitID]*memory.Memory    // persistent per-unit data (survives CloseUnit)
}

// New returns an empty Factory with no units open.
func New() *Factory {
	return &Factory{
		open:  make(map[storageunit.UnitID]storageunit.Epoch),
		store: make(map[storageunit.UnitID]*memory.Memory),
	}
}

// OpenUnit mounts unit u at epoch. It returns an error if u is already
// open at an equal-or-higher epoch (a unit has at most one live writer;
// re-opening at a not-strictly-higher epoch is a double-open / stale
// acquire, exactly the BackendFactory contract). The unit's data persists
// across a CloseUnit (see Factory doc), so re-opening returns the SAME
// underlying store with whatever was written before.
func (f *Factory) OpenUnit(u storageunit.UnitID, epoch storageunit.Epoch) (backend.Backend, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := f.open[u]; ok && epoch <= cur {
		return nil, fmt.Errorf("memfactory: unit %d already open at epoch %d, refusing open at %d", u, cur, epoch)
	}
	b, ok := f.store[u]
	if !ok {
		b = memory.New()
		f.store[u] = b
	}
	f.open[u] = epoch
	return b, nil
}

// CloseUnit unmounts unit u. Idempotent: closing a unit that is not open
// is a no-op returning nil. The unit's data is RETAINED (only the writer
// lease is released) so a later OpenUnit at a higher epoch sees it again.
func (f *Factory) CloseUnit(u storageunit.UnitID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.open, u)
	return nil
}

// CurrentEpoch reports the epoch u is currently mounted at, and ok=false
// if u is not mounted. Local in-process view only.
func (f *Factory) CurrentEpoch(u storageunit.UnitID) (storageunit.Epoch, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.open[u]
	return e, ok
}

// OpenUnits returns the currently-mounted units in ascending order. The
// returned slice is a fresh copy the caller may retain.
func (f *Factory) OpenUnits() []storageunit.UnitID {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]storageunit.UnitID, 0, len(f.open))
	for u := range f.open {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
