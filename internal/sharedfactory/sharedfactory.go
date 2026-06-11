// Package sharedfactory is a SHARED-BACKING in-memory
// storageunit.BackendFactory for the v0.8 Phase 3 lease-handoff tests. It
// models the property a real object-store-backed factory (slatedb) has but
// the per-factory internal/memfactory does NOT: the unit DATA and the
// per-unit writer-EPOCH live in ONE place that MULTIPLE per-node factory
// handles reference.
//
// Why a second test factory: memfactory retains a unit's data across
// CloseUnit, but its store is PER-FACTORY. Each node in an in-process
// cluster test gets its own memfactory, so a handoff of unit U from node A
// to node B would land B against a DIFFERENT, empty store - it could never
// see A's writes. That is fine for Phase 2 (static topology, no handoff)
// but useless for a COPY-FREE handoff test, where the whole point is that B
// opens the SAME underlying bytes A wrote (zero copy) because they live in
// shared object storage.
//
// The model:
//
//   - A Backing holds, per unit: ONE shared *memory.Memory (the durable
//     bytes) and ONE durable writer-epoch (the manifest writer-epoch
//     analogue). It is the "shared object storage" every node points at.
//   - Handle(nodeID) returns a per-node factory handle. Many handles share
//     one Backing. Each handle tracks which units IT currently has open and
//     at what epoch (its in-process view), but the AUTHORITATIVE epoch is in
//     the Backing.
//   - OpenUnit fences against the Backing's durable epoch: it must open at a
//     STRICTLY HIGHER epoch than the durable one, bumps the durable epoch to
//     the opened epoch, and returns a fencedBackend wrapping the SHARED
//     *memory.Memory. So when U hands off A -> B, B opens the SAME bytes
//     (data persists, zero copy) and the Backing's durable epoch advances,
//     which FENCES A: A's fencedBackend now observes a durable epoch above
//     its own captured epoch and fails every write.
//
// This is test support. It lives under internal/ and is NOT a shipped
// backend; the slatedb per-unit factory is the production implementation
// (later work). It deliberately mirrors the slatedb fencing semantics so a
// Phase 3 handoff test exercises the real cross-node invariant: NO ACKED
// WRITE LOST + the prior owner is fenced.
package sharedfactory

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// ErrFenced is returned by a write against a unit whose lease has moved: the
// shared durable epoch has advanced past the epoch this handle opened the
// unit at, so a higher-epoch owner has fenced this (now stale) writer. It is
// the in-test analogue of slatedb's writer-epoch fencing error. A read after
// fencing still succeeds (the bytes are shared + durable); only WRITES from
// the fenced handle fail, which is exactly the single-writer guarantee.
var ErrFenced = errors.New("sharedfactory: writer fenced; unit lease moved to a higher epoch")

// Backing is the shared object-storage analogue: per-unit durable bytes +
// per-unit durable writer-epoch, referenced by every per-node Handle. One
// Backing per CLUSTER in a test; one Handle per NODE off that Backing.
type Backing struct {
	mu     sync.Mutex
	stores map[storageunit.UnitID]*memory.Memory // shared durable bytes per unit
	epochs map[storageunit.UnitID]storageunit.Epoch
}

// NewBacking returns an empty shared backing (no units written yet).
func NewBacking() *Backing {
	return &Backing{
		stores: make(map[storageunit.UnitID]*memory.Memory),
		epochs: make(map[storageunit.UnitID]storageunit.Epoch),
	}
}

// storeFor returns the shared *memory.Memory for u, creating it on first
// touch. Caller must hold b.mu.
func (b *Backing) storeFor(u storageunit.UnitID) *memory.Memory {
	s, ok := b.stores[u]
	if !ok {
		s = memory.New()
		b.stores[u] = s
	}
	return s
}

// durableEpoch reports the unit's durable writer-epoch (0 if never opened).
// This is the cross-node source of truth a real factory reads from the
// slatedb manifest. Exposed for test assertions.
func (b *Backing) durableEpoch(u storageunit.UnitID) storageunit.Epoch {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.epochs[u]
}

// DurableEpoch is the exported form of durableEpoch for tests that assert
// the fence advanced the epoch.
func (b *Backing) DurableEpoch(u storageunit.UnitID) storageunit.Epoch {
	return b.durableEpoch(u)
}

// UnitStore returns the shared backend for u (and ok=false if u was never
// opened). Tests use it to assert the SAME bytes are visible regardless of
// which node currently owns the lease (the copy-free property).
func (b *Backing) UnitStore(u storageunit.UnitID) (backend.Backend, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.stores[u]
	return s, ok
}

// acquire opens u on behalf of a Handle that does NOT currently hold u (the
// handoff / cold-acquire case), fencing against the durable epoch. The
// intended epoch is a best-effort FLOOR from the cluster; the AUTHORITATIVE
// fence is the backing's durable epoch. acquire opens at
// max(intended, durableEpoch+1), so it ALWAYS lands strictly above the
// durable epoch (the higher-epoch-fences-lower rule) even when the cluster's
// local epoch hint is stale (it cannot know another node's durable epoch
// from its in-process view - that is the whole point of routing the fence
// through the durable manifest here). It advances the durable epoch to the
// opened epoch (fencing any handle still open at a lower epoch) and returns
// the shared store + the epoch it actually opened at. This NEVER rejects a
// cold acquire for being "too low": a new owner must always be able to take
// the lease. (Re-acquire of a unit THIS handle already holds is gated by
// Handle.OpenUnit before it reaches here.)
func (b *Backing) acquire(u storageunit.UnitID, intended storageunit.Epoch) (*memory.Memory, storageunit.Epoch) {
	b.mu.Lock()
	defer b.mu.Unlock()
	opened := intended
	if floor := b.epochs[u] + 1; floor > opened {
		opened = floor
	}
	b.epochs[u] = opened
	return b.storeFor(u), opened
}

// Handle is a per-node factory handle over a shared Backing. It implements
// storageunit.BackendFactory. Several handles (one per node) share one
// Backing, which is what makes a handoff copy-free + fence-correct.
type Handle struct {
	backing *Backing

	mu   sync.Mutex
	open map[storageunit.UnitID]storageunit.Epoch // units THIS handle has open + at what epoch
}

// Handle returns a fresh per-node handle over this backing. Each node in a
// test gets its own handle; they all share b.
func (b *Backing) Handle() *Handle {
	return &Handle{
		backing: b,
		open:    make(map[storageunit.UnitID]storageunit.Epoch),
	}
}

// OpenUnit acquires unit u against the shared backing, fencing any prior
// owner whose epoch is lower. It returns a fencedBackend over the SHARED
// bytes so this handle sees every write any prior owner made (the copy-free
// handoff) and so this handle's own writes start failing the instant a
// still-higher-epoch owner acquires u later.
//
// Two cases, mirroring the BackendFactory contract:
//
//   - This handle ALREADY holds u open: re-opening at an equal-or-lower
//     epoch is a double-open programming error (a unit has one live writer
//     per handle); reject. A strictly-higher re-open is allowed (the same
//     node bumping its own epoch).
//   - This handle does NOT hold u (the cold-acquire / handoff case): the
//     intended epoch is a best-effort floor; the backing fences
//     authoritatively at max(intended, durableEpoch+1). This NEVER rejects -
//     a new owner must always be able to take the lease - because the
//     cluster cannot know another node's durable epoch from its in-process
//     view (CurrentEpoch returns ok=false for a unit it never held), so it
//     passes a low floor and the durable manifest governs.
func (h *Handle) OpenUnit(u storageunit.UnitID, epoch storageunit.Epoch) (backend.Backend, error) {
	h.mu.Lock()
	if cur, held := h.open[u]; held && epoch <= cur {
		h.mu.Unlock()
		return nil, fmt.Errorf("sharedfactory: unit %d already open on this handle at epoch %d, refusing open at %d", u, cur, epoch)
	}
	h.mu.Unlock()

	store, opened := h.backing.acquire(u, epoch)
	h.mu.Lock()
	h.open[u] = opened
	h.mu.Unlock()
	return &fencedBackend{backing: h.backing, unit: u, epoch: opened, store: store}, nil
}

// CloseUnit releases u from THIS handle. It flushes nothing (memory is
// always "durable" here) and does NOT lower the backing's durable epoch -
// releasing a lease never un-fences a higher-epoch owner. Idempotent:
// closing a unit this handle does not hold is a no-op returning nil. The
// shared bytes are retained in the Backing so a later OpenUnit (here or on
// another handle) at a higher epoch sees them - this is the durable-bytes-
// survive-unmount property a real handoff relies on.
func (h *Handle) CloseUnit(u storageunit.UnitID) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.open, u)
	return nil
}

// CurrentEpoch reports the epoch THIS handle currently holds u open at, and
// ok=false if this handle does not have u open. LOCAL in-process view only,
// per the BackendFactory contract: the cross-node source of truth is the
// Backing's durable epoch (DurableEpoch), which OpenUnit fences against.
func (h *Handle) CurrentEpoch(u storageunit.UnitID) (storageunit.Epoch, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.open[u]
	return e, ok
}

// OpenUnits returns the units THIS handle currently has open, ascending. A
// fresh copy the caller may retain.
func (h *Handle) OpenUnits() []storageunit.UnitID {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]storageunit.UnitID, 0, len(h.open))
	for u := range h.open {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// fencedBackend wraps the shared *memory.Memory for one unit, captured at
// the epoch THIS handle opened it. Reads pass through unconditionally (the
// bytes are shared + durable). WRITES first check the backing's durable
// epoch: if it has advanced past this backend's captured epoch, a
// higher-epoch owner has FENCED this writer and the write returns ErrFenced.
// This is the single-writer guarantee the lease handoff depends on: after B
// acquires u at epoch+1, A's fencedBackend (still at epoch) cannot write,
// even though A has not yet observed the membership change.
type fencedBackend struct {
	backing *Backing
	unit    storageunit.UnitID
	epoch   storageunit.Epoch
	store   *memory.Memory
}

// fenced reports whether this writer has been superseded: the durable epoch
// is now ABOVE the epoch this backend was opened at.
func (f *fencedBackend) fenced() bool {
	return f.backing.durableEpoch(f.unit) > f.epoch
}

func (f *fencedBackend) Put(key, value []byte) error {
	if f.fenced() {
		return ErrFenced
	}
	return f.store.Put(key, value)
}

func (f *fencedBackend) Delete(key []byte) error {
	if f.fenced() {
		return ErrFenced
	}
	return f.store.Delete(key)
}

func (f *fencedBackend) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	if f.fenced() {
		return nil, ErrFenced
	}
	return f.store.Begin(level)
}

// Get + ScanPrefix are reads: always allowed against the shared bytes, even
// once fenced. (A fenced node should not SERVE in practice - the cluster's
// reconcile unmounts it - but the backend itself must not invent data loss
// on read; the bytes are real + shared.)
func (f *fencedBackend) Get(key []byte) ([]byte, error) { return f.store.Get(key) }

func (f *fencedBackend) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	return f.store.ScanPrefix(prefix)
}

// Close is a no-op on the wrapper: the shared store outlives any one
// handle's mount (the durable-bytes-survive-unmount property). CloseUnit on
// the Handle is the real release.
func (f *fencedBackend) Close() error { return nil }
