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
	"sync/atomic"
	"time"

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
//
// Keyed by GenUnit (the generation-qualified identity), so a gen-g unit K
// and a gen-(g+1) unit K are SEPARATE durable databases with SEPARATE
// writer-epochs - exactly the property the v0.8 Phase 4 doubling reshard
// depends on, where the old unit keeps serving while its two new-generation
// children are populated.
type Backing struct {
	mu     sync.Mutex
	stores map[storageunit.GenUnit]*memory.Memory // shared durable bytes per unit (R=1)
	epochs map[storageunit.GenUnit]storageunit.Epoch

	// R>1 (v0.8 Phase 2b): per-replica durable identity. A unit's R replicas
	// are INDEPENDENT databases, so they cannot share the R=1 GenUnit-keyed
	// store above. Keyed by ReplicaUnit{GenUnit, Replica}: replica 0 and
	// replica 1 of one unit are separate stores that survive each other, which
	// is what makes a single-replica node loss recoverable from the other copy.
	replicaStores map[storageunit.ReplicaUnit]*memory.Memory
	replicaEpochs map[storageunit.ReplicaUnit]storageunit.Epoch

	// openFaults injects OpenReplicaUnit failures for specific replica positions
	// (test-only, v0.8 Phase 2f degraded-boot gate). A ReplicaUnit present here
	// makes EVERY handle's OpenReplicaUnit return the stored error, modeling a
	// permanently un-openable backing store (a corrupt / truncated durable
	// database that slatedb cannot open). Keyed by ReplicaUnit -> error; cleared
	// by SetOpenReplicaFault(ru, nil) so a test can prove the position RE-MOUNTS
	// once the damage is repaired. A sync.Map so it is safe to arm/clear
	// concurrently with mounts without taking b.mu.
	openFaults sync.Map

	// servingMarkers is the durable, poll-observable SERVING MARKER registry
	// (v0.8 Phase 2e, Option B overlap handoff): per-replica-position, the open
	// epoch of the latest live owner that reached Ready. It is the in-memory
	// analogue of the small durable object the slate factory writes to shared
	// storage keyed by dbNameReplica(ru). It is STRICTLY STRONGER than
	// replicaEpochs (the fence epoch): a marker exists ONLY after a new owner
	// completed its mount flip and started serving, whereas the fence epoch
	// bumps at open-START. The old (draining) owner polls it to release. Stored
	// in the same shared Backing every per-node Handle references, so the new
	// owner's WriteServingMarker is visible to the old owner's ReadServingMarker
	// across nodes. Absence (no entry) is "no live owner yet": ok == false.
	servingMarkers map[storageunit.ReplicaUnit]storageunit.Epoch
}

// NewBacking returns an empty shared backing (no units written yet).
func NewBacking() *Backing {
	return &Backing{
		stores:         make(map[storageunit.GenUnit]*memory.Memory),
		epochs:         make(map[storageunit.GenUnit]storageunit.Epoch),
		replicaStores:  make(map[storageunit.ReplicaUnit]*memory.Memory),
		replicaEpochs:  make(map[storageunit.ReplicaUnit]storageunit.Epoch),
		servingMarkers: make(map[storageunit.ReplicaUnit]storageunit.Epoch),
	}
}

// storeFor returns the shared *memory.Memory for gu, creating it on first
// touch. Caller must hold b.mu.
func (b *Backing) storeFor(gu storageunit.GenUnit) *memory.Memory {
	s, ok := b.stores[gu]
	if !ok {
		s = memory.New()
		b.stores[gu] = s
	}
	return s
}

// durableEpoch reports the unit's durable writer-epoch (0 if never opened).
// This is the cross-node source of truth a real factory reads from the
// slatedb manifest. Exposed for test assertions.
func (b *Backing) durableEpoch(gu storageunit.GenUnit) storageunit.Epoch {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.epochs[gu]
}

// DurableEpoch is the exported form of durableEpoch for tests that assert
// the fence advanced the epoch.
func (b *Backing) DurableEpoch(gu storageunit.GenUnit) storageunit.Epoch {
	return b.durableEpoch(gu)
}

// UnitStore returns the shared backend for gu (and ok=false if gu was never
// opened). Tests use it to assert the SAME bytes are visible regardless of
// which node currently owns the lease (the copy-free property), and to
// inspect a freshly-bisected gen-(g+1) child unit's contents.
func (b *Backing) UnitStore(gu storageunit.GenUnit) (backend.Backend, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.stores[gu]
	return s, ok
}

// WipeUnit empties u's shared bytes IN PLACE: it deletes every key from the
// existing shared *memory.Memory rather than swapping in a fresh one, so ANY
// node that has already opened u (and captured a pointer to that store) sees
// the data vanish too. This is what makes it a faithful data-loss simulation:
// a pointer swap would leave an earlier acquirer reading the old populated
// store, masking the loss. It exists ONLY to let a test simulate a data-loss
// handoff bug (a release that fails to flush, or bytes otherwise lost) so the
// lossless-handoff gate test can prove it actually catches a lost write rather
// than rubber-stamping. NO production code path does this: a real release
// flushes and never destroys durable bytes. The durable epoch is left
// untouched so a subsequent acquire still fences correctly; only the data is
// gone, which is precisely the failure the gate must detect.
func (b *Backing) WipeUnit(gu storageunit.GenUnit) {
	b.mu.Lock()
	s, ok := b.stores[gu]
	b.mu.Unlock()
	if !ok {
		return
	}
	wipeStore(s)
}

// wipeStore deletes every key from s IN PLACE (so any handle holding a pointer
// to s sees the data vanish). Shared by WipeUnit + WipeReplica.
func wipeStore(s *memory.Memory) {
	it, err := s.ScanPrefix(nil)
	if err != nil {
		return
	}
	var keys [][]byte
	for {
		k, _, err := it.Next()
		if err != nil || k == nil {
			break
		}
		kc := make([]byte, len(k))
		copy(kc, k)
		keys = append(keys, kc)
	}
	_ = it.Close()
	for _, k := range keys {
		_ = s.Delete(k)
	}
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
func (b *Backing) acquire(gu storageunit.GenUnit, intended storageunit.Epoch) (*memory.Memory, storageunit.Epoch) {
	b.mu.Lock()
	defer b.mu.Unlock()
	opened := intended
	if floor := b.epochs[gu] + 1; floor > opened {
		opened = floor
	}
	b.epochs[gu] = opened
	return b.storeFor(gu), opened
}

// replicaStoreFor returns the shared *memory.Memory for replica ru, creating
// it on first touch. Caller must hold b.mu. The R>1 analogue of storeFor.
func (b *Backing) replicaStoreFor(ru storageunit.ReplicaUnit) *memory.Memory {
	s, ok := b.replicaStores[ru]
	if !ok {
		s = memory.New()
		b.replicaStores[ru] = s
	}
	return s
}

// ReplicaStore returns the shared backend for replica ru (ok=false if never
// opened). Tests use it to assert a write landed on a SPECIFIC replica copy
// and that the R copies are independent.
func (b *Backing) ReplicaStore(ru storageunit.ReplicaUnit) (backend.Backend, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.replicaStores[ru]
	return s, ok
}

// WipeReplica empties replica ru's shared bytes IN PLACE (same in-place
// deletion semantics as WipeUnit) so a test can simulate the loss of one
// replica copy and prove the gate catches it. Test-only; no production path.
func (b *Backing) WipeReplica(ru storageunit.ReplicaUnit) {
	b.mu.Lock()
	s, ok := b.replicaStores[ru]
	b.mu.Unlock()
	if !ok {
		return
	}
	wipeStore(s)
}

// acquireReplica is the R>1 analogue of acquire: it opens replica ru against
// the per-replica durable store + epoch, fencing at max(intended, durable+1).
func (b *Backing) acquireReplica(ru storageunit.ReplicaUnit, intended storageunit.Epoch) (*memory.Memory, storageunit.Epoch) {
	b.mu.Lock()
	defer b.mu.Unlock()
	opened := intended
	if floor := b.replicaEpochs[ru] + 1; floor > opened {
		opened = floor
	}
	b.replicaEpochs[ru] = opened
	return b.replicaStoreFor(ru), opened
}

// replicaDurableEpoch reports replica ru's durable writer-epoch (0 if never
// opened). The per-replica analogue of durableEpoch.
func (b *Backing) replicaDurableEpoch(ru storageunit.ReplicaUnit) storageunit.Epoch {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.replicaEpochs[ru]
}

// writeServingMarker records the serving marker for replica ru at epoch (the
// shared-storage write the new owner does at its mount flip). It is MONOTONIC:
// it never lowers an already-recorded epoch, so a stale write from a fenced
// prior owner cannot roll the marker back below a live higher-epoch owner's
// value. Idempotent at the same-or-higher epoch.
func (b *Backing) writeServingMarker(ru storageunit.ReplicaUnit, epoch storageunit.Epoch) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur, ok := b.servingMarkers[ru]; ok && cur >= epoch {
		return
	}
	b.servingMarkers[ru] = epoch
}

// readServingMarker reports replica ru's serving-marker epoch and whether a
// marker has been written at all (ok). The point-in-time read the old owner's
// drainCheck polls. ok == false (no entry) means no live owner has reached
// Ready for this position yet.
func (b *Backing) readServingMarker(ru storageunit.ReplicaUnit) (storageunit.Epoch, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.servingMarkers[ru]
	return e, ok
}

// ServingMarker is the exported read of the serving marker for tests that
// assert the new owner wrote it at the expected epoch after the mount flip.
func (b *Backing) ServingMarker(ru storageunit.ReplicaUnit) (storageunit.Epoch, bool) {
	return b.readServingMarker(ru)
}

// Handle is a per-node factory handle over a shared Backing. It implements
// storageunit.BackendFactory and storageunit.ReplicaBackendFactory. Several
// handles (one per node) share one Backing, which is what makes a handoff
// copy-free + fence-correct, and (at R>1) what makes a unit's R independent
// replicas reachable from the nodes that hold them.
type Handle struct {
	backing *Backing

	// acquireDelayNanos, when > 0, makes OpenReplicaUnit (the R>1 acquire path)
	// SLEEP that long before mounting, simulating the latency of opening a
	// per-(unit, replica) slatedb manifest from real object storage. It widens
	// the handoff acquiring-window deterministically so the v0.8 Phase 2d
	// write-availability gate can reproduce the pre-fix ack-rate drop (and prove
	// the fix rides it out). Zero = instant (every existing test). Test-only.
	acquireDelayNanos atomic.Int64

	mu          sync.Mutex
	open        map[storageunit.GenUnit]storageunit.Epoch     // units THIS handle has open + at what epoch (R=1)
	openReplica map[storageunit.ReplicaUnit]storageunit.Epoch // replicas THIS handle has open + at what epoch (R>1)
}

// Handle returns a fresh per-node handle over this backing. Each node in a
// test gets its own handle; they all share b.
func (b *Backing) Handle() *Handle {
	return &Handle{
		backing:     b,
		open:        make(map[storageunit.GenUnit]storageunit.Epoch),
		openReplica: make(map[storageunit.ReplicaUnit]storageunit.Epoch),
	}
}

// SetAcquireDelay sets the artificial OpenReplicaUnit acquire latency for this
// handle (see acquireDelayNanos). Safe to call concurrently with mounts. A test
// arms it before triggering a membership change to widen the acquiring window.
func (h *Handle) SetAcquireDelay(d time.Duration) {
	h.acquireDelayNanos.Store(int64(d))
}

// SetOpenReplicaFault arms (err != nil) or clears (err == nil) an injected
// OpenReplicaUnit failure for replica position ru on the SHARED backing, so
// every handle that tries to open ru sees it - modeling a permanently
// un-openable backing store. Test-only (v0.8 Phase 2f degraded-boot gate).
func (b *Backing) SetOpenReplicaFault(ru storageunit.ReplicaUnit, err error) {
	if err == nil {
		b.openFaults.Delete(ru)
		return
	}
	b.openFaults.Store(ru, err)
}

// OpenReplicaUnit opens replica position ru.Replica of unit ru.Unit against
// the per-replica shared backing, fencing any prior writer of the SAME
// replica position. It returns a fencedReplicaBackend over the SHARED
// per-replica bytes so a node re-acquiring this replica position sees its
// prior writes, and so its writes start failing the instant a higher-epoch
// owner of the same position acquires it. Distinct replica positions are
// independent stores: opening replica 1 never touches replica 0.
func (h *Handle) OpenReplicaUnit(ru storageunit.ReplicaUnit, epoch storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	// Injected open fault (test-only, Phase 2f): model a permanently un-openable
	// backing store. Checked BEFORE any state mutation so a faulted open is a
	// clean no-op, exactly like a real slatedb open that errors before mounting.
	if f, ok := h.backing.openFaults.Load(ru); ok {
		return nil, 0, f.(error)
	}
	h.mu.Lock()
	if cur, held := h.openReplica[ru]; held && epoch <= cur {
		h.mu.Unlock()
		return nil, 0, fmt.Errorf("sharedfactory: replica %s already open on this handle at epoch %d, refusing open at %d", ru, cur, epoch)
	}
	h.mu.Unlock()

	// Simulate object-store open latency to widen the acquiring window (test
	// support for the Phase 2d write-availability gate). The sleep is BEFORE the
	// epoch bump, so the unit is genuinely unmounted (a routed op gets the
	// retryable acquiring-window error) for the whole delay.
	if d := h.acquireDelayNanos.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}

	store, opened := h.backing.acquireReplica(ru, epoch)
	h.mu.Lock()
	h.openReplica[ru] = opened
	h.mu.Unlock()
	// opened is the EXACT fence epoch this open landed at (max(intended,
	// durable+1)); the caller uses it as this node's open epoch.
	return &fencedReplicaBackend{backing: h.backing, unit: ru, epoch: opened, store: store}, opened, nil
}

// DurableEpochReplica reports replica ru's DURABLE writer-epoch from the
// shared Backing WITHOUT opening it (0 if never opened). It is the test analogue
// of reading the per-replica slatedb manifest writer-epoch from object storage:
// the CROSS-NODE source of truth every handle sees, regardless of which handle
// currently holds the position open. The overlap handoff's old owner reads it
// as a LIVENESS HINT (the durable epoch advancing past its own open epoch means
// a new owner fenced); it never releases on a bare advance. It cannot fail here
// (the in-memory backing always answers), so err is always nil.
func (h *Handle) DurableEpochReplica(ru storageunit.ReplicaUnit) (storageunit.Epoch, error) {
	return h.backing.replicaDurableEpoch(ru), nil
}

// WriteServingMarker writes the durable serving marker for replica ru at epoch
// to the shared Backing (v0.8 Phase 2e). The new owner calls it EXACTLY ONCE at
// its Acquiring -> Ready mount flip; because the marker lives in the shared
// Backing every per-node Handle references, the old owner's Handle observes it
// via ReadServingMarker across nodes. The write is monotonic (never lowers a
// recorded epoch) so a stale write cannot roll the marker back. It cannot fail
// here (the in-memory backing always answers), so err is always nil.
func (h *Handle) WriteServingMarker(ru storageunit.ReplicaUnit, epoch storageunit.Epoch) error {
	h.backing.writeServingMarker(ru, epoch)
	return nil
}

// ReadServingMarker reads the durable serving marker for replica ru from the
// shared Backing WITHOUT opening it (v0.8 Phase 2e). It is the point-in-time
// liveness observation the old owner's drainCheck polls: it releases ONLY on
// ok == true AND epoch >= its own open epoch. ok == false means no live owner
// has reached Ready yet, so the old owner stays Draining + keeps serving. It
// cannot fail here, so err is always nil.
func (h *Handle) ReadServingMarker(ru storageunit.ReplicaUnit) (storageunit.Epoch, bool, error) {
	e, ok := h.backing.readServingMarker(ru)
	return e, ok, nil
}

// CloseReplicaUnit releases replica ru from THIS handle. Idempotent; the
// shared per-replica bytes are retained in the Backing.
func (h *Handle) CloseReplicaUnit(ru storageunit.ReplicaUnit) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.openReplica, ru)
	return nil
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
func (h *Handle) OpenUnit(gu storageunit.GenUnit, epoch storageunit.Epoch) (backend.Backend, error) {
	h.mu.Lock()
	if cur, held := h.open[gu]; held && epoch <= cur {
		h.mu.Unlock()
		return nil, fmt.Errorf("sharedfactory: unit %s already open on this handle at epoch %d, refusing open at %d", gu, cur, epoch)
	}
	h.mu.Unlock()

	store, opened := h.backing.acquire(gu, epoch)
	h.mu.Lock()
	h.open[gu] = opened
	h.mu.Unlock()
	return &fencedBackend{backing: h.backing, unit: gu, epoch: opened, store: store}, nil
}

// CloseUnit releases gu from THIS handle. It flushes nothing (memory is
// always "durable" here) and does NOT lower the backing's durable epoch -
// releasing a lease never un-fences a higher-epoch owner. Idempotent:
// closing a unit this handle does not hold is a no-op returning nil. The
// shared bytes are retained in the Backing so a later OpenUnit (here or on
// another handle) at a higher epoch sees them - this is the durable-bytes-
// survive-unmount property a real handoff relies on.
func (h *Handle) CloseUnit(gu storageunit.GenUnit) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.open, gu)
	return nil
}

// CurrentEpoch reports the epoch THIS handle currently holds gu open at, and
// ok=false if this handle does not have gu open. LOCAL in-process view only,
// per the BackendFactory contract: the cross-node source of truth is the
// Backing's durable epoch (DurableEpoch), which OpenUnit fences against.
func (h *Handle) CurrentEpoch(gu storageunit.GenUnit) (storageunit.Epoch, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.open[gu]
	return e, ok
}

// OpenUnits returns the units THIS handle currently has open, ascending by
// (Generation, UnitID). A fresh copy the caller may retain.
func (h *Handle) OpenUnits() []storageunit.GenUnit {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]storageunit.GenUnit, 0, len(h.open))
	for gu := range h.open {
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
	unit    storageunit.GenUnit
	epoch   storageunit.Epoch
	store   *memory.Memory
}

// fenced reports whether this writer has been superseded: the durable epoch
// is now ABOVE the epoch this backend was opened at.
//
// TODO(test-hardening): the fenced()-then-store.Put sequence below is not
// atomic. A concurrent acquire that advances durableEpoch BETWEEN the fenced
// check and the store write can let a write that should be fenced slip into
// the shared store. This is a test-double fidelity gap (the real slatedb
// manifest fence is atomic), not a production path. To close it, take a
// per-unit lock spanning the fenced check + the store mutation so acquire
// and a fenced write cannot interleave. Harmless to the current gate (which
// does not race a write against an in-flight acquire of the same unit), but
// worth tightening before relying on the factory for concurrency fuzzing.
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

// fencedReplicaBackend is fencedBackend keyed by a per-replica durable epoch
// (R>1, v0.8 Phase 2b). Reads pass through; writes fail once a higher-epoch
// owner of the SAME replica position has acquired it. Independent of the R=1
// GenUnit-keyed fence.
type fencedReplicaBackend struct {
	backing *Backing
	unit    storageunit.ReplicaUnit
	epoch   storageunit.Epoch
	store   *memory.Memory
}

func (f *fencedReplicaBackend) fenced() bool {
	return f.backing.replicaDurableEpoch(f.unit) > f.epoch
}

func (f *fencedReplicaBackend) Put(key, value []byte) error {
	if f.fenced() {
		return ErrFenced
	}
	return f.store.Put(key, value)
}

func (f *fencedReplicaBackend) Delete(key []byte) error {
	if f.fenced() {
		return ErrFenced
	}
	return f.store.Delete(key)
}

func (f *fencedReplicaBackend) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	if f.fenced() {
		return nil, ErrFenced
	}
	return f.store.Begin(level)
}

func (f *fencedReplicaBackend) Get(key []byte) ([]byte, error) { return f.store.Get(key) }

func (f *fencedReplicaBackend) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	return f.store.ScanPrefix(prefix)
}

func (f *fencedReplicaBackend) Close() error { return nil }
