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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// ErrFenced is returned by an op against a unit whose lease has moved: the
// shared durable epoch has advanced past the epoch this handle opened the
// unit at, so a higher-epoch owner has fenced this (now stale) writer. It is
// the in-test analogue of slatedb's writer-epoch fencing error. By default a
// fenced replica handle fails READS (Get + ScanPrefix) too, modeling slatedb's
// closed-handle-on-read; SetStrictReadFencing(false) opts a test back into the
// permissive reads-pass-through model (the bytes are shared + durable).
//
// It WRAPS backend.ErrFenced so errors.Is(err, backend.ErrFenced) resolves it -
// the backend-agnostic contract every real backend honors (pkg/backend:
// "the writer MUST wrap its native fence error"). Without the wrap the cluster's
// fence-recode self-heal (fenceToTransient) cannot recognize the test double's
// fence, so a test would silently NOT exercise the eviction path it means to.
var ErrFenced = fmt.Errorf("sharedfactory: writer fenced; unit lease moved to a higher epoch: %w", backend.ErrFenced)

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

	// strictReadFencing, when true, makes a FENCED handle fail READS (Get +
	// ScanPrefix) with ErrFenced too, not just writes. Real slatedb CLOSES a
	// fenced handle, so every op on it (read OR write) errors - and that is the
	// DEFAULT (set true in NewBacking), so tests run under production-shaped
	// semantics unless they opt out. The permissive mode (reads pass through a
	// fenced handle) exists only for tests that specifically need the old
	// timing and must justify the SetStrictReadFencing(false) call inline. The
	// permissive default masked the #433 routed-GET wedge; do not flip it back.
	strictReadFencing atomic.Bool

	// eagerFence, when true, models real slatedb's fence timing: the durable
	// writer-epoch bumps at open-START (inside DbBuilder.Build), fencing the prior
	// owner the INSTANT the successor begins its (slow) open, NOT at mount
	// completion. eagerFence bumps the epoch first, then sleeps, matching
	// production - and it is the DEFAULT (set true in NewBacking). The permissive
	// fence-at-completion mode sleeps the acquireDelay BEFORE the epoch bump, so
	// a displaced/draining owner stays UNFENCED for the whole modeled mount and
	// keeps serving the union - a fidelity gap that masked the union-write 1-ack
	// wedge (on real slatedb the fenced prior owner CANNOT serve, so the union
	// write reaches only the un-displaced co-replica and WEDGES). A test that
	// specifically needs the old fence-at-completion timing must justify its
	// SetEagerFence(false) opt-out inline.
	eagerFence atomic.Bool

	// openHangs holds, per replica position, a channel OpenReplicaUnit blocks
	// on at entry until the test releases it (HangOpenReplica) - modeling an
	// open WEDGED mid-FFI against a degraded store, the permit-watchdog pin's
	// scenario. Distinct from acquireDelayNanos (a bounded latency) and
	// openFaults (an immediate error). A sync.Map (arm/release without mu).
	openHangs sync.Map

	// openReplicaStarts counts OpenReplicaUnit ENTRIES per position across
	// every handle (guarded by mu). The permit-watchdog pin asserts a stuck
	// position is opened EXACTLY ONCE (no double-open) even after the permit
	// is released around it. The OpenTimeline records only COMPLETED opens,
	// so a hung open is visible here and nowhere else.
	openReplicaStarts map[storageunit.ReplicaUnit]int

	// replicaFlushes counts backend.Flusher.Flush calls per replica position
	// across every handle's backends (test observability for the cluster's
	// DISPLACEMENT FLUSH, v0.8 Phase 2e: the Owned -> Draining edge flushes the
	// displaced owner's backend exactly once). Guarded by mu; read via
	// ReplicaFlushCount.
	replicaFlushes map[storageunit.ReplicaUnit]int

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

	// markerAuthors records WHO wrote each servingMarkers entry (the gaining
	// node's ID), the in-memory analogue of the slate factory's sibling author
	// object. It is written in the SAME critical section as servingMarkers so
	// the author always corresponds to the recorded epoch. A position written
	// via the author-less WriteServingMarker has NO entry here, which reads back
	// as authorID == "" (UNKNOWN) - exactly the legacy/rolling-upgrade shape the
	// release gates fall back to the epoch-only rule for.
	markerAuthors map[storageunit.ReplicaUnit]string
}

// NewBacking returns an empty shared backing (no units written yet) with
// PRODUCTION-SHAPED fence semantics: strict read fencing (a fenced handle
// fails reads too, real slatedb's close-on-fence) and eager fence timing
// (the epoch bumps at open-START, real slatedb's DbBuilder.Build). A test
// that specifically needs the old permissive timing opts out via
// SetStrictReadFencing(false) / SetEagerFence(false) with an inline
// justification.
func NewBacking() *Backing {
	b := &Backing{
		stores:            make(map[storageunit.GenUnit]*memory.Memory),
		epochs:            make(map[storageunit.GenUnit]storageunit.Epoch),
		replicaStores:     make(map[storageunit.ReplicaUnit]*memory.Memory),
		replicaEpochs:     make(map[storageunit.ReplicaUnit]storageunit.Epoch),
		replicaFlushes:    make(map[storageunit.ReplicaUnit]int),
		openReplicaStarts: make(map[storageunit.ReplicaUnit]int),
		servingMarkers:    make(map[storageunit.ReplicaUnit]storageunit.Epoch),
		markerAuthors:     make(map[storageunit.ReplicaUnit]string),
	}
	b.strictReadFencing.Store(true)
	b.eagerFence.Store(true)
	return b
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
// authorID is the writing node's ID, or "" for an author-less (legacy) write.
// It is recorded in the SAME critical section as the epoch, so a reader never
// observes an author paired with a different epoch.
func (b *Backing) writeServingMarker(ru storageunit.ReplicaUnit, epoch storageunit.Epoch, authorID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur, ok := b.servingMarkers[ru]; ok && cur >= epoch {
		return
	}
	b.servingMarkers[ru] = epoch
	if authorID == "" {
		// An author-less write at a HIGHER epoch supersedes the prior author:
		// leaving the old ID behind would attribute this epoch to a node that
		// did not write it. Absence reads back as UNKNOWN, the safe fallback.
		delete(b.markerAuthors, ru)
		return
	}
	b.markerAuthors[ru] = authorID
}

// readServingMarker reports replica ru's serving-marker epoch and whether a
// marker has been written at all (ok). The point-in-time read the old owner's
// drainCheck polls. ok == false (no entry) means no live owner has reached
// Ready for this position yet.
func (b *Backing) readServingMarker(ru storageunit.ReplicaUnit) (storageunit.Epoch, string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.servingMarkers[ru]
	return e, b.markerAuthors[ru], ok
}

// ServingMarker is the exported read of the serving marker for tests that
// assert the new owner wrote it at the expected epoch after the mount flip.
func (b *Backing) ServingMarker(ru storageunit.ReplicaUnit) (storageunit.Epoch, bool) {
	e, _, ok := b.readServingMarker(ru)
	return e, ok
}

// ServingMarkerAuthor is the exported read of the serving marker's AUTHOR (the
// ID of the node that wrote it) for tests that assert the attribution, "" when
// the marker was written author-less.
func (b *Backing) ServingMarkerAuthor(ru storageunit.ReplicaUnit) string {
	_, author, _ := b.readServingMarker(ru)
	return author
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

	// openInFlight / openMaxInFlight track concurrent OpenReplicaUnit calls so a
	// test can assert the boot mount opens positions IN PARALLEL up to a bound
	// (v0.8 Phase 2g bounded-concurrency mount). openMaxInFlight is the
	// high-water mark of simultaneously in-flight opens. Test-only.
	openInFlight    atomic.Int64
	openMaxInFlight atomic.Int64

	mu          sync.Mutex
	open        map[storageunit.GenUnit]storageunit.Epoch     // units THIS handle has open + at what epoch (R=1)
	openReplica map[storageunit.ReplicaUnit]storageunit.Epoch // replicas THIS handle has open + at what epoch (R>1)

	// openSpans records every OpenReplicaUnit call's (position, start, end)
	// in completion order (guarded by mu; read via OpenTimeline). Test
	// support for the handoff-cycle latency pins. Test-only.
	openSpans []OpenSpan
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

// MaxConcurrentOpens reports the high-water mark of simultaneously in-flight
// OpenReplicaUnit calls on this handle. Test support for the v0.8 Phase 2g
// bounded-concurrency boot-mount gate (assert the mount opened positions in
// parallel up to, and never beyond, the configured bound).
func (h *Handle) MaxConcurrentOpens() int64 { return h.openMaxInFlight.Load() }

// OpenSpan is one recorded OpenReplicaUnit call on a handle: the position and
// the wall-clock start/end of the call. Test support for the handoff-cycle
// latency pins (assert queued opens CHAIN event-driven - the next open starts
// the moment the previous finishes - rather than waiting a reconcile tick).
type OpenSpan struct {
	RU    storageunit.ReplicaUnit
	Start time.Time
	End   time.Time
}

// OpenTimeline returns a copy of every OpenReplicaUnit span recorded on this
// handle, in completion order.
func (h *Handle) OpenTimeline() []OpenSpan {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]OpenSpan(nil), h.openSpans...)
}

// recordOpenSpan appends one completed open to the handle's timeline.
func (h *Handle) recordOpenSpan(ru storageunit.ReplicaUnit, start time.Time) {
	h.mu.Lock()
	h.openSpans = append(h.openSpans, OpenSpan{RU: ru, Start: start, End: time.Now()})
	h.mu.Unlock()
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

// HangOpenReplica arms a HANG on every subsequent OpenReplicaUnit of ru: the
// open blocks at entry until the returned release func is called (idempotent).
// Models an open wedged mid-FFI against a degraded store. Test-only; the
// caller MUST release before test end or the blocked goroutine leaks.
func (b *Backing) HangOpenReplica(ru storageunit.ReplicaUnit) (release func()) {
	ch := make(chan struct{})
	b.openHangs.Store(ru, ch)
	var once sync.Once
	return func() {
		once.Do(func() {
			b.openHangs.Delete(ru)
			close(ch)
		})
	}
}

// OpenReplicaStartCount reports how many OpenReplicaUnit calls for ru have
// STARTED across every handle (completed or still in flight). Test-only.
func (b *Backing) OpenReplicaStartCount(ru storageunit.ReplicaUnit) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.openReplicaStarts[ru]
}

// countOpenReplicaStart records one OpenReplicaUnit entry for ru.
func (b *Backing) countOpenReplicaStart(ru storageunit.ReplicaUnit) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.openReplicaStarts[ru]++
}

// SetStrictReadFencing toggles whether a fenced handle also fails READS (Get +
// ScanPrefix) with ErrFenced, modeling real slatedb's close-on-fence. Default
// TRUE (production-shaped). Passing false opts a test into the permissive
// reads-pass-through model; the call site must justify why inline. Test-only.
func (b *Backing) SetStrictReadFencing(on bool) { b.strictReadFencing.Store(on) }

// SetEagerFence toggles fence-at-open-START timing (real slatedb's DbBuilder.Build
// behavior) versus fence-at-completion. When on, OpenReplicaUnit bumps the durable
// writer-epoch (fencing the prior owner) BEFORE it sleeps the acquireDelay, so a
// displaced/draining owner is fenced for the whole modeled mount and cannot serve
// the union - the production timing. Default TRUE (production-shaped). Passing
// false opts a test into the old fence-at-completion timing; the call site must
// justify why inline. Test-only.
func (b *Backing) SetEagerFence(on bool) { b.eagerFence.Store(on) }

// OpenReplicaUnit opens replica position ru.Replica of unit ru.Unit against
// the per-replica shared backing, fencing any prior writer of the SAME
// replica position. It returns a fencedReplicaBackend over the SHARED
// per-replica bytes so a node re-acquiring this replica position sees its
// prior writes, and so its writes start failing the instant a higher-epoch
// owner of the same position acquires it. Distinct replica positions are
// independent stores: opening replica 1 never touches replica 0.
func (h *Handle) OpenReplicaUnit(ru storageunit.ReplicaUnit, epoch storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	// Timeline recording (test-only): every call's (start, end) span, so the
	// handoff-cycle latency pins can assert queued opens chain event-driven.
	openStart := time.Now()
	defer h.recordOpenSpan(ru, openStart)
	h.backing.countOpenReplicaStart(ru)
	// Injected hang (test-only): block at entry until released, modeling an
	// open wedged mid-FFI. Before the fault check so a hang + fault compose
	// as hang-then-fail.
	if ch, ok := h.backing.openHangs.Load(ru); ok {
		<-ch.(chan struct{})
	}
	// Concurrency high-water tracking (test-only, Phase 2g): record the peak
	// number of simultaneously in-flight opens so a test can assert the boot
	// mount runs them in parallel up to its bound. Wraps the whole call (defer)
	// so every return path decrements.
	cur := h.openInFlight.Add(1)
	for {
		m := h.openMaxInFlight.Load()
		if cur <= m || h.openMaxInFlight.CompareAndSwap(m, cur) {
			break
		}
	}
	defer h.openInFlight.Add(-1)

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

	// EAGER FENCE (real slatedb timing, the DEFAULT): bump the durable writer-epoch
	// FIRST, so the prior owner is fenced the instant this slow open begins
	// (DbBuilder.Build fences at open-START), THEN sleep the mount latency. The
	// store is still not returned until after the sleep, so this node stays
	// unmounted (routed ops get the transient) for the whole delay - but the prior
	// owner is ALREADY fenced and cannot serve the union.
	if h.backing.eagerFence.Load() {
		store, opened := h.backing.acquireReplica(ru, epoch)
		if d := h.acquireDelayNanos.Load(); d > 0 {
			time.Sleep(time.Duration(d))
		}
		h.mu.Lock()
		h.openReplica[ru] = opened
		h.mu.Unlock()
		return &fencedReplicaBackend{backing: h.backing, unit: ru, epoch: opened, store: store}, opened, nil
	}

	// OPT-OUT (fence-at-completion, SetEagerFence(false)): simulate object-store
	// open latency to widen the acquiring window (test support for the Phase 2d
	// write-availability gate). The sleep is BEFORE the epoch bump, so the unit is
	// genuinely unmounted (a routed op gets the retryable acquiring-window error)
	// for the whole delay AND the prior owner stays unfenced (the fence-timing
	// fidelity gap the eager default closes).
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
	h.backing.writeServingMarker(ru, epoch, "")
	return nil
}

// WriteServingMarkerFrom is WriteServingMarker carrying the writing node's ID
// (storageunit.AuthoredMarkerFactory). The author is recorded atomically with
// the epoch in the shared Backing, so a draining owner's ReadServingMarkerFrom
// always sees an author that matches the epoch it read.
func (h *Handle) WriteServingMarkerFrom(ru storageunit.ReplicaUnit, epoch storageunit.Epoch, authorID string) error {
	h.backing.writeServingMarker(ru, epoch, authorID)
	return nil
}

// ReadServingMarker reads the durable serving marker for replica ru from the
// shared Backing WITHOUT opening it (v0.8 Phase 2e). It is the point-in-time
// liveness observation the old owner's drainCheck polls: it releases ONLY on
// ok == true AND epoch >= its own open epoch. ok == false means no live owner
// has reached Ready yet, so the old owner stays Draining + keeps serving. It
// cannot fail here, so err is always nil.
func (h *Handle) ReadServingMarker(ru storageunit.ReplicaUnit) (storageunit.Epoch, bool, error) {
	e, _, ok := h.backing.readServingMarker(ru)
	return e, ok, nil
}

// ReadServingMarkerFrom is ReadServingMarker plus the ID of the node that wrote
// the marker (storageunit.AuthoredMarkerFactory). authorID is "" for a marker
// written author-less (the legacy shape), which callers treat as UNKNOWN.
func (h *Handle) ReadServingMarkerFrom(ru storageunit.ReplicaUnit) (storageunit.Epoch, string, bool, error) {
	e, author, ok := h.backing.readServingMarker(ru)
	return e, author, ok, nil
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
// (R>1, v0.8 Phase 2b). Writes fail once a higher-epoch owner of the SAME
// replica position has acquired it; by default (strictReadFencing) reads fail
// too, modeling real slatedb's close-on-fence, and only the permissive opt-out
// lets reads pass through. Independent of the R=1 GenUnit-keyed fence.
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

func (f *fencedReplicaBackend) Get(key []byte) ([]byte, error) {
	if f.backing.strictReadFencing.Load() && f.fenced() {
		return nil, ErrFenced
	}
	return f.store.Get(key)
}

func (f *fencedReplicaBackend) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if f.backing.strictReadFencing.Load() && f.fenced() {
		return nil, ErrFenced
	}
	return f.store.ScanPrefix(prefix)
}

func (f *fencedReplicaBackend) Close() error { return nil }

// Flush implements the OPTIONAL backend.Flusher capability (the cluster's
// displacement flush, v0.8 Phase 2e). The double has no memtable to make
// durable - the shared *memory.Memory IS the durable bytes - so the flush is
// a semantic no-op; it COUNTS the call on the shared backing (test
// observability: the pin test asserts exactly one flush per Owned -> Draining
// edge) and mirrors the real backend's contract by failing once fenced (a
// higher-epoch owner took the database over; the caller treats it as a
// best-effort miss).
func (f *fencedReplicaBackend) Flush() error {
	f.backing.countReplicaFlush(f.unit)
	if f.fenced() {
		return ErrFenced
	}
	return nil
}

// countReplicaFlush records one Flush call against ru (under mu).
func (b *Backing) countReplicaFlush(ru storageunit.ReplicaUnit) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.replicaFlushes[ru]++
}

// ReplicaFlushCount reports how many times ru's backends were asked to
// Flush, across every handle. Test observability for the displacement
// flush's exactly-once-per-transition guarantee.
func (b *Backing) ReplicaFlushCount(ru storageunit.ReplicaUnit) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.replicaFlushes[ru]
}

// Compile-time capability assertions. The author-attribution capability is
// OPTIONAL on the interface, so nothing forces this Handle to keep implementing
// it - and a silent drop would not break the build, it would silently downgrade
// every release gate exercised by the test suite to the epoch-only rule, making
// the routed-successor regression tests pass vacuously.
var (
	_ storageunit.ReplicaBackendFactory = (*Handle)(nil)
	_ storageunit.AuthoredMarkerFactory = (*Handle)(nil)
)
