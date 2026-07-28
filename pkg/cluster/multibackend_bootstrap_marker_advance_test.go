package cluster

// White-box tests for the reshard-finalize advance of the __cluster/init marker
// (#460): a resharded homogeneous cluster must record its FINALIZED generation in
// the durable marker so a full-cluster restart resumes it instead of re-forming
// gen 0. They drive advanceClusterInitMarker + finalizeReshard directly against
// the in-process MemConditionalStore double - no membership / gRPC / slatedb - to
// pin the CAS properties (monotone, idempotent, concurrent-safe, no-op when
// absent/unwired) and the finalize hook (advance BEFORE the gen commit) in
// isolation. The full split/merge path is the lossless decentralized-split gate.

import (
	"errors"
	"sync"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// readMarker decodes the current __cluster/init record, failing the test if it is
// absent or corrupt.
func readMarker(t *testing.T, cs storageunit.ConditionalStore) clusterInitRecord {
	t.Helper()
	data, _, err := cs.Get(clusterInitMarkerKey)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	rec, err := decodeClusterInit(data)
	if err != nil {
		t.Fatalf("decode marker: %v", err)
	}
	return rec
}

// seedMarker founds the marker at {gen, count} as the founder's PutIfAbsent would.
func seedMarker(t *testing.T, cs storageunit.ConditionalStore, gen uint64, count uint32) {
	t.Helper()
	payload, err := encodeClusterInit(clusterInitRecord{Gen: gen, Count: count})
	if err != nil {
		t.Fatalf("encode seed: %v", err)
	}
	if _, err := cs.PutIfAbsent(clusterInitMarkerKey, payload); err != nil {
		t.Fatalf("seed marker: %v", err)
	}
}

// TestAdvanceClusterInitMarker_AdvancesExistingMarker pins the happy path: a
// founded marker at gen 0 advances to the finalized {gen+1, newCount}.
func TestAdvanceClusterInitMarker_AdvancesExistingMarker(t *testing.T) {
	cs := storageunit.NewMemConditionalStore()
	seedMarker(t, cs, 0, 4)
	c := &Cluster{}
	c.cfg.ConditionalStore = cs

	if err := c.advanceClusterInitMarker(1, storageunit.MustUnitCount(8)); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if rec := readMarker(t, cs); rec.Gen != 1 || rec.Count != 8 {
		t.Fatalf("want {gen:1,count:8}, got {gen:%d,count:%d}", rec.Gen, rec.Count)
	}
}

// TestAdvanceClusterInitMarker_MonotoneNeverRegresses pins that the advance never
// LOWERS the marker: a stale finalizer trying to set an older generation (or the
// same generation) is a no-op, so a fast cluster that has already moved on is
// never dragged back.
func TestAdvanceClusterInitMarker_MonotoneNeverRegresses(t *testing.T) {
	cs := storageunit.NewMemConditionalStore()
	seedMarker(t, cs, 5, 64)
	c := &Cluster{}
	c.cfg.ConditionalStore = cs

	// A lower target generation must not regress the marker.
	if err := c.advanceClusterInitMarker(1, storageunit.MustUnitCount(8)); err != nil {
		t.Fatalf("advance(lower): %v", err)
	}
	if rec := readMarker(t, cs); rec.Gen != 5 || rec.Count != 64 {
		t.Fatalf("regressed on a lower gen: got {gen:%d,count:%d}", rec.Gen, rec.Count)
	}
	// The SAME generation (with a different count) is also a no-op: gen is the
	// monotone authority, count is its paired value.
	if err := c.advanceClusterInitMarker(5, storageunit.MustUnitCount(16)); err != nil {
		t.Fatalf("advance(equal): %v", err)
	}
	if rec := readMarker(t, cs); rec.Gen != 5 || rec.Count != 64 {
		t.Fatalf("changed on an equal gen: got {gen:%d,count:%d}", rec.Gen, rec.Count)
	}
}

// TestAdvanceClusterInitMarker_IdempotentUnderConcurrency pins that many nodes
// finalizing the same generation concurrently produce exactly one advance and no
// error: the CAS loop observes a concurrent winner and returns success.
func TestAdvanceClusterInitMarker_IdempotentUnderConcurrency(t *testing.T) {
	cs := storageunit.NewMemConditionalStore()
	seedMarker(t, cs, 0, 4)

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := &Cluster{}
			c.cfg.ConditionalStore = cs
			<-start
			errs[i] = c.advanceClusterInitMarker(1, storageunit.MustUnitCount(8))
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("node %d advance: %v", i, err)
		}
	}
	if rec := readMarker(t, cs); rec.Gen != 1 || rec.Count != 8 {
		t.Fatalf("want {gen:1,count:8} after concurrent advance, got {gen:%d,count:%d}", rec.Gen, rec.Count)
	}
}

// TestAdvanceClusterInitMarker_AbsentMarkerNoOp pins that the advance never
// CREATES a marker: a cluster founded via the legacy seed-RPC path (no
// __cluster/init) stays markerless, so the reshard path does not accidentally
// give a non-homogeneous cluster a bootstrap marker.
func TestAdvanceClusterInitMarker_AbsentMarkerNoOp(t *testing.T) {
	cs := storageunit.NewMemConditionalStore()
	c := &Cluster{}
	c.cfg.ConditionalStore = cs

	if err := c.advanceClusterInitMarker(1, storageunit.MustUnitCount(8)); err != nil {
		t.Fatalf("advance(absent): %v", err)
	}
	if _, _, err := cs.Get(clusterInitMarkerKey); !errors.Is(err, storageunit.ErrCondNotFound) {
		t.Fatalf("advance created a marker where none existed (err=%v)", err)
	}
}

// TestAdvanceClusterInitMarker_NilStoreNoOp pins that a cluster without a
// ConditionalStore (legacy / single-backend) advances to a clean no-op, so the
// finalize hook is inert there.
func TestAdvanceClusterInitMarker_NilStoreNoOp(t *testing.T) {
	c := &Cluster{}
	if err := c.advanceClusterInitMarker(1, storageunit.MustUnitCount(8)); err != nil {
		t.Fatalf("advance(nil store): %v", err)
	}
}

// TestFinalizeReshard_AdvancesMarkerBeforeCommit pins the hook: finalizeReshard
// advances the durable marker to {gen+1, nextCount} as part of committing the new
// generation. The cluster is set up mid-reshard (nextCount set, every unit cut
// over) with NO mounted parents, so the parent-retire loop is empty and finalize
// reaches its generation commit - exactly the path that must also carry the
// marker forward.
func TestFinalizeReshard_AdvancesMarkerBeforeCommit(t *testing.T) {
	cs := storageunit.NewMemConditionalStore()
	seedMarker(t, cs, 0, 4)

	c := &Cluster{unitCount: storageunit.MustUnitCount(4)}
	c.cfg.ConditionalStore = cs

	// Mid-reshard: gen 0, count 4, splitting to 8, every old unit cut over.
	cutOver := make(map[storageunit.UnitID]struct{})
	for _, k := range storageunit.MustUnitCount(4).IDs() {
		cutOver[k] = struct{}{}
	}
	c.commitGenState(genState{
		gen:       0,
		count:     storageunit.MustUnitCount(4),
		nextCount: storageunit.MustUnitCount(8),
		cutOver:   cutOver,
	})

	c.finalizeReshard(c.genSnapshot())

	if rec := readMarker(t, cs); rec.Gen != 1 || rec.Count != 8 {
		t.Fatalf("marker not advanced by finalize: want {gen:1,count:8}, got {gen:%d,count:%d}", rec.Gen, rec.Count)
	}
	if gs := c.genSnapshot(); gs.gen != 1 || gs.count.N() != 8 || !gs.nextCount.IsZero() {
		t.Fatalf("local gen not finalized: got {gen:%d,count:%d,nextCount:%d}", gs.gen, gs.count.N(), gs.nextCount.N())
	}
}

// TestFinalizeReshard_AdvancesMarkerOnMerge pins that a MERGE advances the marker
// identically to a split: finalize is direction-agnostic (gen+1, count=nextCount),
// so a merge records the new LOWER count. Here gen 0 / count 4 merges to count 2.
func TestFinalizeReshard_AdvancesMarkerOnMerge(t *testing.T) {
	cs := storageunit.NewMemConditionalStore()
	seedMarker(t, cs, 0, 4)

	c := &Cluster{unitCount: storageunit.MustUnitCount(4)}
	c.cfg.ConditionalStore = cs
	cutOver := make(map[storageunit.UnitID]struct{})
	for _, k := range storageunit.MustUnitCount(4).IDs() {
		cutOver[k] = struct{}{}
	}
	c.commitGenState(genState{
		gen:       0,
		count:     storageunit.MustUnitCount(4),
		nextCount: storageunit.MustUnitCount(2), // merge 4 -> 2
		cutOver:   cutOver,
	})

	c.finalizeReshard(c.genSnapshot())

	if rec := readMarker(t, cs); rec.Gen != 1 || rec.Count != 2 {
		t.Fatalf("merge: want {gen:1,count:2}, got {gen:%d,count:%d}", rec.Gen, rec.Count)
	}
}

// TestFinalizeReshard_DefersWhenMarkerAdvanceFails pins the safety ordering: if
// the marker advance fails (a corrupt marker stands in for a store error here),
// finalize must NOT advance the local generation - leaving the parents resumable
// at gen-g - so the next reconcile tick retries rather than retiring parents
// against a stale marker.
func TestFinalizeReshard_DefersWhenMarkerAdvanceFails(t *testing.T) {
	cs := storageunit.NewMemConditionalStore()
	// A corrupt marker makes advanceClusterInitMarker return an error (decode
	// failure), standing in for any transient store failure.
	if _, err := cs.PutIfAbsent(clusterInitMarkerKey, []byte("not-json")); err != nil {
		t.Fatalf("seed corrupt marker: %v", err)
	}

	c := &Cluster{unitCount: storageunit.MustUnitCount(4)}
	c.cfg.ConditionalStore = cs
	cutOver := make(map[storageunit.UnitID]struct{})
	for _, k := range storageunit.MustUnitCount(4).IDs() {
		cutOver[k] = struct{}{}
	}
	c.commitGenState(genState{
		gen:       0,
		count:     storageunit.MustUnitCount(4),
		nextCount: storageunit.MustUnitCount(8),
		cutOver:   cutOver,
	})

	c.finalizeReshard(c.genSnapshot())

	if gs := c.genSnapshot(); gs.gen != 0 || gs.nextCount.IsZero() {
		t.Fatalf("finalize advanced the generation despite a failed marker write: got {gen:%d,nextCount:%d}", gs.gen, gs.nextCount.N())
	}
}

// flakyCondStore wraps a ConditionalStore and fails the first failGets Get calls
// with a transient error, to exercise the self-healing retry after a TRANSIENT
// (not permanent) marker-read failure - the recover-then-succeed half the corrupt-
// marker defer test cannot reach.
type flakyCondStore struct {
	storageunit.ConditionalStore
	failGets int
}

func (f *flakyCondStore) Get(key string) ([]byte, string, error) {
	if f.failGets > 0 {
		f.failGets--
		return nil, "", errors.New("transient store error")
	}
	return f.ConditionalStore.Get(key)
}

// TestFinalizeReshard_SelfHealsAfterTransientMarkerFailure pins the self-healing
// the docs promise: a TRANSIENT marker-write failure defers the finalize (tick 1),
// and a later tick (the store having recovered) advances the marker AND commits
// the generation (tick 2). This is the recover-then-succeed transition that the
// permanently-corrupt-marker defer test cannot exercise.
func TestFinalizeReshard_SelfHealsAfterTransientMarkerFailure(t *testing.T) {
	mem := storageunit.NewMemConditionalStore()
	seedMarker(t, mem, 0, 4)
	flaky := &flakyCondStore{ConditionalStore: mem, failGets: 1}

	c := &Cluster{unitCount: storageunit.MustUnitCount(4)}
	c.cfg.ConditionalStore = flaky
	cutOver := make(map[storageunit.UnitID]struct{})
	for _, k := range storageunit.MustUnitCount(4).IDs() {
		cutOver[k] = struct{}{}
	}
	c.commitGenState(genState{
		gen:       0,
		count:     storageunit.MustUnitCount(4),
		nextCount: storageunit.MustUnitCount(8),
		cutOver:   cutOver,
	})

	// Tick 1: the marker read fails transiently -> finalize defers; nothing moves.
	c.finalizeReshard(c.genSnapshot())
	if gs := c.genSnapshot(); gs.gen != 0 || gs.nextCount.IsZero() {
		t.Fatalf("tick 1 must defer: got {gen:%d,nextCount:%d}", gs.gen, gs.nextCount.N())
	}
	if rec := readMarker(t, mem); rec.Gen != 0 {
		t.Fatalf("tick 1 must not advance the marker: got gen %d", rec.Gen)
	}

	// Tick 2: the store has recovered -> finalize advances the marker + commits gen+1.
	c.finalizeReshard(c.genSnapshot())
	if rec := readMarker(t, mem); rec.Gen != 1 || rec.Count != 8 {
		t.Fatalf("tick 2 must advance: got {gen:%d,count:%d}", rec.Gen, rec.Count)
	}
	if gs := c.genSnapshot(); gs.gen != 1 || !gs.nextCount.IsZero() {
		t.Fatalf("tick 2 must finalize: got {gen:%d,nextCount:%d}", gs.gen, gs.nextCount.N())
	}
}

// TestFinalizeSplit_AdvancesMarkerWithRealParents is the end-to-end proof that the
// marker advance coexists with the REAL parent retire: a replicated cluster mid-
// split, with a founded marker, finalizes - advancing the durable marker to the
// finalized generation AND retiring its owned parent in the same finalize. It
// reuses the split driver's harness (the sibling TestFinalizeSplit_Advances-
// AndRetiresParents, which has NO marker, pins the retire alone).
func TestFinalizeSplit_AdvancesMarkerWithRealParents(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")
	c.cfg.ConditionalStore = storageunit.NewMemConditionalStore()
	seedMarker(t, c.cfg.ConditionalStore, 0, 4)
	enterSplit(t, c)

	parentRU := mountParentAndChildren(t, c, 0, storageunit.MustUnitCount(4))
	pb, _ := c.mounts.backendFor(parentRU)
	for _, k := range keysInUnit(t, c, 0, storageunit.MustUnitCount(4), 4) {
		env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1, NodeID: "n1"}, Payload: []byte("x")})
		if err := pb.Put(k, env); err != nil {
			t.Fatal(err)
		}
	}
	next := c.genSnapshot().clone()
	for _, u := range next.count.IDs() {
		next.cutOver[u] = struct{}{}
	}
	c.commitGenState(next)

	c.finalizeReshard(c.genSnapshot())

	if rec := readMarker(t, c.cfg.ConditionalStore); rec.Gen != 1 || rec.Count != 8 {
		t.Fatalf("marker not advanced through a real split: want {gen:1,count:8}, got {gen:%d,count:%d}", rec.Gen, rec.Count)
	}
	_, stillMounted := c.mounts.backendFor(parentRU)
	if stillMounted {
		t.Fatalf("parent %v should be retired after finalize", parentRU)
	}
	if gs := c.genSnapshot(); gs.gen != 1 || gs.count.N() != 8 {
		t.Fatalf("local gen not finalized: got {gen:%d,count:%d}", gs.gen, gs.count.N())
	}
}
