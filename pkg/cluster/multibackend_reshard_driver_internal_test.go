package cluster

// White-box tests for the v0.9 split driver's deterministic steps. The full
// multi-node convergence (all nodes publishing slot markers, the staggered
// flip, zero acked loss) is the lossless-split oracle (tests/integration).

import (
	"bytes"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestObserveReshard_AdvancesThenEnters pins the agreement->genState handshake: a
// Retarget makes the first tick Advance the agreed epoch, the next tick enter the
// split in-flight (set nextCount) - one observable step per tick.
func TestObserveReshard_AdvancesThenEnters(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")
	c.cfg.ConditionalStore = storageunit.NewMemConditionalStore()
	if err := c.initReshardArbiter(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.arbiter.Retarget(storageunit.MustUnitCount(8)); err != nil {
		t.Fatal(err)
	}

	c.observeReshard() // tick 1: advance agreed 4 -> 8
	if s, _, _ := c.arbiter.Read(); s.Count.N() != 8 {
		t.Fatalf("after tick 1 agreed count = %d, want 8 (advanced)", s.Count.N())
	}
	if !c.genSnapshot().nextCount.IsZero() {
		t.Fatalf("tick 1 should not enter in-flight yet")
	}

	c.observeReshard() // tick 2: enter in-flight (live 4 < agreed 8)
	if nc := c.genSnapshot().nextCount; nc.IsZero() || nc.N() != 8 {
		t.Fatalf("tick 2 should enter in-flight with nextCount 8, got %v", nc)
	}
}

// TestObserveCutoverMarkers_SetsCutOver pins that a node flips exactly the units
// whose durable cut-over marker it observes (the per-node flip, cluster-ordered
// by the durable marker).
func TestObserveCutoverMarkers_SetsCutOver(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")
	c.cfg.ConditionalStore = storageunit.NewMemConditionalStore()
	enterSplit(t, c)

	if err := c.publishCutoverMarker(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := c.publishCutoverMarker(0, 2); err != nil {
		t.Fatal(err)
	}

	c.observeCutoverMarkers(c.genSnapshot())
	got := c.genSnapshot()
	if !got.hasCutOver(1) || !got.hasCutOver(2) {
		t.Fatalf("units 1 and 2 should be flipped, cutOver=%v", got.cutOver)
	}
	if got.hasCutOver(0) || got.hasCutOver(3) {
		t.Fatalf("units 0 and 3 have no marker and must NOT be flipped, cutOver=%v", got.cutOver)
	}
}

// TestFinalizeSplit_AdvancesAndRetiresParents pins that once every unit has cut
// over, finalize advances the live generation and retires the obsolete parent
// mounts.
func TestFinalizeSplit_AdvancesAndRetiresParents(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")
	enterSplit(t, c)

	parentRU := mountParentAndChildren(t, c, 0, storageunit.MustUnitCount(4))
	pb, _ := c.mounts.backendFor(parentRU)
	for _, k := range keysInUnit(t, c, 0, storageunit.MustUnitCount(4), 4) {
		env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1, NodeID: "n1"}, Payload: []byte("x")})
		if err := pb.Put(k, env); err != nil {
			t.Fatal(err)
		}
	}

	// Mark every old unit cut over (the allCutOver finalize gate).
	next := c.genSnapshot().clone()
	for _, u := range next.count.IDs() {
		next.cutOver[u] = struct{}{}
	}
	c.commitGenState(next)

	c.finalizeReshard(c.genSnapshot())

	after := c.genSnapshot()
	if after.gen != 1 || after.count.N() != 8 || !after.nextCount.IsZero() || len(after.cutOver) != 0 {
		t.Fatalf("after finalize genState = %+v, want gen1 count8 nextCount0 cutOver{}", after)
	}
	_, stillMounted := c.mounts.backendFor(parentRU)
	if stillMounted {
		t.Fatalf("parent %v should be retired after finalize", parentRU)
	}
}

// TestFinalizeSplit_QuiescesParentWrites is the regression pin for the P0
// acked-write-loss the adversarial review found: a lagging-node parent-leg write
// that lands AFTER finalize's final copy but BEFORE the parent is retired must
// not be silently dropped. The retire boundary must be write-quiesced (a per-unit
// pause): the write is either captured into the child or rejected (the apply
// fails on the quiesced+retired parent so it was never acked) - never
// applied-to-parent-then-vanished. The TestingFinalizeRetireDelay widens the
// window so the race is deterministic; without the pause this FAILS, with it the
// injected write is blocked then rejected.
func TestFinalizeSplit_QuiescesParentWrites(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")
	enterSplit(t, c)
	gs := c.genSnapshot()
	parentRU := mountParentAndChildren(t, c, 0, storageunit.MustUnitCount(4))
	parentB, _ := c.mounts.backendFor(parentRU)

	for _, k := range keysInUnit(t, c, 0, storageunit.MustUnitCount(4), 4) {
		env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1, NodeID: "n1"}, Payload: []byte("base")})
		if err := parentB.Put(k, env); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.copyParentUntilCaughtUp(parentRU, gs); err != nil {
		t.Fatal(err)
	}

	// A key in unit 0 (whichever child it lands in); grab that child's backend.
	xKey := keysInUnit(t, c, 0, storageunit.MustUnitCount(4), 1)[0]
	h := storageunit.HashShardKey(c.shardKey(xKey))
	childRU := storageunit.NewReplicaUnit(storageunit.NewGenUnit(1, storageunit.UnitForHash(h, storageunit.MustUnitCount(8))), 0)
	childB, _ := c.mounts.backendFor(childRU)

	next := gs.clone()
	for _, u := range next.count.IDs() {
		next.cutOver[u] = struct{}{}
	}
	c.commitGenState(next)
	c.cfg.TestingFinalizeRetireDelay = 300 * time.Millisecond

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.finalizeReshard(c.genSnapshot())
	}()
	time.Sleep(80 * time.Millisecond) // let the final copy finish + enter the widened window

	// Inject the lagging-node parent-leg write through the PRODUCTION apply path
	// (resolve + apply), which is what the dual-write fan-out uses. With the
	// quiesce it blocks until the parent retires, then resolves !ok (transient);
	// without it the write lands on the about-to-retire parent and is lost.
	xEnv := Encode(Envelope{Stamp: Stamp{TimestampNanos: 99, NodeID: "nLagger"}, Payload: []byte("X")})
	applyErr := c.resolveAndApplyReplicaPut(parentRU, xKey, xEnv)
	<-done
	_ = parentB

	got, getErr := childB.Get(xKey)
	inChild := false
	if getErr == nil {
		d, _ := Decode(got)
		inChild = bytes.Equal(d.Payload, []byte("X"))
	}
	if applyErr == nil && !inChild {
		t.Fatalf("P0: a parent write applied (would ack) but is ABSENT from the child after finalize - acked-write LOST (retire boundary not quiesced)")
	}
}
