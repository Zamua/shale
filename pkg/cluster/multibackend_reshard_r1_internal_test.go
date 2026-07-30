package cluster

// White-box tests for the R=1 arbiter-driven reshard's two safety pieces
// (multibackend_reshard_r1.go), pinned DETERMINISTICALLY - no timing luck:
//
//  1. THE WRITE CUT-OVER BOUNDARY. bisectUnitOnlineR1's pause-held
//     clear+copy+flip must DRAIN an in-flight writer (one that resolved the
//     parent backend under the pause READ side, exactly as
//     withLocalWriteBackend / localBeginForKey do) before the flip commits,
//     so the drained write is captured by the clear+copy and survives into
//     the gen-(g+1) child. Neutralizing the pause WRITE-side hold makes this
//     test fail: the flip lands while the writer is still in flight and the
//     writer's ack'd bytes stay in the parent, invisible at gen g+1.
//
//  2. PARENT-ANCHORED PLACEMENT. While an R=1 split is in flight,
//     placementGenUnitForKey must keep resolving the PARENT GenUnit{g, K} -
//     ignoring cutOver - so every node forwards a splitting unit's key-space
//     to the SAME node (its gen-g owner) for the whole window, while only the
//     owner's LOCAL resolution (genUnitForKey) honors the flip. Neutralizing
//     the anchor (resolving cutOver for placement) makes this test fail.
//
// Both drive the unexported internals directly (package cluster) because the
// interleavings they pin are exactly the ones an end-to-end run only hits by
// races; the integration gates (tests/integration/lossless_multinode_reshard_
// gate_test.go) cover the same invariant end to end under real concurrency.

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// keysInSameUnit returns two distinct keys that both resolve (steady state,
// gen 0) to the SAME unit, plus that unit's id. Deterministic scan.
func keysInSameUnit(t *testing.T, c *Cluster) (string, string, storageunit.UnitID) {
	t.Helper()
	first := ""
	var unit storageunit.UnitID
	for i := range 100000 {
		k := fmt.Sprintf("r1cut-%d", i)
		gu := c.genUnitForKey([]byte(k))
		if first == "" {
			first, unit = k, gu.ID
			continue
		}
		if gu.ID == unit {
			return first, k, unit
		}
	}
	t.Fatal("no two keys found sharing a unit")
	return "", "", 0
}

// enterR1Split moves the cluster into an in-flight R=1 split (nextCount =
// 2*count) exactly the way observeReshard's reshardGenStep does when the
// arbiter's agreed count runs ahead: clone, set nextCount, commit.
func enterR1Split(t *testing.T, c *Cluster) genState {
	t.Helper()
	gs := c.genSnapshot().clone()
	nc, err := gs.count.Double()
	if err != nil {
		t.Fatalf("double %s: %v", gs.count, err)
	}
	gs.nextCount = nc
	c.commitGenState(gs)
	return c.genSnapshot()
}

// TestR1SplitCutoverBoundary_DrainsInflightWriteIntoChild pins THE WRITE
// CUT-OVER BOUNDARY of the R=1 multi-node reshard (the single most
// safety-critical piece of the freeze-barrier retirement): an in-flight write
// that resolved the PARENT backend under the pause READ side (the production
// write discipline) must be DRAINED by the cut-over's pause WRITE-side hold,
// captured by the pause-held clear+copy, and be readable at gen g+1. The
// sequence is fully controlled - resolve, hold, start cut-over, observe the
// cut-over BLOCKED, release, observe the flip + the write in the CHILD - so a
// regression fails deterministically, not probabilistically.
func TestR1SplitCutoverBoundary_DrainsInflightWriteIntoChild(t *testing.T) {
	// Keep the periodic reconcile out of the controlled sequence below (it
	// could otherwise mount/copy units mid-test). Restored via Cleanup; the
	// ticker latches the value at Open.
	oldInterval := reconcileInterval
	reconcileInterval = time.Hour
	t.Cleanup(func() { reconcileInterval = oldInterval })

	const n = 4
	c, _ := openReshardCluster(t, n)

	// Wire the conditional store AFTER Open, straight onto cfg: the marker
	// publish (the flip's durable signal) needs it, while leaving c.arbiter
	// nil keeps observeReshard inert so nothing races the controlled bisect.
	store := storageunit.NewMemConditionalStore()
	c.cfg.ConditionalStore = store

	baseKey, inflightKey, unit := keysInSameUnit(t, c)
	baseVal := []byte("baseline-before-split")
	inflightVal := []byte("acked-across-the-cutover")

	// Baseline write through the public surface (pre-split data the copy must
	// carry into the child).
	if err := c.Put([]byte(baseKey), baseVal); err != nil {
		t.Fatalf("baseline Put: %v", err)
	}

	gs := enterR1Split(t, c)

	// Vacuity guard: the migrated R=1 branch must be the active one - an
	// in-flight split (nextCount set) on a non-replica layout.
	if c.replicaLayout() || gs.nextCount.IsZero() {
		t.Fatalf("not on the R=1 in-flight branch: replicaLayout=%v nextCount=%v", c.replicaLayout(), gs.nextCount)
	}

	// WRITER IN FLIGHT: resolve the write backend under the pause READ side -
	// byte-for-byte what withLocalWriteBackend does - then hold the apply so
	// the write is mid-flight when the cut-over wants the WRITE side.
	parentBE, ok := c.mounts.backendFor(replica0(storageunit.NewGenUnit(gs.gen, unit)))
	if !ok {
		t.Fatalf("parent unit %d not mounted", unit)
	}
	resolved := make(chan struct{})
	release := make(chan struct{})
	writerDone := make(chan error, 1)
	var writerBE backend.Backend
	go func() {
		be, _, unlock, ok := c.localWriteBackendForKey([]byte(inflightKey))
		if !ok {
			unlock()
			close(resolved)
			writerDone <- fmt.Errorf("writer: unit %d not mounted for local write", unit)
			return
		}
		writerBE = be
		close(resolved)
		<-release
		err := be.Put([]byte(inflightKey), inflightVal)
		unlock()
		writerDone <- err
	}()
	<-resolved

	// Vacuity guard: the writer resolved the PARENT mount (same decorated
	// handle), i.e. it is exactly the pre-flip writer the boundary must drain.
	if writerBE != parentBE {
		t.Fatalf("in-flight writer did not resolve the parent backend (resolved %T)", writerBE)
	}

	// Start the cut-over for the unit while the writer is in flight.
	bisectDone := make(chan error, 1)
	go func() { bisectDone <- c.bisectUnitOnlineR1(gs, unit) }()

	// THE BOUNDARY: with a writer holding the pause READ side, the cut-over
	// (which needs the WRITE side for clear+copy+flip) must NOT complete.
	var bisectErr error
	bisectFired := false
	select {
	case bisectErr = <-bisectDone:
		bisectFired = true
		t.Errorf("BOUNDARY VIOLATED: cut-over of unit %d completed (err=%v) while a write was in flight - the pause WRITE-side hold is not draining writers", unit, bisectErr)
	case <-time.After(200 * time.Millisecond):
		// Blocked, as required.
	}
	if c.genSnapshot().hasCutOver(unit) {
		t.Errorf("BOUNDARY VIOLATED: cutOver committed for unit %d while an in-flight writer held the pause read side", unit)
	}

	// Release the writer: it applies to its resolved (parent) backend and
	// drops the pause; the cut-over must then drain it and proceed.
	close(release)
	if err := <-writerDone; err != nil {
		t.Fatalf("in-flight write failed: %v", err)
	}
	if !bisectFired {
		select {
		case bisectErr = <-bisectDone:
		case <-time.After(10 * time.Second):
			t.Fatal("cut-over did not complete after the in-flight writer released the pause")
		}
	}
	if bisectErr != nil {
		t.Fatalf("bisectUnitOnlineR1(%d): %v", unit, bisectErr)
	}

	// The flip committed and published its durable marker (the cross-node
	// finalize signal peers converge on).
	post := c.genSnapshot()
	if !post.hasCutOver(unit) {
		t.Fatalf("unit %d not in the cutOver set after bisect", unit)
	}
	present, err := c.cutoverMarkerPresent(gs.gen, unit)
	if err != nil || !present {
		t.Fatalf("durable cut-over marker for gen %d unit %d: present=%v err=%v", gs.gen, unit, present, err)
	}

	// THE ORACLE: both the baseline write and the DRAINED in-flight write are
	// readable post-flip (the read path now resolves the gen-(g+1) child)...
	for _, kv := range []struct {
		k string
		v []byte
	}{{baseKey, baseVal}, {inflightKey, inflightVal}} {
		got, err := c.Get([]byte(kv.k))
		if err != nil {
			t.Fatalf("ORACLE VIOLATED: %q unreadable after the cut-over: %v (acked write lost at the boundary)", kv.k, err)
		}
		if !bytes.Equal(got, kv.v) {
			t.Fatalf("ORACLE VIOLATED: %q = %q after the cut-over, want %q", kv.k, got, kv.v)
		}
	}

	// ...and PHYSICALLY live in the gen-(g+1) CHILD database (not merely
	// readable out of the still-mounted parent): the clear+copy captured the
	// drained write. This is what makes the finalize's no-final-copy parent
	// retire safe.
	for _, kv := range []struct {
		k string
		v []byte
	}{{baseKey, baseVal}, {inflightKey, inflightVal}} {
		childGU := c.genUnitForKey([]byte(kv.k))
		if childGU.Gen != gs.gen+1 {
			t.Fatalf("post-flip resolve of %q = %s, want a gen-%d child", kv.k, childGU, gs.gen+1)
		}
		cbe, ok := c.mounts.backendFor(replica0(childGU))
		if !ok {
			t.Fatalf("child %s not mounted after bisect", childGU)
		}
		got, err := cbe.Get([]byte(kv.k))
		if err != nil {
			t.Fatalf("ORACLE VIOLATED: %q absent from child %s: %v (the pause-held clear+copy did not capture it; it would be lost when finalize retires the parent)", kv.k, childGU, err)
		}
		if !bytes.Equal(got, kv.v) {
			t.Fatalf("ORACLE VIOLATED: %q in child %s = %q, want %q", kv.k, childGU, got, kv.v)
		}
	}
	t.Logf("drained in-flight write %q captured in child %s across the cut-over of unit %d", inflightKey, c.genUnitForKey([]byte(inflightKey)), unit)
}

// TestR1PlacementParentAnchored_InFlightSplit pins PARENT-ANCHORED PLACEMENT,
// the other half of the migrated cut-over boundary: for the WHOLE in-flight
// window of an R=1 split - even after a unit has flipped locally - OWNERSHIP
// placement (placementGenUnitForKey, used by unitOwnerOf for forwarding) must
// keep resolving the PARENT GenUnit{g, K}, while the LOCAL resolve
// (genUnitForKey) honors the flip. That divergence is what funnels every
// node's writes for a splitting unit to its one gen-g owner regardless of
// which cut-over markers each node has observed; anchoring must then RELEASE
// at finalize (steady state resolves the children for both).
func TestR1PlacementParentAnchored_InFlightSplit(t *testing.T) {
	const n = 4
	c, _ := openReshardCluster(t, n)

	key, _, unit := keysInSameUnit(t, c)
	parent := storageunit.NewGenUnit(0, unit)

	// Steady state: placement == local resolve == the gen-0 unit.
	if got := c.placementGenUnitForKey([]byte(key)); got != parent {
		t.Fatalf("steady placement = %s, want %s", got, parent)
	}
	if got := c.genUnitForKey([]byte(key)); got != parent {
		t.Fatalf("steady local resolve = %s, want %s", got, parent)
	}

	// In flight, BEFORE the flip: both still resolve the parent.
	enterR1Split(t, c)
	if got := c.placementGenUnitForKey([]byte(key)); got != parent {
		t.Fatalf("in-flight pre-flip placement = %s, want parent %s", got, parent)
	}
	if got := c.genUnitForKey([]byte(key)); got != parent {
		t.Fatalf("in-flight pre-flip local resolve = %s, want parent %s", got, parent)
	}

	// Flip the unit locally (what the owner's bisect commits under the pause).
	flipped := c.genSnapshot().clone()
	flipped.cutOver[unit] = struct{}{}
	c.commitGenState(flipped)

	// Vacuity guard + the split-brain hazard the anchor removes: the LOCAL
	// resolve honors the flip (gen-1 child)...
	child := c.genUnitForKey([]byte(key))
	if child.Gen != 1 {
		t.Fatalf("post-flip local resolve = %s, want a gen-1 child", child)
	}
	// ...so placement WOULD diverge across nodes here if it honored cutOver
	// (nodes observe markers at different times). The anchor: placement still
	// resolves the PARENT for the whole in-flight window.
	if got := c.placementGenUnitForKey([]byte(key)); got != parent {
		t.Fatalf("ANCHOR VIOLATED: in-flight post-flip placement = %s, want parent %s (a splitting unit's key-space must keep funneling to its gen-g owner)", got, parent)
	}

	// FINALIZE (what observeReshard commits once every unit cut over): the
	// anchor must release - steady state at gen 1 resolves the child for
	// placement and local resolve alike (a sticky anchor would mis-route
	// forever).
	fin := genState{
		gen:     1,
		count:   c.genSnapshot().nextCount,
		cutOver: make(map[storageunit.UnitID]struct{}),
	}
	c.commitGenState(fin)
	if got := c.placementGenUnitForKey([]byte(key)); got != child {
		t.Fatalf("post-finalize placement = %s, want child %s (anchor did not release)", got, child)
	}
	if got := c.genUnitForKey([]byte(key)); got != child {
		t.Fatalf("post-finalize local resolve = %s, want child %s", got, child)
	}
}
