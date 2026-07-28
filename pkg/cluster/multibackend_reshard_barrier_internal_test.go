package cluster

// White-box tests for the v0.8 multi-node reshard barrier (cluster-wide freeze).
// They live in package cluster so they can drive the unexported per-node phase
// handlers (reshardFreeze / reshardBisect / reshardFlip / reshardAbort), inspect
// the mount map + the gen-(g+1) child stores directly, and exercise the
// stale-freeze self-heal + membership-identity guards. The full coordinated
// cross-node flow is covered end to end in tests/integration; these pin the
// per-node invariants the review found gaps in.

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestBisectStatic_DrainsInFlightWriterViaWritePause is the P1-A regression: the
// freeze flag flip alone does not quiesce an in-flight writer (one past the gate
// before the flip is mid-Put under the per-old-unit write-pause READ side). The
// bisect must take that pause's WRITE side around the copy, which BLOCKS until
// every in-flight writer releases. We prove the WRITE-lock is actually taken: a
// goroutine holds the old-unit pause READ side (modeling an in-flight writer);
// reshardBisect must not complete until we release it. Without the drain the
// bisect would finish immediately while the "writer" still holds the read side.
func TestBisectStatic_DrainsInFlightWriterViaWritePause(t *testing.T) {
	const n = 4
	c, _ := openReshardCluster(t, n)

	// Seed so every unit has data to bisect.
	for i := range 40 {
		if err := c.Put(fmt.Appendf(nil, "k-%03d", i), []byte("v")); err != nil {
			t.Fatalf("seed Put: %v", err)
		}
	}

	// Pick an old unit id K this node mounts and grab its write-pause READ side -
	// the same lock localWriteBackendForKey holds across an in-flight Put.
	old := c.mountedOldUnits(0)
	if len(old) == 0 {
		t.Fatal("no gen-0 units mounted")
	}
	k := old[0]
	pause := c.pauseLockFor(k)
	pause.RLock() // model an in-flight writer to K's key-space

	if err := c.reshardFreeze(1); err != nil {
		pause.RUnlock()
		t.Fatalf("freeze: %v", err)
	}

	var bisectDone atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		// bisectUnitStatic for unit k must block on pause.Lock() until the
		// "writer" releases. (reshardBisect bisects every mounted old unit; the
		// one for k blocks the whole pass.)
		_ = c.reshardBisect(1)
		bisectDone.Store(true)
	})

	// Give the bisect time to reach (and block on) the WRITE-lock for k.
	time.Sleep(80 * time.Millisecond)
	if bisectDone.Load() {
		pause.RUnlock()
		wg.Wait()
		t.Fatal("P1-A VIOLATED: bisect completed while an in-flight writer still held the old-unit write-pause; the WRITE-side drain is missing")
	}

	// Release the in-flight writer; the bisect must now complete.
	pause.RUnlock()
	wg.Wait()
	if !bisectDone.Load() {
		t.Fatal("bisect did not complete after the in-flight writer released")
	}
	_ = c.reshardAbort(1)
}

// childStoreOf opens (or re-opens) the gen-(g+1) child store for old unit K that
// holds key h's bytes, and reports whether key is present in it. It reads via
// the mount map the bisect populated, so it sees exactly what routing would
// resolve at gen g+1.
func barrierChildHas(t *testing.T, c *Cluster, gen storageunit.Generation, key string, oldCount storageunit.UnitCount) bool {
	t.Helper()
	newCount, err := oldCount.Double()
	if err != nil {
		t.Fatalf("double %s: %v", oldCount, err)
	}
	h := storageunit.HashShardKey(c.shardKey([]byte(key)))
	child := storageunit.UnitForHash(h, newCount)
	gu := storageunit.NewGenUnit(gen+1, child)
	be, ok := c.mounts.backendFor(replica0(gu))
	if !ok {
		t.Fatalf("child store %s not mounted", gu)
	}
	v, err := be.Get([]byte(key))
	return err == nil && v != nil
}

// TestBisectStatic_AbortThenRetryDoesNotResurrectDeletedKey is the P1-B
// regression: an aborted reshard CloseUnit's (but does not wipe) the half-built
// gen-(g+1) children; on a retry, bisectUnitStatic re-OpenUnit's the SAME child
// (still holding the aborted run's bytes). Without the clear-before-copy a key
// DELETED between the aborted run and the retry would survive in the child and
// be resurrected at FLIP. This drives FREEZE -> BISECT -> ABORT, deletes a key,
// then FREEZE -> BISECT again and asserts the deleted key is ABSENT from the
// freshly-rebuilt child.
func TestBisectStatic_AbortThenRetryDoesNotResurrectDeletedKey(t *testing.T) {
	const n = 4
	c, _ := openReshardCluster(t, n)
	oldCount := storageunit.MustUnitCount(n)

	// Seed keys spanning the unit space.
	const nKeys = 80
	keys := make([]string, nKeys)
	for i := range nKeys {
		k := fmt.Sprintf("res-%04d", i)
		keys[i] = k
		if err := c.Put([]byte(k), []byte("v1")); err != nil {
			t.Fatalf("seed Put %q: %v", k, err)
		}
	}

	// Run 1: FREEZE + BISECT, then ABORT (no FLIP). The children are built then
	// discarded (CloseUnit, bytes retained in the shared backing).
	if err := c.reshardFreeze(1); err != nil {
		t.Fatalf("freeze run1: %v", err)
	}
	if err := c.reshardBisect(1); err != nil {
		t.Fatalf("bisect run1: %v", err)
	}
	// The child holds res-0000 right now (built from the seed).
	victim := keys[0]
	if !barrierChildHas(t, c, 0, victim, oldCount) {
		t.Fatalf("precondition: %q absent from its gen-1 child after the first bisect", victim)
	}
	if err := c.reshardAbort(1); err != nil {
		t.Fatalf("abort run1: %v", err)
	}

	// Between the aborted run and the retry, DELETE the victim from the live
	// gen-0 unit (the cluster is back at gen 0, unfrozen).
	if err := c.Delete([]byte(victim)); err != nil {
		t.Fatalf("delete %q between runs: %v", victim, err)
	}

	// Run 2: FREEZE + BISECT again. clearBackend before the copy must drop the
	// stale victim bytes the aborted run left in the child.
	if err := c.reshardFreeze(1); err != nil {
		t.Fatalf("freeze run2: %v", err)
	}
	if err := c.reshardBisect(1); err != nil {
		t.Fatalf("bisect run2: %v", err)
	}

	if barrierChildHas(t, c, 0, victim, oldCount) {
		t.Fatalf("P1-B VIOLATED: deleted key %q resurrected in its gen-1 child after abort+retry (clear-before-copy missing)", victim)
	}
	// A surviving key must still be present.
	if !barrierChildHas(t, c, 0, keys[1], oldCount) {
		t.Fatalf("surviving key %q absent from its gen-1 child after retry", keys[1])
	}
	_ = c.reshardAbort(1) // leave the cluster clean
}

// TestReshardFreeze_TakesOverStrandedFreeze is the P1-C re-freeze fix: a node
// that FLIPPED to gen g+1 but stayed frozen (a dropped RESUME) must let a NEW
// reshard toward g+2 freeze it, rather than refuse because it is "already frozen
// toward g+1". We simulate the stranded state (frozen, gen advanced to the
// target) then assert reshardFreeze(g+2) succeeds and re-targets.
func TestReshardFreeze_TakesOverStrandedFreeze(t *testing.T) {
	c, _ := openReshardCluster(t, 4)

	// FREEZE + FLIP to gen 1 but DO NOT resume: the node is now at gen 1, still
	// frozen toward gen 1 (a stranded freeze - the RESUME was "dropped").
	if err := c.reshardFreeze(1); err != nil {
		t.Fatalf("freeze->1: %v", err)
	}
	if err := c.reshardBisect(1); err != nil {
		t.Fatalf("bisect->1: %v", err)
	}
	if err := c.reshardFlip(1); err != nil {
		t.Fatalf("flip->1: %v", err)
	}
	if c.genSnapshot().gen != 1 {
		t.Fatalf("precondition: gen = %d, want 1 after flip", c.genSnapshot().gen)
	}
	if !c.TestingIsFrozen() {
		t.Fatal("precondition: node should be stranded frozen after flip-without-resume")
	}

	// A new reshard targets gen 2 (curGen+1). reshardFreeze(2) must TAKE OVER the
	// stranded gen-1 freeze, not refuse.
	if err := c.reshardFreeze(2); err != nil {
		t.Fatalf("P1-C VIOLATED: reshardFreeze(2) refused a stranded already-flipped node: %v", err)
	}
	if !c.frozenFor(2) {
		t.Fatalf("P1-C VIOLATED: after take-over the freeze target should be 2, got frozenFor(2)=false")
	}
	_ = c.reshardAbort(2)
}

// TestClearStaleFreeze_UnfreezesFlippedButNotResumedNode is the P1-C self-heal:
// a node FLIPPED to its target but stranded frozen (dropped RESUME) is auto-
// unfrozen by the self-heal once it has aged past staleFreezeGrace. A node still
// AT gen g frozen toward g+1 (mid-bisect) is NOT cleared (clearing it would
// break the barrier).
func TestClearStaleFreeze_UnfreezesFlippedButNotResumedNode(t *testing.T) {
	// Shrink the grace so the test does not sleep for 25s.
	old := staleFreezeGrace
	staleFreezeGrace = 20 * time.Millisecond
	defer func() { staleFreezeGrace = old }()

	// Case A: still AT gen 0, frozen toward gen 1 (mid-FREEZE/BISECT). clearStale
	// must NOT clear it, even after the grace - the node has not flipped.
	{
		ca, _ := openReshardCluster(t, 4)
		if err := ca.reshardFreeze(1); err != nil {
			t.Fatalf("freeze->1: %v", err)
		}
		time.Sleep(40 * time.Millisecond) // age past the grace
		if ca.clearStaleFreeze() {
			t.Fatal("P1-C VIOLATED: clearStaleFreeze cleared a node still AT gen g frozen toward g+1 (mid-bisect); that would break the barrier")
		}
		if !ca.TestingIsFrozen() {
			t.Fatal("node should still be frozen (mid-bisect must not auto-unfreeze)")
		}
		_ = ca.reshardAbort(1)
	}

	// Case B: FLIP to gen 1 (still no resume) on a FRESH cluster so the freeze is
	// young right after the flip. Before the grace clearStale must NOT fire (it
	// would race a normal in-flight RESUME); after the grace it must unfreeze.
	c, _ := openReshardCluster(t, 4)
	if err := c.reshardFreeze(1); err != nil {
		t.Fatalf("freeze->1: %v", err)
	}
	if err := c.reshardBisect(1); err != nil {
		t.Fatalf("bisect->1: %v", err)
	}
	if err := c.reshardFlip(1); err != nil {
		t.Fatalf("flip->1: %v", err)
	}
	// Immediately after flip it is younger than the grace: not yet stale.
	if c.clearStaleFreeze() {
		t.Fatal("clearStaleFreeze fired before the grace elapsed (would race a normal in-flight RESUME)")
	}
	time.Sleep(40 * time.Millisecond) // age past the grace
	if !c.clearStaleFreeze() {
		t.Fatal("P1-C VIOLATED: a flipped-but-stranded node was NOT auto-unfrozen past staleFreezeGrace")
	}
	if c.TestingIsFrozen() {
		t.Fatal("P1-C VIOLATED: node still frozen after a successful clearStaleFreeze")
	}
	// Writes must now resume at gen 1.
	if err := c.Put([]byte("after-heal"), []byte("ok")); err != nil {
		t.Fatalf("write after stale-freeze self-heal should succeed: %v", err)
	}
}

// TestBroadcastFlip_ReportsAckedCountForStraddleDetection is the P2-B fix: the
// FLIP broadcast reports how many nodes flipped before a failure so the
// coordinator can distinguish a clean FLIP failure (acked == 0, nobody advanced)
// from a partial-FLIP STRADDLE (acked > 0). We build a 2-member list where the
// LOCAL node flips successfully (self, applied directly - no RPC) and a bogus
// peer's FLIP RPC fails (unreachable addr), and assert acked == 1 with an error.
func TestBroadcastFlip_ReportsAckedCountForStraddleDetection(t *testing.T) {
	c, _ := openReshardCluster(t, 4)

	// Freeze + bisect the local node so its FLIP succeeds when sendPhase applies
	// it directly (the coordinator is also a participant).
	if err := c.reshardFreeze(1); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	if err := c.reshardBisect(1); err != nil {
		t.Fatalf("bisect: %v", err)
	}

	members := []memberRPC{
		{id: c.cfg.NodeID, addr: c.cfg.GRPCAddr}, // self: flips locally, no RPC
		{id: "ghost", addr: "127.0.0.1:1"},       // unreachable peer: FLIP RPC fails
	}
	acked, err := c.broadcastFlip(members, 1)
	if err == nil {
		t.Fatal("broadcastFlip over an unreachable peer should error")
	}
	if acked != 1 {
		t.Fatalf("P2-B VIOLATED: broadcastFlip acked = %d, want 1 (the self node flipped before the peer failed); the coordinator needs this to detect a straddle", acked)
	}
	// The coordinator wraps acked>0 as ErrReshardStraddle; confirm the sentinel
	// classifies (the coordinator's wrapping uses %w on ErrReshardStraddle).
	straddleErr := fmt.Errorf("cluster: Reshard FLIP barrier failed after %d node(s) flipped: %w: %w", acked, ErrReshardStraddle, err)
	if !errors.Is(straddleErr, ErrReshardStraddle) {
		t.Fatal("P2-B VIOLATED: a partial-FLIP error does not classify as ErrReshardStraddle")
	}
	// Local node did flip to gen 1 (the straddle's surviving advanced node).
	if c.genSnapshot().gen != 1 {
		t.Fatalf("local node should have flipped to gen 1, got %d", c.genSnapshot().gen)
	}
}

// TestMembershipChanged_IdentityNotJustCount is the P2-C fix: the REAL
// membershipChanged compares the member id SET, so a count-preserving swap (X
// leaves, Y joins, count unchanged) is detected as a change. A count-only check
// would miss it and drive a stale set. We drive the actual function by swapping
// the cluster's ring membership between snapshots.
func TestMembershipChanged_IdentityNotJustCount(t *testing.T) {
	c, _ := openReshardCluster(t, 4)
	// Populate a 3-member ring directly (the barrier's membershipChanged reads
	// c.Members(), which reads c.ring).
	c.ring = ring.New()
	for _, id := range []string{"a", "b", "c"} {
		c.ring.Add(ring.Member{ID: id, Addr: id + ":1"})
	}
	want := map[string]struct{}{"a": {}, "b": {}, "c": {}}

	// Unchanged set: no change.
	if changed, err := c.membershipChanged(want); changed {
		t.Fatalf("unchanged set reported as changed: %v", err)
	}

	// Count-preserving swap: c leaves, d joins. Count stays 3 but identity moved.
	c.ring = ring.New()
	for _, id := range []string{"a", "b", "d"} {
		c.ring.Add(ring.Member{ID: id, Addr: id + ":1"})
	}
	if changed, _ := c.membershipChanged(want); !changed {
		t.Fatal("P2-C VIOLATED: a count-preserving leave+join swap was NOT detected (count-only check would miss it)")
	}

	// A plain leave (count drops) is also a change.
	c.ring = ring.New()
	for _, id := range []string{"a", "b"} {
		c.ring.Add(ring.Member{ID: id, Addr: id + ":1"})
	}
	if changed, _ := c.membershipChanged(want); !changed {
		t.Fatal("a member leave was not detected")
	}
}
