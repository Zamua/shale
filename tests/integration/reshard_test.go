package integration

// End-to-end coverage for the v0.8 Phase 4 doubling resharder. These tests
// stand up in-process multi-backend clusters on the SHARED-BACKING factory
// (internal/sharedfactory - the test analogue of shared object storage with a
// per-(generation,unit) durable writer-epoch) and double the unit count online
// via Cluster.Reshard().
//
// THE INVARIANT UNDER TEST is the data-loss-sensitive one Phase 4 is built to:
//
//	NO ACKED WRITE MAY BE LOST DURING THE BISECT.
//
// A write that returned success before / during a reshard MUST still be
// readable, with its exact value, after the cluster reaches the new
// generation (2N units). The single-node bisect mechanics (copy split by the
// new hash bit, catch-up drain, atomic cut-over) are pinned white-box in
// pkg/cluster; this file proves the wired-together multi-node reshard.

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
)

// reshardAll runs Cluster.Reshard() on every node, the way an operator drives
// a fleet-wide doubling: each node bisects the units it locally mounts, and
// the new gen-(g+1) units redistribute across nodes by the reused Phase 3
// lease handoff. Order does not matter for correctness (each unit's bisect is
// independent), but we run the founder last so the others have advanced first.
func reshardAll(t *testing.T, nodes []*sharedNode) {
	t.Helper()
	for _, n := range nodes {
		if err := n.Cluster.Reshard(); err != nil {
			t.Fatalf("Reshard on %s: %v", n.ID, err)
		}
	}
}

// TestReshard_SingleNodeNoAckedWriteLost is the simplest reshard gate: a single
// multi-backend node writes a spread of acked keys, doubles N -> 2N via
// Reshard, and every acked key must still be readable with its exact value.
func TestReshard_SingleNodeNoAckedWriteLost(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "rs1", "", unitCount, backing)

	want := make(map[string][]byte)
	const nKeys = 300
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("rs-%04d", i)
		v := []byte(fmt.Sprintf("v-%04d", i))
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 5*time.Second); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
		want[k] = v
	}

	if err := n1.Cluster.Reshard(); err != nil {
		t.Fatalf("Reshard: %v", err)
	}

	for k, v := range want {
		got, err := getWithRetryUnavailable(t, n1.Cluster, k, 5*time.Second)
		if err != nil {
			t.Fatalf("acked key %q unreadable after reshard: %v (NO ACKED WRITE LOST violated)", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("acked key %q = %q, want %q after reshard", k, got, v)
		}
	}

	// New keys (written after the reshard, at 2N) round-trip too.
	if err := putWithRetryUnavailable(t, n1.Cluster, "post-reshard", "new", 5*time.Second); err != nil {
		t.Fatalf("post-reshard Put: %v", err)
	}
	got, err := getWithRetryUnavailable(t, n1.Cluster, "post-reshard", 5*time.Second)
	if err != nil || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("post-reshard key: got=%q err=%v", got, err)
	}
}

// TestReshard_TwoNodeNoAckedWriteLost is THE multi-node Phase 4 gate. Two nodes
// share a backing; they write acked keys across the unit space, then BOTH
// reshard (N -> 2N). After the reshard + reconcile settle, every acked key must
// still be readable through the cluster (routing to the new gen-1 unit owner),
// and the cluster keeps serving new writes at the doubled unit count.
func TestReshard_TwoNodeNoAckedWriteLost(t *testing.T) {
	const unitCount = 16
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "r2a", "", unitCount, backing)
	n2 := startSharedNode(t, "r2b", n1.BindAddr, unitCount, backing)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	// Let the join-driven reconcile settle so ownership is stable before we
	// record the baseline.
	time.Sleep(700 * time.Millisecond)

	want := make(map[string][]byte)
	const nKeys = 400
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("r2-%05d", i)
		v := []byte(fmt.Sprintf("rv-%05d", i))
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 8*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	// Operator drives the doubling on every node.
	nodes := []*sharedNode{n1, n2}
	reshardAll(t, nodes)

	// After both nodes reshard, the live generation on each is 1 and the unit
	// space is 2N. The 2N units redistribute by the reused lease handoff
	// (bumpRingGen -> reconcile). Wait for that to settle: every node's mounted
	// set should be its OWNED gen-1 set, with the two nodes partitioning the 2N
	// units (disjoint + complete). This is the same convergence Phase 3 waits
	// on after a membership change.
	if !waitUntil(12*time.Second, func() bool {
		o1 := n1.Handle.OpenUnits()
		o2 := n2.Handle.OpenUnits()
		seen := make(map[uint32]int)
		for _, gu := range o1 {
			if gu.Gen != 1 {
				return false
			}
			seen[uint32(gu.ID)]++
		}
		for _, gu := range o2 {
			if gu.Gen != 1 {
				return false
			}
			seen[uint32(gu.ID)]++
		}
		// Every gen-1 unit mounted exactly once (no overlap, no old-gen left).
		if len(seen) != 2*unitCount {
			return false
		}
		for _, c := range seen {
			if c != 1 {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("reshard reconcile did not settle to a clean 2N partition: n1=%v n2=%v", n1.Handle.OpenUnits(), n2.Handle.OpenUnits())
	}

	// Now assert no acked write was lost - read EVERY key from BOTH nodes
	// (forwarding to the current owner), retrying the transient acquiring-window
	// error.
	for k, v := range want {
		for _, n := range nodes {
			got, err := getWithRetryUnavailable(t, n.Cluster, k, 10*time.Second)
			if err != nil {
				t.Fatalf("acked key %q unreadable via %s after reshard: %v (NO ACKED WRITE LOST violated)", k, n.ID, err)
			}
			if !bytes.Equal(got, v) {
				t.Fatalf("acked key %q via %s = %q, want %q after reshard", k, n.ID, got, v)
			}
		}
	}

	// The cluster still serves writes at the doubled unit count.
	if err := putWithRetryUnavailable(t, n2.Cluster, "after-2x", "ok", 8*time.Second); err != nil {
		t.Fatalf("post-reshard Put on n2: %v", err)
	}
	got, err := getWithRetryUnavailable(t, n1.Cluster, "after-2x", 8*time.Second)
	if err != nil || !bytes.Equal(got, []byte("ok")) {
		t.Fatalf("post-reshard read on n1: got=%q err=%v", got, err)
	}
}

// TestReshard_ConcurrentWritesNoAckedWriteLost hammers a single shared-backing
// node with ACKED writes through the FULL cluster surface (gRPC-registered,
// routed) WHILE a reshard runs, then asserts every acked write survived. This
// is the wired-together analogue of the white-box concurrent-writes test:
// it exercises the routed Put path's write-pause integration, not just the
// in-process backend.
func TestReshard_ConcurrentWritesNoAckedWriteLost(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "rc1", "", unitCount, backing)

	// Seed so every unit has data.
	for i := 0; i < 150; i++ {
		k := fmt.Sprintf("cseed-%04d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, "s", 5*time.Second); err != nil {
			t.Fatalf("seed Put: %v", err)
		}
	}

	var mu sync.Mutex
	acked := make(map[string][]byte)
	var stop atomic.Bool
	var wg sync.WaitGroup
	const writers = 4
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for !stop.Load() {
				k := fmt.Sprintf("chot-%d-%06d", w, i)
				v := []byte(fmt.Sprintf("cv-%d-%06d", w, i))
				// Retry the transient acquiring-window error; any other error is
				// a real failure. A write only counts as acked once Put returns
				// nil.
				if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 5*time.Second); err != nil {
					t.Errorf("concurrent Put %q during reshard: %v", k, err)
					return
				}
				mu.Lock()
				acked[k] = v
				mu.Unlock()
				i++
			}
		}(w)
	}

	if err := n1.Cluster.Reshard(); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("Reshard under concurrent writes: %v", err)
	}
	stop.Store(true)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(acked) < writers {
		t.Fatalf("only %d acked writes recorded; writers did not run", len(acked))
	}
	for k, v := range acked {
		got, err := getWithRetryUnavailable(t, n1.Cluster, k, 5*time.Second)
		if err != nil {
			t.Fatalf("acked-during-reshard key %q unreadable: %v (NO ACKED WRITE LOST violated)", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("acked-during-reshard key %q = %q, want %q", k, got, v)
		}
	}
}
