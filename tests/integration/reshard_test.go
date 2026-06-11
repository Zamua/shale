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
// pkg/cluster; this file proves the wired-together SINGLE-NODE reshard (the
// supported, concurrent-write-safe surface) and that a multi-node reshard is
// refused until the cluster-wide generation barrier lands.

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
)

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

// TestReshard_MultiNodeRefused pins the v0.8 Phase 4 scope boundary. The online
// doubling reshard is supported on a SINGLE-NODE cluster (gate-validated
// lossless under concurrent writes). A MULTI-NODE reshard needs a cluster-wide
// generation barrier so every node routes at one generation and the write-pause
// spans the logical key-space; Reshard is a purely local trigger with no such
// coordination, so a multi-node reshard under concurrent writes could lose acked
// writes. Until the barrier lands, Reshard REFUSES on a multi-node cluster
// rather than ship that footgun. This test proves the refusal is clean: it
// errors, and the cluster is unharmed (every acked key still readable, still
// serving writes).
func TestReshard_MultiNodeRefused(t *testing.T) {
	const unitCount = 16
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "r2a", "", unitCount, backing)
	n2 := startSharedNode(t, "r2b", n1.BindAddr, unitCount, backing)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	// Let the join-driven reconcile settle so ownership is stable.
	time.Sleep(700 * time.Millisecond)

	want := make(map[string][]byte)
	const nKeys = 100
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("r2-%05d", i)
		v := []byte(fmt.Sprintf("rv-%05d", i))
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 8*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	// Reshard must REFUSE on each node of a multi-node cluster.
	for _, n := range []*sharedNode{n1, n2} {
		err := n.Cluster.Reshard()
		if err == nil {
			t.Fatalf("Reshard on multi-node %s succeeded; want a refusal (multi-node reshard is not yet supported)", n.ID)
		}
		if !strings.Contains(err.Error(), "multi-node") {
			t.Fatalf("Reshard on %s error = %q, want a multi-node-not-supported refusal", n.ID, err)
		}
	}

	// The refused reshard left the cluster unharmed: every acked key is still
	// readable through both nodes, and writes still work.
	for k, v := range want {
		for _, n := range []*sharedNode{n1, n2} {
			got, err := getWithRetryUnavailable(t, n.Cluster, k, 10*time.Second)
			if err != nil {
				t.Fatalf("acked key %q unreadable via %s after refused reshard: %v", k, n.ID, err)
			}
			if !bytes.Equal(got, v) {
				t.Fatalf("acked key %q via %s = %q, want %q", k, n.ID, got, v)
			}
		}
	}
	if err := putWithRetryUnavailable(t, n2.Cluster, "after-refuse", "ok", 8*time.Second); err != nil {
		t.Fatalf("Put after refused reshard on n2: %v", err)
	}
	got, err := getWithRetryUnavailable(t, n1.Cluster, "after-refuse", 8*time.Second)
	if err != nil || !bytes.Equal(got, []byte("ok")) {
		t.Fatalf("read after refused reshard on n1: got=%q err=%v", got, err)
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
