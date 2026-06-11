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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// TestReshard_MultiNodeFreezeBarrier proves the v0.8 multi-node doubling via the
// cluster-wide WRITE-FREEZE barrier. A 2-node multi-backend cluster (both nodes
// gRPC-registered + sharing one backing) writes a spread of acked keys, then a
// reshard called on one node (the COORDINATOR) drives FREEZE -> BISECT -> FLIP
// -> RESUME across both nodes. After it returns, EVERY acked key must still be
// readable with its exact value (NO ACKED WRITE LOST), the cluster must serve
// new writes at the doubled generation, and a doubled-count key (one that maps
// to a unit id >= N, only reachable after 2N) must round-trip - proving routing
// advanced to the new generation cluster-wide.
func TestReshard_MultiNodeFreezeBarrier(t *testing.T) {
	const unitCount = 16
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "fb-a", "", unitCount, backing)
	n2 := startSharedNode(t, "fb-b", n1.BindAddr, unitCount, backing)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	// Let the join-driven reconcile settle so ownership is stable before the
	// reshard snapshots the participant set.
	time.Sleep(700 * time.Millisecond)

	want := make(map[string][]byte)
	const nKeys = 300
	for i := 0; i < nKeys; i++ {
		k := fmt.Sprintf("fb-%05d", i)
		v := []byte(fmt.Sprintf("fv-%05d", i))
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 8*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	// COORDINATOR-driven reshard on n1: it freezes both nodes, each bisects its
	// owned units under the freeze (static copy), both flip to gen g+1, resume.
	if err := n1.Cluster.Reshard(); err != nil {
		t.Fatalf("multi-node Reshard (coordinator n1): %v", err)
	}

	// Let the post-RESUME redistribution reconcile settle so every gen-(g+1)
	// unit lands on its ring owner (some keys retry through the acquiring window
	// until then, which getWithRetryUnavailable absorbs).
	time.Sleep(700 * time.Millisecond)

	// NO ACKED WRITE LOST: every acked key still readable with its exact value,
	// through BOTH nodes (routing is consistent cluster-wide post-flip).
	for k, v := range want {
		for _, n := range []*sharedNode{n1, n2} {
			got, err := getWithRetryUnavailable(t, n.Cluster, k, 10*time.Second)
			if err != nil {
				t.Fatalf("acked key %q unreadable via %s after reshard: %v (NO ACKED WRITE LOST violated)", k, n.ID, err)
			}
			if !bytes.Equal(got, v) {
				t.Fatalf("acked key %q via %s = %q, want %q after reshard", k, n.ID, got, v)
			}
		}
	}

	// The cluster serves new writes at the doubled generation. Write through n2,
	// read through n1: both must agree, proving both nodes route at gen g+1.
	if err := putWithRetryUnavailable(t, n2.Cluster, "post-fb", "new", 8*time.Second); err != nil {
		t.Fatalf("post-reshard Put via n2: %v", err)
	}
	got, err := getWithRetryUnavailable(t, n1.Cluster, "post-fb", 8*time.Second)
	if err != nil || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("post-reshard read via n1: got=%q err=%v", got, err)
	}

	// Routing advanced to 2N: find a key whose unit id at 2N is >= N (a unit
	// that does not exist before the doubling). It must round-trip, proving the
	// new generation's unit space is live cluster-wide.
	newCount := storageunit.MustUnitCount(2 * unitCount)
	var highKey string
	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("hi-%d", i)
		u := storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), newCount)
		if uint32(u) >= unitCount {
			highKey = k
			break
		}
	}
	if highKey == "" {
		t.Fatal("could not find a key mapping to a doubled-only unit (>= N at 2N)")
	}
	if err := putWithRetryUnavailable(t, n1.Cluster, highKey, "high", 8*time.Second); err != nil {
		t.Fatalf("Put doubled-only-unit key %q: %v", highKey, err)
	}
	hgot, err := getWithRetryUnavailable(t, n2.Cluster, highKey, 8*time.Second)
	if err != nil || !bytes.Equal(hgot, []byte("high")) {
		t.Fatalf("read doubled-only-unit key %q via n2: got=%q err=%v", highKey, hgot, err)
	}
}

// TestReshard_MultiNodeFreezeRefusesWritesThenResumes pins the write-freeze
// semantics directly: while a node is frozen (the FREEZE phase applied but
// before RESUME), a Put returns the retryable codes.Unavailable error - it is
// NEVER acked - and a READ still succeeds. After the node resumes, the write
// succeeds on retry at the new generation. This is the per-node freeze contract
// the barrier's NO-ACKED-WRITE-LOST invariant rests on, exercised through the
// public surface by driving the phases via the cluster's exported phase entry.
func TestReshard_MultiNodeFreezeRefusesWritesThenResumes(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "fz-a", "", unitCount, backing)
	n2 := startSharedNode(t, "fz-b", n1.BindAddr, unitCount, backing)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster}, 2, 15*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(700 * time.Millisecond)

	// Seed a key so the read-during-freeze assertion has data.
	if err := putWithRetryUnavailable(t, n1.Cluster, "seed", "v0", 8*time.Second); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	// Drive only the FREEZE phase on n1 (target gen 1). No coordinator loop, so
	// the node stays frozen until we resume it - a deterministic window.
	if err := n1.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FREEZE, 1); err != nil {
		t.Fatalf("apply FREEZE on n1: %v", err)
	}

	// A WRITE that routes to n1's local unit must be refused with a retryable
	// codes.Unavailable, NEVER acked. Find a key n1 owns so we exercise the
	// local write path's freeze gate (a forwarded write would be refused too,
	// but we want the local gate here).
	frozenKey := localKeyFor(t, n1, unitCount)
	err := n1.Cluster.Put([]byte(frozenKey), []byte("should-not-ack"))
	if err == nil {
		t.Fatal("Put on a frozen node succeeded; want a retryable refusal (write must NOT be acked during freeze)")
	}
	if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
		t.Fatalf("frozen Put error code = %v, want codes.Unavailable (retryable)", st.Code())
	}

	// READS continue while frozen: the seeded key is still served.
	if got, err := n1.Cluster.Get([]byte("seed")); err != nil || !bytes.Equal(got, []byte("v0")) {
		t.Fatalf("read while frozen: got=%q err=%v, want v0 (reads must continue under freeze)", got, err)
	}

	// Begin is also frozen (the CAS commit write path is frozen).
	if _, err := n1.Cluster.Begin(0); err == nil {
		t.Fatal("Begin on a frozen node succeeded; want a retryable refusal")
	}

	// ABORT clears the freeze (no flip happened; the node stays at gen 0).
	if err := n1.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_ABORT, 1); err != nil {
		t.Fatalf("apply ABORT on n1: %v", err)
	}

	// After the abort the write succeeds (still gen 0, the cluster un-froze).
	if err := putWithRetryUnavailable(t, n1.Cluster, frozenKey, "now-ok", 8*time.Second); err != nil {
		t.Fatalf("Put after abort/resume: %v", err)
	}
	got, err := getWithRetryUnavailable(t, n2.Cluster, frozenKey, 8*time.Second)
	if err != nil || !bytes.Equal(got, []byte("now-ok")) {
		t.Fatalf("read after abort via n2: got=%q err=%v", got, err)
	}
}

// putWithRetryReshard is the reshard-aware write retry. It retries the two
// transient signals a write can hit during a coordinated multi-node reshard:
//
//   - codes.Unavailable: the write-freeze refusal (frozen for the barrier) and
//     the Phase 3 acquiring-window (owner-but-unmounted during redistribution).
//   - codes.FailedPrecondition: the forwarding loop-guard, which fires during
//     the brief FLIP window when the originating node and the destination
//     momentarily disagree about the generation (one flipped, one not), so the
//     forwarded write lands on a node whose ring view no longer owns the key. A
//     real SDK refreshes its ring view and retries; the cluster re-keys onto
//     the new generation and the retry routes correctly.
//
// Neither refusal acks the write (Put returned an error), so retrying upholds
// NO-ACKED-WRITE-LOST: only a write that returns nil is counted acked. This is
// the faithful client-retry behavior the freeze barrier's invariant rests on.
func putWithRetryReshard(t *testing.T, c *cluster.Cluster, key, val string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := c.Put([]byte(key), []byte(val))
		if err == nil {
			return nil
		}
		code := status.Code(err)
		retryable := code == codes.Unavailable || code == codes.FailedPrecondition
		if !retryable || time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// localKeyFor returns a key whose unit at the current count is owned by node n
// on the ring built from n's Members() - so a Put of it on n hits the LOCAL
// write path (and thus the local freeze gate), not a forward to a peer.
func localKeyFor(t *testing.T, n *sharedNode, unitCount int) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("lk-%d", i)
		if unitOwnerOnRing(n.Cluster, k, unitCount) == n.ID {
			return k
		}
	}
	t.Fatalf("no local key found for node %s", n.ID)
	return ""
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

// TestReshard_MultiNodeConcurrentWritesNoAckedWriteLost is the strongest
// multi-node oracle: writers hammer the FULL cluster surface (gRPC-registered,
// routed across both nodes) with ACKED writes WHILE a coordinator-driven
// freeze-barrier reshard runs. Writes attempted DURING the freeze get the
// retryable codes.Unavailable and are retried (never counted acked until Put
// returns nil); the rest land at gen g or gen g+1. After the reshard, EVERY
// acked write must be readable with its exact value through both nodes. This
// proves the cluster-wide freeze removes the cross-node concurrent-write hazard
// that made the single-node bisect unsafe on a multi-node cluster.
func TestReshard_MultiNodeConcurrentWritesNoAckedWriteLost(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "mc-a", "", unitCount, backing)
	n2 := startSharedNode(t, "mc-b", n1.BindAddr, unitCount, backing)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(700 * time.Millisecond)

	// Seed so every unit has data to bisect.
	for i := 0; i < 150; i++ {
		k := fmt.Sprintf("mcseed-%04d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, "s", 8*time.Second); err != nil {
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
			// Alternate the entry node so writes route through both first-hops
			// (local on one, forwarded on the other) - exercising both freeze
			// gates. A write only counts as acked once Put returns nil; the
			// freeze refusal (codes.Unavailable) is retried by the helper.
			i := 0
			for !stop.Load() {
				entry := n1
				if w%2 == 1 {
					entry = n2
				}
				k := fmt.Sprintf("mchot-%d-%07d", w, i)
				v := []byte(fmt.Sprintf("mcv-%d-%07d", w, i))
				if err := putWithRetryReshard(t, entry.Cluster, k, string(v), 10*time.Second); err != nil {
					t.Errorf("concurrent Put %q during multi-node reshard: %v", k, err)
					return
				}
				mu.Lock()
				acked[k] = v
				mu.Unlock()
				i++
			}
		}(w)
	}

	// Coordinator-driven reshard on n1 while writers hammer both nodes.
	if err := n1.Cluster.Reshard(); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("multi-node Reshard under concurrent writes: %v", err)
	}
	stop.Store(true)
	wg.Wait()

	// Let the post-RESUME redistribution settle so every gen-(g+1) unit is on
	// its ring owner before the final read sweep.
	time.Sleep(700 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(acked) < writers {
		t.Fatalf("only %d acked writes recorded; writers did not run", len(acked))
	}
	for k, v := range acked {
		for _, n := range []*sharedNode{n1, n2} {
			got, err := getWithRetryUnavailable(t, n.Cluster, k, 10*time.Second)
			if err != nil {
				t.Fatalf("acked-during-multi-node-reshard key %q unreadable via %s: %v (NO ACKED WRITE LOST violated)", k, n.ID, err)
			}
			if !bytes.Equal(got, v) {
				t.Fatalf("acked-during-multi-node-reshard key %q via %s = %q, want %q", k, n.ID, got, v)
			}
		}
	}
}
