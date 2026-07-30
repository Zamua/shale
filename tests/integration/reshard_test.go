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
// pkg/cluster; this file proves the wired-together SINGLE-NODE reshard and the
// MULTI-NODE delegated (arbiter-driven) Reshard() surface: on a multi-node
// cluster Reshard() retargets the shared CAS arbiter and the per-tick
// reconcile driver converges every node online (parent-anchored at R=1), with
// a typed refusal when no ConditionalStore is configured.

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// startR1StoreNode brings up one R=1 multi-backend node sharing `backing` AND
// the given ConditionalStore - the shape a multi-node (arbiter-driven) reshard
// requires. All nodes of one in-process cluster share ONE MemConditionalStore,
// the test analogue of the shared MinIO/S3 conditional store.
func startR1StoreNode(t *testing.T, id, seedAddr string, unitCount int, backing *sharedfactory.Backing, store storageunit.ConditionalStore) *sharedNode {
	t.Helper()
	return startReplicatedNodeCfg(t, id, seedAddr, unitCount, 1, backing, func(cfg *cluster.Config) {
		cfg.ConditionalStore = store
	})
}

// startReconcilePump drives every node's reconcile on a fast background cadence
// (the accelerated stand-in for the production reconcileInterval tick), so a
// delegated Reshard() call - which converges only as every node's reconcile
// drives its share of the split - completes promptly in-process. Returns a stop
// func the caller MUST invoke before tearing the nodes down.
func startReconcilePump(cs []*cluster.Cluster) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			for _, c := range cs {
				c.TestingRunReconcile()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	return func() { close(done); wg.Wait() }
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
	for i := range nKeys {
		k := fmt.Sprintf("rs-%04d", i)
		v := fmt.Appendf(nil, "v-%04d", i)
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

// TestReshard_MultiNodeArbiterDoubling proves the multi-node doubling via the
// DELEGATED (arbiter-driven) Reshard(). A 2-node R=1 multi-backend cluster
// (both nodes gRPC-registered, sharing one backing + one ConditionalStore)
// writes a spread of acked keys, then Reshard() on one node retargets the
// shared arbiter and the per-tick reconcile driver converges both nodes online
// (parent-anchored placement, per-unit pause-held cut-over, durable markers).
// After it returns, EVERY acked key must still be readable with its exact
// value (NO ACKED WRITE LOST), the cluster must serve new writes at the
// doubled generation, and a doubled-count key (one that maps to a unit id >=
// N, only reachable after 2N) must round-trip - proving routing advanced to
// the new generation cluster-wide.
func TestReshard_MultiNodeArbiterDoubling(t *testing.T) {
	const unitCount = 16
	backing := sharedfactory.NewBacking()
	store := storageunit.NewMemConditionalStore()
	n1 := startR1StoreNode(t, "fb-a", "", unitCount, backing, store)
	n2 := startR1StoreNode(t, "fb-b", n1.BindAddr, unitCount, backing, store)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	// Let the join-driven reconcile settle so ownership is stable before the
	// reshard begins.
	time.Sleep(700 * time.Millisecond)

	want := make(map[string][]byte)
	const nKeys = 300
	for i := range nKeys {
		k := fmt.Sprintf("fb-%05d", i)
		v := fmt.Appendf(nil, "fv-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 8*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	// Delegated reshard on n1: Retarget(2N) + converge. The pump stands in for
	// the production reconcile cadence on both nodes; it keeps running through
	// the assertions so the post-finalize redistribution keeps moving too.
	stopPump := startReconcilePump(clusters)
	defer stopPump()
	if err := n1.Cluster.Reshard(); err != nil {
		t.Fatalf("multi-node Reshard (delegated, n1): %v", err)
	}

	// Let the post-finalize redistribution reconcile settle so every gen-(g+1)
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
	for i := range 100000 {
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

// TestReshard_MultiNodeRefusesWithoutConditionalStore pins the typed refusal:
// a multi-node Reshard() without a Config.ConditionalStore cannot run (the
// arbiter-driven reshard's agreement object lives there), so it must refuse
// with cluster.ErrReshardNeedsConditionalStore - and the cluster must stay
// fully serving at the old generation (the refusal is a clean no-op).
func TestReshard_MultiNodeRefusesWithoutConditionalStore(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "fz-a", "", unitCount, backing)
	n2 := startSharedNode(t, "fz-b", n1.BindAddr, unitCount, backing)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster}, 2, 15*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(700 * time.Millisecond)

	if err := putWithRetryUnavailable(t, n1.Cluster, "seed", "v0", 8*time.Second); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	err := n1.Cluster.Reshard()
	if err == nil {
		t.Fatal("multi-node Reshard without a ConditionalStore succeeded; want the typed refusal")
	}
	if !errors.Is(err, cluster.ErrReshardNeedsConditionalStore) {
		t.Fatalf("multi-node Reshard refusal = %v, want errors.Is(_, cluster.ErrReshardNeedsConditionalStore)", err)
	}

	// The refusal is a clean no-op: still gen 0, still serving reads + writes.
	if gen, count := n1.Cluster.GenStateSnapshot(); gen != 0 || count != unitCount {
		t.Fatalf("after refusal: gen=%d count=%d, want gen 0 count %d", gen, count, unitCount)
	}
	if got, err := getWithRetryUnavailable(t, n2.Cluster, "seed", 8*time.Second); err != nil || !bytes.Equal(got, []byte("v0")) {
		t.Fatalf("read after refusal: got=%q err=%v", got, err)
	}
	if err := putWithRetryUnavailable(t, n1.Cluster, "post-refusal", "ok", 8*time.Second); err != nil {
		t.Fatalf("write after refusal: %v", err)
	}
}

// putWithRetryReshard is the reshard-aware write retry. It retries the two
// transient signals a write can hit during a multi-node reshard:
//
//   - codes.Unavailable: the per-unit cut-over pause / acquiring-window
//     refusals (owner-but-unmounted, a fenced mount mid-redistribution).
//   - codes.FailedPrecondition: the forwarding loop-guard, which fires during
//     the staggered finalize window when the originating node and the
//     destination momentarily disagree about the generation (one finalized,
//     one still in flight), so the forwarded write lands on a node whose ring
//     view no longer owns the key. A real SDK refreshes its ring view and
//     retries; the cluster re-keys onto the new generation and the retry
//     routes correctly.
//
// Neither refusal acks the write (Put returned an error), so retrying upholds
// NO-ACKED-WRITE-LOST: only a write that returns nil is counted acked.
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

// awaitFirstAckBarrier blocks until every writer has closed its ready channel
// (each closes its own after landing its FIRST acked write), or the deadline
// passes.
//
// This is the barrier that makes the "concurrent" in the concurrent-write
// reshard tests REAL rather than assumed. An in-process single-node reshard
// completes in a few HUNDRED MICROSECONDS, which is faster than the Go runtime
// wakes a parked P to run a freshly spawned writer goroutine on an otherwise
// idle machine. Without the barrier the main goroutine can finish the reshard
// and set the stop flag before any writer is ever scheduled; each writer then
// observes stop on its very first loop check and exits having issued ZERO
// writes, so the reshard races nothing and the read-back oracle pins nothing.
// Blocking here until every writer has provably landed an acked write, while
// the writers keep looping through the reshard that follows, makes the overlap
// a structural property instead of a bet on the reshard being slow.
func awaitFirstAckBarrier(t *testing.T, ready []chan struct{}, timeout time.Duration, stop *atomic.Bool, wg *sync.WaitGroup) {
	t.Helper()
	deadline := time.After(timeout)
	for w, ch := range ready {
		select {
		case <-ch:
		case <-deadline:
			stop.Store(true)
			wg.Wait()
			t.Fatalf("writer %d landed no acked write within %v; cannot start a reshard that provably races live writes", w, timeout)
		}
	}
}

// localKeyFor returns a key whose unit at the current count is owned by node n
// on the ring built from n's Members() - so a Put of it on n hits the LOCAL
// write path (and thus the local freeze gate), not a forward to a peer.
func localKeyFor(t *testing.T, n *sharedNode, unitCount int) string {
	t.Helper()
	for i := range 100000 {
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
	for i := range 150 {
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
	// Each writer closes its own ready channel after its FIRST acked write so
	// the barrier below can hold the reshard until every writer is in flight.
	ready := make([]chan struct{}, writers)
	for w := range ready {
		ready[w] = make(chan struct{})
	}
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for !stop.Load() {
				k := fmt.Sprintf("chot-%d-%06d", w, i)
				v := fmt.Appendf(nil, "cv-%d-%06d", w, i)
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
				if i == 0 {
					close(ready[w])
				}
				i++
			}
		}(w)
	}

	// BARRIER: hold the reshard until every writer has provably landed an acked
	// write. The writers keep looping through the reshard that follows, so the
	// acked set spans it by construction rather than by timing luck.
	awaitFirstAckBarrier(t, ready, 30*time.Second, &stop, &wg)

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
// multi-node oracle in this file: writers hammer the FULL cluster surface
// (gRPC-registered, routed across both nodes) with ACKED writes WHILE a
// delegated (arbiter-driven) reshard runs. Writes attempted during a per-unit
// cut-over or the staggered finalize window get the retryable refusals and are
// retried (never counted acked until Put returns nil); the rest land at gen g
// or gen g+1. After the reshard, EVERY acked write must be readable with its
// exact value through both nodes. This proves parent-anchored placement + the
// per-unit pause-held cut-over remove the cross-node concurrent-write hazard
// that made the plain single-node bisect unsafe on a multi-node cluster.
func TestReshard_MultiNodeConcurrentWritesNoAckedWriteLost(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	store := storageunit.NewMemConditionalStore()
	n1 := startR1StoreNode(t, "mc-a", "", unitCount, backing, store)
	n2 := startR1StoreNode(t, "mc-b", n1.BindAddr, unitCount, backing, store)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(700 * time.Millisecond)

	// Seed so every unit has data to bisect.
	for i := range 150 {
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
	// Each writer closes its own ready channel after its FIRST acked write so
	// the barrier below can hold the reshard until every writer is in flight.
	ready := make([]chan struct{}, writers)
	for w := range ready {
		ready[w] = make(chan struct{})
	}
	for w := range writers {
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
				v := fmt.Appendf(nil, "mcv-%d-%07d", w, i)
				if err := putWithRetryReshard(t, entry.Cluster, k, string(v), 10*time.Second); err != nil {
					t.Errorf("concurrent Put %q during multi-node reshard: %v", k, err)
					return
				}
				mu.Lock()
				acked[k] = v
				mu.Unlock()
				if i == 0 {
					close(ready[w])
				}
				i++
			}
		}(w)
	}

	// BARRIER: hold the reshard until every writer has provably landed an acked
	// write through its entry node, so the delegated reshard below genuinely
	// races live writes on both first-hops.
	awaitFirstAckBarrier(t, ready, 30*time.Second, &stop, &wg)

	// Delegated reshard on n1 while writers hammer both nodes. The pump stands
	// in for the production reconcile cadence and keeps running through the
	// assertions so the redistribution keeps moving.
	stopPump := startReconcilePump(clusters)
	defer stopPump()
	if err := n1.Cluster.Reshard(); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("multi-node Reshard under concurrent writes: %v", err)
	}
	stop.Store(true)
	wg.Wait()

	// Let the post-finalize redistribution settle so every gen-(g+1) unit is on
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
