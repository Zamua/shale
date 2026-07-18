package cluster_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/clustertest"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	"google.golang.org/grpc"
)

func TestOpen_RequiresNodeID(t *testing.T) {
	if _, err := cluster.Open(cluster.Config{Backend: memory.New()}); err == nil {
		t.Fatalf("Open with empty NodeID should error")
	}
}

func TestOpen_RequiresBackend(t *testing.T) {
	if _, err := cluster.Open(cluster.Config{NodeID: "n1"}); err == nil {
		t.Fatalf("Open with nil Backend should error")
	}
}

func TestSingleNode_RoundTrip(t *testing.T) {
	c, err := cluster.Open(cluster.Config{NodeID: "n1", Backend: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("got %q want v", got)
	}
}

func TestClosed(t *testing.T) {
	c, _ := cluster.Open(cluster.Config{NodeID: "n1", Backend: memory.New()})
	_ = c.Close()
	if err := c.Put([]byte("k"), []byte("v")); !errors.Is(err, backend.ErrClosed) {
		t.Fatalf("Put on closed should be ErrClosed, got %v", err)
	}
}

// TestTwoNode_PutRoutesToOwner brings up two in-process Cluster nodes
// with independent memory backends, waits for membership convergence,
// then verifies that a Put issued on whichever node does NOT own the
// key still lands in the owning node's backend (read back directly via
// that backend, bypassing the cluster).
//
// The test is "static topology" friendly: it does not care which node
// owns the key, only that the routing decision is correct + the data
// crosses the gRPC boundary when it has to.
func TestTwoNode_PutRoutesToOwner(t *testing.T) {
	n1Mem := memory.New()
	n2Mem := memory.New()

	n1MemberPort := freePort(t)
	n2MemberPort := freePort(t)

	n1GRPC, n1stop := startGRPC(t)
	defer n1stop()
	n2GRPC, n2stop := startGRPC(t)
	defer n2stop()

	c1, err := cluster.Open(cluster.Config{
		NodeID:    "n1",
		Backend:   n1Mem,
		BindAddr:  hostPort(n1MemberPort),
		GRPCAddr:  n1GRPC.addr,
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("open n1: %v", err)
	}
	defer func() { _ = c1.Close() }()
	n1GRPC.register(c1)

	c2, err := cluster.Open(cluster.Config{
		NodeID:    "n2",
		Backend:   n2Mem,
		BindAddr:  hostPort(n2MemberPort),
		GRPCAddr:  n2GRPC.addr,
		Seeds:     []string{hostPort(n1MemberPort)},
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("open n2: %v", err)
	}
	defer func() { _ = c2.Close() }()
	n2GRPC.register(c2)

	if err := waitForRingSize(c1, 2, 5*time.Second); err != nil {
		t.Fatalf("n1 ring: %v", err)
	}
	if err := waitForRingSize(c2, 2, 5*time.Second); err != nil {
		t.Fatalf("n2 ring: %v", err)
	}

	// v0.3 + joiner-bootstrap: c2 may be in StateReceiving for the
	// partitions it picked up at Open time. Wait for both nodes to
	// settle before exercising the routing/forwarding path; the
	// rebalance behavior itself is covered by dedicated tests.
	for _, c := range []*cluster.Cluster{c1, c2} {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		if err := c.WaitForRebalanceIdle(ctx); err != nil {
			cancel()
			t.Fatalf("%s rebalance idle: %v", c.NodeID(), err)
		}
		cancel()
	}

	// Find a key owned by n2 + write it via n1 (forces forwarding).
	// Then find a key owned by n1 + write it via n2 (forwarding in
	// the other direction). The static partition assignment is
	// deterministic across both processes since both compute the
	// same ring from the same membership.
	keyOwnedByN2 := findKeyOwnedBy(t, c1, "n2", 1000)
	keyOwnedByN1 := findKeyOwnedBy(t, c2, "n1", 1000)

	if err := c1.Put([]byte(keyOwnedByN2), []byte("from-n1")); err != nil {
		t.Fatalf("c1.Put(n2-key): %v", err)
	}
	if err := c2.Put([]byte(keyOwnedByN1), []byte("from-n2")); err != nil {
		t.Fatalf("c2.Put(n1-key): %v", err)
	}

	// Read DIRECTLY from each backend to confirm the data physically
	// landed on the owning node (not just that the cluster Get
	// round-tripped back).
	got, err := n2Mem.Get([]byte(keyOwnedByN2))
	if err != nil {
		t.Fatalf("n2 backend.Get(n2-key): %v", err)
	}
	if string(got) != "from-n1" {
		t.Fatalf("n2 backend: want from-n1, got %q", got)
	}

	got, err = n1Mem.Get([]byte(keyOwnedByN1))
	if err != nil {
		t.Fatalf("n1 backend.Get(n1-key): %v", err)
	}
	if string(got) != "from-n2" {
		t.Fatalf("n1 backend: want from-n2, got %q", got)
	}

	// Round-trip via cluster Get on the OTHER node also works
	// (covers Get-forwarding too, not just Put-forwarding).
	val, err := c2.Get([]byte(keyOwnedByN2))
	if err != nil {
		t.Fatalf("c2.Get(n2-key) forwarded: %v", err)
	}
	if string(val) != "from-n1" {
		t.Fatalf("c2.Get: want from-n1, got %q", val)
	}
}

// freePort + hostPort delegate to the shared harness package so the
// port-probing strategy cannot drift from the integration tree's copy.
// See internal/clustertest.
func freePort(t *testing.T) int {
	t.Helper()
	return clustertest.FreePort(t)
}

func hostPort(port int) string {
	return clustertest.HostPort(port)
}

// grpcHarness is a started gRPC server with a known address that the
// test can later register a Cluster against. The two-step shape (start
// the listener so we have an Addr before Open, then register the
// cluster handler) mirrors the order shaled's main.go uses.
type grpcHarness struct {
	addr    string
	server  *grpc.Server
	lis     net.Listener
	done    chan struct{}
	started bool
}

func startGRPC(t *testing.T) (*grpcHarness, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	h := &grpcHarness{
		addr:   lis.Addr().String(),
		server: grpc.NewServer(),
		lis:    lis,
		done:   make(chan struct{}),
	}
	return h, func() {
		if h.started {
			h.server.GracefulStop()
			<-h.done
		} else {
			_ = h.lis.Close()
		}
	}
}

func (h *grpcHarness) register(c *cluster.Cluster) {
	rpc.NewServer(c).Register(h.server)
	go func() {
		defer close(h.done)
		_ = h.server.Serve(h.lis)
	}()
	h.started = true
}

// waitForWriteReady, isTransientWarmupErr and waitForRingSize delegate
// to the shared harness package so this tree and the integration tree
// gate on identical readiness semantics. See internal/clustertest.
func waitForWriteReady(t *testing.T, clusters []*cluster.Cluster, deadline time.Duration) {
	t.Helper()
	clustertest.WaitForWriteReady(t, clusters, deadline)
}

func waitForRingSize(c *cluster.Cluster, want int, timeout time.Duration) error {
	return clustertest.WaitForRingSize(c, want, timeout)
}

// pollUntil polls cond every interval until it returns true or the
// timeout elapses. Returns true on success, false on timeout. Use this
// to gate an assertion on a condition the test drives toward
// asynchronously (a reconcile tick landing, a source-side sweep
// dropping a stale copy) instead of a fixed settle sleep: the loop
// returns the instant the condition holds, and only burns the full
// budget when something is genuinely wrong.
func pollUntil(timeout, interval time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}

// findKeyOwnedBy scans synthetic keys until it finds one whose owner
// (as the given cluster sees it) is wantOwner. The lookup uses the
// public Members() snapshot + a freshly-constructed ring, so it sees
// exactly the routing decision Put/Get would make. Bounded by maxProbes
// iterations so a misconfigured test fails loudly instead of looping.
func findKeyOwnedBy(t *testing.T, c *cluster.Cluster, wantOwner string, maxProbes int) string {
	t.Helper()
	if len(c.Members()) < 2 {
		t.Fatalf("findKeyOwnedBy: ring needs >=2 members, got %v", c.Members())
	}
	for i := range maxProbes {
		k := fmt.Sprintf("probe-%d", i)
		if ownerFor(c, k) == wantOwner {
			return k
		}
	}
	t.Fatalf("could not find a key owned by %s in %d probes", wantOwner, maxProbes)
	return ""
}

// ownerFor mirrors what cluster.ownerOf does, using the public
// Members() snapshot + the same ring algorithm. Implemented here in
// the test so we don't have to export an internal hook. Two rings
// built from the same Members() snapshot agree (consistent hashing is
// deterministic), so this is a faithful preview of the routing.
func ownerFor(c *cluster.Cluster, key string) string {
	r := ring.New()
	for _, m := range c.Members() {
		r.Add(m)
	}
	return r.LocateKey([]byte(key)).ID
}

func TestAggregate_SingleNode(t *testing.T) {
	c, _ := cluster.Open(cluster.Config{NodeID: "n1", Backend: memory.New()})
	t.Cleanup(func() { _ = c.Close() })
	_ = c.Put([]byte("a"), []byte("1"))
	_ = c.Put([]byte("b"), []byte("2"))

	results := c.Aggregate(func(b backend.Backend) any {
		it, _ := b.ScanPrefix(nil)
		defer func() { _ = it.Close() }()
		count := 0
		for {
			k, _, err := it.Next()
			if err != nil || k == nil {
				break
			}
			count++
		}
		return count
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d (%v)", len(results), results)
	}
	if results[0].Err != nil {
		t.Fatalf("unexpected aggregate err: %v", results[0].Err)
	}
	if got := results[0].Value.(int); got != 2 {
		t.Fatalf("expected single-node aggregate count=2, got %d (%v)", got, results)
	}
}

// TestCloseRace runs Close concurrently with Put + Get + Delete +
// ScanPrefix in a tight loop. The contract: no panic, no deadlock,
// no race-detector warning. Closed-after-start ops return
// backend.ErrClosed; ops that landed before Close succeed.
func TestCloseRace(t *testing.T) {
	for range 100 {
		c, err := cluster.Open(cluster.Config{NodeID: "n1", Backend: memory.New()})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(4)
		go func() {
			defer wg.Done()
			_ = c.Put([]byte("a"), []byte("v"))
		}()
		go func() {
			defer wg.Done()
			_, _ = c.Get([]byte("a"))
		}()
		go func() {
			defer wg.Done()
			_ = c.Delete([]byte("a"))
		}()
		go func() {
			defer wg.Done()
			it, err := c.ScanPrefix(nil)
			if err == nil && it != nil {
				_, _, _ = it.Next()
				_ = it.Close()
			}
		}()

		// Race the operations against Close, and ALSO call Close
		// twice concurrently to pin the sync.Once guard.
		go func() { _ = c.Close() }()
		_ = c.Close()
		wg.Wait()
	}
}

// TestAggregateResult_DistinguishesPeerError forces one peer to
// return an error from its snapshot path + asserts that
// AggregateResult.Err is set distinctly from AggregateResult.Value.
//
// We force the error by killing one peer's gRPC server before
// calling Aggregate. The local node still has the peer on its ring
// (no leave event yet) so Aggregate fans out to it; the snapshot
// transport fails; the per-peer result lands in .Err.
func TestAggregateResult_DistinguishesPeerError(t *testing.T) {
	n1Mem := memory.New()
	n2Mem := memory.New()

	n1MemberPort := freePort(t)
	n2MemberPort := freePort(t)

	n1GRPC, n1stop := startGRPC(t)
	defer n1stop()
	n2GRPC, _ := startGRPC(t)

	c1, err := cluster.Open(cluster.Config{
		NodeID:    "n1",
		Backend:   n1Mem,
		BindAddr:  hostPort(n1MemberPort),
		GRPCAddr:  n1GRPC.addr,
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("open n1: %v", err)
	}
	defer func() { _ = c1.Close() }()
	n1GRPC.register(c1)

	c2, err := cluster.Open(cluster.Config{
		NodeID:    "n2",
		Backend:   n2Mem,
		BindAddr:  hostPort(n2MemberPort),
		GRPCAddr:  n2GRPC.addr,
		Seeds:     []string{hostPort(n1MemberPort)},
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("open n2: %v", err)
	}
	defer func() { _ = c2.Close() }()
	n2GRPC.register(c2)

	if err := waitForRingSize(c1, 2, 5*time.Second); err != nil {
		t.Fatalf("ring: %v", err)
	}

	// Hard-stop n2's gRPC. n1's ring still has n2; Aggregate's
	// snapshotPeer for n2 will hit a transport error.
	n2GRPC.server.Stop()
	<-n2GRPC.done
	// Poll until a fresh dial to n2's gRPC address is actually refused
	// (the OS tears the listener down asynchronously after Stop), so
	// the Aggregate below sees a definite transport error rather than
	// racing the teardown. Deterministic replacement for a fixed settle
	// sleep: returns the instant the port stops accepting.
	if !pollUntil(5*time.Second, 20*time.Millisecond, func() bool {
		conn, err := net.DialTimeout("tcp", n2GRPC.addr, 100*time.Millisecond)
		if err != nil {
			return true
		}
		_ = conn.Close()
		return false
	}) {
		t.Fatalf("n2 gRPC listener at %s never stopped accepting after Stop", n2GRPC.addr)
	}

	results := c1.Aggregate(func(_ backend.Backend) any {
		return "fn-ran"
	})
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d (%v)", len(results), results)
	}
	// Exactly one entry has Value="fn-ran" (the local n1 path) and
	// exactly one has Err set (the n2 snapshot failure).
	var okCount, errCount int
	for _, r := range results {
		if r.Err != nil {
			errCount++
			if r.Value != nil {
				t.Fatalf("AggregateResult has both Value=%v + Err=%v - they should be disjoint", r.Value, r.Err)
			}
		} else {
			okCount++
			if r.Value != "fn-ran" {
				t.Fatalf("success result Value should be %q, got %v", "fn-ran", r.Value)
			}
		}
	}
	if okCount != 1 || errCount != 1 {
		t.Fatalf("expected 1 ok + 1 err, got ok=%d err=%d (%v)", okCount, errCount, results)
	}
}

// TestFounderGrows_RebalanceReachesEveryKey exercises the founder-grows
// (1 -> 2) topology that the v0.3 ring-vs-ring plan was blind to:
//
//  1. Open a SINGLE founder node and load 100 keys into it while it is
//     the only member (it owns every partition).
//  2. Join a 2nd node. The converged ring advances ownership of ~half
//     the partitions to the joiner. Under the old code, a partition the
//     joiner owns in both its (possibly self-only) bootstrap snapshot
//     and the converged ring produced NO Receive -- the keys stayed on
//     the founder, unreachable from the new owner.
//  3. WaitForRebalanceIdle on both nodes, then let the reconcile pass +
//     sweep settle.
//  4. Read every key directly off the ring-owner's backend: no key may
//     be orphaned on the founder while the ring routes to the joiner.
//
// This is the cluster-level companion to the rebalance package's
// TestReconcile_RepairsOwnedButMissing: the reconcile pass keyed on
// physical placement must pull every owned-but-missing partition so
// physical placement matches ring assignment.
func TestFounderGrows_RebalanceReachesEveryKey(t *testing.T) {
	rebalance.SetSweepInterval(50 * time.Millisecond)

	founderMem := memory.New()
	joinerMem := memory.New()

	founderBind := hostPort(freePort(t))
	joinerBind := hostPort(freePort(t))

	// Founder comes up ALONE and takes all 100 keys before any peer
	// exists. This is the load-then-grow ordering the 2 -> 3 test never
	// hits (that test joins every node before writing).
	founder, founderStop := openClusterNodeAt(t, "fg-founder", founderBind, "", founderMem)
	defer founderStop()

	if err := waitForRingSize(founder, 1, 5*time.Second); err != nil {
		t.Fatalf("founder solo ring: %v", err)
	}

	keys := make([]string, 100)
	for i := range 100 {
		k := fmt.Sprintf("fg-%04d", i)
		if err := putWithMigrationRetry(founder, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		keys[i] = k
	}
	if got := countBackend(t, founderMem); got != 100 {
		t.Fatalf("founder pre-growth key count = %d, want 100", got)
	}

	// Now grow: a 2nd node joins the founder.
	joiner, joinerStop := openClusterNodeAt(t, "fg-joiner", joinerBind, founderBind, joinerMem)
	defer joinerStop()

	for _, c := range []*cluster.Cluster{founder, joiner} {
		if err := waitForRingSize(c, 2, 5*time.Second); err != nil {
			t.Fatalf("2-node ring on %s: %v", c.NodeID(), err)
		}
	}

	// Drive a couple of settle-timer Evaluates worth of wall clock so
	// the reconcile pass (folded into Evaluate) runs against the
	// converged ring on the joiner and pulls every owned-but-missing
	// partition. WaitForRebalanceIdle bounds the wait per node.
	for _, c := range []*cluster.Cluster{founder, joiner} {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		if err := c.WaitForRebalanceIdle(ctx); err != nil {
			cancel()
			t.Fatalf("%s did not idle: %v", c.NodeID(), err)
		}
		cancel()
	}

	// Verify physical placement matches the 2-node ring: every key must
	// be physically present on the backend of the node the ring routes
	// it to. A key the ring sends to the joiner that still lives only on
	// the founder is the founder-grows orphan this fix closes. The
	// reconcile pass + ring-vs-ring plan can be re-armed by a late
	// membership reconcile tick, so we poll the placement check rather
	// than asserting once: the loop returns the instant every owned key
	// has landed on its owner, and only burns the budget if a partition
	// is genuinely stranded.
	r := ring.New()
	for _, m := range founder.Members() {
		r.Add(m)
	}
	backends := map[string]*memory.Memory{
		"fg-founder": founderMem,
		"fg-joiner":  joinerMem,
	}
	missing := 0
	var firstMissing string
	placed := pollUntil(5*time.Second, 50*time.Millisecond, func() bool {
		missing, firstMissing = 0, ""
		for _, k := range keys {
			owner := r.LocateKey([]byte(k)).ID
			if _, err := backends[owner].Get([]byte(k)); err != nil {
				if missing == 0 {
					firstMissing = fmt.Sprintf("%s -> owner %s", k, owner)
				}
				missing++
			}
		}
		return missing == 0
	})
	if !placed {
		t.Fatalf("%d/%d keys missing on the ring-owner's backend (first: %s); founder-grows orphan not repaired",
			missing, len(keys), firstMissing)
	}
}

// openClusterNodeAt brings up a Cluster + gRPC server at known
// bind + gRPC addresses, registers it for cleanup, and returns
// the cluster + teardown closure. Caller supplies bindAddr so
// peer-discovery seeds are predictable.
func openClusterNodeAt(t *testing.T, id, bindAddr, seedBindAddr string, mem *memory.Memory) (*cluster.Cluster, func()) {
	t.Helper()
	grpcHarness, stop := startGRPC(t)
	cfg := cluster.Config{
		NodeID:                 id,
		Backend:                mem,
		BindAddr:               bindAddr,
		GRPCAddr:               grpcHarness.addr,
		LogOutput:              io.Discard,
		RebalanceSettleDelay:   500 * time.Millisecond,
		RebalanceGraceDuration: 1500 * time.Millisecond,
	}
	if seedBindAddr != "" {
		cfg.Seeds = []string{seedBindAddr}
	}
	c, err := cluster.Open(cfg)
	if err != nil {
		stop()
		t.Fatalf("openClusterNodeAt %s: %v", id, err)
	}
	grpcHarness.register(c)
	return c, func() {
		_ = c.Close()
		stop()
	}
}

// putWithMigrationRetry wraps Put with a bounded retry on the v0.4
// transient codes, delegating the code CLASSIFICATION to the shared
// harness package (see internal/clustertest.PutWithTransientRetry) so it
// cannot drift from the integration tree's equivalent. The 50-attempt /
// 50ms budget (~2.5s wall-clock) is this tree's own; the integration
// tree budgets its retry window differently.
func putWithMigrationRetry(c *cluster.Cluster, key, value []byte) error {
	return clustertest.PutWithTransientRetry(c, key, value, 50, 50*time.Millisecond)
}

func countBackend(t *testing.T, be *memory.Memory) int {
	t.Helper()
	it, err := be.ScanPrefix(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	n := 0
	for {
		k, _, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if k == nil {
			return n
		}
		n++
	}
}
