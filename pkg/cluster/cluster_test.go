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
	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
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
	n1Bind := hostPort(freePort(t))
	n2Bind := hostPort(freePort(t))

	c1, stop1 := openMultiBackendNodeAt(t, "n1", n1Bind, "")
	defer stop1()
	c2, stop2 := openMultiBackendNodeAt(t, "n2", n2Bind, n1Bind)
	defer stop2()

	if err := waitForRingSize(c1, 2, 5*time.Second); err != nil {
		t.Fatalf("n1 ring: %v", err)
	}
	if err := waitForRingSize(c2, 2, 5*time.Second); err != nil {
		t.Fatalf("n2 ring: %v", err)
	}

	// The joiner's arrival re-assigns units; wait for the debounced
	// reconcile to run AND apply its mounts before exercising routing, so
	// a write cannot land mid-handoff.
	for _, c := range []*cluster.Cluster{c1, c2} {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		if err := c.WaitForRebalanceIdle(ctx); err != nil {
			cancel()
			t.Fatalf("%s rebalance idle: %v", c.NodeID(), err)
		}
		cancel()
	}

	// Find a key whose UNIT is owned by n2 + write it via n1 (forces
	// forwarding). Then the same in the other direction. Unit placement is
	// deterministic across both nodes since both compute the same ring from
	// the same membership.
	keyOwnedByN2 := findKeyOwnedBy(t, c1, "n2", 1000)
	keyOwnedByN1 := findKeyOwnedBy(t, c2, "n1", 1000)

	if err := c1.Put([]byte(keyOwnedByN2), []byte("from-n1")); err != nil {
		t.Fatalf("c1.Put(n2-key): %v", err)
	}
	if err := c2.Put([]byte(keyOwnedByN1), []byte("from-n2")); err != nil {
		t.Fatalf("c2.Put(n1-key): %v", err)
	}

	// Read from each owner's LOCAL mount to confirm the data physically
	// landed on the owning node, not merely that a routed Get round-tripped.
	got, err := c2.LocalGet([]byte(keyOwnedByN2))
	if err != nil {
		t.Fatalf("n2 local get(n2-key): %v", err)
	}
	if string(got) != "from-n1" {
		t.Fatalf("n2 local: want from-n1, got %q", got)
	}

	got, err = c1.LocalGet([]byte(keyOwnedByN1))
	if err != nil {
		t.Fatalf("n1 local get(n1-key): %v", err)
	}
	if string(got) != "from-n2" {
		t.Fatalf("n1 local: want from-n2, got %q", got)
	}

	// Round-trip via cluster Get on the OTHER node also works (covers
	// Get-forwarding, not just Put-forwarding).
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
	n1Bind := hostPort(freePort(t))
	n2Bind := hostPort(freePort(t))

	c1, stop1 := openMultiBackendNodeAt(t, "n1", n1Bind, "")
	defer stop1()
	c2, stop2, n2GRPC := openMultiBackendNodeAtWithHarness(t, "n2", n2Bind, n1Bind)
	defer stop2()
	_ = c2

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

// openMultiBackendNodeAt brings up a multi-backend Cluster + its gRPC
// server at a known bind address, registers it for cleanup, and returns
// the cluster + teardown closure. Caller supplies bindAddr so
// peer-discovery seeds are predictable.
//
// The gRPC server is REAL and registered before the joiner dials: a
// multi-backend joiner asks a live seed for the cluster's unit generation
// during Open (learnGenerationFromSeed), so a fixture that advertises an
// address nothing serves makes the joiner's Open burn its whole
// gen-learn budget and fail.
func openMultiBackendNodeAt(t *testing.T, id, bindAddr, seedBindAddr string) (*cluster.Cluster, func()) {
	c, stop, _ := openMultiBackendNodeAtWithHarness(t, id, bindAddr, seedBindAddr)
	return c, stop
}

// openMultiBackendNodeAtWithHarness is openMultiBackendNodeAt plus the gRPC
// harness, for tests that need to kill one node's server mid-test.
func openMultiBackendNodeAtWithHarness(t *testing.T, id, bindAddr, seedBindAddr string) (*cluster.Cluster, func(), *grpcHarness) {
	t.Helper()
	grpcHarness, stop := startGRPC(t)
	cfg := cluster.Config{
		NodeID:               id,
		BackendFactory:       memfactory.New(),
		UnitCount:            storageunit.MustUnitCount(8),
		BindAddr:             bindAddr,
		GRPCAddr:             grpcHarness.addr,
		LogOutput:            io.Discard,
		RebalanceSettleDelay: 100 * time.Millisecond,
	}
	if seedBindAddr != "" {
		cfg.Seeds = []string{seedBindAddr}
	}
	c, err := cluster.Open(cfg)
	if err != nil {
		stop()
		t.Fatalf("openMultiBackendNodeAt %s: %v", id, err)
	}
	grpcHarness.register(c)
	return c, func() {
		_ = c.Close()
		stop()
	}, grpcHarness
}

// TestOpen_MountsBeforeEventsLoop pins the publish ordering in Open: the
// mount map must be initialized (initMultiBackend) BEFORE the events /
// reconcile goroutines spawn. Starting the goroutines first means the
// very first membership join calls bumpRingGen -> scheduleReconcile,
// which reads mount state while Open is concurrently writing it.
//
// With the race detector enabled (this binary always runs -race in CI),
// the regression surfaces as a data race when the first NotifyJoin
// arrives. The test drives that shape: open a seed, then join nodes to it
// so the events / reconcile loops are doing real work the moment they
// start, and verify each joiner reaches a coherent post-Open state
// without -race complaining.
//
// It lives in the EXTERNAL test package because a multi-backend joiner
// dials its seed's gRPC during Open, and pkg/rpc imports pkg/cluster, so
// an in-package test cannot stand up a server to answer.
func TestOpen_MountsBeforeEventsLoop(t *testing.T) {
	seedBind := hostPort(freePort(t))
	_, seedStop := openMultiBackendNodeAt(t, "race-seed", seedBind, "")
	defer seedStop()

	// Join a few nodes in a row. With the bug, even one is enough for
	// -race to fire; the loop shortens the odds on a quiet machine.
	for i := range 3 {
		joiner, joinerStop := openMultiBackendNodeAt(t,
			fmt.Sprintf("race-joiner-%d", i), hostPort(freePort(t)), seedBind)
		if err := waitForRingSize(joiner, 2, 5*time.Second); err != nil {
			t.Fatalf("joiner %d never saw the seed: %v", i, err)
		}
		joinerStop()
	}
}
