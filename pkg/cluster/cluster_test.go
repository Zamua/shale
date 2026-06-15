package cluster_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// freePort returns an OS-assigned ephemeral port that's free on both
// TCP and UDP. memberlist binds both protocols on the same port, so a
// TCP-only probe can hand back a port already taken on UDP, causing
// flaky bind failures under load (especially on CI). Probe both, retry
// on collision.
func freePort(t *testing.T) int {
	t.Helper()
	for range 16 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			continue
		}
		port := l.Addr().(*net.TCPAddr).Port
		udp, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", port))
		_ = l.Close()
		if err != nil {
			continue
		}
		_ = udp.Close()
		return port
	}
	t.Fatalf("freePort: exhausted 16 attempts to find a port free on both TCP+UDP")
	return 0
}

func hostPort(port int) string {
	return "127.0.0.1:" + strconv.Itoa(port)
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

// waitForWriteReady gates a freshly-stood-up multi-node cluster on the
// one condition the membership + rebalance-idle waits do NOT cover: that
// the routed + replicated write path actually round-trips at the
// cluster's configured WriteConsistency, from EVERY node, for keys that
// land on EVERY owner. Ring convergence proves every node is a MEMBER; it
// does NOT prove (a) every peer's gRPC HTTP/2 server goroutine is
// servicing connections yet, nor (b) that every node has closed the
// StateReceiving window its bootstrap rebalance opened. Either gap makes
// a fan-out leg come back transient (a peer's server not accepting yet,
// or a local-self leg hitting the migration guard), and a transient leg
// counts as NEITHER ack NOR failure - so a WriteAll R3 lands 2 acks, 0
// failures and the test's seed Put fails "write needed 3 acks, got 2
// (0 failures: <nil>)".
//
// The single-node, single-key probe the prior fix used only warmed ONE
// (origin-node, replica-set) pair, ONCE. Two gaps remained that this gate
// closes:
//
//  1. SPREAD across nodes + keys. The tests fire their seed Put through
//     DIFFERENT nodes (nodes[0]/nodes[1]/nodes[2]) and against many keys,
//     so a node whose receiving window was still open - or whose server
//     lagged - for a DIFFERENT key range was never proven ready. This
//     gate probes a spread of keys (enough that, under consistent
//     hashing, every member is the local-self leg for at least one)
//     THROUGH EVERY node.
//  2. STABILITY across time. The rebalance-idle wait can return idle,
//     then a LATE-arriving SWIM gossip join notification (push/pull sync
//     still converging under load) fires a fresh debounced reconcile that
//     reopens a StateReceiving window MID-TEST - so a write partway
//     through a test fails even though the first writes succeeded. A
//     single passing probe does not prove the ring will stay stable. This
//     gate therefore requires a full CLEAN SWEEP: every (node, key) probe
//     must succeed within ONE uninterrupted pass. Any transient anywhere
//     in a pass restarts the whole pass. Completing a clean sweep proves
//     the ring held stable (no reopened receiving window, every server
//     accepting) for the full duration of that pass - the deterministic
//     stand-in for "the late gossip rounds have drained."
//
// It retries ONLY transient warmup errors (Unavailable: refused dial /
// not enough acks yet; ResourceExhausted: a partition still mid-
// migration) until a clean sweep completes or the bounded deadline. A
// non-transient error, or exhausting the deadline, fails the test loudly.
//
// CRUCIAL: this retry is scoped to SETUP. Once it returns the cluster is
// proven write-ready and any Unavailable a test then sees in its body is
// a REAL failure - this helper never wraps those. The probe keys are
// namespaced + tombstoned afterward so they cannot pollute a test that
// reads replica backends directly.
func waitForWriteReady(t *testing.T, clusters []*cluster.Cluster, deadline time.Duration) {
	t.Helper()
	// 24 distinct keys give consistent hashing ample spread to cover
	// every owner as a local-self leg across a small cluster, without
	// making the warmup expensive.
	const probeKeys = 24
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		sweepClean := true
		for from, c := range clusters {
			for k := range probeKeys {
				probeKey := fmt.Appendf(nil, "__warmup_probe__/%d/%d", from, k)
				err := c.Put(probeKey, []byte("ready"))
				if err == nil {
					// Best-effort cleanup: leave no stray probe key on
					// the replica backends a test might read directly.
					_ = c.Delete(probeKey)
					continue
				}
				if !isTransientWarmupErr(err) {
					t.Fatalf("waitForWriteReady: probe write through node %d returned a non-transient error (real failure, not a warmup race): %v", from, err)
				}
				// A transient anywhere taints this sweep: the ring is not
				// stably write-ready yet. Back off and restart the sweep
				// so readiness means "a full clean pass with no reopened
				// receiving window," not "one lucky probe."
				lastErr = err
				sweepClean = false
				time.Sleep(50 * time.Millisecond)
				break
			}
			if !sweepClean {
				break
			}
		}
		if sweepClean {
			return
		}
	}
	t.Fatalf("waitForWriteReady: cluster never completed a clean write-readiness sweep within %s; last probe error: %v", deadline, lastErr)
}

// isTransientWarmupErr reports whether err is one of the transient
// signals expected while a freshly-joined cluster's gRPC forwarding
// servers come up: Unavailable (peer's server not accepting yet /
// connection refused / not enough acks landed) or ResourceExhausted (a
// partition is still mid-migration). Only these are retried during the
// setup warmup window; everything else is a real failure.
func isTransientWarmupErr(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// waitForRingSize polls c.Members() until it has want entries or the
// timeout expires.
func waitForRingSize(c *cluster.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(c.Members()) == want {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("ring size %d != want %d (members=%v)", len(c.Members()), want, c.Members())
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

// TestRemoteTx_CommitsViaCAS pins the v0.6 CAS contract for a
// single-shard transaction whose shard is owned by a REMOTE node.
// Pre-v0.6 this returned backend.ErrCrossShard (no remote proxy); under
// the CAS model the client buffers reads + writes locally and ships a
// single CommitCAS to the remote owner, which validate-and-applies.
//
// Two assertions:
//  1. A remote-pinned single-shard tx (all keys on n2, opened from n1)
//     COMMITS, and the write is visible via a routed Get afterward.
//  2. A genuinely cross-shard tx (one key on n2, one on n1) STILL fails
//     with backend.ErrCrossShard at the offending op, before anything
//     goes on the wire.
func TestRemoteTx_CommitsViaCAS(t *testing.T) {
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
	// Let any joiner-bootstrap rebalance settle so the forwarded reads /
	// CommitCAS below don't race a StateReceiving window on n2.
	for _, c := range []*cluster.Cluster{c1, c2} {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		if err := c.WaitForRebalanceIdle(ctx); err != nil {
			cancel()
			t.Fatalf("%s rebalance idle: %v", c.NodeID(), err)
		}
		cancel()
	}

	// (1) Remote single-shard tx commits via CAS. Pin on a key owned by
	// n2; do a buffered Get (records an expect_absent read-check) + a
	// Put; Commit ships ONE CommitCAS to n2.
	remoteKey := findKeyOwnedBy(t, c1, "n2", 1000)
	tx, err := c1.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Get([]byte(remoteKey)); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("buffered Get of absent remote key: want ErrNotFound, got %v", err)
	}
	if err := tx.Put([]byte(remoteKey), []byte("v")); err != nil {
		t.Fatalf("buffered Put on remote shard: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit of remote single-shard tx: want nil, got %v", err)
	}
	// The committed write must be visible via a routed Get from n1.
	got, err := c1.Get([]byte(remoteKey))
	if err != nil {
		t.Fatalf("Get after remote commit: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get after remote commit: want %q, got %q", "v", got)
	}

	// (2) Genuinely cross-shard tx still fails with ErrCrossShard. Pin on
	// a key owned by n2, then touch a key owned by n1 (different owner):
	// the cross-shard guard fires at the offending op, before any wire.
	localKey := findKeyOwnedBy(t, c1, "n1", 1000)
	tx2, err := c1.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	defer func() { _ = tx2.Rollback() }()
	if err := tx2.Put([]byte(remoteKey), []byte("a")); err != nil {
		t.Fatalf("tx2 first Put (pins n2): %v", err)
	}
	if err := tx2.Put([]byte(localKey), []byte("b")); !errors.Is(err, backend.ErrCrossShard) {
		t.Fatalf("tx2 cross-shard Put: want ErrCrossShard, got %v", err)
	}
	if _, err := tx2.Get([]byte(localKey)); !errors.Is(err, backend.ErrCrossShard) {
		t.Fatalf("tx2 cross-shard Get: want ErrCrossShard, got %v", err)
	}
}

// TestTransact_RemoteContention drives the CAS retry loop over the WIRE:
// both nodes concurrently Transact a counter pinned to a key owned by n2,
// so n1's increments forward a CommitCAS RPC to n2 while n2's run the
// in-process fast-path. The owner's casCommitMu must serialize all of
// them so the counter converges to the exact increment count (no lost
// update across the local + remote commit paths).
func TestTransact_RemoteContention(t *testing.T) {
	n1Mem := memory.New()
	n2Mem := memory.New()

	n1MemberPort := freePort(t)
	n2MemberPort := freePort(t)

	n1GRPC, n1stop := startGRPC(t)
	defer n1stop()
	n2GRPC, n2stop := startGRPC(t)
	defer n2stop()

	c1, err := cluster.Open(cluster.Config{
		NodeID: "n1", Backend: n1Mem,
		BindAddr: hostPort(n1MemberPort), GRPCAddr: n1GRPC.addr, LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("open n1: %v", err)
	}
	defer func() { _ = c1.Close() }()
	n1GRPC.register(c1)

	c2, err := cluster.Open(cluster.Config{
		NodeID: "n2", Backend: n2Mem,
		BindAddr: hostPort(n2MemberPort), GRPCAddr: n2GRPC.addr,
		Seeds: []string{hostPort(n1MemberPort)}, LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("open n2: %v", err)
	}
	defer func() { _ = c2.Close() }()
	n2GRPC.register(c2)

	for _, c := range []*cluster.Cluster{c1, c2} {
		if err := waitForRingSize(c, 2, 5*time.Second); err != nil {
			t.Fatalf("%s ring: %v", c.NodeID(), err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		if err := c.WaitForRebalanceIdle(ctx); err != nil {
			cancel()
			t.Fatalf("%s rebalance idle: %v", c.NodeID(), err)
		}
		cancel()
	}

	// Raise the retry budget so heavy single-key contention across the
	// local + remote commit paths still converges (the default 10 can be
	// tight when many goroutines hammer one key).
	oldMax := cluster.CASMaxAttempts
	cluster.CASMaxAttempts = 200
	defer func() { cluster.CASMaxAttempts = oldMax }()

	counter := findKeyOwnedBy(t, c1, "n2", 1000)
	if err := c1.Put([]byte(counter), []byte("0")); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	const perNode = 15
	inc := func(c *cluster.Cluster, wg *sync.WaitGroup) {
		defer wg.Done()
		err := c.Transact([]byte(counter), func(tx backend.Transaction) error {
			cur, err := tx.Get([]byte(counter))
			if err != nil {
				return err
			}
			var n int
			_, _ = fmt.Sscanf(string(cur), "%d", &n)
			return tx.Put([]byte(counter), []byte(strconv.Itoa(n+1)))
		})
		if err != nil {
			t.Errorf("Transact: %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2 * perNode)
	for range perNode {
		go inc(c1, &wg) // forwards CommitCAS to n2
		go inc(c2, &wg) // in-process fast-path on n2
	}
	wg.Wait()

	got, err := c1.Get([]byte(counter))
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	if string(got) != strconv.Itoa(2*perNode) {
		t.Fatalf("counter: want %d, got %q", 2*perNode, got)
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

// TestTwoNode_RebalanceOnMembershipGrowth wires the v0.3 cluster
// integration end-to-end:
//
//  1. Open 2 nodes with empty memory backends.
//  2. Put 100 keys via node1; some land on each node.
//  3. Add a 3rd node; the v0.3 rebalance hook on membership join
//     fires Evaluate, which migrates the partitions whose new
//     owner is n3.
//  4. WaitForRebalanceIdle on every node.
//  5. Read each node's backend DIRECTLY (bypassing routing): every
//     key the new ring assigns to a given node must physically live
//     on that node's backend. No key is lost.
//
// Tunables: short settle + grace keep the test under 10s while
// preserving the v0.3 protocol semantics. Sweep tick is shrunk via
// the same hook integration tests use.
func TestTwoNode_RebalanceOnMembershipGrowth(t *testing.T) {
	rebalance.SetSweepInterval(50 * time.Millisecond)

	n1Mem := memory.New()
	n2Mem := memory.New()

	n1BindAddr := hostPort(freePort(t))
	n2BindAddr := hostPort(freePort(t))

	n1Cluster, n1Stop := openClusterNodeAt(t, "rb-n1", n1BindAddr, "", n1Mem)
	defer n1Stop()
	n2Cluster, n2Stop := openClusterNodeAt(t, "rb-n2", n2BindAddr, n1BindAddr, n2Mem)
	defer n2Stop()

	if err := waitForRingSize(n1Cluster, 2, 5*time.Second); err != nil {
		t.Fatalf("n1 ring: %v", err)
	}
	if err := waitForRingSize(n2Cluster, 2, 5*time.Second); err != nil {
		t.Fatalf("n2 ring: %v", err)
	}

	keys := make([]string, 100)
	for i := range 100 {
		k := fmt.Sprintf("rb-%04d", i)
		if err := putWithMigrationRetry(n1Cluster, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		keys[i] = k
	}

	if total := countBackend(t, n1Mem) + countBackend(t, n2Mem); total != 100 {
		t.Fatalf("pre-growth total keys = %d, want 100", total)
	}

	n3Mem := memory.New()
	n3BindAddr := hostPort(freePort(t))
	n3Cluster, n3Stop := openClusterNodeAt(t, "rb-n3", n3BindAddr, n1BindAddr, n3Mem)
	defer n3Stop()

	for _, c := range []*cluster.Cluster{n1Cluster, n2Cluster, n3Cluster} {
		if err := waitForRingSize(c, 3, 5*time.Second); err != nil {
			t.Fatalf("3-node ring on %s: %v", c.NodeID(), err)
		}
	}

	// Wait for each node's Coordinator to settle. Bounded so a stuck
	// migration fails loud.
	for _, c := range []*cluster.Cluster{n1Cluster, n2Cluster, n3Cluster} {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		if err := c.WaitForRebalanceIdle(ctx); err != nil {
			cancel()
			t.Fatalf("%s did not idle: %v", c.NodeID(), err)
		}
		cancel()
	}

	// Verify physical placement matches the 3-node ring. For every
	// key, the node the ring assigns must be the only physical
	// holder. WaitForRebalanceIdle above blocks through the
	// HandedOff -> sweep -> Done cycle (HandedOff is non-terminal), so
	// the source-side copies are already dropped by the time it
	// returns; no settle sleep is needed. We still poll the placement
	// check briefly so a late reconcile re-arm cannot race the
	// assertion, but the loop returns the instant placement is correct.
	r := ring.New()
	for _, m := range n1Cluster.Members() {
		r.Add(m)
	}
	backends := map[string]*memory.Memory{
		"rb-n1": n1Mem,
		"rb-n2": n2Mem,
		"rb-n3": n3Mem,
	}
	missing := 0
	wrong := 0
	placed := pollUntil(5*time.Second, 50*time.Millisecond, func() bool {
		missing, wrong = 0, 0
		for _, k := range keys {
			owner := r.LocateKey([]byte(k)).ID
			got, err := backends[owner].Get([]byte(k))
			if err != nil {
				missing++
				continue
			}
			if string(got) != "v" {
				wrong++
			}
		}
		return missing == 0 && wrong == 0
	})
	if !placed {
		if missing > 0 {
			t.Fatalf("%d/%d keys missing on the ring-owner's backend", missing, len(keys))
		}
		t.Fatalf("%d/%d keys had wrong value on the ring-owner's backend", wrong, len(keys))
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
// transient codes: ResourceExhausted from the migration-window write
// rejection (docs/SPEC.md "Cutover") and FailedPrecondition from
// the forwarding loop-guard. Per docs/SPEC.md the SDK is expected
// to do this; tests do too so the assertion checks "rebalance
// eventually succeeds" rather than "no transient rejection during
// bootstrap." Unavailable is intentionally NOT retried here: it
// signals a real peer-down condition, distinct from a mid-handoff
// transient (which uses ResourceExhausted as of v0.4).
func putWithMigrationRetry(c *cluster.Cluster, key, value []byte) error {
	var lastErr error
	for range 50 {
		err := c.Put(key, value)
		if err == nil {
			return nil
		}
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.ResourceExhausted || st.Code() == codes.FailedPrecondition {
				lastErr = err
				time.Sleep(50 * time.Millisecond)
				continue
			}
		}
		return err
	}
	return lastErr
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
