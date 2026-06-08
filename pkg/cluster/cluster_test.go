package cluster_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
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
	defer c1.Close()
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
	defer c2.Close()
	n2GRPC.register(c2)

	if err := waitForRingSize(c1, 2, 5*time.Second); err != nil {
		t.Fatalf("n1 ring: %v", err)
	}
	if err := waitForRingSize(c2, 2, 5*time.Second); err != nil {
		t.Fatalf("n2 ring: %v", err)
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

// freePort returns an OS-assigned ephemeral TCP port. The listener is
// closed before return; the small race window is fine for loopback.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
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

// findKeyOwnedBy scans synthetic keys until it finds one whose owner
// (as the given cluster sees it) is wantOwner. The lookup uses the
// public Members() snapshot + a freshly-constructed ring, so it sees
// exactly the routing decision Put/Get would make. Bounded by max
// iterations so a misconfigured test fails loudly instead of looping.
func findKeyOwnedBy(t *testing.T, c *cluster.Cluster, wantOwner string, max int) string {
	t.Helper()
	if len(c.Members()) < 2 {
		t.Fatalf("findKeyOwnedBy: ring needs >=2 members, got %v", c.Members())
	}
	for i := 0; i < max; i++ {
		k := fmt.Sprintf("probe-%d", i)
		if ownerFor(c, k) == wantOwner {
			return k
		}
	}
	t.Fatalf("could not find a key owned by %s in %d probes", wantOwner, max)
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
		defer it.Close()
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
	if len(results) != 1 || results[0].(int) != 2 {
		t.Fatalf("expected single-node aggregate count=2, got %v", results)
	}
}
