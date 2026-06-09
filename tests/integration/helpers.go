// Package integration drives shale's multi-node code paths end to end:
// real memberlist gossip over loopback UDP, real gRPC forwarding between
// in-process Cluster instances, real consistent-hash routing. The shared
// fixtures live here; each *_test.go file builds on them.
//
// Why a separate tests/ tree (vs adding to pkg/cluster/cluster_test.go):
// the cluster package's own tests cover the routing decision at the
// boundary; this tree covers the wired-together system. Failures here
// are integration failures (one node disagrees with another about a
// ring; a forwarded RPC drops on the floor; a leaving node leaves a
// stale client cached), which the per-package tests cannot see.
package integration

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	"google.golang.org/grpc"
)

// testNode is the bundle of state behind one node in an integration
// fixture: the cluster handle the test drives, the in-memory backend
// the test peers at to confirm physical placement, and the gRPC server
// that other nodes forward requests to. Tests almost always interact
// via the Cluster; the backend handle is escape-hatch only.
type testNode struct {
	ID       string
	Cluster  *cluster.Cluster
	Backend  *memory.Memory
	BindAddr string
	GRPCAddr string

	stop func()
}

// Close tears the node down: shuts the gRPC server, closes the cluster
// (which closes membership + backend). Safe to call once.
func (n *testNode) Close() {
	if n.Cluster != nil {
		_ = n.Cluster.Close()
		n.Cluster = nil
	}
	if n.stop != nil {
		n.stop()
		n.stop = nil
	}
}

// startTestNode brings up one cluster node with its own memberlist +
// gRPC endpoint, joined to the given seed (empty = first node). The
// returned node registers itself for cleanup via t.Cleanup so tests
// that forget to Close it explicitly still tear down.
//
// Port allocation: both the memberlist BindAddr + the gRPC addr come
// from freePort, which uses the OS ephemeral-port pool. There is a
// tiny race window between "port returned" and "port bound" but
// loopback under a test process never hits a conflicting bind in
// practice; the harness in pkg/cluster relies on the same pattern.
func startTestNode(t *testing.T, id, seedAddr string) *testNode {
	t.Helper()

	mem := memory.New()
	bindAddr := hostPort(freePort(t))

	// The gRPC listener has to exist BEFORE cluster.Open so we know the
	// address to advertise via memberlist Meta. Same two-phase pattern
	// shaled's main.go uses.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startTestNode %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:    id,
		Backend:   mem,
		BindAddr:  bindAddr,
		GRPCAddr:  grpcAddr,
		LogOutput: io.Discard,
	}
	if seedAddr != "" {
		cfg.Seeds = []string{seedAddr}
	}

	c, err := cluster.Open(cfg)
	if err != nil {
		_ = lis.Close()
		t.Fatalf("startTestNode %s: cluster.Open: %v", id, err)
	}

	rpc.NewServer(c).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &testNode{
		ID:       id,
		Cluster:  c,
		Backend:  mem,
		BindAddr: bindAddr,
		GRPCAddr: grpcAddr,
		stop: func() {
			grpcSrv.GracefulStop()
			<-serveDone
		},
	}
	t.Cleanup(n.Close)
	return n
}

// waitForMembers polls c.Members() until it reports want entries or
// the deadline expires. Returns an error describing the last observed
// size so failing tests can pinpoint the divergence quickly.
func waitForMembers(c *cluster.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := len(c.Members()); got == want {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	got := c.Members()
	return fmt.Errorf("members=%d want=%d (have=%v)", len(got), want, got)
}

// waitForMembersAll polls every cluster in cs until they each report
// want members. Convergence under SWIM gossip is not synchronous - a
// fresh join can reach the seed before reaching the other peers - so
// any multi-node assertion must wait for every node to agree on the
// ring before issuing routed ops.
func waitForMembersAll(cs []*cluster.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allOK := true
		for _, c := range cs {
			if len(c.Members()) != want {
				allOK = false
				break
			}
		}
		if allOK {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	sizes := make([]int, len(cs))
	for i, c := range cs {
		sizes[i] = len(c.Members())
	}
	return fmt.Errorf("not all nodes converged to %d members: sizes=%v", want, sizes)
}

// ownerOf is the test-side mirror of cluster.ownerOf. Two rings built
// from the same Members() snapshot agree (consistent hashing is
// deterministic + hashed on Member.ID), so this faithfully previews
// which node a Put/Get would route to.
func ownerOf(c *cluster.Cluster, key string) string {
	r := ring.New()
	for _, m := range c.Members() {
		r.Add(m)
	}
	return r.LocateKey([]byte(key)).ID
}

// freePort grabs an OS-assigned ephemeral TCP port + releases the
// listener so the caller can bind it for real. The tiny race between
// release + rebind is harmless under loopback.
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
