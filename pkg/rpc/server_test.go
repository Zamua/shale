package rpc_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rpc"
	"google.golang.org/grpc"
)

// newTestServer spins up an rpc.Server over a memory-backed cluster on
// an OS-assigned port. Returns the dial address + a cleanup func that
// stops the gRPC server and closes the cluster.
func newTestServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()

	c, err := cluster.Open(cluster.Config{
		NodeID:  "test-node",
		Backend: memory.New(),
	})
	if err != nil {
		t.Fatalf("cluster.Open: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	g := grpc.NewServer()
	rpc.NewServer(c).Register(g)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = g.Serve(lis)
	}()

	return lis.Addr().String(), func() {
		g.GracefulStop()
		<-done
		_ = c.Close()
	}
}

func newTestClient(t *testing.T, addr string) *rpc.Client {
	t.Helper()
	cli, err := rpc.NewClient(addr)
	if err != nil {
		t.Fatalf("rpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func TestRoundTripPutGetDelete(t *testing.T) {
	addr, cleanup := newTestServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	key, val := []byte("alpha"), []byte("one")

	if err := cli.Put(ctx, key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, found, err := cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatalf("Get: want found, got not_found")
	}
	if string(got) != string(val) {
		t.Fatalf("Get: want %q, got %q", val, got)
	}

	if err := cli.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, found, err = cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if found {
		t.Fatalf("Get after delete: want not_found, got found")
	}
}

func TestPing(t *testing.T) {
	addr, cleanup := newTestServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := cli.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestTopologySingleNode(t *testing.T) {
	addr, cleanup := newTestServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := cli.Topology(ctx)
	if err != nil {
		t.Fatalf("Topology: %v", err)
	}
	if !resp.GetSingleNode() {
		t.Fatalf("Topology: want single_node=true")
	}
	if resp.GetNodeId() != "test-node" {
		t.Fatalf("Topology: want node_id=test-node, got %q", resp.GetNodeId())
	}
	if got := len(resp.GetNodes()); got != 1 {
		t.Fatalf("Topology: want 1 node, got %d", got)
	}
	if resp.GetNodes()[0].GetNodeId() != "test-node" {
		t.Fatalf("Topology: want nodes[0].node_id=test-node, got %q", resp.GetNodes()[0].GetNodeId())
	}
}

func TestStatsCountersAndKeyCount(t *testing.T) {
	addr, cleanup := newTestServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Drive a known mix: 3 puts, 1 get, 1 delete.
	for _, k := range []string{"a", "b", "c"} {
		if err := cli.Put(ctx, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	if _, _, err := cli.Get(ctx, []byte("a")); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := cli.Delete(ctx, []byte("c")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	resp, err := cli.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if resp.GetPuts() != 3 {
		t.Fatalf("puts: want 3, got %d", resp.GetPuts())
	}
	if resp.GetGets() != 1 {
		t.Fatalf("gets: want 1, got %d", resp.GetGets())
	}
	if resp.GetDeletes() != 1 {
		t.Fatalf("deletes: want 1, got %d", resp.GetDeletes())
	}
	// The Stats RPC doesn't drive a ScanPrefix RPC, so scans stays 0
	// for this test mix. The dedicated ScanPrefix test exercises the
	// counter.
	if resp.GetScans() != 0 {
		t.Fatalf("scans: want 0, got %d", resp.GetScans())
	}
	if resp.GetKeysHeld() != 2 {
		t.Fatalf("keys_held: want 2 (a + b after c deleted), got %d", resp.GetKeysHeld())
	}
}

func TestScanPrefixStreams(t *testing.T) {
	addr, cleanup := newTestServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for _, k := range []string{"users/alice", "users/bob", "pastes/x"} {
		if err := cli.Put(ctx, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	stream, err := cli.ScanPrefix(ctx, []byte("users/"))
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}

	var keys []string
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		keys = append(keys, string(msg.GetKey()))
	}

	if len(keys) != 2 || keys[0] != "users/alice" || keys[1] != "users/bob" {
		t.Fatalf("ScanPrefix: want [users/alice users/bob], got %v", keys)
	}

	stats, err := cli.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.GetScans() != 1 {
		t.Fatalf("scans counter: want 1, got %d", stats.GetScans())
	}
}
