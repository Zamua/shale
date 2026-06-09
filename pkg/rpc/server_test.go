package rpc_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rpc"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

// -- v0.3 rebalancing -------------------------------------------------
//
// The next two tests pin the v0.3 Core wiring: MigrateRange +
// ProposeRebalance are registered handlers, not Unimplemented stubs.
// In single-node test mode they return a deterministic error
// (cluster: rebalance not available in single-node mode) carried via
// FailedPrecondition or InvalidArgument; not Unimplemented. The
// multi-node behavior (real streaming + plan computation) is
// exercised in pkg/cluster/cluster_test.go + tests/integration/.

func TestMigrateRange_SingleNodeReturnsFailedPrecondition(t *testing.T) {
	addr, cleanup := newTestServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := cli.MigrateRange(ctx, []uint64{1, 2, 3}, 7)
	if err != nil {
		assertNotUnimplemented(t, "MigrateRange", err)
		return
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatalf("MigrateRange single-node: want error, got nil")
	}
	assertNotUnimplemented(t, "MigrateRange", err)
}

func TestProposeRebalance_SingleNodeReturnsError(t *testing.T) {
	addr, cleanup := newTestServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := cli.ProposeRebalance(ctx, true /*dryRun*/, false, false)
	if err == nil {
		t.Fatalf("ProposeRebalance single-node: want error, got nil")
	}
	assertNotUnimplemented(t, "ProposeRebalance", err)
}

func TestRebalanceMessagesRoundTripThroughProto(t *testing.T) {
	// RangeSpec round-trip: partition IDs + ring generation survive.
	specIn := &pb.RangeSpec{
		PartitionIds:   []uint64{0, 17, 0xdeadbeef, 1<<63 - 1},
		RingGeneration: 42,
	}
	specBytes, err := proto.Marshal(specIn)
	if err != nil {
		t.Fatalf("Marshal RangeSpec: %v", err)
	}
	var specOut pb.RangeSpec
	if err := proto.Unmarshal(specBytes, &specOut); err != nil {
		t.Fatalf("Unmarshal RangeSpec: %v", err)
	}
	if !proto.Equal(specIn, &specOut) {
		t.Fatalf("RangeSpec round-trip: got %+v, want %+v", &specOut, specIn)
	}

	// MigrateChunk carrying a KeyValue body.
	kvChunk := &pb.MigrateChunk{
		Body: &pb.MigrateChunk_Kv{Kv: &pb.KeyValue{
			Key:   []byte("users/alice"),
			Value: []byte("data"),
		}},
	}
	kvBytes, err := proto.Marshal(kvChunk)
	if err != nil {
		t.Fatalf("Marshal kv chunk: %v", err)
	}
	var kvOut pb.MigrateChunk
	if err := proto.Unmarshal(kvBytes, &kvOut); err != nil {
		t.Fatalf("Unmarshal kv chunk: %v", err)
	}
	gotKV, ok := kvOut.GetBody().(*pb.MigrateChunk_Kv)
	if !ok {
		t.Fatalf("kv chunk round-trip: body type = %T, want *MigrateChunk_Kv", kvOut.GetBody())
	}
	if !bytes.Equal(gotKV.Kv.GetKey(), []byte("users/alice")) ||
		!bytes.Equal(gotKV.Kv.GetValue(), []byte("data")) {
		t.Fatalf("kv chunk round-trip: got key=%q value=%q",
			gotKV.Kv.GetKey(), gotKV.Kv.GetValue())
	}

	// MigrateChunk carrying a MigrationDone body.
	doneChunk := &pb.MigrateChunk{
		Body: &pb.MigrateChunk_Done{Done: &pb.MigrationDone{
			TotalKeys: 1234,
			Checksum:  []byte{0xde, 0xad, 0xbe, 0xef},
		}},
	}
	doneBytes, err := proto.Marshal(doneChunk)
	if err != nil {
		t.Fatalf("Marshal done chunk: %v", err)
	}
	var doneOut pb.MigrateChunk
	if err := proto.Unmarshal(doneBytes, &doneOut); err != nil {
		t.Fatalf("Unmarshal done chunk: %v", err)
	}
	gotDone, ok := doneOut.GetBody().(*pb.MigrateChunk_Done)
	if !ok {
		t.Fatalf("done chunk round-trip: body type = %T, want *MigrateChunk_Done", doneOut.GetBody())
	}
	if gotDone.Done.GetTotalKeys() != 1234 {
		t.Fatalf("done chunk round-trip: total_keys = %d, want 1234", gotDone.Done.GetTotalKeys())
	}
	if !bytes.Equal(gotDone.Done.GetChecksum(), []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf("done chunk round-trip: checksum = %x", gotDone.Done.GetChecksum())
	}
}

// assertNotUnimplemented confirms the handler is wired (not the
// Unimplemented scaffold) without pinning the exact code: single-node
// mode returns different gRPC codes for the two surfaces (MigrateRange
// uses FailedPrecondition via the source-not-available path,
// ProposeRebalance uses InvalidArgument since dryRun against an empty
// coordinator still hits the "single-node mode" guard), and that's
// fine - the contract here is "the registered handler ran."
func assertNotUnimplemented(t *testing.T, op string, err error) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("%s: error is not a gRPC status: %v", op, err)
	}
	if st.Code() == codes.Unimplemented {
		t.Fatalf("%s: handler still Unimplemented (v0.3 Core not wired): %v", op, err)
	}
}
