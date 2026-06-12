package rpc_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rpc"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
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

// TestCommitCAS_WireRoundTrip exercises the CommitCAS handler directly
// over gRPC: a commit that applies a write-set, then a stale-read commit
// that the owner reports as a conflict (typed bool, NOT a gRPC error).
func TestCommitCAS_WireRoundTrip(t *testing.T) {
	addr, cleanup := newTestServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Seed via a CAS commit with an expect-absent read-check + a Put.
	resp, err := cli.CommitCAS(ctx, &pb.CommitCASRequest{
		PinKey: []byte("k"),
		Reads:  []*pb.ReadCheck{{Key: []byte("k"), ExpectAbsent: true}},
		Writes: []*pb.WriteOp{{Key: []byte("k"), Value: []byte("v1")}},
	})
	if err != nil {
		t.Fatalf("CommitCAS (seed): %v", err)
	}
	if !resp.GetCommitted() || resp.GetConflict() || resp.GetError() != "" {
		t.Fatalf("seed: want committed, got %+v", resp)
	}

	// A commit whose read-check expects the OLD absence now conflicts
	// (the key exists), and the write-set is not applied.
	resp, err = cli.CommitCAS(ctx, &pb.CommitCASRequest{
		PinKey: []byte("k"),
		Reads:  []*pb.ReadCheck{{Key: []byte("k"), ExpectAbsent: true}},
		Writes: []*pb.WriteOp{{Key: []byte("k"), Value: []byte("v2")}},
	})
	if err != nil {
		t.Fatalf("CommitCAS (conflict): unexpected gRPC error %v", err)
	}
	if resp.GetConflict() != true || resp.GetCommitted() || resp.GetError() != "" {
		t.Fatalf("conflict: want conflict=true, got %+v", resp)
	}

	// A commit whose read-check matches the current value commits + the
	// write-set (a delete) applies.
	resp, err = cli.CommitCAS(ctx, &pb.CommitCASRequest{
		PinKey: []byte("k"),
		Reads:  []*pb.ReadCheck{{Key: []byte("k"), ExpectedValue: []byte("v1")}},
		Writes: []*pb.WriteOp{{Key: []byte("k"), Delete: true}},
	})
	if err != nil {
		t.Fatalf("CommitCAS (delete): %v", err)
	}
	if !resp.GetCommitted() {
		t.Fatalf("delete commit: want committed, got %+v", resp)
	}
	if _, found, _ := cli.Get(ctx, []byte("k")); found {
		t.Fatalf("after delete commit: key should be absent")
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

// TestApplyBatch_WireAppliesVerbatim pins the v0.6.x replica-side handler:
// ApplyBatch writes each (key, envelope) verbatim to the local backend in
// one transaction. On a single-node (R=1) server the stored bytes are
// exactly the envelope the owner shipped (a plain Get returns raw stored
// bytes at R=1), so we can assert the verbatim apply by decoding what we
// read back. A tombstone envelope (empty payload) is written verbatim too.
func TestApplyBatch_WireAppliesVerbatim(t *testing.T) {
	addr, cleanup := newTestServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Build two already-encoded envelopes under one shared stamp (the
	// shape the owner ships): a value and a tombstone.
	stamp := cluster.Stamp{TimestampNanos: 42, NodeID: "owner-x"}
	valEnv := cluster.Encode(cluster.Envelope{Stamp: stamp, Payload: []byte("hello")})
	tombEnv := cluster.Encode(cluster.Envelope{Stamp: stamp, Payload: nil})

	resp, err := cli.ApplyBatch(ctx, &pb.ApplyBatchRequest{
		Writes: []*pb.EnvelopeWrite{
			{Key: []byte("a"), Envelope: valEnv},
			{Key: []byte("b"), Envelope: tombEnv},
		},
	})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if resp.GetError() != "" {
		t.Fatalf("ApplyBatch: unexpected error string %q", resp.GetError())
	}

	// At R=1 a Get returns the raw stored bytes; they must be the exact
	// envelope bytes the handler was handed (verbatim, no re-stamp).
	gotA, found, err := cli.Get(ctx, []byte("a"))
	if err != nil || !found {
		t.Fatalf("Get a: found=%v err=%v", found, err)
	}
	if !bytes.Equal(gotA, valEnv) {
		t.Fatalf("Get a: stored bytes are not the verbatim envelope")
	}
	envA, err := cluster.Decode(gotA)
	if err != nil {
		t.Fatalf("Decode a: %v", err)
	}
	if !bytes.Equal(envA.Payload, []byte("hello")) || envA.Stamp != stamp {
		t.Fatalf("Decode a: got payload=%q stamp=%+v", envA.Payload, envA.Stamp)
	}

	gotB, found, err := cli.Get(ctx, []byte("b"))
	if err != nil || !found {
		t.Fatalf("Get b (tombstone envelope is a present key at R=1): found=%v err=%v", found, err)
	}
	if !bytes.Equal(gotB, tombEnv) {
		t.Fatalf("Get b: tombstone envelope not stored verbatim")
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

// newMultiBackendServer spins up an rpc.Server over a MULTI-BACKEND cluster
// (single node, owns the whole unit space) so a test can freeze it for a
// cluster-wide reshard and exercise the freeze-window wire behavior. Returns the
// dial address, the cluster (so the test can drive ApplyReshardPhase), and a
// cleanup func.
func newMultiBackendServer(t *testing.T) (addr string, c *cluster.Cluster, cleanup func()) {
	t.Helper()
	c, err := cluster.Open(cluster.Config{
		NodeID:         "mb-node",
		BackendFactory: memfactory.New(),
		UnitCount:      storageunit.MustUnitCount(4),
	})
	if err != nil {
		t.Fatalf("cluster.Open (multi): %v", err)
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
	return lis.Addr().String(), c, func() {
		g.GracefulStop()
		<-done
		_ = c.Close()
	}
}

// TestCommitCAS_FrozenIsRetryableUnavailableOverWire is the P2-A regression: a
// CAS commit against a FROZEN multi-backend owner must reach the client as a
// gRPC status with codes.Unavailable (the retryable freeze-window code), NOT a
// flattened response-error string that the client would wrap as codes.Unknown.
// Without preserving the status across CommitCAS, a client classifying frozen
// writes as retryable would treat a frozen remote CAS commit as a hard failure.
func TestCommitCAS_FrozenIsRetryableUnavailableOverWire(t *testing.T) {
	addr, c, cleanup := newMultiBackendServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Freeze the node for a cluster-wide reshard toward gen 1.
	if err := c.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FREEZE, 1); err != nil {
		t.Fatalf("freeze: %v", err)
	}

	// A CAS commit while frozen: the write path refuses with errWriteFrozen
	// (codes.Unavailable). It must arrive as a gRPC status error, NOT a typed
	// response field, so the client can classify it retryable.
	_, err := cli.CommitCAS(ctx, &pb.CommitCASRequest{
		PinKey: []byte("k"),
		Reads:  []*pb.ReadCheck{{Key: []byte("k"), ExpectAbsent: true}},
		Writes: []*pb.WriteOp{{Key: []byte("k"), Value: []byte("v1")}},
	})
	if err == nil {
		t.Fatal("P2-A VIOLATED: frozen CommitCAS returned no error; want a retryable Unavailable status")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("P2-A VIOLATED: frozen CommitCAS error is not a gRPC status: %v", err)
	}
	if st.Code() != codes.Unavailable {
		t.Fatalf("P2-A VIOLATED: frozen CommitCAS code = %v, want codes.Unavailable (retryable freeze window)", st.Code())
	}

	// Unfreeze (RESUME) and confirm the same commit now succeeds at the new gen.
	if err := c.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_BISECT, 1); err != nil {
		t.Fatalf("bisect: %v", err)
	}
	if err := c.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FLIP, 1); err != nil {
		t.Fatalf("flip: %v", err)
	}
	if err := c.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_RESUME, 1); err != nil {
		t.Fatalf("resume: %v", err)
	}
	resp, err := cli.CommitCAS(ctx, &pb.CommitCASRequest{
		PinKey: []byte("k"),
		Reads:  []*pb.ReadCheck{{Key: []byte("k"), ExpectAbsent: true}},
		Writes: []*pb.WriteOp{{Key: []byte("k"), Value: []byte("v1")}},
	})
	if err != nil {
		t.Fatalf("CommitCAS after RESUME: %v", err)
	}
	if !resp.GetCommitted() {
		t.Fatalf("CommitCAS after RESUME: want committed, got %+v", resp)
	}
}

// TestReshardRPC_AdvancesGeneration: the operator-facing Reshard RPC against a
// multi-backend node doubles the unit count + advances the generation, and
// reports the {from, to} counts in the response with an empty error.
func TestReshardRPC_AdvancesGeneration(t *testing.T) {
	addr, c, cleanup := newMultiBackendServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.Reshard(ctx)
	if err != nil {
		t.Fatalf("Reshard RPC: %v", err)
	}
	if resp.GetError() != "" {
		t.Fatalf("Reshard RPC returned error string %q, want empty (success)", resp.GetError())
	}
	if resp.GetFromUnitCount() != 4 || resp.GetToUnitCount() != 8 {
		t.Fatalf("Reshard RPC reported from=%d to=%d, want from=4 to=8", resp.GetFromUnitCount(), resp.GetToUnitCount())
	}
	if _, count := c.GenStateSnapshot(); count != 8 {
		t.Fatalf("cluster unit count = %d after Reshard RPC, want 8", count)
	}
}

// TestReshardRPC_LegacyModeRefusedInResponse: the Reshard RPC against a LEGACY
// single-Backend node carries the refusal in the response Error string (NOT a
// gRPC status), matching the CommitCAS / ApplyBatch convention so the CLI can
// distinguish a refused reshard from a transport failure. This is the
// "malformed / again request is handled cleanly" path: a reshard against a node
// that cannot reshard returns a clean typed refusal, not a panic or hang.
func TestReshardRPC_LegacyModeRefusedInResponse(t *testing.T) {
	addr, cleanup := newTestServer(t) // legacy single-Backend memory cluster
	defer cleanup()
	cli := newTestClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.Reshard(ctx)
	if err != nil {
		t.Fatalf("Reshard RPC (legacy) returned a transport error %v, want a clean response with Error set", err)
	}
	if resp.GetError() == "" {
		t.Fatal("Reshard RPC against a legacy node returned empty Error, want a refusal string")
	}
	if resp.GetFromUnitCount() != 0 || resp.GetToUnitCount() != 0 {
		t.Fatalf("legacy refusal reported from=%d to=%d, want 0,0 (nothing moved)", resp.GetFromUnitCount(), resp.GetToUnitCount())
	}
}

// TestReshardRPC_ConcurrentRejectedInResponse: while one reshard is in flight
// (held open via the cluster's entry hook), a SECOND Reshard RPC is rejected
// with the in-flight refusal in the response Error string, rather than queuing
// behind the first. Deterministic via the entry hook.
func TestReshardRPC_ConcurrentRejectedInResponse(t *testing.T) {
	addr, c, cleanup := newMultiBackendServer(t)
	defer cleanup()
	cli := newTestClient(t, addr)

	inReshard := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	c.TestingSetReshardEntryHook(func() {
		once.Do(func() { close(inReshard) })
		<-release
	})

	// First reshard via a direct cluster call so it parks in the hook holding
	// reshardMu (driving it over the wire too would need a second client; the
	// in-flight guard is identical whichever caller holds the lock).
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := c.TriggerReshard()
		firstDone <- err
	}()
	<-inReshard

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.Reshard(ctx)
	if err != nil {
		t.Fatalf("concurrent Reshard RPC returned a transport error %v, want a clean in-flight refusal", err)
	}
	if resp.GetError() == "" {
		t.Fatal("concurrent Reshard RPC returned empty Error, want the in-flight refusal string")
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first reshard: %v", err)
	}
}
