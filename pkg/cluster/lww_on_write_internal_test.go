package cluster

// White-box behavioral test for the v0.7 lock-scope change: CommitCASApply
// must RELEASE casCommitMu before the post-commit write-set fan-out, not hold
// it across the network. The public-API LWW-on-write tests (no lost update,
// read-repair-does-not-clobber, apply-if-newer directness, reordered fan-out
// convergence) live in lww_on_write_test.go; this file pins the lock-scope
// property that those rely on but cannot directly observe.
//
// Approach: stand up a node at R=2 + WriteAll with a HUNG peer injected as the
// other replica (it accepts the ApplyBatch RPC but never returns). Each CAS
// commit's fan-out blocks on that hung peer until WriteTimeout, then returns
// under-W. We fire TWO CAS commits on DIFFERENT keys concurrently:
//
//   - Lock RELEASED before fan-out (v0.7, correct): both commits validate +
//     commit locally fast, then both fan-outs block on the hung peer IN
//     PARALLEL. Total wall-clock ~= 1x WriteTimeout.
//   - Lock HELD across fan-out (the pre-v0.7 / regressed shape): the second
//     commit blocks on casCommitMu.Lock() until the first commit's fan-out
//     finishes (~WriteTimeout), THEN runs its own fan-out (~another
//     WriteTimeout). Total wall-clock ~= 2x WriteTimeout.
//
// We assert total elapsed is comfortably under 2x WriteTimeout, which a
// serialized fan-out could not achieve. Reuses the hung-peer harness
// (startHangingPeerInternal) + ring-injection pattern from
// replicate_fixes_internal_test.go.

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/ring"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc"
)

// hangingBatchNode is a minimal gRPC ShaleNode whose ApplyBatch handler
// blocks until ctx fires (the CAS write-set fan-out dispatches via ApplyBatch,
// so this is what makes a fan-out block until WriteTimeout). Put/Get inherit
// the base hanging behavior; unimplemented RPCs fall through to the embedded
// UnimplementedShaleNodeServer.
type hangingBatchNode struct {
	hangingShaleNode
}

func (s *hangingBatchNode) ApplyBatch(ctx context.Context, _ *pb.ApplyBatchRequest) (*pb.ApplyBatchResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// startHangingBatchPeer stands up the hangingBatchNode on an ephemeral port
// and returns its addr + a stop func. Mirrors startHangingPeerInternal but
// registers the ApplyBatch-hanging server.
func startHangingBatchPeer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterShaleNodeServer(srv, &hangingBatchNode{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(lis)
	}()
	return lis.Addr().String(), func() {
		srv.Stop()
		<-done
	}
}

// TestLWWOnWrite_CASLockReleasedBeforeFanOut pins the lock-scope property:
// two concurrent CAS commits on different keys do NOT serialize behind each
// other's replication latency, proving casCommitMu is released before the
// fan-out. See the file header for the timing argument.
func TestLWWOnWrite_CASLockReleasedBeforeFanOut(t *testing.T) {
	// Park the reconciler so it can't tear out our hand-injected hung peer
	// (membership doesn't know about it). Same guard the hung-peer Put test
	// uses.
	saved := reconcileInterval
	reconcileInterval = time.Hour
	t.Cleanup(func() { reconcileInterval = saved })

	const writeTimeout = 300 * time.Millisecond

	hungAddr, stopHung := startHangingBatchPeer(t)
	defer stopHung()

	be := memory.New()
	c, err := Open(Config{
		NodeID:            "lockscope-local",
		Backend:           be,
		LogOutput:         io.Discard,
		ReplicationFactor: 2,
		WriteConsistency:  WriteAll, // W=2 -> the fan-out MUST collect the hung peer's ack, which never comes.
		ReadConsistency:   ReadNearest,
		WriteTimeout:      writeTimeout,
		ReadTimeout:       writeTimeout,
		BindAddr:          hp(freeTCPPort(t)),
		GRPCAddr:          "127.0.0.1:1",
		// Park settle delay so the bootstrap Evaluate never fires + the hand-
		// injected peer isn't treated as a Send target (which would trip the
		// migration guard on local writes).
		RebalanceSettleDelay: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = c.Close() }()

	if !waitForLocalInRing(c, 2*time.Second) {
		t.Fatalf("local member never landed in ring")
	}
	// Inject the hung peer so LocateKeyN returns (local, hung) for every key:
	// the CAS write-set fan-out dispatches to the hung peer + blocks until
	// WriteTimeout.
	c.ring.Add(ring.Member{ID: "lockscope-hung", Addr: hungAddr})

	// Sanity: replicated path is active (R>1 + populated ring) so the fan-out
	// actually runs.
	if !c.casReplicated() {
		t.Fatalf("expected casReplicated()==true at R=2 with a populated ring")
	}

	// One CommitCASApply on key. It validates + commits locally (fast), then
	// fans the write-set out to the hung peer, which blocks until WriteTimeout
	// and returns under-W (codes.Unavailable). The local commit lands
	// regardless; we don't care about the returned error here, only the
	// timing.
	commit := func(key []byte) {
		writes := []backend.WriteOp{{Key: key, Value: []byte("v")}}
		// No read-set: a fresh-key write. CommitCASApply validates the (empty)
		// read-set, commits locally, releases the lock, then fans out.
		_ = c.CommitCASApply(context.Background(), backend.SnapshotIsolation, key, nil, writes)
	}

	// Fire two commits on DIFFERENT keys concurrently + time the whole batch.
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); commit([]byte("lock-key-a")) }()
	go func() { defer wg.Done(); commit([]byte("lock-key-b")) }()
	wg.Wait()
	elapsed := time.Since(start)

	// If casCommitMu were held across the fan-out, the second commit would
	// block on the lock until the first's fan-out hit WriteTimeout, then run
	// its own fan-out: ~2x writeTimeout total. With the lock released before
	// the fan-out, both fan-outs overlap: ~1x writeTimeout. Assert well under
	// 2x (use 1.8x as the threshold to leave slack for scheduling + the
	// gRPC dial, while still being unreachable by a serialized run).
	threshold := time.Duration(float64(writeTimeout) * 1.8)
	if elapsed >= threshold {
		t.Fatalf("two CAS commits on different keys took %v (>= %v): casCommitMu appears HELD across the fan-out (serialized behind replication latency). It must be released before fan-out.",
			elapsed, threshold)
	}

	// Both local commits must have landed despite the under-W fan-out (the
	// fan-out is best-effort-to-W after the owner-local commit). Decode each
	// key's stored envelope to confirm.
	for _, key := range [][]byte{[]byte("lock-key-a"), []byte("lock-key-b")} {
		raw, gerr := be.Get(key)
		if gerr != nil {
			t.Fatalf("local backend missing %q after commit: %v", key, gerr)
		}
		env, derr := Decode(raw)
		if derr != nil {
			t.Fatalf("decode %q: %v", key, derr)
		}
		if string(env.Payload) != "v" {
			t.Errorf("%q payload %q want v", key, env.Payload)
		}
	}
}
