package cluster

// White-box tests for the single-backend replicated-write retry-on-transient
// (symmetric with the v0.8 Phase 2d multi-backend handoff retry).
//
// The CI flake these pin: a replicated Put can fall short of W in ONE fan-out
// pass purely because a replica leg came back TRANSIENT (a still-open
// StateReceiving migration-guard window, e.g. reopened by a late SWIM gossip
// join mid-write). fanout counts a transient leg toward NEITHER the ack budget
// NOR the failure budget, so such a write lands acks<W with ZERO failures. The
// fix makes putReplicated RETRY that pass (bounded by WriteTimeout) instead of
// returning Unavailable with "0 failures". A REAL failure (a down peer that
// exhausted the failure budget) must still fast-fail, unretried.
//
// Both tests drive the REAL putReplicated end-to-end against a controllable
// gRPC peer that is the SECOND replica (R=2, W=WriteAll, so a single short leg
// is enough to miss W). Determinism comes from a call-count gate on the peer,
// not from gossip timing.

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/ring"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scriptedPeer is a minimal gRPC ShaleNode whose forwarded Put returns a
// scripted gRPC code for the first transientCalls invocations, then succeeds.
// failCode lets a test pick the transient code (ResourceExhausted, the
// migration-guard sentinel) or a real down-peer code (Unavailable).
type scriptedPeer struct {
	pb.UnimplementedShaleNodeServer
	failCode       codes.Code
	transientCalls int32 // first N Put calls fail with failCode; rest succeed
	calls          atomic.Int32
}

func (s *scriptedPeer) Put(_ context.Context, _ *pb.PutRequest) (*pb.PutResponse, error) {
	n := s.calls.Add(1)
	if n <= s.transientCalls {
		return nil, status.Errorf(s.failCode, "scripted peer refusal #%d", n)
	}
	return &pb.PutResponse{}, nil
}

func startScriptedPeer(t *testing.T, p *scriptedPeer) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterShaleNodeServer(srv, p)
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

// openRetryTestCluster stands up a single local node at R=2 / WriteAll with the
// scripted peer hand-injected as the second ring member, so LocateKeyN(key, 2)
// returns (local, peer) and W = 2. The reconciler + bootstrap Evaluate are
// parked far in the future so the hand-injected peer is never torn out of the
// ring or treated as a migration target.
func openRetryTestCluster(t *testing.T, peerAddr string, writeTimeout time.Duration) *Cluster {
	t.Helper()
	saved := reconcileInterval
	reconcileInterval = time.Hour
	t.Cleanup(func() { reconcileInterval = saved })

	c, err := Open(Config{
		NodeID:               "retry-local",
		Backend:              memory.New(),
		LogOutput:            io.Discard,
		ReplicationFactor:    2,
		WriteConsistency:     WriteAll, // W = R = 2: a single short leg misses W
		ReadConsistency:      ReadNearest,
		WriteTimeout:         writeTimeout,
		ReadTimeout:          writeTimeout,
		BindAddr:             hp(freeTCPPort(t)),
		GRPCAddr:             "127.0.0.1:1",
		RebalanceSettleDelay: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if !waitForLocalInRing(c, 2*time.Second) {
		t.Fatalf("local member never landed in ring")
	}
	c.ring.Add(ring.Member{ID: "retry-peer", Addr: peerAddr})
	return c
}

// TestPutReplicated_TransientLeg_RetriesAndSucceeds pins the fix: with R=2 /
// WriteAll, a peer replica leg that returns the transient migration-guard code
// (codes.ResourceExhausted) for the first few attempts makes the fan-out land
// acks=1 (local) with ZERO failures. The single-backend write path must RETRY
// (bounded by WriteTimeout) until the transient window closes, then ack.
//
// Pre-fix, putReplicated returned codes.Unavailable on the FIRST pass with the
// telltale "0 failures" signature - exactly the CI flake.
func TestPutReplicated_TransientLeg_RetriesAndSucceeds(t *testing.T) {
	peer := &scriptedPeer{
		failCode:       codes.ResourceExhausted, // the migration-guard transient
		transientCalls: 3,                       // first 3 forwarded Puts are transient
	}
	addr, stop := startScriptedPeer(t, peer)
	defer stop()

	// Generous budget so the backoff sequence comfortably outlasts 3 transients.
	c := openRetryTestCluster(t, addr, 5*time.Second)

	start := time.Now()
	if err := c.Put([]byte("retry-key"), []byte("retry-val")); err != nil {
		t.Fatalf("Put should retry through the transient window and succeed, got %v", err)
	}
	elapsed := time.Since(start)

	// The peer must have been re-dispatched past its transient window: at least
	// transientCalls+1 forwarded Puts landed (one per fan-out attempt).
	if got := peer.calls.Load(); got < peer.transientCalls+1 {
		t.Fatalf("expected the write to retry the fan-out: peer saw %d calls, want >= %d",
			got, peer.transientCalls+1)
	}
	// It succeeded WITHIN the WriteTimeout budget (bounded latency, not a hang).
	if elapsed > 5*time.Second {
		t.Fatalf("Put took %v, past the WriteTimeout budget", elapsed)
	}
	t.Logf("retry-on-transient: succeeded after %d peer calls over %v", peer.calls.Load(), elapsed)
}

// TestPutReplicated_RealFailureLeg_FastFails pins the fast-fail guard: a peer
// that is genuinely DOWN (codes.Unavailable) exhausts the (R-W)=0 failure
// budget on the FIRST pass, so the write must return Unavailable IMMEDIATELY,
// unretried - the retry must never paper over a real outage by spinning to
// WriteTimeout.
func TestPutReplicated_RealFailureLeg_FastFails(t *testing.T) {
	peer := &scriptedPeer{
		failCode:       codes.Unavailable, // genuine down-peer, NOT transient
		transientCalls: 1 << 30,           // always fails (never a success)
	}
	addr, stop := startScriptedPeer(t, peer)
	defer stop()

	// Long budget on purpose: a fast-fail must NOT consume it.
	c := openRetryTestCluster(t, addr, 5*time.Second)

	start := time.Now()
	err := c.Put([]byte("hard-key"), []byte("hard-val"))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected Unavailable from a down replica at WriteAll, got nil")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v (%v)", status.Code(err), err)
	}
	// Fast-fail: returned well under the 5s budget, and the peer was dispatched
	// only a small number of times (one fan-out pass, possibly a client retry
	// or two inside gRPC - but NOT the dozens a spin-to-timeout would show).
	if elapsed > 1*time.Second {
		t.Fatalf("real failure took %v; must fast-fail, not spin to WriteTimeout", elapsed)
	}
	if got := peer.calls.Load(); got > 5 {
		t.Fatalf("real failure should not retry the fan-out: peer saw %d calls (spin?)", got)
	}
	t.Logf("fast-fail: returned %v after %d peer calls in %v", status.Code(err), peer.calls.Load(), elapsed)
}
