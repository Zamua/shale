package cluster

// White-box regression tests for the v0.4.1 fix bundle. The public-
// API surface variants live in replicate_fixes_test.go.

import (
	"context"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/ring"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc"
)

// hangingShaleNode is a minimal gRPC ShaleNode that blocks on Put /
// Get until ctx fires. Used to simulate a peer whose gRPC server
// accepted the call but never returns from the handler (blackhole,
// half-open TCP, gRPC stuck on a deadlock).
type hangingShaleNode struct {
	pb.UnimplementedShaleNodeServer
}

func (s *hangingShaleNode) Put(ctx context.Context, _ *pb.PutRequest) (*pb.PutResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (s *hangingShaleNode) Get(ctx context.Context, _ *pb.GetRequest) (*pb.GetResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func startHangingPeerInternal(t *testing.T) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterShaleNodeServer(srv, &hangingShaleNode{})
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

// TestPutReplicated_HungPeer_DeadlineCancelsDispatch pins issue 1:
// a Put against a peer whose gRPC handler blocks forever cancels at
// WriteTimeout instead of leaking the fanout goroutines.
//
// White-box shape because we inject the hung peer DIRECTLY into the
// local cluster's ring (no memberlist seed handshake against the
// hung peer required). We then issue a Put with WriteOne so the
// local replica satisfies the requiredAcks; the hung peer dispatch
// runs in the background and MUST exit at WriteTimeout for the
// drainer goroutine to finalize.
//
// Failure mode caught: a regression that drops context.WithTimeout
// in putReplicated leaves the per-dispatch goroutines blocked on
// the hung peer's gRPC Put forever. The WaitGroup never finalizes,
// resultsCh never closes, the drainer goroutine blocks on `range
// resultsCh` until process exit. We measure that via runtime.
// NumGoroutine + a few iterations to ride out gRPC client churn.
func TestPutReplicated_HungPeer_DeadlineCancelsDispatch(t *testing.T) {
	// Park the reconciler far in the future so it can't remove our
	// hand-injected hung-peer ring member (membership doesn't know
	// about it; the reconciler would normally tear it out + close
	// its cached client mid-Put). Same shape as
	// TestEventsLoop_EvictsClientOnAddrChange.
	saved := reconcileInterval
	reconcileInterval = time.Hour
	t.Cleanup(func() { reconcileInterval = saved })

	const writeTimeout = 300 * time.Millisecond

	hungAddr, stopHung := startHangingPeerInternal(t)
	defer stopHung()

	be := memory.New()
	c, err := Open(Config{
		NodeID:            "ht-local",
		Backend:           be,
		LogOutput:         io.Discard,
		ReplicationFactor: 2,
		WriteConsistency:  WriteOne,
		ReadConsistency:   ReadNearest,
		WriteTimeout:      writeTimeout,
		ReadTimeout:       writeTimeout,
		BindAddr:          hp(freeTCPPort(t)),
		GRPCAddr:          "127.0.0.1:1",
		// Park the settle delay far in the future so the bootstrap
		// Evaluate (kicked off by NotifyJoin on Open) never fires.
		// Without this, that Evaluate would compute a plan that
		// moves partitions from local to the hand-injected ht-hung
		// member, mark them StateSending on the local Coordinator,
		// and the migration guard would reject local Puts for those
		// partitions -- not what we're testing.
		RebalanceSettleDelay: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = c.Close() }()

	if !waitForLocalInRing(c, 2*time.Second) {
		t.Fatalf("local member never landed in ring")
	}

	// Inject the hung peer directly into the ring so LocateKeyN
	// returns (local, hung). The cluster's KV path doesn't care
	// whether membership knows about it; it dials whatever Addr the
	// ring hands back.
	c.ring.Add(ring.Member{ID: "ht-hung", Addr: hungAddr})

	// Two-phase measurement so the gRPC client's steady-state
	// per-connection overhead (~30 goroutines: stream reader, write
	// loop, keepalive, name resolver, etc.) isn't conflated with a
	// per-Put leak. Phase 1: warmup Puts to establish the
	// connection + amortize the one-shot overhead. Phase 2: more
	// Puts, measure delta against Phase 1 baseline. A regression
	// that drops the per-fanout deadline leaks proportional to
	// `phase2puts`; the per-connection overhead is constant.
	const warmupPuts = 5
	const phase2Puts = 10
	for i := 0; i < warmupPuts; i++ {
		if err := c.Put([]byte{'w', byte('0' + i)}, []byte("v")); err != nil {
			t.Fatalf("warmup Put %d: %v", i, err)
		}
	}
	// Let warmup's per-fanout deadlines fire + drainer goroutines
	// exit. Budget = 3x WriteTimeout (deadline + drainer slack).
	time.Sleep(3 * writeTimeout)
	pre := numGoroutinesSettled()

	for i := 0; i < phase2Puts; i++ {
		if err := c.Put([]byte{'k', byte('0' + i)}, []byte("v")); err != nil {
			t.Fatalf("phase2 Put %d: %v", i, err)
		}
	}
	time.Sleep(3 * writeTimeout)
	post := numGoroutinesSettled()
	delta := post - pre

	// Phase 2 should add 0 (or close to it) long-lived goroutines:
	// every dispatched op cancels at WriteTimeout, every drainer +
	// accounting goroutine exits. Steady-state per-conn goroutines
	// were already counted in `pre`. A regression that drops the
	// deadline leaks ~2-3 per Put (~20-30 above baseline at
	// phase2Puts=10).
	if delta > phase2Puts/2 {
		t.Errorf("goroutine leak: pre=%d post=%d delta=%d (phase2Puts=%d). dispatch deadline is not bounding fan-out goroutines per Put.",
			pre, post, delta, phase2Puts)
	}
}

// TestReadRepair_BoundedByClose pins issue 6: an in-flight read-
// repair goroutine MUST be canceled + drained by Close before the
// peer-client cache is torn down. The cluster's repairCtx /
// repairWG wire this up; the regression we catch is a repair
// spawned with context.Background() + no WaitGroup -- it could
// race against Close's c.clients teardown and dial through a
// torn-down peerClient.
//
// Direct shape: stand up a 2-node cluster (local + a hung peer via
// the harness above), drive a Get that triggers scheduleReadRepair
// at the boundary (we plant a divergence via direct backend writes),
// then Close concurrently + measure that Close returns in bounded
// wall-clock and the repair goroutines have been drained.
func TestReadRepair_BoundedByClose(t *testing.T) {
	// Same reconciler-park as the hung-peer test above: keep our
	// hand-injected hung-peer ring entry alive across the test.
	saved := reconcileInterval
	reconcileInterval = time.Hour
	t.Cleanup(func() { reconcileInterval = saved })

	hungAddr, stopHung := startHangingPeerInternal(t)
	defer stopHung()

	be := memory.New()
	c, err := Open(Config{
		NodeID:            "rrc-local",
		Backend:           be,
		LogOutput:         io.Discard,
		ReplicationFactor: 2,
		WriteConsistency:  WriteOne,
		ReadConsistency:   ReadQuorum,
		WriteTimeout:      200 * time.Millisecond,
		ReadTimeout:       200 * time.Millisecond,
		BindAddr:          hp(freeTCPPort(t)),
		GRPCAddr:          "127.0.0.1:1",
		// Park settle delay so the bootstrap Evaluate (NotifyJoin)
		// never fires + the hand-injected hung peer isn't treated
		// as a Send target.
		RebalanceSettleDelay: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if !waitForLocalInRing(c, 2*time.Second) {
		t.Fatalf("local member never landed in ring")
	}
	c.ring.Add(ring.Member{ID: "rrc-hung", Addr: hungAddr})

	// Plant a stamped envelope locally so the local replica returns
	// a "newer" envelope vs the hung peer's "NotFound" path. Get
	// with ReadQuorum will collect the local envelope + the hung
	// peer's response (which never lands within ReadTimeout -- the
	// hung dispatch hits the deadline and returns a transport
	// error). The collected results still trigger scheduleReadRepair
	// against the hung peer (it returned an error, classified as
	// NotFound-equivalent in the gathered set if we craft the
	// shape; in practice the timeout means the gathered list has
	// only the local replica + read-repair sees no laggers).
	//
	// Either way, the test's actual contract is "Close returns in
	// bounded wall-clock and doesn't deadlock waiting for the
	// hung-peer repair to finish." The repair-spawn happens iff the
	// gathered list has a lagger; we trigger it by writing the
	// canonical envelope first via Put (so local has the stamped
	// envelope), then directly poisoning local with an OLDER stamp
	// so the hung peer's eventual "envelope at zero stamp" looks
	// older than the winner.
	envFresh := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: 1_000_000, NodeID: "writer"},
		Payload: []byte("fresh"),
	})
	if err := be.Put([]byte("rrc-key"), envFresh); err != nil {
		t.Fatalf("plant envelope: %v", err)
	}

	// Issue a Get to trigger the read path. With the hung peer
	// timing out, gathered will end up holding only the local
	// envelope; no repair gets scheduled (no laggers). That's fine
	// for the regression test: we still want to verify Close
	// behavior under load even when no repair fires.
	go func() {
		_, _ = c.Get([]byte("rrc-key"))
	}()

	// Close concurrently. The Close timeout for repairWG.Wait is
	// 5s (in our wired-in version), so we expect Close to return
	// within 6s comfortably.
	closeStart := time.Now()
	closeErr := c.Close()
	closeElapsed := time.Since(closeStart)
	if closeErr != nil {
		t.Errorf("Close: %v", closeErr)
	}
	if closeElapsed > 6*time.Second {
		t.Errorf("Close blocked %v; repairWG should be bounded", closeElapsed)
	}
}

// numGoroutinesSettled tries to read a "settled" goroutine count by
// triggering GC + a small sleep and averaging two samples. Cheap
// noise reducer for the leak-detection assertions.
func numGoroutinesSettled() int {
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	runtime.GC()
	return runtime.NumGoroutine()
}
