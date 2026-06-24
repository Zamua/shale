package cluster

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/ring"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubGenServer is a minimal ShaleNode gRPC server that answers ONLY GenState.
// It returns Unavailable for the first failFirst calls, then the configured
// {gen, count} - modelling a seed that is transiently not-ready at cold start
// (its own mount has not finished, so its real server is not serving yet) and
// then comes up. calls counts every GenState attempt the joiner makes.
type stubGenServer struct {
	pb.UnimplementedShaleNodeServer
	mu        sync.Mutex
	calls     int
	failFirst int
	gen       uint64
	count     uint32
}

func (s *stubGenServer) GenState(_ context.Context, _ *pb.GenStateRequest) (*pb.GenStateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failFirst {
		return nil, status.Errorf(codes.Unavailable, "stub: not ready yet (call %d <= failFirst %d)", s.calls, s.failFirst)
	}
	return &pb.GenStateResponse{Generation: s.gen, UnitCount: s.count}, nil
}

func (s *stubGenServer) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// serveStubGen stands up stub on a fresh loopback listener and returns its addr.
func serveStubGen(t *testing.T, stub *stubGenServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	g := grpc.NewServer()
	pb.RegisterShaleNodeServer(g, stub)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return lis.Addr().String()
}

// TestLearnGenerationFromSeed_PatientRidesOutColdStartingSeed pins the cold-start
// fix: a joiner's seed-generation query RE-SWEEPS its seeds for GenLearnBudget
// rather than failing closed after a single attempt. A seed that is itself cold
// starting answers Unavailable until its mount finishes; a single-shot query
// turns that transient window into a crash-loop (the pre-fix failure mode the
// "impatient" sub-test pins), while the patient re-sweep WAITS the seed out.
func TestLearnGenerationFromSeed_PatientRidesOutColdStartingSeed(t *testing.T) {
	// Shrink the inter-sweep pause so the patient case completes in well under a
	// second; production keeps the 2s default. Restored after the test.
	savedBackoff := genLearnBackoff
	genLearnBackoff = 20 * time.Millisecond
	t.Cleanup(func() { genLearnBackoff = savedBackoff })

	t.Run("patient re-sweep waits out a transiently-unavailable seed", func(t *testing.T) {
		backing := sharedfactory.NewBacking()
		c := newReplicatedCluster(t, "joiner", 4, 2, backing, "joiner")
		c.clients = make(map[string]*peerClient) // the bare-struct helper omits the peer-client cache
		// A generous budget relative to (failFirst * (rpc + backoff)) so the seed
		// is waited out, but still bounded.
		c.cfg.GenLearnBudget = 5 * time.Second

		stub := &stubGenServer{failFirst: 3, gen: 7, count: 4}
		addr := serveStubGen(t, stub)
		c.ring.Add(ring.Member{ID: "seed-1", Addr: addr})

		if err := c.learnGenerationFromSeed(); err != nil {
			t.Fatalf("patient learnGenerationFromSeed should ride out a cold-starting seed, got: %v", err)
		}
		if got := c.genSnapshot().gen; got != storageunit.Generation(7) {
			t.Fatalf("committed generation = %d, want 7 (learned from the seed once it came up)", got)
		}
		if got := c.genSnapshot().count.N(); got != 4 {
			t.Fatalf("committed unit count = %d, want 4", got)
		}
		// It MUST have re-swept (not succeeded on the first attempt): failFirst=3
		// means success required a 4th call. Proves the retry loop, not luck.
		if calls := stub.callCount(); calls < 4 {
			t.Fatalf("expected >=4 GenState attempts (3 Unavailable + 1 success), got %d - did the joiner re-sweep?", calls)
		}
	})

	t.Run("impatient budget fails closed (the pre-fix crash-loop trigger)", func(t *testing.T) {
		backing := sharedfactory.NewBacking()
		c := newReplicatedCluster(t, "joiner", 4, 2, backing, "joiner")
		c.clients = make(map[string]*peerClient) // the bare-struct helper omits the peer-client cache
		// A 1ms budget allows ~one attempt: this is the OLD single-shot behavior.
		c.cfg.GenLearnBudget = 1 * time.Millisecond

		stub := &stubGenServer{failFirst: 1000, gen: 7, count: 4} // never ready in-test
		addr := serveStubGen(t, stub)
		c.ring.Add(ring.Member{ID: "seed-1", Addr: addr})

		err := c.learnGenerationFromSeed()
		if err == nil {
			t.Fatalf("a seed that never becomes ready within the budget must fail Open (fail closed), got nil")
		}
		// Must NOT have fallen back to gen 0 silently: Open fails, the caller
		// supervises a restart. The joiner's own genState stays at its gen-0
		// default (founder default in this harness) - never a wrong live value.
		if got := c.genSnapshot().gen; got != storageunit.Generation(0) {
			t.Fatalf("a failed gen-learn must not commit a generation, got %d", got)
		}
	})
}
