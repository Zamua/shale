package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/rpc"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestForwardingLoopGuard simulates a ring-divergence scenario by
// dialing a node DIRECTLY (skipping ring routing) with forwarded=true
// on a key the node does NOT own. The server must refuse with
// FailedPrecondition instead of bouncing the request back to the
// "real" owner, which on diverged rings would loop A->B->A->...
// indefinitely.
//
// Why this maps to a real scenario: in production, two nodes can
// briefly disagree on the ring during a membership transition. Node
// A thinks key K is owned by B (so it forwards to B); meanwhile B's
// ring has just updated + now thinks K is owned by A. Without a
// loop guard, B re-forwards to A, A re-forwards to B, and so on.
// The forwarded=true flag tells the receiver "I already routed
// once; you HAVE to handle this or fail loudly."
func TestForwardingLoopGuard(t *testing.T) {
	f := newThreeNodeFixture(t)

	// Pick a key + identify its real owner. We'll talk directly to a
	// node that is NOT the owner to mimic the diverged-ring case.
	const key = "loop-guard-key"
	owner := ownerOf(f.N1.Cluster, key)

	var nonOwner *testNode
	for _, n := range f.Nodes() {
		if n.ID != owner {
			nonOwner = n
			break
		}
	}
	if nonOwner == nil {
		t.Fatalf("could not find a non-owner of %q (owner=%s)", key, owner)
	}

	// Dial the non-owner directly + send a forwarded=true Put. The
	// server should refuse rather than re-forward.
	conn, err := grpc.NewClient(nonOwner.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial non-owner %s: %v", nonOwner.ID, err)
	}
	defer conn.Close()
	api := pb.NewShaleNodeClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = api.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte("v"), Forwarded: true})
	if err == nil {
		t.Fatalf("expected refused Put with forwarded=true on non-owner, got nil error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected codes.FailedPrecondition, got %v (%v)", st.Code(), err)
	}
	if !strings.Contains(st.Message(), "forwarding loop refused") {
		t.Fatalf("expected loop-refusal message, got: %q", st.Message())
	}

	// Same probe for Get + Delete + ScanPrefix - all per-key RPCs
	// must implement the guard.
	if _, err := api.Get(ctx, &pb.GetRequest{Key: []byte(key), Forwarded: true}); err == nil {
		t.Fatalf("expected refused Get with forwarded=true on non-owner")
	} else if st, _ := status.FromError(err); st.Code() != codes.FailedPrecondition {
		t.Fatalf("Get: expected FailedPrecondition, got %v", st.Code())
	}
	if _, err := api.Delete(ctx, &pb.DeleteRequest{Key: []byte(key), Forwarded: true}); err == nil {
		t.Fatalf("expected refused Delete with forwarded=true on non-owner")
	} else if st, _ := status.FromError(err); st.Code() != codes.FailedPrecondition {
		t.Fatalf("Delete: expected FailedPrecondition, got %v", st.Code())
	}

	stream, err := api.ScanPrefix(ctx, &pb.ScanPrefixRequest{Prefix: []byte(key), Forwarded: true})
	if err != nil {
		t.Fatalf("ScanPrefix open: %v", err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatalf("expected refused ScanPrefix with forwarded=true on non-owner")
	}
	if st, _ := status.FromError(err); st.Code() != codes.FailedPrecondition {
		t.Fatalf("ScanPrefix: expected FailedPrecondition, got %v", st.Code())
	}

	// Sanity: a NON-forwarded request to the same non-owner still
	// works (the server routes internally as usual). This makes sure
	// the guard only fires for forwarded=true.
	cli, err := rpc.NewClient(nonOwner.GRPCAddr)
	if err != nil {
		t.Fatalf("rpc.NewClient: %v", err)
	}
	defer cli.Close()
	if err := cli.Put(ctx, []byte(key), []byte("ok")); err != nil {
		t.Fatalf("non-forwarded Put via non-owner should succeed (routes internally), got: %v", err)
	}
}
