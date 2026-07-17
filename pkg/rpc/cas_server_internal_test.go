package rpc

// White-box tests for casCommitResponse: the CASResult -> CommitCAS wire
// mapping. The check order is load-bearing - Committed is tested FIRST so a
// committed-but-under-replicated result (outcome (c): Committed=true,
// UnderReplicated=true, Err=codes.Unavailable) crosses the wire as
// {committed, under_replicated} with NO gRPC error, and a forwarding node
// treats it as success rather than re-committing an already-durable write.
// See docs/SPEC.md "The four commit outcomes".

import (
	"errors"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestCasCommitResponse_UnderReplicated_CommittedNoGrpcError pins outcome (c)
// over the wire: the durable-but-under-W commit maps to a response with
// committed=true AND under_replicated=true and NO gRPC error. Before the fix
// this hit the Err branch and, because the under-W error carries a gRPC
// status, returned (nil, codes.Unavailable) - which the forwarding client
// classified retryable and re-committed.
func TestCasCommitResponse_UnderReplicated_CommittedNoGrpcError(t *testing.T) {
	underW := status.Error(codes.Unavailable, "shale: write needed 3 acks, got 2")
	resp, err := casCommitResponse(backend.CASResult{Committed: true, UnderReplicated: true, Err: underW})
	if err != nil {
		t.Fatalf("an under-replicated (durable) commit must NOT surface a gRPC error; got %v", err)
	}
	if !resp.GetCommitted() {
		t.Fatalf("response must carry committed=true across the wire; got %+v", resp)
	}
	if !resp.GetUnderReplicated() {
		t.Fatalf("response must carry under_replicated=true across the wire; got %+v", resp)
	}
	if resp.GetError() != "" {
		t.Fatalf("the under-W detail must NOT travel as the response error string; got %q", resp.GetError())
	}
	if resp.GetConflict() {
		t.Fatalf("an under-replicated commit is not a conflict; got %+v", resp)
	}
}

// TestCasCommitResponse_Committed_NotUnderReplicated pins outcome (b): a fully
// replicated commit is committed=true, under_replicated=false, no error.
func TestCasCommitResponse_Committed_NotUnderReplicated(t *testing.T) {
	resp, err := casCommitResponse(backend.CASResult{Committed: true})
	if err != nil {
		t.Fatalf("committed maps to a nil gRPC error; got %v", err)
	}
	if !resp.GetCommitted() || resp.GetUnderReplicated() {
		t.Fatalf("want committed=true under_replicated=false; got %+v", resp)
	}
}

// TestCasCommitResponse_Conflict pins outcome (a): a conflict travels as the
// typed bool, not a gRPC error, unchanged by this fix.
func TestCasCommitResponse_Conflict(t *testing.T) {
	resp, err := casCommitResponse(backend.CASResult{Conflict: true})
	if err != nil {
		t.Fatalf("a conflict is not a gRPC error; got %v", err)
	}
	if !resp.GetConflict() || resp.GetCommitted() {
		t.Fatalf("want conflict=true committed=false; got %+v", resp)
	}
}

// TestCasCommitResponse_NotCommittedRetryable_IsGrpcStatus pins outcome (d): a
// NOT-committed retryable refusal (a freeze codes.Unavailable) must reach the
// client AS a gRPC status error so status.Code() classifies it retryable.
// Committed==false is the discriminator: same code as (c), opposite handling.
func TestCasCommitResponse_NotCommittedRetryable_IsGrpcStatus(t *testing.T) {
	frozen := status.Error(codes.Unavailable, "shale: cluster write-frozen")
	resp, err := casCommitResponse(backend.CASResult{Err: frozen})
	if resp != nil {
		t.Fatalf("a not-committed gRPC-status refusal returns a nil response + the status error; got resp %+v", resp)
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("the not-committed refusal must travel as codes.Unavailable; got ok=%v err=%v", ok, err)
	}
}

// TestCasCommitResponse_NotCommittedPlainError_IsErrorString pins the plain
// (statusless) not-committed backend failure: it travels as the response error
// string (the owner rolled back), not a gRPC error.
func TestCasCommitResponse_NotCommittedPlainError_IsErrorString(t *testing.T) {
	resp, err := casCommitResponse(backend.CASResult{Err: errors.New("backend exploded")})
	if err != nil {
		t.Fatalf("a statusless backend error travels in the response string, not a gRPC error; got %v", err)
	}
	if resp.GetError() == "" || resp.GetCommitted() {
		t.Fatalf("want a non-empty error string and committed=false; got %+v", resp)
	}
}
