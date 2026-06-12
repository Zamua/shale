package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeReshardClient stands in for rpc.Client so the reshard CLI's output
// formatting + exit-code mapping is pinned without a real gRPC round trip.
type fakeReshardClient struct {
	resp *pb.ReshardResponse
	err  error
}

func (f *fakeReshardClient) Reshard(_ context.Context) (*pb.ReshardResponse, error) {
	return f.resp, f.err
}

// TestRunReshard_SuccessHuman: a clean reshard prints the from -> to transition
// and exits 0.
func TestRunReshard_SuccessHuman(t *testing.T) {
	cli := &fakeReshardClient{resp: &pb.ReshardResponse{FromUnitCount: 2, ToUnitCount: 4}}
	var stdout, stderr bytes.Buffer

	if code := runReshardWithClient(cli, testOpts(false), &stdout, &stderr); code != exitOK {
		t.Fatalf("exit code = %d, want %d. stderr=%q", code, exitOK, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "reshard: 2 -> 4" {
		t.Fatalf("stdout = %q, want %q", got, "reshard: 2 -> 4")
	}
}

// TestRunReshard_SuccessJSON: under --json the transition is a structured
// object with stable snake_case field names.
func TestRunReshard_SuccessJSON(t *testing.T) {
	cli := &fakeReshardClient{resp: &pb.ReshardResponse{FromUnitCount: 4, ToUnitCount: 8}}
	var stdout, stderr bytes.Buffer

	if code := runReshardWithClient(cli, testOpts(true), &stdout, &stderr); code != exitOK {
		t.Fatalf("exit code = %d, want %d. stderr=%q", code, exitOK, stderr.String())
	}
	var got struct {
		From uint32 `json:"from_unit_count"`
		To   uint32 `json:"to_unit_count"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout is not valid JSON: %v (%q)", err, stdout.String())
	}
	if got.From != 4 || got.To != 8 {
		t.Fatalf("JSON from=%d to=%d, want from=4 to=8", got.From, got.To)
	}
}

// TestRunReshard_RefusedInResponse: a guard rejection carried in the response
// Error string (legacy mode / in-flight / unstable membership) is reported to
// stderr and exits 1 (generic), NOT a transport code. The refused reshard left
// the generation untouched, so the CLI surfaces it as a recoverable refusal.
func TestRunReshard_RefusedInResponse(t *testing.T) {
	cli := &fakeReshardClient{resp: &pb.ReshardResponse{Error: "a reshard is already in flight"}}
	var stdout, stderr bytes.Buffer

	if code := runReshardWithClient(cli, testOpts(false), &stdout, &stderr); code != exitGeneric {
		t.Fatalf("exit code = %d, want %d (refusal). stderr=%q", code, exitGeneric, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty on a refused reshard", stdout.String())
	}
	if !strings.Contains(stderr.String(), "already in flight") {
		t.Fatalf("stderr = %q, want the refusal reason", stderr.String())
	}
}

// TestRunReshard_TransportTimeout: a gRPC DeadlineExceeded maps to exit 4
// (timeout) via exitCodeForRPCError, distinct from a response-level refusal.
func TestRunReshard_TransportTimeout(t *testing.T) {
	cli := &fakeReshardClient{err: status.Error(codes.DeadlineExceeded, "deadline")}
	var stdout, stderr bytes.Buffer

	if code := runReshardWithClient(cli, testOpts(false), &stdout, &stderr); code != exitTimeout {
		t.Fatalf("exit code = %d, want %d (timeout). stderr=%q", code, exitTimeout, stderr.String())
	}
}

// TestRunReshard_TransportUnavailable: a gRPC Unavailable (node unreachable)
// maps to exit 3 (connection).
func TestRunReshard_TransportUnavailable(t *testing.T) {
	cli := &fakeReshardClient{err: status.Error(codes.Unavailable, "no route")}
	var stdout, stderr bytes.Buffer

	if code := runReshardWithClient(cli, testOpts(false), &stdout, &stderr); code != exitConnection {
		t.Fatalf("exit code = %d, want %d (connection). stderr=%q", code, exitConnection, stderr.String())
	}
}
