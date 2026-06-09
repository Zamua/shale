package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeRebalanceClient is the minimal stand-in for rpc.Client used to
// pin the formatting + dispatch logic without paying for a real gRPC
// round trip. Each call records its arguments so tests can assert the
// CLI translated flags into the right RPC shape.
type fakeRebalanceClient struct {
	topoResp *pb.TopologyResponse
	topoErr  error

	propResp *pb.ProposeRebalanceResponse
	propErr  error

	lastDryRun bool
	lastApply  bool
	lastCancel bool
	callCount  int
}

func (f *fakeRebalanceClient) Topology(_ context.Context) (*pb.TopologyResponse, error) {
	return f.topoResp, f.topoErr
}

func (f *fakeRebalanceClient) ProposeRebalance(_ context.Context, dryRun, apply, cancel bool) (*pb.ProposeRebalanceResponse, error) {
	f.callCount++
	f.lastDryRun = dryRun
	f.lastApply = apply
	f.lastCancel = cancel
	return f.propResp, f.propErr
}

// testOpts returns a globalOpts with a short timeout so a buggy code
// path that accidentally blocks fails the test loudly instead of
// stalling the whole suite.
func testOpts(json bool) globalOpts {
	return globalOpts{addr: "ignored", json: json, timeout: 250 * time.Millisecond}
}

// samplePlan mirrors the spec's example: 3 nodes, 12 moves, totals
// matching the worked example so the human output check pins both the
// per-line + summary arithmetic.
func samplePlan() *pb.ProposeRebalanceResponse {
	moves := []*pb.RangePlanItem{
		{PartitionId: 17, CurrentOwner: "bench-1", ProposedOwner: "bench-2", EstimatedKeyCount: 340},
		{PartitionId: 23, CurrentOwner: "bench-1", ProposedOwner: "bench-3", EstimatedKeyCount: 340},
		{PartitionId: 41, CurrentOwner: "bench-2", ProposedOwner: "bench-3", EstimatedKeyCount: 340},
	}
	return &pb.ProposeRebalanceResponse{Ranges: moves}
}

func sampleTopology() *pb.TopologyResponse {
	return &pb.TopologyResponse{
		NodeId: "bench-1",
		Nodes: []*pb.NodeInfo{
			{NodeId: "bench-1"},
			{NodeId: "bench-2"},
			{NodeId: "bench-3"},
		},
	}
}

// TestRebalanceModeValidation pins the "exactly one of {--dry-run,
// --apply, --cancel}" rule. The top-level run() entrypoint is used so
// we also cover flag-parse + dispatch wiring, not just the helper.
func TestRebalanceModeValidation(t *testing.T) {
	t.Setenv(envAddr, "127.0.0.1:1")
	cases := []struct {
		name    string
		args    []string
		wantSub string
	}{
		{"no mode", []string{"rebalance"}, "required"},
		{"dry-run + apply", []string{"rebalance", "--dry-run", "--apply"}, "mutually exclusive"},
		{"dry-run + cancel", []string{"rebalance", "--dry-run", "--cancel"}, "mutually exclusive"},
		{"apply + cancel", []string{"rebalance", "--apply", "--cancel"}, "mutually exclusive"},
		{"all three", []string{"rebalance", "--dry-run", "--apply", "--cancel"}, "mutually exclusive"},
		{"extra positional", []string{"rebalance", "--dry-run", "junk"}, "Usage"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != exitGeneric {
				t.Fatalf("want exitGeneric, got %d (stderr=%q)", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantSub) {
				t.Fatalf("stderr should mention %q, got %q", tc.wantSub, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout should be empty on validation failure, got %q", stdout.String())
			}
		})
	}
}

// TestRebalanceDryRunHumanFormat pins the spec'd line shape: cluster
// header, "proposed plan: N partition moves" header, per-partition
// arrow lines with the est-keys parenthetical, and the trailing
// total. Order matters because operators eyeball top-down.
func TestRebalanceDryRunHumanFormat(t *testing.T) {
	fake := &fakeRebalanceClient{
		topoResp: sampleTopology(),
		propResp: samplePlan(),
	}
	var stdout, stderr bytes.Buffer
	code := runRebalanceWithClient(fake, testOpts(false), true, false, false, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("want exitOK, got %d (stderr=%q)", code, stderr.String())
	}
	if !fake.lastDryRun || fake.lastApply || fake.lastCancel {
		t.Fatalf("RPC called with wrong flags: dryRun=%v apply=%v cancel=%v", fake.lastDryRun, fake.lastApply, fake.lastCancel)
	}
	got := stdout.String()
	wantLines := []string{
		"cluster: 3 nodes",
		"proposed plan: 3 partition moves",
		"  partition 17  bench-1 → bench-2  (est ~340 keys)",
		"  partition 23  bench-1 → bench-3  (est ~340 keys)",
		"  partition 41  bench-2 → bench-3  (est ~340 keys)",
		"total estimated keys to migrate: 1020",
	}
	for _, want := range wantLines {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q; got:\n%s", want, got)
		}
	}
	// Pin top-to-bottom ordering. If a later refactor flips lines we
	// catch it here instead of via a human reading the output.
	prev := -1
	for _, want := range wantLines {
		idx := strings.Index(got, want)
		if idx <= prev {
			t.Fatalf("line %q out of order; got:\n%s", want, got)
		}
		prev = idx
	}
}

// TestRebalanceDryRunEmptyPlan covers the "ring already balanced"
// case: no moves, zero total. The header still prints so the operator
// sees "I did look at the cluster, it's just nothing to do".
func TestRebalanceDryRunEmptyPlan(t *testing.T) {
	fake := &fakeRebalanceClient{
		topoResp: sampleTopology(),
		propResp: &pb.ProposeRebalanceResponse{},
	}
	var stdout, stderr bytes.Buffer
	if code := runRebalanceWithClient(fake, testOpts(false), true, false, false, &stdout, &stderr); code != exitOK {
		t.Fatalf("want exitOK, got %d (stderr=%q)", code, stderr.String())
	}
	got := stdout.String()
	for _, want := range []string{
		"cluster: 3 nodes",
		"proposed plan: 0 partition moves",
		"total estimated keys to migrate: 0",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q; got:\n%s", want, got)
		}
	}
}

// TestRebalanceDryRunTopologyDegradesGracefully confirms that a
// Topology error does NOT fail the dry-run. The plan is the main
// payload; the cluster header is a nice-to-have. Suppressing the
// header keeps `--dry-run` useful when the operator points the CLI at
// a node that misbehaves on Topology but answers ProposeRebalance.
func TestRebalanceDryRunTopologyDegradesGracefully(t *testing.T) {
	fake := &fakeRebalanceClient{
		topoErr:  errors.New("topology boom"),
		propResp: samplePlan(),
	}
	var stdout, stderr bytes.Buffer
	if code := runRebalanceWithClient(fake, testOpts(false), true, false, false, &stdout, &stderr); code != exitOK {
		t.Fatalf("want exitOK, got %d (stderr=%q)", code, stderr.String())
	}
	got := stdout.String()
	if strings.Contains(got, "cluster:") {
		t.Fatalf("cluster header should be suppressed on topology error; got:\n%s", got)
	}
	if !strings.Contains(got, "proposed plan: 3 partition moves") {
		t.Fatalf("plan body still expected; got:\n%s", got)
	}
}

// TestRebalanceDryRunJSON pins the JSON shape. Field names must be
// snake_case (stable across proto regenerations) and the top-level
// value must be a JSON array per spec.
func TestRebalanceDryRunJSON(t *testing.T) {
	fake := &fakeRebalanceClient{
		topoResp: sampleTopology(),
		propResp: samplePlan(),
	}
	var stdout, stderr bytes.Buffer
	if code := runRebalanceWithClient(fake, testOpts(true), true, false, false, &stdout, &stderr); code != exitOK {
		t.Fatalf("want exitOK, got %d (stderr=%q)", code, stderr.String())
	}
	var got []struct {
		PartitionID       uint64 `json:"partition_id"`
		CurrentOwner      string `json:"current_owner"`
		ProposedOwner     string `json:"proposed_owner"`
		EstimatedKeyCount uint64 `json:"estimated_key_count"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON %q: %v", stdout.String(), err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 entries, got %d (%+v)", len(got), got)
	}
	if got[0].PartitionID != 17 || got[0].CurrentOwner != "bench-1" || got[0].ProposedOwner != "bench-2" || got[0].EstimatedKeyCount != 340 {
		t.Fatalf("first entry wrong: %+v", got[0])
	}
	// JSON mode should not emit the human header at all.
	if strings.Contains(stdout.String(), "cluster:") {
		t.Fatalf("JSON output should not include human header; got %q", stdout.String())
	}
}

// TestRebalanceApplyPrintsOK pins the --apply success contract: one
// line "ok", exit 0, the RPC called with apply=true exactly once.
func TestRebalanceApplyPrintsOK(t *testing.T) {
	fake := &fakeRebalanceClient{propResp: &pb.ProposeRebalanceResponse{}}
	var stdout, stderr bytes.Buffer
	if code := runRebalanceWithClient(fake, testOpts(false), false, true, false, &stdout, &stderr); code != exitOK {
		t.Fatalf("want exitOK, got %d (stderr=%q)", code, stderr.String())
	}
	if got := stdout.String(); got != "ok\n" {
		t.Fatalf("apply: want %q, got %q", "ok\n", got)
	}
	if fake.callCount != 1 || !fake.lastApply || fake.lastDryRun || fake.lastCancel {
		t.Fatalf("RPC called with wrong flags: count=%d dryRun=%v apply=%v cancel=%v",
			fake.callCount, fake.lastDryRun, fake.lastApply, fake.lastCancel)
	}
}

// TestRebalanceCancelPrintsOK mirrors the apply contract for --cancel.
func TestRebalanceCancelPrintsOK(t *testing.T) {
	fake := &fakeRebalanceClient{propResp: &pb.ProposeRebalanceResponse{}}
	var stdout, stderr bytes.Buffer
	if code := runRebalanceWithClient(fake, testOpts(false), false, false, true, &stdout, &stderr); code != exitOK {
		t.Fatalf("want exitOK, got %d (stderr=%q)", code, stderr.String())
	}
	if got := stdout.String(); got != "ok\n" {
		t.Fatalf("cancel: want %q, got %q", "ok\n", got)
	}
	if fake.callCount != 1 || !fake.lastCancel || fake.lastDryRun || fake.lastApply {
		t.Fatalf("RPC called with wrong flags: count=%d dryRun=%v apply=%v cancel=%v",
			fake.callCount, fake.lastDryRun, fake.lastApply, fake.lastCancel)
	}
}

// TestRebalanceRPCErrorMapsToExitCode confirms a transport-level
// failure exits non-zero and writes a diagnostic to stderr, regardless
// of mode. Unavailable maps to exitConnection per exitCodeForRPCError.
func TestRebalanceRPCErrorMapsToExitCode(t *testing.T) {
	cases := []struct {
		name          string
		dry, apl, cnl bool
	}{
		{"dry-run", true, false, false},
		{"apply", false, true, false},
		{"cancel", false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeRebalanceClient{
				propErr: status.Error(codes.Unavailable, "node down"),
			}
			var stdout, stderr bytes.Buffer
			code := runRebalanceWithClient(fake, testOpts(false), tc.dry, tc.apl, tc.cnl, &stdout, &stderr)
			if code != exitConnection {
				t.Fatalf("want exitConnection, got %d (stderr=%q)", code, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout should be empty on RPC error, got %q", stdout.String())
			}
			if !strings.Contains(stderr.String(), "shale rebalance") {
				t.Fatalf("stderr should be prefixed with subcommand name, got %q", stderr.String())
			}
		})
	}
}

// TestRebalanceTopologyMissingNodeListFallsBackTo1 covers the
// degenerate single-node response shape where Nodes is empty but
// NodeId is set: count it as 1 so the header is still informative.
func TestRebalanceTopologyMissingNodeListFallsBackTo1(t *testing.T) {
	fake := &fakeRebalanceClient{
		topoResp: &pb.TopologyResponse{NodeId: "solo", SingleNode: true},
		propResp: &pb.ProposeRebalanceResponse{},
	}
	var stdout, stderr bytes.Buffer
	if code := runRebalanceWithClient(fake, testOpts(false), true, false, false, &stdout, &stderr); code != exitOK {
		t.Fatalf("want exitOK, got %d (stderr=%q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "cluster: 1 nodes") {
		t.Fatalf("want single-node header, got:\n%s", stdout.String())
	}
}
