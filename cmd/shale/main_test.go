package main

import (
	"bytes"
	"encoding/json"
	"net"
	"strings"
	"testing"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rpc"
	"google.golang.org/grpc"
)

// startNode spins up a real rpc.Server backed by an in-memory
// cluster. Returns the dial address + cleanup. Mirrors the shape
// pkg/rpc/server_test.go uses, so changes there carry over here.
func startNode(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	c, err := cluster.Open(cluster.Config{
		NodeID:  "cli-test-node",
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

func TestRunPutGetDeleteRoundtrip(t *testing.T) {
	addr, cleanup := startNode(t)
	defer cleanup()
	t.Setenv(envAddr, "")

	var stdout, stderr bytes.Buffer

	// put
	if code := run([]string{"--addr", addr, "put", "alpha", "one"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("put: code=%d stderr=%s", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("put: want silent stdout, got %q", stdout.String())
	}

	// get hit
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--addr", addr, "get", "alpha"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("get: code=%d stderr=%s", code, stderr.String())
	}
	if got := stdout.String(); got != "one\n" {
		t.Fatalf("get: want %q, got %q", "one\n", got)
	}

	// delete
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--addr", addr, "delete", "alpha"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("delete: code=%d stderr=%s", code, stderr.String())
	}

	// get after delete -> not found, exit 2, no stdout
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--addr", addr, "get", "alpha"}, &stdout, &stderr); code != exitNotFound {
		t.Fatalf("get missing: want code %d, got %d (stderr=%s)", exitNotFound, code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("get missing: want silent stdout, got %q", stdout.String())
	}
}

func TestRunDeleteIsIdempotent(t *testing.T) {
	addr, cleanup := startNode(t)
	defer cleanup()
	t.Setenv(envAddr, "")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"--addr", addr, "delete", "never-existed"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("delete of missing: want exitOK, got %d (stderr=%s)", code, stderr.String())
	}
}

func TestRunScanStreamsKeyValueLines(t *testing.T) {
	addr, cleanup := startNode(t)
	defer cleanup()
	t.Setenv(envAddr, "")

	var stdout, stderr bytes.Buffer
	for _, kv := range [][2]string{{"users/alice", "A"}, {"users/bob", "B"}, {"pastes/x", "P"}} {
		stdout.Reset()
		stderr.Reset()
		if code := run([]string{"--addr", addr, "put", kv[0], kv[1]}, &stdout, &stderr); code != exitOK {
			t.Fatalf("put %s: code=%d", kv[0], code)
		}
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--addr", addr, "scan", "users/"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("scan: code=%d stderr=%s", code, stderr.String())
	}
	lines := strings.Split(strings.TrimRight(stdout.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("scan: want 2 lines, got %d (%q)", len(lines), stdout.String())
	}
	if lines[0] != "users/alice=A" || lines[1] != "users/bob=B" {
		t.Fatalf("scan: unexpected output %v", lines)
	}
}

func TestRunTopologyHumanAndJSON(t *testing.T) {
	addr, cleanup := startNode(t)
	defer cleanup()
	t.Setenv(envAddr, "")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"--addr", addr, "topology"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("topology: code=%d stderr=%s", code, stderr.String())
	}
	if got := stdout.String(); got != "single-node cluster, node=cli-test-node\n" {
		t.Fatalf("topology: want single-node line, got %q", got)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--addr", addr, "--json", "topology"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("topology --json: code=%d stderr=%s", code, stderr.String())
	}
	var out struct {
		NodeID     string `json:"node_id"`
		SingleNode bool   `json:"single_node"`
		Nodes      []struct {
			NodeID string `json:"node_id"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("topology --json: invalid JSON %q: %v", stdout.String(), err)
	}
	if out.NodeID != "cli-test-node" || !out.SingleNode || len(out.Nodes) != 1 || out.Nodes[0].NodeID != "cli-test-node" {
		t.Fatalf("topology --json: unexpected payload %+v", out)
	}
}

func TestRunStatsCounters(t *testing.T) {
	addr, cleanup := startNode(t)
	defer cleanup()
	t.Setenv(envAddr, "")

	var stdout, stderr bytes.Buffer
	// Drive a deterministic mix.
	for _, k := range []string{"a", "b"} {
		stdout.Reset()
		stderr.Reset()
		if code := run([]string{"--addr", addr, "put", k, "v"}, &stdout, &stderr); code != exitOK {
			t.Fatalf("put %s: code=%d", k, code)
		}
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--addr", addr, "--json", "stats"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("stats: code=%d stderr=%s", code, stderr.String())
	}
	var out struct {
		KeysHeld uint64 `json:"keys_held"`
		Puts     uint64 `json:"puts"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("stats --json: invalid JSON %q: %v", stdout.String(), err)
	}
	if out.KeysHeld != 2 || out.Puts != 2 {
		t.Fatalf("stats: want keys_held=2 puts=2, got %+v", out)
	}
}

func TestRunPingHappyPath(t *testing.T) {
	addr, cleanup := startNode(t)
	defer cleanup()
	t.Setenv(envAddr, "")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"--addr", addr, "ping"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("ping: code=%d stderr=%s", code, stderr.String())
	}
	if got := stdout.String(); got != "ok\n" {
		t.Fatalf("ping: want %q, got %q", "ok\n", got)
	}
}

func TestRunPingUnreachableIsConnection(t *testing.T) {
	t.Setenv(envAddr, "")
	// 127.0.0.1:1 - reserved, nothing should answer. We pick a
	// short timeout so the test stays snappy.
	var stdout, stderr bytes.Buffer
	args := []string{"--addr", "127.0.0.1:1", "ping"}
	// Reach into a custom-timeout run by going through parseGlobal +
	// runPing directly so we can lower opts.timeout. The top-level
	// run() uses defaultTimeout (5s) which is too long for tests.
	opts, sub, subArgs, err := parseGlobal(args)
	if err != nil {
		t.Fatalf("parseGlobal: %v", err)
	}
	if sub != "ping" {
		t.Fatalf("sub: want ping, got %q", sub)
	}
	opts.timeout = 250 * 1000 * 1000 // 250ms in ns; avoids importing time here for one constant
	code := runPing(opts, subArgs, &stdout, &stderr)
	if code != exitConnection && code != exitTimeout {
		t.Fatalf("ping to dead addr: want %d or %d, got %d (stderr=%s)", exitConnection, exitTimeout, code, stderr.String())
	}
}

func TestRunUnknownSubcommand(t *testing.T) {
	t.Setenv(envAddr, "")
	var stdout, stderr bytes.Buffer
	code := run([]string{"nope"}, &stdout, &stderr)
	if code != exitGeneric {
		t.Fatalf("unknown sub: want %d, got %d", exitGeneric, code)
	}
	if !strings.Contains(stderr.String(), "unknown subcommand") {
		t.Fatalf("stderr should mention unknown subcommand, got %q", stderr.String())
	}
}

func TestRunNoArgsShowsUsage(t *testing.T) {
	t.Setenv(envAddr, "")
	var stdout, stderr bytes.Buffer
	code := run([]string{}, &stdout, &stderr)
	if code != exitGeneric {
		t.Fatalf("no args: want %d, got %d", exitGeneric, code)
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Fatalf("stderr should show usage, got %q", stderr.String())
	}
}

func TestRunHelpFlagShowsUsageAndExitsZero(t *testing.T) {
	t.Setenv(envAddr, "")
	for _, f := range []string{"-h", "--help", "help"} {
		var stdout, stderr bytes.Buffer
		code := run([]string{f}, &stdout, &stderr)
		if code != exitOK {
			t.Fatalf("%s: want exitOK, got %d", f, code)
		}
		if !strings.Contains(stderr.String(), "Usage:") {
			t.Fatalf("%s: stderr missing usage, got %q", f, stderr.String())
		}
	}
}
