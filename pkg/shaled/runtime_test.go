package shaled

import (
	"bytes"
	"context"
	"flag"
	"log"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/backend"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestStdConfig_RequiresNodeID(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	std := BindStdFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse: %v", err)
	}
	err := std.Validate()
	if err == nil || !strings.Contains(err.Error(), "--node-id") {
		t.Fatalf("want --node-id required error, got %v", err)
	}
}

func TestStdConfig_DefaultsAndEnv(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "node-from-env")
	t.Setenv("SHALE_GRPC_ADDR", "")
	t.Setenv("SHALE_BIND_ADDR", "")
	t.Setenv("SHALE_SEEDS", "")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	std := BindStdFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := std.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	if std.NodeID != "node-from-env" {
		t.Fatalf("NodeID: want node-from-env, got %q", std.NodeID)
	}
	if std.GRPCAddr != ":7947" {
		t.Fatalf("GRPCAddr default: want :7947, got %q", std.GRPCAddr)
	}
	if std.BindAddr != ":7946" {
		t.Fatalf("BindAddr default: want :7946, got %q", std.BindAddr)
	}
	if len(std.Seeds) != 0 {
		t.Fatalf("Seeds: want empty, got %v", std.Seeds)
	}
}

func TestStdConfig_FlagOverridesEnv(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "from-env")
	t.Setenv("SHALE_GRPC_ADDR", ":1111")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	std := BindStdFlags(fs)
	if err := fs.Parse([]string{"--node-id", "from-flag", "--grpc-addr", ":2222"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := std.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if std.NodeID != "from-flag" {
		t.Fatalf("node-id: flag should win; got %q", std.NodeID)
	}
	if std.GRPCAddr != ":2222" {
		t.Fatalf("grpc-addr: flag should win; got %q", std.GRPCAddr)
	}
}

func TestStdConfig_SeedsParsing(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	std := BindStdFlags(fs)
	if err := fs.Parse([]string{"--seeds", " a:1 , , b:2 ,"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := std.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	want := []string{"a:1", "b:2"}
	if len(std.Seeds) != len(want) {
		t.Fatalf("seeds: want %v, got %v", want, std.Seeds)
	}
	for i := range want {
		if std.Seeds[i] != want[i] {
			t.Fatalf("seeds[%d]: want %q, got %q", i, want[i], std.Seeds[i])
		}
	}
}

func TestSplitSeeds(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"  ", nil},
		{"a:1", []string{"a:1"}},
		{"a:1,b:2", []string{"a:1", "b:2"}},
		{" a:1 , , b:2 ,", []string{"a:1", "b:2"}},
	}
	for _, tc := range cases {
		got := SplitSeeds(tc.in)
		if !equalSlices(got, tc.want) {
			t.Errorf("SplitSeeds(%q): want %v, got %v", tc.in, tc.want, got)
		}
	}
}

func TestEnvOr(t *testing.T) {
	t.Setenv("SHALED_TEST_KEY", "")
	if got := EnvOr("SHALED_TEST_KEY", "fallback"); got != "fallback" {
		t.Errorf("want fallback when env is empty, got %q", got)
	}
	t.Setenv("SHALED_TEST_KEY", "value")
	if got := EnvOr("SHALED_TEST_KEY", "fallback"); got != "value" {
		t.Errorf("want env value when set, got %q", got)
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRun_RejectsNoBackendMode pins that Run fails closed when neither the
// legacy Backend nor the multi-backend BackendFactory+UnitCount pair is set.
// This is the guard a per-backend main relies on so a misconfigured launch
// (forgot to open any backend) refuses rather than panicking later.
func TestRun_RejectsNoBackendMode(t *testing.T) {
	err := Run(RunConfig{
		Std:    StdConfig{NodeID: "n1", GRPCAddr: "127.0.0.1:0"},
		Logger: log.New(&syncBuf{}, "", 0),
	})
	if err == nil || !strings.Contains(err.Error(), "multi-backend") {
		t.Fatalf("want backend-mode-required error, got %v", err)
	}
}

// TestRun_MultiBackendWiresFactoryAcrossUnits is the deploy-gap smoke test:
// it boots a single node through shaled.Run in MULTI-BACKEND mode (a
// BackendFactory + UnitCount=2, no single Backend) over an ephemeral gRPC
// port, drives Put/Get for the live ShaleNode gRPC surface, and asserts the
// writes physically landed across MORE THAN ONE storage unit. That last
// assertion is what proves the factory pair was actually forwarded into
// cluster.Open (legacy single-Backend mode has no units to span). It is the
// in-process analogue of launching shaled-slate with --unit-count > 1.
func TestRun_MultiBackendWiresFactoryAcrossUnits(t *testing.T) {
	const unitCount = 2
	fac := memfactory.New()
	buf := &syncBuf{}
	logger := log.New(buf, "", log.LstdFlags)

	runErr := make(chan error, 1)
	go func() {
		runErr <- Run(RunConfig{
			Std:            StdConfig{NodeID: "mb-solo", GRPCAddr: "127.0.0.1:0"},
			BackendLabel:   "mem",
			BackendFactory: fac,
			UnitCount:      storageunit.MustUnitCount(unitCount),
			Logger:         logger,
		})
	}()
	// Make sure we tear the node down even if an assertion fails early.
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
		select {
		case err := <-runErr:
			if err != nil {
				t.Errorf("Run returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not shut down after SIGTERM")
		}
	}
	defer stop()

	grpcAddr := waitForGRPCAddr(t, buf, 10*time.Second)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", grpcAddr, err)
	}
	defer func() { _ = conn.Close() }()
	api := pb.NewShaleNodeClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Spread enough distinct keys that, with N=2, both units receive at
	// least one (a single key could hash to one unit; 16 keys make a
	// single-unit outcome astronomically unlikely and would itself flag a
	// real placement bug rather than flake).
	keys := make([][]byte, 0, 16)
	for i := 0; i < 16; i++ {
		keys = append(keys, []byte("deploy-gap-key-"+strings.Repeat("x", i)+string(rune('a'+i))))
	}
	for _, k := range keys {
		if _, err := api.Put(ctx, &pb.PutRequest{Key: k, Value: []byte("v")}); err != nil {
			t.Fatalf("Put %q over gRPC: %v", k, err)
		}
	}
	for _, k := range keys {
		resp, err := api.Get(ctx, &pb.GetRequest{Key: k})
		if err != nil {
			t.Fatalf("Get %q over gRPC: %v", k, err)
		}
		if resp.GetNotFound() || !bytes.Equal(resp.GetValue(), []byte("v")) {
			t.Fatalf("Get %q: notFound=%v value=%q, want present value=v", k, resp.GetNotFound(), resp.GetValue())
		}
	}

	// The factory must have OPENED at least 2 distinct units AND placed
	// keys in more than one of them - the proof that multi-backend routing
	// (not a single Backend) served these writes.
	mounted := fac.OpenUnits()
	if len(mounted) < 2 {
		t.Fatalf("factory mounted %d units, want >= 2 (multi-backend mode did not engage): %v", len(mounted), mounted)
	}
	unitsWithKeys := 0
	for _, gu := range mounted {
		be, ok := fac.UnitBackend(gu)
		if !ok {
			continue
		}
		if backendHasAnyKey(t, be) {
			unitsWithKeys++
		}
	}
	if unitsWithKeys < 2 {
		t.Fatalf("keys landed in only %d unit(s); want spread across >= 2 (single-Backend mode would use 0 units): mounted=%v", unitsWithKeys, mounted)
	}

	stop()
}

// backendHasAnyKey reports whether be holds at least one key (a full-prefix
// local scan). Used to confirm a unit actually received data.
func backendHasAnyKey(t *testing.T, be backend.Backend) bool {
	t.Helper()
	it, err := be.ScanPrefix(nil)
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}
	defer func() { _ = it.Close() }()
	k, _, err := it.Next()
	if err != nil {
		t.Fatalf("scan next: %v", err)
	}
	return k != nil
}

var grpcAddrRe = regexp.MustCompile(`grpc=(\S+)`)

// waitForGRPCAddr polls the captured startup log until Run has logged its
// resolved (ephemeral) gRPC address, then returns it. Run binds the listener
// on the configured :0 and logs the resolved host:port; this is how a test
// learns the port without Run exposing it directly.
func waitForGRPCAddr(t *testing.T, buf *syncBuf, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m := grpcAddrRe.FindStringSubmatch(buf.String()); m != nil {
			return m[1]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Run did not log a gRPC address within %s; log so far:\n%s", timeout, buf.String())
	return ""
}

// syncBuf is a goroutine-safe bytes.Buffer: Run's logger writes from the
// serving goroutine while the test reads to learn the bound address.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}
