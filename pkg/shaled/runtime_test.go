package shaled

import (
	"flag"
	"io"
	"log"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/storageunit"
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

func TestStdConfig_ReplicationFactorAndUnitCountDefaults(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_REPLICATION_FACTOR", "")
	t.Setenv("SHALE_UNIT_COUNT", "")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	std := BindStdFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := std.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if std.ReplicationFactor != 1 {
		t.Fatalf("ReplicationFactor default: want 1, got %d", std.ReplicationFactor)
	}
	if std.UnitCount.N() != 1 {
		t.Fatalf("UnitCount default: want N=1, got %d", std.UnitCount.N())
	}
}

func TestStdConfig_ReplicationFactorAndUnitCountFromFlags(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	std := BindStdFlags(fs)
	if err := fs.Parse([]string{"--replication-factor", "3", "--unit-count", "8"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := std.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if std.ReplicationFactor != 3 {
		t.Fatalf("ReplicationFactor: want 3, got %d", std.ReplicationFactor)
	}
	if std.UnitCount.N() != 8 {
		t.Fatalf("UnitCount: want N=8, got %d", std.UnitCount.N())
	}
}

func TestStdConfig_UnitCountFromEnv(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_UNIT_COUNT", "16")
	t.Setenv("SHALE_REPLICATION_FACTOR", "2")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	std := BindStdFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := std.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if std.UnitCount.N() != 16 {
		t.Fatalf("UnitCount from env: want N=16, got %d", std.UnitCount.N())
	}
	if std.ReplicationFactor != 2 {
		t.Fatalf("ReplicationFactor from env: want 2, got %d", std.ReplicationFactor)
	}
}

func TestStdConfig_RejectsNonPowerOfTwoUnitCount(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	std := BindStdFlags(fs)
	if err := fs.Parse([]string{"--unit-count", "3"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	err := std.Validate()
	if err == nil || !strings.Contains(err.Error(), "--unit-count") {
		t.Fatalf("want --unit-count power-of-two error, got %v", err)
	}
	if !strings.Contains(err.Error(), "power of two") {
		t.Fatalf("error should mention power of two, got %v", err)
	}
}

func TestRun_RejectsBothBackendAndFactory(t *testing.T) {
	err := Run(RunConfig{
		Std:            StdConfig{NodeID: "n1"},
		Backend:        memory.New(),
		BackendFactory: memfactory.New(),
		Logger:         log.New(io.Discard, "", 0),
	})
	if err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("want both-set error, got %v", err)
	}
}

func TestRun_RejectsNeitherBackendNorFactory(t *testing.T) {
	err := Run(RunConfig{
		Std:    StdConfig{NodeID: "n1"},
		Logger: log.New(io.Discard, "", 0),
	})
	if err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("want neither-set error, got %v", err)
	}
}

// TestRun_MultiBackendSingleNode drives Run end to end in MULTI-BACKEND
// mode with an in-process memory factory (no -tags slatedb, no MinIO):
// single-node (BindAddr empty), an ephemeral gRPC listener, UnitCount
// threaded from Std. Run blocks until SIGTERM; a clean nil return proves
// cluster.Open accepted the BackendFactory + UnitCount Run built into the
// cluster.Config (a misthreaded UnitCount would make Open fail the
// validateBackendMode XOR). CloseFactory is invoked on shutdown.
func TestRun_MultiBackendSingleNode(t *testing.T) {
	std := StdConfig{
		NodeID:            "n1",
		GRPCAddr:          "127.0.0.1:0", // ephemeral
		ReplicationFactor: 1,
		UnitCount:         storageunit.MustUnitCount(4),
	}
	closed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Run(RunConfig{
			Std:            std,
			BackendLabel:   "memfactory",
			BackendFactory: memfactory.New(),
			CloseFactory:   func() error { close(closed); return nil },
			Logger:         log.New(io.Discard, "", 0),
		})
	}()

	// Give Run a moment to reach the serve loop, then signal a clean
	// shutdown via SIGTERM (the path Run installs a notifier on).
	time.Sleep(150 * time.Millisecond)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signal: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run multi-backend: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after SIGTERM")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("CloseFactory was not invoked on shutdown")
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

// -- single-backend bind-address resolution ---------------------------
//
// A single Backend is single-node only. The shared flag constructor
// defaults --bind-addr to ":7946" because shaled-slate needs that default
// (its multi-backend mode is a real multi-node configuration), so a
// single-backend binary has to tell a DEFAULTED bind address apart from an
// explicit one. These pin all three outcomes.

func parseStdFlags(t *testing.T, args ...string) *StdConfig {
	t.Helper()
	t.Setenv("SHALE_NODE_ID", "n1")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	std := BindStdFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse %v: %v", args, err)
	}
	if err := std.Validate(); err != nil {
		t.Fatalf("validate %v: %v", args, err)
	}
	return std
}

func TestResolveSingleBackendBindAddr_DefaultedIsBlanked(t *testing.T) {
	t.Setenv("SHALE_BIND_ADDR", "")
	std := parseStdFlags(t)
	if std.BindAddr == "" {
		t.Fatal("precondition: the flag default should be non-empty")
	}
	got, err := resolveSingleBackendBindAddr(std)
	if err != nil {
		t.Fatalf("bare invocation should resolve, got %v", err)
	}
	if got != "" {
		t.Fatalf("defaulted bind address should be blanked, got %q", got)
	}
}

func TestResolveSingleBackendBindAddr_ExplicitFlagIsKept(t *testing.T) {
	t.Setenv("SHALE_BIND_ADDR", "")
	std := parseStdFlags(t, "--bind-addr", "127.0.0.1:9999")
	got, err := resolveSingleBackendBindAddr(std)
	if err != nil {
		t.Fatalf("explicit bind address should resolve here (Open refuses it later), got %v", err)
	}
	// Kept, so cluster.Open can refuse it with the message naming the
	// multi-backend requirement instead of silently downgrading.
	if got != "127.0.0.1:9999" {
		t.Fatalf("explicit bind address should be kept, got %q", got)
	}
}

func TestResolveSingleBackendBindAddr_ExplicitEnvIsKept(t *testing.T) {
	t.Setenv("SHALE_BIND_ADDR", "127.0.0.1:9998")
	std := parseStdFlags(t)
	got, err := resolveSingleBackendBindAddr(std)
	if err != nil {
		t.Fatalf("env-supplied bind address should resolve here, got %v", err)
	}
	if got != "127.0.0.1:9998" {
		t.Fatalf("env-supplied bind address should be kept, got %q", got)
	}
}

// The trap that blanking a defaulted bind address would otherwise open: an
// operator who passes seeds but no bind address would get a node that
// silently ignores them and joins nothing. Across a fleet that is N
// independent divergent stores, so it is an error.
func TestResolveSingleBackendBindAddr_SeedsWithoutBindAddrIsAnError(t *testing.T) {
	t.Setenv("SHALE_BIND_ADDR", "")
	std := parseStdFlags(t, "--seeds", "10.0.0.1:7946")
	_, err := resolveSingleBackendBindAddr(std)
	if err == nil {
		t.Fatal("seeds without an explicit bind address should be refused")
	}
	if !strings.Contains(err.Error(), "--bind-addr") {
		t.Fatalf("error should name --bind-addr, got %v", err)
	}
}

// Run must surface that refusal, not just the helper.
func TestRun_SingleBackendRejectsSeedsWithoutBindAddr(t *testing.T) {
	t.Setenv("SHALE_BIND_ADDR", "")
	std := parseStdFlags(t, "--seeds", "10.0.0.1:7946", "--grpc-addr", "127.0.0.1:0")
	err := Run(RunConfig{
		Std:     *std,
		Logger:  log.New(io.Discard, "", 0),
		Backend: memory.New(),
	})
	if err == nil {
		t.Fatal("Run should refuse seeds without a bind address")
	}
	if !strings.Contains(err.Error(), "--bind-addr") {
		t.Fatalf("error should name --bind-addr, got %v", err)
	}
}
