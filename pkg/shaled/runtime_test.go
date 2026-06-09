package shaled

import (
	"flag"
	"strings"
	"testing"
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
