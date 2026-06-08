package main

import (
	"strings"
	"testing"
)

func TestParseFlags_RequiresNodeID(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "")
	t.Setenv("SHALE_BACKEND", "memory")

	_, err := parseFlags(nil)
	if err == nil || !strings.Contains(err.Error(), "--node-id") {
		t.Fatalf("want --node-id required error, got %v", err)
	}
}

func TestParseFlags_RequiresBackend(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_BACKEND", "")

	_, err := parseFlags(nil)
	if err == nil || !strings.Contains(err.Error(), "--backend") {
		t.Fatalf("want --backend required error, got %v", err)
	}
}

func TestParseFlags_RejectsUnknownBackend(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_BACKEND", "")

	_, err := parseFlags([]string{"--backend", "bogus"})
	if err == nil || !strings.Contains(err.Error(), "must be one of memory|slate") {
		t.Fatalf("want unknown-backend error, got %v", err)
	}
}

func TestParseFlags_SlateRequiresBucketAndCreds(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_BACKEND", "")

	_, err := parseFlags([]string{"--backend", "slate"})
	if err == nil || !strings.Contains(err.Error(), "--slate-bucket") {
		t.Fatalf("want slate-bucket required error, got %v", err)
	}

	_, err = parseFlags([]string{
		"--backend", "slate",
		"--slate-bucket", "b",
	})
	if err == nil || !strings.Contains(err.Error(), "--slate-db-name") {
		t.Fatalf("want slate-db-name required error, got %v", err)
	}

	_, err = parseFlags([]string{
		"--backend", "slate",
		"--slate-bucket", "b",
		"--slate-db-name", "d",
	})
	if err == nil || !strings.Contains(err.Error(), "--slate-access-key") {
		t.Fatalf("want slate-access-key required error, got %v", err)
	}
}

func TestParseFlags_DefaultsAndEnv(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "node-from-env")
	t.Setenv("SHALE_BACKEND", "memory")
	t.Setenv("SHALE_GRPC_ADDR", "")
	t.Setenv("SHALE_BIND_ADDR", "")
	t.Setenv("SHALE_SEEDS", "")

	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.nodeID != "node-from-env" {
		t.Fatalf("node-id from env: want node-from-env, got %q", cfg.nodeID)
	}
	if cfg.grpcAddr != ":7947" {
		t.Fatalf("grpc-addr default: want :7947, got %q", cfg.grpcAddr)
	}
	if cfg.bindAddr != ":7946" {
		t.Fatalf("bind-addr default: want :7946, got %q", cfg.bindAddr)
	}
	if cfg.backend != "memory" {
		t.Fatalf("backend: want memory, got %q", cfg.backend)
	}
	if len(cfg.seeds) != 0 {
		t.Fatalf("seeds: want empty, got %v", cfg.seeds)
	}
}

func TestParseFlags_FlagOverridesEnv(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "from-env")
	t.Setenv("SHALE_BACKEND", "memory")
	t.Setenv("SHALE_GRPC_ADDR", ":1111")

	cfg, err := parseFlags([]string{"--node-id", "from-flag", "--grpc-addr", ":2222"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if cfg.nodeID != "from-flag" {
		t.Fatalf("node-id: flag should win; got %q", cfg.nodeID)
	}
	if cfg.grpcAddr != ":2222" {
		t.Fatalf("grpc-addr: flag should win; got %q", cfg.grpcAddr)
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
		got := splitSeeds(tc.in)
		if !equalSlices(got, tc.want) {
			t.Errorf("splitSeeds(%q): want %v, got %v", tc.in, tc.want, got)
		}
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
