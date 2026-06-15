package main

import (
	"os"
	"strings"
	"testing"
)

func TestRun_RequiresNodeID(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "d")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "a")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "s")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--node-id") {
		t.Fatalf("want --node-id required error, got %v", err)
	}
}

func TestRun_RequiresBucket(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--slate-bucket") {
		t.Fatalf("want --slate-bucket required error, got %v", err)
	}
}

func TestRun_RequiresDbName(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--slate-db-name") {
		t.Fatalf("want --slate-db-name required error, got %v", err)
	}
}

func TestRun_RequiresCreds(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "d")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--slate-access-key") {
		t.Fatalf("want --slate-access-key required error, got %v", err)
	}
}

// In multi-backend mode --slate-db-name is NOT required (per-unit DbNames
// are derived from the GenUnit by the factory). The config validation must
// pass with bucket + creds + --multi-backend even when db-name is empty;
// the !slatedb build then fails fast at openSlateFactory with the rebuild
// hint (NOT at the --slate-db-name validation), which proves the multi path
// was selected and skipped the db-name requirement.
func TestRun_MultiBackendDoesNotRequireDbName(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "a")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "s")
	err := run([]string{"--multi-backend", "true", "--unit-count", "4"}, os.Stderr)
	if err == nil {
		t.Fatal("want an error from the !slatedb stub, got nil")
	}
	if strings.Contains(err.Error(), "--slate-db-name") {
		t.Fatalf("multi-backend mode must NOT require --slate-db-name, got %v", err)
	}
	if !strings.Contains(err.Error(), "tags slatedb") {
		t.Fatalf("want rebuild-with-tags-slatedb fast-fail from openSlateFactory, got %v", err)
	}
}

// In single-backend mode --slate-db-name is still required.
func TestRun_SingleBackendStillRequiresDbName(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "a")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "s")
	err := run([]string{"--multi-backend", "false"}, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--slate-db-name") {
		t.Fatalf("want --slate-db-name required in single mode, got %v", err)
	}
}

// --multi-backend DEFAULTS to false: with no --multi-backend flag (and no
// SHALE_MULTI_BACKEND env), the binary takes the single-backend path, which
// requires --slate-db-name. The resulting --slate-db-name error (NOT the
// multi-backend "tags slatedb" fast-fail) proves the default selected the
// single-backend path.
func TestRun_MultiBackendDefaultsFalse(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "a")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "s")
	t.Setenv("SHALE_MULTI_BACKEND", "")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--slate-db-name") {
		t.Fatalf("default (no --multi-backend) must take single-backend path requiring --slate-db-name, got %v", err)
	}
	if strings.Contains(err.Error(), "tags slatedb") {
		t.Fatalf("default must NOT take the multi-backend path, got %v", err)
	}
}

// SHALE_MULTI_BACKEND=true (env, no flag) selects the multi-backend path:
// db-name becomes optional and the !slatedb stub fast-fails at the factory.
// Pins the env fallback for the --multi-backend flag.
func TestRun_MultiBackendFromEnv(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "a")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "s")
	t.Setenv("SHALE_MULTI_BACKEND", "true")
	t.Setenv("SHALE_UNIT_COUNT", "4")
	err := run(nil, os.Stderr)
	if err == nil {
		t.Fatal("want an error from the !slatedb stub, got nil")
	}
	if strings.Contains(err.Error(), "--slate-db-name") {
		t.Fatalf("multi-backend (from env) must NOT require --slate-db-name, got %v", err)
	}
	if !strings.Contains(err.Error(), "tags slatedb") {
		t.Fatalf("want rebuild-with-tags-slatedb fast-fail from the multi path, got %v", err)
	}
}

// A non-power-of-two --unit-count is rejected at std validation, before any
// slate config is touched (works in BOTH builds: the check is in
// pkg/shaled, no slatedb needed).
func TestRun_RejectsNonPowerOfTwoUnitCount(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "a")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "s")
	err := run([]string{"--multi-backend", "true", "--unit-count", "6"}, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--unit-count") {
		t.Fatalf("want --unit-count power-of-two error, got %v", err)
	}
}
