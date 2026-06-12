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

// TestRun_RejectsNonPowerOfTwoUnitCount pins that --unit-count is validated
// through storageunit.NewUnitCount, so a non-power-of-two N is rejected
// before any backend is opened (the bisect invariant must never be violated
// by a bad operator config).
func TestRun_RejectsNonPowerOfTwoUnitCount(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "a")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "s")
	t.Setenv("SHALE_UNIT_COUNT", "3")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--unit-count") {
		t.Fatalf("want --unit-count invalid error, got %v", err)
	}
}

// TestRun_RejectsNonIntegerUnitCount pins a non-numeric --unit-count is a
// clean error (not a panic or a silent fallback to 1).
func TestRun_RejectsNonIntegerUnitCount(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "a")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "s")
	t.Setenv("SHALE_UNIT_COUNT", "two")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "not an integer") {
		t.Fatalf("want non-integer --unit-count error, got %v", err)
	}
}

// TestRun_MultiBackendDoesNotRequireDbName pins the mode-dependent
// validation: with --unit-count > 1 (multi-backend mode), --slate-db-name
// is NOT required (per-unit DbNames are factory-derived). Validation must
// pass and the run must proceed to the FACTORY branch (NOT the legacy
// single-backend branch, which would have failed earlier on the missing
// db-name). The assertion is build-agnostic on purpose:
//
//   - tag-less build: the stub openSlateFactory fails with the
//     "rebuild with -tags slatedb" message.
//   - slatedb-tagged build: the real factory + cluster.Open engage and
//     fail later trying to reach object storage (this test deliberately
//     uses a junk bucket so it never depends on a live MinIO).
//
// In BOTH cases the error must NOT be the --slate-db-name requirement,
// which is the whole point: multi-backend mode skipped it. The real
// end-to-end multi-backend serve (Put/Get across >1 unit) is covered by
// the pkg/shaled wiring test (in-memory factory) and, with MinIO, by the
// real-cluster chaos harness.
func TestRun_MultiBackendDoesNotRequireDbName(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "") // absent on purpose
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "a")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "s")
	t.Setenv("SHALE_UNIT_COUNT", "4")
	err := run(nil, os.Stderr)
	if err == nil {
		t.Fatalf("want a downstream error from the factory branch, got nil")
	}
	if strings.Contains(err.Error(), "--slate-db-name") {
		t.Fatalf("multi-backend mode must NOT require --slate-db-name, got %v", err)
	}
	// Either the tag-less stub error or the tagged real-path error proves
	// the factory branch was taken (the legacy branch never opens a
	// Backing / cluster, so neither message can come from it).
	gotFactoryBranch := strings.Contains(err.Error(), "tags slatedb") || // tag-less stub
		strings.Contains(err.Error(), "open cluster") || // cluster.Open engaged (tagged)
		strings.Contains(err.Error(), "open slate backing") // NewBacking engaged (tagged)
	if !gotFactoryBranch {
		t.Fatalf("want evidence the factory branch was taken, got %v", err)
	}
}

// TestRun_LegacyStillRequiresDbName pins that the default (--unit-count
// unset, i.e. 1) keeps requiring --slate-db-name - the legacy contract is
// unchanged. Complements TestRun_RequiresDbName by being explicit that the
// requirement is mode-gated, not removed.
func TestRun_LegacyStillRequiresDbName(t *testing.T) {
	t.Setenv("SHALE_NODE_ID", "n1")
	t.Setenv("SHALE_SLATE_BUCKET", "b")
	t.Setenv("SHALE_SLATE_DB_NAME", "")
	t.Setenv("SHALE_SLATE_ACCESS_KEY", "a")
	t.Setenv("SHALE_SLATE_SECRET_KEY", "s")
	t.Setenv("SHALE_UNIT_COUNT", "1")
	err := run(nil, os.Stderr)
	if err == nil || !strings.Contains(err.Error(), "--slate-db-name") {
		t.Fatalf("want --slate-db-name required in legacy mode, got %v", err)
	}
}
