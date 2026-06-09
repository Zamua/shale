// main_test.go pins the smallest end-to-end behaviors of the
// shale-bench harness. Anything more elaborate would either duplicate
// pkg/cluster's own coverage or hard-code machine-specific timings
// that would just flake on slow CI - this file only verifies the
// scenarios actually wire up + that --json emits the documented shape.
package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// TestRun_RawMemory_HumanOutput: the simplest scenario shouldn't need
// any external state and should print the canonical two-phase summary.
func TestRun_RawMemory_HumanOutput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--mode", "raw",
		"--backend", "memory",
		"--ops", "200",
		"--concurrency", "2",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run rc=%d stderr=%q", code, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{"scenario=raw-memory", "phase=write", "phase=read", "ops=200"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// TestRun_ClusterMemory_JSONShape: pin the JSON envelope shape so a
// downstream script (scripts/run-bench.sh) breaking would surface
// here too, not just in CI.
func TestRun_ClusterMemory_JSONShape(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--mode", "cluster",
		"--backend", "memory",
		"--nodes", "1",
		"--rf", "1",
		"--ops", "200",
		"--concurrency", "2",
		"--json",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run rc=%d stderr=%q", code, stderr.String())
	}

	var parsed benchOutput
	if err := json.Unmarshal(stdout.Bytes(), &parsed); err != nil {
		t.Fatalf("json parse: %v\noutput:\n%s", err, stdout.String())
	}
	if parsed.Scenario != "cluster-memory-n1-r1" {
		t.Errorf("scenario=%q want cluster-memory-n1-r1", parsed.Scenario)
	}
	if parsed.Mode != "cluster" || parsed.Backend != "memory" {
		t.Errorf("mode/backend = %s/%s", parsed.Mode, parsed.Backend)
	}
	if parsed.Nodes != 1 || parsed.R != 1 {
		t.Errorf("nodes/r = %d/%d", parsed.Nodes, parsed.R)
	}
	if len(parsed.Phases) != 2 {
		t.Fatalf("phases=%d want 2", len(parsed.Phases))
	}
	if parsed.Phases[0].Phase != "write" || parsed.Phases[1].Phase != "read" {
		t.Errorf("phase order: %s, %s", parsed.Phases[0].Phase, parsed.Phases[1].Phase)
	}
	for _, p := range parsed.Phases {
		if p.Ops != 200 {
			t.Errorf("%s ops=%d want 200", p.Phase, p.Ops)
		}
		if p.Throughput <= 0 {
			t.Errorf("%s throughput=%f, want > 0", p.Phase, p.Throughput)
		}
	}
}

// TestRun_SlateBackend_AcceptedButFailsWithoutTag confirms the flag
// validator no longer rejects --backend=slate up-front (so the slatedb-
// tagged build can take over via init()), and that the default no-tag
// build still surfaces a clear "requires -tags slatedb" error when an
// operator picks slate without rebuilding the harness. Anchors the
// "stub returns error" contract from openSlateBackend in main.go.
func TestRun_SlateBackend_AcceptedButFailsWithoutTag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--mode", "raw",
		"--backend", "slate",
		"--ops", "1",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("expected non-zero rc, got 0 (stdout=%q)", stdout.String())
	}
	if !strings.Contains(stderr.String(), "slatedb") {
		t.Errorf("expected stderr to mention slatedb build tag; got %q", stderr.String())
	}
}

// TestRun_SlateAwaitDurable_FlagAccepted pins that --slate-await-durable
// parses cleanly under the no-tag build (which is what 99% of
// contributors will be running unit tests against). The actual slate
// open still fails because there is no slatedb runtime; we only assert
// the flag itself doesn't break flag parsing and that the cross-tag
// handoff plumbing (slateWriteOpts + makeSlateWriteOptions) is wired
// without panic. Anchors the "operator can opt into relaxed mode via
// CLI" contract that scripts/run-bench-slate.sh depends on.
func TestRun_SlateAwaitDurable_FlagAccepted(t *testing.T) {
	for _, awaitDurable := range []string{"true", "false"} {
		t.Run("await-durable="+awaitDurable, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run([]string{
				"--mode", "raw",
				"--backend", "slate",
				"--slate-await-durable=" + awaitDurable,
				"--ops", "1",
			}, &stdout, &stderr)
			// Slate without the slatedb tag must still fail (the
			// existing contract). The flag itself must not have
			// caused a parse error (rc=2) - we expect rc=1, the
			// "backend open failed" code, which carries the
			// slatedb-tag message.
			if code != 1 {
				t.Fatalf("rc=%d want 1 (open failure). stderr=%q", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "slatedb") {
				t.Errorf("expected slatedb-tag message in stderr, got %q", stderr.String())
			}
		})
	}
}

// TestRun_SlateAwaitDurable_HandoffResetsAcrossRuns pins that the
// package-level slateWriteOpts var is cleared on exit so a relaxed-mode
// run does NOT leak into a subsequent strict run. Without the deferred
// reset in run(), a test suite (or a script invoking the harness
// multiple times in-process) could see the second case silently inherit
// the first case's WriteOptions. Anchors that defer.
func TestRun_SlateAwaitDurable_HandoffResetsAcrossRuns(t *testing.T) {
	if slateWriteOpts != nil {
		t.Fatalf("precondition: slateWriteOpts must be nil; got %v", slateWriteOpts)
	}
	var stdout, stderr bytes.Buffer
	_ = run([]string{
		"--mode", "raw",
		"--backend", "slate",
		"--slate-await-durable=false",
		"--ops", "1",
	}, &stdout, &stderr)
	if slateWriteOpts != nil {
		t.Errorf("slateWriteOpts must be cleared on exit; still %v", slateWriteOpts)
	}
}

// TestRun_InvalidFlags rejects the obviously wrong combinations.
func TestRun_InvalidFlags(t *testing.T) {
	cases := []struct {
		name string
		argv []string
	}{
		{"raw with nodes >1", []string{"--mode", "raw", "--nodes", "3"}},
		{"rf > nodes", []string{"--mode", "cluster", "--nodes", "2", "--rf", "3"}},
		{"unknown backend", []string{"--backend", "sqlite"}},
		{"unknown mode", []string{"--mode", "wat"}},
		{"zero ops", []string{"--ops", "0"}},
		{"zero conc", []string{"--concurrency", "0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(tc.argv, &stdout, &stderr); code == 0 {
				t.Errorf("rc=0, expected non-zero for %v", tc.argv)
			}
		})
	}
}
