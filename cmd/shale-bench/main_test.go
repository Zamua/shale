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
