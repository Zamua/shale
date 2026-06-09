package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPercentileNearestRank pins the percentile helper's nearest-rank
// math against a hand-checked input. p50 on 1..10 = the 5th element
// (50% * 10 = 5, nearest-rank picks index 5 -> value 5). p99 on 1..100
// = the 99th element. p0 -> min, p100 -> max. Empty -> 0.
func TestPercentileNearestRank(t *testing.T) {
	mk := func(n int) []time.Duration {
		out := make([]time.Duration, n)
		for i := range out {
			out[i] = time.Duration(i+1) * time.Millisecond
		}
		return out
	}

	cases := []struct {
		name string
		xs   []time.Duration
		q    float64
		want time.Duration
	}{
		{"empty", nil, 0.5, 0},
		{"p50 of 1..10", mk(10), 0.50, 5 * time.Millisecond},
		{"p99 of 1..100", mk(100), 0.99, 99 * time.Millisecond},
		{"p100 -> max", mk(10), 1.0, 10 * time.Millisecond},
		{"p0 -> min", mk(10), 0.0, 1 * time.Millisecond},
		{"single element", mk(1), 0.99, 1 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// percentile sorts in place; pass a copy so input order
			// before the call is preserved for the next iteration.
			xs := append([]time.Duration(nil), tc.xs...)
			got := percentile(xs, tc.q)
			if got != tc.want {
				t.Fatalf("percentile(%s, %v) = %v, want %v", tc.name, tc.q, got, tc.want)
			}
		})
	}
}

// TestPercentileShuffledInputSorted confirms percentile sorts before
// indexing; an unsorted input must still return the correct rank.
func TestPercentileShuffledInputSorted(t *testing.T) {
	xs := []time.Duration{
		7 * time.Millisecond,
		1 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		3 * time.Millisecond,
	}
	if got, want := percentile(xs, 0.50), 5*time.Millisecond; got != want {
		t.Fatalf("p50 of shuffled 1..10/5 = %v, want %v", got, want)
	}
}

// TestKeyAtIsLexSortable pins the key format. Bench relies on these
// keys being stable across phases (the read phase re-derives them from
// the same counter) and lexically sortable so they don't accidentally
// cluster on one shard when keys are hashed.
func TestKeyAtIsLexSortable(t *testing.T) {
	a := string(keyAt("bench:", 1))
	b := string(keyAt("bench:", 2))
	c := string(keyAt("bench:", 10))
	if a != "bench:00000001" || b != "bench:00000002" || c != "bench:00000010" {
		t.Fatalf("unexpected keys: %q %q %q", a, b, c)
	}
	if !(a < b && b < c) {
		t.Fatalf("keys not lex sortable: %q %q %q", a, b, c)
	}
}

// TestCounterDistributionExactCount simulates the bench fan-out using
// the same atomic-counter pattern runPhase uses, and verifies the total
// op count is exactly N regardless of worker count. This is the only
// invariant that matters for "exact --writes / --reads" - if it breaks,
// the bench numbers are wrong.
func TestCounterDistributionExactCount(t *testing.T) {
	const total = 5000
	for _, workers := range []int{1, 2, 8, 64} {
		var next int64
		var done int64
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					idx := atomic.AddInt64(&next, 1) - 1
					if idx >= int64(total) {
						return
					}
					atomic.AddInt64(&done, 1)
				}
			}()
		}
		wg.Wait()
		if got := atomic.LoadInt64(&done); got != total {
			t.Fatalf("workers=%d: did %d ops, want %d", workers, got, total)
		}
	}
}

// TestRunBenchFlagParsingRejectsBadValues confirms each flag boundary
// the user might trip over. We don't need to dial for these - the
// validation happens before any RPC.
func TestRunBenchFlagParsingRejectsBadValues(t *testing.T) {
	t.Setenv(envAddr, "")
	cases := [][]string{
		{"bench", "--writes", "-1"},
		{"bench", "--reads", "-1"},
		{"bench", "--value-size", "-1"},
		{"bench", "--concurrency", "0"},
		{"bench", "--replication-factor", "-1"},
		{"bench", "extra-arg"},
	}
	for _, args := range cases {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		if code != exitGeneric {
			t.Fatalf("args=%v: want exitGeneric, got %d (stderr=%q)", args, code, stderr.String())
		}
	}
}

// TestRunBenchReplicationFactorPropagates pins that --replication-factor
// shows up in both the human output (single label line at the top) and
// the JSON envelope (top-level replication_factor field). This is the
// only contract bench owes: it's a label, not a switch, but the label
// MUST round-trip so R1/R2/R3 comparison runs stay distinguishable.
func TestRunBenchReplicationFactorPropagates(t *testing.T) {
	addr, cleanup := startNode(t)
	defer cleanup()
	t.Setenv(envAddr, "")

	t.Run("human", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		args := []string{
			"--addr", addr, "bench",
			"--writes", "5", "--reads", "5",
			"--value-size", "8", "--concurrency", "2",
			"--replication-factor", "3",
		}
		if code := run(args, &stdout, &stderr); code != exitOK {
			t.Fatalf("code=%d stderr=%s", code, stderr.String())
		}
		if !strings.Contains(stdout.String(), "replication_factor=3") {
			t.Fatalf("human output missing replication_factor=3:\n%s", stdout.String())
		}
	})

	t.Run("json", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		args := []string{
			"--addr", addr, "bench",
			"--writes", "5", "--reads", "5",
			"--value-size", "8", "--concurrency", "2",
			"--replication-factor", "2",
			"--json",
		}
		if code := run(args, &stdout, &stderr); code != exitOK {
			t.Fatalf("code=%d stderr=%s", code, stderr.String())
		}
		var out struct {
			ReplicationFactor int `json:"replication_factor"`
		}
		if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
			t.Fatalf("invalid JSON: %v\n%s", err, stdout.String())
		}
		if out.ReplicationFactor != 2 {
			t.Fatalf("JSON replication_factor: got %d want 2", out.ReplicationFactor)
		}
	})
}

// TestRunBenchHumanRoundtrip drives a real bench against an in-process
// node and checks the human output shape. Small N keeps the test fast.
func TestRunBenchHumanRoundtrip(t *testing.T) {
	addr, cleanup := startNode(t)
	defer cleanup()
	t.Setenv(envAddr, "")

	var stdout, stderr bytes.Buffer
	args := []string{
		"--addr", addr, "bench",
		"--writes", "20", "--reads", "20",
		"--value-size", "16", "--concurrency", "4",
	}
	if code := run(args, &stdout, &stderr); code != exitOK {
		t.Fatalf("bench: code=%d stderr=%s", code, stderr.String())
	}
	out := stdout.String()
	for _, frag := range []string{"phase=write", "phase=read", "ops=20", "throughput=", "p50=", "p99="} {
		if !strings.Contains(out, frag) {
			t.Fatalf("output missing %q; got:\n%s", frag, out)
		}
	}
}

// TestRunBenchJSONRoundtrip drives a bench with --json and confirms
// the structured shape matches benchOutput.
func TestRunBenchJSONRoundtrip(t *testing.T) {
	addr, cleanup := startNode(t)
	defer cleanup()
	t.Setenv(envAddr, "")

	var stdout, stderr bytes.Buffer
	args := []string{
		"--addr", addr, "bench",
		"--writes", "10", "--reads", "10",
		"--value-size", "8", "--concurrency", "2",
		"--json",
	}
	if code := run(args, &stdout, &stderr); code != exitOK {
		t.Fatalf("bench --json: code=%d stderr=%s", code, stderr.String())
	}
	var out struct {
		Writes      int    `json:"writes"`
		Reads       int    `json:"reads"`
		KeysPrefix  string `json:"keys_prefix"`
		ValueSize   int    `json:"value_size_bytes"`
		Concurrency int    `json:"concurrency"`
		Phases      []struct {
			Phase      string  `json:"phase"`
			Ops        int     `json:"ops"`
			DurationMs float64 `json:"duration_ms"`
			Throughput float64 `json:"throughput_ops_per_sec"`
			P50Ms      float64 `json:"p50_ms"`
			P99Ms      float64 `json:"p99_ms"`
		} `json:"phases"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("invalid JSON %q: %v", stdout.String(), err)
	}
	if out.Writes != 10 || out.Reads != 10 || out.Concurrency != 2 || out.ValueSize != 8 {
		t.Fatalf("envelope wrong: %+v", out)
	}
	if len(out.Phases) != 2 {
		t.Fatalf("want 2 phases, got %d", len(out.Phases))
	}
	if out.Phases[0].Phase != "write" || out.Phases[1].Phase != "read" {
		t.Fatalf("phase order/name wrong: %+v", out.Phases)
	}
	for _, p := range out.Phases {
		if p.Ops != 10 {
			t.Fatalf("phase %s: want ops=10, got %d", p.Phase, p.Ops)
		}
	}
}
