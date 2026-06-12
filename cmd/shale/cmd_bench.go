// cmd_bench.go implements `shale bench`, a quick-and-dirty throughput +
// latency probe against a running node's gRPC endpoint.
//
// Design notes:
//
//   - Workload: N writer goroutines hammer Put with keys drawn from a
//     shared atomic counter, ensuring exactly --writes Puts are issued
//     regardless of concurrency. After the write phase finishes we
//     repeat with Gets re-using the same counter-driven key space, so
//     reads exercise keys that were just written (hit rate = 100% on a
//     single-node cluster; lower if a shard has migrated between
//     phases).
//   - Latency aggregation: each goroutine appends per-op latencies to a
//     pre-allocated slice (one slot per planned op, written by index
//     so no locking is needed beyond the atomic counter). After the
//     phase ends we sort.Slice the slice in place and index-pick the
//     p50 / p99 entries. This is O(N log N) once per phase and uses
//     no third-party deps. For N <= a few million it's well under a
//     second of post-processing, which is fine for an operator-facing
//     bench tool. If/when we need streaming percentiles or merging
//     across processes, swap in a t-digest or HDR histogram - the
//     phaseStats struct is the seam.
//   - Output: human-readable single-line-per-phase summary by default,
//     or one combined JSON object with --json. The JSON shape covers
//     both phases so a wrapper script can `jq` it as a single document.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zamua/shale/pkg/rpc"
)

// phaseStats holds the timing data for one phase (writes or reads).
// Latencies are stored in nanoseconds to avoid float drift in the hot
// path; conversion to ms happens at format time.
type phaseStats struct {
	Phase      string        `json:"phase"`
	Ops        int           `json:"ops"`
	Duration   time.Duration `json:"-"`
	Throughput float64       `json:"throughput_ops_per_sec"`
	P50        time.Duration `json:"-"`
	P99        time.Duration `json:"-"`

	DurationMs float64 `json:"duration_ms"`
	P50Ms      float64 `json:"p50_ms"`
	P99Ms      float64 `json:"p99_ms"`
}

// benchOutput is the JSON envelope when --json is set.
type benchOutput struct {
	Writes      int    `json:"writes"`
	Reads       int    `json:"reads"`
	KeysPrefix  string `json:"keys_prefix"`
	ValueSize   int    `json:"value_size_bytes"`
	Concurrency int    `json:"concurrency"`
	// ReplicationFactor is the R the operator was driving the target
	// cluster at; bench itself only talks to one node's gRPC + does
	// not configure replication on the server side, so this is a
	// label-only field that lets a wrapper script keep R1 / R2 / R3
	// runs distinguishable in the output stream. Per docs/SPEC.md
	// "Replication (v0.4+)" R is per-cluster config, set when shaled
	// starts; bench reports the value the operator asked it to record.
	ReplicationFactor int          `json:"replication_factor,omitempty"`
	Phases            []phaseStats `json:"phases"`
}

// runBench handles `shale bench`. See file header for design notes.
func runBench(opts globalOpts, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `Usage: shale bench [flags]

Flags:
  --writes N                Put operations (default 10000)
  --reads N                 Get operations after writes (default 10000)
  --keys-prefix PREFIX      key prefix (default "bench:")
  --value-size BYTES        payload bytes per Put (default 256, random fill)
  --concurrency N           parallel goroutines (default 8)
  --replication-factor R    record the cluster's R in the output (label-only;
                            the actual R is set on the running node, this
                            flag just tags the run so R1/R2/R3 comparisons
                            stay distinguishable in --json output)
  --json                    emit a single JSON object covering both phases
`)
	}
	writes := fs.Int("writes", 10000, "Put operations")
	reads := fs.Int("reads", 10000, "Get operations after writes")
	keysPrefix := fs.String("keys-prefix", "bench:", "key prefix")
	valueSize := fs.Int("value-size", 256, "payload bytes per Put")
	concurrency := fs.Int("concurrency", 8, "parallel goroutines")
	replicationFactor := fs.Int("replication-factor", 0, "label-only R for output (default 0 = unspecified)")
	jsonOut := fs.Bool("json", false, "structured output")
	if err := fs.Parse(args); err != nil {
		return exitGeneric
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return exitGeneric
	}
	if *writes < 0 || *reads < 0 {
		_, _ = fmt.Fprintf(stderr, "shale bench: --writes and --reads must be >= 0\n")
		return exitGeneric
	}
	if *valueSize < 0 {
		_, _ = fmt.Fprintf(stderr, "shale bench: --value-size must be >= 0\n")
		return exitGeneric
	}
	if *concurrency < 1 {
		_, _ = fmt.Fprintf(stderr, "shale bench: --concurrency must be >= 1\n")
		return exitGeneric
	}
	if *replicationFactor < 0 {
		_, _ = fmt.Fprintf(stderr, "shale bench: --replication-factor must be >= 0\n")
		return exitGeneric
	}
	// --json on either the global or subcommand flag turns on JSON
	// mode. Mirrors the spec ("--json structured output") while still
	// honoring the top-level convention.
	wantJSON := opts.json || *jsonOut

	cli, cleanup, code := dial(opts.addr, stderr)
	if code != exitOK {
		return code
	}
	defer cleanup()

	payload := make([]byte, *valueSize)
	if _, err := rand.Read(payload); err != nil {
		_, _ = fmt.Fprintf(stderr, "shale bench: random fill: %v\n", err)
		return exitGeneric
	}

	phases := make([]phaseStats, 0, 2)

	if *writes > 0 {
		ws, err := runPhase("write", cli, *concurrency, *writes, *keysPrefix, payload, true)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "shale bench (write): %v\n", err)
			return exitCodeForRPCError(err)
		}
		phases = append(phases, ws)
	}

	if *reads > 0 {
		rs, err := runPhase("read", cli, *concurrency, *reads, *keysPrefix, nil, false)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "shale bench (read): %v\n", err)
			return exitCodeForRPCError(err)
		}
		phases = append(phases, rs)
	}

	if wantJSON {
		out := benchOutput{
			Writes:            *writes,
			Reads:             *reads,
			KeysPrefix:        *keysPrefix,
			ValueSize:         *valueSize,
			Concurrency:       *concurrency,
			ReplicationFactor: *replicationFactor,
			Phases:            phases,
		}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			return exitGeneric
		}
		return exitOK
	}

	if *replicationFactor > 0 {
		_, _ = fmt.Fprintf(stdout, "replication_factor=%d\n", *replicationFactor)
	}
	for _, p := range phases {
		_, _ = fmt.Fprintln(stdout, formatPhaseHuman(p))
	}
	return exitOK
}

// runPhase executes one phase against the cluster. write=true issues
// Puts using payload; write=false issues Gets. The shared atomic
// counter guarantees exactly total ops are issued (no over- or
// under-shoot regardless of concurrency).
//
// Any RPC error short-circuits the phase and is returned; the partial
// data is discarded. We deliberately don't try to mask transient errors
// here - this is a measurement tool, and a failed Put silently dropped
// would invalidate the numbers it's about to print.
func runPhase(name string, cli *rpc.Client, concurrency, total int, prefix string, payload []byte, write bool) (phaseStats, error) {
	if total == 0 {
		return phaseStats{Phase: name}, nil
	}
	latencies := make([]time.Duration, total)
	var next int64
	var firstErr atomic.Value // holds error
	var wg sync.WaitGroup

	start := time.Now()
	for range concurrency {
		wg.Go(func() {
			for {
				idx := atomic.AddInt64(&next, 1) - 1
				if idx >= int64(total) {
					return
				}
				if firstErr.Load() != nil {
					return
				}
				key := keyAt(prefix, idx)
				ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
				t0 := time.Now()
				var err error
				if write {
					err = cli.Put(ctx, key, payload)
				} else {
					_, _, err = cli.Get(ctx, key)
				}
				cancel()
				latencies[idx] = time.Since(t0)
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
			}
		})
	}
	wg.Wait()
	dur := time.Since(start)

	if v := firstErr.Load(); v != nil {
		return phaseStats{}, v.(error)
	}

	p50, p99 := percentile(latencies, 0.50), percentile(latencies, 0.99)
	tput := 0.0
	if dur > 0 {
		tput = float64(total) / dur.Seconds()
	}
	return phaseStats{
		Phase:      name,
		Ops:        total,
		Duration:   dur,
		Throughput: tput,
		P50:        p50,
		P99:        p99,
		DurationMs: float64(dur.Microseconds()) / 1000.0,
		P50Ms:      float64(p50.Microseconds()) / 1000.0,
		P99Ms:      float64(p99.Microseconds()) / 1000.0,
	}, nil
}

// keyAt formats the byte slice for the i-th key in the bench key
// space. Width is 8 to keep keys lexically sortable up to 99999999 ops
// (more than this bench would ever issue in one run).
func keyAt(prefix string, i int64) []byte {
	return fmt.Appendf(nil, "%s%08d", prefix, i)
}

// percentile sorts xs in place and returns the q-th percentile via
// nearest-rank. q is expressed as a fraction (0.50 -> p50). Returns 0
// for an empty slice. The caller is expected to be OK with xs being
// mutated; bench discards the slice after this call.
func percentile(xs []time.Duration, q float64) time.Duration {
	n := len(xs)
	if n == 0 {
		return 0
	}
	slices.Sort(xs)
	if q <= 0 {
		return xs[0]
	}
	if q >= 1 {
		return xs[n-1]
	}
	// Nearest-rank: index = ceil(q * n) - 1, clamped.
	idx := min(max(int(q*float64(n)+0.999999999), 1), n)
	return xs[idx-1]
}

// formatPhaseHuman renders one phase as the canonical single line.
// Format matches the spec exactly so an operator scanning two runs can
// eyeball-diff them.
func formatPhaseHuman(p phaseStats) string {
	return fmt.Sprintf("phase=%-5s ops=%d  duration=%s  throughput=%.1f ops/s  p50=%s  p99=%s",
		p.Phase, p.Ops, formatDur(p.Duration), p.Throughput, formatDur(p.P50), formatDur(p.P99))
}

// formatDur picks a human unit for a Duration. We override stdlib's
// String() to get the "ms" / "s" granularity the spec sample uses,
// without the "µs" / "ns" detours stdlib falls into for very small
// values.
func formatDur(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000.0)
	case d >= time.Microsecond:
		return fmt.Sprintf("%.1fµs", float64(d.Nanoseconds())/1000.0)
	default:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
}
