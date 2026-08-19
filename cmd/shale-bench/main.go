// Command shale-bench is the comparative benchmark harness introduced
// in v0.5. It measures the cost of running the shale cluster layer
// (sharding + optional replication) against the same backend exercised
// directly, end to end, in one binary so the numbers are apples-to-
// apples.
//
// Why a separate binary (not `shale bench`):
//
//   - `shale bench` is the operator-facing latency probe: it dials one
//     running shaled node over gRPC + replays a workload. It can't
//     answer "what does R=3 cost?" because shaled today has no flag to
//     set R; that's a deliberate v0.5 omission (the cluster API is
//     stable, but the operator surface for tuning R lives at v0.5+).
//   - The "raw backend" baseline number has no gRPC server to dial in
//     the first place. Running the same key/value workload against a
//     bare pebble.Pebble (no shale, no gRPC) is the apples-to-apples
//     baseline for "what does shale cost?". A separate harness keeps
//     that comparison honest.
//   - In-process clusters spin up + tear down in seconds via the same
//     pattern tests/integration uses (in-process coordination +
//     ephemeral ports). One binary can drive every scenario in the
//     suite without an operator wrangling multiple shaled processes.
//
// What the harness DOES NOT do:
//
//   - It does not pretend to be the production load test. A
//     single Go process with N goroutines is a
//     best-case-with-coordination measurement: zero packet loss, no
//     real network, shared L3 cache. Cross-host numbers will diverge
//     (likely WORSE for shale because the gRPC hop becomes a real
//     RTT; likely BETTER for shale because backends can finally run
//     on independent disks). The harness output documents this; see
//     docs/BENCH-v0.5.md for the operator-facing commentary.
//   - It does not exercise the slate backend. That requires MinIO +
//     the slatedb build tag; the v0.5 suite intentionally focuses on
//     pebble (local-disk, durable, pure Go) and memory (lower-bound,
//     isolates the shale cost from any backend cost) so the table is
//     reproducible without external infrastructure. A slate run can
//     be wired in once v0.5's MinIO CI fixture lands (v0.5.1+).
//
// Output: human-readable single-line-per-scenario summary by default,
// or one JSON object with --json. scripts/run-bench.sh consumes the
// JSON form to assemble the markdown comparison table.
package main

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zamua/shale/backends/pebble"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// scenario captures the inputs that drive one bench run.
type scenario struct {
	Mode        string // "raw" or "cluster"
	Backend     string // "memory", "pebble", or "slate"
	Nodes       int    // 1, 3, ...
	R           int    // replication factor
	Ops         int    // total ops per phase
	ValueSize   int    // payload bytes per put
	Concurrency int    // parallel goroutines
	PebbleDir   string // base dir for pebble backend (one subdir per node)
}

// phaseStats holds the latency + throughput for one phase. Mirrors the
// shape of cmd/shale/cmd_bench.go's phaseStats so JSON output is
// uniform across the two benches.
type phaseStats struct {
	Phase      string  `json:"phase"`
	Ops        int     `json:"ops"`
	DurationMs float64 `json:"duration_ms"`
	Throughput float64 `json:"throughput_ops_per_sec"`
	P50Ms      float64 `json:"p50_ms"`
	P99Ms      float64 `json:"p99_ms"`
}

// benchOutput is the JSON envelope written when --json is set.
type benchOutput struct {
	Scenario    string       `json:"scenario"`
	Mode        string       `json:"mode"`
	Backend     string       `json:"backend"`
	Nodes       int          `json:"nodes"`
	R           int          `json:"replication_factor"`
	Ops         int          `json:"ops"`
	ValueSize   int          `json:"value_size_bytes"`
	Concurrency int          `json:"concurrency"`
	Phases      []phaseStats `json:"phases"`
}

// putGetter is the minimal surface we benchmark. Both *cluster.Cluster
// and a raw backend.Backend satisfy this via thin adapters in
// rawAdapter / clusterAdapter below. Keeping the bench loop ignorant
// of which one it's hitting means the loop itself contributes the same
// fixed cost to every scenario; the only thing that varies is what
// PutGetter.Put / .Get actually do under the hood.
type putGetter interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
}

func run(argv []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("shale-bench", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `Usage: shale-bench [flags]

Drives one comparative benchmark scenario end to end (write phase, then
read phase) and prints latency + throughput.

Flags:
  --mode raw|cluster        raw hits the backend directly (no shale layer);
                            cluster wires a single-process N-node shale
                            cluster with the requested R and routes through
                            it. (default cluster)
  --backend memory|pebble|slate
                            storage engine to drive (default pebble).
                            slate requires the binary built with
                            -tags slatedb plus SHALE_BENCH_S3_* env vars
                            (see cmd/shale-bench/slate.go for the list).
  --nodes N                 cluster node count (cluster mode only; default 1)
  --rf R                    replication factor (cluster mode only; default 1)
  --ops N                   operations per phase (default 10000)
  --value-size BYTES        payload bytes per put (default 1024)
  --concurrency N           parallel client goroutines (default 4)
  --scenario NAME           label used in JSON output (default auto)
  --slate-await-durable B   (slate only) when false, opens the slate
                            backend with WriteOptions{AwaitDurable:false}
                            for relaxed-durability mode. Defaults true
                            (slatedb's own default), which is byte-exact
                            with the pre-flag behavior. Pair relaxed mode
                            with --rf >= 2 in production; R=1 relaxed is
                            unsafe (single replica crash loses un-flushed
                            writes). See docs/SPEC.md "Backend durability
                            is a backend concern".
  --json                    emit one JSON object instead of text
`)
	}
	mode := fs.String("mode", "cluster", "raw|cluster")
	backendName := fs.String("backend", "pebble", "memory|pebble|slate")
	nodes := fs.Int("nodes", 1, "cluster node count")
	rf := fs.Int("rf", 1, "replication factor")
	ops := fs.Int("ops", 10000, "operations per phase")
	valueSize := fs.Int("value-size", 1024, "payload bytes per put")
	concurrency := fs.Int("concurrency", 4, "parallel goroutines")
	scenarioLabel := fs.String("scenario", "", "label for JSON output (default auto)")
	slateAwaitDurable := fs.Bool("slate-await-durable", true, "(slate only) AwaitDurable on per-write WriteOptions")
	jsonOut := fs.Bool("json", false, "emit a JSON object")
	if err := fs.Parse(argv); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return 2
	}
	if *mode != "raw" && *mode != "cluster" {
		_, _ = fmt.Fprintf(stderr, "shale-bench: --mode must be raw or cluster\n")
		return 2
	}
	if *backendName != "memory" && *backendName != "pebble" && *backendName != "slate" {
		_, _ = fmt.Fprintf(stderr, "shale-bench: --backend must be memory, pebble, or slate\n")
		return 2
	}
	if *ops < 1 {
		_, _ = fmt.Fprintf(stderr, "shale-bench: --ops must be >= 1\n")
		return 2
	}
	if *concurrency < 1 {
		_, _ = fmt.Fprintf(stderr, "shale-bench: --concurrency must be >= 1\n")
		return 2
	}
	if *mode == "raw" && (*nodes != 1 || *rf != 1) {
		_, _ = fmt.Fprintf(stderr, "shale-bench: --mode=raw forces --nodes=1 --rf=1\n")
		return 2
	}
	if *mode == "cluster" {
		// The harness measures the cluster layer's cost over ONE embedded
		// backend, so it runs a single node. Multi-node scenarios used to
		// run over Config.Backend + a bind address, which is the retired
		// per-node coordination model; a multi-node cluster is now
		// multi-backend and needs a fencing-capable BackendFactory, which
		// neither of this harness's backends (memory, pebble) can provide.
		if *nodes != 1 || *rf != 1 {
			_, _ = fmt.Fprintf(stderr, "shale-bench: --mode=cluster is single-node (--nodes=1 --rf=1); "+
				"multi-node requires a BackendFactory-capable backend, which this harness does not wire\n")
			return 2
		}
	}

	// Pebble needs a per-run scratch dir. We always create it and
	// always delete it on exit (even on bench failure) so a CI run
	// never leaks gigabytes into /tmp.
	var pebbleBaseDir string
	if *backendName == "pebble" {
		dir, err := os.MkdirTemp("", "shale-bench-pebble-")
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "shale-bench: scratch dir: %v\n", err)
			return 1
		}
		pebbleBaseDir = dir
		defer func() { _ = os.RemoveAll(pebbleBaseDir) }()
	}

	// Plumb --slate-await-durable through the cross-tag handoff. Only
	// meaningful for --backend=slate; the no-tag stub returns nil so
	// builds without -tags slatedb still compile. We reset on exit so
	// tests that share the package-level var across cases don't leak
	// one case's choice into the next.
	if *backendName == "slate" && !*slateAwaitDurable {
		slateWriteOpts = makeSlateWriteOptions(false)
	}
	defer func() { slateWriteOpts = nil }()

	sc := scenario{
		Mode:        *mode,
		Backend:     *backendName,
		Nodes:       *nodes,
		R:           *rf,
		Ops:         *ops,
		ValueSize:   *valueSize,
		Concurrency: *concurrency,
		PebbleDir:   pebbleBaseDir,
	}
	label := *scenarioLabel
	if label == "" {
		label = autoLabel(sc)
	}

	target, cleanup, err := buildTarget(sc, stderr)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "shale-bench: build target: %v\n", err)
		return 1
	}
	defer cleanup()

	payload := make([]byte, sc.ValueSize)
	if _, err := rand.Read(payload); err != nil {
		_, _ = fmt.Fprintf(stderr, "shale-bench: random fill: %v\n", err)
		return 1
	}

	writeStats, err := runPhase("write", target, sc.Concurrency, sc.Ops, payload, true)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "shale-bench: write phase: %v\n", err)
		return 1
	}
	readStats, err := runPhase("read", target, sc.Concurrency, sc.Ops, nil, false)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "shale-bench: read phase: %v\n", err)
		return 1
	}

	if *jsonOut {
		out := benchOutput{
			Scenario:    label,
			Mode:        sc.Mode,
			Backend:     sc.Backend,
			Nodes:       sc.Nodes,
			R:           sc.R,
			Ops:         sc.Ops,
			ValueSize:   sc.ValueSize,
			Concurrency: sc.Concurrency,
			Phases:      []phaseStats{writeStats, readStats},
		}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			return 1
		}
		return 0
	}

	_, _ = fmt.Fprintf(stdout, "scenario=%s mode=%s backend=%s nodes=%d r=%d ops=%d value=%dB conc=%d\n",
		label, sc.Mode, sc.Backend, sc.Nodes, sc.R, sc.Ops, sc.ValueSize, sc.Concurrency)
	_, _ = fmt.Fprintln(stdout, formatPhaseHuman(writeStats))
	_, _ = fmt.Fprintln(stdout, formatPhaseHuman(readStats))
	return 0
}

// autoLabel synthesizes a stable scenario name from the inputs so two
// runs of the same shape collide on the same label in --json output.
func autoLabel(sc scenario) string {
	if sc.Mode == "raw" {
		return fmt.Sprintf("raw-%s", sc.Backend)
	}
	return fmt.Sprintf("cluster-%s-n%d-r%d", sc.Backend, sc.Nodes, sc.R)
}

// buildTarget constructs whatever putGetter the scenario calls for,
// and returns a teardown function that releases its resources.
func buildTarget(sc scenario, stderr io.Writer) (putGetter, func(), error) {
	if sc.Mode == "raw" {
		be, err := openBackend(sc.Backend, sc.PebbleDir, "node-0")
		if err != nil {
			return nil, nil, fmt.Errorf("open backend: %w", err)
		}
		return &rawAdapter{be: be}, func() { _ = be.Close() }, nil
	}
	return buildCluster(sc, stderr)
}

// rawAdapter exposes a backend.Backend through the putGetter surface so
// the bench loop can drive either a raw backend or a shale cluster
// through the same call site.
type rawAdapter struct {
	be backend.Backend
}

func (r *rawAdapter) Put(key, value []byte) error { return r.be.Put(key, value) }
func (r *rawAdapter) Get(key []byte) ([]byte, error) {
	v, err := r.be.Get(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// clusterAdapter exposes a *cluster.Cluster through putGetter.
type clusterAdapter struct{ c *cluster.Cluster }

func (c *clusterAdapter) Put(key, value []byte) error { return c.c.Put(key, value) }
func (c *clusterAdapter) Get(key []byte) ([]byte, error) {
	v, err := c.c.Get(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// openBackend builds one backend instance. id distinguishes pebble
// data directories AND (for slate) per-node DbName prefixes so a
// multi-node scenario doesn't collide on the same store. memory
// ignores both args.
func openBackend(name, baseDir, id string) (backend.Backend, error) {
	switch name {
	case "memory":
		return memory.New(), nil
	case "pebble":
		dir := baseDir + "/" + id
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", dir, err)
		}
		return pebble.New(pebble.Config{Dir: dir})
	case "slate":
		return openSlateBackend(id)
	default:
		return nil, fmt.Errorf("unknown backend %q", name)
	}
}

// openSlateBackend is overridden by the slatedb-tagged build (see
// slate.go). The default (no-tag) build returns a clear error so the
// harness fails fast when an operator picks --backend=slate without
// rebuilding with -tags slatedb.
var openSlateBackend = func(id string) (backend.Backend, error) {
	return nil, fmt.Errorf("slate backend requires -tags slatedb build (got id=%s)", id)
}

// slateWriteOpts is the cross-build-tag handoff for the
// --slate-await-durable flag. Holds an opaque *slatedb.WriteOptions
// when set; the slate-tagged newSlateBackend constructor (slate.go)
// reads it back and threads it through slate.Config.WriteOptions. Nil
// means "no WriteOptions on the slate config" which leaves slatedb on
// its own default of AwaitDurable=true.
//
// Stored as `any` because this file (main.go) compiles without the
// slatedb build tag and cannot import the slatedb package. The slate.go
// file owns the type and the construction.
var slateWriteOpts any

// makeSlateWriteOptions is the type-erased factory for slateWriteOpts.
// Overridden by the slatedb-tagged build to return a typed
// *slatedb.WriteOptions{AwaitDurable: <flag>}; the default no-tag stub
// returns nil. This stub is only ever reached via --backend=slate,
// which already errors out earlier without the slatedb tag, so the
// no-tag branch is effectively unreachable.
var makeSlateWriteOptions = func(_ bool) any { return nil }

// builtCluster captures the state we have to tear down at end of run.
type builtCluster struct {
	clusters []*cluster.Cluster
	backends []backend.Backend
}

// buildCluster spins up ONE shale node in-process over the scenario's
// backend and returns it wrapped in a putGetter, so the bench measures
// what the cluster layer costs on top of the raw backend.
//
// Single-node by construction: no coordinator, no ring, no gRPC. A
// multi-node bench would need a BackendFactory (the only distributed model
// shale has is the unit lease handoff, and that requires opening a unit and
// fencing the prior writer); memory and pebble cannot satisfy that, and
// wiring the one backend that can (slate) would put MinIO on the critical
// path of every run, which this harness deliberately avoids.
func buildCluster(sc scenario, _ io.Writer) (putGetter, func(), error) {
	if sc.Nodes != 1 || sc.R != 1 {
		return nil, nil, fmt.Errorf("cluster mode is single-node (nodes=1, rf=1); got nodes=%d rf=%d", sc.Nodes, sc.R)
	}
	bc := &builtCluster{}
	cleanup := func() {
		for _, c := range bc.clusters {
			_ = c.Close()
		}
		for _, b := range bc.backends {
			_ = b.Close()
		}
	}

	be, err := openBackend(sc.Backend, sc.PebbleDir, "n1")
	if err != nil {
		return nil, nil, err
	}
	bc.backends = append(bc.backends, be)

	c, err := cluster.Open(cluster.Config{
		NodeID:    "n1",
		Backend:   be,
		LogOutput: io.Discard,
	})
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("cluster.Open(n1): %w", err)
	}
	bc.clusters = append(bc.clusters, c)

	return &clusterAdapter{c: bc.clusters[0]}, cleanup, nil
}

// runPhase issues exactly `total` operations with `concurrency` workers,
// pulling indexes from a shared atomic counter so over/undershoot is
// impossible regardless of how the scheduler interleaves goroutines.
// Latencies are recorded by index; no locking needed beyond the atomic.
// First error short-circuits the phase and is returned to the caller.
//
// The shape mirrors cmd/shale/cmd_bench.go's runPhase so the two
// benches produce comparable numbers (same workload, same accounting,
// same percentile computation).
func runPhase(name string, target putGetter, concurrency, total int, payload []byte, write bool) (phaseStats, error) {
	if total == 0 {
		return phaseStats{Phase: name}, nil
	}
	latencies := make([]time.Duration, total)
	var next int64
	var firstErr atomic.Value
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
				key := keyAt("bench:", idx)
				t0 := time.Now()
				var err error
				if write {
					err = target.Put(key, payload)
				} else {
					_, err = target.Get(key)
				}
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
		DurationMs: float64(dur.Microseconds()) / 1000.0,
		Throughput: tput,
		P50Ms:      float64(p50.Microseconds()) / 1000.0,
		P99Ms:      float64(p99.Microseconds()) / 1000.0,
	}, nil
}

// keyAt formats the i-th key in the bench keyspace. Width 8 keeps keys
// sorted up to 99999999 ops (well past anything this harness produces).
func keyAt(prefix string, i int64) []byte {
	return fmt.Appendf(nil, "%s%08d", prefix, i)
}

// percentile sorts xs in place and returns the q-th percentile via
// nearest-rank. Same algorithm `shale bench` uses, lifted intentionally
// so the two binaries produce identical p50 / p99 from the same data.
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
	idx := min(max(int(q*float64(n)+0.999999999), 1), n)
	return xs[idx-1]
}

// formatPhaseHuman renders one phase as the single-line summary the
// scripts/run-bench.sh driver and human readers both consume.
func formatPhaseHuman(p phaseStats) string {
	return fmt.Sprintf("  phase=%-5s ops=%d  duration=%.1fms  throughput=%.1f ops/s  p50=%.3fms  p99=%.3fms",
		p.Phase, p.Ops, p.DurationMs, p.Throughput, p.P50Ms, p.P99Ms)
}
