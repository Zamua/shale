//go:build chaosreal

package chaos

// REAL-CLUSTER chaos orchestrator (the step-2 "prove it on a REAL cluster"
// validation). It graduates the SAME no-acked-write-loss oracle (oracle.go, which
// carries no build tag) from the in-process soak to a deploy of SEPARATE OS
// PROCESSES: real shaled-slate children, real gRPC + memberlist network, a SHARED
// MinIO bucket as the durable object-storage backing. One invocation manages the
// whole lifecycle:
//
//	go test -tags chaosreal ./tests/chaos/ -run TestRealClusterChaos -v -timeout 10m
//
// gated behind //go:build chaosreal so the normal suite and the in-process chaos
// soak (//go:build chaos) are untouched.
//
// REQUIRED ENV (the run reports the precise missing piece + skips, rather than
// faking a run, when anything is absent):
//
//	SHALE_REAL_BINARY     abs path to a prebuilt shaled-slate (-tags slatedb, cgo)
//	SHALE_REAL_LIBDIR     dir holding libslatedb_uniffi.* (DYLD_LIBRARY_PATH)
//	SHALE_REAL_ENDPOINT   S3-compatible endpoint URL (MinIO), e.g. http://127.0.0.1:9000
//	SHALE_REAL_BUCKET     a FRESH bucket dedicated to this run (the shared backing)
//	SHALE_REAL_ACCESS_KEY S3 access key
//	SHALE_REAL_SECRET_KEY S3 secret key
//	SHALE_REAL_REGION     (optional; default us-east-1)
//	SHALE_REAL_NODES      (optional; default 2)
//	SHALE_REAL_DURATION   (optional; default 20s workload window)
//	SHALE_REAL_WRITERS    (optional; default 4)
//	SHALE_REAL_SEED        (optional; default 1; ALWAYS logged for reproducibility)
//
// ===========================================================================
// WHAT THIS DEPLOY CAN AND CANNOT PROVE (read before trusting a green run)
// ===========================================================================
//
// shaled-slate runs the LEGACY per-node backend mode: ONE slate.Slate per
// process, the cluster ring routing each key to a single owner node. It is NOT
// wired for multi-backend mode (BackendFactory + UnitCount), because no slatedb
// BackendFactory exists in the repo and shaled-slate exposes no unit-count flag.
// Two consequences bound this orchestrator:
//
//  1. RESHARD is unavailable. Cluster.Reshard refuses outside multi-backend mode
//     ("Reshard is only valid in multi-backend mode"), and there is no operator
//     Reshard RPC on the gRPC surface anyway. The Reshard seam is present and
//     returns errReshardUnsupported; the orchestrator records the gap.
//
//  2. CROSS-PROCESS DURABILITY HANDOFF is unavailable. Each node owns a DISTINCT
//     slatedb instance (a distinct DbName prefix in the shared bucket, because two
//     writers on one (bucket, dbName) fence each other). When a node is SIGKILLed,
//     its OWNED keys live durably in ITS OWN slatedb instance - but the survivor
//     owns a DIFFERENT instance and legacy mode has no per-unit lease handoff to
//     re-mount the dead node's instance. So a survivor cannot serve a dead OWNER's
//     keys. Multi-backend mode (one slatedb-per-unit, keyed by GenUnit, re-mounted
//     by the survivor on lease re-acquire) is what would close this; it is not yet
//     wired for slate.
//
// THEREFORE the durability assertion here is scoped HONESTLY:
//
//   - It SIGKILLs a non-founder node mid-workload and, for every key the oracle
//     acked before the kill, classifies the post-kill read from the SURVIVING
//     FOUNDER as SURVIVED (correct value served) or UNAVAILABLE (no live node
//     serves it). It NEVER counts an UNAVAILABLE owner-killed key as a test
//     failure - that is the known legacy-mode gap, reported as a metric.
//   - The HARD ASSERTION (a test failure) is the no-acked-write-loss oracle on
//     the keys the cluster CAN still serve: a key that a survivor serves must
//     serve the CORRECT latest acked value (never a stale / corrupt / resurrected
//     one), and every key the FOUNDER owns must survive any non-founder death.
//     A WRONG value, or a founder-owned key going missing, is a real bug and fails
//     the run. This proves the real gRPC + memberlist + durable-object-store path
//     for the slice of the durability story legacy mode is responsible for, while
//     documenting (not faking) the slice it cannot cover.
//   - The killed node is RESTARTED and the run asserts it rejoins (member count
//     re-converges) and serves its re-derived keys.
//
// A green run here means: the real network + real process death + real object
// storage do not lose or corrupt a write the legacy single-owner model is
// responsible for. It does NOT mean cross-process durable handoff works (that
// needs the multi-backend slate factory). See tests/chaos/README.md for the full
// detection-power discussion the in-process harness shares.

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// realMetrics is the accounting reported at the end. Atomic so the writer/reader
// goroutines update them lock-free.
type realMetrics struct {
	ackedPuts        atomic.Int64
	ackedDeletes     atomic.Int64
	readsVerified    atomic.Int64
	retryableRetries atomic.Int64
	transientReads   atomic.Int64

	evKill    atomic.Int64
	evRestart atomic.Int64
	evJoin    atomic.Int64
	evLeave   atomic.Int64

	// Durability-probe classification of acked keys after a mid-workload SIGKILL.
	durSurvived    atomic.Int64 // served correct latest value by a survivor
	durUnavailable atomic.Int64 // no live node served it (the legacy-mode gap)

	// churnWrongValues counts wrong-value reads observed during PHASE 2 (churn
	// coverage). These are INFORMATIONAL: under legacy per-node mode, ring churn with
	// no inter-instance data movement makes resurrection/staleness inherent, so they
	// are the documented gap, not a coordination bug and not a test failure.
	churnWrongValues atomic.Int64
	// writeFailures counts writes that never acked during PHASE 2 (the key's owner
	// was mid-departure with no handoff). Also INFORMATIONAL: never an acked-write
	// loss (the oracle records only acks), just a phase-2 availability count.
	writeFailures atomic.Int64
}

// realConfig carries the run knobs, resolved from env.
type realConfig struct {
	cluster  realClusterConfig
	nodes    int
	duration time.Duration
	writers  int
	seed     int64
}

// realViolation is one oracle verdict that failed (a HARD failure for this run).
type realViolation struct {
	verdict Verdict
	detail  string
}

type realViolationLog struct {
	mu   sync.Mutex
	list []realViolation
}

func (vl *realViolationLog) add(v Verdict, detail string) {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	vl.list = append(vl.list, realViolation{verdict: v, detail: detail})
}

func (vl *realViolationLog) snapshot() []realViolation {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	return append([]realViolation(nil), vl.list...)
}

// TestRealClusterChaos is the single-invocation orchestrator. It stands up the
// real cluster, runs a concurrent Put/Get/Delete workload through the real gRPC
// client while injecting real process kills/restarts/joins/leaves, asserts the
// no-acked-write-loss oracle on what the deploy is responsible for, runs the
// durability probe + a final sweep, and tears everything down (no orphans).
func TestRealClusterChaos(t *testing.T) {
	cfg, ok, why := resolveRealConfig(t)
	if !ok {
		t.Skipf("real-cluster chaos skipped: %s", why)
	}
	t.Logf("real-cluster chaos config: seed=%d nodes=%d duration=%s writers=%d bucket=%s endpoint=%s binary=%s",
		cfg.seed, cfg.nodes, cfg.duration, cfg.writers, cfg.cluster.bucket, cfg.cluster.endpoint, cfg.cluster.binaryPath)

	rep, err := runReal(cfg, t.Logf)
	if err != nil {
		t.Fatalf("real-cluster chaos: %v", err)
	}
	t.Log(rep.String())

	if vac, why := rep.vacuous(); vac {
		t.Fatalf("real-cluster chaos run was VACUOUS (proved nothing): %s", why)
	}
	if len(rep.violations) > 0 {
		for _, v := range rep.violations {
			t.Errorf("VIOLATION [%s]: %s", v.verdict, v.detail)
		}
		t.Fatalf("real-cluster chaos: %d acked-write-loss violation(s) on keys the deploy is responsible for", len(rep.violations))
	}
}

// resolveRealConfig reads the env. Returns ok=false + a precise reason when a
// required piece is missing, so the run REPORTS the blocker instead of faking it.
func resolveRealConfig(t *testing.T) (realConfig, bool, string) {
	t.Helper()
	bin := os.Getenv("SHALE_REAL_BINARY")
	if bin == "" {
		return realConfig{}, false, "SHALE_REAL_BINARY not set (build shaled-slate first: CGO_ENABLED=1 CGO_LDFLAGS=-L$HOME/.local/lib go build -tags slatedb -o /tmp/shaled-slate ./cmd/shaled-slate from backends/slate)"
	}
	if fi, err := os.Stat(bin); err != nil || fi.IsDir() {
		return realConfig{}, false, fmt.Sprintf("SHALE_REAL_BINARY %q is not a runnable file: %v", bin, err)
	}
	endpoint := os.Getenv("SHALE_REAL_ENDPOINT")
	if endpoint == "" {
		return realConfig{}, false, "SHALE_REAL_ENDPOINT not set (the MinIO/S3 URL, e.g. http://127.0.0.1:9000)"
	}
	bucket := os.Getenv("SHALE_REAL_BUCKET")
	if bucket == "" {
		return realConfig{}, false, "SHALE_REAL_BUCKET not set (a fresh dedicated bucket for this run)"
	}
	access := os.Getenv("SHALE_REAL_ACCESS_KEY")
	secret := os.Getenv("SHALE_REAL_SECRET_KEY")
	if access == "" || secret == "" {
		return realConfig{}, false, "SHALE_REAL_ACCESS_KEY / SHALE_REAL_SECRET_KEY not set (S3 credentials)"
	}
	libDir := os.Getenv("SHALE_REAL_LIBDIR")
	if libDir == "" {
		libDir = os.Getenv("HOME") + "/.local/lib"
	}
	region := os.Getenv("SHALE_REAL_REGION")
	if region == "" {
		region = "us-east-1"
	}

	cfg := realConfig{
		cluster: realClusterConfig{
			binaryPath: bin,
			libDir:     libDir,
			bucket:     bucket,
			endpoint:   endpoint,
			accessKey:  access,
			secretKey:  secret,
			region:     region,
			useSSL:     strings.EqualFold(os.Getenv("SHALE_REAL_USE_SSL"), "true"),
			logDir:     t.TempDir(),
		},
		nodes:    envIntReal("SHALE_REAL_NODES", 2),
		duration: envDurReal("SHALE_REAL_DURATION", 20*time.Second),
		writers:  envIntReal("SHALE_REAL_WRITERS", 4),
		seed:     int64(envIntReal("SHALE_REAL_SEED", 1)),
	}
	if cfg.nodes < 2 {
		cfg.nodes = 2
	}
	return cfg, true, ""
}

func envIntReal(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDurReal(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// realReport is the end-of-run accounting + verdict.
type realReport struct {
	seed             int64
	duration         time.Duration
	ackedPuts        int64
	ackedDeletes     int64
	readsVerified    int64
	retryableRetries int64
	transientReads   int64
	events           map[string]int64
	durSurvived      int64
	durUnavailable   int64
	churnWrongValues int64
	writeFailures    int64
	finalNodes       int
	reshardSupported bool
	violations       []realViolation
}

// vacuous reports whether the run failed to stress anything (so a green run cannot
// rubber-stamp a config that never overlapped chaos with the workload).
func (r *realReport) vacuous() (bool, string) {
	if r.ackedPuts == 0 {
		return true, "zero acked Puts: the workload never ran"
	}
	total := int64(0)
	for _, v := range r.events {
		total += v
	}
	if total == 0 {
		return true, "zero chaos events: the scheduler never fired"
	}
	if r.events["kill"] == 0 {
		return true, "zero kills: the durability test (the whole point) never fired a SIGKILL"
	}
	return false, ""
}

func (r *realReport) String() string {
	vio := "none"
	if len(r.violations) > 0 {
		vio = strconv.Itoa(len(r.violations))
	}
	return fmt.Sprintf(
		"real-cluster chaos report (seed=%d duration=%s):\n"+
			"  acked_puts=%d acked_deletes=%d\n"+
			"  reads_verified=%d retryable_retries=%d transient_reads=%d\n"+
			"  chaos_events=%v\n"+
			"  PHASE 1 (stable-membership durability, HARD-gated):\n"+
			"    durability_probe after SIGKILL: survived=%d unavailable(legacy-mode gap)=%d\n"+
			"  PHASE 2 (churn coverage, INFORMATIONAL - the legacy gap, not failures):\n"+
			"    churn_wrong_values=%d write_failures=%d\n"+
			"  reshard_supported=%t (false = legacy per-node mode; multi-backend slate factory absent)\n"+
			"  final_node_count=%d\n"+
			"  violations=%s",
		r.seed, r.duration,
		r.ackedPuts, r.ackedDeletes,
		r.readsVerified, r.retryableRetries, r.transientReads,
		r.events,
		r.durSurvived, r.durUnavailable,
		r.churnWrongValues, r.writeFailures,
		r.reshardSupported,
		r.finalNodes,
		vio,
	)
}
