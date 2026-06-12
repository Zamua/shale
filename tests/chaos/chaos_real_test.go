//go:build chaosreal

package chaos

// REAL-CLUSTER chaos orchestrator (the step-2 "prove it on a REAL cluster"
// validation that CLOSES the v0.8 deploy gap). It graduates the SAME
// no-acked-write-loss oracle (oracle.go, which carries no build tag) from the
// in-process soak to a deploy of SEPARATE OS PROCESSES in MULTI-BACKEND mode: real
// shaled-slate children launched with `--unit-count > 1`, real gRPC + memberlist
// network, a SHARED MinIO bucket + key-prefix as the durable object-storage
// backing, and a real operator Reshard RPC driving an N -> 2N doubling on the
// running cluster. One invocation manages the whole lifecycle:
//
//	go test -tags chaosreal ./tests/chaos/ -run TestRealClusterChaos -v -timeout 15m
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
//	SHALE_REAL_UNIT_COUNT (optional; default 2 - a power of two > 1, multi-backend)
//	SHALE_REAL_KEY_PREFIX (optional; default a fresh per-run prefix)
//	SHALE_REAL_NODES      (optional; default 2)
//	SHALE_REAL_DURATION   (optional; default 20s workload window)
//	SHALE_REAL_WRITERS    (optional; default 4)
//	SHALE_REAL_SEED        (optional; default 1; ALWAYS logged for reproducibility)
//
// ===========================================================================
// WHAT THIS DEPLOY PROVES (read before trusting a green run)
// ===========================================================================
//
// shaled-slate runs MULTI-BACKEND mode: every node mounts the SAME shared bucket +
// key-prefix at the same unit count, owning whichever GenUnits the ring leases it,
// each unit a real slatedb LSM in object storage. This is the deployable v0.8
// model, so the orchestrator gates the FULL no-acked-write-loss story end to end:
//
//  1. DURABLE CROSS-PROCESS HANDOFF. When a node is SIGKILLed, its owned units are
//     real slatedb databases at a fixed object-store prefix. A survivor re-acquires
//     those leases (the reconcile-driven copy-free handoff) and re-OpenUnits them,
//     reading the dead node's acked bytes back FROM OBJECT STORAGE. So every acked
//     key the dead node owned must SURVIVE on a survivor once the handoff settles +
//     the client's faithful retry rides out the cutover. A key that stays
//     unreadable past the retry budget AND a post-settle re-probe is a real LOSS,
//     hard-gated - NOT a tolerated "legacy gap".
//
//  2. OPERATOR RESHARD (N -> 2N). The orchestrator issues the operator Reshard RPC
//     mid-run, driving a real cluster-coordinated doubling: the freeze barrier
//     coordinates across processes, each unit bisects by copying its keys through
//     object storage into two gen-(g+1) children, routing flips atomically. Every
//     acked write must survive the doubling. The run is VACUOUS (a failure) unless
//     at least one reshard COMMITTED, so a green run proves the reshard actually
//     fired - non-vacuously.
//
// THE HARD ASSERTION (a test failure): the unchanged no-acked-write-loss oracle on
// EVERY acked key, served through the real gRPC client across real process death,
// real handoff, and a real reshard. A key that reads back a WRONG value (stale /
// corrupt / resurrected) OR cannot be served at all by any live node past the
// retry-plus-settle budget is a violation. A read that is transiently unavailable
// or stale mid-cutover and then converges is the expected retryable turbulence
// (counted, not failed) - the same transient-vs-persisted rule the in-process soak
// uses. The smallest doubling the deployable binary supports is N=2 -> N=4 (the
// binary reserves --unit-count 1 for legacy single-Backend mode); the default run
// starts at N=2 and reshards to N=4.
//
// A green run here means: the real network + real process death + real object
// storage + a real reshard do not lose, corrupt, or resurrect any acked write. It
// is the validation that the v0.8 multi-backend model is actually DEPLOYABLE, not
// merely in-process correct. See tests/chaos/README.md for the full detection-power
// discussion the in-process harness shares.

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
	evReshard atomic.Int64 // operator reshards COMMITTED (the non-vacuous proof)

	// Durability-probe classification of acked keys after a mid-workload SIGKILL.
	// In multi-backend mode a survivor re-OpenUnits the dead node's units (the
	// copy-free handoff), so a killed owner's keys must SURVIVE once the handoff
	// settles. durSurvived is the success count; a key that stays unreadable past
	// the settle re-probe is a hard LOSS recorded in the violation log, NOT a
	// tolerated gap (legacy mode's "unavailable" tolerance is gone).
	durSurvived atomic.Int64

	// reshardFrom / reshardTo record the unit-count transition of the LAST committed
	// reshard (e.g. 2 -> 4). Zero/zero if no reshard committed (then the run is
	// vacuous - the reshard is the whole point).
	reshardFrom atomic.Uint64
	reshardTo   atomic.Uint64
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
	t.Logf("real-cluster chaos config: seed=%d nodes=%d unit_count=%d key_prefix=%q duration=%s writers=%d bucket=%s endpoint=%s binary=%s",
		cfg.seed, cfg.nodes, cfg.cluster.unitCount, cfg.cluster.keyPrefix, cfg.duration, cfg.writers, cfg.cluster.bucket, cfg.cluster.endpoint, cfg.cluster.binaryPath)

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
		t.Fatalf("real-cluster chaos: %d acked-write-loss violation(s) across real process death + handoff + reshard", len(rep.violations))
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

	// Multi-backend unit count: a power of two > 1 (the binary reserves 1 for legacy
	// mode, which cannot reshard). Default 2 - the smallest count that reshards (to
	// 4). Validate eagerly so a bad value reports a clear blocker, not a launch fail.
	unitCount := envIntReal("SHALE_REAL_UNIT_COUNT", 2)
	if unitCount < 2 || (unitCount&(unitCount-1)) != 0 {
		return realConfig{}, false, fmt.Sprintf("SHALE_REAL_UNIT_COUNT must be a power of two > 1 (multi-backend mode; 1 is legacy and cannot reshard), got %d", unitCount)
	}
	// Shared key-prefix: EVERY node mounts under this one prefix so they form one
	// cluster over one durable backing. A fresh per-run default keeps repeated runs
	// against the same bucket from colliding. Slate's BackingConfig expects a prefix
	// ending in "/" when non-empty.
	keyPrefix := os.Getenv("SHALE_REAL_KEY_PREFIX")
	if keyPrefix == "" {
		keyPrefix = fmt.Sprintf("real-%d/", time.Now().UnixNano())
	} else if !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
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
			unitCount:  unitCount,
			keyPrefix:  keyPrefix,
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
	startUnitCount   int
	reshardFrom      uint64
	reshardTo        uint64
	finalNodes       int
	violations       []realViolation
}

// vacuous reports whether the run failed to stress anything (so a green run cannot
// rubber-stamp a config that never overlapped chaos with the workload). For the
// deploy-gap validation a run MUST: ack writes, fire a SIGKILL (the handoff test),
// and COMMIT a reshard (the operator-reshard test). Any of these missing means the
// run proved nothing it was built to prove.
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
		return true, "zero kills: the durable-handoff test (a SIGKILL under load) never fired"
	}
	if r.events["reshard"] == 0 || r.reshardTo == 0 {
		return true, "zero reshards committed: the operator-reshard test (the deploy-gap proof) never fired"
	}
	return false, ""
}

func (r *realReport) String() string {
	vio := "none"
	if len(r.violations) > 0 {
		vio = strconv.Itoa(len(r.violations))
	}
	reshard := "NONE COMMITTED"
	if r.reshardTo != 0 {
		reshard = fmt.Sprintf("%d -> %d (committed)", r.reshardFrom, r.reshardTo)
	}
	return fmt.Sprintf(
		"real-cluster chaos report (seed=%d duration=%s, multi-backend):\n"+
			"  start_unit_count=%d\n"+
			"  acked_puts=%d acked_deletes=%d\n"+
			"  reads_verified=%d retryable_retries=%d transient_reads=%d\n"+
			"  chaos_events=%v\n"+
			"  durable-handoff probe after SIGKILL: survived=%d (killed-owner keys re-served by a survivor)\n"+
			"  operator reshard: %s\n"+
			"  final_node_count=%d\n"+
			"  violations=%s",
		r.seed, r.duration,
		r.startUnitCount,
		r.ackedPuts, r.ackedDeletes,
		r.readsVerified, r.retryableRetries, r.transientReads,
		r.events,
		r.durSurvived,
		reshard,
		r.finalNodes,
		vio,
	)
}
