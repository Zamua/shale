//go:build chaos

package chaos

// The gated entrypoint for the multi-node chaos/soak harness. It is built ONLY
// under `-tags chaos`, so the normal `go test ./...` never picks it up (the soak
// is long-running + churns nodes). Run it with:
//
//	go test -tags chaos ./tests/chaos/ -run TestChaosSoak -v
//	SHALE_CHAOS_SEED=1234 SHALE_CHAOS_DURATION=60s go test -tags chaos ./tests/chaos/ -run TestChaosSoak -v -timeout 5m
//
// See README.md for the full knob list, the scenarios, the oracle, and the
// metrics. The pass condition is: zero oracle violations AND the run was not
// vacuous (it actually acked writes, fired chaos events, and hit cutover
// windows).

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/goleakignore"
	"github.com/Zamua/shale/pkg/rebalance"
	"go.uber.org/goleak"
)

// TestMain shrinks the rebalance sweep tick so the in-process reconcile fires
// sub-second (the soak needs handoffs to settle within the per-event budgets),
// and wraps goleak so a regression that leaks a Cluster goroutine (events loop,
// reconcile, reshard barrier, peer fanout) surfaces as a binary leak. Known
// third-party background goroutines are ignored via the shared canonical list.
func TestMain(m *testing.M) {
	rebalance.SetSweepInterval(50 * time.Millisecond)
	goleak.VerifyTestMain(m, goleakignore.Options()...)
}

// loadConfig reads the run knobs from the environment, defaulting to a short,
// CI-friendly soak. Every knob is env-driven (no go-test flag plumbing). The seed
// is ALWAYS logged so a failing run is reproducible by re-exporting it.
func loadConfig(t *testing.T) config {
	t.Helper()
	cfg := config{
		seed:        1,
		duration:    5 * time.Second,
		writers:     6,
		readers:     4,
		nodes:       3,
		units:       8,
		chaosEvery:  600 * time.Millisecond,
		settleDelay: 300 * time.Millisecond,
	}
	cfg.seed = envInt64("SHALE_CHAOS_SEED", cfg.seed)
	cfg.duration = envDur("SHALE_CHAOS_DURATION", cfg.duration)
	cfg.writers = int(envInt64("SHALE_CHAOS_WRITERS", int64(cfg.writers)))
	cfg.readers = int(envInt64("SHALE_CHAOS_READERS", int64(cfg.readers)))
	cfg.nodes = int(envInt64("SHALE_CHAOS_NODES", int64(cfg.nodes)))
	cfg.units = int(envInt64("SHALE_CHAOS_UNITS", int64(cfg.units)))
	cfg.chaosEvery = envDur("SHALE_CHAOS_CHAOS_EVERY", cfg.chaosEvery)
	if cfg.nodes < 1 {
		cfg.nodes = 1
	}
	if cfg.writers < 1 {
		cfg.writers = 1
	}
	return cfg
}

// TestChaosSoak is the soak. It runs the seeded workload + chaos schedule and
// asserts zero oracle violations and a non-vacuous run. On failure it logs the
// full report (including the seed) and every violation so the failure is
// reproducible + diagnosable.
func TestChaosSoak(t *testing.T) {
	cfg := loadConfig(t)
	t.Logf("starting chaos soak: seed=%d duration=%s writers=%d readers=%d nodes=%d units=%d chaosEvery=%s",
		cfg.seed, cfg.duration, cfg.writers, cfg.readers, cfg.nodes, cfg.units, cfg.chaosEvery)
	t.Logf("REPRODUCE this run with: SHALE_CHAOS_SEED=%d SHALE_CHAOS_DURATION=%s go test -tags chaos ./tests/chaos/ -run TestChaosSoak -v",
		cfg.seed, cfg.duration)

	rep, err := run(cfg, t.Logf)
	if err != nil {
		t.Fatalf("chaos soak setup/run error: %v", err)
	}

	t.Log(rep.String())

	// The pass condition: ZERO oracle violations.
	if len(rep.violations) > 0 {
		for i, v := range rep.violations {
			if i >= 50 {
				t.Logf("  ... and %d more violations", len(rep.violations)-50)
				break
			}
			t.Errorf("ORACLE VIOLATION [%s]: %s", v.verdict, v.detail)
		}
		t.Fatalf("NO-ACKED-WRITE-LOST VIOLATED: %d oracle violation(s) across the soak (seed=%d). The harness caught a lost/stale/corrupt/resurrected acked write under chaos.", len(rep.violations), cfg.seed)
	}

	// Guard against a vacuous "green" run that never actually stressed the system.
	if vac, why := rep.vacuous(); vac {
		t.Fatalf("VACUOUS SOAK: %s (seed=%d). The run passed but proved nothing; increase duration / writers / chaos rate.", why, cfg.seed)
	}

	t.Logf("PASS: %d acked writes survived %d chaos events with zero loss (seed=%d)",
		rep.ackedPuts+rep.ackedDeletes+rep.ackedTransacts, totalEvents(rep), cfg.seed)
}

func totalEvents(r *report) int64 {
	var n int64
	for _, v := range r.events {
		n += v
	}
	return n
}

// --- env helpers ----------------------------------------------------------

func envInt64(name string, def int64) int64 {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func envDur(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
