//go:build chaos && slatedb

package chaos

// THE BIG ONE: the full in-process multi-node chaos harness driven against the
// REAL slatedb BackendFactory over a real MinIO bucket, across a seed sweep,
// asserting ZERO acked-write loss.
//
// This is the production realization of the no-acked-write-loss gate. The
// in-memory soak (TestChaosSoak, default provider) proves the cluster logic; this
// proves the SAME logic against REAL durable storage + REAL slatedb fencing. The
// difference that matters: when a node is "killed" mid-soak, its units are real
// slatedb databases in MinIO, so a survivor's re-OpenUnit (the lease handoff)
// reads the dead node's acked bytes back FROM OBJECT STORAGE. A passing run
// therefore proves DURABLE-handoff losslessness - the thing the legacy
// real-cluster test could not, because it had no durable per-unit backing to read
// back across a kill.
//
// Gating: chaos && slatedb. Needs cgo + the native slatedb lib on the loader path
// + a running MinIO. Run it with:
//
//	CGO_ENABLED=1 \
//	CGO_LDFLAGS="-L$HOME/.local/lib" \
//	DYLD_LIBRARY_PATH="$HOME/.local/lib" \
//	SLATE_MINIO_ENDPOINT=http://127.0.0.1:9000 \
//	SLATE_MINIO_ACCESS=admin SLATE_MINIO_SECRET=supersecret \
//	go test -tags "chaos slatedb" ./tests/chaos/ -run TestChaosSoakSlate -v -timeout 30m
//
// Knobs (env, all optional):
//
//	SHALE_CHAOS_SLATE_SEEDS      number of seeds in the sweep (default 3)
//	SHALE_CHAOS_SLATE_SEED0      first seed value (default 1; seed i = SEED0+i)
//	SHALE_CHAOS_SLATE_DURATION   per-seed workload duration (default 25s)
//	SHALE_CHAOS_SLATE_UNITS      base unit count (default 4)
//	SHALE_CHAOS_SLATE_NODES      starting node count (default 3)
//	SHALE_CHAOS_SLATE_WRITERS    writer goroutines (default 4)
//	SHALE_CHAOS_SLATE_READERS    reader goroutines (default 3)
//
// Scale rationale: every unit is now a real LSM with its own memtable + its own
// object-store writer, so a slate-backed soak runs at a SMALLER scale than the
// in-memory soak (fewer units, a shorter duration). The defaults still fire the
// full v0.8 model - lease handoff on every kill/join/leave, the doubling reshard
// (the maxGen ceiling in runScheduler caps doublings so the real unit count stays
// bounded), the cluster-wide freeze, and the join-after-reshard generation path -
// while keeping N real slatedb instances comfortably in one process.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Zamua/shale/pkg/rebalance"
)

// env returns the value of key, or def when unset/empty.
func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// slateSweepConfig holds the per-sweep knobs read from env.
type slateSweepConfig struct {
	seeds       int
	seed0       int64
	duration    time.Duration
	units       int
	nodes       int
	writers     int
	readers     int
	chaosEvery  time.Duration
	settleScale float64
}

func loadSlateSweepConfig() slateSweepConfig {
	c := slateSweepConfig{
		seeds:    3,
		seed0:    1,
		duration: 30 * time.Second,
		units:    2,
		nodes:    3,
		writers:  3,
		readers:  2,
		// Slower chaos cadence than the in-memory soak: each event's settle is
		// backend-bound (real OpenUnit/flush round-trips to MinIO), so firing
		// events faster than the cluster can settle just piles up unsettled churn.
		chaosEvery: 600 * time.Millisecond,
		// Stretch the convergence + settle budgets: every OpenUnit / flush is a
		// real object-store round-trip, so a multi-unit cluster needs far longer to
		// mount every unit on its ring owner than the in-memory soak's
		// millisecond-scale budgets allow. Without this, the final sweep reads a
		// half-mounted cluster and reports durable-but-not-yet-mounted units as
		// false LOST verdicts. 8x was chosen empirically (a 2-unit/3-node slate
		// cluster settles well within the scaled budget while the in-memory soak's
		// behavior at 1x is unchanged).
		settleScale: 8,
	}
	c.seeds = int(envInt64("SHALE_CHAOS_SLATE_SEEDS", int64(c.seeds)))
	c.seed0 = envInt64("SHALE_CHAOS_SLATE_SEED0", c.seed0)
	c.duration = envDur("SHALE_CHAOS_SLATE_DURATION", c.duration)
	c.units = int(envInt64("SHALE_CHAOS_SLATE_UNITS", int64(c.units)))
	c.nodes = int(envInt64("SHALE_CHAOS_SLATE_NODES", int64(c.nodes)))
	c.writers = int(envInt64("SHALE_CHAOS_SLATE_WRITERS", int64(c.writers)))
	c.readers = int(envInt64("SHALE_CHAOS_SLATE_READERS", int64(c.readers)))
	c.chaosEvery = envDur("SHALE_CHAOS_SLATE_CHAOS_EVERY", c.chaosEvery)
	if sc := envInt64("SHALE_CHAOS_SLATE_SETTLE_SCALE", 0); sc > 0 {
		c.settleScale = float64(sc)
	}
	if c.seeds < 1 {
		c.seeds = 1
	}
	return c
}

// makeSlateBucket connects to MinIO and creates a uniquely-named bucket for the
// whole sweep, registering teardown on t.Cleanup. The bucket is shared across
// seeds; each seed runs under its own KeyPrefix (rotated by the provider's Reset)
// so no seed sees a prior seed's units. A unique bucket per test invocation keeps
// parallel / repeated runs from colliding.
func makeSlateBucket(t *testing.T, endpoint, access, secret string) string {
	t.Helper()
	host := endpoint
	for _, p := range []string{"http://", "https://"} {
		if len(host) >= len(p) && host[:len(p)] == p {
			host = host[len(p):]
		}
	}
	mc, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	bucket := fmt.Sprintf("shale-chaos-slate-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("create bucket %q (is MinIO running at %s?): %v", bucket, endpoint, err)
	}
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cleanCancel()
		// Empty the bucket, then remove it. Because a node may still be flushing a
		// slatedb instance when teardown begins (an OpenUnit/flush in flight on a
		// node being torn down), one list+delete pass can race a late-written
		// object and leave the bucket non-empty ("bucket is not empty" on Remove).
		// Loop a bounded handful of passes until a list returns zero objects, then
		// remove. This is teardown hygiene only; the unique per-run bucket name
		// means a leftover never collides with the next run.
		for pass := 0; pass < 6; pass++ {
			objCh := mc.ListObjects(cleanCtx, bucket, minio.ListObjectsOptions{Recursive: true})
			n := 0
			for rErr := range mc.RemoveObjects(cleanCtx, bucket, countingObjects(objCh, &n), minio.RemoveObjectsOptions{}) {
				if rErr.Err != nil {
					t.Logf("teardown: remove object %s: %v", rErr.ObjectName, rErr.Err)
				}
			}
			if n == 0 {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		if err := mc.RemoveBucket(cleanCtx, bucket); err != nil {
			t.Logf("teardown: remove bucket %q: %v", bucket, err)
		}
	})
	return bucket
}

// TestChaosSoakSlate runs the seed sweep against the real slatedb factory and
// asserts ZERO oracle violations and a non-vacuous run for every seed. The bucket
// the provider creates IS removed by makeSlateBucket's t.Cleanup; the provider's
// own Close (which would also remove the bucket) is wired so the provider does NOT
// re-create the bucket - it reuses the one this test made. To keep ownership of
// the bucket lifecycle in ONE place (the test, via t.Cleanup), the provider is
// constructed with the already-created bucket and its Close only prunes objects
// best-effort.
func TestChaosSoakSlate(t *testing.T) {
	endpoint := env("SLATE_MINIO_ENDPOINT", "http://127.0.0.1:9000")
	access := env("SLATE_MINIO_ACCESS", "admin")
	secret := env("SLATE_MINIO_SECRET", "supersecret")

	// The package-level TestMain (chaos_soak_test.go, built under -tags chaos)
	// already wraps the whole run in goleak.VerifyTestMain, so a leaked Cluster
	// goroutine surfaces at process exit. We re-assert the reconcile tick here so a
	// `-run TestChaosSoakSlate` in isolation still settles handoffs sub-second
	// within the per-event budgets (TestMain sets it too; setting it again is
	// idempotent).
	rebalance.SetSweepInterval(50 * time.Millisecond)

	bucket := makeSlateBucket(t, endpoint, access, secret)
	provider, err := newSlateFactoryProvider(slateProviderConfig{
		Endpoint:  endpoint,
		AccessKey: access,
		SecretKey: secret,
		Bucket:    bucket,
	})
	if err != nil {
		t.Fatalf("build slate provider: %v", err)
	}
	defer func() { _ = provider.Close() }()

	// Drain window for the cgo/HTTP teardown before the package-level goleak
	// (TestMain) samples at process exit. A slatedb instance Close + the minio-go
	// keep-alive pool finish their background goroutines ASYNC; this t.Cleanup runs
	// AFTER the sweep's CloseAll but BEFORE goleak, giving those non-Cluster
	// goroutines a moment to wind down so a rare slow teardown does not flap the
	// leak check. (The ignore list in goleak_slate_test.go is the primary guard;
	// this just removes the residual timing flake.) Registered before the sweep so
	// it runs last (LIFO).
	t.Cleanup(func() { time.Sleep(1500 * time.Millisecond) })

	swp := loadSlateSweepConfig()
	t.Logf("slate chaos sweep: seeds=%d seed0=%d duration=%s units=%d nodes=%d writers=%d readers=%d chaosEvery=%s settleScale=%.0fx bucket=%s endpoint=%s",
		swp.seeds, swp.seed0, swp.duration, swp.units, swp.nodes, swp.writers, swp.readers, swp.chaosEvery, swp.settleScale, bucket, endpoint)

	var totalAcked int64
	for i := 0; i < swp.seeds; i++ {
		seed := swp.seed0 + int64(i)
		cfg := config{
			seed:        seed,
			duration:    swp.duration,
			writers:     swp.writers,
			readers:     swp.readers,
			nodes:       swp.nodes,
			units:       swp.units,
			chaosEvery:  swp.chaosEvery,
			settleDelay: 300 * time.Millisecond,
			settleScale: swp.settleScale,
			noReshard:   envBool("SHALE_CHAOS_SLATE_NO_RESHARD"),
			reshardOnly: envBool("SHALE_CHAOS_SLATE_RESHARD_ONLY"),
		}
		t.Logf("=== slate seed %d/%d (seed=%d) ===", i+1, swp.seeds, seed)
		t.Logf("REPRODUCE: SHALE_CHAOS_SLATE_SEED0=%d SHALE_CHAOS_SLATE_SEEDS=1 SHALE_CHAOS_SLATE_DURATION=%s go test -tags \"chaos slatedb\" ./tests/chaos/ -run TestChaosSoakSlate -v", seed, swp.duration)

		rep, err := runWithProvider(provider, cfg, t.Logf)
		if err != nil {
			t.Fatalf("slate seed %d: setup/run error: %v", seed, err)
		}
		t.Log(rep.String())

		if len(rep.violations) > 0 {
			for j, v := range rep.violations {
				if j >= 50 {
					t.Logf("  ... and %d more violations", len(rep.violations)-50)
					break
				}
				t.Errorf("ORACLE VIOLATION (slate, seed=%d) [%s]: %s", seed, v.verdict, v.detail)
			}
			t.Fatalf("DURABLE-HANDOFF LOSS (slate, seed=%d): %d oracle violation(s). A real slatedb-backed handoff lost/staled/resurrected an acked write.", seed, len(rep.violations))
		}
		if vac, why := rep.vacuous(); vac {
			t.Fatalf("VACUOUS SLATE SOAK (seed=%d): %s. The run passed but proved nothing; raise duration/units.", seed, why)
		}
		acked := rep.ackedPuts + rep.ackedDeletes + rep.ackedTransacts
		totalAcked += acked
		t.Logf("PASS slate seed %d: %d acked writes survived %d chaos events with ZERO loss against real slatedb+MinIO",
			seed, acked, totalEvents(rep))
	}

	t.Logf("SLATE SWEEP PASS: %d seeds, %d total acked writes, ZERO durable-handoff loss across the sweep", swp.seeds, totalAcked)
}

// countingObjects adapts a ListObjects channel to the ObjectInfo channel
// RemoveObjects consumes, incrementing *n for each object passed through so the
// teardown loop can tell when a pass found nothing left to delete.
func countingObjects(in <-chan minio.ObjectInfo, n *int) <-chan minio.ObjectInfo {
	out := make(chan minio.ObjectInfo)
	go func() {
		defer close(out)
		for o := range in {
			if o.Err == nil {
				*n++
			}
			out <- o
		}
	}()
	return out
}
