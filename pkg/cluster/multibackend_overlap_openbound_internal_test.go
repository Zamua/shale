package cluster

// White-box pin for the node-wide open bound on the BACKGROUND overlap
// acquires (docs/SPEC.md "The pending owners acquire in the background"):
// acquireReplicaUnitOverlap's goroutines take a permit sized by
// Config.OpenConcurrency around each OpenReplicaUnit, so a node gaining many
// positions at once never runs more concurrent real-data opens than the SAME
// knob that bounds the boot mount pool (mountReplicaUnits,
// TestMountReplicaUnits_BoundedConcurrency). The default (unset -> 1) is the
// proven-safe strictly-sequential mode; concurrent real-data FFI opens are
// the documented read-corruption trigger (see defaultOpenConcurrency).

import (
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
)

// TestOverlapAcquire_BoundedByOpenConcurrency spawns many overlap acquires at
// once (each widened with an artificial delay so workers genuinely overlap)
// and reads back the factory handle's high-water mark of simultaneously
// in-flight opens. The default bound must degenerate to strictly-sequential
// opens (high-water exactly 1: the second open does not start until the
// first completes); an explicit limit must be reached but never exceeded.
// Every queued position must still end up mounted (the bound sequences the
// opens, it never drops one).
func TestOverlapAcquire_BoundedByOpenConcurrency(t *testing.T) {
	run := func(t *testing.T, limit int, wantMax int64) {
		t.Helper()
		backing := sharedfactory.NewBacking()
		// Single-member ring: n1 owns a position for every unit, giving plenty
		// of distinct positions to acquire concurrently.
		c := newReplicatedCluster(t, "n1", 16, 2, backing, "n1")
		if limit > 0 {
			// Set BEFORE the first acquire: the permit gate is sized lazily at
			// first use (mirroring the boot pool sizing itself at call time).
			c.cfg.OpenConcurrency = limit
		}
		h, ok := c.factory.(*sharedfactory.Handle)
		if !ok {
			t.Fatalf("factory is %T, want *sharedfactory.Handle", c.factory)
		}
		// Widen each open so bounded workers genuinely overlap; without a delay
		// an open can finish before the next goroutine is scheduled, hiding the
		// concurrency the bound would have permitted.
		h.SetAcquireDelay(60 * time.Millisecond)

		targets := c.desiredReplicaUnits()
		if len(targets) < 6 {
			t.Fatalf("test needs >=6 owned positions to exercise the bound, got %d", len(targets))
		}
		targets = targets[:6]
		// The reconcile's fresh-acquire sequence: record Acquiring, then spawn
		// the background open. All spawns land before any open completes, so
		// without the bound all 6 opens would run concurrently.
		for _, ru := range targets {
			c.beginAcquire(ru)
			c.acquireReplicaUnitOverlap(ru)
		}
		c.loopWG.Wait()

		for _, ru := range targets {
			if _, mounted := c.localBackendForReplicaUnit(ru); !mounted {
				t.Fatalf("limit %d: position %s never mounted (the bound must queue opens, not drop them)", limit, ru)
			}
		}
		if peak := h.MaxConcurrentOpens(); peak != wantMax {
			t.Fatalf("limit %d: max concurrent overlap opens = %d, want exactly %d (unbounded acquires would race the FFI open)",
				limit, peak, wantMax)
		}
	}

	// Default (OpenConcurrency unset): strictly sequential - the second open
	// must not start until the first completes.
	t.Run("default_is_sequential", func(t *testing.T) { run(t, 0, 1) })
	// Explicit limit: reached, never exceeded.
	t.Run("limit3_runs_3_in_parallel", func(t *testing.T) { run(t, 3, 3) })
}
