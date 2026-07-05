package cluster

// White-box pins for the union read/scan ALL-LEGS-TRANSIENT retry (docs/SPEC.md
// "Union reads" guard 2 / "Union scans"): a read that sweeps the union and finds
// every leg transiently unable to serve re-polls within the ReadTimeout budget
// instead of surfacing an error, and a cluster where no leg ever serves still
// errors at ~ReadTimeout (budget respected, no infinite wait).
//
// The transient window is created with the REAL fence seams (SetEagerFence +
// SetStrictReadFencing + a per-handle acquire delay): a foreign higher-epoch
// open fences the mounted copy at open-START, strict read fencing makes the
// fenced handle fail reads with ErrFenced (real slatedb's closed-on-fence), the
// fence-self-heal recodes that to the transient + evicts, and the cluster's
// re-acquire heals the position mid-budget - the exact fence-window blip
// observed on the staging surge (isolated single-cycle "unit handing off").

import (
	"bytes"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// openFenceWindow fences ru's mounted copy via a foreign higher-epoch open that
// sleeps holdFor before completing (eager fence: the bump lands at open ENTRY),
// returning once the fence is provably engaged. heal re-acquires the position
// on the cluster after healAfter, restoring a serving leg mid-budget.
func openFenceWindow(t *testing.T, c *Cluster, backing *sharedfactory.Backing, ru storageunit.ReplicaUnit, holdFor, healAfter time.Duration) {
	t.Helper()
	h := backing.Handle()
	h.SetAcquireDelay(holdFor)
	before := backing.OpenReplicaStartCount(ru)
	go func() {
		if b, _, err := h.OpenReplicaUnit(ru, 100); err == nil {
			_ = b.Close()
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for backing.OpenReplicaStartCount(ru) == before {
		if time.Now().After(deadline) {
			t.Fatalf("fence window never engaged (foreign open of %v did not start)", ru)
		}
		time.Sleep(5 * time.Millisecond)
	}
	go func() {
		time.Sleep(healAfter)
		// The cluster re-acquires its position above the foreign epoch - the
		// self-heal a real cluster's reconcile performs after the eviction.
		c.reconcileMu.Lock()
		c.acquireReplicaUnit(ru)
		c.reconcileMu.Unlock()
	}()
}

func TestUnionReadRetry_ServesThroughFenceWindow(t *testing.T) {
	backing := sharedfactory.NewBacking()
	backing.SetEagerFence(true)        // real slatedb: fence at open-START
	backing.SetStrictReadFencing(true) // real slatedb: fenced handle fails reads
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self")
	c.cfg.ReadTimeout = 3 * time.Second

	key := []byte("rr-window-key")
	ru := storageunit.NewReplicaUnit(c.genUnitForKey(key), 0)
	c.reconcileMu.Lock()
	c.acquireReplicaUnit(ru)
	c.reconcileMu.Unlock()
	env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1, NodeID: "seed"}, Payload: []byte("v1")})
	c.mountMu.RLock()
	b := c.mountMap[ru]
	c.mountMu.RUnlock()
	if b == nil {
		t.Fatalf("fixture: position %v not mounted", ru)
	}
	if err := b.Put(key, env); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// GET issued INTO the fence window: every union leg is transient (the
	// fenced mount recodes + evicts, then the position is mid-acquire) until
	// the heal at +400ms. The read must WAIT it out and serve, not error.
	openFenceWindow(t, c, backing, ru, 300*time.Millisecond, 400*time.Millisecond)
	t0 := time.Now()
	got, err := c.getReplicatedUnit(key)
	elapsed := time.Since(t0)
	if err != nil {
		t.Fatalf("Get into the fence window must succeed within ReadTimeout, got error after %v: %v", elapsed, err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get returned %q, want %q", got, "v1")
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("Get returned in %v - too fast to have crossed the fence window (window did not engage?)", elapsed)
	}
	if elapsed >= c.cfg.ReadTimeout {
		t.Fatalf("Get took %v, exceeding the ReadTimeout budget %v", elapsed, c.cfg.ReadTimeout)
	}
	t.Logf("fence-window GET served after %v (window heal at 400ms, budget %v)", elapsed, c.cfg.ReadTimeout)

	// SCAN issued INTO a fresh fence window: same contract.
	openFenceWindow(t, c, backing, ru, 300*time.Millisecond, 400*time.Millisecond)
	t0 = time.Now()
	it, err := c.scanReplicatedUnit(key)
	elapsed = time.Since(t0)
	if err != nil {
		t.Fatalf("ScanPrefix into the fence window must succeed within ReadTimeout, got error after %v: %v", elapsed, err)
	}
	seen := 0
	for {
		k, _, nerr := it.Next()
		if nerr != nil {
			t.Fatalf("scan Next: %v", nerr)
		}
		if k == nil {
			break
		}
		seen++
	}
	_ = it.Close()
	if seen == 0 {
		t.Fatalf("scan returned no pairs for a seeded key")
	}
	if elapsed < 200*time.Millisecond || elapsed >= c.cfg.ReadTimeout {
		t.Fatalf("scan elapsed %v outside the expected fence-window range [200ms, %v)", elapsed, c.cfg.ReadTimeout)
	}
	t.Logf("fence-window SCAN served after %v (window heal at 400ms, budget %v)", elapsed, c.cfg.ReadTimeout)
}

func TestUnionReadRetry_BudgetRespectedWhenNoLegServes(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self")
	c.cfg.ReadTimeout = 600 * time.Millisecond

	// Nothing is ever mounted: every sweep is all-transient forever. The read
	// must keep re-polling until ~ReadTimeout, then surface the retryable
	// acquiring error - bounded, never an infinite wait, never a not-found.
	key := []byte("rr-budget-key")
	t0 := time.Now()
	_, err := c.getReplicatedUnit(key)
	elapsed := time.Since(t0)
	if err == nil {
		t.Fatalf("Get with no serving leg must error at the budget")
	}
	if !isAcquiringErr(err) {
		t.Fatalf("Get error = %v, want the retryable acquiring error", err)
	}
	if elapsed < 550*time.Millisecond {
		t.Fatalf("Get gave up after %v - the budget (%v) was not used for re-polling", elapsed, c.cfg.ReadTimeout)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("Get took %v - far past the ReadTimeout budget %v", elapsed, c.cfg.ReadTimeout)
	}
	t.Logf("no-serving-leg GET surfaced the retryable error after %v (budget %v)", elapsed, c.cfg.ReadTimeout)

	t0 = time.Now()
	_, err = c.scanReplicatedUnit(key)
	elapsed = time.Since(t0)
	if err == nil {
		t.Fatalf("ScanPrefix with no serving leg must error at the budget")
	}
	if !isAcquiringErr(err) {
		t.Fatalf("ScanPrefix error = %v, want the retryable acquiring error", err)
	}
	if elapsed < 550*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("ScanPrefix elapsed %v outside the budget window [550ms, 3s)", elapsed)
	}
	t.Logf("no-serving-leg SCAN surfaced the retryable error after %v (budget %v)", elapsed, c.cfg.ReadTimeout)
}
