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
	"errors"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// openFenceWindow fences ru's mounted copy via a foreign higher-epoch open that
// sleeps holdFor before completing (eager fence: the bump lands at open ENTRY),
// returning once the fence is provably engaged. heal re-acquires the position
// on the cluster at armT+healAfter, restoring a serving leg mid-budget.
//
// It returns armT (the window origin every timing assertion anchors on) and
// healDone; the CALLER MUST drain healDone before opening another window or
// returning, so no heal goroutine ever leaks into a later window or a later
// -count iteration (the per-invocation reset hygiene that keeps these tests
// deterministic under -count=N).
func openFenceWindow(t *testing.T, c *Cluster, backing *sharedfactory.Backing, ru storageunit.ReplicaUnit, holdFor, healAfter time.Duration) (armT time.Time, healDone chan struct{}) {
	t.Helper()
	armT = time.Now()
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
	healDone = make(chan struct{})
	go func() {
		defer close(healDone)
		time.Sleep(time.Until(armT.Add(healAfter)))
		// The cluster re-acquires its position above the foreign epoch - the
		// (scripted, deterministic) self-heal a real cluster's reconcile
		// performs after the eviction.
		c.reconcileMu.Lock()
		c.acquireReplicaUnit(ru)
		c.reconcileMu.Unlock()
	}()
	return armT, healDone
}

func TestUnionReadRetry_ServesThroughFenceWindow(t *testing.T) {
	backing := sharedfactory.NewBacking()
	backing.SetEagerFence(true)        // real slatedb: fence at open-START
	backing.SetStrictReadFencing(true) // real slatedb: fenced handle fails reads
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self")
	c.cfg.ReadTimeout = 3 * time.Second
	// Pin the eviction-armed debounced reconcile (evictStaleMount ->
	// scheduleReconcile) out of the test window: the binary-wide test settle
	// delay is 300ms, so without this the cluster's OWN self-heal re-mounts the
	// evicted position ~300ms in and races the scripted 400ms heal - the
	// nondeterminism seen under -count=N. The scripted heal is the only healer.
	c.cfg.RebalanceSettleDelay = time.Hour

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

	const holdFor, healAfter = 300 * time.Millisecond, 400 * time.Millisecond

	// GET issued INTO the fence window: every union leg is transient (the
	// fenced mount recodes + evicts, then the position is mid-acquire) until
	// the heal at armT+healAfter. The read must WAIT it out and serve, not
	// error - and it cannot have served BEFORE the heal (there is nothing to
	// serve from), which is what the armT+healAfter lower bound pins.
	armT, healDone := openFenceWindow(t, c, backing, ru, holdFor, healAfter)
	t0 := time.Now()
	got, err := c.getReplicatedUnit(key)
	servedAt := time.Now()
	elapsed := servedAt.Sub(t0)
	if err != nil {
		t.Fatalf("Get into the fence window must succeed within ReadTimeout, got error after %v: %v", elapsed, err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get returned %q, want %q", got, "v1")
	}
	if servedAt.Before(armT.Add(healAfter)) {
		t.Fatalf("Get served %v after the window opened - BEFORE the heal at +%v; something other than the "+
			"scripted heal served the read (fixture leak)", servedAt.Sub(armT), healAfter)
	}
	if elapsed >= c.cfg.ReadTimeout {
		t.Fatalf("Get took %v, exceeding the ReadTimeout budget %v", elapsed, c.cfg.ReadTimeout)
	}
	t.Logf("fence-window GET served %v after the window opened (heal at +%v, budget %v)", servedAt.Sub(armT), healAfter, c.cfg.ReadTimeout)
	<-healDone // never let a pending heal leak into the next window

	// SCAN issued INTO a fresh fence window: same contract.
	armT, healDone = openFenceWindow(t, c, backing, ru, holdFor, healAfter)
	t0 = time.Now()
	it, err := c.scanReplicatedUnit(key)
	servedAt = time.Now()
	elapsed = servedAt.Sub(t0)
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
	if servedAt.Before(armT.Add(healAfter)) {
		t.Fatalf("scan served %v after the window opened - BEFORE the heal at +%v (fixture leak)", servedAt.Sub(armT), healAfter)
	}
	if elapsed >= c.cfg.ReadTimeout {
		t.Fatalf("scan took %v, exceeding the ReadTimeout budget %v", elapsed, c.cfg.ReadTimeout)
	}
	t.Logf("fence-window SCAN served %v after the window opened (heal at +%v, budget %v)", servedAt.Sub(armT), healAfter, c.cfg.ReadTimeout)
	<-healDone // reset hygiene: no goroutine outlives the test invocation
}

func TestUnionReadRetry_BudgetRespectedWhenNoLegServes(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self")
	c.cfg.ReadTimeout = 600 * time.Millisecond
	c.cfg.RebalanceSettleDelay = time.Hour // no background reconcile inside the budget window

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

// closedStubBackend models a mount whose close landed under an in-flight read
// leg (releaseReplicaUnit removes the map entry BEFORE CloseReplicaUnit, so a
// leg that resolved the handle in that instant reads a closing backend): every
// op returns backend.ErrClosed.
type closedStubBackend struct{}

func (closedStubBackend) Put(_, _ []byte) error        { return backend.ErrClosed }
func (closedStubBackend) Get(_ []byte) ([]byte, error) { return nil, backend.ErrClosed }
func (closedStubBackend) Delete(_ []byte) error        { return backend.ErrClosed }
func (closedStubBackend) ScanPrefix(_ []byte) (backend.Iterator, error) {
	return nil, backend.ErrClosed
}
func (closedStubBackend) Begin(_ backend.IsolationLevel) (backend.Transaction, error) {
	return nil, backend.ErrClosed
}
func (closedStubBackend) Close() error { return nil }

// TestUnionReadRetry_ClosedMountLegIsTransient pins the closed-mid-release
// classification (docs/SPEC.md "Union reads" guard 2): a union read/scan leg
// that reads backend.ErrClosed from a mount whose release landed under it is
// TRANSIENT - absorbed by the re-poll, which serves once the position is
// re-acquired - never a client-visible "backend: closed".
func TestUnionReadRetry_ClosedMountLegIsTransient(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self")
	c.cfg.ReadTimeout = 3 * time.Second
	c.cfg.RebalanceSettleDelay = time.Hour // scripted heal is the only healer

	key := []byte("rr-closed-key")
	ru := storageunit.NewReplicaUnit(c.genUnitForKey(key), 0)

	// Seed the position's durable store, then plant a CLOSED mount handle at
	// the position - the state a read leg observes when the release's close
	// lands between its map resolve and its Get.
	seedH := backing.Handle()
	sb, _, err := seedH.OpenReplicaUnit(ru, 1)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1, NodeID: "seed"}, Payload: []byte("v1")})
	if err := sb.Put(key, env); err != nil {
		t.Fatalf("seed put: %v", err)
	}
	c.mountMu.Lock()
	c.mountMap[ru] = closedStubBackend{}
	c.mountMu.Unlock()

	const healAfter = 300 * time.Millisecond
	heal := func() chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			time.Sleep(healAfter)
			c.reconcileMu.Lock()
			c.acquireReplicaUnit(ru) // re-mounts over the closed handle
			c.reconcileMu.Unlock()
		}()
		return done
	}

	armT := time.Now()
	healDone := heal()
	got, err := c.getReplicatedUnit(key)
	servedAt := time.Now()
	if err != nil {
		t.Fatalf("Get across a closed-mid-release mount must be absorbed by the re-poll, got after %v: %v",
			servedAt.Sub(armT), err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get returned %q, want %q", got, "v1")
	}
	if servedAt.Before(armT.Add(healAfter)) {
		t.Fatalf("Get served %v after arming - before the heal; something else served (fixture leak)", servedAt.Sub(armT))
	}
	t.Logf("closed-mount GET absorbed: served %v after arming (heal at +%v)", servedAt.Sub(armT), healAfter)
	<-healDone

	// Same for a SCAN leg: re-plant the closed handle over the healed mount.
	c.mountMu.Lock()
	c.mountMap[ru] = closedStubBackend{}
	c.mountMu.Unlock()
	armT = time.Now()
	healDone = heal()
	it, err := c.scanReplicatedUnit(key)
	servedAt = time.Now()
	if err != nil {
		t.Fatalf("ScanPrefix across a closed-mid-release mount must be absorbed, got after %v: %v",
			servedAt.Sub(armT), err)
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
	if servedAt.Before(armT.Add(healAfter)) {
		t.Fatalf("scan served %v after arming - before the heal (fixture leak)", servedAt.Sub(armT))
	}
	t.Logf("closed-mount SCAN absorbed: served %v after arming (heal at +%v)", servedAt.Sub(armT), healAfter)
	<-healDone
}

// TestUnionReadRetry_ClosedLegClassification pins the classifier itself,
// including the WIRE form: gRPC wraps a non-status error as codes.Unknown
// carrying the error text, so a remote leg's "backend: closed" arrives as
// exactly the captured `rpc error: code = Unknown desc = backend: closed`.
func TestUnionReadRetry_ClosedLegClassification(t *testing.T) {
	if !isTransientReadLegErr(backend.ErrClosed) {
		t.Fatalf("in-process backend.ErrClosed must classify transient on a read leg")
	}
	if !isTransientReadLegErr(status.Error(codes.Unknown, "backend: closed")) {
		t.Fatalf("wire-form Unknown/backend: closed must classify transient on a read leg")
	}
	if isTransientReadLegErr(status.Error(codes.Unknown, "some real server bug")) {
		t.Fatalf("an unrelated Unknown error must stay non-transient")
	}
	if isTransientReadLegErr(status.Error(codes.Unavailable, "connection refused")) {
		t.Fatalf("a down peer's dial error must stay non-transient for the read gather")
	}
}

// TestUnionReadRetry_ClosedClusterStillFailsFast pins the boundary: a client
// op entering a GENUINELY closed cluster gets backend.ErrClosed immediately
// from the entry not-ready check - the closed-mount reclassification applies
// to union legs only, never to the caller's own shut-down cluster.
func TestUnionReadRetry_ClosedClusterStillFailsFast(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self")
	c.cfg.ReadTimeout = 3 * time.Second
	c.closed.Store(true)

	t0 := time.Now()
	_, err := c.Get([]byte("any-key"))
	if !errors.Is(err, backend.ErrClosed) {
		t.Fatalf("Get on a closed cluster = %v, want backend.ErrClosed", err)
	}
	if el := time.Since(t0); el > 100*time.Millisecond {
		t.Fatalf("Get on a closed cluster took %v - must fail fast, not retry", el)
	}
	_, err = c.ScanPrefix([]byte("any-key"))
	if !errors.Is(err, backend.ErrClosed) {
		t.Fatalf("ScanPrefix on a closed cluster = %v, want backend.ErrClosed", err)
	}
}
