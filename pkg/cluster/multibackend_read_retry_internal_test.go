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
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/ring"
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
	dial := status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7947: connect: connection refused"`)
	if isTransientReadLegErr(dial) {
		t.Fatalf("a refused dial is the UNREACHABLE class, not handoff-class transient (it earns the capped re-poll)")
	}
	if !isUnreachableLegErr(dial) {
		t.Fatalf("a refused dial must classify as an unreachable leg (docs/SPEC.md guard 2)")
	}
	if isTransientReadLegErr(status.Error(codes.FailedPrecondition, "shale: forwarding loop refused")) {
		t.Fatalf("the loop-guard refusal must stay non-transient")
	}
	if isTransientReadLegErr(status.Error(codes.Unknown, "apply batch: backend: closed (wrapped)")) {
		t.Fatalf("a wrapped error merely EMBEDDING the closed text must stay hard (exact-message match only)")
	}
}

// TestUnionReadRetry_OutageSurfacesDialErrorFast pins the time-bounded
// re-poll for unreachable-only sweeps (docs/SPEC.md guard 2): a replica set
// that is genuinely ALL down surfaces the FIRST dial error VERBATIM at ~the
// unreachable grace - not before it (the grace must be honored, or ring-lag
// windows surface spurious errors) and never the full ReadTimeout stall.
func TestUnionReadRetry_OutageSurfacesDialErrorFast(t *testing.T) {
	backing := sharedfactory.NewBacking()
	// self is NOT a ring member: every routed leg is a remote dial to a dead
	// address (bind a loopback port, close it, dial refused).
	c := newReplicatedCluster(t, "self", 4, 2, backing, "g1", "g2")
	c.cfg.ReadTimeout = 5 * time.Second
	c.cfg.RebalanceSettleDelay = time.Hour
	c.clients = make(map[string]*peerClient)
	t.Cleanup(func() {
		for _, cli := range c.clients {
			_ = cli.Close()
		}
	})
	rg := ring.New()
	for _, id := range []string{"g1", "g2"} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("probe listen: %v", err)
		}
		addr := l.Addr().String()
		_ = l.Close()
		rg.Add(ring.Member{ID: id, Addr: addr})
	}
	c.ring = rg

	key := []byte("rr-outage-key")
	t0 := time.Now()
	_, err := c.getReplicatedUnit(key)
	elapsed := time.Since(t0)
	if err == nil {
		t.Fatalf("Get against an all-down replica set must error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable || !strings.Contains(st.Message(), "connection refused") {
		t.Fatalf("Get outage error = %v, want the dial error surfaced verbatim (Unavailable + connection refused)", err)
	}
	if elapsed < unreachableOnlyGrace-200*time.Millisecond {
		t.Fatalf("Get outage surfaced after %v - BEFORE the %v unreachable grace (ring-lag windows would surface spurious errors)", elapsed, unreachableOnlyGrace)
	}
	if elapsed >= 3500*time.Millisecond {
		t.Fatalf("Get outage surfaced after %v - must be bounded at ~the %v grace, well under the %v budget", elapsed, unreachableOnlyGrace, c.cfg.ReadTimeout)
	}
	t.Logf("outage GET surfaced the dial error after %v (grace %v, budget %v)", elapsed, unreachableOnlyGrace, c.cfg.ReadTimeout)

	t0 = time.Now()
	_, err = c.scanReplicatedUnit(key)
	elapsed = time.Since(t0)
	if err == nil {
		t.Fatalf("ScanPrefix against an all-down replica set must error")
	}
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.Unavailable || !strings.Contains(st.Message(), "connection refused") {
		t.Fatalf("scan outage error = %v, want the dial error surfaced verbatim", err)
	}
	if elapsed < unreachableOnlyGrace-200*time.Millisecond || elapsed >= 3500*time.Millisecond {
		t.Fatalf("scan outage surfaced after %v - must land at ~the %v grace, well under the %v budget", elapsed, unreachableOnlyGrace, c.cfg.ReadTimeout)
	}
	t.Logf("outage SCAN surfaced the dial error after %v (grace %v, budget %v)", elapsed, unreachableOnlyGrace, c.cfg.ReadTimeout)
}

// TestUnionScan_WedgedLegBoundedByBudget pins the scan walk's deadline
// (docs/SPEC.md "Union scans"): a WEDGED-BUT-CONNECTED leg (a listener that
// accepts and never speaks) must not hang ScanPrefix - the open+prime is cut
// at the ReadTimeout deadline and the scan surfaces a bounded retryable
// error.
func TestUnionScan_WedgedLegBoundedByBudget(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "wedge")
	c.cfg.ReadTimeout = 800 * time.Millisecond
	c.cfg.RebalanceSettleDelay = time.Hour
	c.clients = make(map[string]*peerClient)
	t.Cleanup(func() {
		for _, cli := range c.clients {
			_ = cli.Close()
		}
	})

	// A live listener that accepts connections and never responds: the
	// transport connects, the stream prime blocks forever server-side.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("wedge listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, aerr := l.Accept()
			if aerr != nil {
				return
			}
			go func() {
				buf := make([]byte, 4096)
				for {
					if _, rerr := conn.Read(buf); rerr != nil {
						_ = conn.Close()
						return
					}
				}
			}()
		}
	}()
	rg := ring.New()
	rg.Add(ring.Member{ID: "wedge", Addr: l.Addr().String()})
	c.ring = rg

	key := []byte("rr-wedged-key")
	t0 := time.Now()
	_, err = c.scanReplicatedUnit(key)
	elapsed := time.Since(t0)
	if err == nil {
		t.Fatalf("ScanPrefix against a wedged leg must surface a bounded error")
	}
	if elapsed >= 3*time.Second {
		t.Fatalf("ScanPrefix took %v against a wedged leg - the walk must be cut at the %v budget, never hang", elapsed, c.cfg.ReadTimeout)
	}
	t.Logf("wedged-leg SCAN surfaced bounded error after %v (budget %v): %v", elapsed, c.cfg.ReadTimeout, err)
}

// TestUnionReadRetry_DeadMemberLegIsTransient pins the unreachable-member
// classification end to end (the staging capture: a union read leg dialed a
// just-departed member's dead address and the refused dial poisoned the
// gather). One routed leg dials a REAL refused address, the other is
// mid-acquire; the read must absorb both (re-poll) and serve once the local
// position mounts - never surface the dial error.
func TestUnionReadRetry_DeadMemberLegIsTransient(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "ghost")
	c.cfg.ReadTimeout = 3 * time.Second
	c.cfg.RebalanceSettleDelay = time.Hour
	c.clients = make(map[string]*peerClient) // the fixture never dials; this test's ghost leg does
	t.Cleanup(func() {
		for _, cli := range c.clients {
			_ = cli.Close()
		}
	})

	// Point the ghost member at a genuinely REFUSED loopback address (bind,
	// note the port, close) so its leg fails with the captured dial error.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	deadAddr := l.Addr().String()
	_ = l.Close()
	rg := ring.New()
	rg.Add(ring.Member{ID: "self", Addr: "self:0"})
	rg.Add(ring.Member{ID: "ghost", Addr: deadAddr})
	c.ring = rg

	key := []byte("rr-dead-member-key")
	gu := c.genUnitForKey(key)
	reps := c.unitReplicas(gu)
	selfIdx := -1
	for i, m := range reps {
		if m.ID == "self" {
			selfIdx = i
		}
	}
	if selfIdx < 0 {
		t.Fatalf("fixture: self not a replica of %q", key)
	}
	ru := storageunit.NewReplicaUnit(gu, uint8(selfIdx))

	// Seed the durable store for self's position, but leave it UNMOUNTED (the
	// mid-acquire leg); heal at +300ms.
	seedH := backing.Handle()
	sb, _, err := seedH.OpenReplicaUnit(ru, 1)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1, NodeID: "seed"}, Payload: []byte("v1")})
	if err := sb.Put(key, env); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	const healAfter = 300 * time.Millisecond
	armT := time.Now()
	healDone := make(chan struct{})
	go func() {
		defer close(healDone)
		time.Sleep(healAfter)
		c.reconcileMu.Lock()
		c.acquireReplicaUnit(ru)
		c.reconcileMu.Unlock()
	}()

	got, err := c.getReplicatedUnit(key)
	servedAt := time.Now()
	if err != nil {
		t.Fatalf("Get with a dead-member leg must be absorbed by the re-poll, got after %v: %v", servedAt.Sub(armT), err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get returned %q, want %q", got, "v1")
	}
	if servedAt.Before(armT.Add(healAfter)) {
		t.Fatalf("Get served %v after arming - before the heal (fixture leak)", servedAt.Sub(armT))
	}
	t.Logf("dead-member GET absorbed: served %v after arming (heal at +%v)", servedAt.Sub(armT), healAfter)
	<-healDone

	// Scan through the same shape (dead leg + now-mounted self leg): the walk
	// must skip the dead leg and serve from self.
	it, err := c.scanReplicatedUnit(key)
	if err != nil {
		t.Fatalf("ScanPrefix with a dead-member leg: %v", err)
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

// TestUnionReadRetry_LingeringDeadMemberAbsorbed pins the ring-lag window
// (docs/SPEC.md guard 2): a just-departed member's address lingers in the
// routed union until the membership update propagates and the ring rebuilds.
// During that window every sweep is unreachable-only - EXPECTED, transient -
// and the read must keep re-polling (within the unreachable grace) so it
// serves the moment the ring updates and a live position mounts, instead of
// surfacing the dial error mid-lag (the staging jwt11 battery-3 read-canary
// 500s).
func TestUnionReadRetry_LingeringDeadMemberAbsorbed(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "g1", "g2")
	c.cfg.ReadTimeout = 5 * time.Second
	c.cfg.RebalanceSettleDelay = time.Hour
	c.clients = make(map[string]*peerClient)
	t.Cleanup(func() {
		for _, cli := range c.clients {
			_ = cli.Close()
		}
	})

	// Phase 1 ring: both routed members dead (the reader's STALE view right
	// after the members departed - the lag window).
	staleRing := ring.New()
	for _, id := range []string{"g1", "g2"} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("probe listen: %v", err)
		}
		addr := l.Addr().String()
		_ = l.Close()
		staleRing.Add(ring.Member{ID: id, Addr: addr})
	}
	c.ring = staleRing

	key := []byte("rr-lag-key")
	gu := c.genUnitForKey(key)
	ru := storageunit.NewReplicaUnit(gu, 0)
	seedH := backing.Handle()
	sb, _, err := seedH.OpenReplicaUnit(ru, 1)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1, NodeID: "seed"}, Payload: []byte("v1")})
	if err := sb.Put(key, env); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	// At +800ms (well inside the unreachable grace, far past the old 2-sweep
	// cap) the RING UPDATES: the departed members vanish, self becomes the
	// owner and mounts the position - the reconcile-after-reap a real reader
	// performs.
	const ringUpdateAt = 800 * time.Millisecond
	armT := time.Now()
	updated := make(chan struct{})
	go func() {
		defer close(updated)
		time.Sleep(ringUpdateAt)
		// Mutate the ring IN PLACE (Remove/Add under the ring's own lock),
		// exactly as reconcileRingFromMembership does - the cluster never
		// swaps the ring pointer while routing reads it.
		c.ring.Remove("g1")
		c.ring.Remove("g2")
		c.ring.Add(ring.Member{ID: "self", Addr: "self:0"})
		c.reconcileMu.Lock()
		c.acquireReplicaUnit(ru)
		c.reconcileMu.Unlock()
	}()

	got, err := c.getReplicatedUnit(key)
	servedAt := time.Now()
	if err != nil {
		t.Fatalf("Get through the ring-lag window must be absorbed (re-polled until the ring updates), "+
			"got after %v: %v", servedAt.Sub(armT), err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get returned %q, want %q", got, "v1")
	}
	if servedAt.Before(armT.Add(ringUpdateAt)) {
		t.Fatalf("Get served %v after arming - before the ring update (fixture leak)", servedAt.Sub(armT))
	}
	t.Logf("ring-lag GET absorbed: served %v after arming (ring update at +%v)", servedAt.Sub(armT), ringUpdateAt)
	<-updated
}
