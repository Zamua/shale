package cluster

// White-box tests that need access to unexported names (the coordination
// field, the clients map, the settle-timer accounting) to pin regression
// behavior the public-API tests cannot reach.

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/coordstatic"
	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// waitForLocalInView polls the coordination view until it contains the local
// member or the deadline expires.
func waitForLocalInView(c *Cluster, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range c.Members() {
			if m.ID == c.cfg.NodeID {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// openStaticNode opens a real multi-node Cluster on a transport-free static
// coordinator holding only this node, so the tests fully control when (and
// whether) a view change arrives.
func openStaticNode(t *testing.T, nodeID string, mutate func(*Config)) *Cluster {
	t.Helper()
	self := coord.Node{ID: storageunit.NodeID(nodeID), Addr: "127.0.0.1:1"}
	cfg := Config{
		NodeID:         nodeID,
		BackendFactory: memfactory.New(),
		UnitCount:      storageunit.MustUnitCount(2),
		GRPCAddr:       "127.0.0.1:1",
		LogOutput:      io.Discard,
		Coordinator:    coordstatic.New(self, []coord.Node{self}),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// forceReconcileNow bypasses the settle-timer debounce and runs the
// pending reconcile immediately, adopting whatever pending obligation is
// outstanding so settlePending accounting stays balanced: if a live timer
// is armed we stop it and take ITS obligation (its callback will never
// fire), otherwise we mint a fresh one for this drive. Either way
// runScheduledReconcile's defer releases exactly one.
func forceReconcileNow(c *Cluster) {
	c.settleMu.Lock()
	if c.settleTimer != nil {
		if !c.settleTimer.Stop() {
			// Callback already fired and owns its own decrement.
			c.settlePending.Add(1)
		}
		c.settleTimer = nil
	} else {
		c.settlePending.Add(1)
	}
	c.settleMu.Unlock()
	c.runScheduledReconcile()
}

// TestEventsLoop_EvictsClientOnAddrChange pins issue 6: when a peer's
// GRPCAddr changes (same node ID, different address in the view), the
// coordination loop must (a) update the view to point the ID at the new
// Addr and (b) evict the cached gRPC client for the OLD addr, so a stale
// connection to a defunct endpoint cannot serve subsequent requests for
// that peer.
//
// The addr flip is driven through the static coordinator's member seam:
// the cluster reacts to the change hint by re-reading the whole view and
// sweeping the client cache, so any mechanism that changes a member's
// address exercises the same eviction path.
//
// Why this test would have caught a regression: if the evictStaleClients
// sweep in onViewChanged is dropped, the old client stays cached + a
// future forward dial returns the dead conn. The assertion on
// clients[oldAddr] absence fires.
func TestEventsLoop_EvictsClientOnAddrChange(t *testing.T) {
	// Park the cluster's unit reconcile far in the future so the manual view +
	// clients state set up below cannot be steamrolled by a tick between the
	// addr flip + assertion.
	saved := reconcileInterval
	reconcileInterval = time.Hour
	t.Cleanup(func() { reconcileInterval = saved })

	c := openStaticNode(t, "solo", nil)

	if !waitForLocalInView(c, 2*time.Second) {
		t.Fatalf("local member never landed in the coordination view")
	}

	// Stand up two throwaway gRPC listeners so dialing oldAddr +
	// newAddr returns real connections (grpc.NewClient is lazy enough
	// that even an unreachable addr "works", but a real listener
	// keeps the test honest re: what evict closes).
	oldLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("oldLis: %v", err)
	}
	defer func() { _ = oldLis.Close() }()
	oldSrv := grpc.NewServer()
	go func() { _ = oldSrv.Serve(oldLis) }()
	defer oldSrv.Stop()
	oldAddr := oldLis.Addr().String()

	newLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("newLis: %v", err)
	}
	defer func() { _ = newLis.Close() }()
	newSrv := grpc.NewServer()
	go func() { _ = newSrv.Serve(newLis) }()
	defer newSrv.Stop()
	newAddr := newLis.Addr().String()

	// Seed the view + client cache as if a peer "peer-1" had joined
	// at oldAddr previously. Flip the same ID to newAddr; the
	// coordination loop should rewrite the view + evict the oldAddr
	// client.
	addCoordMember(c, coord.Node{ID: "peer-1", Addr: oldAddr})

	conn, err := grpc.NewClient(oldAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial oldAddr: %v", err)
	}
	c.clientsMu.Lock()
	c.clients[oldAddr] = &peerClient{conn: conn}
	c.clientsMu.Unlock()

	addCoordMember(c, coord.Node{ID: "peer-1", Addr: newAddr})

	// Let the events loop drain + react. Poll for both conditions
	// (ring updated + client evicted) up to 2s; the actual reaction
	// is sub-millisecond on a quiet machine.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.clientsMu.RLock()
		_, stillCached := c.clients[oldAddr]
		c.clientsMu.RUnlock()
		gotAddr := ""
		for _, m := range c.Members() {
			if m.ID == "peer-1" {
				gotAddr = m.Addr
				break
			}
		}
		if !stillCached && gotAddr == newAddr {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Failure: report what we observed.
	c.clientsMu.RLock()
	_, stillCached := c.clients[oldAddr]
	c.clientsMu.RUnlock()
	var gotAddr string
	for _, m := range c.Members() {
		if m.ID == "peer-1" {
			gotAddr = m.Addr
		}
	}
	t.Fatalf("addr-change eviction did not happen: view peer-1 addr=%q (want %q), oldAddr still cached=%v",
		gotAddr, newAddr, stillCached)
}

// TestWaitForRebalanceIdle_BlocksWhileDebouncePending pins the
// pending-aware contract added for the test-sync rework: a node with a
// settle-timer evaluation SCHEDULED-but-not-yet-fired is NOT
// rebalance-idle, even though the Coordinator's range table is still
// empty. Before the fix, WaitForRebalanceIdle delegated straight to the
// Coordinator, which saw zero non-terminal ranges during the debounce
// window and returned "idle" prematurely; the evaluation then fired
// mid-assertion. This test arms a pending evaluation with a settle
// delay long enough that it cannot fire on its own, and asserts the
// wait blocks until the evaluation is forced to run + drain.
func TestWaitForRebalanceIdle_BlocksWhileDebouncePending(t *testing.T) {
	// Park the reconcile loop far in the future so a background reconcile
	// tick cannot bump settlePending behind the manual drive below.
	saved := reconcileInterval
	reconcileInterval = time.Hour
	t.Cleanup(func() { reconcileInterval = saved })

	// Long delay: the armed timer must NOT fire on its own during the test. We
	// drive the firing explicitly below.
	c := openStaticNode(t, "rb-pending", func(cfg *Config) {
		cfg.RebalanceSettleDelay = time.Hour
	})

	if !waitForLocalInView(c, 2*time.Second) {
		t.Fatalf("local member never landed in the coordination view")
	}

	// Sanity: a quiescent node is idle immediately (no pending, no
	// in-flight ranges). The static coordinator fires no hint at Open, so
	// the at-rest baseline is deterministic.
	if c.settlePending.Load() != 0 {
		t.Fatalf("expected settlePending==0 at rest, got %d", c.settlePending.Load())
	}
	ctxFast, cancelFast := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelFast()
	if err := c.WaitForRebalanceIdle(ctxFast); err != nil {
		t.Fatalf("quiescent node should be idle immediately, got %v", err)
	}

	// Arm a pending reconcile directly (mirrors what a membership event
	// does via bumpRingGen -> scheduleReconcile). The long settle delay
	// means the AfterFunc will not fire during the test window.
	c.scheduleReconcile()
	if got := c.settlePending.Load(); got != 1 {
		t.Fatalf("expected settlePending==1 after scheduleReconcile, got %d", got)
	}

	// WaitForRebalanceIdle MUST block now: the evaluation is scheduled
	// but unrun, so the node is not idle even though no range is in
	// flight yet. A bounded wait should time out (ctx deadline), proving
	// it did not return early.
	ctxBlock, cancelBlock := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelBlock()
	if err := c.WaitForRebalanceIdle(ctxBlock); err == nil {
		t.Fatal("WaitForRebalanceIdle returned nil while a debounce was pending; expected it to block")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		// The timeout is WRAPPED (it carries the settle machinery's state so
		// a stuck idle-wait names its stuck component); identity via Is.
		t.Fatalf("expected a DeadlineExceeded-wrapping error while pending, got %v", err)
	}

	// Re-arming a still-live timer must NOT double-count the pending
	// obligation.
	c.scheduleReconcile()
	if got := c.settlePending.Load(); got != 1 {
		t.Fatalf("expected settlePending to stay 1 on re-arm, got %d", got)
	}

	// Force the pending reconcile to run + drain. forceReconcileNow stops
	// the live timer, adopts its pending obligation, runs the reconcile,
	// and runScheduledReconcile's defer releases it. With a single-node
	// ring this node already owns every unit, so the reconcile applies no
	// moves and settles immediately.
	forceReconcileNow(c)

	// Now the node is idle: pending drained AND no non-terminal range.
	ctxIdle, cancelIdle := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelIdle()
	if err := c.WaitForRebalanceIdle(ctxIdle); err != nil {
		t.Fatalf("WaitForRebalanceIdle should return after the evaluation drained, got %v", err)
	}
	if got := c.settlePending.Load(); got != 0 {
		t.Fatalf("expected settlePending==0 after the evaluation drained, got %d", got)
	}
}

// TestScheduleReconcileIn_DebouncedRearmMustNotPostponeImmediate pins the
// arming machine THROUGH THE REAL TIMER: a debounced re-arm over a pending
// IMMEDIATE arm must leave the pass immediate, observed as the pending
// obligation draining promptly rather than waiting out the hour-long debounce.
// The decision itself is enumerated in internal/decide; what this adds is that
// the decision reaches a live timer whose callback actually runs the pass.
//
// The pending-immediate state is INJECTED (a live stand-in timer + the flag) so
// the arm is deterministic.
func TestScheduleReconcileIn_DebouncedRearmMustNotPostponeImmediate(t *testing.T) {
	saved := reconcileInterval
	reconcileInterval = time.Hour
	t.Cleanup(func() { reconcileInterval = saved })

	c := openStaticNode(t, "sticky-prompt", func(cfg *Config) {
		cfg.RebalanceSettleDelay = time.Hour
	})
	if !waitForLocalInView(c, 2*time.Second) {
		t.Fatal("local member never landed in the coordination view")
	}

	// Inject a PENDING IMMEDIATE arm: a live stand-in timer (an hour out, so
	// it cannot fire behind the assertions) plus the immediate flag and its
	// pending obligation - exactly the state the boot-defer prompt leaves
	// between scheduleReconcileIn(0) and the callback firing.
	c.settleMu.Lock()
	c.settlePending.Add(1)
	c.settleImmediate = true
	c.settleTimer = time.AfterFunc(time.Hour, func() {})
	c.settleMu.Unlock()

	// The coalesced view hint lands: bumpRingGen -> scheduleReconcile. The
	// guard must refuse the hour-long debounced replacement and keep the pass
	// immediate (it swaps in a zero-delay timer that fires the real callback).
	c.scheduleReconcile()

	// Not postponed == the obligation drains promptly. Pre-fix the debounced
	// re-arm owned the timer, so nothing fired for an hour and settlePending
	// stayed pinned at 1.
	deadline := time.Now().Add(3 * time.Second)
	for c.settlePending.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("immediate reconcile was postponed by a debounced re-arm (settlePending=%d)",
				c.settlePending.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The other direction is unchanged: an IMMEDIATE arm replaces a pending
	// DEBOUNCED one and the pass runs promptly.
	c.settleMu.Lock()
	c.settlePending.Add(1)
	c.settleImmediate = false
	c.settleTimer = time.AfterFunc(time.Hour, func() {})
	c.settleMu.Unlock()
	c.scheduleReconcileIn(0)
	deadline = time.Now().Add(3 * time.Second)
	for c.settlePending.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("zero-delay arm failed to win over a pending debounced timer (settlePending=%d)",
				c.settlePending.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestScheduleReconcileIn_FiredTimerRearmOwnsFreshObligation pins the SHELL's
// half of the fired-timer rule: that a timer which already fired is reported to
// the arming core as FIRED (the Stop probe returning false is the only signal
// there is), and that the core's fresh-obligation answer becomes a real
// settlePending increment. The rule's decision table is in internal/decide;
// mis-wiring the probe polarity or the increment here still double-releases one
// obligation, drives the counter negative and wedges every later idle-wait.
func TestScheduleReconcileIn_FiredTimerRearmOwnsFreshObligation(t *testing.T) {
	c := &Cluster{}
	c.cfg.RebalanceSettleDelay = time.Hour

	// A FIRED timer whose callback has not yet cleared the field: the timer
	// object has run (empty func - the callback's bookkeeping is played out
	// below), the field still references it, one obligation outstanding.
	fired := time.AfterFunc(0, func() {})
	time.Sleep(20 * time.Millisecond) // let it fire; Stop() now returns false
	c.settleMu.Lock()
	c.settlePending.Add(1)
	c.settleImmediate = true
	c.settleTimer = fired
	c.settleMu.Unlock()

	// The racing re-arm (the join view hint behind the boot-defer prompt).
	c.scheduleReconcile()

	if got := c.settlePending.Load(); got != 2 {
		t.Fatalf("re-arm over a FIRED timer must own a fresh obligation: settlePending=%d, want 2", got)
	}

	// The in-flight callback releases the ORIGINAL obligation; the
	// replacement timer's callback would release its own. Nothing can reach
	// -1 from here.
	c.settlePending.Add(-1)
	c.settleMu.Lock()
	if c.settleTimer != nil {
		c.settleTimer.Stop()
		c.settleTimer = nil
	}
	c.settleImmediate = false
	c.settleMu.Unlock()
	c.settlePending.Add(-1)
	if got := c.settlePending.Load(); got != 0 {
		t.Fatalf("balanced release should land at 0, got %d", got)
	}
}

// TestWaitForRebalanceIdle_NegativePendingDegradesToIdle pins the defense
// polarity: if the obligation counter ever drifts below zero despite the
// accounting fix, the idle-wait must degrade to EARLY idle, never spin until
// deadline (==0 is unsatisfiable from -1, which is how one boot-time race
// turned into every later readiness wait timing out).
func TestWaitForRebalanceIdle_NegativePendingDegradesToIdle(t *testing.T) {
	c := &Cluster{}
	c.settlePending.Store(-1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.WaitForRebalanceIdle(ctx); err != nil {
		t.Fatalf("negative pending must read as idle, got %v", err)
	}
}
