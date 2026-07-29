package cluster

// White-box tests that need access to unexported names (the coordination
// field, the clients map, the settle-timer accounting) to pin regression
// behavior the public-API tests cannot reach.

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// freeTCPPort returns an OS-assigned ephemeral TCP port. Mirrors the
// helper in cluster_test.go but duplicated here so the internal test
// file compiles standalone.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeTCPPort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func hp(port int) string {
	return "127.0.0.1:" + strconv.Itoa(port)
}

// waitForLocalInView polls the coordination view until it contains the local
// member or the deadline expires. The coordinator seeds itself from its own
// join notification at Start, so this should be near-immediate but is racy
// enough to need a poll.
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

// openGossipNode opens a real multi-node Cluster on a loopback gossip
// coordinator whose ring reconcile is parked far in the future, so a
// background heal cannot steamroll a divergence a test induces on purpose.
func openGossipNode(t *testing.T, nodeID string, mutate func(*Config)) *Cluster {
	t.Helper()
	port := freeTCPPort(t)
	cfg := Config{
		NodeID:         nodeID,
		BackendFactory: memfactory.New(),
		UnitCount:      storageunit.MustUnitCount(2),
		GRPCAddr:       "127.0.0.1:1",
		LogOutput:      io.Discard,
		Coordinator: gossip.New(gossip.Config{
			BindAddr:          hp(port),
			LogOutput:         io.Discard,
			ReconcileInterval: time.Hour,
		}),
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

// gossipCoord exposes c's coordinator as the concrete gossip adapter, for the
// tests that drive its test seams.
func gossipCoord(c *Cluster) *gossip.Coordinator { return c.coord.(*gossip.Coordinator) }

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

// quiesceInitialSelfJoin drains the evaluation the local node's OWN
// self-join schedules, so a test can establish a deterministic at-rest
// baseline.
//
// The gossip transport fires a join notification for the local node, which the
// coordinator turns into a change hint. runCoordLoop drains it asynchronously
// and calls bumpRingGen -> scheduleEvaluate, which does settlePending.Add(1).
// That drain is NOT ordered against the test body: waitForLocalInView returns
// as soon as the view is SEEDED (synchronously, in Open), which can happen
// before the self-join hint is processed. Under -race the events loop is
// scheduled late enough that the re-Add / settlePending bump lands AFTER a
// test's at-rest assertion, which is the sole cause of both
// TestWaitForRebalanceIdle_BlocksWhileDebouncePending flaking under -race.
//
// This helper waits for that scheduled evaluation to appear
// (settlePending == 1), then runs it to completion via runEvaluateNow so
// settlePending returns to 0 and no background goroutine has an unrun re-arm
// left in flight. After it returns, the only writers that can still touch
// settlePending are the reconcile loop (which callers park far in the future)
// and the test's own explicit drives.
func quiesceInitialSelfJoin(t *testing.T, c *Cluster) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for c.settlePending.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("self-join evaluation never scheduled; settlePending stayed 0")
		}
		time.Sleep(5 * time.Millisecond)
	}
	forceReconcileNow(c)
	if got := c.settlePending.Load(); got != 0 {
		t.Fatalf("self-join evaluation did not drain; settlePending=%d", got)
	}
}

// TestEventsLoop_EvictsClientOnAddrChange pins issue 6: when a peer's
// GRPCAddr changes (NotifyUpdate path - same node ID, different Meta
// bytes), the events loop must (a) update the ring to point the ID at
// the new Addr and (b) evict the cached gRPC client for the OLD addr,
// so a stale connection to a defunct endpoint cannot serve subsequent
// requests for that peer.
//
// Drives the path via membership.TestingInjectUpdate (the test-only
// hook in pkg/membership), since memberlist exposes no public way to
// force a Meta-rebroadcast with a different addr for an existing
// node ID.
//
// Why this test would have caught a regression: if priorAddrForID or
// the evictClient call in runEventsLoop is dropped, the old client
// stays cached + a future forward dial returns the dead conn. The
// assertion on clients[oldAddr] absence fires.
func TestEventsLoop_EvictsClientOnAddrChange(t *testing.T) {
	// Park BOTH reconcilers far in the future (the cluster's unit reconcile and
	// the coordinator's own ring heal) so the manual view + clients state we set
	// up below cannot be steamrolled by a tick between the inject + assertion.
	saved := reconcileInterval
	reconcileInterval = time.Hour
	t.Cleanup(func() { reconcileInterval = saved })

	c := openGossipNode(t, "solo", nil)

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

	// Seed the ring + client cache as if a peer "peer-1" had joined
	// at oldAddr previously. Inject a NotifyUpdate that flips the
	// same ID to newAddr; the events loop should rewrite the ring +
	// evict the oldAddr client.
	addCoordMember(c, coord.Node{ID: "peer-1", Addr: oldAddr})

	conn, err := grpc.NewClient(oldAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial oldAddr: %v", err)
	}
	c.clientsMu.Lock()
	c.clients[oldAddr] = &peerClient{conn: conn}
	c.clientsMu.Unlock()

	gossipCoord(c).TestingInjectUpdate("peer-1", newAddr)

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
	c := openGossipNode(t, "rb-pending", func(cfg *Config) {
		cfg.RebalanceSettleDelay = time.Hour
	})

	if !waitForLocalInView(c, 2*time.Second) {
		t.Fatalf("local member never landed in the coordination view")
	}

	// Drain the evaluation the local node's own self-join schedules so we
	// have a deterministic at-rest baseline. Without this, the self-join's
	// scheduleEvaluate races the assertion below: with a time.Hour settle
	// the bump never decrements on its own, so an unhandled self-join makes
	// settlePending read 1 here under -race's late goroutine scheduling.
	quiesceInitialSelfJoin(t, c)

	// Sanity: a quiescent node is idle immediately (no pending, no
	// in-flight ranges).
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
	} else if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded while pending, got %v", err)
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
// settle-timer arming state machine in the direction the doc comment always
// promised but the code did not enforce: once an IMMEDIATE (zero-delay) pass
// is pending - the boot-defer prompt, the stale-mount evict - a later
// DEBOUNCED re-arm must not postpone it. Last-writer-wins here silently pushed
// the boot-defer prompt out a full production settle delay whenever a
// coalesced view hint landed just after boot (the coordination port delivers
// boot-time hints after Open returns), which turned "arming the reconcile
// immediately" into a multi-second write-refusal window that outlived the
// consumer's documented retry budget. Caught by
// TestFreshBoot_StaggeredJoin_NoWriteGap failing ~1-in-2 under the port.
//
// The pending-immediate state is INJECTED (a live fake timer + the flag), so
// the guard's decision is deterministic: the debounced re-arm must leave the
// pass immediate - observed behaviorally as the pending obligation draining
// promptly instead of waiting out the hour-long debounce it was (pre-fix)
// re-armed with.
func TestScheduleReconcileIn_DebouncedRearmMustNotPostponeImmediate(t *testing.T) {
	saved := reconcileInterval
	reconcileInterval = time.Hour
	t.Cleanup(func() { reconcileInterval = saved })

	c := openGossipNode(t, "sticky-prompt", func(cfg *Config) {
		cfg.RebalanceSettleDelay = time.Hour
	})
	if !waitForLocalInView(c, 2*time.Second) {
		t.Fatal("local member never landed in the coordination view")
	}
	quiesceInitialSelfJoin(t, c)

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
