package membership

import (
	"fmt"
	"testing"
	"time"
)

// This file pins the periodic local-Meta re-broadcast loop (the
// MetaRefreshInterval fix for the rolling-restart stale-Meta convergence bug,
// #473). The loop calls memberlist.UpdateNode on a bounded interval so a peer
// holding a STALE view of this node's Meta (the reset-incarnation case after a
// staggered rolling restart) self-heals. The unit tests below pin the loop's
// scheduling / disable / skip / stop lifecycle via the MetaRefreshAttempts +
// MetaRefreshSkips observability counters; the propagation test pins the
// end-to-end effect (a local Meta change re-broadcast by the loop reaches a
// peer's snapshot).

// TestMetaRefreshLoopPeriodicallyBroadcasts pins the core fix: a Membership
// opened with a non-zero MetaRefreshInterval runs a background goroutine that
// periodically calls memberlist.UpdateNode (re-broadcasting the local Meta).
// Without the loop (the pre-fix state), MetaRefreshAttempts stays 0 forever and
// this fails. Unlike the rejoin loop it needs NO seeds: a node re-publishes its
// OWN metadata regardless of whether it has peers.
func TestMetaRefreshLoopPeriodicallyBroadcasts(t *testing.T) {
	port := ephemeralPort(t)

	m := openTestMembership(t, Config{
		NodeID:              "meta-solo",
		BindAddr:            bindAddr(port),
		GRPCAddr:            "127.0.0.1:28401",
		MetaRefreshInterval: 40 * time.Millisecond,
		// No Seeds: the meta-refresh loop must run anyway.
	})

	// Expect at least a couple of re-broadcasts within a comfortable window
	// (interval is 40ms; 3 attempts ~= 120ms).
	got, ok := waitForAtomic(m.MetaRefreshAttempts, 3, 3*time.Second)
	if !ok {
		t.Fatalf("meta-refresh loop did not run: MetaRefreshAttempts=%d, want >=3", got)
	}
}

// TestMetaRefreshDisabledWhenIntervalZero pins that MetaRefreshInterval == 0
// (the in-process test default) runs NO meta-refresh loop.
func TestMetaRefreshDisabledWhenIntervalZero(t *testing.T) {
	port := ephemeralPort(t)

	m := openTestMembership(t, Config{
		NodeID:   "meta-solo0",
		BindAddr: bindAddr(port),
		GRPCAddr: "127.0.0.1:28411",
		// MetaRefreshInterval left 0 -> disabled.
	})

	time.Sleep(200 * time.Millisecond)
	if n := m.MetaRefreshAttempts(); n != 0 {
		t.Fatalf("meta-refresh loop ran despite interval 0: MetaRefreshAttempts=%d, want 0", n)
	}
}

// TestMetaRefreshLoopSkipsAfterLeave pins that once a node has called Leave
// (graceful departure), the meta-refresh loop stops re-broadcasting and instead
// records skips. A departing / draining node must NOT re-advertise itself back
// into the cluster - identical guard to the rejoin loop. Without the
// leaving-guard this fails: attempts keep climbing after Leave.
func TestMetaRefreshLoopSkipsAfterLeave(t *testing.T) {
	port := ephemeralPort(t)

	m := openTestMembership(t, Config{
		NodeID:              "meta-leave",
		BindAddr:            bindAddr(port),
		GRPCAddr:            "127.0.0.1:28421",
		MetaRefreshInterval: 30 * time.Millisecond,
	})

	// Let the loop run a few attempts first.
	if _, ok := waitForAtomic(m.MetaRefreshAttempts, 2, 3*time.Second); !ok {
		t.Fatalf("meta-refresh loop never started")
	}

	if err := m.Leave(); err != nil {
		t.Fatalf("Leave: %v", err)
	}

	// After Leave, attempts must freeze and skips must climb.
	frozen := m.MetaRefreshAttempts()
	if _, ok := waitForAtomic(m.MetaRefreshSkips, 2, 3*time.Second); !ok {
		t.Fatalf("meta-refresh loop did not skip after Leave: MetaRefreshSkips=%d", m.MetaRefreshSkips())
	}
	// Give a couple more ticks to confirm attempts stay frozen.
	time.Sleep(150 * time.Millisecond)
	if after := m.MetaRefreshAttempts(); after != frozen {
		t.Fatalf("meta-refresh broadcast after Leave: was %d, now %d", frozen, after)
	}
}

// TestMetaRefreshLoopStopsAfterClose pins that Close stops the meta-refresh
// goroutine cleanly (attempts freeze; no panic on a torn-down memberlist).
func TestMetaRefreshLoopStopsAfterClose(t *testing.T) {
	port := ephemeralPort(t)

	m := openTestMembership(t, Config{
		NodeID:              "meta-close",
		BindAddr:            bindAddr(port),
		GRPCAddr:            "127.0.0.1:28431",
		MetaRefreshInterval: 30 * time.Millisecond,
	})

	if _, ok := waitForAtomic(m.MetaRefreshAttempts, 2, 3*time.Second); !ok {
		t.Fatalf("meta-refresh loop never started")
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frozen := m.MetaRefreshAttempts()
	time.Sleep(150 * time.Millisecond)
	if after := m.MetaRefreshAttempts(); after != frozen {
		t.Fatalf("meta-refresh broadcast after Close: was %d, now %d", frozen, after)
	}
}

// TestMetaRefreshPropagatesMetaToPeer is the end-to-end assertion: a node's
// LOCAL Meta change, when the only thing re-publishing it is the periodic
// meta-refresh loop, PROPAGATES to a peer's snapshot. This is the property the
// declarative-reshard unanimity gate depends on - a node's gossiped Meta (here,
// its declared unit count) reaching every peer.
//
// Construction: two nodes converge. n1 runs the meta-refresh loop; n2 does not
// touch n1's Meta. We mutate n1's local declared unit count IN-PACKAGE (the
// metaDelegate field, under its mutex - the same field SetDraining flips, the
// in-process stand-in for an operator re-declare), but we do NOT call
// UpdateNode by hand. The ONLY mechanism that can carry the new count to n2 is
// the periodic loop's UpdateNode re-broadcast. We then assert n2's snapshot of
// n1 picks up the new count. n1's own snapshot is NOT asserted: unlike
// SetDraining, this raw field poke does not update n1's local cache, so this
// test isolates the over-the-wire propagation that the loop drives.
func TestMetaRefreshPropagatesMetaToPeer(t *testing.T) {
	n1Port := ephemeralPort(t)
	n2Port := ephemeralPort(t)

	const n1Grpc = "127.0.0.1:28501"
	const n2Grpc = "127.0.0.1:28502"

	m1 := openTestMembership(t, Config{
		NodeID:              "mp-n1",
		BindAddr:            bindAddr(n1Port),
		GRPCAddr:            n1Grpc,
		MetaRefreshInterval: 40 * time.Millisecond,
	})
	m2 := openTestMembership(t, Config{
		NodeID:   "mp-n2",
		BindAddr: bindAddr(n2Port),
		GRPCAddr: n2Grpc,
		Seeds:    []string{bindAddr(n1Port)},
	})

	want := map[string]string{"mp-n1": n1Grpc, "mp-n2": n2Grpc}
	if err := waitForMembers(m1, want, 5*time.Second); err != nil {
		t.Fatalf("n1 convergence: %v", err)
	}
	if err := waitForMembers(m2, want, 5*time.Second); err != nil {
		t.Fatalf("n2 convergence: %v", err)
	}

	// Baseline: n2 sees n1 with an unknown (0) declared count.
	if mem, _ := findMember(m2, "mp-n1"); mem.DeclaredUnitCount != 0 {
		t.Fatalf("n2 baseline view of n1 declared count = %d, want 0", mem.DeclaredUnitCount)
	}

	// Mutate n1's LOCAL declared count without calling UpdateNode ourselves.
	// Only the periodic meta-refresh loop's re-broadcast can carry this to n2.
	const newCount = 16
	m1.meta.mu.Lock()
	m1.meta.declaredUnitCount = newCount
	m1.meta.mu.Unlock()

	// The peer must observe the new count purely via the loop's re-broadcast.
	if err := waitForDeclaredCount(m2, "mp-n1", newCount, 5*time.Second); err != nil {
		t.Fatalf("n2 did not pick up n1's re-broadcast declared count: %v", err)
	}

	// Sanity: the loop actually fired (the broadcasts are what carried it).
	// Poll rather than read instantly: a tick's MetaRefreshAttempts increment
	// lands only AFTER its UpdateNode returns (bounded by metaUpdateTimeout),
	// and the queued broadcast can already have propagated the new Meta to the
	// peer before that return - so an instant read can still see 0 even though
	// the loop is what carried the change.
	if _, ok := waitForAtomic(m1.MetaRefreshAttempts, 1, 5*time.Second); !ok {
		t.Fatalf("declared count propagated but MetaRefreshAttempts stayed 0 - propagation was not from the loop")
	}
}

// waitForDeclaredCount polls until the snapshot's member id reports the wanted
// declared unit count, or the timeout fires.
func waitForDeclaredCount(m *Membership, id string, want uint32, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last Member
	for time.Now().Before(deadline) {
		mem, ok := findMember(m, id)
		last = mem
		if ok && mem.DeclaredUnitCount == want {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("did not observe %s declaredUnitCount=%d in %s; last=%+v", id, want, timeout, last)
}
