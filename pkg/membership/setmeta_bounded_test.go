package membership

// Regression pin for the SetJoining / SetDraining UpdateNode hang (the
// workstream-B goleak catch): memberlist.UpdateNode(0) means "wait FOREVER for
// the update broadcast's notify", and with any remembered-alive peer the notify
// can never fire (a superseding same-node broadcast invalidates it, or the peer
// is unreachable mid-teardown) - wedging the CALLER (the cluster's settle-timer
// reconcile goroutine, via maintainJoiningState -> setSelfJoining) permanently.
// The fix bounds both setters with metaUpdateTimeout, the same discipline the
// meta-refresh loop already uses for the identical hazard.
//
// The deterministic reproduction: a 2-node cluster whose PEER is hard-killed
// (transport shut down with no Leave) stays REMEMBERED as alive for the SWIM
// suspicion window, so anyAlive() is true and the broadcast can never be
// acknowledged. Pre-fix, SetJoining(false) blocks indefinitely; post-fix it
// returns within the bounded window.

import (
	"io"
	"testing"
	"time"
)

func TestSetJoiningReturnsBoundedWithUnreachablePeer(t *testing.T) {
	p1, p2 := ephemeralPort(t), ephemeralPort(t)
	m1, err := Open(Config{
		NodeID:   "sb1",
		BindAddr: bindAddr(p1),
		GRPCAddr: "127.0.0.1:45001",
		// Joining set at Open so the first Meta carries it; the CLEAR below is the
		// call under test.
		Joining:   true,
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("open m1: %v", err)
	}
	defer func() { _ = m1.Close() }()
	m2, err := Open(Config{
		NodeID:    "sb2",
		BindAddr:  bindAddr(p2),
		GRPCAddr:  "127.0.0.1:45002",
		Seeds:     []string{bindAddr(p1)},
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("open m2: %v", err)
	}

	// Wait until m1 sees the peer (so anyAlive() is true for the broadcast wait).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(m1.Members()) < 2 {
		time.Sleep(50 * time.Millisecond)
	}
	if len(m1.Members()) < 2 {
		t.Fatalf("m1 never saw the peer")
	}

	// HARD-KILL the peer (no Leave): m1 still remembers it alive for the SWIM
	// suspicion window, so an UpdateNode broadcast cannot be acknowledged.
	_ = m2.TestingShutdownNoLeave()

	// The call under test: clearing the Joining bit must return within the
	// bounded window (metaUpdateTimeout + margin), never hang. Pre-fix
	// (UpdateNode(0)) this blocked until the peer was reaped or forever.
	done := make(chan error, 1)
	go func() { done <- m1.SetJoining(false) }()
	select {
	case <-done:
		// Returned (nil on flush, or the bounded timeout error): both fine - the
		// queued broadcast propagates via normal gossip either way.
	case <-time.After(metaUpdateTimeout + 3*time.Second):
		t.Fatalf("SetJoining(false) did not return within the bounded window with an unreachable peer "+
			"(the UpdateNode(0) wait-forever hang; metaUpdateTimeout=%v)", metaUpdateTimeout)
	}

	// Same bound for the draining setter (the identical hazard, fixed together).
	done2 := make(chan error, 1)
	go func() { done2 <- m1.SetDraining(true) }()
	select {
	case <-done2:
	case <-time.After(metaUpdateTimeout + 3*time.Second):
		t.Fatalf("SetDraining(true) did not return within the bounded window with an unreachable peer")
	}
}
