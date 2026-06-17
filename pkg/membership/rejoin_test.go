package membership

import (
	"testing"
	"time"
)

// waitForAtomic polls fn until it returns >= want or the deadline passes.
// Returns the last observed value + whether the threshold was reached.
func waitForAtomic(fn func() uint64, want uint64, timeout time.Duration) (uint64, bool) {
	deadline := time.Now().Add(timeout)
	var last uint64
	for time.Now().Before(deadline) {
		last = fn()
		if last >= want {
			return last, true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return last, false
}

// TestRejoinLoopPeriodicallyJoins pins the core fix: a Membership opened
// with seeds + a non-zero RejoinInterval runs a background goroutine that
// periodically re-Joins the seeds. Without the loop (the pre-fix state),
// RejoinAttempts stays 0 forever and this fails.
func TestRejoinLoopPeriodicallyJoins(t *testing.T) {
	seedPort := ephemeralPort(t)
	joinerPort := ephemeralPort(t)

	seed := openTestMembership(t, Config{
		NodeID:   "seed",
		BindAddr: bindAddr(seedPort),
		GRPCAddr: "127.0.0.1:28001",
	})
	_ = seed

	joiner := openTestMembership(t, Config{
		NodeID:         "joiner",
		BindAddr:       bindAddr(joinerPort),
		GRPCAddr:       "127.0.0.1:28002",
		Seeds:          []string{bindAddr(seedPort)},
		RejoinInterval: 40 * time.Millisecond,
	})

	// Expect at least a couple of re-Join attempts to land within a
	// comfortable window (interval is 40ms; 3 attempts ~= 120ms).
	got, ok := waitForAtomic(joiner.RejoinAttempts, 3, 3*time.Second)
	if !ok {
		t.Fatalf("rejoin loop did not run: RejoinAttempts=%d, want >=3", got)
	}
}

// TestRejoinDisabledWhenIntervalZero pins that RejoinInterval == 0 (the
// in-process test default) runs NO rejoin loop, even with seeds.
func TestRejoinDisabledWhenIntervalZero(t *testing.T) {
	seedPort := ephemeralPort(t)
	joinerPort := ephemeralPort(t)

	openTestMembership(t, Config{
		NodeID:   "seed0",
		BindAddr: bindAddr(seedPort),
		GRPCAddr: "127.0.0.1:28101",
	})
	joiner := openTestMembership(t, Config{
		NodeID:   "joiner0",
		BindAddr: bindAddr(joinerPort),
		GRPCAddr: "127.0.0.1:28102",
		Seeds:    []string{bindAddr(seedPort)},
		// RejoinInterval left 0 -> disabled.
	})

	time.Sleep(200 * time.Millisecond)
	if n := joiner.RejoinAttempts(); n != 0 {
		t.Fatalf("rejoin loop ran despite interval 0: RejoinAttempts=%d, want 0", n)
	}
}

// TestRejoinLoopSkipsAfterLeave pins that once a node has called Leave
// (graceful departure), the rejoin loop stops re-Joining and instead
// records skips. A departing / draining node must NOT re-advertise itself
// back into the cluster. Without the leaving-guard this fails: attempts
// keep climbing after Leave.
func TestRejoinLoopSkipsAfterLeave(t *testing.T) {
	seedPort := ephemeralPort(t)
	joinerPort := ephemeralPort(t)

	openTestMembership(t, Config{
		NodeID:   "seed2",
		BindAddr: bindAddr(seedPort),
		GRPCAddr: "127.0.0.1:28201",
	})
	joiner := openTestMembership(t, Config{
		NodeID:         "joiner2",
		BindAddr:       bindAddr(joinerPort),
		GRPCAddr:       "127.0.0.1:28202",
		Seeds:          []string{bindAddr(seedPort)},
		RejoinInterval: 30 * time.Millisecond,
	})

	// Let the loop run a few attempts first.
	if _, ok := waitForAtomic(joiner.RejoinAttempts, 2, 3*time.Second); !ok {
		t.Fatalf("rejoin loop never started")
	}

	if err := joiner.Leave(); err != nil {
		t.Fatalf("Leave: %v", err)
	}

	// After Leave, attempts must freeze and skips must climb.
	frozen := joiner.RejoinAttempts()
	if _, ok := waitForAtomic(joiner.RejoinSkips, 2, 3*time.Second); !ok {
		t.Fatalf("rejoin loop did not skip after Leave: RejoinSkips=%d", joiner.RejoinSkips())
	}
	// Give a couple more ticks to confirm attempts stay frozen.
	time.Sleep(150 * time.Millisecond)
	if after := joiner.RejoinAttempts(); after != frozen {
		t.Fatalf("rejoin attempted after Leave: was %d, now %d", frozen, after)
	}
}

// TestRejoinLoopStopsAfterClose pins that Close stops the rejoin
// goroutine cleanly (attempts freeze; no panic on a torn-down memberlist).
func TestRejoinLoopStopsAfterClose(t *testing.T) {
	seedPort := ephemeralPort(t)
	joinerPort := ephemeralPort(t)

	openTestMembership(t, Config{
		NodeID:   "seed3",
		BindAddr: bindAddr(seedPort),
		GRPCAddr: "127.0.0.1:28301",
	})
	joiner := openTestMembership(t, Config{
		NodeID:         "joiner3",
		BindAddr:       bindAddr(joinerPort),
		GRPCAddr:       "127.0.0.1:28302",
		Seeds:          []string{bindAddr(seedPort)},
		RejoinInterval: 30 * time.Millisecond,
	})

	if _, ok := waitForAtomic(joiner.RejoinAttempts, 2, 3*time.Second); !ok {
		t.Fatalf("rejoin loop never started")
	}
	if err := joiner.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	frozen := joiner.RejoinAttempts()
	time.Sleep(150 * time.Millisecond)
	if after := joiner.RejoinAttempts(); after != frozen {
		t.Fatalf("rejoin attempted after Close: was %d, now %d", frozen, after)
	}
}
