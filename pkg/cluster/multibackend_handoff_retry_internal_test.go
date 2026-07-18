package cluster

// Unit tests for the v0.8 Phase 2d retry-on-acquiring policy (mechanism A).
//
// These pin the PURE policy of retryWriteThroughHandoff in isolation - no
// cluster, no backend, no network - by feeding it a hand-rolled `attempt`
// closure that returns scripted writeAttempt outcomes. They assert the three
// invariants the integration gate cannot cheaply isolate:
//
//	(1) BOUNDED BY WriteTimeout: a forever-retryable attempt does NOT spin
//	    forever; it returns the last retryable error once the wall-clock budget
//	    is spent, and it stops near the deadline (not wildly past it).
//	(2) NO BUSY-SPIN: the wrapper sleeps a backoff between attempts, so a
//	    forever-retryable run makes a small bounded number of attempts over the
//	    budget, not thousands of pure-spin re-runs.
//	(3) FAST PATHS: success returns immediately with no error; a NON-retryable
//	    (hard) outcome is returned immediately, unretried, so a real outage is
//	    never papered over by spinning until WriteTimeout.
//
// Plus the two pure helpers (classifyWriteAttempt, jitteredBackoff).

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryTestCluster builds the minimal Cluster the retry wrapper needs: it only
// reads c.cfg.WriteTimeout and c.retryAfterMs(). Nothing else is touched.
func retryTestCluster(writeTimeout time.Duration, retryAfterMs int) *Cluster {
	return &Cluster{cfg: Config{
		WriteTimeout:          writeTimeout,
		RebalanceRetryAfterMs: retryAfterMs,
	}}
}

// (1) + (2): a forever-retryable attempt is bounded by WriteTimeout and does
// NOT busy-spin (the backoff caps the attempt count well below a spin loop).
func TestRetryWriteThroughHandoff_BoundedByWriteTimeout(t *testing.T) {
	const writeTimeout = 300 * time.Millisecond
	c := retryTestCluster(writeTimeout, 20) // 20ms backoff base

	wantErr := status.Errorf(codes.Unavailable, "replicas mid-acquire")
	var attempts int
	start := time.Now()
	err := c.retryWriteThroughHandoff(func(ctx context.Context) writeAttempt {
		attempts++
		// The attempt context must carry a deadline at/under the budget.
		dl, ok := ctx.Deadline()
		if !ok {
			t.Errorf("attempt ctx has no deadline")
		} else if dl.After(start.Add(writeTimeout + 50*time.Millisecond)) {
			t.Errorf("attempt ctx deadline %v is past the WriteTimeout budget", dl)
		}
		return writeAttempt{err: wantErr, retryable: true}
	})
	elapsed := time.Since(start)

	// Bounded: it stopped, and surfaced the last retryable error.
	if err == nil {
		t.Fatalf("expected a retryable error after the budget expired, got nil")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v (%v)", status.Code(err), err)
	}
	// It must not run wildly past the deadline (allow generous slack for the
	// final backoff sleep clamp + scheduler).
	if elapsed > writeTimeout+250*time.Millisecond {
		t.Fatalf("ran %v, well past the %v WriteTimeout budget (did not bound?)", elapsed, writeTimeout)
	}
	// No busy-spin: with a >=10ms backoff base over a 300ms budget, attempts
	// stay in the low tens, not thousands. (A pure spin would be >>1000.)
	if attempts < 2 {
		t.Fatalf("expected at least 2 attempts (warm-up + at least one retry), got %d", attempts)
	}
	if attempts > 100 {
		t.Fatalf("busy-spin: %d attempts in %v (backoff not applied?)", attempts, writeTimeout)
	}
	t.Logf("bounded retry: %d attempts over %v, surfaced %v", attempts, elapsed, err)
}

// (3a) FAST PATH: a success on the first attempt returns nil immediately, no
// retries, well under the budget.
func TestRetryWriteThroughHandoff_SuccessImmediate(t *testing.T) {
	c := retryTestCluster(5*time.Second, 50)

	var attempts int
	start := time.Now()
	err := c.retryWriteThroughHandoff(func(_ context.Context) writeAttempt {
		attempts++
		return writeAttempt{} // success
	})
	if err != nil {
		t.Fatalf("expected nil on immediate success, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected exactly 1 attempt on success, got %d", attempts)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("success path took %v, expected near-instant", d)
	}
}

// (3b) FAST PATH: a NON-retryable (hard) outcome is returned immediately and
// is NOT retried - a real outage is never papered over by spinning to timeout.
func TestRetryWriteThroughHandoff_HardFailureNotRetried(t *testing.T) {
	c := retryTestCluster(5*time.Second, 50) // long budget on purpose

	hard := status.Errorf(codes.Unavailable, "shale: 2 acks needed, got 1 (1 failures: peer down)")
	var attempts int
	start := time.Now()
	err := c.retryWriteThroughHandoff(func(_ context.Context) writeAttempt {
		attempts++
		return writeAttempt{err: hard, retryable: false}
	})
	if err == nil {
		t.Fatalf("expected the hard error to surface, got nil")
	}
	if !errors.Is(err, hard) {
		t.Fatalf("expected the exact hard error, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("hard failure must NOT be retried: expected 1 attempt, got %d", attempts)
	}
	// It returned fast despite the 5s budget (did not spin to timeout).
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("hard failure path took %v, expected fast-fail (not spin to WriteTimeout)", d)
	}
}

// (1b) The retry RIDES OUT a transient window: an attempt that is retryable for
// the first K calls and then succeeds returns nil, having retried exactly K
// times - this is the "turn the blip into bounded latency" behavior.
func TestRetryWriteThroughHandoff_RidesOutThenSucceeds(t *testing.T) {
	c := retryTestCluster(5*time.Second, 10)

	const ridesOut = 3
	var attempts int
	err := c.retryWriteThroughHandoff(func(_ context.Context) writeAttempt {
		attempts++
		if attempts <= ridesOut {
			return writeAttempt{
				err:       status.Errorf(codes.Unavailable, "mid-acquire"),
				retryable: true,
			}
		}
		return writeAttempt{} // acquire finished; W now met
	})
	if err != nil {
		t.Fatalf("expected success once the window cleared, got %v", err)
	}
	if attempts != ridesOut+1 {
		t.Fatalf("expected %d attempts (rode out %d transients + 1 success), got %d",
			ridesOut+1, ridesOut, attempts)
	}
}

// (3c) CONTEXT CANCEL: the wrapper bounds attempts to the WriteTimeout it was
// constructed with; a tiny budget degrades to a single timeout-bounded error,
// never a deadlock. (The retry has no separate cancel channel by design - the
// WriteTimeout IS the cancel - so this asserts the budget-as-cancel contract.)
func TestRetryWriteThroughHandoff_TinyBudgetDegradesGracefully(t *testing.T) {
	c := retryTestCluster(1*time.Millisecond, 50)

	var attempts int
	start := time.Now()
	err := c.retryWriteThroughHandoff(func(_ context.Context) writeAttempt {
		attempts++
		return writeAttempt{
			err:       status.Errorf(codes.Unavailable, "mid-acquire"),
			retryable: true,
		}
	})
	if err == nil {
		t.Fatalf("expected a timeout-bounded error on a 1ms budget, got nil")
	}
	// One (maybe two) attempts, then the budget is spent. Crucially: it returns,
	// it does not hang.
	if attempts < 1 {
		t.Fatalf("expected at least one attempt, got %d", attempts)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("a 1ms-budget retry took %v - it should degrade to one bounded error fast", d)
	}
}

// classifyWriteAttempt: acks>=w -> success; acks<w & no errs -> retryable
// (acquiring); acks<w & errs -> hard (not retryable).
func TestClassifyWriteAttempt(t *testing.T) {
	if got := classifyWriteAttempt(2, 2, nil, nil); got.err != nil {
		t.Fatalf("acks==w should succeed, got err %v", got.err)
	}
	if got := classifyWriteAttempt(3, 2, nil, nil); got.err != nil {
		t.Fatalf("acks>w should succeed, got err %v", got.err)
	}

	transient := classifyWriteAttempt(1, 2, nil, nil)
	if transient.err == nil || !transient.retryable {
		t.Fatalf("acks<w with no errs must be a RETRYABLE shortfall, got %+v", transient)
	}
	if status.Code(transient.err) != codes.Unavailable {
		t.Fatalf("retryable shortfall should be Unavailable, got %v", status.Code(transient.err))
	}

	hard := classifyWriteAttempt(1, 2, []error{errors.New("peer down")}, nil)
	if hard.err == nil || hard.retryable {
		t.Fatalf("acks<w WITH errs must be a HARD (non-retryable) shortfall, got %+v", hard)
	}
}

// jitteredBackoff stays in [0.5d, 1.0d) and never panics on a zero/negative d.
func TestJitteredBackoff(t *testing.T) {
	if got := jitteredBackoff(0); got != 0 {
		t.Fatalf("jitteredBackoff(0) = %v, want 0", got)
	}
	if got := jitteredBackoff(-1); got != 0 {
		t.Fatalf("jitteredBackoff(negative) = %v, want 0", got)
	}
	const d = 100 * time.Millisecond
	for range 200 {
		got := jitteredBackoff(d)
		if got < d/2 || got >= d {
			t.Fatalf("jitteredBackoff(%v) = %v, want [%v, %v)", d, got, d/2, d)
		}
	}
}
