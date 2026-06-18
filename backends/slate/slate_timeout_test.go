//go:build slatedb

package slate

import (
	"testing"
	"time"
)

// TestRunWithTimeout_Completes: a fn that finishes within the budget returns its
// result and timedOut=false.
func TestRunWithTimeout_Completes(t *testing.T) {
	got, timedOut := runWithTimeout(time.Second, func() int { return 42 })
	if timedOut {
		t.Fatalf("timedOut=true for a fast fn")
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

// TestRunWithTimeout_TimesOut: a fn that runs longer than the budget returns the
// zero value and timedOut=true (and the call returns promptly, not after the fn
// finishes - the whole point: a stalled open must not block the caller).
func TestRunWithTimeout_TimesOut(t *testing.T) {
	start := time.Now()
	got, timedOut := runWithTimeout(20*time.Millisecond, func() int {
		time.Sleep(2 * time.Second) // simulates a stalled, un-cancellable open
		return 99
	})
	elapsed := time.Since(start)
	if !timedOut {
		t.Fatalf("timedOut=false for a fn that overran the budget")
	}
	if got != 0 {
		t.Fatalf("got %d, want the zero value 0 on timeout", got)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("runWithTimeout blocked %s; it must return at ~the timeout, not wait for fn", elapsed)
	}
}

// TestOpenTimeout_Default: no env override -> the default.
func TestOpenTimeout_Default(t *testing.T) {
	t.Setenv("SLATE_DB_OPEN_TIMEOUT", "")
	if got := openTimeout(); got != defaultDbOpenTimeout {
		t.Fatalf("openTimeout()=%s, want default %s", got, defaultDbOpenTimeout)
	}
}

// TestOpenTimeout_EnvOverride: a valid duration is honored; invalid/non-positive
// fall back to the default.
func TestOpenTimeout_EnvOverride(t *testing.T) {
	t.Setenv("SLATE_DB_OPEN_TIMEOUT", "40s")
	if got := openTimeout(); got != 40*time.Second {
		t.Fatalf("openTimeout()=%s, want 40s", got)
	}
	for _, bad := range []string{"nonsense", "0s", "-5s"} {
		t.Setenv("SLATE_DB_OPEN_TIMEOUT", bad)
		if got := openTimeout(); got != defaultDbOpenTimeout {
			t.Fatalf("openTimeout() for %q = %s, want default %s", bad, got, defaultDbOpenTimeout)
		}
	}
}
