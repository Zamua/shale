package cluster

import (
	"testing"
	"time"
)

// The shared retry wait is the single copy of a guard that used to be re-typed
// at every retry site: check the budget BEFORE sleeping, and never sleep past
// it. These pin the three schedule shapes the ported sites rely on.

// A budgeted schedule doubles up to its cap and then STAYS there.
func TestBudgetRetryWaitDoublesAndClampsAtCap(t *testing.T) {
	w := newBudgetRetryWait(1*time.Millisecond, 4*time.Millisecond, time.Now().Add(time.Minute))
	want := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		4 * time.Millisecond,
		4 * time.Millisecond,
		4 * time.Millisecond,
	}
	for i, d := range want {
		if got := w.interval(); got != d {
			t.Fatalf("interval before wait %d = %v, want %v", i, got, d)
		}
		if res := w.wait(nil); res != retryWaitProceed {
			t.Fatalf("wait %d = %v, want proceed", i, res)
		}
	}
}

// A spent budget reports exhausted WITHOUT sleeping, so a caller can surface its
// terminal error immediately.
func TestBudgetRetryWaitExhaustedDoesNotSleep(t *testing.T) {
	w := newBudgetRetryWait(time.Second, time.Second, time.Now().Add(-time.Millisecond))
	if !w.exhausted() {
		t.Fatal("exhausted() = false on an elapsed deadline, want true")
	}
	start := time.Now()
	if res := w.wait(nil); res != retryWaitExhausted {
		t.Fatalf("wait = %v, want exhausted", res)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("wait on an exhausted budget slept %v, want no sleep", elapsed)
	}
}

// The budget CLAMPS the sleep: a 10s interval against a ~20ms remaining budget
// must not sleep for 10s. This is the guard the write and read handoff retries
// both hand-rolled.
func TestBudgetRetryWaitClampsSleepToRemainingBudget(t *testing.T) {
	w := newBudgetRetryWait(10*time.Second, 10*time.Second, time.Now().Add(20*time.Millisecond))
	start := time.Now()
	if res := w.wait(nil); res != retryWaitProceed {
		t.Fatalf("wait = %v, want proceed", res)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("wait slept %v past a 20ms budget, want it clamped", elapsed)
	}
	if !w.exhausted() {
		t.Fatal("exhausted() = false after the budget elapsed, want true")
	}
}

// A capped schedule has NO wall clock: it ends once the next interval would
// exceed the cap. base=1ms cap=8ms yields exactly 4 waits (1, 2, 4, 8).
func TestCappedRetryWaitEndsOnceIntervalExceedsCap(t *testing.T) {
	w := newCappedRetryWait(1*time.Millisecond, 8*time.Millisecond)
	waits := 0
	for !w.exhausted() {
		if res := w.wait(nil); res != retryWaitProceed {
			t.Fatalf("wait %d = %v, want proceed", waits, res)
		}
		waits++
		if waits > 10 {
			t.Fatal("capped schedule did not terminate")
		}
	}
	if waits != 4 {
		t.Fatalf("capped schedule ran %d waits, want 4 (1, 2, 4, 8ms)", waits)
	}
}

// A constant schedule never grows and never ends on its own; its caller owns the
// budget check.
func TestConstantRetryWaitIsFixedAndNeverExhausts(t *testing.T) {
	w := newConstantRetryWait(time.Millisecond)
	for i := 0; i < 5; i++ {
		if w.exhausted() {
			t.Fatalf("constant schedule exhausted at wait %d, want never", i)
		}
		if got := w.interval(); got != time.Millisecond {
			t.Fatalf("interval at wait %d = %v, want 1ms", i, got)
		}
		if res := w.wait(nil); res != retryWaitProceed {
			t.Fatalf("wait %d = %v, want proceed", i, res)
		}
	}
}

// A cancel that fires mid-sleep ends the wait promptly and does NOT advance the
// schedule (it is abandoned, not continued).
func TestRetryWaitCancelStopsTheSleep(t *testing.T) {
	w := newCappedRetryWait(10*time.Second, 10*time.Second)
	cancel := make(chan struct{})
	close(cancel)
	start := time.Now()
	if res := w.wait(cancel); res != retryWaitCanceled {
		t.Fatalf("wait = %v, want canceled", res)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("canceled wait took %v, want it to return promptly", elapsed)
	}
	if got := w.interval(); got != 10*time.Second {
		t.Fatalf("interval after a canceled wait = %v, want it unadvanced at 10s", got)
	}
}
