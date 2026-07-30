// The single inter-attempt WAIT shared by shale's hand-rolled retry loops.
//
// Several loops in this package re-run an operation while it keeps reporting a
// transient outcome: the write / read handoff retries (Layer 2 of the
// acquiring-window absorption), the overlap acquire re-drive, and the reshard
// RESUME retry. They differ in what they attempt and in how they classify an
// attempt - that part is domain logic and stays at each site. What they had in
// common, and had COPIED, is the wait between attempts: an exponential sequence
// with a cap, optional jitter, and (for the budgeted ones) the rule that the
// remaining wall clock is checked BEFORE sleeping and clamps the sleep so a
// retry can never sleep past the budget it is bounded by.
//
// That last rule is the correctness-sensitive part and the reason this is
// single-sourced rather than left duplicated: it is a guard, and a guard that
// has to be re-typed at every site is a guard that will eventually be absent
// from one of them.
//
// WHY NOT A LIBRARY (cenkalti/backoff and friends). These loops check the
// budget BEFORE sleeping against a wall clock SHARED with the per-attempt
// context deadline, and they interleave domain classification (is this shortfall
// an acquiring blip or a real outage? did every union leg come back unreachable?
// which nodes have not acked yet?) between attempts. A library that owns the
// loop cannot express either without the callback contortions that would cost
// more clarity than the ~10 lines it saves.
//
// TERMINATION comes in two shapes, one per constructor, and each site keeps the
// exact one it had:
//   - BUDGETED (newBudgetRetryWait): a shared wall-clock deadline ends the
//     schedule; the cap CLAMPS the growth of the interval. Used by the write and
//     read handoff retries, whose budgets are WriteTimeout / ReadTimeout.
//   - CAPPED (newCappedRetryWait): there is no wall clock; the schedule ends
//     once the next interval would exceed the cap. Used by the overlap acquire
//     re-drive, which hands the retry back to the periodic reconcile at that
//     point rather than to a caller.
//
// A third constructor (newConstantRetryWait) is a degenerate schedule: a fixed,
// un-jittered interval that never ends on its own, for a caller that owns its
// own budget check. See its doc for why the reshard RESUME retry uses it rather
// than the budgeted shape.

package cluster

import (
	"math/rand"
	"time"
)

// retryWaitResult is why a wait returned, so a caller can tell the three cases
// apart and keep its own terminal error / log line for each.
type retryWaitResult int

const (
	// retryWaitProceed: the wait slept its interval; attempt again.
	retryWaitProceed retryWaitResult = iota
	// retryWaitExhausted: the schedule is spent (budget elapsed, or the next
	// interval would exceed the cap). NO sleep happened.
	retryWaitExhausted
	// retryWaitCanceled: the cancel channel fired mid-sleep. Only reachable
	// when the caller passes a non-nil cancel channel.
	retryWaitCanceled
)

// retryWait is one retry loop's backoff schedule. It is NOT safe for concurrent
// use; each loop owns its own.
type retryWait struct {
	// next is the un-jittered interval the next wait will sleep. It doubles
	// after every slept interval.
	next time.Duration
	// max bounds next. Its meaning depends on the termination shape: a budgeted
	// schedule CLAMPS next at max, a capped schedule ENDS once next exceeds max.
	max time.Duration
	// deadline is the shared wall clock a budgeted schedule is bounded by. Zero
	// for the capped and constant shapes, which have no wall clock.
	deadline time.Time
	// capTerminates selects the capped shape (see max).
	capTerminates bool
	// jitter applies jitteredBackoff to each interval. Off only for the
	// constant shape, whose single caller has no herd to de-synchronize.
	jitter bool
}

// newBudgetRetryWait returns the schedule the handoff retries use: intervals
// start at base and double up to max, each jittered, and the whole sequence is
// bounded by deadline - which both ends the schedule and clamps any sleep that
// would otherwise run past it.
func newBudgetRetryWait(base, maxInterval time.Duration, deadline time.Time) *retryWait {
	return &retryWait{next: base, max: maxInterval, deadline: deadline, jitter: true}
}

// newCappedRetryWait returns the schedule the overlap acquire re-drive uses:
// jittered intervals starting at base and doubling with NO clamp, ending once
// the next interval would exceed max. There is no wall clock: the bound is the
// number of doublings between base and max.
func newCappedRetryWait(base, maxInterval time.Duration) *retryWait {
	return &retryWait{next: base, max: maxInterval, capTerminates: true, jitter: true}
}

// newConstantRetryWait returns a fixed, UN-jittered interval that never ends on
// its own: exhausted always reports false, so the caller's own budget check is
// the only terminator.
//
// This is the reshard RESUME retry's shape, and it is deliberately NOT the
// budgeted one. Two of its properties differ in kind from the handoff retries:
// it does not jitter (a reshard has a single coordinator, so there is no herd of
// retriers to de-synchronize), and its wall-clock check is the caller's, placed
// where it can return the right per-node error. Folding it into the budgeted
// shape would have silently added jitter and clamped its final sleep, changing
// how many RESUME rounds a coordinator issues near the end of its budget. Both
// behaviours are preserved as they were.
func newConstantRetryWait(d time.Duration) *retryWait {
	return &retryWait{next: d, max: d}
}

// interval reports the un-jittered interval the next wait will sleep, for the
// callers that log their upcoming backoff.
func (w *retryWait) interval() time.Duration { return w.next }

// exhausted reports whether the schedule is spent, WITHOUT sleeping. wait
// re-checks this itself; it is exported within the package for callers that
// want to emit a distinct log line or terminal error before giving up.
func (w *retryWait) exhausted() bool {
	if w.capTerminates {
		return w.next > w.max
	}
	if w.deadline.IsZero() {
		return false
	}
	return time.Until(w.deadline) <= 0
}

// wait sleeps the next interval and reports why it returned. A budgeted
// schedule clamps the sleep to the remaining budget, so it can never sleep past
// its deadline. cancel may be nil for an uncancellable sleep.
func (w *retryWait) wait(cancel <-chan struct{}) retryWaitResult {
	if w.exhausted() {
		return retryWaitExhausted
	}
	sleep := w.next
	if w.jitter {
		sleep = jitteredBackoff(sleep)
	}
	if !w.deadline.IsZero() {
		if remaining := time.Until(w.deadline); sleep > remaining {
			sleep = remaining
		}
	}
	if cancel == nil {
		time.Sleep(sleep)
		w.advance()
		return retryWaitProceed
	}
	t := time.NewTimer(sleep)
	defer t.Stop()
	select {
	case <-cancel:
		// Do NOT advance: the schedule is abandoned, not continued.
		return retryWaitCanceled
	case <-t.C:
		w.advance()
		return retryWaitProceed
	}
}

// advance doubles the interval, clamping at max unless the cap is the
// schedule's terminator (in which case exceeding it is precisely the signal).
func (w *retryWait) advance() {
	w.next *= 2
	if !w.capTerminates && w.next > w.max {
		w.next = w.max
	}
}

// jitteredBackoff returns d scaled by a uniform factor in [0.5, 1.0), so a
// thundering herd of simultaneously-retrying callers does not re-collide on the
// same retry tick. Matches the v0.3 cutover / acquiring-window retry shape.
func jitteredBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	// 50%..100% of d.
	factor := 0.5 + rand.Float64()*0.5
	return time.Duration(float64(d) * factor)
}
