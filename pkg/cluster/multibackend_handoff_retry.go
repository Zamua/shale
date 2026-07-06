// Write-availability through an R>1 multi-backend membership change
// (v0.8 Phase 2d, "Option A: retry-on-acquiring").
//
// During an R>1 membership change reconcileReplicaUnitsOverlap re-mounts units whose
// replica assignment moved. Until a re-mount finishes, a routed op for that
// replica returns the acquiring-window refusal (errUnitAcquiring). Layer 1
// (the errAcquiringSentinel tag + recodeForwardedReplicaErr) already makes a
// SINGLE mid-acquire replica a fan-out transient that consumes neither the ack
// nor the failure budget. This file is LAYER 2: when ENOUGH of a unit's
// replicas are simultaneously mid-acquire that W acks are not reachable in one
// fan-out pass, the whole op is RE-RUN after a backoff, bounded by the SAME
// WriteTimeout wall-clock budget, so the handoff blip becomes bounded LATENCY
// instead of an error.
//
// INVARIANTS PRESERVED (see docs/SPEC.md "v0.8 Phase 2d"):
//   - NO ACKED WRITE LOST: the retry changes only WHEN a write is acked, never
//     WHETHER an acked write survives. A write is still acked only after W of R
//     replicas durably applied the SAME pre-stamped envelope; the retry just
//     gives the fan-out more time to collect those W acks across the handoff
//     window. The caller stamps ONCE and reuses the envelope across attempts,
//     so apply-if-newer makes a re-applied identical envelope a no-op.
//   - SINGLE-WRITER FENCE intact: the retry only re-dispatches a write that was
//     REFUSED (the acquiring replica explicitly did NOT apply it). It never
//     relaxes the epoch fence and never dispatches the same write to two live
//     holders of one (unit, replica) database.
//   - REAL OUTAGES STILL FAIL FAST: a retry fires ONLY for the acquiring /
//     quorum-unavailable family (W missed purely because replicas were mid-
//     acquire). A hard failure - genuinely-down peers exhausting the budget,
//     an empty-value rejection, a closed cluster, a decode error - is returned
//     IMMEDIATELY, unretried, so the retry never papers over a real outage by
//     spinning until WriteTimeout.

package cluster

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// writeAttempt is the structured outcome of ONE fan-out pass of a replicated
// write, so the retry wrapper can distinguish "W not met because replicas were
// mid-acquire" (retryable) from "W not met because replicas are down" (hard).
type writeAttempt struct {
	// err is nil when the attempt satisfied W. Non-nil means the attempt did
	// not reach W; retryable says whether the wrapper should re-run it.
	err error
	// retryable is true only when the shortfall was caused purely by acquiring
	// (transient) replicas - i.e. NO non-transient failure landed, W was simply
	// not reachable yet. It is meaningless when err is nil.
	retryable bool
}

// classifyWriteAttempt turns a fan-out's (acks, w, errs) into a writeAttempt.
// fanout/isTransientReplicaErr already filters transient (acquiring / migration
// guard) outcomes OUT of errs, so:
//   - acks >= w                -> success (err nil).
//   - acks < w, len(errs) == 0 -> W missed ONLY because of acquiring/transient
//     replicas (none counted as failure): RETRYABLE handoff blip.
//   - acks < w, len(errs) > 0  -> real non-transient failures exhausted the
//     budget: HARD, returned immediately (not retryable).
func classifyWriteAttempt(acks, w int, errs []error) writeAttempt {
	if acks >= w {
		return writeAttempt{}
	}
	if len(errs) == 0 {
		return writeAttempt{
			err: status.Errorf(codes.Unavailable,
				"shale: write needed %d acks, got %d (replicas mid-acquire)", w, acks),
			retryable: true,
		}
	}
	return writeAttempt{
		err: status.Errorf(codes.Unavailable,
			"shale: write needed %d acks, got %d (%d failures: %v)",
			w, acks, len(errs), firstErr(errs)),
		retryable: false,
	}
}

// Layer-2 retry policy knobs. The backoff reuses the existing RebalanceRetryAfterMs
// handoff retry-after hint as its base; the entire sequence is bounded by the
// caller's WriteTimeout wall clock (NOT a separate retry-count cap), so a fast-
// acquiring cluster retries a few times and a pathologically-slow one degrades
// to one timeout-bounded error per write (never a busy-spin, never a deadlock).
const handoffRetryBackoffCap = 500 * time.Millisecond

// retryWriteThroughHandoff runs attempt repeatedly until it succeeds, hits a
// non-retryable outcome, or the WriteTimeout wall-clock budget is exhausted.
//
// Each attempt is given a context whose deadline is min(now+remaining-budget).
// The FIRST attempt starts the clock. Between attempts the wrapper sleeps a
// jittered exponential backoff (base RebalanceRetryAfterMs, x2, capped at
// handoffRetryBackoffCap), checking the remaining budget BEFORE sleeping so it
// never sleeps past the deadline. When the budget is exhausted it surfaces the
// last retryable error (a timeout-bounded Unavailable the client retries),
// exactly as a single-shot write would on a slow cluster.
func (c *Cluster) retryWriteThroughHandoff(attempt func(ctx context.Context) writeAttempt) error {
	if c.cfg.TestingForceCleanCut {
		// Break-demo only (docs/SPEC.md "v0.8 Phase 2e" gate): disable the
		// Option-A retry so it cannot mask the clean-cut gap. ONE attempt; a
		// retryable acquiring shortfall surfaces immediately as an error, so the
		// ack rate reflects the clean-cut availability with nothing absorbing the
		// slow mount.
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.WriteTimeout)
		defer cancel()
		return attempt(ctx).err
	}
	deadline := time.Now().Add(c.cfg.WriteTimeout)
	base := time.Duration(c.retryAfterMs()) * time.Millisecond
	backoff := base

	var lastErr error
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			// Budget exhausted; surface the last retryable error (or a generic
			// timeout if we never got one, which should not happen since we only
			// loop on a retryable outcome).
			if lastErr != nil {
				return lastErr
			}
			return status.Error(codes.Unavailable,
				"shale: write timed out waiting for replicas to finish acquiring")
		}

		attemptCtx, cancel := context.WithDeadline(context.Background(), deadline)
		res := attempt(attemptCtx)
		cancel()

		if res.err == nil {
			return nil
		}
		if !res.retryable {
			// Hard failure (down peers, empty value, decode error, closed): do
			// not paper over a real outage by spinning. Return immediately.
			return res.err
		}
		lastErr = res.err

		// Retryable handoff blip: back off (bounded by the remaining budget),
		// then re-run. Re-check the budget so we never sleep past the deadline.
		remaining = time.Until(deadline)
		if remaining <= 0 {
			return lastErr
		}
		sleep := jitteredBackoff(backoff)
		if sleep > remaining {
			sleep = remaining
		}
		time.Sleep(sleep)

		backoff *= 2
		if backoff > handoffRetryBackoffCap {
			backoff = handoffRetryBackoffCap
		}
	}
}

// jitteredBackoff returns d scaled by a uniform factor in [0.5, 1.0), so a
// thundering herd of simultaneously-acquiring writes does not re-collide on the
// same retry tick. Matches the v0.3 cutover / freeze-window retry shape.
func jitteredBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	// 50%..100% of d.
	factor := 0.5 + rand.Float64()*0.5
	return time.Duration(float64(d) * factor)
}

// retryReadThroughHandoff is the READ-side mirror of retryWriteThroughHandoff
// (docs/SPEC.md "Union reads" guard 2 / "Union scans"): it re-runs attempt
// while the outcome is the ALL-LEGS-TRANSIENT acquiring error (every union leg
// mid-acquire or fenced-recode - the sub-second fence-at-open-start blip),
// bounded by the ReadTimeout wall clock, with the same jittered exponential
// backoff the write retry uses. Any other outcome - success, not-found, a
// non-transient leg failure - is returned IMMEDIATELY, unretried, so a real
// outage still fails fast; only the budget expiring mid-blip surfaces the
// retryable acquiring error. attempt receives the shared wall-clock deadline
// so per-attempt fan-out contexts never outlive the overall budget, and each
// attempt re-resolves the routed union from the live ring. Under
// TestingForceCleanCut (the break-demo) the retry is disabled exactly as the
// write retry is, so it cannot mask the clean-cut gap the demo measures.
func retryReadThroughHandoff[T any](c *Cluster, attempt func(deadline time.Time) (T, error)) (T, error) {
	deadline := time.Now().Add(c.cfg.ReadTimeout)
	if c.cfg.TestingForceCleanCut {
		v, err := attempt(deadline)
		return v, unwrapUnreachableOnly(err)
	}
	backoff := time.Duration(c.retryAfterMs()) * time.Millisecond
	unreachableSweeps := 0

	for {
		v, err := attempt(deadline)
		if err == nil {
			return v, nil
		}
		switch {
		case isAcquiringErr(err):
			// Handoff-class evidence: full-budget re-poll; reset the
			// unreachable-only cap (a live transition is in progress).
			unreachableSweeps = 0
		case isUnreachableOnly(err):
			// Unreachable-only sweep: weaker evidence, CAPPED re-poll
			// (docs/SPEC.md guard 2). Surface the dial error verbatim once the
			// cap is hit, so a genuine outage costs a sub-second re-poll, not
			// the full budget.
			unreachableSweeps++
			if unreachableSweeps >= unreachableOnlySweepCap {
				return v, unwrapUnreachableOnly(err)
			}
		default:
			return v, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return v, unwrapUnreachableOnly(err)
		}
		sleep := jitteredBackoff(backoff)
		if sleep > remaining {
			sleep = remaining
		}
		time.Sleep(sleep)

		backoff *= 2
		if backoff > handoffRetryBackoffCap {
			backoff = handoffRetryBackoffCap
		}
	}
}

// unreachableOnlySweepCap bounds how many unreachable-only sweeps the read
// retry re-polls before surfacing the first dial error verbatim: the second
// such sweep (one backoff apart, still nothing but dead legs) is the give-up
// point - a genuine outage surfaces in well under a second.
const unreachableOnlySweepCap = 2

// isUnreachableOnly reports whether err is the unreachable-only sweep marker.
func isUnreachableOnly(err error) bool {
	var uo *unreachableOnlyError
	return errors.As(err, &uo)
}

// unwrapUnreachableOnly strips the internal unreachable-only marker so callers
// only ever see the underlying dial error; any other error passes through.
func unwrapUnreachableOnly(err error) error {
	var uo *unreachableOnlyError
	if errors.As(err, &uo) {
		return uo.inner
	}
	return err
}
