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
	"fmt"
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

// classifyWriteAttempt turns a fan-out's (acks, w, errs, transient) into a
// writeAttempt. fanout/isTransientReplicaErr already filters transient
// (acquiring / migration guard) outcomes OUT of errs and into transient, so:
//   - acks >= w                -> success (err nil).
//   - acks < w, len(errs) == 0 -> W missed ONLY because of acquiring/transient
//     replicas (none counted as failure): RETRYABLE handoff blip.
//   - acks < w, len(errs) > 0  -> real non-transient failures exhausted the
//     budget: HARD, returned immediately (not retryable).
//
// REASON IDENTITY THROUGH THE COLLAPSE. This is where R>1 stops being a fan-out
// and becomes ONE error the caller sees, so it is where a leg's refusal reason
// would otherwise be lost: the legs are summarized into a fresh status and every
// leg value is dropped. A consumer gating on errors.Is(err, ErrAcquiring) would
// therefore get a FALSE NEGATIVE on exactly the config (R>1) where the handoff
// window is most visible. The retryable terminal is minted through reasonTerminal
// so the reason of the underlying legs SURVIVES the collapse, on both halves of
// the contract (in-process sentinel + wire detail, see reason.go).
//
// The reason is taken from EVIDENCE (legsCarryReason over the actual transient
// legs), never from the branch: the retryable branch is also reachable with only
// migration-guard transients, which are a DIFFERENT reason and must not be
// reported as acquiring. When the legs carry no known reason the terminal is the
// same plain status it has always been.
//
// THE MIXED CASE (some legs acquiring, others genuinely down) reports as a HARD
// failure and deliberately does NOT carry the acquiring reason. ErrAcquiring's
// documented contract is that the window is bounded by a mount rather than an
// outage, so a bounded retry is guaranteed to observe it end; once a real failure
// is in the mix that promise is false and the wait is bounded by whatever revives
// the down peer. Claiming the reason here would send a consumer into a bounded
// retry against a genuine outage - the false positive this whole mechanism exists
// to prevent - and would also contradict shale's own judgment, since this branch
// is precisely the one that sets retryable=false and refuses to retry internally.
func classifyWriteAttempt(acks, w int, errs, transient []error) writeAttempt {
	if acks >= w {
		return writeAttempt{}
	}
	if len(errs) == 0 {
		msg := fmt.Sprintf(
			"shale: write needed %d acks, got %d (replicas mid-acquire)", w, acks)
		if legsCarryReason(transient, ReasonAcquiring) {
			return writeAttempt{
				err:       reasonTerminal(codes.Unavailable, ReasonAcquiring, msg),
				retryable: true,
			}
		}
		return writeAttempt{
			err:       status.Error(codes.Unavailable, msg),
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
			//
			// lastErr is the terminal that carries the legs' refusal reason
			// (classifyWriteAttempt), so returning it verbatim is what makes the
			// identity survive the RETRY as well as the fan-out collapse: an
			// exhausted budget reports the same reason the attempts did.
			if lastErr != nil {
				return lastErr
			}
			// No attempt ever completed (the budget was already spent on entry),
			// so there is NO leg evidence. This deliberately carries no reason:
			// the message names acquiring, but an unattributed timeout - a
			// non-positive WriteTimeout, say - is not evidence of a handoff, and
			// the sentinel must only ever be attached to something a leg actually
			// presented.
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
	var unreachableRunStart time.Time

	for {
		v, err := attempt(deadline)
		if err == nil {
			return v, nil
		}
		switch {
		case isAcquiringErr(err):
			// Handoff-class evidence: full-budget re-poll; reset the
			// unreachable-only run (a live transition is in progress).
			unreachableRunStart = time.Time{}
		case isUnreachableOnly(err):
			// Unreachable-only sweep: weaker evidence, TIME-BOUNDED re-poll
			// (docs/SPEC.md guard 2). A just-departed member's address lingers
			// in the routed union until the ring rebuilds, so unreachable-only
			// sweeps inside that lag are EXPECTED; keep re-polling for the
			// unreachable grace, then surface the dial error verbatim - a
			// genuine outage costs ~the grace, never the full budget.
			if unreachableRunStart.IsZero() {
				unreachableRunStart = time.Now()
			}
			if time.Since(unreachableRunStart) >= unreachableOnlyGrace {
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

// unreachableOnlyGrace bounds how long the read retry re-polls consecutive
// unreachable-only sweeps before surfacing the first dial error verbatim. It
// must OUTLAST the ring-lag window (a departed member's address lingering in
// the routed union until the membership update propagates, the ring rebuilds,
// and the reconcile settles - broadcast plus settle in a graceful leave, the
// suspicion timeout in a crash), or mid-rollout reads surface spurious dial
// errors; 2s covers those with headroom while keeping a genuine outage's
// surface time well under the default 5s read budget. Always additionally
// bounded by ReadTimeout (a shorter budget wins).
const unreachableOnlyGrace = 2 * time.Second

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
