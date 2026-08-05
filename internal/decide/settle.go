package decide

import "time"

// SettleState is the settle timer as the arming goroutine observes it, under
// the lock that guards the timer.
type SettleState struct {
	// Armed reports that a timer reference is held.
	Armed bool
	// Immediate reports that the armed timer is a ZERO-DELAY arm. Those exist
	// to close a consumer-visible unavailability window NOW (the boot-defer
	// prompt, the stale-mount evict), not to debounce churn.
	Immediate bool
	// Fired reports that the armed timer has already run: its callback is in
	// flight and owns - and will release - the pending obligation that timer
	// was armed with. Meaningless unless Armed.
	Fired bool
}

// SettleArmDecision is the action an arm resolves to.
type SettleArmDecision struct {
	// Delay is the delay to arm at, which is NOT always the requested one: a
	// debounced request over a pending immediate arm is admitted at zero.
	Delay time.Duration
	// Immediate is the flag the newly armed timer carries, so a later arm
	// reads it as SettleState.Immediate.
	Immediate bool
	// NewObligation reports that this arm owns a FRESH pending obligation, so
	// the caller increments its pending counter. False means the arm INHERITS
	// the obligation of the timer it displaces.
	NewObligation bool
}

// SettleArm decides how an incoming arm resolves against the pending one.
//
// There is no "keep the pending timer" outcome. Learning SettleState.Fired
// costs a Stop probe, and that probe CONSUMES a live timer, so by the time the
// decision is made the pending timer no longer exists: every admitted arm
// installs a replacement. Rule (a) is what keeping means here - the immediate
// arm is preserved as an EFFECT (delay zero), not as a timer object.
//
// Two rules carry the machine:
//
// (a) A DEBOUNCED arm must not POSTPONE a pending IMMEDIATE one. An immediate
// arm exists to close a consumer-visible unavailability window now;
// last-writer-wins pushes it out a full settle delay, turning "arm the pass
// immediately" into a multi-second refusal window that outlives consumer retry
// budgets. The debounced obligation is subsumed rather than dropped: the
// immediate pass IS the same single pass, sooner.
//
// (b) An arm over a FIRED timer owns a FRESH obligation. The fired timer's
// callback is in flight and will release the obligation it was armed with, so
// an arm that rode it would have two releases against one increment: the
// pending counter goes NEGATIVE and every later idle-wait blocks forever on
// "== 0", which is unsatisfiable from below. The obligation is genuinely new
// anyway - the in-flight pass may have snapshotted state from before the
// change that prompted this arm.
//
// A LIVE timer is the opposite case: displacing it means its callback never
// runs, so the replacement inherits its obligation and must not double-count.
func SettleArm(cur SettleState, requested time.Duration) SettleArmDecision {
	if cur.Armed && !cur.Fired {
		if cur.Immediate && requested > 0 {
			return SettleArmDecision{Delay: 0, Immediate: true} // (a)
		}
		// A zero-delay arm wins over a pending debounced one (same pass,
		// sooner); debounced over debounced replaces too, which is what makes
		// the debounce extend to one delay after the LAST change.
		return SettleArmDecision{Delay: requested, Immediate: requested == 0}
	}
	return SettleArmDecision{Delay: requested, Immediate: requested == 0, NewObligation: true} // (b), and the fresh arm
}
