package cluster

// White-box tests for the R>1 WRITE terminal's refusal reason.
//
// At R>1 a write is not one refusal, it is a FAN-OUT that gets collapsed into a
// single error by classifyWriteAttempt. That collapse is where a leg's reason
// would otherwise be lost: every leg value is dropped and a fresh status is
// minted. These pin that the reason of the underlying legs survives the
// collapse, that it survives for a REMOTE acquiring leg as well as a local one,
// and - just as load-bearing - that it is NOT claimed when the legs did not
// present it.

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A local (in-process) acquiring leg must carry its reason into the collapsed
// terminal. This is the R>1 half of the path-independence contract: a consumer
// running R=2 gets the same errors.Is match a consumer running R=1 gets.
func TestAcquiringReason_WriteShortfallFromLocalLegCarriesReason(t *testing.T) {
	got := classifyWriteAttempt(1, 2, nil, []error{errUnitAcquiring("Put")})

	if got.err == nil || !got.retryable {
		t.Fatalf("a pure acquiring shortfall must be a retryable error, got %+v", got)
	}
	if !errors.Is(got.err, ErrAcquiring) {
		t.Fatalf("R>1 write terminal lost the legs' acquiring reason: %v (%T)", got.err, got.err)
	}
	if code := status.Code(got.err); code != codes.Unavailable {
		t.Fatalf("client-facing code moved: got %v want Unavailable", code)
	}
	// The WIRE half: this terminal is minted on whichever node the write
	// entered, which for a forwarding consumer is a PEER. The detail has to
	// ride along or the identity dies at the node boundary.
	if r, ok := reasonOf(got.err); !ok || r != ReasonAcquiring {
		t.Fatalf("terminal carries no wire reason detail: reason=%q ok=%v", r, ok)
	}
}

// The same, for an acquiring leg that arrived from a PEER. A forwarded replica
// leg is re-coded to ResourceExhausted on the way out and decoded by the peer
// client on the way in, so this reconstructs exactly the value the fan-out sees
// and asserts the terminal still names the reason. Without the detail on the
// re-code, a remote mid-acquire replica is indistinguishable from any other
// transient and the terminal could not honestly report it.
func TestAcquiringReason_WriteShortfallFromForwardedLegCarriesReason(t *testing.T) {
	// The exact round trip: recode on the replica, decode at the originator.
	leg := decodeRefusalReason(recodeForwardedReplicaErr(errUnitAcquiring("Put")))
	if !errors.Is(leg, ErrAcquiring) {
		t.Fatalf("forwarded acquiring leg did not decode to ErrAcquiring: %v (%T)", leg, leg)
	}

	got := classifyWriteAttempt(1, 2, nil, []error{leg})
	if !errors.Is(got.err, ErrAcquiring) {
		t.Fatalf("R>1 write terminal lost a REMOTE leg's acquiring reason: %v (%T)", got.err, got.err)
	}
	if code := status.Code(got.err); code != codes.Unavailable {
		t.Fatalf("client-facing code moved: got %v want Unavailable", code)
	}
}

// EVIDENCE, NOT BRANCH. The retryable branch is also reached when the only
// transients were v0.3 migration guards, which are a DIFFERENT reason. The
// terminal must not claim acquiring there, or the sentinel degrades into
// "some transient happened" and stops being worth gating on.
func TestAcquiringReason_WriteShortfallWithoutAcquiringLegsHasNoReason(t *testing.T) {
	guard := status.Error(codes.ResourceExhausted, "cluster: unit is receiving a handoff")

	got := classifyWriteAttempt(1, 2, nil, []error{guard})
	if got.err == nil || !got.retryable {
		t.Fatalf("a transient shortfall is still retryable, got %+v", got)
	}
	if errors.Is(got.err, ErrAcquiring) {
		t.Fatalf("terminal claimed the acquiring reason with no acquiring leg: %v", got.err)
	}
	if code := status.Code(got.err); code != codes.Unavailable {
		t.Fatalf("client-facing code moved: got %v want Unavailable", code)
	}
}

// THE MIXED CASE, pinned deliberately. Some legs were mid-acquire, others were
// genuinely down. This reports as a HARD failure and does NOT match ErrAcquiring.
//
// ErrAcquiring promises a window bounded by a MOUNT rather than an outage, so a
// bounded retry is guaranteed to observe it end. Once a real failure is in the
// mix that promise is false: the wait is bounded by whatever revives the peer.
// Matching here would send a consumer into a bounded retry against a genuine
// outage - the false positive the whole mechanism exists to prevent - and would
// contradict shale's own judgment, since this is precisely the branch that sets
// retryable=false and refuses to retry internally.
func TestAcquiringReason_MixedAcquiringAndPeerDownIsNotAcquiring(t *testing.T) {
	peerDown := status.Error(codes.Unavailable, "connection refused")

	got := classifyWriteAttempt(1, 3, []error{peerDown}, []error{errUnitAcquiring("Put")})

	if got.err == nil || got.retryable {
		t.Fatalf("a shortfall with a real failure must be HARD (not retryable), got %+v", got)
	}
	if errors.Is(got.err, ErrAcquiring) {
		t.Fatalf("a terminal with a genuinely-down leg matched ErrAcquiring; "+
			"a consumer would retry a real outage as a handoff blip: %v", got.err)
	}
	if _, ok := reasonOf(got.err); ok {
		t.Fatalf("a hard terminal must not carry a reason detail either: %v", got.err)
	}
	if code := status.Code(got.err); code != codes.Unavailable {
		t.Fatalf("client-facing code moved: got %v want Unavailable", code)
	}
}

// The exported sentinel must NOT drag the PRIVATE classifier tag along. The
// private tag means "a LOCAL in-process mid-acquire replica" to the fan-out and
// read-leg classifiers; if the synthesized terminal started matching it, budget
// classification would move silently. This is the invariant that keeps the
// consumer-facing identity and the internal accounting separate.
func TestAcquiringReason_WriteTerminalStaysOutOfLocalClassifiers(t *testing.T) {
	got := classifyWriteAttempt(1, 2, nil, []error{errUnitAcquiring("Put")})

	if isAcquiringErr(got.err) {
		t.Fatalf("the synthesized terminal widened the PRIVATE acquiring tag; "+
			"fan-out and read-leg budgets would shift: %v", got.err)
	}
	if isTransientReplicaErr(got.err) {
		t.Fatalf("the synthesized terminal became a fan-out transient: %v", got.err)
	}
}

// The re-code keeps its BUDGET class exactly. Only the identity is additive:
// the code the fan-out reads is still ResourceExhausted, so a forwarded
// mid-acquire replica still consumes neither the ack nor the failure budget
// (the Phase 2d fix), and it still does not read as an unreachable leg.
func TestAcquiringReason_ForwardedRecodeKeepsBudgetClass(t *testing.T) {
	recoded := recodeForwardedReplicaErr(errUnitAcquiring("Put"))

	if code := status.Code(recoded); code != codes.ResourceExhausted {
		t.Fatalf("re-code changed the fan-out's budget code: got %v want ResourceExhausted", code)
	}
	if !isTransientReplicaErr(recoded) {
		t.Fatalf("re-coded acquiring leg stopped being a fan-out transient: %v", recoded)
	}
	if isUnreachableLegErr(recoded) {
		t.Fatalf("re-coded acquiring leg read as an unreachable leg: %v", recoded)
	}
	// A non-acquiring error still passes through by identity.
	other := status.Error(codes.Unavailable, "peer down")
	if got := recodeForwardedReplicaErr(other); got != other {
		t.Fatalf("re-code no longer passes non-acquiring errors through verbatim: %v", got)
	}

	// And once DECODED at the originator, it grants the exported sentinel only.
	decoded := decodeRefusalReason(recoded)
	if !errors.Is(decoded, ErrAcquiring) {
		t.Fatalf("decoded re-code lost the exported sentinel: %v", decoded)
	}
	if isAcquiringErr(decoded) {
		t.Fatalf("decoded re-code widened the PRIVATE acquiring tag: %v", decoded)
	}
	if !isTransientReplicaErr(decoded) {
		t.Fatalf("decoded re-code stopped being a fan-out transient: %v", decoded)
	}
}

// The zero-evidence timeout terminal (the budget was already spent before any
// attempt completed) must NOT claim the reason. Its message names acquiring,
// but no leg ever reported, and the sentinel is only ever attached to evidence.
// A non-positive WriteTimeout would otherwise mint a false acquiring signal on
// every write.
func TestAcquiringReason_ZeroEvidenceTimeoutHasNoReason(t *testing.T) {
	c := &Cluster{}
	c.cfg.WriteTimeout = -1 // budget already exhausted on entry

	err := c.retryWriteThroughHandoff(func(_ context.Context) writeAttempt {
		t.Fatal("no attempt should run on an already-exhausted budget")
		return writeAttempt{}
	})
	if err == nil {
		t.Fatal("an exhausted budget must still return an error")
	}
	if errors.Is(err, ErrAcquiring) {
		t.Fatalf("an unattributed timeout claimed the acquiring reason: %v", err)
	}
	if code := status.Code(err); code != codes.Unavailable {
		t.Fatalf("timeout terminal code: got %v want Unavailable", code)
	}
}
