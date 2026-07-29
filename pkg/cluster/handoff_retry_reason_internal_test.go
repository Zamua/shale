package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The read retry loop used to discard the one thing only it knew. As the budget
// runs out, the final attempt's context expires and its legs report a deadline;
// that hard error was returned in place of the acquiring evidence the loop had
// already gathered, so a caller could not tell a unit that was MOVING from a
// read that was merely SLOW.
//
// These pin the discrimination. The guard is deliberately narrow: evidence of an
// actual handoff, plus a terminal that is specifically a deadline. Widening
// beyond that would make every slow read look retryable, which is the retry
// storm a consumer cannot afford during a real outage.
func TestAttributeToAcquiring(t *testing.T) {
	deadlineStatus := status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	wrappedLocal := fmt.Errorf("get k: %w", context.DeadlineExceeded)
	somethingElse := errors.New("disk on fire")

	cases := []struct {
		name         string
		err          error
		sawAcquiring bool
		wantAcq      bool
		wantOrig     bool // the original error must remain matchable
	}{
		{"deadline with handoff evidence gains the reason", wrappedLocal, true, true, true},
		{"grpc deadline with handoff evidence gains the reason", deadlineStatus, true, true, true},
		{"deadline WITHOUT evidence stays a plain deadline", wrappedLocal, false, false, true},
		{"non-deadline with evidence is left alone", somethingElse, true, false, true},
		{"non-deadline without evidence is left alone", somethingElse, false, false, true},
		{"nil stays nil", nil, true, false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := attributeToAcquiring(tc.err, tc.sawAcquiring)
			if tc.err == nil {
				if got != nil {
					t.Fatalf("nil in, %v out", got)
				}
				return
			}
			if gotAcq := errors.Is(got, ErrAcquiring); gotAcq != tc.wantAcq {
				t.Fatalf("errors.Is(err, ErrAcquiring) = %v, want %v (err=%v)", gotAcq, tc.wantAcq, got)
			}
			if tc.wantOrig && !errors.Is(got, tc.err) {
				t.Fatalf("original error no longer matchable through the join: %v", got)
			}
		})
	}
}

// An already-acquiring terminal must not be double-joined: a caller that prints
// the error should not see the reason twice, and errors.Is must stay true.
func TestAttributeToAcquiring_AlreadyAcquiringIsUnchanged(t *testing.T) {
	in := fmt.Errorf("leg 3: %w", ErrAcquiring)
	got := attributeToAcquiring(in, true)
	if got != in {
		t.Fatalf("expected the error returned unchanged, got a new one: %v", got)
	}
	if !errors.Is(got, ErrAcquiring) {
		t.Fatal("lost ErrAcquiring")
	}
}

// isDeadlineErr must recognize BOTH shapes a fan-out leg produces: the local
// context deadline, and the gRPC status a remote leg returns when its own
// context expired. Missing the remote shape is what let the reason escape in
// the first place, since the observed failures were remote legs.
func TestIsDeadlineErr_BothShapes(t *testing.T) {
	if !isDeadlineErr(fmt.Errorf("wrapped: %w", context.DeadlineExceeded)) {
		t.Error("local context deadline not recognized")
	}
	if !isDeadlineErr(status.Error(codes.DeadlineExceeded, "context deadline exceeded")) {
		t.Error("remote gRPC deadline not recognized")
	}
	if isDeadlineErr(status.Error(codes.Unavailable, "peer down")) {
		t.Error("Unavailable must not count as a deadline")
	}
	if isDeadlineErr(errors.New("plain")) {
		t.Error("plain error must not count as a deadline")
	}
}
