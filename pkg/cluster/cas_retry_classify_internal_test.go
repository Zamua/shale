package cluster

// White-box characterization test for commitRetryable, the classifier
// Transact uses to decide whether a failed CommitCAS is a TRANSIENT
// reshard-window signal (re-run fn, do not spend a conflict attempt) or
// a terminal failure (abort).
//
// The load-bearing case is codes.FailedPrecondition: a forwarded
// CommitCAS that lands on the node that JUST lost ownership of pin_key
// across the reshard FLIP/redistribution cutover returns the "re-pin
// against the current ring" loop-guard. Before the fix, Transact only
// retried codes.Unavailable, so this FailedPrecondition fell through to
// the terminal-error return and a Transact spanning the reshard cutover
// failed instead of retrying-then-committing. commitRetryable now
// classifies BOTH codes as retryable; this test pins that and guards the
// boundary so a future change that drops FailedPrecondition (regressing
// the flake) fails here.

import (
	"errors"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCommitRetryable_ClassifiesReshardWindowSignals(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "FailedPrecondition (reshard cutover re-pin) is retryable",
			err:  status.Error(codes.FailedPrecondition, "shale: CommitCAS: this node does not own pin_key; re-pin against the current ring"),
			want: true,
		},
		{
			name: "Unavailable (handoff / acquiring window) is retryable",
			err:  status.Error(codes.Unavailable, "cluster: CommitCASApply: unit for key is handing off to this node; retry shortly"),
			want: true,
		},
		{
			name: "ResourceExhausted (migration guard) is NOT in this classifier",
			err:  status.Error(codes.ResourceExhausted, "key is migrating out"),
			want: false,
		},
		{
			name: "Internal (a real backend failure) is terminal",
			err:  status.Error(codes.Internal, "backend exploded"),
			want: false,
		},
		{
			name: "ErrCASConflict (the OCC conflict sentinel) is not a status, not retryable here",
			err:  backend.ErrCASConflict,
			want: false,
		},
		{
			name: "a plain non-status error is terminal",
			err:  errors.New("cluster: CommitCAS owner error: something"),
			want: false,
		},
		{
			name: "nil is not retryable",
			err:  nil,
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := commitRetryable(tc.err); got != tc.want {
				t.Fatalf("commitRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
