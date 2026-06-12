package main

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestExitCodeForRPCError pins the mapping from gRPC client errors to
// CLI exit codes. Spec calls out: 0 ok, 1 generic, 2 not-found,
// 3 connection, 4 timeout. Anything we add to the map later needs to
// land here too.
func TestExitCodeForRPCError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nil-is-ok", nil, exitOK},
		{"context-deadline-is-timeout", context.DeadlineExceeded, exitTimeout},
		{"context-canceled-is-connection", context.Canceled, exitConnection},
		{"status-deadline-is-timeout", status.Error(codes.DeadlineExceeded, "deadline"), exitTimeout},
		{"status-unavailable-is-connection", status.Error(codes.Unavailable, "unavailable"), exitConnection},
		{"status-canceled-is-connection", status.Error(codes.Canceled, "canceled"), exitConnection},
		{"status-not-found-is-not-found", status.Error(codes.NotFound, "absent"), exitNotFound},
		{"status-ok-is-ok", status.Error(codes.OK, ""), exitOK},
		{"status-internal-is-generic", status.Error(codes.Internal, "boom"), exitGeneric},
		{"status-invalid-arg-is-generic", status.Error(codes.InvalidArgument, "bad"), exitGeneric},
		{"plain-error-is-generic", errors.New("something broke"), exitGeneric},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := exitCodeForRPCError(c.err)
			if got != c.want {
				t.Fatalf("exitCodeForRPCError(%v) = %d, want %d", c.err, got, c.want)
			}
		})
	}
}

// TestExitCodeForWrappedContextErr makes sure errors that wrap the
// context sentinels (which the gRPC stack does when it surfaces a
// deadline/cancel) still map to the right exit code.
func TestExitCodeForWrappedContextErr(t *testing.T) {
	wrapped := errors.New("dial tcp: " + context.DeadlineExceeded.Error())
	// A plain wrap that doesn't preserve the sentinel via Is should
	// fall through to generic.
	if got := exitCodeForRPCError(wrapped); got != exitGeneric {
		t.Fatalf("plain string wrap: got %d, want %d (generic)", got, exitGeneric)
	}

	// A proper errors.Join including the sentinel must still map.
	joined := errors.Join(errors.New("dial tcp"), context.DeadlineExceeded)
	if got := exitCodeForRPCError(joined); got != exitTimeout {
		t.Fatalf("errors.Join(DeadlineExceeded): got %d, want %d (timeout)", got, exitTimeout)
	}
}
