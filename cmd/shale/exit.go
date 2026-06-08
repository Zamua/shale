package main

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Exit codes, mirroring docs/SPEC.md. Kept as ints (not iota) so the
// numeric values are obvious at the call site.
const (
	exitOK         = 0
	exitGeneric    = 1
	exitNotFound   = 2
	exitConnection = 3
	exitTimeout    = 4
)

// exitCodeForRPCError maps a gRPC client error to one of the shale
// CLI exit codes. The mapping is deliberately small + spec-driven:
//
//   - context.DeadlineExceeded / codes.DeadlineExceeded -> 4 (timeout)
//   - codes.Unavailable / context.Canceled              -> 3 (connection)
//   - codes.NotFound                                    -> 2 (not-found)
//   - anything else                                     -> 1 (generic)
//
// Get's own "absent key" path does NOT come through here - the gRPC
// service surfaces absence as not_found=true in the response, not as
// a status error. That branch maps to exitNotFound in runGet directly.
// We still honor codes.NotFound here for any future RPC that chooses
// to surface absence via gRPC status (e.g. an admin lookup).
//
// A nil error returns exitOK so callers can write
// `return exitCodeForRPCError(err)` for the success path too.
func exitCodeForRPCError(err error) int {
	if err == nil {
		return exitOK
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return exitTimeout
	}
	if errors.Is(err, context.Canceled) {
		return exitConnection
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.OK:
			return exitOK
		case codes.DeadlineExceeded:
			return exitTimeout
		case codes.Unavailable, codes.Canceled:
			return exitConnection
		case codes.NotFound:
			return exitNotFound
		default:
			return exitGeneric
		}
	}
	return exitGeneric
}
