package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Zamua/shale/pkg/rpc"
)

// dial wraps rpc.NewClient + reports any synchronous dial error to
// stderr in a consistent shape. The returned cleanup func is safe to
// call even if dial failed (it's a no-op).
//
// gRPC's NewClient is non-blocking, so a stale --addr won't surface
// here - it surfaces on the first RPC. The RPC-level handlers map
// that to exitConnection via exitCodeForRPCError.
func dial(addr string, stderr io.Writer) (*rpc.Client, func(), int) {
	cli, err := rpc.NewClient(addr)
	if err != nil {
		fmt.Fprintf(stderr, "shale: dial %s: %v\n", addr, err)
		return nil, func() {}, exitConnection
	}
	return cli, func() { _ = cli.Close() }, exitOK
}

// withTimeout returns a derived context that respects opts.timeout.
// The caller is responsible for cancelling it.
func withTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}
