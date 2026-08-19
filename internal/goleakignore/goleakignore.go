// Package goleakignore holds the ONE canonical set of goleak ignore
// options shared by every test binary that stands up a Cluster (the
// cluster package's own TestMain and the integration TestMain).
//
// Why a shared package: both binaries spin up the same third-party
// background goroutines (gRPC client + server loops) whose teardown is
// async and can still be in their finishing path when goleak samples
// ONCE at the end of the run. Each binary must ignore the exact same
// set or it flakes; routing both TestMains through Options() makes the
// lists unable to drift.
//
// These goroutines are stable + well-behaved in production; the ignores
// only paper over the async teardown timing of test fixtures, never a
// real Cluster leak (the Cluster's OWN goroutines - events loop, sweep,
// reconcile, read-repair, fan-out drainers - are deliberately NOT
// ignored, so a regression that leaks one of those still fails goleak).
package goleakignore

import "go.uber.org/goleak"

// Options returns the canonical goleak ignore set. Pass the result
// (spread) to goleak.VerifyTestMain.
func Options() []goleak.Option {
	return []goleak.Option{
		// gRPC client + server background loops that linger briefly past
		// Stop. Not a real leak; gRPC's teardown is async.
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ClientConn).updateResolverStateAndUnlock"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc/internal/transport.(*http2Client).reader"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc/internal/transport.(*http2Server).keepalive"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc/internal/transport.(*http2Server).HandleStreams"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc/internal/transport.NewServerTransport.func2"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc.(*Server).handleStream"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc.(*addrConn).resetTransportAndUnlock"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc.(*addrConn).connect"),
	}
}
