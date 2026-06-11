// Package goleakignore holds the ONE canonical set of goleak ignore
// options shared by every test binary that stands up a Cluster (the
// cluster package's own TestMain and the integration TestMain).
//
// Why a shared package: both binaries spin up the same third-party
// background goroutines (memberlist gossip / probe / push-pull, gRPC
// client + server loops, hashicorp/go-metrics) whose teardown is async
// and can still be in their finishing path when goleak samples ONCE at
// the end of the run. Each binary must ignore the exact same set or it
// flakes. Historically the integration list was a hand-copied SUBSET of
// the cluster list and drifted: it missed the IgnoreAnyFunction form for
// memberlist pushPullTrigger, so a push-pull goroutine blocked in a TCP
// dial+read at teardown (its TOP frame in net/bufio, pushPullTrigger
// only DEEPER in the stack) slipped past the integration list's
// IgnoreTopFunction and failed the whole binary intermittently. Routing
// both TestMains through Options() makes that drift impossible.
//
// These goroutines are stable + well-behaved in production; the ignores
// only paper over the async teardown timing of test fixtures, never a
// real Cluster leak (the Cluster's OWN goroutines - events loop, sweep,
// reconcile, read-repair, fan-out drainers - are deliberately NOT
// ignored, so a regression that leaks one of those still fails goleak).
package goleakignore

import "go.uber.org/goleak"

// Options returns the canonical goleak ignore set. Pass the result
// (spread) to goleak.VerifyTestMain. Both forms appear for memberlist
// pushPullTrigger on purpose: IgnoreTopFunction matches when the
// goroutine is parked in pushPullTrigger itself; IgnoreAnyFunction
// matches when it is blocked DEEPER (a TCP dial+read during an in-flight
// push-pull at teardown) with pushPullTrigger only in the stack.
func Options() []goleak.Option {
	return []goleak.Option{
		// memberlist background workers; teardown is async + can
		// linger past Cluster.Close under heavy goroutine churn.
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).probeNode"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).pushPullTrigger"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).gossip"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).triggerFunc"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).probe"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).streamListen"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).packetListen"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).packetHandler"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*Memberlist).deschedule"),
		goleak.IgnoreTopFunction("github.com/hashicorp/memberlist.(*memberlistBroadcast).Finished"),
		// An in-flight push-pull blocked in network I/O at teardown has
		// pushPullTrigger DEEPER in the stack, not as its top frame, so
		// it needs the AnyFunction form to match. memberlist.Shutdown
		// only closes the stop channel; it does NOT wait for a push-pull
		// already dialing a peer.
		goleak.IgnoreAnyFunction("github.com/hashicorp/memberlist.(*Memberlist).pushPullTrigger"),
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
		// hashicorp/go-metrics signal handler pulled in transitively by
		// memberlist.
		goleak.IgnoreAnyFunction("github.com/hashicorp/go-metrics.(*inmemSignal).run"),
	}
}
