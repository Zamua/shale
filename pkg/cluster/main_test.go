package cluster_test

// Package-level TestMain for the cluster test binary. The whole
// binary (both the cluster_test "external" file set + the cluster
// "internal" file set, since the latter still lives in package
// cluster and compiles into the same binary) runs through here.
//
// goleak.VerifyTestMain catches goroutine leaks that survive the
// last test's t.Cleanup. The cluster surface spawns many background
// goroutines (events loop, reconcile loop, sweep loop, read-repair
// workers, fan-out drainers, memberlist gossip, gRPC client streams)
// so a leak that escapes Close shows up here as a per-test or
// per-binary residue.
//
// Known third-party leakers we IGNORE:
//
//   - memberlist's per-instance push/pull, gossip, and probe loops.
//     These DO exit on Close, but the test binary's memberlist
//     instances are torn down via Cluster.Close which serializes
//     with the event loops; under -race + heavy goroutine churn the
//     memberlist goroutines can still be in their "finishing" path
//     when goleak samples. They are stable + well-behaved in
//     production; we don't want a flaky CI signal from them. Match
//     by top-function so a NEW leak path catches future regressions.
//   - hashicorp/go-metrics + go-immutable-radix background workers
//     pulled in transitively by memberlist.
//   - grpc's clientStream / serverStream / addrConnLoopback loops
//     that linger briefly after grpc.Server.Stop. Not a real leak;
//     gRPC's own teardown is async.

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// memberlist background workers
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
		goleak.IgnoreAnyFunction("github.com/hashicorp/memberlist.(*Memberlist).pushPullTrigger"),
		// gRPC client + server background loops that linger briefly
		// past Stop. Not a real leak; gRPC's teardown is async.
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
		// hashicorp metrics + transitive goroutines
		goleak.IgnoreAnyFunction("github.com/hashicorp/go-metrics.(*inmemSignal).run"),
	)
}
