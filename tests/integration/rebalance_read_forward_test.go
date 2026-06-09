package integration

// Read-forwarding scenario: per docs/SPEC.md "Cutover", a Get for a
// key whose partition is being received INTO this node (StateReceiving)
// must be transparently forwarded back to the SOURCE. The destination
// is not yet authoritative; the source still owns the read. From the
// caller's perspective the Get succeeds rather than returning a
// transient error.
//
// Pre-fix behavior: the destination returned codes.FailedPrecondition
// with "try other owner" instead of forwarding. Clients saw a
// transient error during the entire receive window + had to implement
// retry-against-the-old-owner logic themselves -- behavior the spec
// explicitly puts on the cluster, not the SDK.
//
// This test fires reads at the destination's GRPC server's Get path
// directly via the cluster handle during the receive window + asserts
// each read succeeds. The window is racy (memory backend + loopback
// gRPC complete streams in milliseconds), so the test hammers the
// target key during the entire bootstrap + flags any error response
// other than the post-rebalance succeed.

import (
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
)

func TestRebalance_ReadDuringReceiveForwardsToSource(t *testing.T) {
	t.Parallel()

	// 2-node baseline.
	n1 := startTestNode(t, "rf-n1", "")
	n2 := startTestNode(t, "rf-n2", n1.BindAddr)
	pair := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(pair, 2, 10*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}

	// Pick a hash-tag prefix whose owner moves to rf-n3 on join +
	// seed MANY keys under that tag so they all hash to the same
	// partition. A larger receive load stretches the StateReceiving
	// window long enough for the read-forwarding path to actually
	// fire in the test (memory backend + loopback gRPC make small
	// receives near-instantaneous).
	futureRing := ring.New()
	for _, m := range n1.Cluster.Members() {
		futureRing.Add(m)
	}
	futureRing.Add(ring.Member{ID: "rf-n3", Addr: "127.0.0.1:0"})
	currentRing := ring.New()
	for _, m := range n1.Cluster.Members() {
		currentRing.Add(m)
	}

	var tag string
	for i := 0; i < 5000; i++ {
		candidate := fmt.Sprintf("u%05d", i)
		probe := "{" + candidate + "}/probe"
		if currentRing.LocateKey([]byte(probe)).ID != futureRing.LocateKey([]byte(probe)).ID &&
			futureRing.LocateKey([]byte(probe)).ID == "rf-n3" {
			tag = candidate
			break
		}
	}
	if tag == "" {
		t.Fatalf("could not find a hash-tag whose owner moves to rf-n3 (ring distribution unexpectedly skewed)")
	}

	// Seed many keys under the tag, each with a hefty value. All keys
	// share the same partition (Redis-style hash tagging via
	// ring.ShardKey), so the receive for that partition has real
	// streaming work to do (the receive-window the spec's read
	// forwarder needs to span). Memory backend + loopback gRPC are
	// very fast; the receive load has to be big enough to outpace
	// the test's read loop spin-up.
	const seedCount = 1000
	const valueBytes = 64 * 1024
	val := make([]byte, valueBytes)
	for i := range val {
		val[i] = byte('A' + (i % 26))
	}
	seedKeys := make([]string, seedCount)
	for i := 0; i < seedCount; i++ {
		k := fmt.Sprintf("{%s}/item/%05d", tag, i)
		if err := putWithRetry(n1.Cluster, []byte(k), val); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
		seedKeys[i] = k
	}
	target := seedKeys[0]

	// Bring up rf-n3. Its bootstrap Evaluate registers a Receive for
	// the target partition; runReceive starts FetchRange against
	// the source. While that's in flight, any Get that lands on
	// rf-n3 for the target key must be forwarded back to the
	// source via the cluster's Get path + succeed.
	n3 := startTestNode(t, "rf-n3", n1.BindAddr)
	trio := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(trio, 3, 10*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}

	// Hammer Get against rf-n3 for the target key during + past the
	// bootstrap window. We MUST observe at least one read while
	// the receive is still in flight (StateReceiving): the pre-fix
	// path returns Unavailable/FailedPrecondition there, the post-
	// fix path forwards to source + the Get succeeds. Any error
	// here means the receive-window forwarder didn't trigger.
	deadline := time.Now().Add(8 * time.Second)
	totalReads := 0
	for time.Now().Before(deadline) {
		totalReads++
		got, err := n3.Cluster.Get([]byte(target))
		if err != nil {
			t.Fatalf("Get %s via rf-n3 during receive window failed after %d reads: %v\n"+
				"(per docs/SPEC.md \"Cutover\" the destination MUST forward reads to the source while StateReceiving; returning an error here is the pre-fix behavior the read-forwarding fix closes)",
				target, totalReads, err)
		}
		if len(got) != valueBytes {
			t.Fatalf("Get %s via rf-n3 returned wrong-size value: got %d bytes, want %d (forwarding misrouted, or the wrong source is being asked)",
				target, len(got), valueBytes)
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Logf("read-forwarding stable across %d Gets during the receive window", totalReads)

	// Confirm the test actually exercised the receive-window code
	// path. n3's RebalanceSnapshot should contain at least one
	// non-zero KeyCount entry for the target partition the bootstrap
	// pulled in -- if every range there is 0/done with no work
	// done, the receive completed before we even saw it + the test
	// did not meaningfully verify the forwarder.
	snap := n3.Cluster.RebalanceSnapshot()
	sawReceive := false
	for _, s := range snap {
		if s.KeyCount > 0 {
			sawReceive = true
			break
		}
	}
	if !sawReceive {
		t.Fatalf("n3 never recorded a non-empty receive for the bootstrap partition; the receive window never opened + this test did not exercise the forwarder")
	}
}
