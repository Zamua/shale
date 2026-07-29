package integration

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
)

// TestThreeNode_AggregateWaitsForPeerGRPCColdStart pins the fix for the
// cross-shard scan cold-start trap:
//
// The peer gRPC client (clientFor -> newPeerClient) is CACHED for the cluster
// lifetime and FAIL-FAST (gRPC's WaitForReady=false default). If a peer's gRPC
// server is momentarily not-serving at the instant Aggregate first connects (a
// heavy cold-start where the process is busy mounting units), the cached
// ClientConn drops into TRANSIENT_FAILURE + backoff and every fan-out RPC on it
// fails INSTANTLY with "error reading server preface: use of closed network
// connection" for the whole backoff window - so the scan records that peer's
// AggregateResult.Err even though the peer comes up a moment later. This is the
// prod blocker for the hostthis blob migration AND the long-broken sweeper.
//
// The fix (Config.PeerConnectTimeout + peerClient.waitReady, plus fast
// reconnect backoff + keepalive on the peer dial) makes snapshotPeer WAIT for
// the peer's connection to reach READY before opening the scan stream. This
// test stands up a 3-node cluster where node 3's gRPC server is NOT serving at
// first, brings it up shortly after the Aggregate begins dialing it, and
// asserts every peer's result has a nil Err. Reverting the fix (drop the
// waitReady call in snapshotPeer) makes node 3's result fail-fast with the
// preface error, failing this test.
func TestThreeNode_AggregateWaitsForPeerGRPCColdStart(t *testing.T) {
	n1 := startTestNode(t, "pcn1", "")
	n2 := startTestNode(t, "pcn2", n1.BindAddr)
	// n3 joins the gossip ring but its gRPC server is DOWN (port reserved +
	// advertised, nothing listening yet) - exactly the cold-start window where a
	// peer is a known member but not yet serving gRPC.
	n3 := startTestNodeGRPCDown(t, "pcn3", n1.BindAddr)

	cs := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	// Membership converges over gossip (memberlist), independent of n3's gRPC.
	if err := waitForMembersAll(cs, 3, 30*time.Second); err != nil {
		t.Fatalf("ring did not converge to 3 members: %v", err)
	}

	// Bring n3's gRPC up shortly AFTER the Aggregate has started dialing it, so
	// the fan-out's first connect attempt hits a down server and must WAIT for
	// it to come up (well within PeerConnectTimeout's default 30s).
	startedGRPC := make(chan struct{})
	go func() {
		time.Sleep(400 * time.Millisecond)
		startNodeGRPC(t, n3)
		close(startedGRPC)
	}()

	results := n1.Cluster.Aggregate(func(b backend.Backend) any {
		it, err := b.ScanPrefix(nil)
		if err != nil {
			return err
		}
		defer func() { _ = it.Close() }()
		n := 0
		for {
			k, _, e := it.Next()
			if e != nil || k == nil {
				break
			}
			n++
		}
		return n
	})

	<-startedGRPC // ensure the goroutine finished before t.Cleanup tears n3 down

	if len(results) != 3 {
		t.Fatalf("Aggregate returned %d results, want 3 (one per node)", len(results))
	}
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("Aggregate result %d returned Err - a peer whose gRPC was momentarily "+
				"down-then-up at cold-start was NOT tolerated (fail-fast cached client): %v", i, r.Err)
		}
	}
}

// startTestNodeGRPCDown brings up a cluster node that JOINS the gossip ring but
// is NOT serving gRPC yet: the gRPC port is reserved (so the address it
// advertises via memberlist Meta is stable) but no listener is bound, so a peer
// dialing it gets connection-refused until startNodeGRPC is called. Used to
// reproduce the "peer is a known member but its gRPC server is not ready yet"
// cold-start window. Cleanup is wired via t.Cleanup (testNode.Close tolerates a
// nil stop).
func startTestNodeGRPCDown(t *testing.T, id, seedAddr string) *testNode {
	t.Helper()
	h := fixtureBacking(t).Handle()
	grpcAddr := hostPort(freePort(t)) // reserve a port; do NOT listen yet
	cfg := cluster.Config{
		NodeID:               id,
		BackendFactory:       h,
		UnitCount:            storageunit.MustUnitCount(defaultTestUnitCount),
		GRPCAddr:             grpcAddr,
		LogOutput:            io.Discard,
		RebalanceSettleDelay: 500 * time.Millisecond,
		ReplicationFactor:    1,
	}
	gcfg := gossip.Config{LogOutput: io.Discard}
	if seedAddr != "" {
		gcfg.Seeds = []string{seedAddr}
	}
	c, bindAddr := openClusterRetryBind(t, cfg, gcfg)
	n := &testNode{
		ID:       id,
		Cluster:  c,
		Handle:   h,
		BindAddr: bindAddr,
		GRPCAddr: grpcAddr,
		// grpcServer + stop are nil until startNodeGRPC; testNode.Close handles nil.
	}
	t.Cleanup(n.Close)
	return n
}

// startNodeGRPC starts (or restarts) a testNode's gRPC server on its advertised
// GRPCAddr and rewires its stop closure so t.Cleanup tears it down. Pairs with
// startTestNodeGRPCDown to bring a node's gRPC up mid-test.
func startNodeGRPC(t *testing.T, n *testNode) {
	t.Helper()
	lis, err := net.Listen("tcp", n.GRPCAddr)
	if err != nil {
		t.Errorf("startNodeGRPC %s: listen %s: %v", n.ID, n.GRPCAddr, err)
		return
	}
	srv := grpc.NewServer()
	rpc.NewServer(n.Cluster).Register(srv)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = srv.Serve(lis)
	}()
	n.grpcServer = srv
	n.stop = func() {
		srv.GracefulStop()
		<-serveDone
	}
}
