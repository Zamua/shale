package integration

// Integration coverage for multi-backend mode (v0.8 Phase 2): a real
// 2-node cluster where each node mounts the storage units the SHARED ring
// places on it, and a Put issued at one node is forwarded over gRPC to the
// node that owns the key's unit, landing physically in that node's mounted
// unit. This is the wired-together analogue of pkg/cluster's white-box
// per-unit routing tests; it exercises memberlist + gRPC forwarding + the
// unit-based owner guard end to end.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// multiNode bundles a multi-backend cluster node with the factory the test
// peers at to confirm physical per-unit placement.
type multiNode struct {
	ID       string
	Cluster  *cluster.Cluster
	Factory  *memfactory.Factory
	BindAddr string
	GRPCAddr string

	stop func()
}

func (n *multiNode) Close() {
	if n.Cluster != nil {
		_ = n.Cluster.Close()
		n.Cluster = nil
	}
	if n.stop != nil {
		n.stop()
		n.stop = nil
	}
}

// startMultiBackendNode brings up one multi-backend cluster node (factory +
// unit count) joined to seedAddr (empty = first node). Mirrors
// startTestNode's wiring but selects multi-backend mode.
func startMultiBackendNode(t *testing.T, id, seedAddr string, unitCount int) *multiNode {
	t.Helper()

	fac := memfactory.New()
	bindAddr := hostPort(freePort(t))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startMultiBackendNode %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:         id,
		BackendFactory: fac,
		UnitCount:      storageunit.MustUnitCount(unitCount),
		BindAddr:       bindAddr,
		GRPCAddr:       grpcAddr,
		LogOutput:      io.Discard,
	}
	if seedAddr != "" {
		cfg.Seeds = []string{seedAddr}
	}

	c, err := cluster.Open(cfg)
	if err != nil {
		_ = lis.Close()
		t.Fatalf("startMultiBackendNode %s: cluster.Open: %v", id, err)
	}

	rpc.NewServer(c).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &multiNode{
		ID:       id,
		Cluster:  c,
		Factory:  fac,
		BindAddr: bindAddr,
		GRPCAddr: grpcAddr,
		stop: func() {
			grpcSrv.GracefulStop()
			<-serveDone
		},
	}
	t.Cleanup(n.Close)
	return n
}

// unitOwnerID is the test-side mirror of the cluster's unit owner lookup:
// build a ring from the cluster's Members() snapshot, hash the fixed-width
// big-endian unit id (matching unitIDBytes), and report the owning node.
// Two rings built from the same members agree (deterministic hashing), so
// this faithfully previews which node owns a given key's unit.
func unitOwnerID(c *cluster.Cluster, key string, unitCount int) string {
	r := ring.New()
	for _, m := range c.Members() {
		r.Add(m)
	}
	uc := storageunit.MustUnitCount(unitCount)
	u := storageunit.UnitForShardKey(ring.ShardKey([]byte(key)), uc)
	var b [4]byte
	b[0] = byte(uint32(u) >> 24)
	b[1] = byte(uint32(u) >> 16)
	b[2] = byte(uint32(u) >> 8)
	b[3] = byte(uint32(u))
	return r.LocateKey(b[:]).ID
}

// keyForOwner returns a key whose unit is owned by wantOwner under the
// current ring. Fails the test if none is found in a reasonable search.
func keyForOwner(t *testing.T, c *cluster.Cluster, unitCount int, wantOwner string) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("mb-key-%04d", i)
		if unitOwnerID(c, k, unitCount) == wantOwner {
			return k
		}
	}
	t.Fatalf("no key found whose unit is owned by %s", wantOwner)
	return ""
}

// TestMultiBackend_ForwardsToUnitOwner stands up a 2-node multi-backend
// cluster and writes, from N1, a key whose unit N2 owns. The write must be
// forwarded to N2 and land in N2's mounted unit backend (not N1's). It then
// reads the key back from BOTH nodes (each routes to N2) and confirms a
// key owned by N1 lands on N1, so both directions of forwarding work.
func TestMultiBackend_ForwardsToUnitOwner(t *testing.T) {
	const unitCount = 8
	n1 := startMultiBackendNode(t, "mb1", "", unitCount)
	n2 := startMultiBackendNode(t, "mb2", n1.BindAddr, unitCount)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("ring convergence: %v", err)
	}

	// A key whose unit N2 owns, and one whose unit N1 owns.
	keyOnN2 := keyForOwner(t, n1.Cluster, unitCount, "mb2")
	keyOnN1 := keyForOwner(t, n1.Cluster, unitCount, "mb1")

	// Write both from N1. The N2-owned key is forwarded over gRPC.
	if err := n1.Cluster.Put([]byte(keyOnN2), []byte("val-n2")); err != nil {
		t.Fatalf("Put forwarded to N2: %v", err)
	}
	if err := n1.Cluster.Put([]byte(keyOnN1), []byte("val-n1")); err != nil {
		t.Fatalf("Put local on N1: %v", err)
	}

	// Physical placement: the N2-owned key is on N2's storage and absent
	// on N1; vice versa for the N1-owned key. (A key is only on a node if
	// its unit is mounted there, which is exactly what forwarding routed
	// it for.)
	assertPlacedOn(t, n2, keyOnN2, "val-n2")
	assertAbsentOn(t, n1, keyOnN2)
	assertPlacedOn(t, n1, keyOnN1, "val-n1")
	assertAbsentOn(t, n2, keyOnN1)

	// Read-back through BOTH nodes routes to the right owner and returns
	// the value (N2-owned key read from N1 is a forwarded Get).
	for _, c := range clusters {
		got, err := c.Get([]byte(keyOnN2))
		if err != nil {
			t.Fatalf("Get %q via %s: %v", keyOnN2, c.NodeID(), err)
		}
		if !bytes.Equal(got, []byte("val-n2")) {
			t.Fatalf("Get %q via %s = %q, want val-n2", keyOnN2, c.NodeID(), got)
		}
	}

	// Delete the N2-owned key from N1 (forwarded) and confirm it's gone
	// from N2's unit.
	if err := n1.Cluster.Delete([]byte(keyOnN2)); err != nil {
		t.Fatalf("Delete forwarded: %v", err)
	}
	assertAbsentOn(t, n2, keyOnN2)
}

// assertPlacedOn fails unless key=wantVal is physically present on node's
// storage (its union of mounted units, via the admin local scan).
func assertPlacedOn(t *testing.T, node *multiNode, key, wantVal string) {
	t.Helper()
	it, err := node.Cluster.LocalScanPrefix(nil)
	if err != nil {
		t.Fatalf("LocalScanPrefix on %s: %v", node.ID, err)
	}
	defer func() { _ = it.Close() }()
	for {
		k, v, err := it.Next()
		if err != nil {
			t.Fatalf("scan next on %s: %v", node.ID, err)
		}
		if k == nil {
			break
		}
		if string(k) == key {
			if !bytes.Equal(v, []byte(wantVal)) {
				t.Fatalf("key %q on %s has value %q, want %q", key, node.ID, v, wantVal)
			}
			return
		}
	}
	t.Fatalf("key %q not found on %s storage", key, node.ID)
}

// assertAbsentOn fails if key is found anywhere in node's mounted units.
func assertAbsentOn(t *testing.T, node *multiNode, key string) {
	t.Helper()
	it, err := node.Cluster.LocalScanPrefix(nil)
	if err != nil {
		t.Fatalf("LocalScanPrefix on %s: %v", node.ID, err)
	}
	defer func() { _ = it.Close() }()
	for {
		k, _, err := it.Next()
		if err != nil {
			t.Fatalf("scan next on %s: %v", node.ID, err)
		}
		if k == nil {
			return
		}
		if string(k) == key {
			t.Fatalf("key %q unexpectedly present on %s", key, node.ID)
		}
	}
}

// TestMultiBackend_UnitOwnerGuardRefusesForward dials a node DIRECTLY with
// forwarded=true on a key whose unit that node does NOT own/mount. The
// server must refuse with FailedPrecondition (the unit-based owner guard),
// not re-forward, mirroring the legacy loop guard.
//
// The target is the JOINER (mb2): it Opens with both members already
// visible, so it mounts ONLY the units the 2-node ring assigns it - it
// genuinely never mounts mb1's units. (The SEED, by contrast, mounted the
// whole unit space when it was the only member and never unmounts under
// Phase 2 static topology, so forwarding an mb2-owned key to mb1 would be
// ACCEPTED, not refused. That stale-ownership window is the documented
// Phase 2 behavior the Phase 3 lease handoff fixes; we deliberately probe
// the joiner to get a node that truly does not own the unit.)
func TestMultiBackend_UnitOwnerGuardRefusesForward(t *testing.T) {
	const unitCount = 8
	n1 := startMultiBackendNode(t, "mb1", "", unitCount)
	n2 := startMultiBackendNode(t, "mb2", n1.BindAddr, unitCount)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster}, 2, 15*time.Second); err != nil {
		t.Fatalf("ring convergence: %v", err)
	}

	// A key whose unit mb1 owns. We forward it to mb2 (which never mounted
	// mb1's units), so mb2's unit-based guard must refuse it.
	keyOnN1 := keyForOwner(t, n1.Cluster, unitCount, "mb1")

	// Sanity: mb2 really did NOT mount this key's unit (otherwise the test
	// would not be probing the guard).
	if n2.Cluster.OwnsKey([]byte(keyOnN1)) {
		t.Fatalf("precondition failed: mb2 unexpectedly owns/mounts the unit for %q", keyOnN1)
	}

	conn, err := grpc.NewClient(n2.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial n2: %v", err)
	}
	defer func() { _ = conn.Close() }()
	api := pb.NewShaleNodeClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Put forwarded to the non-owner: refused with FailedPrecondition.
	_, err = api.Put(ctx, &pb.PutRequest{Key: []byte(keyOnN1), Value: []byte("v"), Forwarded: true})
	if err == nil {
		t.Fatal("expected forwarded Put on a unit the node does not own to be refused")
	}
	if st, _ := status.FromError(err); st.Code() != codes.FailedPrecondition {
		t.Fatalf("Put: expected FailedPrecondition, got %v (%v)", st.Code(), err)
	}

	// Get forwarded to the non-owner: refused.
	_, err = api.Get(ctx, &pb.GetRequest{Key: []byte(keyOnN1), Forwarded: true})
	if err == nil {
		t.Fatal("expected forwarded Get on a unit the node does not own to be refused")
	}
	if st, _ := status.FromError(err); st.Code() != codes.FailedPrecondition {
		t.Fatalf("Get: expected FailedPrecondition, got %v (%v)", st.Code(), err)
	}

	// ScanPrefix forwarded to the non-owner: refused.
	stream, err := api.ScanPrefix(ctx, &pb.ScanPrefixRequest{Prefix: []byte(keyOnN1), Forwarded: true})
	if err != nil {
		t.Fatalf("ScanPrefix open: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected forwarded ScanPrefix on a non-owned unit to be refused")
	} else if st, _ := status.FromError(err); st.Code() != codes.FailedPrecondition {
		t.Fatalf("ScanPrefix: expected FailedPrecondition, got %v", st.Code())
	}

	// A NON-forwarded request to mb2 still works (it routes internally to
	// mb1, the unit owner).
	if err := n2.Cluster.Put([]byte(keyOnN1), []byte("routed")); err != nil {
		t.Fatalf("non-forwarded Put via mb2 should route to mb1 and succeed: %v", err)
	}
	got, err := n1.Cluster.Get([]byte(keyOnN1))
	if err != nil || !bytes.Equal(got, []byte("routed")) {
		t.Fatalf("routed write not visible on mb1: got=%q err=%v", got, err)
	}
}
