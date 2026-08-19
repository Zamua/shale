package integration

// End-to-end coverage for the v0.8 Phase 3 lease-handoff rebalance. These
// tests stand up in-process multi-backend clusters whose per-node factories
// share ONE backing (internal/sharedfactory) - the test analogue of shared
// object storage + a per-unit durable writer-epoch. That shared backing is
// what makes the handoff COPY-FREE and FENCE-correct across nodes: when a
// unit's lease moves from node A to node B, B opens the SAME underlying
// bytes (zero copy) and the durable epoch advances, fencing A.
//
// The invariant under test is the data-loss-sensitive one Phase 3 is built
// to: NO ACKED WRITE MAY BE LOST. A write that returned success on node A
// before a handoff MUST be readable after the unit's lease moves to node B.
//
// Phase 2's multibackend_phase2_test.go uses the PER-FACTORY memfactory and
// asserts STATIC routing; it cannot express a cross-node handoff because
// each node's store is separate. This file is the dynamic-topology
// counterpart, and it needs the shared backing precisely for that reason.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/clustertest"
	"github.com/Zamua/shale/internal/sharedfactory"
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

// sharedNode is a multi-backend node whose factory is a per-node HANDLE over
// a shared backing. Several sharedNodes off one Backing model a cluster
// whose units live in shared storage, so a lease handoff is copy-free.
type sharedNode struct {
	ID       string
	Cluster  *cluster.Cluster
	Handle   *sharedfactory.Handle
	BindAddr string
	GRPCAddr string

	stop func()
}

func (n *sharedNode) Close() {
	if n.Cluster != nil {
		_ = n.Cluster.Close()
		n.Cluster = nil
	}
	if n.stop != nil {
		n.stop()
		n.stop = nil
	}
}

// startSharedNode brings up one multi-backend node whose factory is a handle
// over backing. A short settle delay keeps the reconcile debounce snappy for
// tests. seedAddr empty = founder.
func startSharedNode(t *testing.T, id, seedAddr string, unitCount int, backing *sharedfactory.Backing) *sharedNode {
	t.Helper()

	h := backing.Handle()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startSharedNode %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:               id,
		BackendFactory:       h,
		UnitCount:            storageunit.MustUnitCount(unitCount),
		GRPCAddr:             grpcAddr,
		LogOutput:            io.Discard,
		RebalanceSettleDelay: 300 * time.Millisecond,
	}
	store, token := coordStoreFor(t, seedAddr)
	c := clustertest.OpenClusterCAS(t, cfg, store)

	rpc.NewServer(c).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &sharedNode{
		ID:       id,
		Cluster:  c,
		Handle:   h,
		BindAddr: token,
		GRPCAddr: grpcAddr,
		stop: func() {
			grpcSrv.GracefulStop()
			<-serveDone
		},
	}
	t.Cleanup(n.Close)
	return n
}

// unitOwnerOnRing previews which node owns key's unit under the ring built
// from c.Members() - the same deterministic hashing the cluster uses.
func unitOwnerOnRing(c *cluster.Cluster, key string, unitCount int) string {
	r := ring.New()
	for _, m := range c.Members() {
		r.Add(m)
	}
	uc := storageunit.MustUnitCount(unitCount)
	u := storageunit.UnitForShardKey(ring.ShardKey([]byte(key)), uc)
	// Encode the gen-0 unit id the way the cluster's genUnitBytes does (8-byte
	// generation prefix, then the 4-byte unit id) so this test ring agrees with
	// routing. These tests do not reshard, so the generation is 0.
	return r.LocateKey(gen0UnitBytes(u)).ID
}

// waitUntil polls fn until it returns true or the deadline passes.
func waitUntil(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fn()
}

// TestLeaseHandoff_NoAckedWriteLost is THE Phase 3 gate. A founder writes a
// batch of ACKED keys (every Put returned success, so every key is durable
// in the shared backing). A second node joins; the ring re-assigns roughly
// half the units to it, and the copy-free lease handoff moves those units'
// LEASES (not bytes) to the joiner. After convergence EVERY acked key must
// still be readable through the cluster - none lost to the handoff - AND a
// key whose unit moved to the joiner must be served by the JOINER (locally,
// from the shared bytes it opened), proving the lease actually moved.
func TestLeaseHandoff_NoAckedWriteLost(t *testing.T) {
	const unitCount = 16
	backing := sharedfactory.NewBacking()

	n1 := startSharedNode(t, "lh1", "", unitCount, backing)

	// Founder owns the whole unit space; write a spread of acked keys.
	const nKeys = 200
	keys := make([]string, 0, nKeys)
	for i := range nKeys {
		k := fmt.Sprintf("rec-%04d", i)
		if err := n1.Cluster.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("founder Put %q: %v", k, err)
		}
		keys = append(keys, k)
	}

	// Join the second node. Membership convergence + the reconcile (debounced
	// by the 300ms settle delay) hand off roughly half the units to it.
	n2 := startSharedNode(t, "lh2", n1.BindAddr, unitCount, backing)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("ring convergence: %v", err)
	}

	// Wait for the handoff to land: at least one unit must move to n2 (with
	// 16 units across 2 nodes this is overwhelmingly likely), so wait until
	// n2 has mounted a non-empty set.
	if !waitUntil(10*time.Second, func() bool {
		return len(n2.Handle.OpenUnits()) > 0
	}) {
		t.Fatalf("n2 never acquired any unit after join (no handoff): n2 open=%v", n2.Handle.OpenUnits())
	}

	// NO ACKED WRITE LOST: every key written before the handoff is still
	// readable through the cluster surface (from either node, which routes
	// to the current owner). Retry the read briefly to ride out the handoff
	// window (a key whose unit is mid-acquire returns a retryable error).
	for _, k := range keys {
		want := []byte("v-" + k)
		var got []byte
		var err error
		ok := waitUntil(5*time.Second, func() bool {
			got, err = n1.Cluster.Get([]byte(k))
			return err == nil
		})
		if !ok {
			t.Fatalf("acked key %q unreadable after handoff: %v (NO ACKED WRITE LOST violated)", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("acked key %q = %q, want %q after handoff", k, got, want)
		}
	}

	// Prove the lease MOVED: find a key whose unit n2 now owns, and confirm
	// n2 serves it from its LOCALLY mounted backend (the shared bytes it
	// opened on acquire) - not by forwarding back to n1 - while n1 (the old
	// owner) has RELEASED it. Both halves of the handoff run asynchronously
	// behind the settle-timer reconcile, so wait for them to land.
	var movedKey string
	for _, k := range keys {
		if unitOwnerOnRing(n1.Cluster, k, unitCount) == n2.ID {
			movedKey = k
			break
		}
	}
	if movedKey == "" {
		t.Skip("no key's unit landed on n2 under this ring; handoff direction not exercised")
	}
	// n2 (new owner) must serve movedKey from its own mounted backend: the
	// acquire mounted the shared bytes copy-free. Wait for the acquire half.
	var got []byte
	if !waitUntil(10*time.Second, func() bool {
		var err error
		got, err = n2.Cluster.LocalGet([]byte(movedKey))
		return err == nil
	}) {
		t.Fatalf("n2.LocalGet(%q) never succeeded: new owner did not acquire the unit's bytes copy-free", movedKey)
	}
	if !bytes.Equal(got, []byte("v-"+movedKey)) {
		t.Fatalf("n2.LocalGet(%q) = %q, want %q (copy-free bytes wrong)", movedKey, got, "v-"+movedKey)
	}
	// n1 (old owner) must RELEASE it: LocalGet on a unit it no longer mounts
	// returns ErrNotFound. Wait for the release half of the handoff.
	if !waitUntil(10*time.Second, func() bool {
		_, err := n1.Cluster.LocalGet([]byte(movedKey))
		return err != nil
	}) {
		t.Fatalf("n1.LocalGet(%q) still succeeds: old owner never released the unit; lease did not fully move", movedKey)
	}
}

// TestLeaseHandoff_NodeLeaveSurvivorReacquires exercises the reconcile's
// ACQUIRE half (acquireUnit) on a membership LEAVE - the path the join case
// does not hit (a joiner mounts its share at Open via initMultiBackend). A
// 3-node cluster writes acked keys; one node leaves; the surviving nodes
// must RE-ACQUIRE the departed node's units via the reconcile, opening them
// at a HIGHER epoch (fencing) and serving the departed node's acked writes
// COPY-FREE from the shared backing. NO ACKED WRITE LOST across a leave.
func TestLeaseHandoff_NodeLeaveSurvivorReacquires(t *testing.T) {
	const unitCount = 16
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "lv1", "", unitCount, backing)
	n2 := startSharedNode(t, "lv2", n1.BindAddr, unitCount, backing)
	n3 := startSharedNode(t, "lv3", n1.BindAddr, unitCount, backing)
	all := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(all, 3, 15*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}

	// Write acked keys whose units are spread across all three nodes.
	const nKeys = 150
	keys := make([]string, 0, nKeys)
	for i := range nKeys {
		k := fmt.Sprintf("lv-%04d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, "v-"+k, 5*time.Second); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
		keys = append(keys, k)
	}

	// Identify the keys whose units n3 owns - those must re-home to a
	// survivor after n3 leaves.
	n3Keys := make([]string, 0)
	for _, k := range keys {
		if unitOwnerOnRing(n1.Cluster, k, unitCount) == n3.ID {
			n3Keys = append(n3Keys, k)
		}
	}
	if len(n3Keys) == 0 {
		t.Skip("no key's unit landed on n3 under this ring; leave-reacquire not exercised")
	}

	// n3 leaves gracefully (broadcasts the leave; survivors converge to 2).
	n3.Close()
	survivors := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(survivors, 2, 15*time.Second); err != nil {
		t.Fatalf("post-leave convergence: %v", err)
	}

	// Every acked key - INCLUDING the ones that were on n3 - must still be
	// readable through a survivor. The survivor RE-ACQUIRED n3's units via
	// the reconcile and reads them copy-free from the shared backing. Retry
	// to ride out the acquiring window.
	for _, k := range keys {
		want := []byte("v-" + k)
		got, err := getWithRetryUnavailable(t, n1.Cluster, k, 10*time.Second)
		if err != nil {
			t.Fatalf("acked key %q unreadable after n3 left: %v (NO ACKED WRITE LOST violated on leave)", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("acked key %q = %q, want %q after leave", k, got, want)
		}
	}

	// Concretely prove a survivor re-acquired (mounts) at least one of n3's
	// units: pick an n3 key and confirm its new ring owner serves it LOCALLY.
	probe := n3Keys[0]
	newOwner := n1
	if unitOwnerOnRing(n1.Cluster, probe, unitCount) == n2.ID {
		newOwner = n2
	}
	if !waitUntil(10*time.Second, func() bool {
		_, err := newOwner.Cluster.LocalGet([]byte(probe))
		return err == nil
	}) {
		t.Fatalf("survivor %s never re-acquired n3's unit for %q (reconcile acquire half not firing on leave)", newOwner.ID, probe)
	}
}

// putWithRetryUnavailable issues a Put, retrying on the retryable
// acquiring-window error (codes.Unavailable) until success or timeout. The
// handoff window is transient; an originator is expected to retry.
func putWithRetryUnavailable(t *testing.T, c *cluster.Cluster, key, val string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := c.Put([]byte(key), []byte(val))
		if err == nil {
			return nil
		}
		if st, _ := status.FromError(err); st.Code() != codes.Unavailable || time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// getWithRetryUnavailable issues a Get, retrying on the retryable
// acquiring-window error until success or timeout.
func getWithRetryUnavailable(t *testing.T, c *cluster.Cluster, key string, timeout time.Duration) ([]byte, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got, err := c.Get([]byte(key))
		if err == nil {
			return got, nil
		}
		if st, _ := status.FromError(err); st.Code() != codes.Unavailable || time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestLeaseHandoff_ForwardedOpToAcquiringOwnerRetryable pins the
// handoff-window behavior at the gRPC boundary: a forwarded op that lands on
// the RING OWNER of a unit that owner has NOT yet mounted returns a RETRYABLE
// error (codes.Unavailable), not a wrong result and not a permanent
// FailedPrecondition "refresh your ring" (the ring is already correct;
// refreshing would loop). We construct the window deterministically on a
// single node: the founder owns every unit on the ring, but we remove one
// unit from its mount via the test-only ClearMountForTest hook, leaving it
// owner-but-unmounted. A forwarded Put for that unit's key must come back
// codes.Unavailable.
func TestLeaseHandoff_ForwardedOpToAcquiringOwnerRetryable(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "win1", "", unitCount, backing)

	key := "window-key"
	u := storageunit.UnitForShardKey(ring.ShardKey([]byte(key)), storageunit.MustUnitCount(unitCount))

	// Founder is the ring owner of u (single node owns all). Drop u from the
	// mount map: now owner-but-unmounted, exactly the handoff window.
	n1.Cluster.TestingClearMount(u)

	conn, err := grpc.NewClient(n1.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial win1: %v", err)
	}
	defer func() { _ = conn.Close() }()
	api := pb.NewShaleNodeClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = api.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte("v"), Forwarded: true})
	if err == nil {
		t.Fatal("forwarded Put to an acquiring owner: expected a retryable error, got nil")
	}
	if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
		t.Fatalf("forwarded Put in handoff window: code = %v, want Unavailable (retryable). err=%v", st.Code(), err)
	}

	// A forwarded Get for the same window is likewise retryable, not a stale
	// / wrong result.
	_, gerr := api.Get(ctx, &pb.GetRequest{Key: []byte(key), Forwarded: true})
	if gerr == nil {
		t.Fatal("forwarded Get to an acquiring owner: expected a retryable error, got nil")
	}
	if st, _ := status.FromError(gerr); st.Code() != codes.Unavailable {
		t.Fatalf("forwarded Get in handoff window: code = %v, want Unavailable. err=%v", st.Code(), gerr)
	}
}
