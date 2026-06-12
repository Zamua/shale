package integration

// End-to-end coverage for v0.8 DECLARATIVE resharding. The trigger is
// desired-state config (--unit-count = the DESIRED count), not an operator RPC:
// the operator bumps the desired count and rolls the deploy, and the cluster
// reshards ITSELF to match via the deterministic coordinator's reconcile loop.
//
// These tests stand up real in-process multi-node clusters over gRPC +
// memberlist + a shared sharedfactory.Backing (the test analogue of shared
// object storage), so the coordinator selection, the desired-count gossip, AND
// the actual freeze-barrier reshard all run end to end - the full path the
// pure-function decision tests in pkg/cluster cannot reach.
//
// THE KEY SETUP: to make the live count DIFFER from the desired count (the whole
// point of declarative resharding - a founder at desired=4 must derive live=2
// from durable state, not re-init at 4 and orphan the existing data), the shared
// backing is PRE-SEEDED with a gen-0 unit set of the LIVE size before any node
// opens. PresentUnits then reports that set, so deriveGenerationFromDurableState
// commits live=2 while cfg.UnitCount=4 is broadcast as the desired.

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
)

// preSeedDurableLiveCount registers the gen-0 unit set {0..liveCount-1} in the
// shared backing so a founder over it derives live = liveCount from durable
// state (rather than re-initializing at the desired --unit-count). Opening each
// unit on a throwaway handle is what creates its durable entry; PresentUnits
// enumerates exactly that set.
func preSeedDurableLiveCount(t *testing.T, backing *sharedfactory.Backing, liveCount int) {
	t.Helper()
	h := backing.Handle()
	for id := 0; id < liveCount; id++ {
		gu := storageunit.NewGenUnit(0, storageunit.UnitID(id))
		be, err := h.OpenUnit(gu, 1)
		if err != nil {
			t.Fatalf("preSeedDurableLiveCount: OpenUnit %s: %v", gu, err)
		}
		_ = be.Close()
		_ = h.CloseUnit(gu)
	}
}

// startDesiredNode brings up one multi-backend node whose --unit-count (the
// DESIRED count, broadcast via Meta) is desiredCount. It is otherwise identical
// to startSharedNode: a per-node handle over the shared backing, wired over real
// gRPC + memberlist. The node's LIVE count comes from durable-state derivation
// (or, for a fresh backing, equals desiredCount). seedAddr empty = founder.
func startDesiredNode(t *testing.T, id, seedAddr string, desiredCount int, backing *sharedfactory.Backing) *sharedNode {
	t.Helper()

	h := backing.Handle()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startDesiredNode %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:               id,
		BackendFactory:       h,
		UnitCount:            storageunit.MustUnitCount(desiredCount),
		GRPCAddr:             grpcAddr,
		LogOutput:            io.Discard,
		RebalanceSettleDelay: 300 * time.Millisecond,
	}
	if seedAddr != "" {
		cfg.Seeds = []string{seedAddr}
	}

	c, bindAddr := openClusterRetryBind(t, cfg)

	rpc.NewServer(c).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &sharedNode{
		ID:       id,
		Cluster:  c,
		Handle:   h,
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

// liveCountOf reads a node's live routed unit count off GenStateSnapshot.
func liveCountOf(n *sharedNode) uint32 {
	_, count := n.Cluster.GenStateSnapshot()
	return count
}

// liveGenOf reads a node's live generation off GenStateSnapshot.
func liveGenOf(n *sharedNode) uint64 {
	gen, _ := n.Cluster.GenStateSnapshot()
	return gen
}

// TestDeclarativeReshard_BumpDesiredSelfReshardsOnce is THE declarative gate: a
// 2-node cluster running at live=2 whose nodes both DESIRE 4 reshards ITSELF
// exactly once to gen=1/count=4 - no operator RPC, driven purely by the
// coordinator's reconcile loop once membership settles and the desired count is
// unanimous. Both nodes converge on the new generation.
func TestDeclarativeReshard_BumpDesiredSelfReshardsOnce(t *testing.T) {
	backing := sharedfactory.NewBacking()
	preSeedDurableLiveCount(t, backing, 2) // founder derives live=2

	n1 := startDesiredNode(t, "dr-a", "", 4, backing)          // desired=4 (coordinator)
	n2 := startDesiredNode(t, "dr-b", n1.BindAddr, 4, backing) // desired=4

	// Founder routes at the derived live count, not the desired.
	if got := liveCountOf(n1); got != 2 {
		t.Fatalf("founder live count = %d, want 2 (derived from durable state, NOT the desired 4)", got)
	}

	// Wait for unanimity on desired=4 across both members.
	if !waitUntil(5*time.Second, func() bool {
		c, ok := n1.Cluster.TestingUnanimousDesiredCount()
		return ok && c == 4
	}) {
		t.Fatalf("cluster never reached unanimous desired=4")
	}

	// The coordinator's reconcile loop self-reshards to count=4 on both nodes.
	if !waitUntil(20*time.Second, func() bool {
		return liveCountOf(n1) == 4 && liveCountOf(n2) == 4
	}) {
		t.Fatalf("cluster did not self-reshard to count=4: n1=%d n2=%d", liveCountOf(n1), liveCountOf(n2))
	}

	// Exactly ONE doubling: gen 0 -> 1, count 2 -> 4 (not 8).
	if g := liveGenOf(n1); g != 1 {
		t.Fatalf("after self-reshard, gen = %d, want 1 (exactly one doubling)", g)
	}
	if c := liveCountOf(n1); c != 4 {
		t.Fatalf("after self-reshard, count = %d, want 4 (exactly one doubling, not 8)", c)
	}

	// Stable: desired==current==4 is now a no-op; it must not keep resharding.
	time.Sleep(1500 * time.Millisecond)
	if c := liveCountOf(n1); c != 4 {
		t.Fatalf("cluster over-resharded past desired: count = %d, want 4", c)
	}
	if g := liveGenOf(n1); g != 1 {
		t.Fatalf("cluster advanced past one doubling: gen = %d, want 1", g)
	}
}

// TestDeclarativeReshard_MixedDesiredDoesNotReshard pins guard 2 end to end: one
// node disagreeing on the desired count (it still wants 2 while the other wants
// 4) blocks the self-reshard. The cluster stays at live=2. This is the rolling-
// deploy safety property: a half-rolled cluster never reshards.
func TestDeclarativeReshard_MixedDesiredDoesNotReshard(t *testing.T) {
	backing := sharedfactory.NewBacking()
	preSeedDurableLiveCount(t, backing, 2)

	n1 := startDesiredNode(t, "dm-a", "", 4, backing)          // desired=4 (coordinator)
	n2 := startDesiredNode(t, "dm-b", n1.BindAddr, 2, backing) // desired=2 (disagrees)

	// Wait for both members to be visible to the coordinator.
	if !waitUntil(5*time.Second, func() bool {
		return len(n1.Cluster.Members()) == 2
	}) {
		t.Fatalf("membership did not converge to 2 members")
	}

	// Desired is NOT unanimous: the coordinator never sees agreement.
	if _, ok := n1.Cluster.TestingUnanimousDesiredCount(); ok {
		t.Fatalf("coordinator saw unanimous desired with mixed (4 vs 2) counts")
	}

	// Give the reconcile loop ample passes to (not) act. Count stays at 2.
	time.Sleep(2 * time.Second)
	if c := liveCountOf(n1); c != 2 {
		t.Fatalf("cluster self-resharded under MIXED desired: count = %d, want 2", c)
	}
	if c := liveCountOf(n2); c != 2 {
		t.Fatalf("node 2 count = %d, want 2 (no reshard)", c)
	}
}

// TestDeclarativeReshard_DesiredEqualsCurrentIsNoOp pins guard 3 end to end: a
// cluster already AT its desired count (live=2, desired=2) never reshards.
func TestDeclarativeReshard_DesiredEqualsCurrentIsNoOp(t *testing.T) {
	backing := sharedfactory.NewBacking()
	// Fresh backing: founder establishes live=2 directly from desired=2, so
	// desired == current from the start.
	n1 := startDesiredNode(t, "de-a", "", 2, backing)
	n2 := startDesiredNode(t, "de-b", n1.BindAddr, 2, backing)

	if !waitUntil(5*time.Second, func() bool {
		c, ok := n1.Cluster.TestingUnanimousDesiredCount()
		return ok && c == 2 && len(n1.Cluster.Members()) == 2
	}) {
		t.Fatalf("cluster never reached unanimous desired=2 across 2 members")
	}

	// Drive the decision directly + over the loop cadence: it stays at gen 0.
	n1.Cluster.TestingReconcileReshard()
	time.Sleep(1500 * time.Millisecond)
	if g := liveGenOf(n1); g != 0 {
		t.Fatalf("desired==current triggered a reshard: gen = %d, want 0", g)
	}
	if c := liveCountOf(n1); c != 2 {
		t.Fatalf("desired==current changed the count: count = %d, want 2", c)
	}
	_ = n2
}

// TestDeclarativeReshard_UnstableMembershipReshardsExactlyOnce pins the
// debounce + idempotency interplay (guard 1): membership churns (a THIRD node
// joins after the cluster is up), but the cluster still ends up resharded
// EXACTLY ONCE to count=4, never twice. The settle debounce holds the reshard
// off until membership stops changing (so it never fires mid-join against a
// transient snapshot), and the idempotency (desired==current after the first
// doubling) prevents a second doubling once the join settles. All three nodes
// agree on desired=4 throughout, so the only variable is membership stability.
func TestDeclarativeReshard_UnstableMembershipReshardsExactlyOnce(t *testing.T) {
	backing := sharedfactory.NewBacking()
	preSeedDurableLiveCount(t, backing, 2)

	n1 := startDesiredNode(t, "du-a", "", 4, backing)          // coordinator
	n2 := startDesiredNode(t, "du-b", n1.BindAddr, 4, backing) // joins immediately

	// Wait for the 2-node cluster to converge + self-reshard to count=4.
	if !waitUntil(20*time.Second, func() bool {
		return liveCountOf(n1) == 4 && liveCountOf(n2) == 4
	}) {
		t.Fatalf("initial 2-node cluster did not reshard to count=4")
	}
	if g := liveGenOf(n1); g != 1 {
		t.Fatalf("after first reshard, gen = %d, want 1", g)
	}

	// Now perturb membership: a THIRD node joins, also desiring 4. Membership
	// churns; the coordinator re-evaluates once it settles. Because the cluster
	// is ALREADY at count=4 (== desired), the re-evaluation is a no-op: no
	// second doubling. The join itself triggers a unit reconcile (lease
	// handoff), which is orthogonal to the reshard decision.
	n3 := startDesiredNode(t, "du-c", n1.BindAddr, 4, backing)
	if !waitUntil(10*time.Second, func() bool {
		return len(n1.Cluster.Members()) == 3 && liveCountOf(n3) == 4
	}) {
		t.Fatalf("third node did not converge into the cluster at count=4")
	}

	// Let the reconcile loop run several passes across the membership change.
	// The cluster must stay at EXACTLY gen=1/count=4 - the debounce prevented a
	// mid-churn reshard and the idempotency prevented a redundant second one.
	time.Sleep(2 * time.Second)
	for _, n := range []*sharedNode{n1, n2, n3} {
		if c := liveCountOf(n); c != 4 {
			t.Fatalf("node %s count = %d, want 4 (no second reshard under churn)", n.ID, c)
		}
		if g := liveGenOf(n); g != 1 {
			t.Fatalf("node %s gen = %d, want 1 (exactly one doubling total)", n.ID, g)
		}
	}
}

// TestDeclarativeReshard_OnlyCoordinatorDrives pins the deterministic-
// coordinator rule end to end: the lowest-id node is the coordinator and the
// only one whose reconcile loop drives a reshard. We assert (a) exactly one node
// reports itself coordinator, and (b) driving the NON-coordinator's decision is
// a no-op even though every other guard holds - so two nodes never both kick off
// a barrier (no double-reshard / no racing freezes).
func TestDeclarativeReshard_OnlyCoordinatorDrives(t *testing.T) {
	backing := sharedfactory.NewBacking()
	preSeedDurableLiveCount(t, backing, 2)

	// "dc-a" < "dc-b": dc-a is the coordinator.
	n1 := startDesiredNode(t, "dc-a", "", 4, backing)
	n2 := startDesiredNode(t, "dc-b", n1.BindAddr, 4, backing)

	if !waitUntil(5*time.Second, func() bool {
		return len(n1.Cluster.Members()) == 2 && len(n2.Cluster.Members()) == 2
	}) {
		t.Fatalf("membership did not converge to 2 members on both nodes")
	}

	// Exactly one coordinator, and it is the lowest-id node.
	if !n1.Cluster.TestingIsReshardCoordinator() {
		t.Fatalf("lowest-id node dc-a should be the coordinator")
	}
	if n2.Cluster.TestingIsReshardCoordinator() {
		t.Fatalf("higher-id node dc-b should NOT be the coordinator")
	}

	// Driving the NON-coordinator's decision must not advance its generation,
	// even though desired (4) > current (2). The coordinator gate is the only
	// thing stopping dc-b. (The real reshard, once it happens via dc-a's loop,
	// would advance BOTH; we capture dc-b's gen BEFORE that converges.)
	beforeGen := liveGenOf(n2)
	for i := 0; i < 3; i++ {
		n2.Cluster.TestingReconcileReshard()
	}
	if g := liveGenOf(n2); g != beforeGen {
		t.Fatalf("non-coordinator dc-b drove a reshard on its own: gen %d -> %d", beforeGen, g)
	}
}
