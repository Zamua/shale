// Package integration drives shale's multi-node code paths end to end:
// real memberlist gossip over loopback UDP, real gRPC forwarding between
// in-process Cluster instances, real consistent-hash routing. The shared
// fixtures live here; each *_test.go file builds on them.
//
// Why a separate tests/ tree (vs adding to pkg/cluster/cluster_test.go):
// the cluster package's own tests cover the routing decision at the
// boundary; this tree covers the wired-together system. Failures here
// are integration failures (one node disagrees with another about a
// ring; a forwarded RPC drops on the floor; a leaving node leaves a
// stale client cached), which the per-package tests cannot see.
package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/clustertest"
	"github.com/Zamua/shale/internal/goleakignore"
	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
)

// TestMain shrinks the rebalance package's sweep tick + grace
// duration once for the whole integration test binary. Production
// stays at the package defaults (10s sweep, 60s grace); the
// integration tests need sub-second feedback so the sweep can fire
// within the per-test wall-clock budgets.
//
// Wraps goleak.VerifyTestMain so a regression that leaks goroutines
// out of a Cluster (events loop, sweep, fanout drainer, read-repair,
// etc.) surfaces as a test-binary leak at the end of the run. Known
// third-party background goroutines (memberlist gossip / probe /
// gRPC keepalive / etc.) are explicitly ignored: they're stable +
// well-behaved in production but their teardown is async + can
// flicker on a fast CI box.
//
// This file is helpers_test.go (not helpers.go) because TestMain is
// only honored in _test.go files (testNode + friends are test-only
// anyway).
func TestMain(m *testing.M) {
	// Share the ONE canonical ignore set with pkg/cluster's TestMain via
	// internal/goleakignore so the two lists can never drift. The drift
	// this closes was real: the integration list used to be a hand-copied
	// SUBSET that missed IgnoreAnyFunction for memberlist pushPullTrigger,
	// so an in-flight push-pull blocked in network I/O at teardown (its
	// top frame net/bufio, pushPullTrigger only deeper in the stack)
	// slipped past the IgnoreTopFunction form and failed the whole binary
	// intermittently.
	goleak.VerifyTestMain(m, goleakignore.Options()...)
}

// defaultTestUnitCount is the unit count every shared multi-node fixture
// opens with. 8 units over 2-3 nodes gives each node several units, so a
// membership change actually moves some without emptying anyone.
const defaultTestUnitCount = 8

// testNode is the bundle of state behind one node in an integration
// fixture: the cluster handle the test drives, the unit factory the test
// peers at to confirm physical placement, and the gRPC server that other
// nodes forward requests to. Tests almost always interact via the
// Cluster; the factory handle is escape-hatch only.
//
// Every multi-node fixture is multi-backend, because that is the only
// distributed model shale has: a key lives in a storage UNIT, and the
// factory is what opens units. "What does this node physically hold" is
// therefore a question about its MOUNTED UNITS, which is what
// physicalGet / physicalKeyCount answer.
type testNode struct {
	ID       string
	Cluster  *cluster.Cluster
	Handle   *sharedfactory.Handle
	BindAddr string
	GRPCAddr string

	stop       func()
	grpcServer *grpc.Server
}

// physicalGet reads key from whichever unit this node has MOUNTED,
// bypassing all routing. It is the multi-backend spelling of "peek under
// the cluster layer at this node's own bytes": a miss means no unit this
// node holds contains the key. Note this is a question about MOUNTS, not
// about the shared store: every node's Handle is a view onto the same
// backing, so a unit's bytes are only "this node's" while this node owns
// the lease.
func (n *testNode) physicalGet(key []byte) ([]byte, error) {
	if v, err := n.Cluster.LocalGet(key); err == nil {
		return v, nil
	}
	// LocalGet resolves ONE position per unit; at R>1 this node may hold the
	// key at a different position. Address each explicitly before concluding
	// the node does not have it.
	for u := range defaultTestUnitCount {
		gu := storageunit.NewGenUnit(0, storageunit.UnitID(u))
		for r := range maxTestReplicas {
			ru := storageunit.NewReplicaUnit(gu, uint8(r))
			v, err := n.Cluster.LocalReplicaGetAt(ru, key)
			if err == nil {
				return v, nil
			}
		}
	}
	return nil, backend.ErrNotFound
}

// physicalKeyCount totals the keys across every unit this node has mounted.
func (n *testNode) physicalKeyCount(t *testing.T) int {
	t.Helper()
	return n.physicalKeyCountPrefix(t, nil)
}

// physicalKeyCountPrefix totals the keys under prefix across every unit
// this node has mounted. A nil prefix counts everything.
func (n *testNode) physicalKeyCountPrefix(t *testing.T, prefix []byte) int {
	t.Helper()
	// A local scan can land mid-handoff, while a unit is being acquired by
	// this node. That is a TRANSIENT refusal with a defined retry, not a
	// count of zero, and callers here are asking "what does this node hold
	// once things settle" - so ride it out rather than reporting a number
	// that was never true.
	deadline := time.Now().Add(10 * time.Second)
	for {
		count, err := n.tryPhysicalKeyCountPrefix(prefix)
		if err == nil {
			return count
		}
		if !clustertest.IsTransientWarmupErr(err) || !time.Now().Before(deadline) {
			t.Fatalf("local scan on %s: %v", n.ID, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (n *testNode) tryPhysicalKeyCountPrefix(prefix []byte) (int, error) {
	keys, err := n.tryPhysicalKeysPrefix(prefix)
	if err != nil {
		return 0, err
	}
	return len(keys), nil
}

// tryPhysicalKeysPrefix returns the DISTINCT keys under prefix that this node
// physically holds, across every replica position it has mounted.
//
// LocalScanPrefix alone is not the right instrument at R>1: it resolves ONE
// backend per unit (the position this node reads at), so a node that holds a
// different position of some other unit reports those keys as absent. The
// symptom is a physical count that looks like a PARTITION of the keyspace on a
// cluster where every node should hold everything. Addressing each replica
// position explicitly, and unioning, is what "what does this node physically
// hold" actually means when a unit has R independent databases.
//
// Positions this node does not hold return the retryable acquiring error,
// which is skipped rather than propagated: not holding a position is the
// normal case, not a failure.
func (n *testNode) tryPhysicalKeysPrefix(prefix []byte) (map[string]struct{}, error) {
	keys := make(map[string]struct{})
	for u := range defaultTestUnitCount {
		gu := storageunit.NewGenUnit(0, storageunit.UnitID(u))
		for r := range maxTestReplicas {
			ru := storageunit.NewReplicaUnit(gu, uint8(r))
			it, err := n.Cluster.LocalReplicaScanAt(ru, prefix)
			if err != nil {
				// Not mounted here (or mid-acquire): nothing to count.
				continue
			}
			for {
				k, _, nerr := it.Next()
				if nerr != nil {
					_ = it.Close()
					return nil, nerr
				}
				if k == nil {
					break
				}
				keys[string(k)] = struct{}{}
			}
			_ = it.Close()
		}
	}
	return keys, nil
}

// maxTestReplicas bounds the replica-position sweep above. No fixture in this
// tree runs above R=3, and probing a position no fixture uses costs one
// cheap not-mounted answer.
const maxTestReplicas = 3

// Close tears the node down: shuts the gRPC server, closes the cluster
// (which closes membership + releases its unit leases). Safe to call once.
func (n *testNode) Close() {
	if n.Cluster != nil {
		_ = n.Cluster.Close()
		n.Cluster = nil
	}
	if n.stop != nil {
		n.stop()
		n.stop = nil
	}
}

// startTestNode brings up one cluster node with its own memberlist +
// gRPC endpoint, joined to the given seed (empty = first node). The
// returned node registers itself for cleanup via t.Cleanup so tests
// that forget to Close it explicitly still tear down.
//
// Port allocation: the gRPC listener binds 127.0.0.1:0 directly (held
// open, no rebind gap). The memberlist BindAddr is allocated through
// openClusterRetryBind, which re-rolls a fresh freePort and retries if
// the release-then-rebind window lets another listener grab the port -
// so a port collision under a stress loop self-heals instead of failing
// the test.
func startTestNode(t *testing.T, id, seedAddr string) *testNode {
	return startTestNodeWithReplication(t, id, seedAddr, 1, 0, 0)
}

// fixtureBacking is the ONE shared durable store every node started by
// startTestNode / startTestNodeWithReplication opens its units against.
//
// This is not a convenience: it is what makes the fixture a faithful
// multi-backend cluster. A unit handoff moves the LEASE, not the bytes, so
// the successor must open the SAME store the predecessor wrote to. Giving
// each node its own factory would make every handoff silently lose the
// unit's data, and the tests would pass or fail for reasons that have
// nothing to do with what they assert.
//
// Keyed per test so parallel tests cannot see each other's units.
var fixtureBackings sync.Map // *testing.T -> *sharedfactory.Backing

func fixtureBacking(t *testing.T) *sharedfactory.Backing {
	t.Helper()
	// Fixtures build nodes from subtests as well as the top-level test, so
	// key on the ROOT test the nodes belong to via t.Name's first segment.
	root := t.Name()
	if i := strings.IndexByte(root, '/'); i >= 0 {
		root = root[:i]
	}
	if v, ok := fixtureBackings.Load(root); ok {
		return v.(*sharedfactory.Backing)
	}
	b := sharedfactory.NewBacking()
	actual, loaded := fixtureBackings.LoadOrStore(root, b)
	if !loaded {
		t.Cleanup(func() { fixtureBackings.Delete(root) })
	}
	return actual.(*sharedfactory.Backing)
}

// startTestNodeWithReplication is the replication-aware variant. R=1
// + zero-valued consistency knobs reproduce startTestNode exactly (the
// Cluster's normalizeConfig fills in WriteQuorum + ReadNearest at
// Open). Used by tests/integration/replicate_*_test.go to stand up
// clusters with R>1 + tunable W/R.
func startTestNodeWithReplication(t *testing.T, id, seedAddr string, replicationFactor int, wc cluster.WriteConsistency, rc cluster.ReadConsistency) *testNode {
	t.Helper()

	h := fixtureBacking(t).Handle()

	// The gRPC listener has to exist BEFORE cluster.Open so we know the
	// address to advertise via memberlist Meta. Same two-phase pattern
	// shaled's main.go uses.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startTestNode %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:         id,
		BackendFactory: h,
		UnitCount:      storageunit.MustUnitCount(defaultTestUnitCount),
		GRPCAddr:       grpcAddr,
		LogOutput:      io.Discard,
		// Shrunken settle delay keeps integration tests snappy without
		// changing protocol semantics: the unit reconcile kicks fast on
		// a membership change instead of waiting the 5s production
		// debounce.
		RebalanceSettleDelay: 500 * time.Millisecond,
		ReplicationFactor:    replicationFactor,
		WriteConsistency:     wc,
		ReadConsistency:      rc,
	}
	gcfg := gossip.Config{LogOutput: io.Discard}
	if seedAddr != "" {
		gcfg.Seeds = []string{seedAddr}
	}

	// openClusterRetryBind fills in the coordinator's bind address (re-rolling
	// a fresh port and retrying if memberlist hits the release-rebind port
	// race) and returns the address actually bound, which the node advertises
	// as its seed.
	c, bindAddr := openClusterRetryBind(t, cfg, gcfg)

	rpc.NewServer(c).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &testNode{
		ID:         id,
		Cluster:    c,
		Handle:     h,
		BindAddr:   bindAddr,
		GRPCAddr:   grpcAddr,
		grpcServer: grpcSrv,
		stop: func() {
			grpcSrv.GracefulStop()
			<-serveDone
		},
	}
	t.Cleanup(n.Close)
	return n
}

// waitForMembersAll delegates to the shared harness package. See
// internal/clustertest.
func waitForMembersAll(cs []*cluster.Cluster, want int, timeout time.Duration) error {
	return clustertest.WaitForMembersAll(cs, want, timeout)
}

// ownerOf is the test-side mirror of cluster.ownerOf. Two rings built
// from the same Members() snapshot agree (consistent hashing is
// deterministic + hashed on Member.ID), so this faithfully previews
// which node a Put/Get would route to.
// ownerOf names the node the ring places key's UNIT on. Routing is
// key -> unit -> owner, so hashing the raw key against the ring (the
// per-node model) would name a different node whenever the two disagree.
// It mirrors the cluster's own lookup without reaching into unexported
// internals; unitOwnerID is the same computation with an explicit unit
// count.
func ownerOf(c *cluster.Cluster, key string) string {
	return unitOwnerID(c, key, defaultTestUnitCount)
}

// freePort, isBindConflict and openClusterRetryBind delegate to the
// shared harness package so the port-probing + bind-retry strategy
// cannot drift from pkg/cluster's external tests. See
// internal/clustertest.
func freePort(t *testing.T) int {
	t.Helper()
	return clustertest.FreePort(t)
}

func isBindConflict(err error) bool {
	return clustertest.IsBindConflict(err)
}

func openClusterRetryBind(t *testing.T, cfg cluster.Config, gcfg gossip.Config, forbiddenPorts ...int) (*cluster.Cluster, string) {
	t.Helper()
	return clustertest.OpenClusterRetryBind(t, cfg, gcfg, forbiddenPorts...)
}

// bindPortOf extracts the port from a "host:port" bind address. Used to
// forbid a just-killed node's port when starting its replacement.
func bindPortOf(t *testing.T, addr string) int {
	t.Helper()
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("bindPortOf %q: %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("bindPortOf %q: parse port: %v", addr, err)
	}
	return port
}

func hostPort(port int) string {
	return clustertest.HostPort(port)
}

// startReplicatedCluster brings up `count` nodes joined into one
// cluster, each configured with the supplied replication factor +
// consistency knobs. Waits for ring convergence AND bootstrap-rebalance
// quiescence on every node before returning, so tests can start writing
// against a fully-settled cluster. Cleanup is wired via t.Cleanup on
// each node.
//
// The two-phase wait matters: ring convergence (every node sees N
// members) happens quickly under SWIM gossip, but the rebalance
// Coordinator on each node debounces membership events for
// RebalanceSettleDelay (500ms in the integration fixture) before
// evaluating + dispatching the migrations the joins triggered. If we
// return after only ring convergence, the first Put in a test can land
// on a partition whose ownership is mid-move - the source sends back
// ResourceExhausted ("key is migrating out") + the test logs as a
// flake. Waiting for WaitForRebalanceIdle on every node closes that
// window.
func startReplicatedCluster(t *testing.T, count, replicationFactor int, wc cluster.WriteConsistency, rc cluster.ReadConsistency) []*testNode {
	t.Helper()
	if count < 1 {
		t.Fatalf("startReplicatedCluster: count must be >= 1")
	}
	nodes := make([]*testNode, 0, count)
	seed := ""
	for i := range count {
		n := startTestNodeWithReplication(t, fmt.Sprintf("rn%d", i+1), seed, replicationFactor, wc, rc)
		if i == 0 {
			seed = n.BindAddr
		}
		nodes = append(nodes, n)
	}
	cs := make([]*cluster.Cluster, len(nodes))
	for i, n := range nodes {
		cs[i] = n.Cluster
	}
	waitForClusterReady(t, cs, 45*time.Second)
	// Final gate: prove the routed forwarding RPC path actually
	// round-trips at the configured WriteConsistency before any test
	// issues its seed Put. waitForClusterReady proves membership +
	// rebalance quiescence, but NOT that every peer's gRPC server
	// goroutine is servicing connections yet (the listener is bound
	// eagerly but Serve runs in a goroutine that can lag under CI
	// starvation). That gap is what produces the "write needed N acks,
	// got M" flake. The probe write through nodes[0].Cluster drives the
	// same routed + replicated path the tests use; its retry is scoped to
	// THIS setup window only.
	waitForWriteReady(t, cs, 45*time.Second)
	return nodes
}

// waitForWriteReady and isTransientWarmupErr delegate to the shared
// harness package so this tree and pkg/cluster's external tests gate on
// identical readiness semantics. See internal/clustertest.
func waitForWriteReady(t *testing.T, clusters []*cluster.Cluster, deadline time.Duration) {
	t.Helper()
	clustertest.WaitForWriteReady(t, clusters, deadline)
}

// waitForClusterReady is the canonical "the fixture is done settling"
// gate. Tests call this right after spinning up + joining all nodes,
// before issuing any Put/Get that depends on stable ownership. It
// guards against the integration-suite flake class where the first
// write into a freshly-joined cluster hits a partition mid-migration
// or a node that hasn't yet noticed its new peers.
//
// Two-phase wait:
//
//  1. Ring convergence: every node's Members() reports len(clusters)
//     entries. Under SWIM gossip this is sub-second on loopback but
//     not synchronous, so a multi-node assertion that fires before
//     convergence races.
//  2. Rebalance quiescence: every Coordinator's WaitForRebalanceIdle
//     returns. This closes the debounced-Evaluate -> Send -> Receive
//     -> sweep cycle that fires on each Notify*Join, so the in-flight
//     bootstrap migrations + their grace-window sweeps are all
//     complete before the test starts writing. WaitForIdle is
//     deterministic across the debounce window: a scheduled-but-unrun
//     evaluation counts as not-idle, so it blocks until the evaluation
//     fires and drains rather than returning a premature "idle".
//
// The deadline argument is the wall-clock budget for BOTH steps; each
// phase consumes part of it. On timeout we fail with a clear per-phase
// diagnostic so a real cluster bug is easy to spot vs a transient
// slow-CI hiccup.
func waitForClusterReady(t *testing.T, clusters []*cluster.Cluster, deadline time.Duration) {
	t.Helper()
	start := time.Now()
	if err := waitForMembersAll(clusters, len(clusters), deadline); err != nil {
		t.Fatalf("waitForClusterReady: ring convergence: %v", err)
	}
	// WaitForRebalanceIdle is now deterministic across the debounce
	// window: a debounce-scheduled-but-unrun evaluation counts as
	// not-idle (settlePending > 0), so calling WaitForIdle before the
	// settle timer fires blocks until the evaluation has fired AND
	// drained rather than returning a premature "idle". The old
	// pre-idle settle sleep + post-idle drain sleep are no longer
	// needed: WaitForIdle alone closes the debounced-Evaluate -> Send
	// -> Receive -> sweep cycle.
	remaining := deadline - time.Since(start)
	if remaining <= 0 {
		t.Fatalf("waitForClusterReady: ring convergence consumed the entire %s budget", deadline)
	}
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	defer cancel()
	for i, c := range clusters {
		if err := c.WaitForRebalanceIdle(ctx); err != nil {
			t.Fatalf("waitForClusterReady: rebalance idle wait on cluster %d (%s): %v", i, c.NodeID(), err)
		}
	}
}
