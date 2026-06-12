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
	"testing"
	"time"

	"github.com/Zamua/shale/internal/goleakignore"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
// only honored in _test.go files; an earlier helpers.go landed the
// setter where go test silently ignored it, leaving every sweep
// running at the 10s production interval + masking timing bugs in
// rebalance tests. Renaming the file fixes that without changing
// content (testNode + friends are test-only anyway).
func TestMain(m *testing.M) {
	rebalance.SetSweepInterval(50 * time.Millisecond)
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

// testNode is the bundle of state behind one node in an integration
// fixture: the cluster handle the test drives, the in-memory backend
// the test peers at to confirm physical placement, and the gRPC server
// that other nodes forward requests to. Tests almost always interact
// via the Cluster; the backend handle is escape-hatch only.
type testNode struct {
	ID       string
	Cluster  *cluster.Cluster
	Backend  *memory.Memory
	BindAddr string
	GRPCAddr string

	stop       func()
	grpcServer *grpc.Server
}

// Close tears the node down: shuts the gRPC server, closes the cluster
// (which closes membership + backend). Safe to call once.
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

// startBlockedDestTestNode brings up a node identical to startTestNode
// but with TestingBlockPeerDials enabled BEFORE Open returns. The block
// has to be in place before the bootstrap Evaluate (which runs inline
// inside Open for any joiner with visible peers) so the destination's
// FetchRange goroutines hit the closed-fd path on their very first
// clientFor call. Used by the destination-crash failure tests to make
// the wired-handoff assertion robust to fast loopbacks: post-Open
// setters race the FetchRange goroutines and let some streams complete
// before the block engages.
func startBlockedDestTestNode(t *testing.T, id, seedAddr string) *testNode {
	t.Helper()

	mem := memory.New()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startBlockedDestTestNode %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:                  id,
		Backend:                 mem,
		GRPCAddr:                grpcAddr,
		LogOutput:               io.Discard,
		RebalanceSettleDelay:    500 * time.Millisecond,
		RebalanceGraceDuration:  3 * time.Second,
		RebalanceHandoffTimeout: 4 * time.Second,
		ReplicationFactor:       1,
		TestingBlockPeerDials:   true,
	}
	if seedAddr != "" {
		cfg.Seeds = []string{seedAddr}
	}

	// openClusterRetryBind sets cfg.BindAddr (re-rolling a fresh port and
	// retrying if memberlist hits the release-rebind port race) and returns
	// the address actually bound, which the node advertises as its seed.
	c, bindAddr := openClusterRetryBind(t, cfg)

	rpc.NewServer(c).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &testNode{
		ID:         id,
		Cluster:    c,
		Backend:    mem,
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

// startTestNodeWithReplication is the replication-aware variant. R=1
// + zero-valued consistency knobs reproduce startTestNode exactly (the
// Cluster's normalizeConfig fills in WriteQuorum + ReadNearest at
// Open). Used by tests/integration/replicate_*_test.go to stand up
// clusters with R>1 + tunable W/R.
func startTestNodeWithReplication(t *testing.T, id, seedAddr string, replicationFactor int, wc cluster.WriteConsistency, rc cluster.ReadConsistency) *testNode {
	t.Helper()

	mem := memory.New()

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
		NodeID:    id,
		Backend:   mem,
		GRPCAddr:  grpcAddr,
		LogOutput: io.Discard,
		// Shrunken rebalance tunables: keep integration tests
		// snappy without changing protocol semantics. Settle delay
		// is short so the eval kicks fast on membership changes;
		// grace is long enough for the destination's pull to
		// complete (a few hundred keys over loopback gRPC), then
		// the sweep fires + the post-test assertions see clean
		// per-node ownership. HandoffTimeout shrunk from the
		// 5-minute default so the failure-mode test that wedges a
		// source-side runSend (destination never asks) fails fast
		// + the integration suite stays under its wall-clock
		// budget; production keeps the wide default.
		RebalanceSettleDelay:    500 * time.Millisecond,
		RebalanceGraceDuration:  3 * time.Second,
		RebalanceHandoffTimeout: 4 * time.Second,
		ReplicationFactor:       replicationFactor,
		WriteConsistency:        wc,
		ReadConsistency:         rc,
	}
	if seedAddr != "" {
		cfg.Seeds = []string{seedAddr}
	}

	// openClusterRetryBind sets cfg.BindAddr (re-rolling a fresh port and
	// retrying if memberlist hits the release-rebind port race) and returns
	// the address actually bound, which the node advertises as its seed.
	c, bindAddr := openClusterRetryBind(t, cfg)

	rpc.NewServer(c).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &testNode{
		ID:         id,
		Cluster:    c,
		Backend:    mem,
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

// waitForMembersAll polls every cluster in cs until they each report
// want members. Convergence under SWIM gossip is not synchronous - a
// fresh join can reach the seed before reaching the other peers - so
// any multi-node assertion must wait for every node to agree on the
// ring before issuing routed ops.
func waitForMembersAll(cs []*cluster.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allOK := true
		for _, c := range cs {
			if len(c.Members()) != want {
				allOK = false
				break
			}
		}
		if allOK {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	sizes := make([]int, len(cs))
	for i, c := range cs {
		sizes[i] = len(c.Members())
	}
	return fmt.Errorf("not all nodes converged to %d members: sizes=%v", want, sizes)
}

// ownerOf is the test-side mirror of cluster.ownerOf. Two rings built
// from the same Members() snapshot agree (consistent hashing is
// deterministic + hashed on Member.ID), so this faithfully previews
// which node a Put/Get would route to.
func ownerOf(c *cluster.Cluster, key string) string {
	r := ring.New()
	for _, m := range c.Members() {
		r.Add(m)
	}
	return r.LocateKey([]byte(key)).ID
}

// freePort grabs an OS-assigned ephemeral TCP port + releases the
// listener so the caller can bind it for real. There is a real race
// between release and rebind: between freePort closing the probe
// listener and memberlist re-binding the same number, another node's
// freePort (or the OS) can take it, and memberlist binds BOTH TCP and
// UDP, so even this TCP-only probe cannot reserve the UDP side. Under a
// stress loop that bind-conflict surfaces as "address already in use"
// out of memberlist.Create (~1/25 iterations observed). Callers that
// bind this for a Cluster must go through openClusterRetryBind, which
// re-rolls a fresh port and retries on exactly that conflict.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// isBindConflict reports whether err is a memberlist.Create failure
// caused by another listener already holding the bind port (the
// release-then-rebind race freePort cannot close on its own). memberlist
// wraps the OS bind error as "failed to start TCP/UDP listener on ...:
// bind: address already in use"; we match the stable substring rather
// than reaching for syscall.EADDRINUSE so the check survives the wrap.
func isBindConflict(err error) bool {
	return err != nil && strings.Contains(err.Error(), "address already in use")
}

// openClusterRetryBind opens a Cluster, retrying on a memberlist bind
// conflict with a FRESH port each time. cfg.BindAddr is replaced with a
// new freePort allocation on every attempt (including the first, so the
// caller does not have to seed it), and the BindAddr actually bound is
// returned so the node fixture advertises the right seed address. This
// waits on a REAL condition (a successful bind), not a sleep: it loops
// only while memberlist reports the port is taken, and fails loudly if
// every attempt in the bounded budget conflicts (which would indicate a
// genuine port-exhaustion problem, not the transient release-rebind
// race). A non-bind error (bad config, join failure) returns
// immediately - those are real test failures, not flakes to retry.
func openClusterRetryBind(t *testing.T, cfg cluster.Config) (*cluster.Cluster, string) {
	t.Helper()
	const maxAttempts = 8
	var lastErr error
	for range maxAttempts {
		bindAddr := hostPort(freePort(t))
		cfg.BindAddr = bindAddr
		c, err := cluster.Open(cfg)
		if err == nil {
			return c, bindAddr
		}
		if !isBindConflict(err) {
			t.Fatalf("openClusterRetryBind %s: cluster.Open: %v", cfg.NodeID, err)
		}
		lastErr = err
	}
	t.Fatalf("openClusterRetryBind %s: %d bind attempts all hit a port conflict: %v", cfg.NodeID, maxAttempts, lastErr)
	return nil, ""
}

func hostPort(port int) string {
	return "127.0.0.1:" + strconv.Itoa(port)
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
	waitForClusterReady(t, cs, 15*time.Second)
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
	waitForWriteReady(t, nodes[0].Cluster, 15*time.Second)
	return nodes
}

// waitForWriteReady gates a freshly-stood-up multi-node cluster on the
// one condition waitForClusterReady does NOT cover: that the routed
// forwarding RPC path actually round-trips at the cluster's configured
// WriteConsistency. Ring convergence proves every node is a MEMBER; it
// does NOT prove every peer's gRPC HTTP/2 server goroutine is servicing
// connections yet. Under CI starvation (-race + GOMAXPROCS contention +
// full-workspace parallelism) the Serve goroutine can lag the listener
// bind, so the seed Put fans out to a replica whose server isn't
// accepting yet and the write can't reach quorum acks ("write needed N
// acks, got M").
//
// The gate issues a PROBE write through the public Cluster.Put path -
// the exact same routed + replicated path the tests use - and retries
// ONLY on transient warmup errors (Unavailable: refused dial / not
// enough acks; ResourceExhausted: a partition still mid-migration) until
// it succeeds or the bounded deadline expires. A non-transient error, or
// exhausting the deadline, fails the test with a clear message.
//
// CRUCIAL: this retry is scoped to SETUP. Once the probe succeeds the
// cluster is proven write-ready, and any Unavailable a test then sees in
// the body under assertion is a REAL failure - this helper never wraps
// those. The probe key is namespaced + tombstoned afterward so it cannot
// pollute a test that reads replica backends directly.
func waitForWriteReady(t *testing.T, c *cluster.Cluster, deadline time.Duration) {
	t.Helper()
	const probeKey = "__warmup_probe__"
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		err := c.Put([]byte(probeKey), []byte("ready"))
		if err == nil {
			// Best-effort cleanup so no stray probe key lands on the
			// replica backends a test might read directly.
			_ = c.Delete([]byte(probeKey))
			return
		}
		if !isTransientWarmupErr(err) {
			t.Fatalf("waitForWriteReady: probe write returned a non-transient error (real failure, not a warmup race): %v", err)
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("waitForWriteReady: cluster never became write-ready within %s; last probe error: %v", deadline, lastErr)
}

// isTransientWarmupErr reports whether err is one of the transient
// signals expected while a freshly-joined cluster's gRPC forwarding
// servers come up: Unavailable (peer's server not accepting yet /
// connection refused / not enough acks landed) or ResourceExhausted (a
// partition still mid-migration). Only these are retried during the setup
// warmup window; everything else is a real failure.
func isTransientWarmupErr(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// waitForClusterReady is the canonical "the fixture is done settling"
// gate. Tests call this right after spinning up + joining all nodes,
// before issuing any Put/Get that depends on stable ownership. It
// guards against the integration-suite flake class where the first
// write into a freshly-joined cluster hits a partition mid-migration
// or a node that hasn't yet noticed its new peers.
//
// Three-phase wait:
//
//  1. Ring convergence: every node's Members() reports len(clusters)
//     entries. Under SWIM gossip this is sub-second on loopback but
//     not synchronous, so a multi-node assertion that fires before
//     convergence races.
//  2. Rebalance quiescence: every Coordinator's WaitForRebalanceIdle
//     returns. This closes the debounced-Evaluate -> Send -> Receive
//     -> sweep cycle that fires on each Notify*Join, so the in-flight
//     bootstrap migrations + their grace-window sweeps are all
//     complete before the test starts writing.
//  3. A small belt-and-suspenders sleep to let any async events
//     (delete-after-send sweep ticks, cache invalidation, etc.) drain.
//
// The deadline argument is the wall-clock budget for ALL three steps;
// each phase consumes part of it. On timeout we fail with a clear
// per-phase diagnostic so a real cluster bug is easy to spot vs a
// transient slow-CI hiccup.
func waitForClusterReady(t *testing.T, clusters []*cluster.Cluster, deadline time.Duration) {
	t.Helper()
	start := time.Now()
	if err := waitForMembersAll(clusters, len(clusters), deadline); err != nil {
		t.Fatalf("waitForClusterReady: ring convergence: %v", err)
	}
	// Pre-WaitForIdle sleep: the seed node's Coordinator debounces
	// each NotifyJoin behind RebalanceSettleDelay (500ms in the
	// integration fixture). If we call WaitForIdle before that timer
	// fires, the Coordinator has nothing pending + returns "idle"
	// immediately - then the Evaluate fires while the test is already
	// putting, and we are back to the original flake. Sleeping past
	// the settle window ensures Evaluate has run and the post-Evaluate
	// Sending/Receiving entries exist for WaitForIdle to actually
	// observe.
	time.Sleep(700 * time.Millisecond)
	remaining := deadline - time.Since(start)
	if remaining <= 0 {
		t.Fatalf("waitForClusterReady: pre-idle waits consumed the entire %s budget", deadline)
	}
	ctx, cancel := context.WithTimeout(context.Background(), remaining)
	defer cancel()
	for i, c := range clusters {
		if err := c.WaitForRebalanceIdle(ctx); err != nil {
			t.Fatalf("waitForClusterReady: rebalance idle wait on cluster %d (%s): %v", i, c.NodeID(), err)
		}
	}
	// Small drain window after Coordinator-reported idle so any async
	// side effects (grace-window sweep ticks, ring-rebuild fan-outs,
	// peer client cache invalidations) settle before the test starts
	// writing. 50ms is well under any test's per-step budget but is
	// long enough to consistently clear the post-idle backlog.
	time.Sleep(50 * time.Millisecond)
}
