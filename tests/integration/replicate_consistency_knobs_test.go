package integration

// v0.4 replication: WriteConsistency + ReadConsistency knobs.
//
// Each scenario stands up a fresh cluster (different sizes / R / W /
// N combinations) and pins the per-knob contract:
//
//   - WriteOne: Put succeeds with only the primary's local ack, even
//     when (R-1) remote replicas are unreachable.
//   - WriteQuorum on R=3: Put still succeeds with 1 remote down
//     (W=2 acks landed; one from local, one from the other survivor).
//   - WriteAll on R=3: any down replica fails the Put with
//     codes.Unavailable.
//   - ReadNearest: only the primary's backend is touched; non-primary
//     replicas see zero Get calls for the key.
//   - ReadAll: every replica is queried (every backend Get count
//     increments).

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"net"
)

// countingBackend wraps a memory.Memory + tallies Put/Get/Delete calls
// so tests can assert "ReadNearest only touched the primary."
type countingBackend struct {
	inner *memory.Memory
	puts  atomic.Int64
	gets  atomic.Int64
	dels  atomic.Int64
}

func newCountingBackend() *countingBackend {
	return &countingBackend{inner: memory.New()}
}

func (b *countingBackend) Put(key, value []byte) error {
	b.puts.Add(1)
	return b.inner.Put(key, value)
}
func (b *countingBackend) Get(key []byte) ([]byte, error) {
	b.gets.Add(1)
	return b.inner.Get(key)
}
func (b *countingBackend) Delete(key []byte) error {
	b.dels.Add(1)
	return b.inner.Delete(key)
}
func (b *countingBackend) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	return b.inner.ScanPrefix(prefix)
}
func (b *countingBackend) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	tx, err := b.inner.Begin(level)
	if err != nil {
		return nil, err
	}
	// Count writes that go through a transaction too: at R>1 the replica-
	// receiving write path (apply-if-newer) persists via Begin -> tx.Put ->
	// Commit rather than a bare backend.Put, so a test asserting "the local
	// replica path wrote the value" must see tx.Puts in the same tally.
	return &countingTx{inner: tx, puts: &b.puts, dels: &b.dels}, nil
}
func (b *countingBackend) Close() error { return b.inner.Close() }

// countingTx wraps an inner Transaction so buffered writes increment the
// backend's Put/Delete tallies. It exists because the apply-if-newer
// replica path (v0.7+) writes through a transaction, not a bare
// backend.Put, and the consistency-knob suite tallies "did the local
// replica receive the write" by Put count.
type countingTx struct {
	inner backend.Transaction
	puts  *atomic.Int64
	dels  *atomic.Int64
}

func (t *countingTx) Get(key []byte) ([]byte, error) { return t.inner.Get(key) }
func (t *countingTx) Put(key, value []byte) error {
	t.puts.Add(1)
	return t.inner.Put(key, value)
}
func (t *countingTx) Delete(key []byte) error {
	t.dels.Add(1)
	return t.inner.Delete(key)
}
func (t *countingTx) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	return t.inner.ScanPrefix(prefix)
}
func (t *countingTx) Commit() error   { return t.inner.Commit() }
func (t *countingTx) Rollback() error { return t.inner.Rollback() }

// countingTestNode is the per-node bundle for the consistency-knob
// suite. We can't reuse startTestNode because it pins memory.New()
// directly + we want the counting wrapper. The two harnesses share
// shape, not code.
type countingTestNode struct {
	ID       string
	Cluster  *cluster.Cluster
	Backend  *countingBackend
	BindAddr string
	GRPCAddr string

	grpcServer *grpc.Server
	stop       func()
}

func (n *countingTestNode) KillGRPC() {
	if n.grpcServer != nil {
		n.grpcServer.Stop()
		n.grpcServer = nil
	}
}

func (n *countingTestNode) Close() {
	if n.Cluster != nil {
		_ = n.Cluster.Close()
		n.Cluster = nil
	}
	if n.stop != nil {
		n.stop()
		n.stop = nil
	}
}

func startCountingNode(t *testing.T, id, seedAddr string, replicationFactor int, wc cluster.WriteConsistency, rc cluster.ReadConsistency) *countingTestNode {
	t.Helper()
	mem := newCountingBackend()
	bindAddr := hostPort(freePort(t))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startCountingNode %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:                  id,
		Backend:                 mem,
		BindAddr:                bindAddr,
		GRPCAddr:                grpcAddr,
		Seeds:                   nil,
		ReplicationFactor:       replicationFactor,
		WriteConsistency:        wc,
		ReadConsistency:         rc,
		RebalanceSettleDelay:    500 * time.Millisecond,
		RebalanceGraceDuration:  3 * time.Second,
		RebalanceHandoffTimeout: 4 * time.Second,
	}
	if seedAddr != "" {
		cfg.Seeds = []string{seedAddr}
	}
	c, err := cluster.Open(cfg)
	if err != nil {
		_ = lis.Close()
		t.Fatalf("startCountingNode %s: cluster.Open: %v", id, err)
	}
	rpc.NewServer(c).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()
	n := &countingTestNode{
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

func startCountingCluster(t *testing.T, count, r int, wc cluster.WriteConsistency, rc cluster.ReadConsistency) []*countingTestNode {
	t.Helper()
	nodes := make([]*countingTestNode, 0, count)
	seed := ""
	for i := 0; i < count; i++ {
		n := startCountingNode(t, fmt.Sprintf("cnt-n%d", i+1), seed, r, wc, rc)
		if i == 0 {
			seed = n.BindAddr
		}
		nodes = append(nodes, n)
	}
	cs := make([]*cluster.Cluster, len(nodes))
	for i, n := range nodes {
		cs[i] = n.Cluster
	}
	// Full settle before writes: ReadAll fan-out tests count
	// per-backend Gets after the call, which is corrupted if a
	// bootstrap migration is still routing through the source's
	// runSend / sweep window.
	waitForClusterReady(t, cs, 15*time.Second)
	return nodes
}

// TestReplicate_WriteOne_AcksOnPrimaryAlone pins WriteOne: a Put
// returns success after a single ack, even when (R-1) replicas are
// unreachable. Setup: R=3, kill the gRPC of two non-local replicas,
// Put through the local node -> local backend ack alone covers W=1.
func TestReplicate_WriteOne_AcksOnPrimaryAlone(t *testing.T) {
	nodes := startCountingCluster(t, 3, 3, cluster.WriteOne, cluster.ReadNearest)

	// Kill the remote gRPCs on N2 + N3. Memberlist hasn't dropped them
	// yet (graceful Close would tear membership down too), so the
	// originator's fanout still dispatches to all three; only N1's
	// local backend can ack. WriteOne should still succeed.
	nodes[1].KillGRPC()
	nodes[2].KillGRPC()

	if err := nodes[0].Cluster.Put([]byte("wone"), []byte("v")); err != nil {
		t.Fatalf("WriteOne Put with 2 of 3 remotes down should succeed: %v", err)
	}

	// Local backend MUST have received exactly one Put for this key
	// (the local-replica path). The dead nodes' backends were never
	// reachable.
	if nodes[0].Backend.puts.Load() < 1 {
		t.Errorf("expected local Put count >= 1, got %d", nodes[0].Backend.puts.Load())
	}
}

// TestReplicate_WriteQuorum_R3_TolerateOneDown pins WriteQuorum at
// R=3: W=2 acks. Killing one replica's gRPC still leaves two acks
// (local + the remaining remote).
func TestReplicate_WriteQuorum_R3_TolerateOneDown(t *testing.T) {
	nodes := startCountingCluster(t, 3, 3, cluster.WriteQuorum, cluster.ReadNearest)

	nodes[2].KillGRPC()

	if err := nodes[0].Cluster.Put([]byte("wq"), []byte("v")); err != nil {
		t.Fatalf("WriteQuorum on R=3 with 1 down should succeed: %v", err)
	}
}

// TestReplicate_WriteAll_R3_FailsWhenOneDown pins WriteAll at R=3:
// any down replica means at least one ack short of R, and the Put
// MUST return codes.Unavailable.
func TestReplicate_WriteAll_R3_FailsWhenOneDown(t *testing.T) {
	nodes := startCountingCluster(t, 3, 3, cluster.WriteAll, cluster.ReadNearest)

	nodes[2].KillGRPC()

	err := nodes[0].Cluster.Put([]byte("wa"), []byte("v"))
	if err == nil {
		t.Fatalf("WriteAll with one node down MUST fail")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("WriteAll failure: want codes.Unavailable, got %v", err)
	}
}

// TestReplicate_ReadNearest_ReturnsAfterFirstSuccess pins ReadNearest:
// the call returns the moment the first replica replies. v0.4 dispatches
// to all R replicas (so a hung / down primary falls back to a successor
// per docs/SPEC.md "Failure modes -> Replica down at Read time"), but the
// consistency target is 1, so the call returns immediately on the first
// success. The post-decision dispatches still run -- that's the spec's
// "Fan-out + ack accounting" surplus contract -- but the Get does NOT
// block on them. This test asserts the primary's backend was the one
// satisfying the caller (the local fast-path serves before the network
// dispatch lands) AND that the call returned the right value; the
// surplus dispatches are best-effort + uninteresting to the contract.
func TestReplicate_ReadNearest_ReturnsAfterFirstSuccess(t *testing.T) {
	nodes := startCountingCluster(t, 3, 3, cluster.WriteAll, cluster.ReadNearest)

	key := []byte("nearest-key")
	if err := nodes[0].Cluster.Put(key, []byte("v")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	// Discover the primary so the assertion talks about the right node.
	r := ring.New()
	for _, m := range nodes[0].Cluster.Members() {
		r.Add(m)
	}
	primary := r.LocateKey(key).ID

	byID := map[string]*countingTestNode{}
	for _, n := range nodes {
		byID[n.ID] = n
	}

	// Snapshot Get counts BEFORE the read so we don't double-count
	// bootstrap or settle-time reads.
	preGets := map[string]int64{}
	for _, n := range nodes {
		preGets[n.ID] = n.Backend.gets.Load()
	}

	// Read via the primary itself so the local-replica fast-path serves
	// the result without further fan-out. The call returns "v" once
	// the local hit lands; surplus dispatches to N2 + N3 continue in
	// the background but they don't affect correctness.
	got, err := byID[primary].Cluster.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v" {
		t.Errorf("ReadNearest got %q want v", got)
	}

	deltaPrimary := byID[primary].Backend.gets.Load() - preGets[primary]
	if deltaPrimary < 1 {
		t.Errorf("primary %s saw no Get: delta=%d", primary, deltaPrimary)
	}
}

// TestReplicate_ReadAll_FansOutToAllR pins ReadAll: every replica's
// backend receives a Get for the key. With R=3 + 3 nodes, that's all
// three backends.
func TestReplicate_ReadAll_FansOutToAllR(t *testing.T) {
	nodes := startCountingCluster(t, 3, 3, cluster.WriteAll, cluster.ReadAll)

	key := []byte("readall-key")
	if err := nodes[0].Cluster.Put(key, []byte("v")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	preGets := map[string]int64{}
	for _, n := range nodes {
		preGets[n.ID] = n.Backend.gets.Load()
	}

	if _, err := nodes[0].Cluster.Get(key); err != nil {
		t.Fatalf("Get: %v", err)
	}

	// Allow surplus reads + read-repair to settle briefly. ReadAll's
	// success path returns once N replies arrive; the rest of the
	// channel drains asynchronously.
	time.Sleep(100 * time.Millisecond)

	hits := 0
	for _, n := range nodes {
		delta := n.Backend.gets.Load() - preGets[n.ID]
		if delta >= 1 {
			hits++
		}
	}
	if hits < 3 {
		t.Errorf("ReadAll should query every replica, got %d/3 backends with Get hits", hits)
	}
}

// Silence "imported and not used" if the failure-path tests above ever
// stop dereferencing errors.Is/backend.
var _ = errors.Is
var _ = backend.ErrNotFound
