package cluster

// White-box tests that need access to unexported names
// (reconcileRingFromMembership, clients map, ring) to pin regression
// behavior the public-API tests cannot reach.

import (
	"bytes"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/membership"
	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/ring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// freeTCPPort returns an OS-assigned ephemeral TCP port. Mirrors the
// helper in cluster_test.go but duplicated here so the internal test
// file compiles standalone.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeTCPPort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

func hp(port int) string {
	return "127.0.0.1:" + strconv.Itoa(port)
}

// waitForLocalInRing polls the ring until it contains the local member
// or the deadline expires. The events loop populates the ring from
// the NotifyJoin memberlist fires on Open, so this should be near-
// immediate but is racy enough to need a poll.
func waitForLocalInRing(c *Cluster, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range c.ring.Members() {
			if m.ID == c.cfg.NodeID {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestReconcileRingFromMembership_RestoresMissingLocal pins issue 3b:
// the reconcileRingFromMembership method must re-add a member that
// membership knows about but the ring has lost (the failure mode where
// an event-channel drop leaves the ring permanently stale).
//
// Approach: open a single-node multi-node-mode Cluster (no seeds, but
// BindAddr set so the membership + reconcile machinery is active),
// wait for the local node to land in the ring via the events loop,
// then manually evict it from the ring to simulate divergence + call
// reconcileRingFromMembership directly. The local member must reappear.
//
// Why this test would have caught a regression: any change that turned
// reconcileRingFromMembership into a no-op (e.g. someone "simplifying"
// by removing the Snapshot() call or guarding it with a wrong nil
// check) leaves the ring missing the local ID forever after this test
// induces the divergence. The assertion fires before the test cleans up.
func TestReconcileRingFromMembership_RestoresMissingLocal(t *testing.T) {
	port := freeTCPPort(t)
	c, err := Open(Config{
		NodeID:    "solo",
		Backend:   memory.New(),
		BindAddr:  hp(port),
		GRPCAddr:  "127.0.0.1:1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if !waitForLocalInRing(c, 2*time.Second) {
		t.Fatalf("local member never landed in ring; ring=%v", c.ring.Members())
	}

	// Simulate the divergence: events-channel drop lost a join, ring
	// no longer contains the local member. Membership.Snapshot() still
	// returns it as authoritative.
	c.ring.Remove("solo")
	if len(c.ring.Members()) != 0 {
		t.Fatalf("post-Remove ring should be empty, got %v", c.ring.Members())
	}

	c.reconcileRingFromMembership()

	members := c.ring.Members()
	if len(members) != 1 || members[0].ID != "solo" {
		t.Fatalf("reconcile did not restore local member; ring=%v", members)
	}
}

// TestEventsLoop_EvictsClientOnAddrChange pins issue 6: when a peer's
// GRPCAddr changes (NotifyUpdate path - same node ID, different Meta
// bytes), the events loop must (a) update the ring to point the ID at
// the new Addr and (b) evict the cached gRPC client for the OLD addr,
// so a stale connection to a defunct endpoint cannot serve subsequent
// requests for that peer.
//
// Drives the path via membership.TestingInjectUpdate (the test-only
// hook in pkg/membership), since memberlist exposes no public way to
// force a Meta-rebroadcast with a different addr for an existing
// node ID.
//
// Why this test would have caught a regression: if priorAddrForID or
// the evictClient call in runEventsLoop is dropped, the old client
// stays cached + a future forward dial returns the dead conn. The
// assertion on clients[oldAddr] absence fires.
func TestEventsLoop_EvictsClientOnAddrChange(t *testing.T) {
	// Park the reconciler far in the future so the manual ring +
	// clients state we set up below cannot be steamrolled by a
	// reconcile tick between the inject + the assertion.
	saved := reconcileInterval
	reconcileInterval = time.Hour
	t.Cleanup(func() { reconcileInterval = saved })

	port := freeTCPPort(t)
	c, err := Open(Config{
		NodeID:    "solo",
		Backend:   memory.New(),
		BindAddr:  hp(port),
		GRPCAddr:  "127.0.0.1:1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if !waitForLocalInRing(c, 2*time.Second) {
		t.Fatalf("local member never landed in ring")
	}

	// Stand up two throwaway gRPC listeners so dialing oldAddr +
	// newAddr returns real connections (grpc.NewClient is lazy enough
	// that even an unreachable addr "works", but a real listener
	// keeps the test honest re: what evict closes).
	oldLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("oldLis: %v", err)
	}
	defer oldLis.Close()
	oldSrv := grpc.NewServer()
	go func() { _ = oldSrv.Serve(oldLis) }()
	defer oldSrv.Stop()
	oldAddr := oldLis.Addr().String()

	newLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("newLis: %v", err)
	}
	defer newLis.Close()
	newSrv := grpc.NewServer()
	go func() { _ = newSrv.Serve(newLis) }()
	defer newSrv.Stop()
	newAddr := newLis.Addr().String()

	// Seed the ring + client cache as if a peer "peer-1" had joined
	// at oldAddr previously. Inject a NotifyUpdate that flips the
	// same ID to newAddr; the events loop should rewrite the ring +
	// evict the oldAddr client.
	c.ring.Add(ring.Member{ID: "peer-1", Addr: oldAddr})

	conn, err := grpc.NewClient(oldAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial oldAddr: %v", err)
	}
	c.clientsMu.Lock()
	c.clients[oldAddr] = &peerClient{conn: conn}
	c.clientsMu.Unlock()

	membership.TestingInjectUpdate(c.membership, "peer-1", newAddr)

	// Let the events loop drain + react. Poll for both conditions
	// (ring updated + client evicted) up to 2s; the actual reaction
	// is sub-millisecond on a quiet machine.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.clientsMu.RLock()
		_, stillCached := c.clients[oldAddr]
		c.clientsMu.RUnlock()
		gotAddr := ""
		for _, m := range c.ring.Members() {
			if m.ID == "peer-1" {
				gotAddr = m.Addr
				break
			}
		}
		if !stillCached && gotAddr == newAddr {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Failure: report what we observed.
	c.clientsMu.RLock()
	_, stillCached := c.clients[oldAddr]
	c.clientsMu.RUnlock()
	var gotAddr string
	for _, m := range c.ring.Members() {
		if m.ID == "peer-1" {
			gotAddr = m.Addr
		}
	}
	t.Fatalf("addr-change eviction did not happen: ring peer-1 addr=%q (want %q), oldAddr still cached=%v",
		gotAddr, newAddr, stillCached)
}

// TestPut_DuringMigrationReturnsFailedPrecondition pins the v0.3
// "Cutover" contract end-to-end on the cluster.Put guard: when the
// local Coordinator says a key's partition is mid-migration
// (StateSending), Put returns codes.FailedPrecondition with a
// retry-after hint, so SDK clients can back off + retry instead of
// silently dropping the write.
//
// White-box because the deterministic path goes through the
// cluster's own Coordinator: directly invoking
// c.rebalance.Evaluate (against a synthetic outgoing ring) puts
// the partitions in StateSending without needing a second real
// node or a timing-race against memberlist convergence. The
// blockingMigrateSource holds the partitions there so the guard
// window stays open long enough for the assertion to run.
func TestPut_DuringMigrationReturnsFailedPrecondition(t *testing.T) {
	port := freeTCPPort(t)
	be := memory.New()
	c, err := Open(Config{
		NodeID:                 "rb-source",
		Backend:                be,
		BindAddr:               hp(port),
		GRPCAddr:               "127.0.0.1:1",
		LogOutput:              io.Discard,
		RebalanceSettleDelay:   500 * time.Millisecond,
		RebalanceGraceDuration: 10 * time.Second,
		RebalanceRetryAfterMs:  77,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if !waitForLocalInRing(c, 2*time.Second) {
		t.Fatalf("local member never landed in ring")
	}

	// Seed enough keys that any randomly migrating partition holds
	// at least one. 200 spreads across 271 partitions ~uniformly.
	var keys [][]byte
	for i := 0; i < 200; i++ {
		k := []byte("rb-" + strconv.Itoa(i))
		if err := c.Put(k, []byte("v")); err != nil {
			t.Fatalf("seed put: %v", err)
		}
		keys = append(keys, k)
	}

	// Build the old + new ring shapes: old = current ring (only
	// rb-source); new = current + a synthetic peer. ~half the
	// partitions become Sends from us to the peer.
	old := ring.New()
	for _, m := range c.ring.Members() {
		old.Add(m)
	}
	new := ring.New()
	for _, m := range c.ring.Members() {
		new.Add(m)
	}
	new.Add(ring.Member{ID: "rb-elsewhere", Addr: "127.0.0.1:65535"})

	// Swap the cluster's Coordinator for one whose source blocks
	// indefinitely. Every Send goroutine pins its partition in
	// StateSending until we release the block, which keeps the
	// guard window open across the assertion.
	src := &blockingTestSource{released: make(chan struct{})}
	opts := rebalance.DefaultOptions()
	opts.GraceDuration = 10 * time.Second
	opts.Source = src
	opts.Destination = &clusterDestination{c: c}
	c.rebalance.Stop()
	c.rebalance = rebalance.New(rebalance.Member{ID: "rb-source"}, be, opts)
	c.rebalance.Evaluate(old, new, c.ringGen.Load())
	defer close(src.released)

	// Wait until at least one of our seeded keys' partitions is
	// reported migrating by the cluster's Coordinator.
	var movingKey []byte
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, k := range keys {
			if c.rebalance.IsMigrating(k) {
				movingKey = k
				break
			}
		}
		if movingKey != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if movingKey == nil {
		t.Fatalf("no seeded key entered StateSending; coordinator snapshot=%+v", c.rebalance.Snapshot())
	}

	// Put against the migrating key MUST return FailedPrecondition.
	err = c.Put(movingKey, []byte("post"))
	if err == nil {
		t.Fatalf("Put for migrating key returned nil; want FailedPrecondition")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Put error is not a gRPC status: %v", err)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("Put for migrating key: want FailedPrecondition, got %s (%v)", st.Code(), err)
	}
	if !bytes.Contains([]byte(st.Message()), []byte("retry")) {
		t.Fatalf("FailedPrecondition message %q lacks a retry hint; SDK clients need it to back off", st.Message())
	}
}

// blockingTestSource holds OpenRange's data channel open until
// released is closed. Used to pin Coordinator partitions in
// StateSending for the duration of an assertion.
type blockingTestSource struct {
	released chan struct{}
}

func (s *blockingTestSource) OpenRange(_ []uint64, _ uint64) (<-chan rebalance.KeyValue, <-chan error) {
	out := make(chan rebalance.KeyValue)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		<-s.released
		errCh <- nil
	}()
	return out, errCh
}
