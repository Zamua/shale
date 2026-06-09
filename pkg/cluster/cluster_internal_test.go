package cluster

// White-box tests that need access to unexported names
// (reconcileRingFromMembership, clients map, ring) to pin regression
// behavior the public-API tests cannot reach.

import (
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
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
