package membership

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

// ephemeralPort returns a free TCP port. memberlist binds both TCP +
// UDP on the same port; on a loopback the TCP probe is a good-enough
// proxy for availability. There is an inherent race between us closing
// the listener + memberlist re-binding; in practice it is fine for
// localhost tests, and the alternative (asking memberlist to pick) is
// not exposed by its API.
func ephemeralPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ephemeralPort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("ephemeralPort close: %v", err)
	}
	return port
}

func bindAddr(port int) string {
	return "127.0.0.1:" + strconv.Itoa(port)
}

// openTestMembership opens a Membership with quiet logging + the
// loopback interface, registering a Cleanup that closes it.
func openTestMembership(t *testing.T, cfg Config) *Membership {
	t.Helper()
	cfg.LogOutput = io.Discard
	m, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open(%s): %v", cfg.NodeID, err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func TestOpenSingleNodeReturnsSelfOnly(t *testing.T) {
	port := ephemeralPort(t)
	grpcAddr := "127.0.0.1:9999"

	m := openTestMembership(t, Config{
		NodeID:   "solo",
		BindAddr: bindAddr(port),
		GRPCAddr: grpcAddr,
	})

	members := m.Members()
	if len(members) != 1 {
		t.Fatalf("want 1 member, got %d (%v)", len(members), members)
	}
	if members[0].ID != "solo" {
		t.Fatalf("want ID=solo, got %q", members[0].ID)
	}
	if members[0].Addr != grpcAddr {
		t.Fatalf("want Addr=%q, got %q", grpcAddr, members[0].Addr)
	}
}

func TestOpenWithUnreachableSeedsFails(t *testing.T) {
	port := ephemeralPort(t)
	dead := ephemeralPort(t)

	_, err := Open(Config{
		NodeID:    "loner",
		BindAddr:  bindAddr(port),
		GRPCAddr:  "127.0.0.1:1",
		Seeds:     []string{bindAddr(dead)},
		LogOutput: io.Discard,
	})
	if err == nil {
		t.Fatalf("want error when seeds are unreachable, got nil")
	}
}

func TestThreeNodesConverge(t *testing.T) {
	n1Port := ephemeralPort(t)
	n2Port := ephemeralPort(t)
	n3Port := ephemeralPort(t)

	n1Grpc := "127.0.0.1:18001"
	n2Grpc := "127.0.0.1:18002"
	n3Grpc := "127.0.0.1:18003"

	m1 := openTestMembership(t, Config{
		NodeID:   "n1",
		BindAddr: bindAddr(n1Port),
		GRPCAddr: n1Grpc,
	})
	m2 := openTestMembership(t, Config{
		NodeID:   "n2",
		BindAddr: bindAddr(n2Port),
		GRPCAddr: n2Grpc,
		Seeds:    []string{bindAddr(n1Port)},
	})
	m3 := openTestMembership(t, Config{
		NodeID:   "n3",
		BindAddr: bindAddr(n3Port),
		GRPCAddr: n3Grpc,
		Seeds:    []string{bindAddr(n1Port)},
	})

	want := map[string]string{
		"n1": n1Grpc,
		"n2": n2Grpc,
		"n3": n3Grpc,
	}

	for _, m := range []*Membership{m1, m2, m3} {
		if err := waitForMembers(m, want, 5*time.Second); err != nil {
			t.Fatalf("convergence: %v", err)
		}
	}
}

func TestLeaveEmitsEvent(t *testing.T) {
	n1Port := ephemeralPort(t)
	n2Port := ephemeralPort(t)

	m1 := openTestMembership(t, Config{
		NodeID:   "alpha",
		BindAddr: bindAddr(n1Port),
		GRPCAddr: "127.0.0.1:19001",
	})
	m2 := openTestMembership(t, Config{
		NodeID:   "beta",
		BindAddr: bindAddr(n2Port),
		GRPCAddr: "127.0.0.1:19002",
		Seeds:    []string{bindAddr(n1Port)},
	})

	want := map[string]string{
		"alpha": "127.0.0.1:19001",
		"beta":  "127.0.0.1:19002",
	}
	if err := waitForMembers(m1, want, 5*time.Second); err != nil {
		t.Fatalf("alpha convergence: %v", err)
	}

	// Drain prior join events so we observe the leave cleanly.
	drainEvents(m1.Events())

	if err := m2.Close(); err != nil {
		t.Fatalf("close beta: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-m1.Events():
			if !ok {
				t.Fatalf("alpha event channel closed before leave seen")
			}
			if ev.Type == EventLeave && ev.Member.ID == "beta" {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for EventLeave for beta; current members: %v", m1.Members())
		}
	}
}

// waitForMembers polls until m.Members() matches want or the deadline
// expires.
func waitForMembers(m *Membership, want map[string]string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last []Member
	for time.Now().Before(deadline) {
		last = m.Members()
		if membersMatch(last, want) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("did not converge in %s; want %v, got %v", timeout, want, last)
}

func membersMatch(got []Member, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for _, m := range got {
		w, ok := want[m.ID]
		if !ok || w != m.Addr {
			return false
		}
	}
	return true
}

// drainEvents reads everything currently buffered on the channel
// without blocking.
func drainEvents(ch <-chan Event) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
