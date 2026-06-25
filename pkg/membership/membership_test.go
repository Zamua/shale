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

func TestOpenSoloStartWhenSeedsUnreachable(t *testing.T) {
	port := ephemeralPort(t)
	dead := ephemeralPort(t)

	// AllowSoloStart: unreachable seeds is NOT fatal; the node comes up solo.
	// This is the homogeneous-bootstrap path - every pod carries the same seed
	// list (a headless Service), and the first one up reaches no peer, starts
	// solo, then contends to form via the caller's durable CAS form-lock.
	m, err := Open(Config{
		NodeID:         "solo",
		BindAddr:       bindAddr(port),
		GRPCAddr:       "127.0.0.1:1",
		Seeds:          []string{bindAddr(dead)},
		LogOutput:      io.Discard,
		AllowSoloStart: true,
	})
	if err != nil {
		t.Fatalf("AllowSoloStart: want solo start when seeds unreachable, got error: %v", err)
	}
	defer func() { _ = m.Close() }()

	if got := m.Members(); len(got) != 1 {
		t.Fatalf("solo node should see exactly itself, got %d members: %v", len(got), got)
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

// TestLeaveBroadcastsWithoutShutdown pins the v0.8 Phase 2e split: Leave()
// broadcasts the graceful departure (the peer observes EventLeave) WITHOUT
// shutting the local transport down, so the leaver keeps serving - Members()
// still returns the live view (non-nil, not the closed sentinel) - and a
// subsequent Close() still tears the transport down cleanly + idempotently.
func TestLeaveBroadcastsWithoutShutdown(t *testing.T) {
	n1Port := ephemeralPort(t)
	n2Port := ephemeralPort(t)

	m1 := openTestMembership(t, Config{
		NodeID:   "alpha",
		BindAddr: bindAddr(n1Port),
		GRPCAddr: "127.0.0.1:19301",
	})
	m2 := openTestMembership(t, Config{
		NodeID:   "beta",
		BindAddr: bindAddr(n2Port),
		GRPCAddr: "127.0.0.1:19302",
		Seeds:    []string{bindAddr(n1Port)},
	})

	want := map[string]string{
		"alpha": "127.0.0.1:19301",
		"beta":  "127.0.0.1:19302",
	}
	if err := waitForMembers(m1, want, 5*time.Second); err != nil {
		t.Fatalf("alpha convergence: %v", err)
	}

	drainEvents(m1.Events())

	// beta leaves WITHOUT shutting its transport down.
	if err := m2.Leave(); err != nil {
		t.Fatalf("beta Leave: %v", err)
	}

	// alpha observes the graceful departure.
	deadline := time.After(10 * time.Second)
	for done := false; !done; {
		select {
		case ev, ok := <-m1.Events():
			if !ok {
				t.Fatalf("alpha event channel closed before leave seen")
			}
			if ev.Type == EventLeave && ev.Member.ID == "beta" {
				done = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for EventLeave for beta after Leave()")
		}
	}

	// The leaver's transport is STILL UP: Members() returns the live view, not
	// the closed-sentinel nil. (beta still knows about itself + alpha.)
	if got := m2.Members(); got == nil {
		t.Fatalf("after Leave() (no Shutdown) Members() must return the live view, got nil")
	}

	// Close() after Leave() still works (idempotent second leave + transport
	// teardown).
	if err := m2.Close(); err != nil {
		t.Fatalf("Close after Leave: %v", err)
	}
	// Now the transport is down: Members() returns the closed sentinel.
	if got := m2.Members(); got != nil {
		t.Fatalf("after Close() Members() must be nil (closed sentinel), got %v", got)
	}
}

// TestCloseReturnsWithPeerAlive pins the bounded graceful-leave fix:
// Close on a node whose peer is still alive must return promptly
// rather than blocking forever on the leave broadcast. Before the fix,
// Membership.Close called memberlist.Leave(0), whose nil timeout
// channel made the wait-for-leave-broadcast a receive on a nil channel
// that blocks indefinitely when a peer is still alive. We assert Close
// returns inside a window well under any reasonable hang (and under the
// 5s bounded leaveTimeout itself).
func TestCloseReturnsWithPeerAlive(t *testing.T) {
	n1Port := ephemeralPort(t)
	n2Port := ephemeralPort(t)

	m1 := openTestMembership(t, Config{
		NodeID:   "alpha",
		BindAddr: bindAddr(n1Port),
		GRPCAddr: "127.0.0.1:19101",
	})
	m2 := openTestMembership(t, Config{
		NodeID:   "beta",
		BindAddr: bindAddr(n2Port),
		GRPCAddr: "127.0.0.1:19102",
		Seeds:    []string{bindAddr(n1Port)},
	})

	want := map[string]string{
		"alpha": "127.0.0.1:19101",
		"beta":  "127.0.0.1:19102",
	}
	if err := waitForMembers(m2, want, 5*time.Second); err != nil {
		t.Fatalf("beta convergence: %v", err)
	}

	// Close beta while alpha is still alive. With the unbounded Leave(0)
	// this never returns; with the bounded leaveTimeout it returns once
	// the broadcast lands (fast, peer is local) or the timeout elapses.
	done := make(chan error, 1)
	go func() { done <- m2.Close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("beta Close: %v", err)
		}
	case <-time.After(leaveTimeout + 3*time.Second):
		t.Fatalf("Close did not return within %s; graceful leave hung", leaveTimeout+3*time.Second)
	}

	// alpha (m1) is the still-alive peer; sanity-check it remains usable
	// after beta's graceful leave (it observes beta's departure rather
	// than hanging the cluster).
	if got := m1.Members(); len(got) == 0 {
		t.Fatalf("alpha lost all members after beta Close: %v", got)
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

// TestMembersReturnsNilAfterClose pins issue 10: Members() must not
// race with Close. After Close, Members returns nil rather than
// reaching into the torn-down memberlist + tripping the race
// detector (or worse, returning stale entries).
func TestMembersReturnsNilAfterClose(t *testing.T) {
	port := ephemeralPort(t)

	m, err := Open(Config{
		NodeID:    "solo",
		BindAddr:  bindAddr(port),
		GRPCAddr:  "127.0.0.1:1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if got := m.Members(); len(got) != 1 {
		t.Fatalf("pre-close Members(): want 1, got %d", len(got))
	}

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := m.Members(); got != nil {
		t.Fatalf("post-close Members(): want nil, got %v", got)
	}
	if got := m.Snapshot(); got != nil {
		t.Fatalf("post-close Snapshot(): want nil, got %v", got)
	}
}

// TestDropCountObservable verifies DropCount goes up when the events
// channel fills + drops. We force the situation by NEVER consuming
// events + thrashing the eventDelegate directly with a few hundred
// synthetic NotifyJoin calls.
//
// We test the eventDelegate at the package-internal level (the same
// path memberlist uses) rather than spinning up enough real nodes to
// overflow the 64-buffer, which is slow + flaky.
func TestDropCountObservable(t *testing.T) {
	port := ephemeralPort(t)
	m, err := Open(Config{
		NodeID:    "solo",
		BindAddr:  bindAddr(port),
		GRPCAddr:  "127.0.0.1:1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	// Saturate the channel with synthetic sends. We bypass memberlist
	// here + poke the delegate via the same path it does internally.
	// Sending more than channel-buffer-size with no reader forces
	// drops; the counter should reflect them.
	for range 256 {
		ev := Event{Type: EventJoin, Member: Member{ID: "x"}}
		m.events.mu.RLock()
		select {
		case m.events.out <- ev:
		default:
			m.events.dropCount.Add(1)
		}
		m.events.mu.RUnlock()
	}
	if got := m.DropCount(); got == 0 {
		t.Fatalf("expected DropCount > 0 after saturating channel, got 0")
	}
}

// findMember returns the cached Member for id and whether it was present.
func findMember(m *Membership, id string) (Member, bool) {
	for _, mem := range m.Members() {
		if mem.ID == id {
			return mem, true
		}
	}
	return Member{}, false
}

// waitForDraining polls until the snapshot's member id has the wanted
// draining value (with its address still intact), or the timeout fires.
func waitForDraining(m *Membership, id string, want bool, wantAddr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last Member
	for time.Now().Before(deadline) {
		mem, ok := findMember(m, id)
		last = mem
		if ok && mem.Draining == want && mem.Addr == wantAddr {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("did not observe %s draining=%v addr=%q in %s; last=%+v", id, want, wantAddr, timeout, last)
}

// TestEncodeDecodeMetaRoundTrip pins the backward-shaped Meta wire
// format: the bare (no draining, no declared count) payload is
// byte-identical to the bare-address legacy format, and the draining bit +
// declared unit count round-trip while the address still decodes cleanly.
func TestEncodeDecodeMetaRoundTrip(t *testing.T) {
	const addr = "127.0.0.1:7001"

	// Bare (non-draining, no declared count) encodes to exactly the bare
	// address (legacy-shaped).
	meta := encodeMeta(addr, false, 0)
	if string(meta) != addr {
		t.Fatalf("bare meta = %q, want bare addr %q", meta, addr)
	}
	gotAddr, draining, count := decodeMeta(meta)
	if gotAddr != addr || draining || count != 0 {
		t.Fatalf("decode(bare) = (%q, %v, %d), want (%q, false, 0)", gotAddr, draining, count, addr)
	}

	// Draining round-trips the bit while keeping the address.
	meta = encodeMeta(addr, true, 0)
	gotAddr, draining, count = decodeMeta(meta)
	if gotAddr != addr || !draining || count != 0 {
		t.Fatalf("decode(draining) = (%q, %v, %d), want (%q, true, 0)", gotAddr, draining, count, addr)
	}

	// A declared unit count round-trips while keeping the address (and not
	// draining).
	meta = encodeMeta(addr, false, 16)
	gotAddr, draining, count = decodeMeta(meta)
	if gotAddr != addr || draining || count != 16 {
		t.Fatalf("decode(count=16) = (%q, %v, %d), want (%q, false, 16)", gotAddr, draining, count, addr)
	}

	// Draining AND a declared count compose: both round-trip together, the
	// address intact, regardless of segment order.
	meta = encodeMeta(addr, true, 8)
	gotAddr, draining, count = decodeMeta(meta)
	if gotAddr != addr || !draining || count != 8 {
		t.Fatalf("decode(draining+count=8) = (%q, %v, %d), want (%q, true, 8)", gotAddr, draining, count, addr)
	}

	// A legacy bare-address payload (no separator, written by an older
	// node) decodes as not-draining, unknown count (0), address intact.
	gotAddr, draining, count = decodeMeta([]byte(addr))
	if gotAddr != addr || draining || count != 0 {
		t.Fatalf("decode(legacy) = (%q, %v, %d), want (%q, false, 0)", gotAddr, draining, count, addr)
	}

	// A legacy draining-only payload (addr\x00D, written by a node that
	// gossips draining but not the count) decodes draining with unknown
	// count - the forward-compat path that keeps the count fail-safe.
	gotAddr, draining, count = decodeMeta([]byte(addr + string(metaSep) + metaDrainingMarker))
	if gotAddr != addr || !draining || count != 0 {
		t.Fatalf("decode(legacy draining) = (%q, %v, %d), want (%q, true, 0)", gotAddr, draining, count, addr)
	}
}

// TestSetDrainingGossipsToPeer pins the core Phase 2e membership
// behavior: a node calls SetDraining(true), and BOTH its own snapshot
// and a peer's snapshot observe Draining=true (with the address still
// resolvable). The node stays a full alive member - it is NOT removed
// from either snapshot. Clearing draining gossips back to false.
func TestSetDrainingGossipsToPeer(t *testing.T) {
	n1Port := ephemeralPort(t)
	n2Port := ephemeralPort(t)

	const n1Grpc = "127.0.0.1:19501"
	const n2Grpc = "127.0.0.1:19502"

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

	want := map[string]string{"n1": n1Grpc, "n2": n2Grpc}
	if err := waitForMembers(m1, want, 5*time.Second); err != nil {
		t.Fatalf("n1 convergence: %v", err)
	}
	if err := waitForMembers(m2, want, 5*time.Second); err != nil {
		t.Fatalf("n2 convergence: %v", err)
	}

	// Baseline: nobody is draining.
	if mem, _ := findMember(m1, "n1"); mem.Draining {
		t.Fatalf("n1 unexpectedly draining at baseline: %+v", mem)
	}

	// n1 enters DRAINING.
	if err := m1.SetDraining(true); err != nil {
		t.Fatalf("SetDraining(true): %v", err)
	}

	// n1 sees itself draining, address intact.
	if err := waitForDraining(m1, "n1", true, n1Grpc, 5*time.Second); err != nil {
		t.Fatalf("n1 self-view: %v", err)
	}
	// The peer sees n1 draining via gossip, still in the snapshot.
	if err := waitForDraining(m2, "n1", true, n1Grpc, 5*time.Second); err != nil {
		t.Fatalf("n2 peer-view of n1 draining: %v", err)
	}
	// n1 stays a full member on the peer (not removed): both nodes present.
	if len(m2.Members()) != 2 {
		t.Fatalf("n1 vanished from n2 snapshot while draining: %v", m2.Members())
	}
	// n2 is not draining (the bit is per-member).
	if mem, _ := findMember(m2, "n2"); mem.Draining {
		t.Fatalf("n2 spuriously draining: %+v", mem)
	}

	// Clearing draining gossips back to false on the peer.
	if err := m1.SetDraining(false); err != nil {
		t.Fatalf("SetDraining(false): %v", err)
	}
	if err := waitForDraining(m2, "n1", false, n1Grpc, 5*time.Second); err != nil {
		t.Fatalf("n2 peer-view of n1 cleared: %v", err)
	}
}

// TestSetDrainingNoChangeIsNoop pins that setting the already-current
// draining value does not error (and is treated as a no-op internally).
func TestSetDrainingNoChangeIsNoop(t *testing.T) {
	port := ephemeralPort(t)
	m := openTestMembership(t, Config{
		NodeID:   "solo",
		BindAddr: bindAddr(port),
		GRPCAddr: "127.0.0.1:19601",
	})
	// Already false: no-op.
	if err := m.SetDraining(false); err != nil {
		t.Fatalf("SetDraining(false) no-op: %v", err)
	}
	if err := m.SetDraining(true); err != nil {
		t.Fatalf("SetDraining(true): %v", err)
	}
	// Already true: no-op.
	if err := m.SetDraining(true); err != nil {
		t.Fatalf("SetDraining(true) repeat no-op: %v", err)
	}
}

// TestSetDrainingAfterCloseIsNoop pins that SetDraining on a closed
// Membership returns nil without touching the torn-down transport.
func TestSetDrainingAfterCloseIsNoop(t *testing.T) {
	port := ephemeralPort(t)
	m := openTestMembership(t, Config{
		NodeID:   "solo",
		BindAddr: bindAddr(port),
		GRPCAddr: "127.0.0.1:19701",
	})
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := m.SetDraining(true); err != nil {
		t.Fatalf("SetDraining after close should be no-op, got %v", err)
	}
}
