package membership

import (
	"fmt"
	"testing"
	"time"
)

// TestEncodeDecodeMetaRoundTrip pins the Meta payload contract: a node with
// a desired count encodes "<addr>|<count>"; one without encodes the bare
// addr (byte-for-byte the legacy format). decodeMeta is the exact inverse,
// and tolerant of malformed / legacy input (decoding to count 0).
func TestEncodeDecodeMetaRoundTrip(t *testing.T) {
	cases := []struct {
		name      string
		addr      string
		count     uint32
		wantBytes string
	}{
		{"no desired count is bare addr", "127.0.0.1:7947", 0, "127.0.0.1:7947"},
		{"desired count appended", "127.0.0.1:7947", 4, "127.0.0.1:7947|4"},
		{"count of one", "10.0.0.2:9000", 1, "10.0.0.2:9000|1"},
		{"large count", "host:1", 1 << 20, "host:1|1048576"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := encodeMeta(tc.addr, tc.count)
			if string(got) != tc.wantBytes {
				t.Fatalf("encodeMeta(%q, %d) = %q, want %q", tc.addr, tc.count, got, tc.wantBytes)
			}
			addr, count := decodeMeta(got)
			if addr != tc.addr || count != tc.count {
				t.Fatalf("decodeMeta(%q) = (%q, %d), want (%q, %d)", got, addr, count, tc.addr, tc.count)
			}
		})
	}
}

// TestDecodeMetaLegacyAndMalformed pins that a peer broadcasting the OLD
// bare-addr Meta (no count segment) decodes to DesiredCount 0, and a
// payload with a non-numeric count segment also decodes to 0 (unknown)
// rather than failing. The reconciler treats 0 as "not yet agreed".
func TestDecodeMetaLegacyAndMalformed(t *testing.T) {
	cases := []struct {
		in       string
		wantAddr string
		wantCnt  uint32
	}{
		{"127.0.0.1:7947", "127.0.0.1:7947", 0},         // legacy bare addr
		{"", "", 0},                                     // empty
		{"127.0.0.1:7947|", "127.0.0.1:7947", 0},        // empty count segment
		{"127.0.0.1:7947|abc", "127.0.0.1:7947", 0},     // non-numeric count
		{"127.0.0.1:7947|-3", "127.0.0.1:7947", 0},      // negative (unparseable as uint)
		{"127.0.0.1:7947|4|extra", "127.0.0.1:7947", 0}, // trailing junk -> "4|extra" unparseable
	}
	for _, tc := range cases {
		addr, cnt := decodeMeta([]byte(tc.in))
		if addr != tc.wantAddr || cnt != tc.wantCnt {
			t.Fatalf("decodeMeta(%q) = (%q, %d), want (%q, %d)", tc.in, addr, cnt, tc.wantAddr, tc.wantCnt)
		}
	}
}

// TestDesiredCountPropagatesViaMeta verifies the end-to-end gossip path: a
// node started with Config.DesiredCount advertises it via Meta, and a peer
// reads it off the Members() snapshot. This is the membership half of the
// declarative-resharding trigger.
func TestDesiredCountPropagatesViaMeta(t *testing.T) {
	n1Port := ephemeralPort(t)
	n2Port := ephemeralPort(t)

	m1 := openTestMembership(t, Config{
		NodeID:       "n1",
		BindAddr:     bindAddr(n1Port),
		GRPCAddr:     "127.0.0.1:18101",
		DesiredCount: 4,
	})
	m2 := openTestMembership(t, Config{
		NodeID:       "n2",
		BindAddr:     bindAddr(n2Port),
		GRPCAddr:     "127.0.0.1:18102",
		DesiredCount: 4,
		Seeds:        []string{bindAddr(n1Port)},
	})

	want := map[string]uint32{"n1": 4, "n2": 4}
	for _, m := range []*Membership{m1, m2} {
		if err := waitForDesiredCounts(m, want, 5*time.Second); err != nil {
			t.Fatalf("desired-count convergence: %v", err)
		}
	}
}

// TestMixedDesiredCountVisible pins that a node WITHOUT a desired count
// (legacy / count 0) and one WITH one coexist: each member's snapshot
// carries the other's count verbatim (0 for the legacy node, the real
// value for the other). This is the mid-roll state the reconciler must
// see as "not unanimous".
func TestMixedDesiredCountVisible(t *testing.T) {
	n1Port := ephemeralPort(t)
	n2Port := ephemeralPort(t)

	m1 := openTestMembership(t, Config{
		NodeID:   "legacy",
		BindAddr: bindAddr(n1Port),
		GRPCAddr: "127.0.0.1:18201",
		// no DesiredCount: stays 0
	})
	_ = openTestMembership(t, Config{
		NodeID:       "new",
		BindAddr:     bindAddr(n2Port),
		GRPCAddr:     "127.0.0.1:18202",
		DesiredCount: 4,
		Seeds:        []string{bindAddr(n1Port)},
	})

	want := map[string]uint32{"legacy": 0, "new": 4}
	if err := waitForDesiredCounts(m1, want, 5*time.Second); err != nil {
		t.Fatalf("mixed-count convergence: %v", err)
	}
}

func waitForDesiredCounts(m *Membership, want map[string]uint32, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last []Member
	for time.Now().Before(deadline) {
		last = m.Members()
		if desiredCountsMatch(last, want) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("desired counts did not converge in %s; want %v, got %v", timeout, want, last)
}

func desiredCountsMatch(got []Member, want map[string]uint32) bool {
	if len(got) != len(want) {
		return false
	}
	for _, mem := range got {
		w, ok := want[mem.ID]
		if !ok || w != mem.DesiredCount {
			return false
		}
	}
	return true
}
