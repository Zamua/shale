package cluster

// White-box tests for the declarative-resharding DECISION RULES (v0.8). These
// live in package cluster so they can drive the unexported, PURE guard helpers
// (validDoublingTarget, unanimousDesired, lowestIDIsCoordinator) directly.
//
// SCOPE. These cover the pure decision logic in isolation: the doubling-target
// math, the unanimity guard (including the 0/unknown branch), and the
// deterministic-coordinator selection. The full END-TO-END declarative reshard
// (real multi-node cluster, gossiped desired counts, the coordinator's loop
// driving the freeze barrier, the generation advancing across both nodes) is in
// tests/integration/declarative_reshard_test.go, which can wire the inter-node
// gRPC service (pkg/rpc) without the import cycle a package-cluster test would
// hit. Splitting it this way keeps the guard logic unit-tested AND the wiring
// integration-tested.

import (
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/membership"
)

// TestValidDoublingTarget pins guard 4 (+ the desired>current half of guard 3):
// a desired is a valid one-step doubling target iff it is a power of two
// strictly above the current count. current is always a power of two (the
// UnitCount invariant), so a higher power-of-two desired is always reachable by
// repeated doubling.
func TestValidDoublingTarget(t *testing.T) {
	cases := []struct {
		current, desired uint32
		want             bool
	}{
		{2, 4, true},   // one doubling
		{2, 8, true},   // two doublings away, still a valid TARGET (loop steps once per pass)
		{4, 8, true},   // one doubling
		{1, 2, true},   // legacy-min to 2
		{2, 2, false},  // equal: idempotent no-op, never an up-reshard
		{4, 2, false},  // shrink: never
		{8, 4, false},  // shrink: never
		{2, 3, false},  // non-power-of-two desired
		{2, 6, false},  // non-power-of-two desired
		{4, 12, false}, // non-power-of-two desired
		{2, 0, false},  // unknown desired (0) is never a target
	}
	for _, tc := range cases {
		if got := validDoublingTarget(tc.current, tc.desired); got != tc.want {
			t.Errorf("validDoublingTarget(current=%d, desired=%d) = %v, want %v",
				tc.current, tc.desired, got, tc.want)
		}
	}
}

// TestUnanimousDesired_Pure pins guard 2, including the 0/unknown branch: a
// member with DesiredCount==0 (a legacy single-backend node, or one whose Meta
// has not yet decoded a count) is treated as "not yet agreed" and blocks the
// trigger even when it is the only disagreement. This is the property that keeps
// a mixed-version or half-rolled cluster from resharding.
func TestUnanimousDesired_Pure(t *testing.T) {
	cases := []struct {
		name  string
		snap  []membership.Member
		want  uint32
		wantO bool
	}{
		{"empty is not agreement", nil, 0, false},
		{"single agrees", mems(4), 4, true},
		{"all agree", mems(4, 4, 4), 4, true},
		{"one disagrees", mems(4, 4, 2), 0, false},
		{"a zero peer blocks (first)", mems(0, 4), 0, false},
		{"a zero peer blocks (later)", mems(4, 0), 0, false},
		{"all zero is not agreement", mems(0, 0), 0, false},
	}
	for _, tc := range cases {
		got, ok := unanimousDesired(tc.snap)
		if got != tc.want || ok != tc.wantO {
			t.Errorf("%s: unanimousDesired = (%d, %v), want (%d, %v)",
				tc.name, got, ok, tc.want, tc.wantO)
		}
	}
}

// TestLowestIDIsCoordinator_Pure pins the deterministic-coordinator rule: the
// lowest-id member (snapshot element 0, since membership.Snapshot sorts by id)
// is the coordinator. An empty snapshot has no coordinator.
func TestLowestIDIsCoordinator_Pure(t *testing.T) {
	snap := []membership.Member{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	if !lowestIDIsCoordinator(snap, "a") {
		t.Errorf("lowest-id 'a' should be coordinator")
	}
	if lowestIDIsCoordinator(snap, "b") {
		t.Errorf("non-lowest 'b' should NOT be coordinator")
	}
	if lowestIDIsCoordinator(snap, "c") {
		t.Errorf("non-lowest 'c' should NOT be coordinator")
	}
	if lowestIDIsCoordinator(nil, "a") {
		t.Errorf("empty snapshot has no coordinator")
	}
}

// TestMembershipStableFor pins guard 1's PERIODIC-tick enforcement
// (membershipStableFor). The periodic tick fires unconditionally, so unlike the
// settle pass it must check stability itself: a ring whose shape changed within
// the window is NOT stable yet, and reconcileReshardIfStable skips. The three
// branches:
//   - a zero lastRingChangeNanos reads as STABLE (a never-changed ring is
//     trivially settled - it satisfies any window),
//   - a just-now change is NOT stable within the window,
//   - a change older than the window IS stable again.
func TestMembershipStableFor(t *testing.T) {
	const window = 200 * time.Millisecond

	// Never changed: zero stamp reads as stable for any window.
	c := &Cluster{}
	if !c.membershipStableFor(window) {
		t.Errorf("never-changed ring (lastRingChangeNanos==0) should be stable")
	}

	// Just-now change: not stable within the window.
	c.lastRingChangeNanos.Store(time.Now().UnixNano())
	if c.membershipStableFor(window) {
		t.Errorf("ring changed just now should NOT be stable within %s", window)
	}

	// Change older than the window: stable again.
	c.lastRingChangeNanos.Store(time.Now().Add(-2 * window).UnixNano())
	if !c.membershipStableFor(window) {
		t.Errorf("ring changed >%s ago should be stable again", window)
	}
}

// mems builds a member slice with the given DesiredCounts, ids assigned a..
// ascending (already sorted, matching membership.Snapshot's contract).
func mems(desired ...uint32) []membership.Member {
	out := make([]membership.Member, len(desired))
	for i, d := range desired {
		out[i] = membership.Member{ID: string(rune('a' + i)), DesiredCount: d}
	}
	return out
}
