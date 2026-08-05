package decide

import (
	"testing"
	"time"
)

const debounce = 5 * time.Second

// TestSettleArm enumerates the arming machine EXHAUSTIVELY: every reachable
// SettleState (unarmed, plus armed x immediate x fired) crossed with both kinds
// of incoming arm (immediate and debounced). The table is the enumeration, so a
// state nobody thought of is a missing row rather than an invisible gap.
func TestSettleArm(t *testing.T) {
	cases := []struct {
		name      string
		cur       SettleState
		requested time.Duration
		want      SettleArmDecision
	}{
		{
			name:      "unarmed/debounced arms fresh at the requested delay",
			cur:       SettleState{},
			requested: debounce,
			want:      SettleArmDecision{Delay: debounce, Immediate: false, NewObligation: true},
		},
		{
			name:      "unarmed/immediate arms fresh at zero",
			cur:       SettleState{},
			requested: 0,
			want:      SettleArmDecision{Delay: 0, Immediate: true, NewObligation: true},
		},
		{
			name:      "live debounced/debounced replaces and extends the debounce",
			cur:       SettleState{Armed: true},
			requested: debounce,
			want:      SettleArmDecision{Delay: debounce, Immediate: false, NewObligation: false},
		},
		{
			name:      "live debounced/immediate wins: same pass, sooner",
			cur:       SettleState{Armed: true},
			requested: 0,
			want:      SettleArmDecision{Delay: 0, Immediate: true, NewObligation: false},
		},
		{
			name:      "live immediate/debounced must NOT postpone the immediate pass",
			cur:       SettleState{Armed: true, Immediate: true},
			requested: debounce,
			want:      SettleArmDecision{Delay: 0, Immediate: true, NewObligation: false},
		},
		{
			name:      "live immediate/immediate stays immediate",
			cur:       SettleState{Armed: true, Immediate: true},
			requested: 0,
			want:      SettleArmDecision{Delay: 0, Immediate: true, NewObligation: false},
		},
		{
			name:      "fired debounced/debounced owns a fresh obligation",
			cur:       SettleState{Armed: true, Fired: true},
			requested: debounce,
			want:      SettleArmDecision{Delay: debounce, Immediate: false, NewObligation: true},
		},
		{
			name:      "fired debounced/immediate owns a fresh obligation",
			cur:       SettleState{Armed: true, Fired: true},
			requested: 0,
			want:      SettleArmDecision{Delay: 0, Immediate: true, NewObligation: true},
		},
		{
			name:      "fired immediate/debounced owns a fresh obligation and is NOT held immediate",
			cur:       SettleState{Armed: true, Immediate: true, Fired: true},
			requested: debounce,
			want:      SettleArmDecision{Delay: debounce, Immediate: false, NewObligation: true},
		},
		{
			name:      "fired immediate/immediate owns a fresh obligation",
			cur:       SettleState{Armed: true, Immediate: true, Fired: true},
			requested: 0,
			want:      SettleArmDecision{Delay: 0, Immediate: true, NewObligation: true},
		},
	}

	seen := map[SettleState]map[time.Duration]bool{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SettleArm(tc.cur, tc.requested); got != tc.want {
				t.Fatalf("SettleArm(%+v, %v) = %+v, want %+v", tc.cur, tc.requested, got, tc.want)
			}
		})
		if seen[tc.cur] == nil {
			seen[tc.cur] = map[time.Duration]bool{}
		}
		seen[tc.cur][tc.requested] = true
	}

	// The table claims to be exhaustive; hold it to that. Fired is meaningless
	// when unarmed, so the reachable state count is 1 + 2*2, each crossed with
	// an immediate and a debounced arm.
	for _, st := range reachableStates() {
		for _, req := range []time.Duration{0, debounce} {
			if !seen[st][req] {
				t.Errorf("no case for state %+v with requested=%v", st, req)
			}
		}
	}
}

// TestSettleArm_DebouncedNeverPostponesImmediate pins rule (a) as a property
// over every live-immediate state: a debounced arm landing on a pending
// immediate one leaves the pass immediate. Last-writer-wins here pushes a
// consumer-visible unavailability window out a full settle delay.
func TestSettleArm_DebouncedNeverPostponesImmediate(t *testing.T) {
	for _, req := range []time.Duration{time.Nanosecond, time.Millisecond, debounce, time.Hour} {
		cur := SettleState{Armed: true, Immediate: true}
		got := SettleArm(cur, req)
		if got.Delay != 0 || !got.Immediate {
			t.Fatalf("debounced arm (%v) over a pending immediate one: got delay=%v immediate=%v, want 0/true",
				req, got.Delay, got.Immediate)
		}
	}
}

// TestSettleArm_ObligationFollowsTheCallback pins rule (b) and its converse as
// one property: an arm owns a FRESH obligation exactly when no in-flight
// callback already owns the pending one. Riding a fired timer's obligation
// double-releases a single increment, drives the pending counter negative and
// wedges every later idle-wait; minting one over a LIVE timer leaks an
// obligation whose callback was displaced and will never run.
func TestSettleArm_ObligationFollowsTheCallback(t *testing.T) {
	for _, st := range reachableStates() {
		for _, req := range []time.Duration{0, debounce} {
			wantFresh := !st.Armed || st.Fired
			if got := SettleArm(st, req).NewObligation; got != wantFresh {
				t.Errorf("SettleArm(%+v, %v).NewObligation = %v, want %v", st, req, got, wantFresh)
			}
		}
	}
}

// reachableStates returns every distinguishable settle state. Fired is
// meaningless without Armed, so the unarmed state appears once.
func reachableStates() []SettleState {
	return []SettleState{
		{},
		{Armed: true},
		{Armed: true, Immediate: true},
		{Armed: true, Fired: true},
		{Armed: true, Immediate: true, Fired: true},
	}
}
