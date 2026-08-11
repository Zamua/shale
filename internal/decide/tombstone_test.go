package decide

import (
	"testing"
	"time"
)

// The purge-eligibility table: every (grace, r, acks) class and which single
// rule refuses it. The W==R rule is the safety-critical row - purge below a
// full ack bar is how a purged delete resurrects.
func TestTombstonePurge_EligibilityTable(t *testing.T) {
	const h = time.Hour
	cases := []struct {
		name     string
		grace    time.Duration
		r, acks  int
		eligible bool
	}{
		{"disabled by zero grace", 0, 2, 2, false},
		{"disabled by negative grace", -h, 2, 2, false},
		{"single node", h, 1, 1, false},
		{"legacy r0", h, 0, 0, false},
		{"R2 quorum is all: eligible", h, 2, 2, true},
		{"R3 write-all: eligible", h, 3, 3, true},
		{"R3 quorum 2<3: refused", h, 3, 2, false},
		{"R5 quorum 3<5: refused", h, 5, 3, false},
	}
	for _, c := range cases {
		v := TombstonePurge(c.grace, c.r, c.acks)
		if v.Eligible != c.eligible {
			t.Errorf("%s: Eligible = %v, want %v (reason %q)", c.name, v.Eligible, c.eligible, v.Reason)
		}
		if !v.Eligible && v.Reason == "" {
			t.Errorf("%s: refusal carries no reason", c.name)
		}
	}
}

// Expiry is strict age over grace, and an unknowable age never expires: a
// zero stamp or a stamp at/ahead of the observer's clock must be kept - the
// fail-closed direction for a deleter.
func TestTombstoneExpired(t *testing.T) {
	const grace = time.Hour
	now := uint64(10 * time.Hour)
	cases := []struct {
		name  string
		stamp uint64
		want  bool
	}{
		{"well past grace", uint64(1 * time.Hour), true},
		{"exactly at grace boundary", now - uint64(grace), false},
		{"one nano past grace", now - uint64(grace) - 1, true},
		{"younger than grace", now - uint64(30*time.Minute), false},
		{"zero stamp never expires", 0, false},
		{"stamp equal to now", now, false},
		{"stamp ahead of clock (skew)", now + uint64(time.Minute), false},
	}
	for _, c := range cases {
		if got := TombstoneExpired(c.stamp, now, grace); got != c.want {
			t.Errorf("%s: TombstoneExpired = %v, want %v", c.name, got, c.want)
		}
	}
}
