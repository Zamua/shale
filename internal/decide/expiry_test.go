package decide

import (
	"maps"
	"slices"
	"testing"
)

const self = "self"

// TestExpireSilent enumerates one observation pass over every way a row can
// relate to what the observer last recorded about it.
func TestExpireSilent(t *testing.T) {
	cases := []struct {
		name        string
		prev        map[string]LeaseObservation
		rows        []LeaseRow
		k           int
		wantNext    map[string]LeaseObservation
		wantExpired []string
	}{
		{
			name: "a member new to the view starts fresh: its first pass is a baseline",
			prev: map[string]LeaseObservation{},
			rows: []LeaseRow{{ID: "a", Inc: "i1", Gen: 9}},
			k:    1,
			wantNext: map[string]LeaseObservation{
				"a": {Inc: "i1", Gen: 9},
			},
		},
		{
			name: "an advancing member stays live and its count resets",
			prev: map[string]LeaseObservation{"a": {Inc: "i1", Gen: 4, Stale: 2}},
			rows: []LeaseRow{{ID: "a", Inc: "i1", Gen: 5}},
			k:    3,
			wantNext: map[string]LeaseObservation{
				"a": {Inc: "i1", Gen: 5},
			},
		},
		{
			name: "a frozen member counts one pass of silence and stays live below K",
			prev: map[string]LeaseObservation{"a": {Inc: "i1", Gen: 5, Stale: 1}},
			rows: []LeaseRow{{ID: "a", Inc: "i1", Gen: 5}},
			k:    3,
			wantNext: map[string]LeaseObservation{
				"a": {Inc: "i1", Gen: 5, Stale: 2},
			},
		},
		{
			name: "at K the member expires, and keeps its count so a lost GC race does not restart it",
			prev: map[string]LeaseObservation{"a": {Inc: "i1", Gen: 5, Stale: 2}},
			rows: []LeaseRow{{ID: "a", Inc: "i1", Gen: 5}},
			k:    3,
			wantNext: map[string]LeaseObservation{
				"a": {Inc: "i1", Gen: 5, Stale: 3},
			},
			wantExpired: []string{"a"},
		},
		{
			name: "a member that RE-APPEARS with a new incarnation resets even at an EQUAL counter",
			// The ABA hole: a GC'd member rejoining restarts its counter at 1,
			// which can equal the counter last tracked.
			prev: map[string]LeaseObservation{"a": {Inc: "i1", Gen: 1, Stale: 2}},
			rows: []LeaseRow{{ID: "a", Inc: "i2", Gen: 1}},
			k:    3,
			wantNext: map[string]LeaseObservation{
				"a": {Inc: "i2", Gen: 1},
			},
		},
		{
			name: "self never expires however long its row sits unchanged",
			prev: map[string]LeaseObservation{self: {Inc: "i1", Gen: 1, Stale: 99}},
			rows: []LeaseRow{{ID: self, Inc: "i1", Gen: 1}},
			k:    1,
			// Self is not tracked at all: the exemption is structural, not a
			// filter applied to an expiry the fold already computed.
			wantNext: map[string]LeaseObservation{},
		},
		{
			name: "a row gone from the document leaves the state, so a return starts fresh",
			prev: map[string]LeaseObservation{
				"a": {Inc: "i1", Gen: 5, Stale: 2},
				"b": {Inc: "i9", Gen: 3, Stale: 1},
			},
			rows: []LeaseRow{{ID: "a", Inc: "i1", Gen: 6}},
			k:    3,
			wantNext: map[string]LeaseObservation{
				"a": {Inc: "i1", Gen: 6},
			},
		},
		{
			name: "one pass judges each member on its own record",
			prev: map[string]LeaseObservation{
				"a": {Inc: "i1", Gen: 5, Stale: 1},
				"b": {Inc: "i2", Gen: 7, Stale: 1},
				"c": {Inc: "i3", Gen: 2, Stale: 1},
			},
			rows: []LeaseRow{
				{ID: "a", Inc: "i1", Gen: 5},   // frozen: expires
				{ID: "b", Inc: "i2", Gen: 8},   // renewed
				{ID: self, Inc: "s", Gen: 1},   // exempt
				{ID: "c", Inc: "i3", Gen: 2},   // frozen: expires
				{ID: "d", Inc: "i4", Gen: 100}, // new
			},
			k: 2,
			wantNext: map[string]LeaseObservation{
				"a": {Inc: "i1", Gen: 5, Stale: 2},
				"b": {Inc: "i2", Gen: 8},
				"c": {Inc: "i3", Gen: 2, Stale: 2},
				"d": {Inc: "i4", Gen: 100},
			},
			wantExpired: []string{"a", "c"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next, expired := ExpireSilent(tc.prev, tc.rows, tc.k, self)
			if !maps.Equal(next, tc.wantNext) {
				t.Errorf("state = %v, want %v", next, tc.wantNext)
			}
			if !slices.Equal(expired, tc.wantExpired) {
				t.Errorf("expired = %v, want %v", expired, tc.wantExpired)
			}
		})
	}
}

// TestExpireSilent_ExpiresAtExactlyK drives consecutive passes over an
// unchanged row: the member must survive K-1 of them and expire on the Kth.
// Detection latency is K*pollInterval, so an off-by-one here is a member
// reaped a whole poll early or carried a poll too long.
func TestExpireSilent_ExpiresAtExactlyK(t *testing.T) {
	for k := 1; k <= 5; k++ {
		state := map[string]LeaseObservation{}
		rows := []LeaseRow{{ID: "a", Inc: "i1", Gen: 3}}
		// The first pass is the baseline: silence is only counted from the
		// second pass on, so expiry lands on pass k+1.
		for pass := 1; pass <= k; pass++ {
			var expired []string
			state, expired = ExpireSilent(state, rows, k, self)
			if len(expired) != 0 {
				t.Fatalf("K=%d: expired %v on pass %d, want live through pass %d", k, expired, pass, k)
			}
		}
		_, expired := ExpireSilent(state, rows, k, self)
		if !slices.Equal(expired, []string{"a"}) {
			t.Fatalf("K=%d: expired %v on pass %d, want [a]", k, expired, k+1)
		}
	}
}

// TestExpireSilent_RenewalKeepsAMemberLiveForever pins that no run of passes
// expires a member that keeps advancing: the judgment is a rate, so the
// observer's own pass count can never be what condemns a live member.
func TestExpireSilent_RenewalKeepsAMemberLiveForever(t *testing.T) {
	state := map[string]LeaseObservation{}
	for pass := 1; pass <= 50; pass++ {
		rows := []LeaseRow{{ID: "a", Inc: "i1", Gen: uint64(pass)}}
		var expired []string
		state, expired = ExpireSilent(state, rows, 3, self)
		if len(expired) != 0 {
			t.Fatalf("pass %d: expired %v while the member was renewing", pass, expired)
		}
	}
}

// TestExpireSilent_SelfNeverExpires pins the self exemption over a run far
// past K with self's row untouched: a node always believes itself alive, and a
// view that dropped self would silently release every unit it holds.
func TestExpireSilent_SelfNeverExpires(t *testing.T) {
	state := map[string]LeaseObservation{}
	rows := []LeaseRow{{ID: self, Inc: "s", Gen: 1}}
	for pass := 1; pass <= 20; pass++ {
		var expired []string
		state, expired = ExpireSilent(state, rows, 2, self)
		if len(expired) != 0 {
			t.Fatalf("pass %d: expired %v, self must be exempt", pass, expired)
		}
	}
	if len(state) != 0 {
		t.Fatalf("self was tracked: %v", state)
	}
}

// TestExpireSilent_RejoinAfterGCSurvives walks the whole ABA sequence: a
// member expires, its row is GC'd, and it rejoins with a fresh incarnation at
// a counter EQUAL to the one last tracked. Judged on the counter alone the
// observer would keep it expired and reap every row it writes.
func TestExpireSilent_RejoinAfterGCSurvives(t *testing.T) {
	const k = 2
	state := map[string]LeaseObservation{}
	rows := []LeaseRow{{ID: "a", Inc: "i1", Gen: 1}}
	var expired []string
	for pass := 1; pass <= k+1; pass++ {
		state, expired = ExpireSilent(state, rows, k, self)
	}
	if !slices.Equal(expired, []string{"a"}) {
		t.Fatalf("expired = %v, want [a] before the GC", expired)
	}

	// GC removes the row, then the member rejoins at leaseGen 1 under a fresh
	// incarnation.
	state, expired = ExpireSilent(state, nil, k, self)
	if len(expired) != 0 || len(state) != 0 {
		t.Fatalf("after the GC: state=%v expired=%v, want both empty", state, expired)
	}
	for pass := 1; pass <= k+1; pass++ {
		rows = []LeaseRow{{ID: "a", Inc: "i2", Gen: uint64(pass)}}
		state, expired = ExpireSilent(state, rows, k, self)
		if len(expired) != 0 {
			t.Fatalf("pass %d after the rejoin: expired %v, want the fresh incarnation live", pass, expired)
		}
	}
}
