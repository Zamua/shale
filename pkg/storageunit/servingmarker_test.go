package storageunit

import (
	"testing"
)

func TestServingMarker_NewFormatRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		m    ServingMarker
	}{
		{"zero", ServingMarker{}},
		{"typical", ServingMarker{Epoch: 7, Owner: "node-a", HeartbeatUnixMs: 1_700_000_000_123}},
		{"empty-owner-nonzero-epoch", ServingMarker{Epoch: 42, Owner: "", HeartbeatUnixMs: 99}},
		{"large-epoch", ServingMarker{Epoch: Epoch(^uint64(0)), Owner: "n", HeartbeatUnixMs: 1}},
		{"owner-with-slashes", ServingMarker{Epoch: 3, Owner: "g1/u5/r0-host", HeartbeatUnixMs: 5}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.m.Marshal()
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			got, err := ParseServingMarker(b)
			if err != nil {
				t.Fatalf("ParseServingMarker: %v", err)
			}
			if got != tc.m {
				t.Fatalf("round-trip mismatch: got %+v want %+v", got, tc.m)
			}
		})
	}
}

func TestServingMarker_NewFormatIsJSON(t *testing.T) {
	// A new-format payload must NOT parse as a bare decimal (so the read-side
	// sniff routes it through the JSON branch, not the legacy branch).
	m := ServingMarker{Epoch: 9, Owner: "node-x", HeartbeatUnixMs: 123}
	b, err := m.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if looksLikeLegacyEpoch(b) {
		t.Fatalf("new-format payload %q should not look like a legacy bare epoch", b)
	}
}

func TestServingMarker_ParseLegacyBareEpoch(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    ServingMarker
	}{
		{"plain", "7", ServingMarker{Epoch: 7, Owner: "", HeartbeatUnixMs: 0}},
		{"zero", "0", ServingMarker{Epoch: 0, Owner: "", HeartbeatUnixMs: 0}},
		{"whitespace", "  42\n", ServingMarker{Epoch: 42, Owner: "", HeartbeatUnixMs: 0}},
		{"max-uint64", "18446744073709551615", ServingMarker{Epoch: Epoch(^uint64(0))}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseServingMarker([]byte(tc.payload))
			if err != nil {
				t.Fatalf("ParseServingMarker(%q): %v", tc.payload, err)
			}
			if got != tc.want {
				t.Fatalf("legacy parse %q: got %+v want %+v", tc.payload, got, tc.want)
			}
		})
	}
}

func TestServingMarker_ParseErrors(t *testing.T) {
	cases := []struct {
		name    string
		payload string
	}{
		{"empty", ""},
		{"blank", "   \n"},
		{"garbage", "not-a-number-not-json"},
		{"negative", "-5"},
		{"truncated-json", `{"epoch":7,`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseServingMarker([]byte(tc.payload)); err == nil {
				t.Fatalf("ParseServingMarker(%q): want error, got nil", tc.payload)
			}
		})
	}
}

func TestServingMarker_IsStale(t *testing.T) {
	const window = 1000 // ms
	live := map[string]bool{"node-a": true, "node-b": true}

	cases := []struct {
		name      string
		m         ServingMarker
		now       int64
		liveSet   map[string]bool
		wantStale bool
	}{
		{
			name:      "live-and-fresh",
			m:         ServingMarker{Epoch: 1, Owner: "node-a", HeartbeatUnixMs: 5000},
			now:       5500,
			liveSet:   live,
			wantStale: false,
		},
		{
			name:      "empty-owner-is-stale",
			m:         ServingMarker{Epoch: 1, Owner: "", HeartbeatUnixMs: 5000},
			now:       5001,
			liveSet:   live,
			wantStale: true,
		},
		{
			name:      "owner-not-in-live-set-is-stale",
			m:         ServingMarker{Epoch: 1, Owner: "node-ghost", HeartbeatUnixMs: 5000},
			now:       5001,
			liveSet:   live,
			wantStale: true,
		},
		{
			name:      "heartbeat-too-old-is-stale",
			m:         ServingMarker{Epoch: 1, Owner: "node-a", HeartbeatUnixMs: 5000},
			now:       6001, // 1001ms > window
			liveSet:   live,
			wantStale: true,
		},
		{
			name:      "heartbeat-exactly-at-window-is-fresh",
			m:         ServingMarker{Epoch: 1, Owner: "node-a", HeartbeatUnixMs: 5000},
			now:       6000, // exactly window: now-hb == window, not strictly greater
			liveSet:   live,
			wantStale: false,
		},
		{
			name:      "nil-live-set-treats-any-owner-as-not-live",
			m:         ServingMarker{Epoch: 1, Owner: "node-a", HeartbeatUnixMs: 5000},
			now:       5001,
			liveSet:   nil,
			wantStale: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.m.IsStale(tc.now, window, tc.liveSet)
			if got != tc.wantStale {
				t.Fatalf("IsStale=%v want %v (m=%+v now=%d)", got, tc.wantStale, tc.m, tc.now)
			}
		})
	}
}

func TestServingMarker_NewerEpochThan(t *testing.T) {
	cases := []struct {
		name string
		a    ServingMarker
		b    ServingMarker
		want bool
	}{
		{"strictly-greater", ServingMarker{Epoch: 5}, ServingMarker{Epoch: 4}, true},
		{"equal-not-greater", ServingMarker{Epoch: 5}, ServingMarker{Epoch: 5}, false},
		{"lower-not-greater", ServingMarker{Epoch: 3}, ServingMarker{Epoch: 4}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.NewerEpochThan(tc.b); got != tc.want {
				t.Fatalf("NewerEpochThan=%v want %v", got, tc.want)
			}
		})
	}
}
