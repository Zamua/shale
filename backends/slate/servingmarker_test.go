package slate

import (
	"strings"
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

// An unreadable serving marker must decode as ABSENT, never as an error.
//
// Returning an error breaks BOTH halves of the drain protocol, and the write
// half is what makes it unrecoverable: the writer reads before writing (to
// honour the monotonic "never lower a recorded marker" guard) and abandons the
// write on a read error, so an unreadable marker permanently blocks its own
// replacement.
//
// This is pinned against the EXACT payload shape observed in production, where
// a cluster carried markers in an older JSON encoding while the current writer
// emits a bare decimal. Every position with a legacy marker wedged; the two
// carrying current-format markers did not. Same cluster, same code, same hour.
//
// This test is deliberately TAGLESS. The marker's object-store round trip lives
// behind the slatedb build tag and cannot be built without the Rust
// libslatedb_uniffi, which CI does not have - so a test for this decision
// written there would never run. The decision therefore lives in a tagless
// file, and so does its test.
func TestParseServingMarker(t *testing.T) {
	legacyJSON := `{"epoch":1,"owner":"hostthis-shard-seed-0","heartbeat_unix_ms":1750276882000}`

	cases := []struct {
		name      string
		raw       string
		wantEpoch storageunit.Epoch
		wantOK    bool
	}{
		{"current encoding", "573", 573, true},
		{"current encoding, zero", "0", 0, true},
		{"trailing newline tolerated", "487\n", 487, true},
		{"legacy JSON marker (the production case)", legacyJSON, 0, false},
		{"empty object", "", 0, false},
		{"whitespace only", "   \n\t", 0, false},
		{"negative", "-5", 0, false},
		{"float", "1.5", 0, false},
		{"garbage", "not-an-epoch", 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotEpoch, gotOK := parseServingMarker([]byte(tc.raw))
			if gotOK != tc.wantOK {
				t.Fatalf("parseServingMarker(%q) ok = %v, want %v", tc.raw, gotOK, tc.wantOK)
			}
			if gotEpoch != tc.wantEpoch {
				t.Fatalf("parseServingMarker(%q) epoch = %d, want %d", tc.raw, gotEpoch, tc.wantEpoch)
			}
		})
	}
}

// What the writer emits must be what the reader accepts. A drift between them
// is precisely the production failure, one encoding-change later.
func TestServingMarkerRoundTrips(t *testing.T) {
	for _, epoch := range []storageunit.Epoch{0, 1, 9, 177, 290, 555, 625, 1 << 40} {
		got, ok := parseServingMarker(encodeServingMarker(epoch))
		if !ok {
			t.Fatalf("epoch %d: encoder output did not parse", epoch)
		}
		if got != epoch {
			t.Fatalf("epoch %d round-tripped to %d", epoch, got)
		}
	}
}

// The legacy encoding must NOT parse. If it ever did, the production diagnosis
// (unreadable marker blocks its own replacement) would be void, and this whole
// fix would be addressing a bug that does not exist.
func TestLegacyMarkerDoesNotParse(t *testing.T) {
	legacy := []byte(`{"epoch":1,"owner":"x","heartbeat_unix_ms":1}`)
	if _, ok := parseServingMarker(legacy); ok {
		t.Fatal("legacy JSON marker parsed as an epoch; the production diagnosis " +
			"rests on it NOT parsing, so this fix would be addressing nothing")
	}
}

// An unreadable object is operator data of unbounded size. Identifying the
// encoding at a glance is the goal; dumping the payload is not.
func TestTruncMarker(t *testing.T) {
	short := []byte(`{"epoch":1}`)
	if got := truncMarker(short); got != string(short) {
		t.Fatalf("short payload altered: %q", got)
	}

	long := make([]byte, 500)
	for i := range long {
		long[i] = 'a'
	}
	got := truncMarker(long)
	if len(got) > 70 {
		t.Fatalf("long payload not bounded: %d bytes would reach the log", len(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("truncation not marked: %q", got)
	}
}
