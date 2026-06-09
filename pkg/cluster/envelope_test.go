package cluster_test

import (
	"bytes"
	"testing"

	"github.com/Zamua/shale/pkg/cluster"
)

func TestEnvelope_RoundTrip(t *testing.T) {
	want := cluster.Envelope{
		Stamp: cluster.Stamp{
			TimestampNanos: 1_717_000_000_000_000_000,
			NodeID:         "node-2",
		},
		Payload: []byte("hello, world"),
	}
	encoded := cluster.Encode(want)
	got, err := cluster.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode after Encode: %v", err)
	}
	if got.Stamp != want.Stamp {
		t.Errorf("stamp mismatch: got %+v, want %+v", got.Stamp, want.Stamp)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("payload mismatch: got %q, want %q", got.Payload, want.Payload)
	}
}

func TestEnvelope_RoundTrip_EmptyPayload(t *testing.T) {
	// Tombstone shape: stamp set, payload empty. Must round-trip
	// without colliding with the v0.3-compat zero-stamp path.
	want := cluster.Envelope{
		Stamp:   cluster.Stamp{TimestampNanos: 42, NodeID: "n1"},
		Payload: nil,
	}
	got, err := cluster.Decode(cluster.Encode(want))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Stamp != want.Stamp {
		t.Errorf("stamp mismatch: got %+v, want %+v", got.Stamp, want.Stamp)
	}
	if len(got.Payload) != 0 {
		t.Errorf("payload should be empty, got %q", got.Payload)
	}
}

func TestEnvelope_RoundTrip_EmptyNodeID(t *testing.T) {
	// Edge case: nodeID-length-prefix 0 must encode + decode cleanly.
	want := cluster.Envelope{
		Stamp:   cluster.Stamp{TimestampNanos: 1, NodeID: ""},
		Payload: []byte("v"),
	}
	got, err := cluster.Decode(cluster.Encode(want))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Stamp != want.Stamp {
		t.Errorf("stamp mismatch: got %+v, want %+v", got.Stamp, want.Stamp)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("payload mismatch: got %q, want %q", got.Payload, want.Payload)
	}
}

func TestDecode_MissingMagic_V03Compat(t *testing.T) {
	// Pre-v0.4 stored bytes have no magic. Decode must return a
	// zero-stamp envelope with the bytes as Payload, so a real
	// stamped write trivially wins LWW + the next Put re-stamps.
	raw := []byte("bare v0.3 value")
	got, err := cluster.Decode(raw)
	if err != nil {
		t.Fatalf("Decode bare bytes: %v", err)
	}
	if (got.Stamp != cluster.Stamp{}) {
		t.Errorf("missing-magic decode should yield zero Stamp, got %+v", got.Stamp)
	}
	if !bytes.Equal(got.Payload, raw) {
		t.Errorf("missing-magic decode should preserve payload, got %q want %q", got.Payload, raw)
	}
}

func TestDecode_EmptyBytes_V03Compat(t *testing.T) {
	got, err := cluster.Decode(nil)
	if err != nil {
		t.Fatalf("Decode nil: %v", err)
	}
	if (got.Stamp != cluster.Stamp{}) {
		t.Errorf("nil decode should yield zero Stamp, got %+v", got.Stamp)
	}
	if len(got.Payload) != 0 {
		t.Errorf("nil decode should yield empty payload, got %q", got.Payload)
	}
}

func TestDecode_TruncatedHeader(t *testing.T) {
	// Magic byte present but header chopped: corruption, not v0.3
	// data. Must error rather than silently re-interpreting.
	truncated := []byte{0xE0, 0x00, 0x00}
	if _, err := cluster.Decode(truncated); err == nil {
		t.Fatalf("Decode of truncated envelope should error")
	}
}

func TestDecode_NodeIDOverrunsBuffer(t *testing.T) {
	// Magic + ts + nodeID-len claiming 99 bytes, but only 2 bytes of
	// nodeID actually present. Header is self-inconsistent: must
	// error.
	bad := []byte{0xE0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 99, 'a', 'b'}
	if _, err := cluster.Decode(bad); err == nil {
		t.Fatalf("Decode with overrun nodeID length should error")
	}
}

func TestStamp_Greater_TimestampWins(t *testing.T) {
	older := cluster.Stamp{TimestampNanos: 100, NodeID: "z"}
	newer := cluster.Stamp{TimestampNanos: 200, NodeID: "a"}
	if !newer.Greater(older) {
		t.Errorf("newer.Greater(older) should be true regardless of nodeID")
	}
	if older.Greater(newer) {
		t.Errorf("older.Greater(newer) should be false even with lex-greater nodeID")
	}
}

func TestStamp_Greater_NodeIDTiebreak(t *testing.T) {
	a := cluster.Stamp{TimestampNanos: 100, NodeID: "node-b"}
	b := cluster.Stamp{TimestampNanos: 100, NodeID: "node-a"}
	if !a.Greater(b) {
		t.Errorf("equal-ts: lex-greater nodeID should win (node-b > node-a)")
	}
	if b.Greater(a) {
		t.Errorf("equal-ts: lex-smaller nodeID should lose")
	}
}

func TestStamp_Greater_SelfNotGreater(t *testing.T) {
	// Strict comparison: a stamp is not Greater than itself.
	s := cluster.Stamp{TimestampNanos: 100, NodeID: "n1"}
	if s.Greater(s) {
		t.Errorf("Greater is strict: s.Greater(s) must be false")
	}
}

func TestStamp_Greater_ZeroStampLoses(t *testing.T) {
	// The v0.3-compat path produces a zero Stamp; any real stamp
	// must beat it.
	zero := cluster.Stamp{}
	real_ := cluster.Stamp{TimestampNanos: 1, NodeID: ""}
	if !real_.Greater(zero) {
		t.Errorf("real stamp should beat zero stamp")
	}
	if zero.Greater(real_) {
		t.Errorf("zero stamp should not beat real stamp")
	}
}
