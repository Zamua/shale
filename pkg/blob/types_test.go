package blob

// Unit tests for the PURE domain types: no I/O, no MinIO. These always run with
// plain `go test ./pkg/blob/...` (no build tags, no env var). They pin the
// pointer wire format, the UNIT-keyed key-layout helpers, and blob-id
// generation.

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPointer_EncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		ptr  Pointer
	}{
		{
			name: "full",
			ptr:  Pointer{ObjKey: FinalKey("0-7", "deadbeef"), Size: 1 << 20, ContentHash: "abc123"},
		},
		{
			name: "no content hash",
			ptr:  Pointer{ObjKey: FinalKey("0-0", "cafef00d"), Size: 42},
		},
		{
			name: "zero size",
			ptr:  Pointer{ObjKey: FinalKey("legacy", "id"), Size: 0},
		},
		{
			name: "size unknown sentinel preserved",
			ptr:  Pointer{ObjKey: FinalKey("0-9", "xyz"), Size: SizeUnknown},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.ptr.Encode()
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := DecodePointer(b)
			if err != nil {
				t.Fatalf("DecodePointer: %v", err)
			}
			if got != tc.ptr {
				t.Fatalf("round-trip mismatch:\n got  %+v\n want %+v", got, tc.ptr)
			}
		})
	}
}

// TestPointer_ContentHashOmitted pins that ContentHash uses `omitempty`: an
// empty hash must not appear in the wire bytes (keeps the persisted pointer
// small) but still decodes back to the empty string.
func TestPointer_ContentHashOmitted(t *testing.T) {
	b, err := Pointer{ObjKey: "blob/s/i", Size: 10}.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if strings.Contains(string(b), "content_hash") {
		t.Fatalf("empty ContentHash should be omitted from wire bytes, got %s", b)
	}
	got, err := DecodePointer(b)
	if err != nil {
		t.Fatalf("DecodePointer: %v", err)
	}
	if got.ContentHash != "" {
		t.Fatalf("ContentHash = %q, want empty", got.ContentHash)
	}
}

// TestDecodePointer_RejectsUnknownVersion pins the fail-closed guarantee: a
// pointer encoded at a higher (unknown) schema version must error rather than
// silently dropping a field the node does not understand.
func TestDecodePointer_RejectsUnknownVersion(t *testing.T) {
	// Hand-roll an envelope at a version above the current one.
	future, err := json.Marshal(struct {
		V       int     `json:"v"`
		Pointer Pointer `json:"ptr"`
	}{V: pointerSchemaVersion + 1, Pointer: Pointer{ObjKey: "blob/s/i", Size: 1}})
	if err != nil {
		t.Fatalf("marshal future envelope: %v", err)
	}
	if _, err := DecodePointer(future); err == nil {
		t.Fatal("DecodePointer of a higher-version pointer should fail closed, got nil error")
	}
}

// TestDecodePointer_RejectsGarbage pins that non-JSON bytes error rather than
// yielding a zero-value pointer.
func TestDecodePointer_RejectsGarbage(t *testing.T) {
	if _, err := DecodePointer([]byte("not json")); err == nil {
		t.Fatal("DecodePointer of garbage should error, got nil")
	}
}

// TestDecodePointer_DefaultVersionRejected guards the zero-version case: a bare
// `{}` decodes to V=0, which is not the supported version, so it must fail
// closed (a missing/zero version is not silently treated as v1).
func TestDecodePointer_DefaultVersionRejected(t *testing.T) {
	if _, err := DecodePointer([]byte(`{}`)); err == nil {
		t.Fatal("DecodePointer of an empty object (V=0) should fail closed, got nil")
	}
}

func TestFinalKey(t *testing.T) {
	// unit tokens are rendered <gen>-<unitID> (e.g. "0-13") by the cluster, or
	// the "legacy" sentinel; FinalKey treats the unit as an opaque string.
	got := FinalKey("0-13", "deadbeef")
	want := "blob/0-13/deadbeef"
	if got != want {
		t.Fatalf("FinalKey = %q, want %q", got, want)
	}
	if !strings.HasPrefix(got, FinalPrefix) {
		t.Fatalf("FinalKey %q must start with FinalPrefix %q", got, FinalPrefix)
	}
}

func TestNewBlobID(t *testing.T) {
	id1, err := NewBlobID()
	if err != nil {
		t.Fatalf("NewBlobID: %v", err)
	}
	// 16 bytes hex-encoded = 32 hex chars.
	if len(id1) != 32 {
		t.Fatalf("NewBlobID len = %d, want 32 hex chars (128 bits)", len(id1))
	}
	for _, c := range id1 {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("NewBlobID produced non-hex char %q in %q", c, id1)
		}
	}
	// Fresh ids must not collide (sanity on the randomness, not a statistical claim).
	id2, err := NewBlobID()
	if err != nil {
		t.Fatalf("NewBlobID (2): %v", err)
	}
	if id1 == id2 {
		t.Fatalf("two NewBlobID calls collided: %q", id1)
	}
}
