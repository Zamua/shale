package blob

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// These domain types are PURE: no I/O, no object-store types, no imports of the
// infrastructure layer. They are safe to construct and test without a store.

// Pointer is the small record phase 2 stores as a slatedb value on the
// owning shard, transactionally co-committed with the app's metadata. It is the
// ONLY durable reference to a blob's bytes (value separation: the LSM holds the
// pointer, the object store holds the bytes). Keep it small - it lives inline in
// the LSM like any other small value.
//
// Encode/Decode use JSON to match how shale encodes its other small durable
// records (see pkg/reshard's epoch State). The schema is versioned so a node
// reading a newer-than-it-understands pointer fails closed rather than silently
// dropping a field it does not know.
type Pointer struct {
	// ObjKey is the FINAL object key the bytes live at (built by FinalKey).
	ObjKey string `json:"objkey"`
	// Size is the stored byte length (the compressed length when compressed).
	Size int64 `json:"size"`
	// ContentHash is an OPTIONAL app-supplied hash of the ORIGINAL (pre-compression)
	// bytes - e.g. a hex sha256 the app computes in the upload tee for integrity
	// or within-record dedup. Empty when the app supplies none. Shale does not
	// interpret it; it is carried verbatim.
	ContentHash string `json:"content_hash,omitempty"`
}

// pointerSchemaVersion is the wire-format version of the encoded Pointer. A
// node that decodes a pointer at an UNKNOWN (higher) version fails closed
// (Decode returns an error) rather than misinterpreting a field it does not
// understand. Bump only with a forward/backward-compatibility plan.
const pointerSchemaVersion = 1

// pointerEnvelope wraps a Pointer with its schema version on the wire. The
// app never sees this; it is purely the persisted encoding.
type pointerEnvelope struct {
	V       int     `json:"v"`
	Pointer Pointer `json:"ptr"`
}

// Encode serializes the pointer for storage as a small slatedb value.
func (p Pointer) Encode() ([]byte, error) {
	b, err := json.Marshal(pointerEnvelope{V: pointerSchemaVersion, Pointer: p})
	if err != nil {
		return nil, fmt.Errorf("blob: encode pointer: %w", err)
	}
	return b, nil
}

// DecodePointer parses a pointer previously produced by Pointer.Encode. It
// fails closed on an unknown (higher) schema version.
func DecodePointer(data []byte) (Pointer, error) {
	var env pointerEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Pointer{}, fmt.Errorf("blob: decode pointer: %w", err)
	}
	if env.V != pointerSchemaVersion {
		return Pointer{}, fmt.Errorf("blob: pointer schema version %d unsupported (want %d)", env.V, pointerSchemaVersion)
	}
	return env.Pointer, nil
}

// Object key layout (design section 5 + section 10.1).
//
// Blob bytes live under the OWNING SHARD's namespace so the owner can enumerate
// "its" blobs for the same-shard orphan sweep:
//
//	blob/<shardKey>/<blobid>
//
// There is NO separate staging namespace: phase 2's StageBlob streams the bytes
// DIRECTLY to this final, shard-keyed key (the caller knows the slug, hence the
// shard, at upload time), and the binding transaction only commits the pointer.
// Bytes sitting here before the pointer commits are unreachable to any reader
// (a reader reaches them only via the committed metadata -> pointer), so a crash
// before the bind leaves a SHARD-LOCAL orphan that the age-gated same-shard
// sweep reclaims. The prefix ends in '/' so a List(prefix) enumerates exactly
// the shard's namespace.
const (
	// FinalPrefix is the namespace root for blob bytes.
	FinalPrefix = "blob/"
)

// FinalKey builds the object key for a blob owned by shardKey. blobid is
// shale-internal (a fresh NewBlobID, or the content sha when the app keys by
// content for within-record dedup).
func FinalKey(shardKey, blobid string) string {
	return FinalPrefix + shardKey + "/" + blobid
}

// FinalPrefixForShard builds the List prefix that enumerates every blob object
// owned by shardKey - the input to the same-shard orphan sweep (phase 2).
func FinalPrefixForShard(shardKey string) string {
	return FinalPrefix + shardKey + "/"
}

// NewBlobID returns a fresh, random, URL-safe blob id (128 bits of crypto/rand
// hex). shale uses crypto/rand elsewhere for random payloads; this keeps id
// generation dependency-free. The app may instead key a blob by its content sha
// (passed to FinalKey) when it wants within-record dedup.
func NewBlobID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("blob: generate id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
