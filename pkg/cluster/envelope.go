// LWW value envelope for replication (v0.4+).
//
// Every Backend value, when R>1, is wrapped in an Envelope before
// storage: a (TimestampNanos, NodeID) Stamp plus the raw payload.
// The cluster layer encodes on Put + decodes on Get; the Backend never
// sees the envelope. On a read fan-out (Quorum / All), the LWW
// comparator picks the winner across replica responses, and async
// read-repair pushes the winner back to lagging replicas.
//
// v0.3 compatibility: a value written before envelopes existed has no
// magic byte. Decode treats such bytes as a payload with a zero Stamp,
// so it loses every comparison against a real stamped write and gets
// re-stamped on the next Put. No offline migration step.
//
// See docs/SPEC.md "Replication (v0.4+) -> Value envelope" + "LWW
// comparator" for the full model.

package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// envelopeMagic is the leading byte that marks bytes as an LWW
// Envelope. Picked arbitrarily; the only requirement is that v0.3
// payloads almost never start with it. Decode treats a missing magic
// as the v0.3 compatibility path.
const envelopeMagic byte = 0xE0

// MaxNodeIDLen bounds Stamp.NodeID so the 2-byte wire-format length
// prefix (uint16, max 65535) can never silently truncate. The limit
// (256 bytes) is the same de-facto max most KV systems use for an
// identifier and leaves enormous headroom for human-readable node IDs
// (DNS-style hostnames, UUIDs, etc.). Cluster.Open rejects
// Config.NodeID longer than this so a never-fits-in-uint16 ID can't
// reach Encode.
const MaxNodeIDLen = 256

// Stamp is the LWW ordering key for a write: the originating node's
// wall-clock at Put time + that node's stable ID. The cluster layer
// stamps once on the originator before fan-out; every replica stores
// the same Stamp for the same write.
type Stamp struct {
	// TimestampNanos is the originating node's time.Now().UnixNano()
	// captured at the moment Put is called. uint64 so the binary
	// encoding is unambiguous (negative pre-1970 timestamps cannot
	// happen here).
	TimestampNanos uint64

	// NodeID is the originator's Config.NodeID. Used as the
	// lexicographic tiebreak when two writes carry identical
	// nanosecond timestamps. The "originator" qualifier is implicit
	// from the surrounding Stamp model (every replica stores the same
	// Stamp for the same write); the field name matches
	// docs/SPEC.md's "Value envelope" section.
	NodeID string
}

// Greater reports whether a's stamp wins LWW against b's. The
// comparator is:
//
//  1. a.TimestampNanos > b.TimestampNanos wins, OR
//  2. timestamps equal AND a.NodeID > b.NodeID
//     lexicographically.
//
// Self-comparison returns false (the relation is strict).
func (a Stamp) Greater(b Stamp) bool {
	if a.TimestampNanos != b.TimestampNanos {
		return a.TimestampNanos > b.TimestampNanos
	}
	return a.NodeID > b.NodeID
}

// Envelope wraps a Backend value with its LWW Stamp + opaque payload.
// Tombstones (Delete) are encoded as an Envelope with an empty
// Payload; Get treats a winning empty-Payload envelope as NotFound.
type Envelope struct {
	Stamp   Stamp
	Payload []byte
}

// Encode serializes env to its on-disk binary form:
//
//	magic(1) | timestamp_be(8) | nodeid_len_be(2) | nodeid_bytes | payload
//
// The payload runs to the end of the buffer (no length prefix needed:
// the storage layer owns the value's total length). Callers must
// ensure Stamp.NodeID is within MaxNodeIDLen; Open enforces this for
// the originator's configured ID.
func Encode(env Envelope) []byte {
	idLen := len(env.Stamp.NodeID)
	out := make([]byte, 0, 1+8+2+idLen+len(env.Payload))
	out = append(out, envelopeMagic)
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], env.Stamp.TimestampNanos)
	out = append(out, tsBuf[:]...)
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(idLen))
	out = append(out, lenBuf[:]...)
	out = append(out, env.Stamp.NodeID...)
	out = append(out, env.Payload...)
	return out
}

// Decode parses an Envelope from its on-disk binary form. Bytes that
// do not start with the magic byte are returned as a zero-Stamp
// envelope carrying the input as Payload - the v0.3 compatibility
// path that lets pre-envelope values participate in LWW (they always
// lose, then get re-stamped on next Put).
//
// Returns an error only when the magic is present but the header is
// truncated or self-inconsistent (e.g. claimed nodeID length runs
// past the end of the buffer). A magic-prefixed buffer with an empty
// payload is valid (that's the tombstone shape).
func Decode(b []byte) (Envelope, error) {
	stamp, payloadStart, err := decodeStamp(b)
	if err != nil {
		return Envelope{}, err
	}
	if payloadStart == 0 {
		// v0.3 compat: hand back the raw bytes as a zero-Stamp
		// payload so the LWW comparator naturally prefers any
		// stamped write. nil and empty both flow through unchanged.
		return Envelope{Payload: b}, nil
	}
	// Copy the payload so callers can mutate it without scribbling
	// into the source buffer (typical Backend.Get contract is "the
	// caller owns the returned slice").
	payload := append([]byte(nil), b[payloadStart:]...)
	return Envelope{Stamp: stamp, Payload: payload}, nil
}

// decodeStamp parses ONLY the Stamp header of an encoded envelope (magic +
// timestamp + nodeID), skipping the payload copy Decode makes. It returns
// exactly what Decode would for the same bytes: a zero Stamp for a v0.3
// bare value (no magic), the identical corruption errors for a truncated
// or self-inconsistent magic-prefixed header. payloadStart is the offset
// the payload begins at; 0 means the no-magic v0.3 case (the whole buffer
// is the payload; a magic-prefixed header is always >= 11 bytes, so 0 is
// unambiguous). The apply-if-newer write paths use this: the stamp compare
// is all they need, since the envelope bytes are written verbatim on
// apply.
func decodeStamp(b []byte) (stamp Stamp, payloadStart int, err error) {
	if len(b) == 0 || b[0] != envelopeMagic {
		return Stamp{}, 0, nil
	}
	// Magic present: header MUST parse cleanly. A truncated envelope
	// is corruption, not v0.3 data.
	if len(b) < 1+8+2 {
		return Stamp{}, 0, errors.New("cluster: envelope header truncated")
	}
	ts := binary.BigEndian.Uint64(b[1:9])
	idLen := int(binary.BigEndian.Uint16(b[9:11]))
	headerEnd := 11 + idLen
	if headerEnd > len(b) {
		return Stamp{}, 0, fmt.Errorf("cluster: envelope nodeID length %d overruns buffer of %d bytes", idLen, len(b))
	}
	return Stamp{TimestampNanos: ts, NodeID: string(b[11:headerEnd])}, headerEnd, nil
}
