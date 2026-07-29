// servingmarker.go: the PURE encode/decode of the serving marker, deliberately
// TAGLESS so it is testable without the slatedb C library.
//
// The rest of the marker path (the object-store round trip) lives in the
// slatedb-tagged factory.go and cannot be built without the Rust
// libslatedb_uniffi. That is fine for I/O, but it must not be where the
// DECISION lives: a bug in this decode wedged a production cluster's drain
// protocol, and a test for it that only runs where the Rust toolchain is
// present is a test that does not run in CI at all.

package slate

import (
	"strconv"
	"strings"

	"github.com/Zamua/shale/pkg/storageunit"
)

// encodeServingMarker renders epoch as the marker payload: a bare decimal.
func encodeServingMarker(epoch storageunit.Epoch) []byte {
	return []byte(strconv.FormatUint(uint64(epoch), 10))
}

// parseServingMarker decodes a marker payload, reporting whether it yielded a
// USABLE epoch. An unparseable payload is reported as ok == false, NOT as an
// error, and that distinction is load-bearing in both directions.
//
// Returning an error for an unreadable marker breaks BOTH halves of the drain
// protocol, and the write half is what makes it unrecoverable:
//
//   - the READER (drainCheck) bails on err before it evaluates the release
//     gate, so the position polls forever without ever comparing epochs;
//   - the WRITER reads first, to honour the monotonic "never lower a recorded
//     marker" guard, and abandons the write on a read error. An unreadable
//     marker therefore permanently blocks its own replacement: the one
//     operation that would repair the position is the one the bad object
//     prevents.
//
// Observed in production. A cluster carried markers in an older encoding (a
// JSON object with owner and heartbeat fields) while the current writer emits a
// bare decimal. Every position with a legacy marker wedged; the two positions
// that happened to carry current-format markers did not. Same cluster, same
// code, same hour, so the encoding was the only variable.
//
// Reporting ok == false is CONSERVATIVE for the reader (a false marker never
// releases a drain, so nothing converges early) and it UNBLOCKS the writer,
// which overwrites the unusable object with the current encoding. The position
// self-heals on its next mount.
//
// Note the adjacent case in the caller already draws exactly this distinction:
// a MISSING object is treated as "no marker written yet", not a failure. An
// unreadable one means the same thing to every consumer of this value, and used
// to be handled the opposite way.
func parseServingMarker(raw []byte) (storageunit.Epoch, bool) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, false
	}
	return storageunit.Epoch(parsed), true
}

// truncMarker bounds an unreadable marker's bytes for the log line. The point
// is to identify the ENCODING at a glance (a JSON object versus a decimal), not
// to dump the payload: the object is operator data of unbounded size and has no
// business reaching logs verbatim.
func truncMarker(raw []byte) string {
	const maxLogged = 64
	if len(raw) <= maxLogged {
		return string(raw)
	}
	return string(raw[:maxLogged]) + "..."
}
