// Monotone per-node stamp source with an observed-stamp ratchet.
//
// Every envelope Stamp this node ORIGINATES (single-key Put fan-out,
// tombstone Delete, the CAS shared commit stamp) draws its
// TimestampNanos from stampClock.Next() instead of raw
// time.Now().UnixNano(). Next() is strictly monotonic and unique per
// call: max(wall clock, last+1), where last is the highest value this
// node has ever ISSUED or OBSERVED. Observe(ts) ratchets last up to any
// stamp the node sees on a read (LWW winner selection), a CAS read-set
// validate (the stored envelope's stamp), or a replica-receiving apply
// (the incoming and stored stamps in the apply-if-newer compare).
//
// Why: with raw wall clocks, a stored envelope stamped T1 by a faster
// clock makes an owner whose clock reads T0 < T1 issue a commit stamp
// that LOSES the LWW compare to the very value it validated against and
// replaced. The owner applies its own CAS commit authoritatively, ACKs,
// but every replica's apply-if-newer rejects the T0 stamp; the next
// quorum read picks the OLD value and read-repair pushes it back over
// the owner's committed write. The acked, validated commit is silently
// un-committed. The ratchet closes that: the validate path Observes T1
// before the commit stamp is issued, so the commit stamp strictly
// exceeds every stamp in the validated read-set and can never lose to
// the value it replaced. See docs/SPEC.md "LWW comparator" +
// "Write-set replication".
//
// The high-water mark is in-memory only: a fresh process starts at zero
// and re-ratchets as it observes stamps. Durable persistence of the
// mark is possible future work (see the residual note in docs/SPEC.md).

package cluster

import (
	"sync/atomic"
	"time"
)

// stampClock is a per-Cluster monotone source for Stamp.TimestampNanos.
// Zero-value ready: last starts at 0 and nowFn nil means the real wall
// clock. All methods are safe for concurrent use.
type stampClock struct {
	// last is the highest nanosecond value this node has issued or
	// observed. Next() always returns at least last+1.
	last atomic.Uint64

	// nowFn is the wall-clock source, injectable by unit tests. nil
	// means time.Now().UnixNano(). Set only before first use; it is
	// never mutated once the clock is in service (no lock needed).
	nowFn func() int64
}

// wallNow reads the wall clock as a uint64 nanosecond count. A negative
// reading (pre-1970, only reachable via an injected test clock) clamps
// to 0 so the uint64 conversion cannot wrap to a huge value.
func (s *stampClock) wallNow() uint64 {
	var n int64
	if s.nowFn != nil {
		n = s.nowFn()
	} else {
		n = time.Now().UnixNano()
	}
	if n < 0 {
		return 0
	}
	return uint64(n)
}

// Next returns the next stamp: max(wall clock, last+1). Strictly
// monotonic and unique per call on this node, even across a wall-clock
// regression (NTP step, VM migration): the clock can stall the wall
// component, but last+1 keeps every issued stamp strictly above
// everything previously issued or observed.
func (s *stampClock) Next() uint64 {
	for {
		next := s.wallNow()
		last := s.last.Load()
		if next <= last {
			next = last + 1
		}
		if s.last.CompareAndSwap(last, next) {
			return next
		}
	}
}

// Observe ratchets the clock so no future Next() returns a value at or
// below ts (an atomic max; a no-op when ts is not ahead). Called with
// every stamp this node sees: the LWW winner on a read, each stored
// envelope stamp a CAS validate decodes, and the incoming + stored
// stamps on a replica-receiving apply.
func (s *stampClock) Observe(ts uint64) {
	for {
		last := s.last.Load()
		if ts <= last {
			return
		}
		if s.last.CompareAndSwap(last, ts) {
			return
		}
	}
}
