package decide

import (
	"fmt"
	"time"
)

// TombstonePurgeVerdict is the eligibility decision for running a tombstone
// purge pass on a replica position (docs/SPEC.md "Tombstone purge").
type TombstonePurgeVerdict struct {
	// Eligible reports whether a purge pass may run at all under this
	// configuration.
	Eligible bool
	// Reason names why not, for the refusal log. Empty when Eligible.
	Reason string
}

// TombstonePurge decides whether a purge pass may run. grace is the configured
// grace window (zero = the feature is disabled). r is the replication factor
// and requiredAcks the write-ack bar for that r.
//
// The load-bearing rule: the grace window proves an acked delete reached every
// replica ONLY when the ack bar is ALL replicas (requiredAcks == r). Below
// that, a lagging replica may hold the pre-delete value as live KV with
// nothing bounding when it applies the delete, so no finite grace is safe and
// a purged tombstone would let the old value resurrect.
//
// r <= 1 is ineligible for a different reason: single-copy deletes are native
// backend deletes already - no shale tombstone exists to purge.
func TombstonePurge(grace time.Duration, r, requiredAcks int) TombstonePurgeVerdict {
	switch {
	case grace <= 0:
		return TombstonePurgeVerdict{Reason: "disabled (TombstoneGracePeriod is zero)"}
	case r <= 1:
		return TombstonePurgeVerdict{Reason: "single-copy mode: deletes are native, no shale tombstones exist"}
	case requiredAcks < r:
		return TombstonePurgeVerdict{Reason: fmt.Sprintf(
			"write ack bar %d < R %d: a lagging replica could resurrect a purged delete; purge requires WriteConsistency covering all replicas", requiredAcks, r)}
	}
	return TombstonePurgeVerdict{Eligible: true}
}

// TombstoneExpired reports whether a tombstone whose stamp carries
// stampNanos is older than grace at the observer's clock nowNanos. A
// zero/absent stamp is NEVER expired: age cannot be established, and the
// fail-closed direction for a deleter is to keep.
func TombstoneExpired(stampNanos, nowNanos uint64, grace time.Duration) bool {
	if stampNanos == 0 || nowNanos <= stampNanos {
		return false
	}
	return time.Duration(nowNanos-stampNanos) > grace
}
