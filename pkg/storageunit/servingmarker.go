package storageunit

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ServingMarker is the durable, poll-observable LEASE that records which node
// is currently serving a replica position (v0.8 Phase 2g). It is the value
// object behind the serving-marker object a node writes at its mount flip and
// peers read WITHOUT opening the position's slatedb database (fence-free).
//
// It carries LIVENESS so a booting node can tell a LIVE owner from a DEAD one:
//
//   - Epoch is the owner's exact open epoch (the strictly-monotone writer fence
//     from OpenReplicaUnit). Unchanged in meaning from the Phase 2e bare-epoch
//     marker; it is still the field the strict-`>` release gate compares.
//   - Owner is the NodeID of the node currently serving the position (sourced
//     from the cluster's own NodeID, the same identity ring + membership use).
//     An empty Owner means "owner unknown" - a legacy bare-epoch marker decodes
//     this way, and the staleness predicate treats it as stale (safe default).
//   - HeartbeatUnixMs is the wall-clock time (Unix milliseconds) the owner last
//     refreshed the lease. The serving owner rewrites it periodically on the
//     reconcile cadence; an age beyond the lease window marks the lease stale.
//
// It is a PURE value object: no I/O, no clock. now / leaseWindow / the live
// member set are passed in by the caller so the predicate is deterministic and
// testable.
type ServingMarker struct {
	Epoch           Epoch  `json:"epoch"`
	Owner           string `json:"owner"`
	HeartbeatUnixMs int64  `json:"heartbeat_unix_ms"`
}

// errEmptyMarker is returned when a marker payload is empty / blank. An absent
// object is signalled by the read layer as "not present", never as an empty
// payload, so an empty payload is a genuine corruption and must fail loudly.
var errEmptyMarker = errors.New("storageunit: empty serving-marker payload")

// Marshal renders the marker as the NEW-format wire payload (compact JSON). The
// JSON object never looks like a bare decimal (it starts with '{'), so the
// read-side sniff in ParseServingMarker routes it through the JSON branch.
func (m ServingMarker) Marshal() ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("storageunit: marshal serving marker: %w", err)
	}
	return b, nil
}

// ParseServingMarker decodes a serving-marker payload, accepting BOTH formats:
//
//   - the NEW format: a JSON object {epoch, owner, heartbeat_unix_ms}.
//   - the LEGACY format (Phase 2e): a bare decimal epoch as text/plain. It
//     decodes to {Epoch: n, Owner: "", HeartbeatUnixMs: 0}, which the staleness
//     predicate treats as stale (owner unknown) - the safe default, so an
//     upgrade reading an old marker CAS-claims the position rather than
//     deferring to a ghost.
//
// The two are sniffed by their leading byte: JSON payloads begin with '{'; a
// legacy payload is all-decimal. An empty / blank payload is an error (an
// absent object is reported as not-present by the read layer, never as empty).
func ParseServingMarker(payload []byte) (ServingMarker, error) {
	trimmed := strings.TrimSpace(string(payload))
	if trimmed == "" {
		return ServingMarker{}, errEmptyMarker
	}
	if !looksLikeLegacyEpoch(payload) {
		var m ServingMarker
		if err := json.Unmarshal([]byte(trimmed), &m); err != nil {
			return ServingMarker{}, fmt.Errorf("storageunit: parse serving marker (%q): %w", trimmed, err)
		}
		return m, nil
	}
	n, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return ServingMarker{}, fmt.Errorf("storageunit: parse legacy serving marker (%q): %w", trimmed, err)
	}
	return ServingMarker{Epoch: Epoch(n)}, nil
}

// looksLikeLegacyEpoch reports whether payload is the LEGACY bare-decimal-epoch
// form (all-digit after trimming) as opposed to the new JSON object form. The
// JSON form always starts with '{', so any all-decimal payload is legacy. An
// empty / blank payload is NOT legacy (it is an error case handled upstream).
func looksLikeLegacyEpoch(payload []byte) bool {
	trimmed := strings.TrimSpace(string(payload))
	if trimmed == "" {
		return false
	}
	for _, r := range trimmed {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// IsStale reports whether a PRESENT marker should be treated as a ghost: NOT
// deferred to at boot and ELIGIBLE to be CAS-stolen at acquire. It is the pure
// staleness predicate from the Phase 2g spec, and the SINGLE source of truth for
// owner/heartbeat staleness (the cluster acquire path layers ONLY its convergence
// gate on top of this; it does not re-implement the comparison). A present marker
// is stale when ANY of:
//
//   - Owner is "" (owner unknown - a legacy bare-epoch marker, or a corrupt
//     write); OR
//   - Owner is NOT in the live gossip member set liveMembers (the recorded
//     owner is not a currently-alive peer); OR
//   - HeartbeatAgedOut(now, leaseWindowMs) - the heartbeat is older than the
//     lease window, so even a still-listed owner is not trusted as live.
//
// A nil / empty liveMembers means no owner is live, so every present marker is
// stale - the all-pods-at-once restart case, where the booting node has not yet
// learned any peer, must NOT defer to anyone.
//
// now and leaseWindowMs are caller-supplied (no clock here) so the predicate is
// a pure function of its inputs.
func (m ServingMarker) IsStale(now int64, leaseWindowMs int64, liveMembers map[string]bool) bool {
	if m.Owner == "" {
		return true
	}
	if !liveMembers[m.Owner] {
		return true
	}
	return m.HeartbeatAgedOut(now, leaseWindowMs)
}

// HeartbeatAgedOut reports whether the lease heartbeat is older than the lease
// window: now - HeartbeatUnixMs > leaseWindowMs. The comparison is STRICT (`>`):
// a heartbeat exactly at the window edge is still fresh. A non-positive
// leaseWindowMs (<= 0) DISABLES the age check (returns false) - the caller has
// opted out of the heartbeat backstop and relies on owner-liveness alone.
//
// It is split out from IsStale so the convergence-gated acquire path can tell the
// two staleness reasons apart: a heartbeat that has aged out is a DEAD owner,
// reclaimable regardless of ring convergence (the backstop), whereas an
// owner-not-in-the-live-set marker whose heartbeat is still fresh might just be a
// live peer whose gossip has not arrived yet, so it is only reclaimable once the
// ring has settled. Keeping both behind this one predicate keeps the comparison
// in a single place.
func (m ServingMarker) HeartbeatAgedOut(now int64, leaseWindowMs int64) bool {
	if leaseWindowMs <= 0 {
		return false
	}
	return now-m.HeartbeatUnixMs > leaseWindowMs
}

// NewerEpochThan reports whether m's epoch is STRICTLY greater than other's. It
// is the release-gate comparison (a draining owner releases only when it reads
// a marker at an epoch strictly above its own open epoch) lifted onto the value
// type so the strict-`>` semantics live in one place.
func (m ServingMarker) NewerEpochThan(other ServingMarker) bool {
	return m.Epoch > other.Epoch
}
