package cluster

import "math"

// MountReadiness is a point-in-time summary of this node's mount state,
// counted over the DESIRED set (the replica positions this node currently
// owns under the full ring - the same set the /debug/shale/state wedge flag
// keys on). It is the programmatic sibling of DebugState: the same facts, as
// cheap struct fields instead of a formatted dump, so an embedding
// application can gate its readiness probe on actual mount state. See
// docs/SPEC.md "Mount readiness".
//
// Invariant: MountedUnits + PendingUnits == DesiredUnits.
type MountReadiness struct {
	// DesiredUnits is the number of replica positions this node owns.
	DesiredUnits int
	// MountedUnits is the number of desired positions currently mounted
	// (serving locally). Mounted-but-no-longer-desired positions (a loser
	// mid-drain) count nowhere: readiness is about what the node owes.
	MountedUnits int
	// PendingUnits is the number of desired positions not yet mounted,
	// whether the acquire is quietly in flight, boot-deferred, or failing.
	PendingUnits int
	// FailedOpenUnits is the subset of pending positions whose most recent
	// acquire attempt recorded an error in the mount table: an open that
	// returned an error, or a boot-deferral that declined to open because a
	// peer holds the serving marker. A successful mount deletes the record,
	// so the count never carries a stale error for a mounted position.
	FailedOpenUnits int
	// LastAcquireError is the recorded error of the first failed position in
	// position order (deterministic across calls at unchanged state); ""
	// when FailedOpenUnits == 0. One representative message; the full
	// per-position detail stays on /debug/shale/state.
	LastAcquireError string
}

// MountReadiness returns this node's current mount-state counts. It reads the
// SAME internal state DebugState reads (the desired set and the mount table's
// mounts + acquire records) but returns plain counts: O(desired positions),
// no formatting, cheap enough to sit behind a probe polled every few seconds.
// In legacy single-backend mode (no per-unit mounts) every count is zero and
// Ready is vacuously true: the one backend either opened at Open or Open
// failed, so there is no deferred mount state to report.
func (c *Cluster) MountReadiness() MountReadiness {
	if !c.multi {
		return MountReadiness{}
	}
	// The desired set is computed OUTSIDE the mount table's lock (it takes the
	// gen + ring locks internally), mirroring DebugState's lock ordering; the
	// tally then runs against ONE coherent view of the table.
	desired := c.desiredReplicaUnits()
	counts := c.mounts.readiness(desired)
	return MountReadiness{
		DesiredUnits:     len(desired),
		MountedUnits:     counts.Mounted,
		PendingUnits:     counts.Pending,
		FailedOpenUnits:  counts.Failed,
		LastAcquireError: counts.FirstError,
	}
}

// Ready reports whether this node has mounted at least
// ceil(minMountedFraction * desired) of its desired positions. See
// MountReadiness.Ready for the edge-case contract.
func (c *Cluster) Ready(minMountedFraction float64) bool {
	return c.MountReadiness().Ready(minMountedFraction)
}

// Ready reports whether the summarized node meets the mount floor:
// MountedUnits >= ceil(f * DesiredUnits) with f = minMountedFraction clamped
// to [0, 1]. Edges, pinned deliberately (docs/SPEC.md "Mount readiness"):
//
//   - DesiredUnits == 0: ready at every fraction. A node with no assigned
//     positions (mid-join, a non-storage role, legacy mode) has nothing to
//     mount; vacuously ready is the answer that does not wedge its rollout.
//   - f <= 0 clamps to 0 (no floor requested: always ready); f >= 1 clamps
//     to 1 (every desired position must be mounted); NaN clamps to 1 (the
//     conservative end - garbage input must not disable the gate).
//   - A node inside its initial reconcile window is NOT special-cased: the
//     predicate reports mount state as it is. Boot grace belongs to the
//     consumer's probe (initial delay / failure threshold), not here.
func (r MountReadiness) Ready(minMountedFraction float64) bool {
	if r.DesiredUnits == 0 {
		return true
	}
	f := minMountedFraction
	switch {
	case math.IsNaN(f), f > 1:
		f = 1
	case f < 0:
		f = 0
	}
	required := int(math.Ceil(f * float64(r.DesiredUnits)))
	if required > r.DesiredUnits {
		required = r.DesiredUnits
	}
	return r.MountedUnits >= required
}
