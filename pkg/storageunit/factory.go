package storageunit

import "github.com/Zamua/shale/pkg/backend"

// Epoch is a monotonic lease/fencing token for one storage unit. slatedb is
// single-writer per database via writer-epoch fencing: opening a unit at a
// higher epoch fences any prior writer that still holds it at a lower epoch.
// That fencing IS the lease primitive, so the unit model threads an Epoch
// through every open.
//
// Lease handoff of unit U from node A to node B:
//
//  1. A flushes U, stops writing, releases (CloseUnit).
//  2. B opens U at epoch+1 (OpenUnit at the next Epoch), which fences any
//     stale A writer, and begins serving U.
//
// Epoch is a value object here (no I/O): where epochs are minted, persisted,
// and compared is an infrastructure concern for a later phase. The domain
// only needs the type so the factory contract can name "open at the next
// epoch."
type Epoch uint64

// BackendFactory is the SEAM between the pure unit model and the storage
// infrastructure. It opens and closes the per-unit backends a node mounts.
// Implementations (a slatedb factory, an in-memory factory for tests) live
// in later phases / other packages; this interface is the only thing the
// domain and the application service depend on.
//
// The unit it operates on is a GenUnit - the generation-qualified storage
// identity (Generation, UnitID), NOT a bare UnitID. This is the v0.8 Phase 4
// change: during a doubling reshard the old gen-g unit K and the new
// gen-(g+1) unit K are DISTINCT databases that coexist (the bisect copies
// K's bytes into its children while K keeps serving), so the factory must
// open them as separate identities. In steady state (no reshard in flight)
// every GenUnit shares the cluster's current generation, so the qualifier is
// a constant prefix and nothing changes for Phase 2 / Phase 3 behavior.
//
// The contract is shaped for lease handoff, not just open/close:
//
//   - OpenUnit takes an Epoch so the new owner can open at epoch+1 and fence
//     the prior owner (the single-writer guarantee).
//   - CloseUnit lets the old owner release a unit it no longer owns without
//     tearing down the whole node, so a handoff moves ONE unit's lease while
//     the node keeps serving its other units.
//
// Implementations MUST be safe to call from the node's mount/unmount
// goroutine; concurrency of the returned backend.Backend is governed by the
// backend.Backend contract, not this factory.
type BackendFactory interface {
	// OpenUnit opens (mounts) the backend for the generation-qualified unit
	// gu at the given epoch and returns it ready to serve. Opening at an
	// epoch higher than the unit's current writer epoch FENCES that prior
	// writer: this is how a new owner acquires the lease during handoff.
	// Opening a GenUnit this factory already has open at the same or a lower
	// epoch is an error (a unit has at most one live writer); callers detect
	// a double-open / stale-epoch acquire that way.
	//
	// Two GenUnits sharing a UnitID but differing in Generation are
	// INDEPENDENT databases: opening gen-(g+1) unit K does NOT fence or touch
	// gen-g unit K, which is exactly what the online bisect relies on (the
	// old unit keeps serving while the new ones fill).
	//
	// The returned Backend is owned by the caller until it passes it to
	// CloseUnit (or the backend is otherwise closed). On error the Backend
	// is nil and nothing was mounted.
	OpenUnit(gu GenUnit, epoch Epoch) (backend.Backend, error)

	// CloseUnit releases (unmounts) the generation-qualified unit gu: flushes
	// anything durable, stops writing, and frees the unit's resources WITHOUT
	// affecting any other unit this factory has open. This is the old owner's
	// release half of a lease handoff, and also how the resharder RETIRES an
	// old gen-g unit after its key-space has cut over to the gen-(g+1)
	// children. It is idempotent: closing a unit that is not open is a no-op
	// and returns nil. After CloseUnit, the unit may be re-opened (here or on
	// another node) at a higher epoch.
	CloseUnit(gu GenUnit) error

	// CurrentEpoch reports the epoch at which this factory currently holds
	// gu open, and ok=false if the factory does not have gu open. This is
	// the LOCAL in-process view only; it is NOT the cross-node source of
	// truth. The authoritative writer epoch for a unit lives in that unit's
	// durable lease state (the slatedb manifest writer-epoch in object
	// storage). A new owner acquiring a unit it has never held does not learn
	// the prior owner's epoch from here (CurrentEpoch returns ok=false for an
	// unmounted unit); instead OpenUnit reads the durable manifest epoch and
	// fences above it. Pure query: no mount/unmount side effect.
	CurrentEpoch(gu GenUnit) (e Epoch, ok bool)

	// OpenUnits returns the set of GenUnits this factory currently has
	// mounted, in ascending (Generation, UnitID) order. The anti-entropy
	// reconcile diffs this against the desired set: units owned-but-not-
	// mounted must be opened (acquire the lease), units mounted-but-not-owned
	// must be closed (release it). Without this enumerator a node would have
	// to track its own mounted set outside the factory. Pure query: no side
	// effect; the returned slice is a copy the caller may retain.
	OpenUnits() []GenUnit
}

// ReplicaBackendFactory is the R>1 (replicated) extension of BackendFactory:
// it opens/closes a unit AT A REPLICA POSITION, so the R replicas of one unit
// are INDEPENDENT durable databases (keyed by ReplicaUnit, not bare GenUnit).
// A factory advertises R>1 support by implementing this in ADDITION to
// BackendFactory; the cluster's R=1 multi-backend paths use only the base
// interface (unchanged), and the R>1 paths type-assert for this capability.
//
// Why a capability interface (not a widened BackendFactory): the R=1 factory
// contract (one durable store per GenUnit, copy-free handoff) is unchanged,
// and the deployable single-replica factories must not be forced to grow a
// replica argument they do not use. A factory that does not implement this is
// simply not usable at R>1, which the cluster validates at Open.
type ReplicaBackendFactory interface {
	BackendFactory

	// OpenReplicaUnit opens (mounts) the durable database for replica position
	// ru.Replica of unit ru.Unit, at the given epoch, returning it ready to
	// serve. Distinct replica positions of one unit are INDEPENDENT databases:
	// opening replica 1 does NOT touch replica 0's bytes, so the loss of the
	// node holding one position leaves the other a complete copy. Epoch
	// semantics match OpenUnit (open higher to fence a prior writer of the SAME
	// replica position).
	//
	// It RETURNS the EXACT epoch it opened at (the fence epoch
	// max(intended, durableEpochReplica+1)). The caller MUST use this returned
	// value as "the epoch this node opened the position at" - for the
	// drain-release gate and the serving marker - rather than re-reading
	// DurableEpochReplica, which is a SHARED monotone counter that any node's
	// later open bumps (a re-read is a moving target). The returned epoch is
	// fixed for the life of this mount.
	OpenReplicaUnit(ru ReplicaUnit, epoch Epoch) (backend.Backend, Epoch, error)

	// CloseReplicaUnit releases the durable database for replica position
	// ru.Replica of unit ru.Unit, WITHOUT affecting any other replica or unit.
	// Idempotent: closing a replica not open is a no-op returning nil.
	CloseReplicaUnit(ru ReplicaUnit) error

	// DurableEpochReplica reports the DURABLE writer-epoch of replica position
	// ru.Replica of unit ru.Unit, read WITHOUT opening (mounting) it. It is the
	// cross-node source of truth a real factory reads from the per-replica
	// slatedb manifest in object storage (the slate Backing already exposes
	// this capability; v0.8 Phase 2e lifts it onto this interface).
	//
	// It is the CROSS-NODE LIVENESS HINT the overlap handoff's old owner uses
	// to detect that a new owner has fenced the position: seeing the durable
	// epoch advance past the old owner's own open epoch proves SOMEONE opened
	// at a higher epoch. It is a HINT, not a release trigger: a bare epoch
	// advance proves a fence happened, NOT that a live owner is serving (a new
	// owner can fence then crash mid-mount). The old owner re-verifies a live
	// owner is serving (a readiness probe) before releasing; it NEVER releases
	// on this bare epoch alone. See CanRelease / Releasable in handoff.go and
	// docs/SPEC.md "v0.8 Phase 2e".
	//
	// It reads durable state without side effect (no mount/unmount). A replica
	// never opened has durable epoch 0. err is non-nil only on an I/O failure
	// reading the durable manifest (an unopened replica is NOT an error: it is
	// epoch 0).
	DurableEpochReplica(ru ReplicaUnit) (Epoch, error)

	// WriteServingMarker writes the durable, poll-observable SERVING MARKER for
	// replica position ru carrying the new owner's open epoch (v0.8 Phase 2e,
	// Option B overlap handoff). The new (gaining) owner calls it EXACTLY ONCE,
	// at the Acquiring -> Ready transition, immediately AFTER its mount flip
	// (after OpenReplicaUnit returned, after mountMap[ru] was inserted, after it
	// started serving locally). The marker means "a live owner is actually
	// SERVING this position at epoch >= the carried epoch," which is STRICTLY
	// STRONGER than the durable manifest FENCE epoch (the fence bumps at
	// open-START, before the mount completes, so a new owner that fences then
	// crashes mid-mount advances the fence WITHOUT ever serving and WITHOUT ever
	// writing a marker). It is the POLL-ONLY release signal of the overlap
	// handoff: there is NO push RPC. The old (draining) owner observes it via
	// ReadServingMarker on its periodic drainCheck cadence.
	//
	// It is keyed by the ReplicaUnit (node-independent, the same durable prefix
	// dbNameReplica(ru) the position lives at), so whichever node currently
	// holds the position writes the marker the predecessor polls. It is a small
	// durable record, NOT a lease/latch. A re-write at the same-or-higher epoch
	// is idempotent (the marker monotonically reflects the latest live owner); a
	// write must never LOWER the recorded epoch (a stale write from a fenced
	// prior owner must not roll the marker back). err is non-nil only on an I/O
	// failure writing the durable record.
	WriteServingMarker(ru ReplicaUnit, epoch Epoch) error

	// ReadServingMarker reads the durable serving marker for replica position
	// ru WITHOUT opening (mounting) it (v0.8 Phase 2e). It is the CROSS-NODE
	// release signal the overlap handoff's old (draining) owner polls on its
	// drainCheck cadence: it releases ONLY when it observes ok == true AND
	// epoch >= its own open epoch (a positive confirmation that a live owner is
	// actually serving the position, which the bare durable fence epoch cannot
	// give). ok == false means no marker has been written yet (no live owner has
	// reached Ready for this position), in which case the old owner stays
	// Draining and KEEPS SERVING.
	//
	// The read is a POINT-IN-TIME liveness OBSERVATION, not a lease acquisition:
	// it carries no expiry and grants no exclusivity. A NEW owner that crashes
	// in the gap between the old owner reading the marker and completing its
	// release leaves the position unserved until the next reconcile, with NO
	// acked-write loss (durable-before-ack); the old owner does not re-read
	// inside the release lock (I/O under the lock is forbidden). err is non-nil
	// only on an I/O failure reading the durable record; a never-written marker
	// is NOT an error (it is ok == false, epoch 0).
	ReadServingMarker(ru ReplicaUnit) (epoch Epoch, ok bool, err error)
}

// AuthoredMarkerFactory is the OPTIONAL author-attributed extension of the
// serving marker (v0.11.2). A ReplicaBackendFactory advertises it by
// implementing these two methods IN ADDITION to WriteServingMarker /
// ReadServingMarker; a factory that does not implement it keeps the exact
// epoch-only marker contract above, and every gate that consults the author
// degrades to the epoch-only rule.
//
// WHY THE AUTHOR IS PART OF THE RELEASE SIGNAL. The epoch-only marker is
// AUTHOR-ANONYMOUS: it says "SOMEBODY is serving this position at epoch E" and
// nothing about WHO. The draining owner's release gate (Releasable in
// handoff.go) therefore surrenders its copy to a successor it cannot name. That
// is safe only while every node agrees on who the successors are. It is NOT
// safe while membership views diverge: a node that has observed a JOINER's
// Joining bit but not yet a LEAVER's Draining bit computes a current/pending
// split whose routed union can EXCLUDE the true post-transition owners entirely
// (a FULL MOVE, where a unit's whole replica set turns over at once). Such a
// node arms a drain, sees the true successor's anonymous marker, releases its
// last local copy - and then routes reads for that unit only to nodes that
// never held it. Every routed leg answers transiently and the read fails
// client-visibly (Get times out; ScanPrefix surfaces the handing-off retryable)
// for as long as the view stays stale.
//
// Attributing the marker closes that hole WITHOUT changing what a marker means:
// the loser additionally requires the marker's AUTHOR to be a node it actually
// routes the position to, so it can never hand its last copy to a successor
// that is invisible to its own readers. See docs/design/overlap-handoff.md
// "Author-attributed serving markers".
//
// COMPATIBILITY IS BUILT IN, in both directions. The epoch record keeps its
// exact prior representation, so an OLD node reads a NEW node's marker
// unchanged. A NEW node reading an OLD (or absent) author record gets
// authorID == "", which every gate treats as UNKNOWN and falls back to the
// epoch-only rule. A mixed-version cluster therefore always makes progress.
type AuthoredMarkerFactory interface {
	// WriteServingMarkerFrom is WriteServingMarker plus the writing node's ID.
	// The gaining owner calls it exactly once at its Acquiring -> Ready mount
	// flip with its own NodeID, in place of WriteServingMarker. It carries the
	// same monotonicity + idempotence contract: it must never LOWER the recorded
	// epoch, and the recorded author must always correspond to the recorded
	// epoch (an implementation that cannot guarantee that pairing must report
	// the author as unknown rather than mismatched).
	WriteServingMarkerFrom(ru ReplicaUnit, epoch Epoch, authorID string) error

	// ReadServingMarkerFrom is ReadServingMarker plus the ID of the node that
	// wrote the marker. authorID is "" when the marker predates author
	// attribution, was written by a node that lacks it, or cannot be paired with
	// the returned epoch - all of which callers MUST treat as UNKNOWN (fall back
	// to the epoch-only rule), never as "authored by nobody". epoch/ok/err carry
	// exactly the ReadServingMarker semantics.
	ReadServingMarkerFrom(ru ReplicaUnit) (epoch Epoch, authorID string, ok bool, err error)
}
