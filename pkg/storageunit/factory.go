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
	OpenReplicaUnit(ru ReplicaUnit, epoch Epoch) (backend.Backend, error)

	// CloseReplicaUnit releases the durable database for replica position
	// ru.Replica of unit ru.Unit, WITHOUT affecting any other replica or unit.
	// Idempotent: closing a replica not open is a no-op returning nil.
	CloseReplicaUnit(ru ReplicaUnit) error
}
