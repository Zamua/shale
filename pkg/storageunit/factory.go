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
	// OpenUnit opens (mounts) the backend for unit u at the given epoch and
	// returns it ready to serve. Opening at an epoch higher than the unit's
	// current writer epoch FENCES that prior writer: this is how a new owner
	// acquires the lease during handoff. Opening a unit this factory already
	// has open at the same or a lower epoch is an error (a unit has at most
	// one live writer); callers detect a double-open / stale-epoch acquire
	// that way.
	//
	// The returned Backend is owned by the caller until it passes it to
	// CloseUnit (or the backend is otherwise closed). On error the Backend
	// is nil and nothing was mounted.
	OpenUnit(u UnitID, epoch Epoch) (backend.Backend, error)

	// CloseUnit releases (unmounts) unit u: flushes anything durable, stops
	// writing, and frees the unit's resources WITHOUT affecting any other
	// unit this factory has open. This is the old owner's release half of a
	// lease handoff. It is idempotent: closing a unit that is not open is a
	// no-op and returns nil. After CloseUnit, the unit may be re-opened
	// (here or on another node) at a higher epoch.
	CloseUnit(u UnitID) error

	// CurrentEpoch reports the epoch at which this factory currently holds
	// unit u open, and ok=false if the factory does not have u open. A new
	// owner uses this against the prior owner's reported epoch to choose the
	// fencing epoch (prior + 1). Pure query: no mount/unmount side effect.
	CurrentEpoch(u UnitID) (e Epoch, ok bool)
}
