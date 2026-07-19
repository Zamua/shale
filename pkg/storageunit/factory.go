package storageunit

import "github.com/Zamua/shale/pkg/backend"

// Epoch is a monotonic lease/fencing token for one storage mount. slatedb is
// single-writer per database via writer-epoch fencing: opening a mount at a
// higher epoch fences any prior writer that still holds it at a lower epoch.
// That fencing IS the lease primitive, so the unit model threads an Epoch
// through every open.
//
// Lease handoff of mount M from node A to node B:
//
//  1. A flushes M, stops writing, releases (CloseUnit).
//  2. B opens M at epoch+1 (OpenUnit at the next Epoch), which fences any
//     stale A writer, and begins serving M.
//
// Epoch is a value object here (no I/O): where epochs are minted, persisted,
// and compared is an infrastructure concern. The domain only needs the type so
// the factory contract can name "open at the next epoch."
type Epoch uint64

// BackendFactory is the SEAM between the pure unit model and the storage
// infrastructure, and it is the ONLY storage port shale has. It opens, closes,
// fences and marks the per-mount backends a node holds.
//
// # One port, keyed by MountRef
//
// Every method is keyed by MountRef: the generation-qualified unit, the replica
// position, and the layout selector (see MountRef). R=1 is replica 0. There is
// no second interface, no capability probe, and no type assertion anywhere in
// shale that asks an adapter which subset of this contract it supports. An
// adapter either satisfies the contract below or it does not, and that is a
// property of the adapter, not a branch in the coordination layer.
//
// That is a deliberate design ruling. shale is a coordination layer: it must
// not carry one implementation for adapters with some capability and a second
// for the rest, selected by what it was handed. The requirement stated here is
// honest and uniform, and an adapter that cannot meet it simply cannot host a
// distributed shale.
//
// # The requirement
//
// The load-bearing part is FENCING: OpenUnit must be able to take a mount away
// from a prior writer, durably, so that at most one writer can ever be live for
// one mount. Everything else in the contract exists to make a lease handoff
// observable across nodes.
//
// Implementations MUST be safe to call from the node's mount/unmount
// goroutines; concurrency of the returned backend.Backend is governed by the
// backend.Backend contract, not this factory.
//
// # Layout
//
// An implementation that derives a storage location from the mount identity
// MUST derive it from the WHOLE MountRef, including MountRef.Replicated. A sole
// mount and a replica-0 mount of the same unit are DIFFERENT mounts holding
// DIFFERENT bytes; resolving one through the other's derivation silently
// orphans a deployed cluster's data. See MountRef.
type BackendFactory interface {
	// OpenUnit opens (mounts) the backend for m at the given epoch and returns
	// it ready to serve. Opening at an epoch higher than the mount's current
	// durable writer epoch FENCES that prior writer: this is how a new owner
	// acquires the lease during handoff. Opening a mount this factory already
	// has open is an error at ANY epoch (a mount has at most one live writer
	// per factory); the caller must CloseUnit first, and callers detect a
	// double-open / stale-epoch acquire that way.
	//
	// The intended epoch is a best-effort FLOOR, not an instruction. The
	// implementation is AUTHORITATIVE: it reads the mount's durable writer
	// epoch and opens strictly above it, at max(intended, durable+1). A stale
	// intended epoch therefore cannot under-fence, and an open MUST NOT be
	// rejected for naming too low a floor (a new owner must always be able to
	// take the lease).
	//
	// It RETURNS the EXACT epoch it opened at. The caller MUST use that
	// returned value as "the epoch this node holds the mount at" (for the
	// drain-release gate and the serving marker) rather than re-reading
	// DurableEpoch, which is a SHARED monotone counter that any node's later
	// open bumps: a re-read is a moving target. The returned epoch is fixed for
	// the life of this mount.
	//
	// Mounts are INDEPENDENT of one another in every coordinate. Two MountRefs
	// differing in Generation are independent databases, which is what the
	// online bisect relies on (the old unit keeps serving while the new ones
	// fill). Two differing in replica position are independent databases, which
	// is what makes the R copies survive each other. Two differing only in the
	// layout selector are ALSO independent, which is what keeps an R=1
	// deployment's bytes distinct from a replica-0 mount.
	//
	// The returned Backend is owned by the caller until it passes it to
	// CloseUnit (or the backend is otherwise closed). On error the Backend is
	// nil, the epoch is 0, and nothing was mounted.
	OpenUnit(m MountRef, epoch Epoch) (backend.Backend, Epoch, error)

	// CloseUnit releases (unmounts) m: flushes anything durable, stops writing,
	// and frees the mount's resources WITHOUT affecting any other mount this
	// factory holds. This is the old owner's release half of a lease handoff,
	// and also how the resharder RETIRES an old generation's unit after its
	// key-space has cut over to the next generation's children.
	//
	// Flush-before-release is load-bearing: every acked write must be durable
	// before the lease can move, so the next owner sees it. It does NOT delete
	// the mount's bytes; the data stays for the next owner.
	//
	// Idempotent: closing a mount that is not open is a no-op returning nil.
	// After CloseUnit the mount may be re-opened (here or on another node) at a
	// higher epoch.
	CloseUnit(m MountRef) error

	// CurrentEpoch reports the epoch at which this factory currently holds m
	// open, and ok=false if this factory does not have m open. This is the
	// LOCAL in-process view only; it is NOT the cross-node source of truth.
	// The authoritative writer epoch lives in the mount's durable lease state,
	// which DurableEpoch reads and OpenUnit fences against. A new owner
	// acquiring a mount it has never held learns nothing from here
	// (CurrentEpoch returns ok=false for an unmounted mount). Pure query: no
	// mount/unmount side effect.
	CurrentEpoch(m MountRef) (Epoch, bool)

	// OpenUnits returns the set of mounts this factory currently holds, sorted
	// by CompareMountRefs. The anti-entropy reconcile diffs this against the
	// desired set: mounts owned-but-not-held must be opened (acquire the
	// lease), mounts held-but-not-owned must be closed (release it). Pure
	// query: no side effect; the returned slice is a copy the caller may
	// retain.
	OpenUnits() []MountRef

	// DurableEpoch reports m's DURABLE writer epoch, read WITHOUT opening
	// (mounting) it. It is the cross-node source of truth: the value OpenUnit
	// fences above, read from the mount's own durable state in shared storage.
	//
	// It is the CROSS-NODE LIVENESS HINT the overlap handoff's old owner uses
	// to detect that a new owner has fenced the mount: seeing the durable epoch
	// advance past the old owner's own open epoch proves SOMEONE opened at a
	// higher epoch. It is a HINT, not a release trigger: a bare epoch advance
	// proves a fence happened, NOT that a live owner is serving (a new owner
	// can fence then crash mid-mount). The old owner releases only on the
	// successor's durable SERVING MARKER at an epoch strictly above its own
	// open epoch; it NEVER releases on this bare epoch alone. See Releasable in
	// handoff.go and ReadServingMarker below.
	//
	// It reads durable state without side effect (no mount/unmount). A mount
	// never opened has durable epoch 0. err is non-nil only on an I/O failure
	// reading the durable state (an unopened mount is NOT an error: it is
	// epoch 0).
	DurableEpoch(m MountRef) (Epoch, error)

	// WriteServingMarker writes m's durable, poll-observable SERVING MARKER
	// carrying the new owner's open epoch. The new (gaining) owner calls it
	// EXACTLY ONCE, at the Acquiring -> Ready transition, immediately AFTER its
	// mount flip (after OpenUnit returned, after the mount was inserted, after
	// it started serving locally). The marker means "a live owner is actually
	// SERVING this mount at epoch >= the carried epoch," which is STRICTLY
	// STRONGER than the durable FENCE epoch: the fence bumps at open-START,
	// before the mount completes, so a new owner that fences then crashes
	// mid-mount advances the fence WITHOUT ever serving and WITHOUT ever
	// writing a marker. It is the POLL-ONLY release signal of the overlap
	// handoff: there is NO push RPC. The old (draining) owner observes it via
	// ReadServingMarker on its periodic drain-check cadence.
	//
	// It is keyed by the MOUNT (node-independent, at the same durable location
	// the mount's bytes live), so whichever node currently holds the mount
	// writes the marker the predecessor polls. It is a small durable record,
	// NOT a lease/latch. A re-write at the same-or-higher epoch is idempotent
	// (the marker monotonically reflects the latest live owner); a write must
	// never LOWER the recorded epoch (a stale write from a fenced prior owner
	// must not roll the marker back). err is non-nil only on an I/O failure
	// writing the durable record.
	WriteServingMarker(m MountRef, epoch Epoch) error

	// ReadServingMarker reads m's durable serving marker WITHOUT opening
	// (mounting) it. It is the CROSS-NODE release signal the overlap handoff's
	// old (draining) owner polls: it releases ONLY when it observes ok == true
	// AND epoch >= its own open epoch (a positive confirmation that a live
	// owner is actually serving, which the bare durable fence epoch cannot
	// give). ok == false means no marker has been written yet (no live owner
	// has reached Ready for this mount), in which case the old owner stays
	// Draining and KEEPS SERVING.
	//
	// The read is a POINT-IN-TIME liveness OBSERVATION, not a lease
	// acquisition: it carries no expiry and grants no exclusivity. A NEW owner
	// that crashes in the gap between the old owner reading the marker and
	// completing its release leaves the mount unserved until the next
	// reconcile, with NO acked-write loss (durable-before-ack); the old owner
	// does not re-read inside the release lock (I/O under the lock is
	// forbidden). err is non-nil only on an I/O failure reading the durable
	// record; a never-written marker is NOT an error (it is ok == false,
	// epoch 0).
	ReadServingMarker(m MountRef) (epoch Epoch, ok bool, err error)
}
