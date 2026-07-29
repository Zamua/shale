// Join write-transparency (v0.8 Phase 2e, pending ranges, entry side). This is
// the quorum-floor-guarded MIRROR of the graceful-leave drain. A node that boots
// into a LIVE cluster with DEFERRED positions (owned-but-not-yet-mounted, because
// a peer's serving marker was present so the node did not fence the live peer) is
// WARMING: it advertises a gossiped Joining bit, so every node's per-op split
// EXCLUDES it from the CURRENT set (subject to the quorum floor in
// currentUnitReplicas) while INCLUDING it in the PENDING set. The still-mounted
// DISPLACED owner therefore stays a stable current owner (it keeps serving,
// dual-written via the union) and this node acquires its positions via the
// background pending-owner path (acquireReplicaUnitOverlap), serving-marking each
// on mount-complete - which gates the displaced owner's drain. A moving shard
// stays AVAILABLE throughout the slow mount, exactly as it does through a leave.
//
// The Joining bit is SET at boot (mountReplicaUnits, when the boot-defer count is
// nonzero) and CLEARED here on the reconcile cadence once this node has fully
// warmed: every position in its PENDING (real-ownership) set is mounted. Once
// cleared the node is an ordinary current owner and routing collapses to the
// steady state.
//
// See docs/SPEC.md "v0.8 Phase 2e" (the CURRENT/PENDING unified split, the
// quorum floor, and the Joining-bit lifecycle).

package cluster

// setSelfJoining flips this node's local Joining flag AND republishes the role
// set through the coordinator, but only on a real change (the atomic Swap
// dedups, so a repeated set/clear costs nothing). The local atomic is the
// source of truth the reconcile's clear-decision reads (avoids racing a
// self-view read); the coordinator carries the authoritative role peers
// observe. Publishing goes through publishRoles because the port is
// declarative - it replaces the whole role set, so joining and draining must
// travel together.
func (c *Cluster) setSelfJoining(v bool) {
	if c.selfJoining.Swap(v) == v {
		return // unchanged
	}
	c.publishRoles()
}

// maintainJoiningState clears this node's Joining bit once it has fully warmed:
// every position in its PENDING (real-ownership) set is mounted. It is a no-op
// unless this node is currently advertising Joining (the common case). Called on
// the reconcile cadence after the overlap reconcile + drain checks, so a node
// that just finished mounting its last deferred position stops excluding itself
// from CURRENT on the next tick and routing collapses to steady state.
//
// The clear is one-directional: this function never SETS Joining (that happens
// only at boot in mountReplicaUnits, on a genuine boot-defer). So a node that has
// warmed and cleared never re-enters Joining just because a later topology change
// moves a new position onto it - that is a fresh transition owned by whichever
// node is joining then, not this one.
func (c *Cluster) maintainJoiningState() {
	if !c.selfJoining.Load() {
		return
	}
	if c.allPendingPositionsMounted() {
		c.setSelfJoining(false)
	}
}

// allPendingPositionsMounted reports whether every position this node owns under
// the PENDING view (its real-ownership set, the ring excluding draining members)
// is mounted. It is the JOIN completion gate - the entry-side mirror of
// allOwnedPositionsHandedOff. True once the node has finished warming, so the
// Joining bit can clear. A node with no pending positions (or no replica factory)
// is trivially warmed. The pending set is derived from the ring/membership
// first, then tested against ONE coherent view of the mount table.
func (c *Cluster) allPendingPositionsMounted() bool {
	if !c.replicaLayout() {
		return true
	}
	pending := c.desiredPendingReplicaUnits(c.drainingIDs())
	if len(pending) == 0 {
		return true
	}
	return c.mounts.allMounted(pending)
}
