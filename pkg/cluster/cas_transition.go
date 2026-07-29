// CAS owner designation through a membership transition (workstream B of the
// join write-transparency work; see docs/SPEC.md "CAS during a transition: ONE
// designated owner + the union write-set fan-out").
//
// The CAS validate-and-apply is owner-local and single-writer by design: it
// cannot ride the union the way a plain put does (two nodes validating the same
// pin unit concurrently would be a lost-update split brain). Instead exactly ONE
// node is DESIGNATED per pin unit: the full-ring head, UNLESS that head is
// Joining (a warming newcomer that has not mounted the unit yet), in which case
// the head of the CURRENT set - the displaced, still-mounted owner. The
// designation is a deterministic pure function of (ring members, joining set);
// the client dispatch (commitCAS) and the server-side ownership gate (OwnsCASPin
// behind the CommitCAS FailedPrecondition re-pin refusal) evaluate the same
// function, so per converged view at most one node accepts commits.
//
// SINGLE-OWNER SAFETY ACROSS VIEW SKEW (peers transiently disagreeing about the
// Joining bit around its CLEAR): the designated owner always serves the pin unit
// at the POSITION-0 durable database. The current head serves its current-index-0
// mount; the newcomer - which must be the full-ring head for the designation to
// change at all - acquires pending-index 0, the SAME durable database. One
// database has one fencing chain, so the newcomer's open fences the displaced
// head before the newcomer can serve: two designated owners can never both
// commit un-fenced.

package cluster

import "github.com/Zamua/shale/pkg/coord"

// casDesignatedOwner resolves the ONE node that serves the owner-side CAS
// validate-and-apply for pinKey. Outside a join transition (and on every
// non-multi-replicated path) it is exactly ownerOf - the full-ring head - so
// steady state, R=1, and legacy behavior are unchanged. During a join transition
// whose full-ring head for the pin unit is a Joining (warming) member, the
// designation moves to the head of the CURRENT set: the displaced, still-mounted
// owner, which can actually serve the owner-local transaction while the newcomer
// mounts. Under the quorum floor (mass boot) the current set falls back to the
// full ring, so the designation falls back to the full-ring head - the safe
// wedge, unchanged from pre-fix behavior.
func (c *Cluster) casDesignatedOwner(pinKey []byte) (coord.Node, bool) {
	owner, isLocal := c.ownerOf(pinKey)
	if !c.multiReplicated() {
		return owner, isLocal
	}
	joining := c.joiningIDs()
	if len(joining) == 0 {
		return owner, isLocal
	}
	if _, ownerJoining := joining[owner.ID]; !ownerJoining {
		return owner, isLocal
	}
	gu := c.genUnitForKey(pinKey)
	current := c.currentUnitReplicas(gu, joining)
	if len(current) == 0 {
		// Defensive: a live ring always yields a non-empty (floored) current set.
		return owner, isLocal
	}
	head := current[0]
	return head, string(head.ID) == c.cfg.NodeID
}

// OwnsCASPin reports whether THIS node is the designated CAS owner for pinKey
// (see casDesignatedOwner). It is the server-side gate the CommitCAS RPC handler
// consults before running the owner-local validate-and-apply; a node that is not
// the designated owner refuses with the FailedPrecondition re-pin so the client
// re-resolves against the live view. Exported for the rpc layer only; it is NOT
// a general ownership predicate (OwnsKey / OwnsReplica serve the plain-op
// guards).
func (c *Cluster) OwnsCASPin(pinKey []byte) bool {
	_, isLocal := c.casDesignatedOwner(pinKey)
	return isLocal
}
