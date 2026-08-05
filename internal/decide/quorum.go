package decide

// CurrentSetChoice names which candidate placement is a unit's CURRENT replica
// set during a join transition.
type CurrentSetChoice int

const (
	// CurrentJoinerExcluded selects the placement computed AS IF the joining
	// members were not members: the still-mounted displaced owners, which is
	// what makes a join write-transparent.
	CurrentJoinerExcluded CurrentSetChoice = iota
	// CurrentFullRing selects the full-ring placement, joining members
	// included: THE QUORUM FLOOR.
	CurrentFullRing
)

func (c CurrentSetChoice) String() string {
	if c == CurrentFullRing {
		return "full-ring (quorum floor)"
	}
	return "joiner-excluded"
}

// CurrentReplicaSet decides which placement is CURRENT for one unit. Only the
// CARDINALITIES decide it: which nodes are in each set never enters.
//
// The joiner-excluded set is CURRENT only when excluding the joiners costs the
// unit no holder (>= as many as the full ring) AND leaves at least R of them.
// Otherwise the floor engages and CURRENT reverts to the full ring.
//
// THE FLOOR IS A DATA-SAFETY WALL, not an availability tweak. The write ack bar
// is requiredWriteAcks(consistency, len(current)), so a CURRENT shorter than R
// lowers the bar below the normal quorum and a write acks on fewer than R
// durable copies - a lost acked write. The mass-boot case makes that concrete:
// when 2+ of a unit's replicas are freshly booted at once, every holder carries
// the joining bit, the excluded set is EMPTY, and an unfloored bar of
// requiredWriteAcks(WriteAll, 0) = 0 acks a write with ZERO durable applies.
// Reverting to the full ring gives up transparency for that one unit and buys
// unavailable-but-SAFE: a routed op to a warming replica returns the
// mid-acquire transient, so the write wedges instead of lying.
//
// The leave direction never needs the floor - a draining node stays a stable
// mounted current owner - so this is asked only of the joining exclusion.
func CurrentReplicaSet(fullRing, joinerExcluded, r int) CurrentSetChoice {
	if joinerExcluded < r || joinerExcluded < fullRing {
		return CurrentFullRing
	}
	return CurrentJoinerExcluded
}
