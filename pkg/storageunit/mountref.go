package storageunit

// MountRef is the identity the BackendFactory port is keyed by: ONE storage
// mount an adapter can open, close, fence and mark. It carries THREE
// components, not two:
//
//	unit       the generation-qualified storage identity (Generation, UnitID)
//	replica    the position in the unit's ordered replica set (R=1 is 0)
//	replicated the LAYOUT SELECTOR (see below)
//
// # Why the third component exists
//
// shale used to state the storage requirement through TWO ports: BackendFactory
// (keyed by GenUnit) and ReplicaBackendFactory (keyed by ReplicaUnit), and it
// asked each adapter which of the two it implemented. That question was a leak:
// a coordination layer should not branch on what an adapter can do. Collapsing
// the two into one port keyed by (unit, replica) removes the question, and R=1
// becomes replica 0.
//
// The literal reading of that collapse is UNSAFE, and the third component is
// what makes it safe. An adapter that stores a unit's bytes under a derived
// prefix does NOT derive the same prefix for "unit U" and "unit U, replica 0":
// the replica position is a CHILD segment, so the two are different strings.
// The slate adapter's encodings are the worked example:
//
//	sole    -> "<keyPrefix>u/g<gen>/u<id>"
//	replica -> "<keyPrefix>u/g<gen>/u<id>/r<replica>"
//
// An existing R=1 deployment's bytes live at the FORMER. If an R=1 open were
// resolved through the replica-0 encoding it would mount an EMPTY database and
// report the unit fresh: silent total data loss presented as a healthy cluster.
//
// Before the collapse the selector was carried IMPLICITLY, by WHICH METHOD the
// cluster called (OpenUnit versus OpenReplicaUnit). One port has one method, so
// that carrier is gone and the bit has to live somewhere explicit. It lives
// here, in the mount's IDENTITY. That keeps the un-leaking intact: the layout
// is a property of the thing being opened, not a question shale asks the
// adapter about itself.
//
// # Construction
//
// The fields are unexported so a MountRef can only be built through SoleMount
// or ReplicaMount. There is deliberately no way to flip the selector on an
// existing ref, and no way to build one that says "replica 3 of the sole
// layout." A ref means one concrete mount and keeps meaning it.
//
// # Map-key property
//
// Because the selector is part of the value, SoleMount(gu) and
// ReplicaMount(NewReplicaUnit(gu, 0)) are DISTINCT map keys, exactly as their
// two prefixes are distinct strings. An adapter may therefore track every mount
// it holds in ONE map without the two layouts evicting each other.
type MountRef struct {
	ru         ReplicaUnit
	replicated bool
}

// SoleMount is the identity of a unit held as its ONE mount: the unit's bytes
// live at the unit's own location, with no replica segment. This is what R=1
// multi-backend opens, and what the reshard paths open for the children they
// build.
//
// It carries replica position 0 (a sole mount IS position 0 of a one-element
// replica set), so Replica and ReplicaUnit answer sensibly, but it is NOT the
// same ref as ReplicaMount at position 0 and does NOT resolve to the same
// bytes.
func SoleMount(gu GenUnit) MountRef {
	return MountRef{ru: NewReplicaUnit(gu, 0)}
}

// ReplicaMount is the identity of ONE replica position of a unit, held as an
// INDEPENDENT durable store alongside the unit's other positions. This is what
// R>1 multi-backend opens. Distinct positions never share bytes, which is the
// property replication is bought with.
func ReplicaMount(ru ReplicaUnit) MountRef {
	return MountRef{ru: ru, replicated: true}
}

// Unit reports the generation-qualified unit this mount belongs to.
func (m MountRef) Unit() GenUnit { return m.ru.Unit }

// Replica reports the replica position this mount holds. A sole mount reports
// 0.
func (m MountRef) Replica() uint8 { return m.ru.Replica }

// ReplicaUnit reports the (unit, position) pair WITHOUT the layout selector.
// Adapters that keep separate per-layout namespaces use it to address the
// replicated one; callers that need the full identity must keep the MountRef.
func (m MountRef) ReplicaUnit() ReplicaUnit { return m.ru }

// Replicated reports the LAYOUT SELECTOR: true for a per-replica mount, false
// for a sole mount. An adapter that derives a storage location from the
// identity MUST branch on this and MUST NOT collapse the two at position 0.
func (m MountRef) Replicated() bool { return m.replicated }

// String renders the identity for logs and errors, naming WHICH of the two
// mounts it is: "unit g1/u5" for a sole mount, "replica g1/u5/r0" for a replica
// mount. The noun is not decoration: it is the difference between two mounts
// whose remaining fields are identical, so a message that drops it cannot be
// read back.
func (m MountRef) String() string {
	if m.replicated {
		return "replica " + m.ru.String()
	}
	return "unit " + m.ru.Unit.String()
}

// CompareMountRefs orders two mount refs: by generation, then unit id, then
// replica position, then layout (sole before replicated). It is a total order
// over distinct refs, so BackendFactory implementations can all sort OpenUnits
// the same way without each inventing a comparison.
func CompareMountRefs(a, b MountRef) int {
	switch {
	case a.ru.Unit.Gen != b.ru.Unit.Gen:
		return cmpUint64(uint64(a.ru.Unit.Gen), uint64(b.ru.Unit.Gen))
	case a.ru.Unit.ID != b.ru.Unit.ID:
		return cmpUint64(uint64(a.ru.Unit.ID), uint64(b.ru.Unit.ID))
	case a.ru.Replica != b.ru.Replica:
		return cmpUint64(uint64(a.ru.Replica), uint64(b.ru.Replica))
	case a.replicated != b.replicated:
		if !a.replicated {
			return -1
		}
		return 1
	}
	return 0
}

func cmpUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
