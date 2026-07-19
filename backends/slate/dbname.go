// dbname.go: the PURE per-unit / per-replica DbName encoding for the
// slatedb-backed BackendFactory. It is the durability-critical key-prefix
// logic - the function that decides where in the shared bucket each unit
// (and each replica position) lives. It is INTENTIONALLY tagless (no
// slatedb / cgo dependency) so the encoding's load-bearing properties -
// per-replica prefix DISJOINTNESS (the replica-independence durability
// guarantee) and R=1 backward-compatibility - can be unit-tested WITHOUT
// the heavy slatedb Rust/cgo build.
//
// The slatedb-tagged BackingConfig.dbName / dbNameReplica methods (factory.go)
// delegate here, so the tagged code and the pure tests share ONE encoding -
// the test can never drift from the code it pins.

package slate

import (
	"fmt"

	"github.com/Zamua/shale/pkg/storageunit"
)

// unitPrefix namespaces the per-unit databases away from any unrelated
// object in the same bucket and gives PresentUnits a single list-prefix to
// scan. A GenUnit gu maps to the DbName "<keyPrefix>u/g<gen>/u<id>".
const unitPrefix = "u/"

// dbNameFor maps a GenUnit to its deterministic slatedb DbName (the
// key-prefix within the shared bucket): "<keyPrefix>u/g<gen>/u<id>". The
// generation segment is ahead of the unit segment, matching the ring's
// genUnitBytes ordering; the pair is collision-free, so two GenUnits never
// share a database. keyPrefix lets one bucket host multiple unrelated
// clusters (default empty; should end in "/" when set).
func dbNameFor(keyPrefix string, gu storageunit.GenUnit) string {
	return keyPrefix + unitPrefix + fmt.Sprintf("g%d/u%d", gu.Gen, gu.ID)
}

// dbNameReplicaFor maps a ReplicaUnit to its deterministic slatedb DbName
// (R>1 multi-backend). It appends a replica segment to the unit's R=1
// DbName, reusing dbNameFor(keyPrefix, ru.Unit) verbatim so the unit prefix
// can never drift between the R=1 and R>1 encodings:
//
//	dbNameReplicaFor(p, ru) = dbNameFor(p, ru.Unit) + "/r<replica>"
//	                        = "<keyPrefix>u/g<gen>/u<id>/r<replica>"
//
// The replica position is a CHILD prefix of the unit's prefix, so each
// (gen, unit, replica) triple maps to a DISTINCT slatedb database at a
// DISTINCT, non-overlapping prefix: replica 0 of g1/u5 lives at
// "u/g1/u5/r0", replica 1 at "u/g1/u5/r1". This prefix-disjointness IS the
// replica-independence durability guarantee - no object key under one
// replica's prefix is ever read or written by another replica's database,
// so losing the node serving one position cannot truncate, fence, or
// corrupt another position's bytes. It is the object-store realization of
// ReplicaUnit.String() ("g<gen>/u<id>/r<replica>").
func dbNameReplicaFor(keyPrefix string, ru storageunit.ReplicaUnit) string {
	return dbNameFor(keyPrefix, ru.Unit) + fmt.Sprintf("/r%d", ru.Replica)
}

// unitRef is the identity the factory's Backing/Handle helpers work over, so
// the fence arithmetic, the open sequence and the release sequence each exist
// ONCE instead of in an R=1 copy and an R>1 copy that can drift apart.
//
// It is now an ALIAS for storageunit.MountRef, the identity shale's one storage
// port is keyed by. The shape did not change: a ReplicaUnit PLUS the one bit
// selecting which of the TWO encodings above the mount's bytes live at.
//
//	Replicated() == false -> dbNameFor(kp, Unit())        = "<kp>u/g<gen>/u<id>"
//	Replicated() == true  -> dbNameReplicaFor(kp, ru)     = "<kp>u/g<gen>/u<id>/r<rep>"
//
// The shape MOVED because the collapse deleted the second port. Before it, the
// selector was carried by which method the cluster called (OpenUnit versus
// OpenReplicaUnit); one port has one method, so the bit had to become part of
// the identity that crosses the seam. This adapter kept its derivation
// byte-for-byte; only where the bit is spelled changed.
//
// THE FLAG IS LOAD-BEARING, NOT COSMETIC, AND THE TWO ENCODINGS DO NOT MEET AT
// REPLICA 0. A deployed R=1 cluster's bytes live at the R=1 prefix; the R>1
// replica-0 prefix is a strict CHILD of it ("u/g1/u5" vs "u/g1/u5/r0"), a
// DIFFERENT string. Resolving the R=1 path through the replica-0 encoding would
// point every existing unit at an empty prefix and silently orphan its data.
//
// dbNameForRef lives in this PURE, TAGLESS file precisely so that disjointness
// is pinned by dbname_test.go without the heavy slatedb cgo build: it is the
// ONLY place the encoding choice is made, and
// TestDbNameForRef_R1AndReplica0DoNotAlias is the guard on it.
//
// Because the flag is part of the value, refUnit(gu) and refReplica(gu, 0) are
// also DISTINCT map keys, which is what lets the factory's Handle track both
// families of mounts in ONE map without aliasing them.
type unitRef = storageunit.MountRef

// refUnit is the sole-mount identity for gu, what an R=1 cluster opens.
func refUnit(gu storageunit.GenUnit) unitRef { return storageunit.SoleMount(gu) }

// refReplica is the identity of replica position ru, what an R>1 cluster opens.
func refReplica(ru storageunit.ReplicaUnit) unitRef { return storageunit.ReplicaMount(ru) }

// dbNameForRef resolves a unitRef to its slatedb DbName. It is the SINGLE
// place the sole / per-replica on-disk encoding split is decided; see unitRef
// for why collapsing the two would orphan a deployed R=1 cluster's data.
func dbNameForRef(keyPrefix string, r unitRef) string {
	if r.Replicated() {
		return dbNameReplicaFor(keyPrefix, r.ReplicaUnit())
	}
	return dbNameFor(keyPrefix, r.Unit())
}

// servingMarkerKeyFor maps a ReplicaUnit to the object key of its durable
// SERVING MARKER (v0.8 Phase 2e, Option B overlap handoff). It is a small
// object that lives ALONGSIDE the position's slatedb database, NOT inside it:
// it carries the new owner's open epoch and is written EXACTLY ONCE at the
// Acquiring -> Ready mount flip, then polled by the old (draining) owner as the
// POLL-ONLY release signal. Keeping it a sibling object (the database prefix +
// a fixed "/serving" suffix) rather than a key INSIDE the slatedb database is
// deliberate: ReadServingMarker must read it WITHOUT opening (mounting) the
// database (the old owner must not mount the position it is releasing), so the
// marker is a plain object the S3 client GETs/PUTs directly.
//
//	servingMarkerKeyFor(p, ru) = dbNameReplicaFor(p, ru) + "/serving"
//	                           = "<keyPrefix>u/g<gen>/u<id>/r<replica>/serving"
//
// The "/serving" suffix is OUTSIDE every slatedb-internal key segment (slatedb
// owns "wal/", "manifest/", "compacted/" under the db prefix), so the marker
// object never collides with the database's own objects, and parseUnitKey
// ignores it (a "serving" tail is not a slatedb object root either way).
func servingMarkerKeyFor(keyPrefix string, ru storageunit.ReplicaUnit) string {
	return dbNameReplicaFor(keyPrefix, ru) + "/serving"
}

// servingMarkerKeyForRef is servingMarkerKeyFor over the MOUNT identity, so the
// marker follows the SAME sole / per-replica split as the mount's database
// rather than being pinned to the replica encoding:
//
//	servingMarkerKeyForRef(p, r) = dbNameForRef(p, r) + "/serving"
//
// For a replicated ref this is byte-for-byte servingMarkerKeyFor(p, ru), which
// is what every marker written to date used and what
// TestServingMarkerKeyForRef_MatchesReplicaEncoding pins. For a sole ref it is
// a DIFFERENT key, which is the point: deriving both through the replica
// encoding would alias a sole mount's marker onto replica 0's, the same class
// of aliasing dbNameForRef exists to prevent.
func servingMarkerKeyForRef(keyPrefix string, r unitRef) string {
	return dbNameForRef(keyPrefix, r) + "/serving"
}
