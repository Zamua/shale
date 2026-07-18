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

// servingMarkerAuthorKeyFor maps a ReplicaUnit to the object key of its serving
// marker's AUTHOR record (v0.11.2): the ID of the node that wrote the marker,
// which the draining owner's release gate requires to be a node it actually
// routes the position to.
//
//	servingMarkerAuthorKeyFor(p, ru) = servingMarkerKeyFor(p, ru) + "-author"
//	                                 = "<keyPrefix>u/g<gen>/u<id>/r<replica>/serving-author"
//
// It is a SEPARATE object from "/serving" rather than a widened payload so the
// epoch record stays byte-identical for nodes that predate author attribution
// (they parse "/serving" exactly as before and never fetch this key). The
// "-author" suffix keeps it outside every slatedb-internal key segment, same as
// "/serving" itself.
func servingMarkerAuthorKeyFor(keyPrefix string, ru storageunit.ReplicaUnit) string {
	return servingMarkerKeyFor(keyPrefix, ru) + "-author"
}
