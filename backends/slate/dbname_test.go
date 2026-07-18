// dbname_test.go: PURE (tagless, no cgo/slatedb) tests of the durability-
// critical DbName encoding (dbname.go). These pin the load-bearing property
// the whole R>1 replica-independence durability guarantee rests on: distinct
// replica POSITIONS of one unit map to DISTINCT, non-overlapping bucket
// prefixes, so no replica's slatedb database can ever read or write another's
// bytes. The encoding is plain string logic, so it is testable HARD without
// the heavy slatedb Rust build (the real end-to-end isolation is then proven
// against real MinIO in the slatedb+integration-tagged test).

package slate

import (
	"strings"
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

func ru(gen storageunit.Generation, id storageunit.UnitID, replica uint8) storageunit.ReplicaUnit {
	return storageunit.NewReplicaUnit(storageunit.NewGenUnit(gen, id), replica)
}

// TestDbNameFor_R1Encoding is a REGRESSION PIN on the R=1 per-unit DbName.
// The exact strings are the on-disk contract: a deployed cluster's data lives
// at these prefixes, so changing the encoding would orphan every existing
// unit's bytes. If this test has to change, that is a data-migration event,
// not a refactor.
func TestDbNameFor_R1Encoding(t *testing.T) {
	cases := []struct {
		keyPrefix string
		gu        storageunit.GenUnit
		want      string
	}{
		{"", storageunit.NewGenUnit(0, 0), "u/g0/u0"},
		{"", storageunit.NewGenUnit(1, 5), "u/g1/u5"},
		{"", storageunit.NewGenUnit(0, 3), "u/g0/u3"},
		{"cluster-a/", storageunit.NewGenUnit(2, 7), "cluster-a/u/g2/u7"},
	}
	for _, c := range cases {
		got := dbNameFor(c.keyPrefix, c.gu)
		if got != c.want {
			t.Errorf("dbNameFor(%q, %s) = %q, want %q", c.keyPrefix, c.gu, got, c.want)
		}
	}
}

// TestDbNameReplicaFor_Encoding pins the R>1 per-replica DbName exact strings:
// the replica position is a CHILD prefix of the unit's R=1 DbName.
func TestDbNameReplicaFor_Encoding(t *testing.T) {
	cases := []struct {
		keyPrefix string
		ru        storageunit.ReplicaUnit
		want      string
	}{
		{"", ru(1, 5, 0), "u/g1/u5/r0"},
		{"", ru(1, 5, 1), "u/g1/u5/r1"},
		{"", ru(0, 0, 2), "u/g0/u0/r2"},
		{"cluster-a/", ru(2, 7, 1), "cluster-a/u/g2/u7/r1"},
	}
	for _, c := range cases {
		got := dbNameReplicaFor(c.keyPrefix, c.ru)
		if got != c.want {
			t.Errorf("dbNameReplicaFor(%q, %s) = %q, want %q", c.keyPrefix, c.ru, got, c.want)
		}
	}
}

// TestDbNameReplicaFor_ReplicaPositionsDistinct is the DURABILITY-CRITICAL
// property: replica 0 and replica 1 of the SAME unit produce DISTINCT DbName
// strings. If they aliased, the two replicas would share one slatedb database
// and replication would buy nothing - losing the node serving one position
// would lose the "other" copy too, because it was never a separate copy.
func TestDbNameReplicaFor_ReplicaPositionsDistinct(t *testing.T) {
	unit := storageunit.NewGenUnit(1, 5)
	r0 := dbNameReplicaFor("", storageunit.NewReplicaUnit(unit, 0))
	r1 := dbNameReplicaFor("", storageunit.NewReplicaUnit(unit, 1))
	if r0 == r1 {
		t.Fatalf("replica 0 and replica 1 of %s alias to the same DbName %q: they would share one database and replication would not be independent", unit, r0)
	}
}

// TestDbNameReplicaFor_GloballyDistinct sweeps a grid of (gen, unit, replica)
// triples and asserts EVERY pair maps to a unique DbName - no two distinct
// triples ever collide. A collision anywhere (across replicas of one unit,
// across units, or across generations) would alias two independent databases
// onto one prefix. Also asserts no DbName is a strict prefix of another (a
// prefix overlap would let one database's object keys fall under another's
// list-prefix, which is the same aliasing failure in object-store terms).
func TestDbNameReplicaFor_GloballyDistinct(t *testing.T) {
	var rus []storageunit.ReplicaUnit
	for gen := storageunit.Generation(0); gen < 3; gen++ {
		for id := storageunit.UnitID(0); id < 4; id++ {
			for r := uint8(0); r < 3; r++ {
				rus = append(rus, ru(gen, id, r))
			}
		}
	}

	names := make(map[string]storageunit.ReplicaUnit, len(rus))
	for _, x := range rus {
		n := dbNameReplicaFor("", x)
		if prev, dup := names[n]; dup {
			t.Fatalf("DbName collision: %s and %s both map to %q", prev, x, n)
		}
		names[n] = x
	}

	// No name is a strict prefix of another (would cause object-key overlap).
	// A replica DbName ends at "/r<n>"; appending the path separator before the
	// containment check makes the test reject true prefix overlap ("u/g0/u1"
	// vs "u/g0/u10") without flagging the shared parent segments that are
	// expected (every name shares the "u/" root).
	for a := range names {
		for b := range names {
			if a == b {
				continue
			}
			if strings.HasPrefix(b, a+"/") {
				t.Fatalf("DbName %q is a path-prefix of %q: their object keys would overlap", a, b)
			}
		}
	}
}

// TestDbNameReplicaFor_SharesUnitPrefix pins that the replica DbName reuses the
// unit's R=1 DbName verbatim as its parent (dbNameReplicaFor(p, ru) is exactly
// dbNameFor(p, ru.Unit) + "/r<n>"). This is what guarantees the unit-prefix
// encoding can never drift between the R=1 and R>1 paths.
func TestDbNameReplicaFor_SharesUnitPrefix(t *testing.T) {
	unit := storageunit.NewGenUnit(1, 5)
	unitName := dbNameFor("p/", unit)
	for r := uint8(0); r < 3; r++ {
		rep := dbNameReplicaFor("p/", storageunit.NewReplicaUnit(unit, r))
		if !strings.HasPrefix(rep, unitName+"/") {
			t.Fatalf("replica DbName %q does not extend unit DbName %q: the unit prefix drifted", rep, unitName)
		}
	}
}

// TestServingMarkerKeyFor_Encoding pins the v0.8 Phase 2e serving-marker object
// key: it is the position's replica DbName plus a fixed "/serving" suffix, a
// SIBLING of the position's slatedb database so it can be GET/PUT directly
// without opening the database. Tagless (no slatedb build) because it is pure
// string logic, like the rest of the DbName encoding.
func TestServingMarkerKeyFor_Encoding(t *testing.T) {
	cases := []struct {
		keyPrefix string
		ru        storageunit.ReplicaUnit
		want      string
	}{
		{"", ru(0, 0, 0), "u/g0/u0/r0/serving"},
		{"", ru(1, 5, 2), "u/g1/u5/r2/serving"},
		{"p/", ru(3, 9, 1), "p/u/g3/u9/r1/serving"},
	}
	for _, c := range cases {
		if got := servingMarkerKeyFor(c.keyPrefix, c.ru); got != c.want {
			t.Errorf("servingMarkerKeyFor(%q, %s) = %q, want %q", c.keyPrefix, c.ru, got, c.want)
		}
	}
}

// TestServingMarkerKeyFor_ExtendsReplicaPrefix pins that the marker key is a
// strict child of the position's replica DbName (so it lives alongside, never
// collides with, the database's own objects, and the marker of one replica
// position never collides with another's).
func TestServingMarkerKeyFor_ExtendsReplicaPrefix(t *testing.T) {
	r := ru(1, 5, 1)
	dbName := dbNameReplicaFor("p/", r)
	marker := servingMarkerKeyFor("p/", r)
	if !strings.HasPrefix(marker, dbName+"/") {
		t.Fatalf("serving marker key %q does not extend replica DbName %q", marker, dbName)
	}
	// A different position's marker must not collide.
	other := servingMarkerKeyFor("p/", ru(1, 5, 0))
	if marker == other {
		t.Fatalf("serving marker keys for distinct replica positions collide: %q", marker)
	}
}

// TestDbNameForRef_MatchesUnconsolidatedEncodings pins that routing a DbName
// through the consolidated unitRef seam produces EXACTLY the string the two
// hand-written encodings produced before the R=1 / R>1 factory mirror was
// merged. dbNameForRef is the only place the split is decided, so this is the
// guard that the merge did not move any deployed cluster's bytes.
func TestDbNameForRef_MatchesUnconsolidatedEncodings(t *testing.T) {
	prefixes := []string{"", "cluster-a/"}
	for _, kp := range prefixes {
		for gen := storageunit.Generation(0); gen < 3; gen++ {
			for id := storageunit.UnitID(0); id < 4; id++ {
				gu := storageunit.NewGenUnit(gen, id)

				// R=1 surface: must resolve to the plain per-unit encoding.
				if got, want := dbNameForRef(kp, refUnit(gu)), dbNameFor(kp, gu); got != want {
					t.Fatalf("dbNameForRef(%q, refUnit(%s)) = %q, want the R=1 encoding %q", kp, gu, got, want)
				}

				// R>1 surface: must resolve to the per-replica encoding, at
				// EVERY position including 0.
				for r := uint8(0); r < 3; r++ {
					x := storageunit.NewReplicaUnit(gu, r)
					if got, want := dbNameForRef(kp, refReplica(x)), dbNameReplicaFor(kp, x); got != want {
						t.Fatalf("dbNameForRef(%q, refReplica(%s)) = %q, want the R>1 encoding %q", kp, x, got, want)
					}
				}
			}
		}
	}
}

// TestDbNameForRef_R1AndReplica0DoNotAlias is the DATA-LOSS GUARD on the
// consolidation. The R=1 surface and the R>1 surface both funnel through
// unitRef, and refUnit(gu) carries replica position 0 - so the one mistake that
// would silently orphan a deployed R=1 cluster's bytes is letting refUnit(gu)
// resolve through the REPLICA encoding (dbNameReplicaFor at replica 0). The two
// prefixes are genuinely different strings ("u/g1/u5" vs "u/g1/u5/r0"), and an
// R=1 deployment's data lives at the former, so an R=1 open resolving to the
// latter would mount an EMPTY database and report the unit as fresh.
func TestDbNameForRef_R1AndReplica0DoNotAlias(t *testing.T) {
	for _, kp := range []string{"", "p/"} {
		gu := storageunit.NewGenUnit(1, 5)
		r1Name := dbNameForRef(kp, refUnit(gu))
		rep0Name := dbNameForRef(kp, refReplica(storageunit.NewReplicaUnit(gu, 0)))
		if r1Name == rep0Name {
			t.Fatalf("R=1 unit %s and its R>1 replica-0 position resolve to the SAME DbName %q: an R=1 deployment's bytes would be orphaned (or aliased onto the replica prefix)", gu, r1Name)
		}
		// And specifically: the R=1 name must NOT have picked up a replica
		// segment (the exact regression the consolidation could introduce).
		if want := dbNameFor(kp, gu); r1Name != want {
			t.Fatalf("R=1 DbName drifted: got %q, want %q", r1Name, want)
		}
	}
}

// TestUnitRef_R1AndReplica0AreDistinctMapKeys pins the OTHER half of the
// no-alias property: the factory's Handle now tracks R=1 mounts and R>1 replica
// mounts in ONE unitRef-keyed map, which is only safe because the replicated
// flag keeps refUnit(gu) and refReplica(gu, 0) distinct as map keys. If they
// collided, an R=1 mount and an R>1 replica-0 mount of the same unit would
// evict each other from the mount map.
func TestUnitRef_R1AndReplica0AreDistinctMapKeys(t *testing.T) {
	gu := storageunit.NewGenUnit(1, 5)
	m := map[unitRef]string{}
	m[refUnit(gu)] = "r1"
	m[refReplica(storageunit.NewReplicaUnit(gu, 0))] = "rep0"
	if len(m) != 2 {
		t.Fatalf("refUnit(%s) and refReplica(%s/r0) collided as map keys: map has %d entries, want 2", gu, gu, len(m))
	}
	if m[refUnit(gu)] != "r1" {
		t.Fatalf("refUnit key was clobbered by refReplica: got %q", m[refUnit(gu)])
	}
}
