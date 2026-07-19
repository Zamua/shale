// servingmarker_ref_test.go: PURE (tagless) pins on the serving-marker key
// derivation after the storage-port collapse. dbname_test.go pins the DbName
// encodings and must not change; these pin the marker key, which moved from
// being addressed by ReplicaUnit to being addressed by the mount identity.

package slate

import (
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

// TestServingMarkerKeyForRef_MatchesReplicaEncoding is the REGRESSION PIN on
// the marker key. Every serving marker written to date lives at
// servingMarkerKeyFor(kp, ru); routing the derivation through the mount
// identity must reproduce that string EXACTLY at every position, or a draining
// owner would poll a key its successor never writes and would never release.
func TestServingMarkerKeyForRef_MatchesReplicaEncoding(t *testing.T) {
	for _, kp := range []string{"", "cluster-a/"} {
		for gen := storageunit.Generation(0); gen < 3; gen++ {
			for id := storageunit.UnitID(0); id < 4; id++ {
				gu := storageunit.NewGenUnit(gen, id)
				for r := uint8(0); r < 3; r++ {
					x := storageunit.NewReplicaUnit(gu, r)
					got := servingMarkerKeyForRef(kp, refReplica(x))
					if want := servingMarkerKeyFor(kp, x); got != want {
						t.Fatalf("servingMarkerKeyForRef(%q, refReplica(%s)) = %q, want the pre-collapse encoding %q", kp, x, got, want)
					}
				}
			}
		}
	}
}

// TestServingMarkerKeyForRef_SoleAndReplica0DoNotAlias is the marker-side twin
// of TestDbNameForRef_R1AndReplica0DoNotAlias. Deriving both layouts' markers
// through the replica encoding would put a sole mount's marker on top of
// replica 0's, so a sole owner and a replica-0 owner would each read the
// other's liveness signal and could both release.
func TestServingMarkerKeyForRef_SoleAndReplica0DoNotAlias(t *testing.T) {
	for _, kp := range []string{"", "p/"} {
		gu := storageunit.NewGenUnit(1, 5)
		soleKey := servingMarkerKeyForRef(kp, refUnit(gu))
		rep0Key := servingMarkerKeyForRef(kp, refReplica(storageunit.NewReplicaUnit(gu, 0)))
		if soleKey == rep0Key {
			t.Fatalf("sole and replica-0 serving markers collide at %q: two independent mounts would share one liveness signal", soleKey)
		}
	}
}

// TestServingMarkerKeyForRef_ExtendsItsOwnDbName pins that whichever layout a
// mount uses, its marker is a strict child of THAT mount's own database prefix.
// The marker must sit alongside the bytes it describes, never under another
// mount's prefix.
func TestServingMarkerKeyForRef_ExtendsItsOwnDbName(t *testing.T) {
	gu := storageunit.NewGenUnit(3, 9)
	refs := []unitRef{
		refUnit(gu),
		refReplica(storageunit.NewReplicaUnit(gu, 0)),
		refReplica(storageunit.NewReplicaUnit(gu, 1)),
	}
	for _, r := range refs {
		db := dbNameForRef("p/", r)
		marker := servingMarkerKeyForRef("p/", r)
		if want := db + "/serving"; marker != want {
			t.Fatalf("servingMarkerKeyForRef(%s) = %q, want %q (a child of its own DbName)", r, marker, want)
		}
	}
}
