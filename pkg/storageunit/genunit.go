package storageunit

import "fmt"

// Generation is the cluster's reshard counter. The cluster boots at
// generation 0 with some unit count N; a DOUBLING reshard (N -> 2N)
// advances it to the next generation and is the ONLY thing that does.
//
// Why the generation exists at all (v0.8 Phase 4): a unit's UnitID alone
// is NOT a stable storage identity across a doubling. Old gen-g unit K
// (under count N) and new gen-(g+1) unit K (under count 2N) are DIFFERENT
// databases that must coexist while the bisect copies K's data into its two
// children - if they shared storage they would collide (the new unit would
// be writing over the old unit's still-serving bytes). Qualifying the
// identity by generation keeps them distinct. See GenUnit.
type Generation uint64

// GenUnit is the generation-qualified STORAGE IDENTITY of a unit: the pair
// (Generation, UnitID). It is what the BackendFactory opens/closes, what the
// cluster keys its mount map by, and what the ring places for ownership.
//
// A bare UnitID is only meaningful paired with the UnitCount it was derived
// under (the same key lands on different UnitIDs at N and 2N). GenUnit makes
// that pairing explicit and DURABLE: two GenUnits that share a UnitID but
// differ in Generation are two distinct databases. This is the central
// Phase-4 change - it lets gen-g unit K and gen-(g+1) unit K (the doubling's
// "old" and one of its two "new" units) live side by side during the online
// bisect without overwriting each other.
type GenUnit struct {
	Gen Generation
	ID  UnitID
}

// NewGenUnit builds a GenUnit. A trivial constructor kept for call-site
// readability (NewGenUnit(g, k) reads better than a bare struct literal at
// the routing seams) and so a future invariant (e.g. validating ID against a
// count) has one place to live.
func NewGenUnit(g Generation, id UnitID) GenUnit { return GenUnit{Gen: g, ID: id} }

// String renders the identity as "g<gen>/u<id>" for logs and test failures,
// e.g. "g1/u5". The slash makes the two coordinates unambiguous at a glance.
func (gu GenUnit) String() string { return fmt.Sprintf("g%d/u%d", gu.Gen, gu.ID) }
