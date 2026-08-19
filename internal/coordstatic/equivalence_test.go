package coordstatic_test

import (
	"io"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/coordstatic"
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/cas"
	"github.com/Zamua/shale/pkg/storageunit"
)

// PLACEMENT EQUIVALENCE WITH THE SHIPPED ADAPTER.
//
// ~60 white-box tests assert placements computed by this static coordinator
// and then reason about production behavior. That inference is only valid if
// this adapter places units EXACTLY where the shipped CAS adapter does over
// the same member set. Both now share internal/placement, so this test is
// what catches a future divergence in either one's ring construction.
func TestLocate_MatchesCASAdapter(t *testing.T) {
	ids := []string{"node-a", "node-b", "node-c", "node-d", "node-e"}
	nodes := make([]coord.Node, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, coord.Node{ID: storageunit.NodeID(id), Addr: id + ":7947"})
	}

	static := coordstatic.New(nodes[0], nodes)

	store := storageunit.NewMemConditionalStore()
	casCoord := cas.New(cas.Config{Store: store, LogOutput: io.Discard})
	if _, err := casCoord.Start(coord.Params{Self: nodes[0], DeclaredUnitCount: 64}); err != nil {
		t.Fatalf("cas Start: %v", err)
	}
	t.Cleanup(func() { _ = casCoord.Close() })
	// Bring the CAS view to the same member set the static adapter holds.
	for _, n := range nodes[1:] {
		joiner := cas.New(cas.Config{Store: store, LogOutput: io.Discard})
		if _, err := joiner.Start(coord.Params{Self: n, DeclaredUnitCount: 64}); err != nil {
			t.Fatalf("cas Start %s: %v", n.ID, err)
		}
		t.Cleanup(func() { _ = joiner.Close() })
	}
	// The CAS view advances by polling, so wait for it to carry every joiner.
	deadline := time.Now().Add(10 * time.Second)
	for len(casCoord.PlacementMembers()) != len(nodes) {
		if time.Now().After(deadline) {
			t.Fatalf("cas view has %d members, want %d; the comparison would be vacuous",
				len(casCoord.PlacementMembers()), len(nodes))
		}
		time.Sleep(20 * time.Millisecond)
	}

	for _, gen := range []storageunit.Generation{0, 1, 7} {
		for id := uint32(0); id < 64; id++ {
			gu := storageunit.NewGenUnit(gen, storageunit.UnitID(id))
			for _, r := range []int{1, 2, 3} {
				s := static.Locate(gu, r, coord.Placement{})
				c := casCoord.Locate(gu, r, coord.Placement{})
				if len(s) != len(c) {
					t.Fatalf("%s r=%d: static returned %d nodes, cas %d", gu, r, len(s), len(c))
				}
				for i := range s {
					if s[i].ID != c[i].ID {
						t.Fatalf("%s r=%d position %d: static=%s cas=%s (placement diverged between adapters)",
							gu, r, i, s[i].ID, c[i].ID)
					}
				}
			}
		}
	}
}
