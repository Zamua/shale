package ring_test

import (
	"testing"

	"github.com/Zamua/shale/internal/placement"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// PLACEMENT STABILITY CANARY.
//
// The ring decides which node MOUNTS which storage unit. A change to that
// answer is a mass handoff across the whole fleet: every reassigned position
// is released by one node and acquired by another, and during a rolling
// upgrade nodes on either side of the change disagree about ownership. That
// is survivable (epoch fencing keeps it lossless) but it is never something
// to discover in production.
//
// Nothing else in the suite would notice. The GenUnitBytes vectors pin the
// ring's INPUT; the cross-adapter equivalence test proves both adapters agree
// with EACH OTHER, which stays true when the shared ring changes underneath
// them. This pins the OUTPUT, so an unintended change - a dependency bump
// that silently alters the hash layout, a config edit - shows up as a red
// test instead of a fleet-wide remount.
//
// A DELIBERATE placement change is allowed to update these vectors, but the
// update must be accompanied by the coordinated-restart upgrade note: see
// docs/SPEC.md "Ring placement is a compatibility surface".
func TestPlacement_GoldenAssignment(t *testing.T) {
	r := ring.New()
	for _, n := range []string{"node-a", "node-b", "node-c"} {
		r.Add(ring.Member{ID: n, Addr: n + ":7947"})
	}
	want := []struct {
		unit  int
		nodes []string
	}{
		{0, []string{"node-c", "node-b"}},
		{1, []string{"node-b", "node-a"}},
		{2, []string{"node-b", "node-a"}},
		{3, []string{"node-a", "node-c"}},
		{4, []string{"node-b", "node-a"}},
		{5, []string{"node-a", "node-c"}},
		{6, []string{"node-b", "node-a"}},
		{7, []string{"node-c", "node-b"}},
	}
	for _, tc := range want {
		gu := storageunit.NewGenUnit(0, storageunit.UnitID(tc.unit))
		got := r.LocateKeyN(placement.GenUnitBytes(gu), 2)
		if len(got) != len(tc.nodes) {
			t.Fatalf("unit %d: got %d replicas, want %d", tc.unit, len(got), len(tc.nodes))
		}
		for i := range got {
			if got[i].ID != tc.nodes[i] {
				t.Fatalf("unit %d replica %d: got %s, want %s\n"+
					"PLACEMENT CHANGED. If deliberate, update these vectors AND ship the "+
					"coordinated-restart note; if not, something altered the ring layout.",
					tc.unit, i, got[i].ID, tc.nodes[i])
			}
		}
	}
}
