package gossip_test

import (
	"testing"

	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestPopulated_TracksViewEmpty pins the port contract: Populated() is
// equivalent to !View().Empty() at all times - including before Start, where
// both must report an empty view.
func TestPopulated_TracksViewEmpty(t *testing.T) {
	unstarted := gossip.New(gossip.Config{})
	if unstarted.Populated() {
		t.Fatal("unstarted coordinator reports Populated; View is empty")
	}
	if !unstarted.View().Empty() {
		t.Fatal("unstarted coordinator's View is non-empty")
	}

	c := staticN(3)
	if !c.Populated() {
		t.Fatal("static 3-member coordinator reports unpopulated")
	}
	if c.View().Empty() {
		t.Fatal("static 3-member coordinator's View is empty")
	}
}

// TestTransitionSets_MatchesViewRoles pins the port contract against View as
// the reference: the sets must contain exactly the members whose view roles
// carry the corresponding bit, and a role absent from every member must come
// back as a NIL map (callers rely on nil for the steady-state fast path).
func TestTransitionSets_MatchesViewRoles(t *testing.T) {
	c := staticN(4)

	j, d := c.TransitionSets()
	if j != nil || d != nil {
		t.Fatalf("steady state must return nil sets, got joining=%v draining=%v", j, d)
	}

	// Flip one member joining and one joining+draining via the static facts
	// override - the same source View reads roles from.
	idJoin := storageunit.NodeID("bench-node-01")
	idBoth := storageunit.NodeID("bench-node-02")
	c.TestingSetFacts(coord.Member{Node: coord.Node{ID: idJoin}, Roles: coord.RoleJoining})
	c.TestingSetFacts(coord.Member{Node: coord.Node{ID: idBoth}, Roles: coord.RoleJoining | coord.RoleDraining})

	j, d = c.TransitionSets()
	wantJ := map[storageunit.NodeID]struct{}{idJoin: {}, idBoth: {}}
	wantD := map[storageunit.NodeID]struct{}{idBoth: {}}
	assertSameSet(t, "joining", j, wantJ)
	assertSameSet(t, "draining", d, wantD)

	// Cross-check against View: same members, same bits.
	viewJ := map[storageunit.NodeID]struct{}{}
	viewD := map[storageunit.NodeID]struct{}{}
	for _, m := range c.View().Members {
		if m.Joining() {
			viewJ[m.ID] = struct{}{}
		}
		if m.Draining() {
			viewD[m.ID] = struct{}{}
		}
	}
	assertSameSet(t, "joining-vs-view", j, viewJ)
	assertSameSet(t, "draining-vs-view", d, viewD)
}

func assertSameSet(t *testing.T, name string, got, want map[storageunit.NodeID]struct{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %v, want %v", name, got, want)
	}
	for id := range want {
		if _, ok := got[id]; !ok {
			t.Fatalf("%s: missing %s (got %v, want %v)", name, id, got, want)
		}
	}
}
