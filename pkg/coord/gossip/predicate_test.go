package gossip_test

import (
	"io"
	"strconv"
	"testing"
	"time"

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

// TestTransitionSets_GossipMode_MatchesViewRoles pins the same contract on
// the PRODUCTION (gossip) path, where View and TransitionSets read the same
// live membership snapshot: a role this node advertises via SetRole must
// appear in both, and retracting it must empty both back to nil.
func TestTransitionSets_GossipMode_MatchesViewRoles(t *testing.T) {
	co := openSolo(t, "pred-solo", time.Hour)

	j, d := co.TransitionSets()
	if j != nil || d != nil {
		t.Fatalf("steady state must return nil sets, got joining=%v draining=%v", j, d)
	}

	if err := co.SetRole(coord.RoleJoining); err != nil {
		t.Fatalf("SetRole: %v", err)
	}
	j, d = co.TransitionSets()
	if _, ok := j["pred-solo"]; !ok || len(j) != 1 || d != nil {
		t.Fatalf("after SetRole(Joining): joining=%v draining=%v, want self joining only", j, d)
	}
	m, ok := co.View().Member("pred-solo")
	if !ok || !m.Joining() {
		t.Fatalf("View disagrees with TransitionSets: member=%+v ok=%v", m, ok)
	}

	if err := co.SetRole(0); err != nil {
		t.Fatalf("SetRole(0): %v", err)
	}
	j, d = co.TransitionSets()
	if j != nil || d != nil {
		t.Fatalf("after role retraction: joining=%v draining=%v, want nil/nil", j, d)
	}
}

// TestStart_SoloFound_ReportsFoundedAndDropsJoining pins the DISCOVERY-based
// bootstrap answer: a node with seeds CONFIGURED but nobody reachable (the
// solo-start first pod) FOUNDED the cluster, whatever its config says. It must
// report BootstrapFounded and retract the tentatively-advertised Joining role
// - a founder that kept advertising Joining would exclude itself from its own
// current set with nobody left to be current.
func TestStart_SoloFound_ReportsFoundedAndDropsJoining(t *testing.T) {
	deadSeed := "127.0.0.1:" + strconv.Itoa(freePort(t))
	co := gossip.New(gossip.Config{
		BindAddr:            "127.0.0.1:" + strconv.Itoa(freePort(t)),
		Seeds:               []string{deadSeed},
		LogOutput:           io.Discard,
		ReconcileInterval:   time.Hour,
		RejoinInterval:      -1,
		MetaRefreshInterval: -1,
	})
	boot, err := co.Start(coord.Params{
		Self:         coord.Node{ID: "solo-found", Addr: "127.0.0.1:1"},
		InitialRoles: coord.RoleJoining,
		SoloStart:    true,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = co.Close() })
	if boot != coord.BootstrapFounded {
		t.Fatalf("solo-start that contacted nobody reported %v, want BootstrapFounded", boot)
	}
	if m, ok := co.View().Member("solo-found"); !ok || m.Joining() {
		t.Fatalf("solo founder still advertises Joining (member=%+v ok=%v); the retraction did not land", m, ok)
	}
	if j, _ := co.TransitionSets(); j != nil {
		t.Fatalf("solo founder appears in the joining set: %v", j)
	}
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

// TestPlacementMembers_ExposesTheBasisSplit pins the method's reason to exist:
// it equals View's member set when the bases agree, and DIVERGES from View when
// the placement ring has lost a member gossip still knows (the dropped-event
// shape) - the deviation a placement-derived guard must be able to observe.
func TestPlacementMembers_ExposesTheBasisSplit(t *testing.T) {
	static := staticN(3)
	pm := static.PlacementMembers()
	vm := static.View().Members
	if len(pm) != len(vm) {
		t.Fatalf("static: placement %d members, view %d - a single-basis mode must agree", len(pm), len(vm))
	}
	for i := range pm {
		if pm[i].ID != vm[i].ID {
			t.Fatalf("static: placement[%d]=%s view[%d]=%s", i, pm[i].ID, i, vm[i].ID)
		}
	}

	co := openSolo(t, "pm-solo", time.Hour)
	if !waitForRingMember(co, "pm-solo", 2*time.Second) {
		t.Fatal("local member never landed in the ring")
	}
	co.TestingRemoveFromRing("pm-solo")
	if got := co.PlacementMembers(); len(got) != 0 {
		t.Fatalf("placement basis should be empty after the ring drop, got %v", got)
	}
	if _, ok := co.View().Member("pm-solo"); !ok {
		t.Fatal("view lost the member on a ring-only drop; it must keep answering from the snapshot")
	}
}
