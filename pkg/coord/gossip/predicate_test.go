package gossip_test

import (
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/gossip"
)

// The Populated-tracks-View and TransitionSets-match-View-roles contract
// tests moved to the shared port-contract harness: internal/coordcontract,
// run here by TestPortContract in contract_test.go. This file keeps the
// PRODUCTION-path (gossip-mode) variants and the divergence behavior only
// this adapter has.

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

// TestPlacementMembers_ExposesRingViewDivergence pins the half of the
// method's contract only THIS adapter has: it DIVERGES from View when the
// placement ring has lost a member gossip still knows (the dropped-event
// shape) - the deviation a placement-derived guard must be able to
// observe. The other half (steady-state agreement with View's member set)
// is pinned for every adapter by the shared harness's
// PlacementMembers_MatchViewBasis.
func TestPlacementMembers_ExposesRingViewDivergence(t *testing.T) {
	co := openSolo(t, "pm-solo", time.Hour)
	if !waitForRingMember(co, "pm-solo", 2*time.Second) {
		t.Fatal("local member never landed in the ring")
	}
	// Re-remove until the drop sticks: a queued membership event delivered
	// after the removal re-adds the member (same shape as the reconcile-heal
	// test; the queue is finite and quiesced, so this converges).
	dropped := false
	for attempt := 0; attempt < 50; attempt++ {
		co.TestingRemoveFromRing("pm-solo")
		if len(co.PlacementMembers()) == 0 {
			dropped = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !dropped {
		t.Fatalf("ring drop never stuck; placement=%v", co.PlacementMembers())
	}
	if _, ok := co.View().Member("pm-solo"); !ok {
		t.Fatal("view lost the member on a ring-only drop; it must keep answering from the snapshot")
	}
}
