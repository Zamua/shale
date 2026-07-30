package gossip_test

import (
	"testing"

	"github.com/Zamua/shale/internal/coordcontract"
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestPortContract runs the shared port-contract harness
// (internal/coordcontract) against this adapter - the same harness every
// coordinator implementation runs.
//
// The wiring uses the STATIC construction: the identical placement math
// and query paths with no transport, which is exactly the fixed, stable
// membership the harness requires, and roles land through the facts
// override (the source the static View reads). The PRODUCTION paths keep
// their own tests in this package: gossip-mode role liveness
// (TestTransitionSets_GossipMode_MatchesViewRoles), bootstrap discovery
// (the TestStart_* tests), and the divergence behavior the harness
// deliberately does NOT pin - the ring trailing the view on a dropped
// event - stays in TestPlacementMembers_ExposesRingViewDivergence and the
// reconcile-heal tests.
func TestPortContract(t *testing.T) {
	coordcontract.RunContract(t, coordcontract.Adapter{
		NewUnstarted: func(_ testing.TB) coord.Coordinator {
			return gossip.New(gossip.Config{})
		},
		New: func(_ testing.TB, self coord.Node, members []coord.Node) coord.Coordinator {
			return gossip.NewStatic(self, members)
		},
		SetMemberRoles: func(_ testing.TB, co coord.Coordinator, id storageunit.NodeID, roles coord.Role) {
			co.(*gossip.Coordinator).TestingSetFacts(coord.Member{
				Node:  coord.Node{ID: id},
				Roles: roles,
			})
		},
	})
}
