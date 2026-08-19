package coordstatic_test

import (
	"testing"

	"github.com/Zamua/shale/internal/coordcontract"
	"github.com/Zamua/shale/internal/coordstatic"
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestPortContract runs the shared port-contract harness
// (internal/coordcontract) against this adapter - the same harness every
// coordinator implementation runs.
//
// The static construction is exactly the fixed, stable membership the
// harness requires, and roles land through the facts override (the source
// the static View reads).
func TestPortContract(t *testing.T) {
	coordcontract.RunContract(t, coordcontract.Adapter{
		NewUnstarted: func(_ testing.TB) coord.Coordinator {
			return coordstatic.NewUnstarted()
		},
		New: func(_ testing.TB, self coord.Node, members []coord.Node) coord.Coordinator {
			return coordstatic.New(self, members)
		},
		SetMemberRoles: func(_ testing.TB, co coord.Coordinator, id storageunit.NodeID, roles coord.Role) {
			co.(*coordstatic.Coordinator).TestingSetFacts(coord.Member{
				Node:  coord.Node{ID: id},
				Roles: roles,
			})
		},
	})
}
