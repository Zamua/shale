package cas_test

import (
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/coordcontract"
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/cas"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestPortContract runs the shared port-contract harness
// (internal/coordcontract) against this adapter - the same harness every
// coordinator implementation runs.
//
// The wiring stands up a REAL cluster per New call: one started Coordinator
// per member, all over one fresh MemConditionalStore (each New is an
// independent cluster, per the harness contract - the exclusion test builds
// a 5-member and a 4-member cluster in one subtest). Membership is pinned
// stable the way the harness requires by PARKING expiry (a huge ExpiryPolls)
// and disabling the renewal loops: rows never expire, so no renewer is
// needed, and nothing can appear or vanish mid-test. Polls stay fast (well
// under the role-visibility timeout) so a peer's SetRole - which is how
// SetMemberRoles lands, driven on the instance owning that id - reaches the
// returned coordinator's snapshot promptly. Adapter-specific behavior
// (lease expiry, GC races, bootstrap discovery) stays in cas_test.go.
func TestPortContract(t *testing.T) {
	var (
		mu       sync.Mutex
		clusters = map[coord.Coordinator]map[storageunit.NodeID]*cas.Coordinator{}
	)

	coordcontract.RunContract(t, coordcontract.Adapter{
		NewUnstarted: func(_ testing.TB) coord.Coordinator {
			return cas.New(cas.Config{})
		},
		New: func(t testing.TB, self coord.Node, members []coord.Node) coord.Coordinator {
			store := storageunit.NewMemConditionalStore()
			peers := make(map[storageunit.NodeID]*cas.Coordinator, len(members))
			var ret *cas.Coordinator
			for _, m := range members {
				co := cas.New(cas.Config{
					Store:           store,
					PollInterval:    10 * time.Millisecond,
					RenewalInterval: -1,      // no renewals needed: expiry is parked
					ExpiryPolls:     1 << 30, // park expiry: membership must stay put
				})
				if _, err := co.Start(coord.Params{Self: m}); err != nil {
					t.Fatalf("starting member %s: %v", m.ID, err)
				}
				t.Cleanup(func() { _ = co.Close() })
				peers[m.ID] = co
				if m.ID == self.ID {
					ret = co
				}
			}
			if ret == nil {
				t.Fatalf("self %s is not among the members", self.ID)
			}
			// The harness requires the view already converged when New
			// returns: wait for the returned coordinator's poll to absorb
			// every member.
			deadline := time.Now().Add(5 * time.Second)
			for len(ret.View().Members) != len(members) {
				if time.Now().After(deadline) {
					t.Fatalf("view never converged to %d members: %+v", len(members), ret.View().Members)
				}
				time.Sleep(2 * time.Millisecond)
			}
			mu.Lock()
			clusters[ret] = peers
			mu.Unlock()
			return ret
		},
		SetMemberRoles: func(t testing.TB, co coord.Coordinator, id storageunit.NodeID, roles coord.Role) {
			mu.Lock()
			peers := clusters[co]
			mu.Unlock()
			peer := peers[id]
			if peer == nil {
				t.Fatalf("no started instance owns member %s", id)
			}
			if err := peer.SetRole(roles); err != nil {
				t.Fatalf("SetRole(%v) on %s: %v", roles, id, err)
			}
		},
	})
}
