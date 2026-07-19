package cluster

import (
	"time"

	"github.com/Zamua/shale/pkg/membership"
)

// Tune the in-process test fixtures for speed. Runs once before any test in the
// pkg/cluster test binary; production is unaffected (it never calls either).
//
// These tunables are what keeps the pkg/cluster suite fast enough for the TIGHT
// CI timeout (.github/workflows/test.yml). If a fixture starts taking tens of
// seconds, the cause is almost always a convergence delay slipping back to a
// production default (settle) - shorten the fixture here, do NOT raise the CI
// timeout.
//
//   - UseLocalGossipForTests: memberlist's tight loopback preset so 3-node
//     fixtures converge in milliseconds instead of the seconds the LAN preset
//     costs.
//   - defaultSettleDelay: the post-membership-change reconcile debounce. The 5s
//     default added a flat 5s to every fixture that did not set
//     RebalanceSettleDelay; 300ms reconciles promptly after the (fast, loopback)
//     gossip settles. Fixtures that set their own settle (incl. the time.Hour
//     manual-reconcile ones) are unaffected.
func init() {
	membership.UseLocalGossipForTests()
	defaultSettleDelay = 300 * time.Millisecond
}
