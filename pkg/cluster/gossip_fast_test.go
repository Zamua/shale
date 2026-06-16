package cluster

import (
	"time"

	"github.com/Zamua/shale/pkg/membership"
	"github.com/Zamua/shale/pkg/rebalance"
)

// Tune the in-process test fixtures for speed. Runs once before any test in the
// pkg/cluster test binary; production is unaffected (it never calls either).
//
// These tunables are what keeps the pkg/cluster suite fast enough for the TIGHT
// CI timeout (.github/workflows/test.yml). If a fixture starts taking tens of
// seconds, the cause is almost always a convergence delay slipping back to a
// production default (grace / settle / sweep) - shorten the fixture here, do NOT
// raise the CI timeout.
//
//   - UseLocalGossipForTests: memberlist's tight loopback preset so 3-node
//     fixtures converge in milliseconds instead of the seconds the LAN preset
//     costs.
//   - SetSweepInterval: the HandedOff -> Done sweep that retires a handed-off
//     range after its grace runs only on this tick. At the 10s production
//     default, every R>1 fixture's WaitForRebalanceIdle blocked ~10s for the
//     next sweep even once the (test-shortened) grace had expired. 200ms keeps
//     the sweep granularity well under the test grace windows without thrashing.
//   - SetDefaultGraceDuration: a handed-off range lingers this long before the
//     sweep retires it. The 30s production default made any fixture that did not
//     set RebalanceGraceDuration (the inline two-node tests) wait a full 30s on
//     bootstrap. The empty bootstrap handoff has nothing to protect, so 500ms is
//     ample, and it keeps fixtures that DO override grace unaffected.
//   - defaultSettleDelay: the post-membership-change reconcile debounce. The 5s
//     default added a flat 5s to every fixture that did not set
//     RebalanceSettleDelay; 300ms reconciles promptly after the (fast, loopback)
//     gossip settles. Fixtures that set their own settle (incl. the time.Hour
//     manual-reconcile ones) are unaffected.
func init() {
	membership.UseLocalGossipForTests()
	rebalance.SetSweepInterval(200 * time.Millisecond)
	rebalance.SetDefaultGraceDuration(500 * time.Millisecond)
	defaultSettleDelay = 300 * time.Millisecond
}
