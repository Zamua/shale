package cluster

import (
	"time"

	"github.com/Zamua/shale/pkg/membership"
	"github.com/Zamua/shale/pkg/rebalance"
)

// Tune the in-process test fixtures for speed. Runs once before any test in the
// pkg/cluster test binary; production is unaffected (it never calls either).
//
//   - UseLocalGossipForTests: memberlist's tight loopback preset so 3-node
//     fixtures converge in milliseconds instead of the seconds the LAN preset
//     costs.
//   - SetSweepInterval: the HandedOff -> Done sweep that retires a handed-off
//     range after its grace runs only on this tick. At the 10s production
//     default, every R>1 fixture's WaitForRebalanceIdle blocked ~10s for the
//     next sweep even once the (test-shortened) grace had expired. 200ms keeps
//     the sweep granularity well under the test grace windows without thrashing.
func init() {
	membership.UseLocalGossipForTests()
	rebalance.SetSweepInterval(200 * time.Millisecond)
}
