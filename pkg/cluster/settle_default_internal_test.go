package cluster

import "time"

// The rebalance debounce is 5s in production. Only ~11 of the ~46 Config
// literals in this package's tests set RebalanceSettleDelay themselves, so
// without this override the rest pay the full 5s on any path that waits for
// the debounce. Not coordinator-specific: it rode in on a since-deleted
// gossip-only test file, and belongs in a file that outlives any adapter.
func init() {
	defaultSettleDelay = 300 * time.Millisecond
}
