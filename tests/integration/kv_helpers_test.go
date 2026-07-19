package integration

// Shared KV helpers for the multi-node integration tests: seed a batch of
// keys, read them back through every node, and peek under the routing
// layer at where the bytes physically live.
//
// These tests do not poke at the reconcile machinery directly; they
// observe the public KV surface + per-node backends, which is where any
// operator (or app) would also observe correctness.

import (
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/clustertest"
	"github.com/Zamua/shale/pkg/cluster"
)

// perNodeKeyCount returns a map nodeID -> physical key count by reading
// each node's mounted units directly. Tests use this to peek under the
// routing layer and confirm where the bytes actually live.
func perNodeKeyCount(t *testing.T, nodes []*testNode) map[string]int {
	t.Helper()
	out := make(map[string]int, len(nodes))
	for _, n := range nodes {
		out[n.ID] = n.physicalKeyCount(t)
	}
	return out
}

// putN writes n keys under prefix through c, retrying each on the
// transient codes a client is expected to retry. Bounded so a
// permanently-broken cluster fails loudly instead of hanging.
func putN(t *testing.T, c *cluster.Cluster, prefix string, n int) []string {
	t.Helper()
	keys := make([]string, n)
	for i := range n {
		k := fmt.Sprintf("%s-%04d", prefix, i)
		if err := putWithRetry(c, []byte(k), []byte(expectedValue(k))); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		keys[i] = k
	}
	return keys
}

// putWithRetry retries Put up to 50 times (~5s wall-clock at 100ms
// backoff) on the transient codes, delegating the code CLASSIFICATION to
// the shared harness package (see
// internal/clustertest.PutWithTransientRetry) so it cannot drift from
// pkg/cluster's equivalent. Mirrors the bounded-retry behavior an SDK
// client would implement. The 50-attempt / 100ms budget is this tree's
// own; pkg/cluster's external tests budget their retry window
// differently.
func putWithRetry(c *cluster.Cluster, key, value []byte) error {
	return clustertest.PutWithTransientRetry(c, key, value, 50, 100*time.Millisecond)
}

// expectedValue is the canonical {key -> value} mapping used by every
// test that goes through putN. Keeps the value derivable from the key
// alone so test assertions never have to remember which value they
// wrote.
func expectedValue(key string) string { return "val:" + key }

// assertAllGettable confirms every key is readable via every supplied
// cluster handle (regardless of which node ends up owning it). A
// failure here is the spec's primary correctness contract: a unit
// handoff must never lose data.
func assertAllGettable(t *testing.T, readers []*cluster.Cluster, keys []string) {
	t.Helper()
	for _, c := range readers {
		for _, k := range keys {
			got, err := c.Get([]byte(k))
			if err != nil {
				t.Fatalf("Get %s via %s after handoff: %v", k, c.NodeID(), err)
			}
			if string(got) != expectedValue(k) {
				t.Fatalf("Get %s via %s: got %q want %q", k, c.NodeID(), got, expectedValue(k))
			}
		}
	}
}
