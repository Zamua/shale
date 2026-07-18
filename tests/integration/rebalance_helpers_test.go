package integration

// Shared helpers for the v0.3 rebalance integration tests
// (rebalance_growth_test.go, rebalance_shrink_test.go,
// rebalance_write_rejection_test.go, rebalance_failure_test.go).
//
// The cluster layer is supposed to wire a per-node rebalance.Coordinator
// off membership events: a join/leave fires the (debounced) Evaluate
// pass, the Coordinator moves the bytes, and once every range is in a
// terminal state the cluster's physical distribution matches the ring's
// ownership map. These tests do not poke at the Coordinator directly;
// they observe the public KV surface + per-node backends, which is
// where any operator (or app) would also observe correctness.

import (
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/clustertest"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
)

// perNodeKeyCount returns a map nodeID -> physical key count by
// reading each backend directly. Tests use this to peek under the
// routing layer and confirm where the bytes actually live.
func perNodeKeyCount(t *testing.T, nodes []*testNode) map[string]int {
	t.Helper()
	out := make(map[string]int, len(nodes))
	for _, n := range nodes {
		c := countBackend(t, n.Backend, n.ID)
		out[n.ID] = c
	}
	return out
}

func countBackend(t *testing.T, be *memory.Memory, label string) int {
	t.Helper()
	it, err := be.ScanPrefix(nil)
	if err != nil {
		t.Fatalf("scan backend %s: %v", label, err)
	}
	defer func() { _ = it.Close() }()
	n := 0
	for {
		k, _, err := it.Next()
		if err != nil {
			t.Fatalf("scan next backend %s: %v", label, err)
		}
		if k == nil {
			return n
		}
		n++
	}
}

// expectedOwnersFor builds a {key -> ringOwnerID} map by asking the
// canonical ring (built from the given cluster's Members view). Used
// to assert that, post-rebalance, every key is physically held by the
// node the ring says owns it.
func expectedOwnersFor(c *cluster.Cluster, keys []string) map[string]string {
	r := ring.New()
	for _, m := range c.Members() {
		r.Add(m)
	}
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		out[k] = r.LocateKey([]byte(k)).ID
	}
	return out
}

// actualHolders returns a {key -> nodeID} map by scanning each
// backend. A key may appear on multiple nodes during the source's
// grace window before the sweep deletes; in that case the returned
// map reports the first hit + a separate count via duplicateHolders.
func actualHolders(t *testing.T, nodes []*testNode, keys []string) map[string]string {
	t.Helper()
	// Build {key: nodeID} by direct backend Get on each node.
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		for _, n := range nodes {
			v, err := n.Backend.Get([]byte(k))
			if err == nil && v != nil {
				out[k] = n.ID
				break
			}
		}
	}
	return out
}

// waitForRebalanceIdle blocks until every key in want lives on the
// node the ring says owns it (per the first cluster's view, which
// must be in agreement with the others before calling), or the
// deadline expires.
//
// This is a stand-in for an explicit Coordinator.WaitForIdle hook on
// the cluster surface (which the integrate-cluster work is expected
// to expose); using a public-surface observation keeps the test
// independent of whichever exact hook the integration lands.
//
// The first 6s of the budget covers the spec's T_settle (5s default)
// plus a tick of slack; the remainder is the streaming + ack window.
func waitForRebalanceIdle(t *testing.T, ref *cluster.Cluster, nodes []*testNode, keys []string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	want := expectedOwnersFor(ref, keys)
	var lastMismatch string
	for time.Now().Before(deadline) {
		have := actualHolders(t, nodes, keys)
		mismatch := 0
		var first string
		for k, owner := range want {
			got := have[k]
			if got != owner {
				mismatch++
				if first == "" {
					first = fmt.Sprintf("key=%s ring-owner=%s physical=%q", k, owner, got)
				}
			}
		}
		if mismatch == 0 {
			return nil
		}
		lastMismatch = fmt.Sprintf("%d/%d keys mismatched; first: %s", mismatch, len(want), first)
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("rebalance did not converge within %s: %s", timeout, lastMismatch)
}

// putN writes n keys via c with the given key/value prefix + returns
// the list of keys written, in order. The value for key K is
// expectedValue(K), which assertAllGettable inverts on reads.
//
// Retries on FailedPrecondition: per docs/SPEC.md "Cutover", that
// code is the transient migration-window rejection + the SDK is
// expected to retry. Bounded so a permanently-broken cluster fails
// loudly instead of hanging.
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
// backoff) on the v0.4 transient codes, delegating the code
// CLASSIFICATION to the shared harness package (see
// internal/clustertest.PutWithTransientRetry) so it cannot drift from
// pkg/cluster's equivalent. Mirrors the bounded-retry behavior an SDK
// client would implement. The 50-attempt / 100ms budget is this tree's
// own; pkg/cluster's external tests budget their retry window
// differently.
func putWithRetry(c *cluster.Cluster, key, value []byte) error {
	return clustertest.PutWithTransientRetry(c, key, value, 50, 100*time.Millisecond)
}

// expectedValue is the canonical {key -> value} mapping used by every
// rebalance test that goes through putN. Keeps the value derivable
// from the key alone so test assertions never have to remember which
// value they wrote.
func expectedValue(key string) string { return "val:" + key }

// assertAllGettable confirms every key is readable via every supplied
// cluster handle (regardless of which node ends up owning it). A
// failure here is the spec's primary correctness contract: rebalance
// must never lose data.
func assertAllGettable(t *testing.T, readers []*cluster.Cluster, keys []string) {
	t.Helper()
	for _, c := range readers {
		for _, k := range keys {
			got, err := c.Get([]byte(k))
			if err != nil {
				t.Fatalf("Get %s via %s after rebalance: %v", k, c.NodeID(), err)
			}
			if string(got) != expectedValue(k) {
				t.Fatalf("Get %s via %s: got %q want %q", k, c.NodeID(), got, expectedValue(k))
			}
		}
	}
}
