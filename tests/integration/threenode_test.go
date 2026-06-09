package integration

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
)

// threeNodeFixture spins up three in-process cluster nodes with N1 as
// the seed for N2 + N3, waits for full membership convergence, and
// returns the trio. Each node uses its own memory backend so tests can
// peer at physical placement directly.
type threeNodeFixture struct {
	N1 *testNode
	N2 *testNode
	N3 *testNode
}

func (f *threeNodeFixture) Nodes() []*testNode { return []*testNode{f.N1, f.N2, f.N3} }

func (f *threeNodeFixture) Clusters() []*cluster.Cluster {
	return []*cluster.Cluster{f.N1.Cluster, f.N2.Cluster, f.N3.Cluster}
}

func newThreeNodeFixture(t *testing.T) *threeNodeFixture {
	t.Helper()
	n1 := startTestNode(t, "n1", "")
	n2 := startTestNode(t, "n2", n1.BindAddr)
	n3 := startTestNode(t, "n3", n1.BindAddr)
	f := &threeNodeFixture{N1: n1, N2: n2, N3: n3}
	if err := waitForMembersAll(f.Clusters(), 3, 10*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	return f
}

// TestThreeNode_PutGetForwarding writes 1000 keys via N1 + reads each
// one back via N3. With three nodes on the ring + uniform random keys,
// roughly two-thirds of the writes hit a remote owner, exercising the
// full forwarding path. Reads from N3 likewise forward to whichever
// node owns the key. End-state: every value round-trips correctly.
func TestThreeNode_PutGetForwarding(t *testing.T) {
	f := newThreeNodeFixture(t)

	const n = 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		if err := f.N1.Cluster.Put(key, val); err != nil {
			t.Fatalf("Put %s via N1: %v", key, err)
		}
	}

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		want := []byte(fmt.Sprintf("val-%04d", i))
		got, err := f.N3.Cluster.Get(key)
		if err != nil {
			t.Fatalf("Get %s via N3: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get %s via N3: got %q want %q", key, got, want)
		}
	}

	// Sanity-check: the writes were actually distributed, not all
	// landing on one node. If routing collapsed to a single owner we
	// would still pass the round-trip above but lose the test's
	// "across-the-cluster" guarantee, so assert the spread here.
	totals := make(map[string]int)
	for _, n := range f.Nodes() {
		it, err := n.Backend.ScanPrefix(nil)
		if err != nil {
			t.Fatalf("backend scan on %s: %v", n.ID, err)
		}
		for {
			k, _, err := it.Next()
			if err != nil {
				t.Fatalf("backend scan next on %s: %v", n.ID, err)
			}
			if k == nil {
				break
			}
			totals[n.ID]++
		}
		_ = it.Close()
	}
	if len(totals) < 2 {
		t.Fatalf("expected at least two nodes to hold keys, got distribution=%v", totals)
	}
	sum := 0
	for _, v := range totals {
		sum += v
	}
	if sum != n {
		t.Fatalf("expected %d total keys across nodes, got %d (distribution=%v)", n, sum, totals)
	}
}

// TestThreeNode_HashTagLocality writes 100 keys under {alice}/* via N2
// + asserts every key lands on the SAME owner node (the one the tag
// "alice" hashes to). This is the load-bearing guarantee for apps that
// want to co-locate related data on one shard.
func TestThreeNode_HashTagLocality(t *testing.T) {
	f := newThreeNodeFixture(t)

	const tag = "alice"
	const n = 100

	expectedOwner := ownerOf(f.N2.Cluster, "{"+tag+"}/anything")

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("{%s}/item/%03d", tag, i))
		if err := f.N2.Cluster.Put(key, []byte("v")); err != nil {
			t.Fatalf("Put %s via N2: %v", key, err)
		}
	}

	// Count via each backend directly. All n keys should be on the
	// expected owner; zero on the others.
	for _, node := range f.Nodes() {
		it, err := node.Backend.ScanPrefix([]byte("{" + tag + "}/"))
		if err != nil {
			t.Fatalf("backend scan on %s: %v", node.ID, err)
		}
		count := 0
		for {
			k, _, err := it.Next()
			if err != nil {
				t.Fatalf("backend scan next on %s: %v", node.ID, err)
			}
			if k == nil {
				break
			}
			count++
		}
		_ = it.Close()

		want := 0
		if node.ID == expectedOwner {
			want = n
		}
		if count != want {
			t.Fatalf("hash-tag locality broken: node %s holds %d {%s}/ keys, want %d (expected owner=%s)",
				node.ID, count, tag, want, expectedOwner)
		}
	}
}

// TestThreeNode_DeletePropagates writes via N1, deletes via N2, then
// reads via N3 + expects backend.ErrNotFound. All three sides agree on
// the same owner; the value of the test is that the delete actually
// crossed the gRPC boundary instead of silently mutating a different
// node's backend.
func TestThreeNode_DeletePropagates(t *testing.T) {
	f := newThreeNodeFixture(t)

	// Pick a key whose owner is NOT N2, so the Delete via N2 has to
	// forward across the wire to exercise the routed-delete path.
	key := findKeyNotOwnedBy(t, f.N1.Cluster, "n2", 1000)

	if err := f.N1.Cluster.Put([]byte(key), []byte("v")); err != nil {
		t.Fatalf("N1 Put: %v", err)
	}
	if _, err := f.N3.Cluster.Get([]byte(key)); err != nil {
		t.Fatalf("N3 Get before delete: %v", err)
	}
	if err := f.N2.Cluster.Delete([]byte(key)); err != nil {
		t.Fatalf("N2 Delete: %v", err)
	}
	if _, err := f.N3.Cluster.Get([]byte(key)); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("N3 Get after delete: want ErrNotFound, got err=%v", err)
	}
}

// TestThreeNode_ScanPrefixSingleShard writes a batch of keys under a
// single hash tag (so they all land on one shard) + scans via a
// non-owning node. The remote ScanPrefix must stream every key back.
func TestThreeNode_ScanPrefixSingleShard(t *testing.T) {
	f := newThreeNodeFixture(t)

	const tag = "bob"
	const n = 25
	prefix := []byte("{" + tag + "}/")

	// Pick a node that does NOT own this prefix so the ScanPrefix
	// goes over gRPC.
	prefixOwner := ownerOf(f.N1.Cluster, string(prefix))
	var scanner *cluster.Cluster
	for _, c := range f.Clusters() {
		if c.NodeID() != prefixOwner {
			scanner = c
			break
		}
	}
	if scanner == nil {
		t.Fatalf("could not find a non-owning node for prefix owner %s", prefixOwner)
	}

	// Write through whichever node we like; routing handles placement.
	expected := make(map[string]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("{%s}/x/%03d", tag, i)
		v := fmt.Sprintf("v-%03d", i)
		expected[k] = v
		if err := f.N1.Cluster.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	it, err := scanner.ScanPrefix(prefix)
	if err != nil {
		t.Fatalf("ScanPrefix via %s: %v", scanner.NodeID(), err)
	}
	defer it.Close()

	got := make(map[string]string)
	for {
		k, v, err := it.Next()
		if err != nil {
			t.Fatalf("scan Next: %v", err)
		}
		if k == nil {
			break
		}
		got[string(k)] = string(v)
	}

	if len(got) != len(expected) {
		t.Fatalf("ScanPrefix returned %d keys, want %d", len(got), len(expected))
	}
	for k, want := range expected {
		if g, ok := got[k]; !ok {
			t.Fatalf("ScanPrefix missing key %s", k)
		} else if g != want {
			t.Fatalf("ScanPrefix key %s: got %q want %q", k, g, want)
		}
	}
}

// TestThreeNode_LeaveSurvives closes N3 + verifies the remaining
// cluster (N1 + N2) still serves reads + writes for keys it owns.
// v0.2 ships R=1 with no rebalance, so keys that USED to be owned by
// N3 are lost; the contract we assert is "the surviving cluster does
// not crash + keeps working for the remaining ownership". This is the
// behavior the spec's "Failure handling" section calls out for R=1.
func TestThreeNode_LeaveSurvives(t *testing.T) {
	f := newThreeNodeFixture(t)

	// Close N3 cleanly. Graceful Leave broadcasts to peers; the
	// remaining nodes should converge to a 2-member ring.
	f.N3.Close()

	survivors := []*cluster.Cluster{f.N1.Cluster, f.N2.Cluster}
	if err := waitForMembersAll(survivors, 2, 10*time.Second); err != nil {
		t.Fatalf("post-leave convergence: %v", err)
	}

	// Pick a key whose new owner is one of the survivors (which any
	// key must be, post-leave). Write via N1, read via N2 - covers
	// both local-only + remote-forwarded paths within the surviving
	// cluster.
	const attempts = 500
	wrote := 0
	for i := 0; i < attempts; i++ {
		k := fmt.Sprintf("survivor-%04d", i)
		owner := ownerOf(f.N1.Cluster, k)
		if owner != "n1" && owner != "n2" {
			// Defense in depth: with N3 gone, no key should resolve
			// to it. If we ever see this, the ring failed to evict.
			t.Fatalf("post-leave routing resolved key %s to %s", k, owner)
		}
		if err := putWithRetry(f.N1.Cluster, []byte(k), []byte("v")); err != nil {
			t.Fatalf("Put %s via N1 post-leave: %v", k, err)
		}
		got, err := f.N2.Cluster.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %s via N2 post-leave: %v", k, err)
		}
		if !bytes.Equal(got, []byte("v")) {
			t.Fatalf("Get %s via N2 post-leave: got %q want v", k, got)
		}
		wrote++
	}
	if wrote == 0 {
		t.Fatalf("expected to write at least one survivor key, wrote 0")
	}
}

// findKeyNotOwnedBy returns a synthetic key whose ring owner (as c
// sees it) is NOT the named node. Useful for tests that want to force
// the remote-forwarding path on a specific node.
func findKeyNotOwnedBy(t *testing.T, c *cluster.Cluster, avoid string, max int) string {
	t.Helper()
	for i := 0; i < max; i++ {
		k := fmt.Sprintf("probe-%d", i)
		if ownerOf(c, k) != avoid {
			return k
		}
	}
	t.Fatalf("could not find a key NOT owned by %s in %d probes", avoid, max)
	return ""
}
