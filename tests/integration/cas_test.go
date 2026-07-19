package integration

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
)

// twoNodeFixture is a 2-node in-process cluster (n1 seeds n2) that has
// fully settled (ring convergence + rebalance quiescence). The CAS
// integration tests drive transactions from n1 against keys the ring
// places on n2, so the CommitCAS RPC + remote read path actually fire,
// and peer at each node's *memory.Memory backend to assert physical
// placement of the committed write-set.
type twoNodeFixture struct {
	N1 *testNode
	N2 *testNode
}

func (f *twoNodeFixture) Clusters() []*cluster.Cluster {
	return []*cluster.Cluster{f.N1.Cluster, f.N2.Cluster}
}

func newTwoNodeFixture(t *testing.T) *twoNodeFixture {
	t.Helper()
	n1 := startTestNode(t, "n1", "")
	n2 := startTestNode(t, "n2", n1.BindAddr)
	f := &twoNodeFixture{N1: n1, N2: n2}
	waitForClusterReady(t, f.Clusters(), 15*time.Second)
	return f
}

// keyOwnedBy returns a synthetic key whose ring owner (as cluster c
// sees it) is wantOwner, bounded by maxProbes so a misconfigured test
// fails loudly instead of looping. Mirrors the package-level
// findKeyOwnedBy but uses tests/integration's own ownerOf helper.
func keyOwnedBy(t *testing.T, c *cluster.Cluster, wantOwner string, maxProbes int) string {
	t.Helper()
	for i := range maxProbes {
		k := fmt.Sprintf("cas-probe-%d", i)
		if ownerOf(c, k) == wantOwner {
			return k
		}
	}
	t.Fatalf("keyOwnedBy: no key owned by %s in %d probes", wantOwner, maxProbes)
	return ""
}

// ownerBackendGet reads key directly from whichever node's local memory
// backend the ring places it on, bypassing routing. This is the escape
// hatch that lets a test assert a CAS commit landed PHYSICALLY on the
// owner's storage (durable on the owner), not merely that a routed Get
// can fetch it back.
func ownerBackendGet(t *testing.T, f *twoNodeFixture, key string) ([]byte, bool) {
	t.Helper()
	owner := ownerOf(f.N1.Cluster, key)
	var node = f.N1
	if owner == "n2" {
		node = f.N2
	}
	v, err := node.physicalGet([]byte(key))
	if errors.Is(err, backend.ErrNotFound) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("ownerBackendGet %q on %s: %v", key, owner, err)
	}
	return v, true
}

// TestCAS_RemoteCommit_DurableOnOwner: a transaction begun on N1 but
// pinned to a shard the ring places on N2 commits its write-set to N2
// over the CommitCAS RPC. We assert (a) the value is durable on N2's
// physical backend, (b) it is NOT on N1's backend (no leak onto the
// originator), and (c) it is routable from N1 via a normal Get.
func TestCAS_RemoteCommit_DurableOnOwner(t *testing.T) {
	f := newTwoNodeFixture(t)
	remoteKey := keyOwnedBy(t, f.N1.Cluster, "n2", 1000)

	tx, err := f.N1.Cluster.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("Begin on N1: %v", err)
	}
	// A buffered Get of the absent key records an expect-absent
	// read-check; the Put buffers the write. Commit ships ONE CommitCAS
	// RPC to N2 (the pin owner).
	if _, err := tx.Get([]byte(remoteKey)); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("buffered Get of absent remote key: want ErrNotFound, got %v", err)
	}
	if err := tx.Put([]byte(remoteKey), []byte("remote-v")); err != nil {
		t.Fatalf("buffered Put on remote shard: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("remote Commit: want nil, got %v", err)
	}

	// (a) Durable on N2's physical backend.
	if v, ok := ownerBackendGet(t, f, remoteKey); !ok || !bytes.Equal(v, []byte("remote-v")) {
		t.Fatalf("owner backend (N2): want remote-v present, got %q ok=%v", v, ok)
	}
	// (b) Did NOT leak onto N1's physical backend.
	if v, err := f.N1.physicalGet([]byte(remoteKey)); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("originator backend (N1): want absent, got %q err=%v", v, err)
	}
	// (c) Routable from N1 via a normal Get.
	got, err := f.N1.Cluster.Get([]byte(remoteKey))
	if err != nil {
		t.Fatalf("routed Get from N1 after remote commit: %v", err)
	}
	if !bytes.Equal(got, []byte("remote-v")) {
		t.Fatalf("routed Get from N1: want remote-v, got %q", got)
	}
}

// TestCAS_ReadYourWrites_Remote: a Get after a same-key Put inside a
// remote-pinned tx returns the BUFFERED value with no round-trip and
// before any commit. We confirm the buffer wins by also checking the
// owner's backend stays absent until Commit.
func TestCAS_ReadYourWrites_Remote(t *testing.T) {
	f := newTwoNodeFixture(t)
	remoteKey := keyOwnedBy(t, f.N1.Cluster, "n2", 1000)

	tx, err := f.N1.Cluster.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put([]byte(remoteKey), []byte("buffered")); err != nil {
		t.Fatalf("buffered Put: %v", err)
	}
	got, err := tx.Get([]byte(remoteKey))
	if err != nil {
		t.Fatalf("read-your-write Get: %v", err)
	}
	if !bytes.Equal(got, []byte("buffered")) {
		t.Fatalf("read-your-write: want buffered, got %q", got)
	}
	// Nothing is on the owner yet: the write is buffered on N1, not
	// shipped, until Commit.
	if _, ok := ownerBackendGet(t, f, remoteKey); ok {
		t.Fatalf("owner backend: value visible before Commit (buffering broken)")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if v, ok := ownerBackendGet(t, f, remoteKey); !ok || !bytes.Equal(v, []byte("buffered")) {
		t.Fatalf("owner backend after Commit: want buffered, got %q ok=%v", v, ok)
	}
}

// TestCAS_ConflictDetection_Remote: open a tx on N1 that reads remote
// key K (value v0); a separate committed write changes K to v1; the
// first tx's Commit returns ErrCASConflict and its write-set is NOT
// applied (v1 stands on the owner).
func TestCAS_ConflictDetection_Remote(t *testing.T) {
	f := newTwoNodeFixture(t)
	remoteKey := keyOwnedBy(t, f.N1.Cluster, "n2", 1000)
	if err := f.N1.Cluster.Put([]byte(remoteKey), []byte("v0")); err != nil {
		t.Fatalf("seed v0: %v", err)
	}

	tx, _ := f.N1.Cluster.Begin(backend.SnapshotIsolation)
	got, err := tx.Get([]byte(remoteKey))
	if err != nil || !bytes.Equal(got, []byte("v0")) {
		t.Fatalf("tx Get: want v0, got %q (err %v)", got, err)
	}
	if err := tx.Put([]byte(remoteKey), []byte("tx-write")); err != nil {
		t.Fatalf("tx Put: %v", err)
	}

	// Concurrent committed change between the read-set capture and the
	// commit. Route it via N2 to exercise a different commit path than
	// the tx (which forwards from N1).
	if err := f.N2.Cluster.Put([]byte(remoteKey), []byte("v1")); err != nil {
		t.Fatalf("concurrent Put via N2: %v", err)
	}

	if err := tx.Commit(); !errors.Is(err, backend.ErrCASConflict) {
		t.Fatalf("Commit: want ErrCASConflict, got %v", err)
	}
	// Write-set NOT applied; the concurrent v1 stands on the owner.
	if v, ok := ownerBackendGet(t, f, remoteKey); !ok || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("after conflict: owner backend want v1 unchanged, got %q ok=%v", v, ok)
	}
}

// TestCAS_ExpectAbsentConflict_Remote: a tx reads remote key K as
// absent and buffers a write to it; someone else creates K first; the
// tx's Commit returns ErrCASConflict (the expect-absent read-check
// fails).
func TestCAS_ExpectAbsentConflict_Remote(t *testing.T) {
	f := newTwoNodeFixture(t)
	remoteKey := keyOwnedBy(t, f.N1.Cluster, "n2", 1000)

	tx, _ := f.N1.Cluster.Begin(backend.SnapshotIsolation)
	if _, err := tx.Get([]byte(remoteKey)); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("tx Get absent: want ErrNotFound, got %v", err)
	}
	if err := tx.Put([]byte(remoteKey), []byte("mine")); err != nil {
		t.Fatalf("tx Put: %v", err)
	}
	// Someone else creates the key first.
	if err := f.N1.Cluster.Put([]byte(remoteKey), []byte("theirs")); err != nil {
		t.Fatalf("concurrent create: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, backend.ErrCASConflict) {
		t.Fatalf("Commit: want ErrCASConflict, got %v", err)
	}
	if v, ok := ownerBackendGet(t, f, remoteKey); !ok || !bytes.Equal(v, []byte("theirs")) {
		t.Fatalf("after expect-absent conflict: want theirs, got %q ok=%v", v, ok)
	}
}

// TestCAS_ConflictFreeCommit_Remote: a tx whose read-set still matches
// at commit time (no intervening change) commits and applies ALL of its
// writes atomically. Uses multiple keys on the same shard to exercise
// the atomic multi-write apply.
func TestCAS_ConflictFreeCommit_Remote(t *testing.T) {
	f := newTwoNodeFixture(t)
	// Two distinct keys that both shard to N2 (co-shard, so no
	// cross-shard guard fires).
	var k1, k2 string
	for i := 0; i < 5000 && (k1 == "" || k2 == ""); i++ {
		k := fmt.Sprintf("cas-cf-%d", i)
		if ownerOf(f.N1.Cluster, k) != "n2" {
			continue
		}
		if k1 == "" {
			k1 = k
		} else if k != k1 {
			k2 = k
		}
	}
	if k1 == "" || k2 == "" {
		t.Fatalf("could not find two distinct N2-owned keys")
	}
	if err := f.N1.Cluster.Put([]byte(k1), []byte("a0")); err != nil {
		t.Fatalf("seed k1: %v", err)
	}

	tx, _ := f.N1.Cluster.Begin(backend.SnapshotIsolation)
	if got, err := tx.Get([]byte(k1)); err != nil || !bytes.Equal(got, []byte("a0")) {
		t.Fatalf("tx Get k1: want a0, got %q (err %v)", got, err)
	}
	if err := tx.Put([]byte(k1), []byte("a1")); err != nil {
		t.Fatalf("tx Put k1: %v", err)
	}
	if err := tx.Put([]byte(k2), []byte("b1")); err != nil {
		t.Fatalf("tx Put k2: %v", err)
	}
	// No intervening change: the read-set still matches.
	if err := tx.Commit(); err != nil {
		t.Fatalf("conflict-free Commit: want nil, got %v", err)
	}
	// Both writes applied atomically, durable on the owner.
	if v, ok := ownerBackendGet(t, f, k1); !ok || !bytes.Equal(v, []byte("a1")) {
		t.Fatalf("k1 after commit: want a1, got %q ok=%v", v, ok)
	}
	if v, ok := ownerBackendGet(t, f, k2); !ok || !bytes.Equal(v, []byte("b1")) {
		t.Fatalf("k2 after commit: want b1, got %q ok=%v", v, ok)
	}
}

// TestCAS_TransactRetry_NoLostUpdate is the headline correctness test.
// Two concurrent Transact closures, one driven from N1 (forwards
// CommitCAS to the N2 owner) and one from N2 (in-process fast-path on
// the owner), each increment the SAME counter. The loser of any commit
// race observes ErrCASConflict, re-runs its closure (fresh read +
// recompute), and eventually succeeds. The final counter must reflect
// BOTH increments per pair: no lost update across the local + remote
// commit paths.
func TestCAS_TransactRetry_NoLostUpdate(t *testing.T) {
	f := newTwoNodeFixture(t)

	// Raise the retry budget: heavy single-key contention across the
	// local + remote commit paths can need more than the default 10
	// attempts before a goroutine wins its commit race.
	oldMax := cluster.CASMaxAttempts
	cluster.CASMaxAttempts = 500
	defer func() { cluster.CASMaxAttempts = oldMax }()

	counter := keyOwnedBy(t, f.N1.Cluster, "n2", 1000)
	if err := f.N1.Cluster.Put([]byte(counter), []byte("0")); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	const perNode = 25
	inc := func(c *cluster.Cluster, wg *sync.WaitGroup) {
		defer wg.Done()
		err := c.Transact([]byte(counter), func(tx backend.Transaction) error {
			cur, err := tx.Get([]byte(counter))
			if err != nil {
				return err
			}
			var n int
			_, _ = fmt.Sscanf(string(cur), "%d", &n)
			return tx.Put([]byte(counter), []byte(strconv.Itoa(n+1)))
		})
		if err != nil {
			t.Errorf("Transact: %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2 * perNode)
	for range perNode {
		go inc(f.N1.Cluster, &wg) // remote: forwards CommitCAS to N2
		go inc(f.N2.Cluster, &wg) // local fast-path on the N2 owner
	}
	wg.Wait()

	want := strconv.Itoa(2 * perNode)
	got, err := f.N1.Cluster.Get([]byte(counter))
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	if string(got) != want {
		t.Fatalf("counter: want %s (no lost update), got %q", want, got)
	}
	// Confirm durability on the physical owner too, not just the routed
	// read.
	if v, ok := ownerBackendGet(t, f, counter); !ok || string(v) != want {
		t.Fatalf("counter on owner backend: want %s, got %q ok=%v", want, v, ok)
	}
}

// TestCAS_CrossShardRejected_Remote: a tx that touches two keys the ring
// places on DIFFERENT shards returns ErrCrossShard at the offending op,
// before any wire round-trip. Pin on an N2 key, then touch an N1 key.
func TestCAS_CrossShardRejected_Remote(t *testing.T) {
	f := newTwoNodeFixture(t)
	n2Key := keyOwnedBy(t, f.N1.Cluster, "n2", 1000)
	n1Key := keyOwnedBy(t, f.N1.Cluster, "n1", 1000)

	tx, _ := f.N1.Cluster.Begin(backend.SnapshotIsolation)
	defer func() { _ = tx.Rollback() }()
	// First op pins the shard to N2.
	if err := tx.Put([]byte(n2Key), []byte("a")); err != nil {
		t.Fatalf("first Put (pins N2): %v", err)
	}
	// A second key on a different shard trips the cross-shard guard, on
	// both the write and the read path.
	if err := tx.Put([]byte(n1Key), []byte("b")); !errors.Is(err, backend.ErrCrossShard) {
		t.Fatalf("cross-shard Put: want ErrCrossShard, got %v", err)
	}
	if _, err := tx.Get([]byte(n1Key)); !errors.Is(err, backend.ErrCrossShard) {
		t.Fatalf("cross-shard Get: want ErrCrossShard, got %v", err)
	}
}

// TestCAS_LocalPath_NoRegression: a tx begun on N1 and pinned to a shard
// N1 itself owns commits via the in-process fast-path (no RPC) and lands
// on N1's physical backend. Guards against a regression where the
// local/remote dispatch picks the wrong branch.
func TestCAS_LocalPath_NoRegression(t *testing.T) {
	f := newTwoNodeFixture(t)
	localKey := keyOwnedBy(t, f.N1.Cluster, "n1", 1000)

	tx, err := f.N1.Cluster.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Get([]byte(localKey)); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("buffered Get of absent local key: want ErrNotFound, got %v", err)
	}
	if err := tx.Put([]byte(localKey), []byte("local-v")); err != nil {
		t.Fatalf("buffered Put on local shard: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("local Commit: %v", err)
	}
	// Durable on N1's physical backend (the local fast-path target), NOT
	// on N2's.
	if v, err := f.N1.physicalGet([]byte(localKey)); err != nil || !bytes.Equal(v, []byte("local-v")) {
		t.Fatalf("N1 backend after local commit: want local-v, got %q err=%v", v, err)
	}
	if _, err := f.N2.physicalGet([]byte(localKey)); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("N2 backend after local commit: want absent, got err=%v", err)
	}
}
