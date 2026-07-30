package integration

// Multi-node reshard regressions for properties that predate the arbiter-driven
// R=1 reshard and must SURVIVE it (they were first pinned against the retired
// freeze barrier; re-pointed at the delegated flow when the barrier was
// deleted):
//
//	1. NO RESURRECTION. A key DELETED before (or between) reshards must stay
//	   deleted at gen g+1 - never resurrected from stale child bytes. The R=1
//	   drive's pause-held CLEAR-before-copy (bisectUnitOnlineR1) is what makes
//	   each child an exact image of its drained parent, deletes included.
//	2. TRANSACT ACROSS A RESHARD. A Transact issued while a multi-node reshard
//	   is in flight must ride out the retryable cut-over / acquiring windows
//	   (never a hard failure, never a lost commit): every nil return is a real,
//	   durable commit that survives to gen g+1.

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deleteWithRetryUnavailable issues a Delete, retrying the retryable
// acquiring-window error (codes.Unavailable) until success or timeout - the
// Delete analogue of putWithRetryUnavailable.
func deleteWithRetryUnavailable(t *testing.T, c *cluster.Cluster, key string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := c.Delete([]byte(key))
		if err == nil {
			return nil
		}
		if st, _ := status.FromError(err); st.Code() != codes.Unavailable || time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// remoteKeyFor returns a key whose unit (at unitCount) is owned by a node OTHER
// than n on the ring built from n's Members() - so a Put / CAS pin of it on n
// FORWARDS to a peer, exercising the remote (RPC) commit path.
func remoteKeyFor(t *testing.T, n *sharedNode, unitCount int) string {
	t.Helper()
	for i := range 100000 {
		k := fmt.Sprintf("rk-%d", i)
		if unitOwnerOnRing(n.Cluster, k, unitCount) != n.ID {
			return k
		}
	}
	t.Fatalf("no remote key found for node %s", n.ID)
	return ""
}

// TestMultiNodeReshard_DeletedKeyStaysDeletedAcrossReshard pins the
// NO-RESURRECTION property on the delegated multi-node reshard. On a 2-node
// shared-backing + shared-store cluster: seed a dataset, DELETE a victim key at
// gen g, then run a delegated Reshard(). The deleted key must stay DELETED at
// gen g+1 (the pause-held clear+copy rebuilds each child as an exact image of
// its drained parent, deletes included), and every surviving key must be intact
// - the reshard is otherwise lossless.
func TestMultiNodeReshard_DeletedKeyStaysDeletedAcrossReshard(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	store := storageunit.NewMemConditionalStore()
	n1 := startR1StoreNode(t, "p1b-1", "", unitCount, backing, store)
	n2 := startR1StoreNode(t, "p1b-2", n1.BindAddr, unitCount, backing, store)
	nodes := []*sharedNode{n1, n2}
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 20*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// Seed a dataset spanning every unit.
	want := make(map[string][]byte)
	const nKeys = 200
	for i := range nKeys {
		k := fmt.Sprintf("res-%05d", i)
		v := fmt.Appendf(nil, "rv-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 10*time.Second); err != nil {
			t.Fatalf("seed Put %q: %v", k, err)
		}
		want[k] = v
	}

	// DELETE the victim at gen g and confirm the delete took.
	victim := "res-00000"
	if err := deleteWithRetryUnavailable(t, n1.Cluster, victim, 8*time.Second); err != nil {
		t.Fatalf("delete victim %q: %v", victim, err)
	}
	delete(want, victim)
	if got, err := getWithRetryUnavailable(t, n1.Cluster, victim, 5*time.Second); err == nil && got != nil {
		t.Fatalf("victim %q still present at gen 0 after delete (= %q); cannot test resurrection", victim, got)
	}

	// The delegated reshard: N -> 2N via the arbiter.
	stopPump := startReconcilePump(clusters)
	defer stopPump()
	if err := n1.Cluster.Reshard(); err != nil {
		t.Fatalf("Reshard: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// === ASSERTION: the deleted key stays DELETED at gen g+1. ===
	for _, n := range nodes {
		got, err := getWithRetryUnavailable(t, n.Cluster, victim, 8*time.Second)
		if err == nil && got != nil {
			t.Fatalf("NO-RESURRECTION VIOLATED: deleted key %q readable at gen g+1 via %s (= %q)", victim, n.ID, got)
		}
	}
	// Also confirm it is physically absent from its gen-1 child store.
	newUC := storageunit.MustUnitCount(2 * unitCount)
	victimU1 := storageunit.UnitForShardKey(ring.ShardKey([]byte(victim)), newUC)
	if found, ok := sharedStoreHas(backing, storageunit.NewGenUnit(1, victimU1), victim, []byte("rv-00000")); ok && found {
		t.Fatalf("NO-RESURRECTION VIOLATED: deleted key %q present in its gen-1 child store", victim)
	}

	// === ASSERTION: every SURVIVING key is intact at gen g+1 (the reshard was
	// otherwise lossless). ===
	for k, v := range want {
		for _, n := range nodes {
			got, err := getWithRetryUnavailable(t, n.Cluster, k, 10*time.Second)
			if err != nil {
				t.Fatalf("surviving key %q unreadable via %s after reshard: %v", k, n.ID, err)
			}
			if !bytes.Equal(got, v) {
				t.Fatalf("surviving key %q via %s = %q, want %q", k, n.ID, got, v)
			}
		}
	}
}

// TestMultiNodeReshard_TransactDuringReshardRetriesThenCommits pins the
// Transact-across-a-reshard property on the delegated flow: transactions -
// LOCAL-pinned AND REMOTE-pinned - issued continuously WHILE a multi-node
// reshard is in flight must ride out the retryable cut-over / acquiring /
// re-pin windows internally (Transact's commitRetryable loop) and COMMIT; a
// nil return is a durable commit whose value survives to gen g+1. Each
// transaction is a single-op pinned write (fn pins via tx.Put, no reads), so
// the only transient signals it can hit are the commit-path windows under
// test; sequential per-counter writes make "the last committed value is the
// readable one" an exact oracle.
func TestMultiNodeReshard_TransactDuringReshardRetriesThenCommits(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	store := storageunit.NewMemConditionalStore()
	n1 := startR1StoreNode(t, "p2a-1", "", unitCount, backing, store)
	n2 := startR1StoreNode(t, "p2a-2", n1.BindAddr, unitCount, backing, store)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 20*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// Seed so every unit has data to bisect.
	for i := range 120 {
		k := fmt.Sprintf("tx-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, fmt.Sprintf("sv-%05d", i), 10*time.Second); err != nil {
			t.Fatalf("seed Put %q: %v", k, err)
		}
	}

	// A LOCAL-pinned counter key (owned by n1) and a REMOTE-pinned counter key
	// (owned by n2, so the commit forwards). Both run through Transact on n1.
	localCounter := localKeyFor(t, n1, unitCount)
	remoteCounter := remoteKeyFor(t, n1, unitCount)

	// Each writer loops sequential Transacts on its counter until the reshard
	// completes, recording the LAST value it got a nil (committed) return for.
	reshardDone := make(chan struct{})
	type txTally struct {
		lastVal string
		commits int
		err     error
	}
	runTxLoop := func(counter string, out *txTally) {
		i := 0
		for {
			select {
			case <-reshardDone:
				return
			default:
			}
			val := fmt.Sprintf("txv-%06d", i)
			err := n1.Cluster.Transact([]byte(counter), func(tx backend.Transaction) error {
				return tx.Put([]byte(counter), []byte(val))
			})
			if err != nil {
				out.err = fmt.Errorf("Transact %q mid-reshard: %w", counter, err)
				return
			}
			out.lastVal = val
			out.commits++
			i++
		}
	}
	var localTally, remoteTally txTally
	var txWg sync.WaitGroup
	txWg.Add(2)
	go func() { defer txWg.Done(); runTxLoop(localCounter, &localTally) }()
	go func() { defer txWg.Done(); runTxLoop(remoteCounter, &remoteTally) }()

	// Let both loops land at least one pre-reshard commit, then run the
	// delegated reshard UNDER the transactions.
	time.Sleep(150 * time.Millisecond)
	stopPump := startReconcilePump(clusters)
	defer stopPump()
	if err := n1.Cluster.Reshard(); err != nil {
		close(reshardDone)
		txWg.Wait()
		t.Fatalf("Reshard under Transacts: %v", err)
	}
	// Keep transacting briefly past the local convergence (the peer may still
	// be finalizing), then stop.
	time.Sleep(300 * time.Millisecond)
	close(reshardDone)
	txWg.Wait()
	time.Sleep(900 * time.Millisecond)

	// === ASSERTION: no Transact failed hard across the reshard. ===
	for _, tally := range []*txTally{&localTally, &remoteTally} {
		if tally.err != nil {
			t.Fatalf("TRANSACT VIOLATED: a Transact spanning the reshard failed instead of retrying-then-committing: %v", tally.err)
		}
		if tally.commits == 0 {
			t.Fatal("TRANSACT VACUOUS: no transaction committed; the loop did not run")
		}
	}

	// === ASSERTION: each counter's LAST committed value is the readable one at
	// gen g+1, from both nodes (sequential same-writer commits: the last nil
	// return must be the surviving value - nothing lost, nothing stale). ===
	for counter, tally := range map[string]*txTally{localCounter: &localTally, remoteCounter: &remoteTally} {
		for _, c := range clusters {
			got, err := getWithRetryUnavailable(t, c, counter, 10*time.Second)
			if err != nil {
				t.Fatalf("TRANSACT VIOLATED: committed counter %q unreadable at gen g+1: %v", counter, err)
			}
			if !bytes.Equal(got, []byte(tally.lastVal)) {
				t.Fatalf("TRANSACT VIOLATED: counter %q = %q, want the last committed %q (%d commits)", counter, got, tally.lastVal, tally.commits)
			}
		}
	}
	t.Logf("transact-across-reshard: local=%d commits, remote=%d commits, last values intact at gen 1", localTally.commits, remoteTally.commits)
}
