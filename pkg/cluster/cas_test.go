package cluster_test

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
)

// newSingleNode returns a single-node cluster over a fresh memory
// backend, registered for cleanup.
func newSingleNode(t *testing.T) *cluster.Cluster {
	t.Helper()
	c, err := cluster.Open(cluster.Config{NodeID: "n1", Backend: memory.New()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestCAS_CommitAppliesWriteSet: a buffered tx that reads an absent key,
// buffers a Put, and Commits applies the write-set on Commit (not before).
func TestCAS_CommitAppliesWriteSet(t *testing.T) {
	c := newSingleNode(t)

	tx, err := c.Begin(backend.SnapshotIsolation)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Get([]byte("k")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Get absent: want ErrNotFound, got %v", err)
	}
	if err := tx.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// The write must NOT be visible before Commit (buffering, not live).
	if _, err := c.Get([]byte("k")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("pre-commit cluster Get: want ErrNotFound, got %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got, err := c.Get([]byte("k"))
	if err != nil {
		t.Fatalf("post-commit Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("post-commit Get: want v1, got %q", got)
	}
}

// TestCAS_ConflictOnChangedValue: the read-set captured value X; a
// concurrent writer changes it to Y before Commit; Commit returns
// ErrCASConflict and the write-set is NOT applied.
func TestCAS_ConflictOnChangedValue(t *testing.T) {
	c := newSingleNode(t)
	if err := c.Put([]byte("k"), []byte("X")); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	tx, _ := c.Begin(backend.SnapshotIsolation)
	got, err := tx.Get([]byte("k"))
	if err != nil || !bytes.Equal(got, []byte("X")) {
		t.Fatalf("tx Get: want X, got %q (err %v)", got, err)
	}
	if err := tx.Put([]byte("k"), []byte("Z")); err != nil {
		t.Fatalf("tx Put: %v", err)
	}

	// Concurrent change between the read and the commit.
	if err := c.Put([]byte("k"), []byte("Y")); err != nil {
		t.Fatalf("concurrent Put: %v", err)
	}

	if err := tx.Commit(); !errors.Is(err, backend.ErrCASConflict) {
		t.Fatalf("Commit: want ErrCASConflict, got %v", err)
	}
	// Write-set NOT applied; the concurrent Y stands.
	got, _ = c.Get([]byte("k"))
	if !bytes.Equal(got, []byte("Y")) {
		t.Fatalf("after conflict: want Y (unchanged), got %q", got)
	}
}

// TestCAS_ConflictExpectAbsentButPresent: the read-set recorded the key
// absent; a concurrent writer creates it; Commit conflicts.
func TestCAS_ConflictExpectAbsentButPresent(t *testing.T) {
	c := newSingleNode(t)

	tx, _ := c.Begin(backend.SnapshotIsolation)
	if _, err := tx.Get([]byte("k")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("tx Get absent: %v", err)
	}
	if err := tx.Put([]byte("k"), []byte("mine")); err != nil {
		t.Fatalf("tx Put: %v", err)
	}
	// Someone else creates the key first.
	if err := c.Put([]byte("k"), []byte("theirs")); err != nil {
		t.Fatalf("concurrent create: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, backend.ErrCASConflict) {
		t.Fatalf("Commit: want ErrCASConflict, got %v", err)
	}
}

// TestCAS_ConflictOnDeletedValue: the read-set captured value X; a
// concurrent Delete removes the key; Commit conflicts (observed value is
// now absent).
func TestCAS_ConflictOnDeletedValue(t *testing.T) {
	c := newSingleNode(t)
	if err := c.Put([]byte("k"), []byte("X")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tx, _ := c.Begin(backend.SnapshotIsolation)
	if _, err := tx.Get([]byte("k")); err != nil {
		t.Fatalf("tx Get: %v", err)
	}
	if err := tx.Put([]byte("k"), []byte("Z")); err != nil {
		t.Fatalf("tx Put: %v", err)
	}
	if err := c.Delete([]byte("k")); err != nil {
		t.Fatalf("concurrent Delete: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, backend.ErrCASConflict) {
		t.Fatalf("Commit: want ErrCASConflict, got %v", err)
	}
}

// TestCAS_ReadYourWrites: a Get after a Put in the same tx returns the
// buffered value WITHOUT a round-trip, and does NOT add a read-check (so
// a later concurrent change to that key would not cause a spurious
// conflict, since the tx itself wrote it).
func TestCAS_ReadYourWrites(t *testing.T) {
	c := newSingleNode(t)

	tx, _ := c.Begin(backend.SnapshotIsolation)
	if err := tx.Put([]byte("k"), []byte("buffered")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := tx.Get([]byte("k"))
	if err != nil {
		t.Fatalf("read-your-write Get: %v", err)
	}
	if !bytes.Equal(got, []byte("buffered")) {
		t.Fatalf("read-your-write: want buffered, got %q", got)
	}
	// Delete buffered, then Get sees absence.
	if err := tx.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := tx.Get([]byte("k")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("read-your-delete Get: want ErrNotFound, got %v", err)
	}
	// Commit (a delete of a never-existed key) succeeds; the key the tx
	// only wrote did not become a read-check, so no conflict can arise.
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// TestCAS_CrossShardGuard_SingleNode: in single-node mode every key is
// local + same-owner, so the guard never fires. This pins that a
// multi-key single-shard tx commits cleanly (the guard is owner-equality,
// and single-node is one owner).
func TestCAS_CrossShardGuard_SingleNode(t *testing.T) {
	c := newSingleNode(t)
	tx, _ := c.Begin(backend.SnapshotIsolation)
	if err := tx.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := tx.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	for k, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := c.Get([]byte(k))
		if err != nil || string(got) != want {
			t.Fatalf("Get %s: want %s, got %q (err %v)", k, want, got, err)
		}
	}
}

// TestCAS_ScanPrefixUnsupported: ScanPrefix inside a CAS tx returns an
// error directing the caller to scan outside the transaction.
func TestCAS_ScanPrefixUnsupported(t *testing.T) {
	c := newSingleNode(t)
	tx, _ := c.Begin(backend.SnapshotIsolation)
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ScanPrefix([]byte("p")); err == nil {
		t.Fatalf("ScanPrefix inside CAS tx: want error, got nil")
	}
}

// TestTransact_RetriesToConvergence: a counter incremented under
// contention by N goroutines via Transact must converge to N (each
// increment is a read-modify-write that retries on conflict).
func TestTransact_RetriesToConvergence(t *testing.T) {
	c := newSingleNode(t)
	if err := c.Put([]byte("counter"), []byte("0")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			err := c.Transact([]byte("counter"), func(tx backend.Transaction) error {
				cur, err := tx.Get([]byte("counter"))
				if err != nil {
					return err
				}
				var n int
				_, _ = fmt.Sscanf(string(cur), "%d", &n)
				return tx.Put([]byte("counter"), []byte(fmt.Sprintf("%d", n+1)))
			})
			if err != nil {
				t.Errorf("Transact: %v", err)
			}
		}()
	}
	wg.Wait()

	got, _ := c.Get([]byte("counter"))
	if string(got) != fmt.Sprintf("%d", workers) {
		t.Fatalf("counter: want %d, got %q", workers, got)
	}
}

// TestTransact_NonConflictErrorAborts: a non-conflict error returned by
// fn aborts immediately and is NOT retried (we count invocations).
func TestTransact_NonConflictErrorAborts(t *testing.T) {
	c := newSingleNode(t)
	sentinel := errors.New("boom")
	calls := 0
	err := c.Transact([]byte("k"), func(_ backend.Transaction) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("fn should run exactly once on a hard error, ran %d", calls)
	}
}

// TestTransact_ExhaustsBudget: a fn that ALWAYS conflicts (a foreign
// writer changes the key on every attempt) exhausts the retry budget and
// returns ErrCASConflict. We force the conflict by mutating the key from
// inside fn AFTER reading (a side effect, used here only to manufacture a
// guaranteed conflict for the test).
func TestTransact_ExhaustsBudget(t *testing.T) {
	c := newSingleNode(t)
	if err := c.Put([]byte("k"), []byte("0")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Shrink the budget + zero the backoff so the test is fast.
	oldMax, oldBackoff := cluster.CASMaxAttempts, cluster.SetCASBaseBackoffZero()
	cluster.CASMaxAttempts = 3
	defer func() { cluster.CASMaxAttempts = oldMax; cluster.RestoreCASBaseBackoff(oldBackoff) }()

	attempts := 0
	err := c.Transact([]byte("k"), func(tx backend.Transaction) error {
		attempts++
		if _, err := tx.Get([]byte("k")); err != nil {
			return err
		}
		// Foreign change AFTER the read guarantees the commit conflicts.
		_ = c.Put([]byte("k"), []byte(fmt.Sprintf("%d", attempts)))
		return tx.Put([]byte("k"), []byte("mine"))
	})
	if !errors.Is(err, backend.ErrCASConflict) {
		t.Fatalf("want ErrCASConflict after exhausting budget, got %v", err)
	}
	if attempts != cluster.CASMaxAttempts {
		t.Fatalf("want %d attempts, got %d", cluster.CASMaxAttempts, attempts)
	}
}
