package storageunit_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

// MemConditionalStore must satisfy the ConditionalStore seam.
var _ storageunit.ConditionalStore = (*storageunit.MemConditionalStore)(nil)

func TestMemConditionalStore_PutIfAbsent(t *testing.T) {
	s := storageunit.NewMemConditionalStore()

	v1, err := s.PutIfAbsent("k", []byte("a"))
	if err != nil {
		t.Fatalf("first PutIfAbsent: %v", err)
	}
	if v1 == "" {
		t.Fatal("PutIfAbsent should return a non-empty version token")
	}

	// A second create on the same key must lose the precondition.
	if _, err := s.PutIfAbsent("k", []byte("b")); !errors.Is(err, storageunit.ErrPrecondition) {
		t.Fatalf("second PutIfAbsent err = %v, want ErrPrecondition", err)
	}

	// The value and version are unchanged by the failed write.
	got, ver, err := s.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "a" || ver != v1 {
		t.Fatalf("Get = %q@%s, want a@%s", got, ver, v1)
	}
}

func TestMemConditionalStore_CompareAndSet(t *testing.T) {
	s := storageunit.NewMemConditionalStore()

	// CAS on an absent key fails (nothing to match).
	if _, err := s.CompareAndSet("k", []byte("x"), "v1"); !errors.Is(err, storageunit.ErrPrecondition) {
		t.Fatalf("CAS on absent key = %v, want ErrPrecondition", err)
	}

	v1, _ := s.PutIfAbsent("k", []byte("a"))

	// CAS with a stale/wrong expected version fails.
	if _, err := s.CompareAndSet("k", []byte("b"), "bogus"); !errors.Is(err, storageunit.ErrPrecondition) {
		t.Fatalf("CAS with wrong version = %v, want ErrPrecondition", err)
	}

	// CAS with the current version succeeds and advances the version.
	v2, err := s.CompareAndSet("k", []byte("b"), v1)
	if err != nil {
		t.Fatalf("CAS with current version: %v", err)
	}
	if v2 == v1 {
		t.Fatal("CompareAndSet must advance the version on success")
	}
	got, _, _ := s.Get("k")
	if string(got) != "b" {
		t.Fatalf("after CAS Get = %q, want b", got)
	}

	// The now-superseded version no longer matches (no ABA reuse).
	if _, err := s.CompareAndSet("k", []byte("c"), v1); !errors.Is(err, storageunit.ErrPrecondition) {
		t.Fatalf("CAS with superseded version = %v, want ErrPrecondition", err)
	}
}

func TestMemConditionalStore_GetAbsent(t *testing.T) {
	s := storageunit.NewMemConditionalStore()
	if _, _, err := s.Get("missing"); !errors.Is(err, storageunit.ErrCondNotFound) {
		t.Fatalf("Get(absent) = %v, want ErrCondNotFound", err)
	}
}

// TestMemConditionalStore_RaceExactlyOneWinner pins the property the whole
// decentralized arbiter rests on: when many nodes race to create the same
// object (e.g. advance the reshard epoch), EXACTLY ONE wins and the rest get
// ErrPrecondition. This is what lets determinism + a CAS race replace an
// elected coordinator.
func TestMemConditionalStore_RaceExactlyOneWinner(t *testing.T) {
	s := storageunit.NewMemConditionalStore()
	const n = 64
	var wins int64
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for i := range n {
		done.Go(func() {
			start.Wait() // release all goroutines at once to maximize contention
			if _, err := s.PutIfAbsent("__reshard/epoch", []byte{byte(i)}); err == nil {
				atomic.AddInt64(&wins, 1)
			} else if !errors.Is(err, storageunit.ErrPrecondition) {
				t.Errorf("loser got %v, want ErrPrecondition", err)
			}
		})
	}
	start.Done()
	done.Wait()

	if wins != 1 {
		t.Fatalf("PutIfAbsent race: %d winners, want exactly 1", wins)
	}
}

// TestMemConditionalStore_CASRaceSerializes pins the CompareAndSet analogue:
// many nodes try to advance the SAME version concurrently; exactly one wins,
// because the winner mints a new version that invalidates the others' expected
// token. This is the "advance the epoch from g to g+1" race.
func TestMemConditionalStore_CASRaceSerializes(t *testing.T) {
	s := storageunit.NewMemConditionalStore()
	v0, _ := s.PutIfAbsent("epoch", []byte("g0"))

	const n = 64
	var wins int64
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for range n {
		done.Go(func() {
			start.Wait()
			if _, err := s.CompareAndSet("epoch", []byte("g1"), v0); err == nil {
				atomic.AddInt64(&wins, 1)
			}
		})
	}
	start.Done()
	done.Wait()

	if wins != 1 {
		t.Fatalf("CompareAndSet race from one version: %d winners, want exactly 1", wins)
	}
}
