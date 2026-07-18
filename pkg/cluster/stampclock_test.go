package cluster

// Unit tests for the monotone per-node stamp source (stampclock.go).
// White-box (package cluster) so the injectable wall clock (nowFn) can
// simulate regressions without touching real time.

import (
	"sort"
	"sync"
	"testing"
)

// TestStampClock_WallRegressionStillMonotonic pins the core property:
// a wall clock that jumps BACKWARD (NTP step, VM migration) cannot make
// Next() repeat or regress. Every call returns a strictly greater value
// than the one before it.
func TestStampClock_WallRegressionStillMonotonic(t *testing.T) {
	walls := []int64{1000, 1001, 500, 400, 400, 2000}
	i := 0
	sc := &stampClock{nowFn: func() int64 {
		w := walls[i%len(walls)]
		i++
		return w
	}}

	prev := uint64(0)
	for call := range walls {
		got := sc.Next()
		if got <= prev {
			t.Fatalf("call %d: Next() = %d, not strictly above previous %d (wall regressed to %d)", call, got, prev, walls[call])
		}
		prev = got
	}
	// The final call saw wall=2000 > last, so it should jump forward to
	// the wall reading, not creep by +1.
	if prev != 2000 {
		t.Fatalf("final Next() = %d, want 2000 (wall ahead of last resumes wall time)", prev)
	}
}

// TestStampClock_ObserveFutureRatchets pins the ratchet: after
// Observe(T) with T far ahead of the wall clock, Next() returns a value
// strictly above T, so a node that has SEEN a stamp never issues at or
// below it.
func TestStampClock_ObserveFutureRatchets(t *testing.T) {
	sc := &stampClock{nowFn: func() int64 { return 100 }}

	sc.Observe(5000)
	if got := sc.Next(); got < 5001 {
		t.Fatalf("Next() after Observe(5000) = %d, want >= 5001", got)
	}

	// Observing something BELOW the high-water mark is a no-op: the
	// clock never moves backward.
	sc.Observe(10)
	if got := sc.Next(); got < 5002 {
		t.Fatalf("Next() after no-op Observe(10) = %d, want >= 5002", got)
	}
}

// TestStampClock_ConcurrentNextUnique pins per-call uniqueness under
// concurrency: N goroutines each drawing M stamps yield N*M all-unique
// values, and each goroutine's own sequence is strictly increasing
// (monotonically consumable).
func TestStampClock_ConcurrentNextUnique(t *testing.T) {
	// A frozen wall clock maximizes CAS contention on the last+1 path.
	sc := &stampClock{nowFn: func() int64 { return 42 }}

	const goroutines = 8
	const perG = 2000

	results := make([][]uint64, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seq := make([]uint64, 0, perG)
			for range perG {
				seq = append(seq, sc.Next())
			}
			results[g] = seq
		}()
	}
	wg.Wait()

	all := make([]uint64, 0, goroutines*perG)
	for g, seq := range results {
		for i := 1; i < len(seq); i++ {
			if seq[i] <= seq[i-1] {
				t.Fatalf("goroutine %d: stamp %d (%d) not strictly above its predecessor (%d)", g, i, seq[i], seq[i-1])
			}
		}
		all = append(all, seq...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	for i := 1; i < len(all); i++ {
		if all[i] == all[i-1] {
			t.Fatalf("duplicate stamp issued: %d", all[i])
		}
	}
}
