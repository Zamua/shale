package reshard_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Zamua/shale/pkg/reshard"
	"github.com/Zamua/shale/pkg/storageunit"
)

func uc(n int) storageunit.UnitCount { return storageunit.MustUnitCount(n) }

// converge advances a to its stored Target and returns the settled count.
func converge(t *testing.T, a *reshard.Arbiter) int {
	t.Helper()
	for range 64 {
		_, stepped, err := a.Advance()
		if err != nil {
			t.Fatalf("advance: %v", err)
		}
		if !stepped {
			s, _, err := a.Read()
			if err != nil {
				t.Fatal(err)
			}
			return int(s.Count.N())
		}
	}
	t.Fatal("did not converge")
	return 0
}

func TestArbiter_SeedIdempotent(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	a := reshard.NewArbiter(store)

	s1, err := a.Seed(uc(2))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if s1.Epoch != 0 || s1.Count.N() != 2 || s1.Target.N() != 2 || s1.Plan != reshard.PlanNone {
		t.Fatalf("seed = %+v, want epoch0 count2 target2 none", s1)
	}

	// A second Seed (even at a different count) must adopt the existing seed.
	s2, err := a.Seed(uc(8))
	if err != nil {
		t.Fatalf("re-seed: %v", err)
	}
	if s2.Epoch != 0 || s2.Count.N() != 2 || s2.Target.N() != 2 {
		t.Fatalf("re-seed = %+v, want the original epoch0 count2 target2", s2)
	}
}

func TestArbiter_AdvanceBeforeSeed(t *testing.T) {
	a := reshard.NewArbiter(storageunit.NewMemConditionalStore())
	if _, _, err := a.Advance(); !errors.Is(err, storageunit.ErrCondNotFound) {
		t.Fatalf("Advance before Seed = %v, want ErrCondNotFound (retry-after-seed contract)", err)
	}
}

func TestArbiter_RetargetThenSplit(t *testing.T) {
	a := reshard.NewArbiter(storageunit.NewMemConditionalStore())
	if _, err := a.Seed(uc(2)); err != nil {
		t.Fatal(err)
	}
	st, err := a.Retarget(uc(8))
	if err != nil {
		t.Fatalf("retarget: %v", err)
	}
	if st.Target.N() != 8 || st.Count.N() != 2 {
		t.Fatalf("after retarget = %+v, want target8 count2 (target changes, count does not)", st)
	}

	s, stepped, err := a.Advance()
	if err != nil {
		t.Fatal(err)
	}
	if !stepped || s.Epoch != 1 || s.Count.N() != 4 || s.Plan != reshard.PlanSplit {
		t.Fatalf("advance step1 = %+v, want epoch1 count4 split", s)
	}
	if got := converge(t, a); got != 8 {
		t.Fatalf("converged to %d, want 8", got)
	}
}

func TestArbiter_RetargetThenMerge(t *testing.T) {
	a := reshard.NewArbiter(storageunit.NewMemConditionalStore())
	if _, err := a.Seed(uc(8)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Retarget(uc(2)); err != nil {
		t.Fatal(err)
	}
	s, stepped, err := a.Advance()
	if err != nil {
		t.Fatal(err)
	}
	if !stepped || s.Count.N() != 4 || s.Plan != reshard.PlanMerge {
		t.Fatalf("merge step1 = %+v, want count4 merge", s)
	}
	if got := converge(t, a); got != 2 {
		t.Fatalf("converged to %d, want 2", got)
	}
}

func TestArbiter_ReachFarTarget(t *testing.T) {
	a := reshard.NewArbiter(storageunit.NewMemConditionalStore())
	if _, err := a.Seed(uc(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Retarget(uc(16)); err != nil {
		t.Fatal(err)
	}
	steps := 0
	for {
		_, stepped, err := a.Advance()
		if err != nil {
			t.Fatal(err)
		}
		if !stepped {
			break
		}
		steps++
		if steps > 10 {
			t.Fatal("did not converge")
		}
	}
	if steps != 3 {
		t.Fatalf("2 -> 16 took %d steps, want 3", steps)
	}
}

// TestArbiter_NoFlap is the fix for the review's major finding: because the
// Target lives in the durable State, a node cannot reverse another node's
// reshard by calling Advance. Once converged to the agreed Target, Advance is a
// no-op; only an explicit Retarget changes direction.
func TestArbiter_NoFlap(t *testing.T) {
	a := reshard.NewArbiter(storageunit.NewMemConditionalStore())
	if _, err := a.Seed(uc(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Retarget(uc(8)); err != nil {
		t.Fatal(err)
	}
	if got := converge(t, a); got != 8 {
		t.Fatalf("converge = %d, want 8", got)
	}
	// Repeated Advance must NOT drift away from the agreed target.
	for range 5 {
		if _, stepped, err := a.Advance(); err != nil || stepped {
			t.Fatalf("Advance at target: stepped=%v err=%v, want a no-op (no flap)", stepped, err)
		}
	}
	if s, _, _ := a.Read(); s.Count.N() != 8 {
		t.Fatalf("count drifted to %d, want a stable 8", s.Count.N())
	}
	// Direction only changes via an explicit Retarget.
	if _, err := a.Retarget(uc(2)); err != nil {
		t.Fatal(err)
	}
	if got := converge(t, a); got != 2 {
		t.Fatalf("after retarget converge = %d, want 2", got)
	}
}

// TestArbiter_ConcurrentAdvanceStepsExactlyOnce: many nodes (each its own
// Arbiter over the SAME store) concurrently Advance the single available step.
// The CAS race lets EXACTLY ONE perform it; the rest adopt the winner.
func TestArbiter_ConcurrentAdvanceStepsExactlyOnce(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	seed := reshard.NewArbiter(store)
	if _, err := seed.Seed(uc(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Retarget(uc(4)); err != nil { // one step away
		t.Fatal(err)
	}

	const n = 64
	var stepWins int64
	var start, done sync.WaitGroup
	start.Add(1)
	for range n {
		done.Go(func() {
			a := reshard.NewArbiter(store) // each node its own arbiter
			start.Wait()
			_, stepped, err := a.Advance()
			if err != nil {
				t.Errorf("advance: %v", err)
				return
			}
			if stepped {
				atomic.AddInt64(&stepWins, 1)
			}
		})
	}
	start.Done()
	done.Wait()

	if stepWins != 1 {
		t.Fatalf("concurrent Advance of one step: %d performed it, want exactly 1", stepWins)
	}
	final, _, err := seed.Read()
	if err != nil {
		t.Fatal(err)
	}
	if final.Epoch != 1 || final.Count.N() != 4 {
		t.Fatalf("after concurrent round epoch=%d count=%d, want epoch1 count4 (advanced exactly once)", final.Epoch, final.Count.N())
	}
}
