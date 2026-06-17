package reshard_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Zamua/shale/pkg/reshard"
	"github.com/Zamua/shale/pkg/storageunit"
)

func uc(n int) storageunit.UnitCount { return storageunit.MustUnitCount(n) }

func TestArbiter_SeedIdempotent(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	a := reshard.NewArbiter(store)

	s1, err := a.Seed(uc(2))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if s1.Epoch != 0 || s1.Count.N() != 2 || s1.Plan != reshard.PlanNone {
		t.Fatalf("seed = %+v, want epoch0 count2 none", s1)
	}

	// A second Seed (even at a different count) must adopt the existing seed,
	// never overwrite it.
	s2, err := a.Seed(uc(8))
	if err != nil {
		t.Fatalf("re-seed: %v", err)
	}
	if s2.Epoch != 0 || s2.Count.N() != 2 {
		t.Fatalf("re-seed = %+v, want the original epoch0 count2", s2)
	}
}

func TestArbiter_AdvanceSplit(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	a := reshard.NewArbiter(store)
	if _, err := a.Seed(uc(2)); err != nil {
		t.Fatal(err)
	}

	s, stepped, err := a.Advance(uc(8))
	if err != nil {
		t.Fatal(err)
	}
	if !stepped || s.Epoch != 1 || s.Count.N() != 4 || s.Plan != reshard.PlanSplit {
		t.Fatalf("advance 2->8 step1 = %+v stepped=%v, want epoch1 count4 split", s, stepped)
	}

	s, stepped, err = a.Advance(uc(8))
	if err != nil {
		t.Fatal(err)
	}
	if !stepped || s.Epoch != 2 || s.Count.N() != 8 || s.Plan != reshard.PlanSplit {
		t.Fatalf("advance step2 = %+v, want epoch2 count8 split", s)
	}

	// Already at desired: no-op.
	s, stepped, err = a.Advance(uc(8))
	if err != nil {
		t.Fatal(err)
	}
	if stepped || s.Count.N() != 8 {
		t.Fatalf("advance at desired = %+v stepped=%v, want no-op count8", s, stepped)
	}
}

func TestArbiter_AdvanceMerge(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	a := reshard.NewArbiter(store)
	if _, err := a.Seed(uc(8)); err != nil {
		t.Fatal(err)
	}

	s, stepped, err := a.Advance(uc(2))
	if err != nil {
		t.Fatal(err)
	}
	if !stepped || s.Epoch != 1 || s.Count.N() != 4 || s.Plan != reshard.PlanMerge {
		t.Fatalf("merge 8->2 step1 = %+v, want epoch1 count4 merge", s)
	}

	s, _, err = a.Advance(uc(2))
	if err != nil {
		t.Fatal(err)
	}
	if s.Epoch != 2 || s.Count.N() != 2 {
		t.Fatalf("merge step2 = %+v, want epoch2 count2", s)
	}
}

func TestArbiter_ReachFarTargetByRepeatedAdvance(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	a := reshard.NewArbiter(store)
	if _, err := a.Seed(uc(2)); err != nil {
		t.Fatal(err)
	}

	// 2 -> 16 must take exactly 3 split steps.
	steps := 0
	for {
		s, stepped, err := a.Advance(uc(16))
		if err != nil {
			t.Fatal(err)
		}
		if !stepped {
			if s.Count.N() != 16 {
				t.Fatalf("stopped at count %d, want 16", s.Count.N())
			}
			break
		}
		steps++
		if steps > 10 {
			t.Fatal("did not converge to 16")
		}
	}
	if steps != 3 {
		t.Fatalf("2 -> 16 took %d steps, want 3", steps)
	}
}

// TestArbiter_ConcurrentAdvanceStepsExactlyOnce is the agreement property: many
// nodes (each its own Arbiter over the SAME store) concurrently Advance the
// single available step from the same epoch. The CAS race lets EXACTLY ONE
// perform it; the rest adopt the winner. No two nodes ever fork the epoch.
func TestArbiter_ConcurrentAdvanceStepsExactlyOnce(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	if _, err := reshard.NewArbiter(store).Seed(uc(2)); err != nil {
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
			_, stepped, err := a.Advance(uc(4)) // a single step away: 2 -> 4
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
	final, _, err := reshard.NewArbiter(store).Read()
	if err != nil {
		t.Fatal(err)
	}
	if final.Epoch != 1 || final.Count.N() != 4 {
		t.Fatalf("after concurrent round epoch=%d count=%d, want epoch1 count4 (advanced exactly once)", final.Epoch, final.Count.N())
	}
}
