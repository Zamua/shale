package cluster

// White-box tests for the v0.8 founder-restart generation derivation
// (deriveGenerationFromDurableState + liveGenFromPresentUnits). They pin: the
// pure derivation picks the highest COMPLETE power-of-two generation (robust to
// retired lower-generation units left in the backing and to a partial aborted
// higher generation), and a founder restarting against a populated shared
// backing recovers the live {gen, count} instead of re-init'ing at the desired
// count. Fresh-cluster + non-enumerable-factory fall through to the default.

import (
	"sync"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// genUnits is a tiny builder for a present-units slice.
func genUnits(specs ...[2]int) []storageunit.GenUnit {
	out := make([]storageunit.GenUnit, 0, len(specs))
	for _, s := range specs {
		out = append(out, storageunit.NewGenUnit(storageunit.Generation(s[0]), storageunit.UnitID(s[1])))
	}
	return out
}

func TestLiveGenFromPresentUnits(t *testing.T) {
	cases := []struct {
		name      string
		present   []storageunit.GenUnit
		wantGen   storageunit.Generation
		wantCount uint32
		wantOK    bool
	}{
		{
			name:      "fresh single generation count 2",
			present:   genUnits([2]int{0, 0}, [2]int{0, 1}),
			wantGen:   0,
			wantCount: 2,
			wantOK:    true,
		},
		{
			name:      "completed reshard: old gen-0 units retired but still present, live is gen-1 count-4",
			present:   genUnits([2]int{0, 0}, [2]int{0, 1}, [2]int{1, 0}, [2]int{1, 1}, [2]int{1, 2}, [2]int{1, 3}),
			wantGen:   1,
			wantCount: 4,
			wantOK:    true,
		},
		{
			// A doubling from count-2 bisects unit K into children {K, K+2}. An
			// abort after only unit 0 bisected leaves gen-1 children {0, 2} (NOT
			// contiguous 0..N-1 for any power-of-two N), so gen-1 is incomplete and
			// the derivation falls back to the complete gen-0/count-2.
			name:      "aborted reshard: gen-1 partial children {0,2} falls back to complete gen-0 count-2",
			present:   genUnits([2]int{0, 0}, [2]int{0, 1}, [2]int{1, 0}, [2]int{1, 2}),
			wantGen:   0,
			wantCount: 2,
			wantOK:    true,
		},
		{
			name:      "count 1 single unit",
			present:   genUnits([2]int{0, 0}),
			wantGen:   0,
			wantCount: 1,
			wantOK:    true,
		},
		{
			name:    "no complete generation: gap in the only generation",
			present: genUnits([2]int{0, 0}, [2]int{0, 2}, [2]int{0, 3}), // size 3, not power of two
			wantOK:  false,
		},
		{
			name:    "no complete generation: size is power of two but a low id is missing",
			present: genUnits([2]int{0, 1}, [2]int{0, 2}), // size 2 but ids {1,2}, missing 0
			wantOK:  false,
		},
		{
			name:      "two completed generations both complete: pick the higher",
			present:   genUnits([2]int{0, 0}, [2]int{0, 1}, [2]int{1, 0}, [2]int{1, 1}, [2]int{1, 2}, [2]int{1, 3}, [2]int{2, 0}, [2]int{2, 1}, [2]int{2, 2}, [2]int{2, 3}, [2]int{2, 4}, [2]int{2, 5}, [2]int{2, 6}, [2]int{2, 7}),
			wantGen:   2,
			wantCount: 8,
			wantOK:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gen, count, ok := liveGenFromPresentUnits(tc.present)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if gen != tc.wantGen {
				t.Fatalf("gen = %d, want %d", gen, tc.wantGen)
			}
			if count.N() != tc.wantCount {
				t.Fatalf("count = %d, want %d", count.N(), tc.wantCount)
			}
		})
	}
}

// founderCluster builds a minimal multi-backend founder (no seeds) wired to a
// shared-backing handle at the given DESIRED count, ready to run
// deriveGenerationFromDurableState. genOwner is the local-node-owns-all default
// (nil ring) so any later mount derivation stays local.
func founderCluster(self string, desired int, factory storageunit.BackendFactory) *Cluster {
	c := &Cluster{
		cfg:        Config{NodeID: self}, // no Seeds: founder
		multi:      true,
		factory:    factory,
		unitCount:  storageunit.MustUnitCount(desired),
		mountMap:   make(map[storageunit.GenUnit]backend.Backend),
		pauseUnits: make(map[storageunit.UnitID]*sync.RWMutex),
		closeCh:    make(chan struct{}),
	}
	c.initGenState() // seeds gen-0 / desired, the default a fresh founder keeps
	return c
}

// seedBackingAtGen opens (and writes a key to) every unit 0..N-1 at gen g on the
// backing, so PresentUnits reports a complete generation-g unit set. Returns the
// handle used (so the caller can close it before a fresh founder takes over).
func seedBackingAtGen(t *testing.T, backing *sharedfactory.Backing, g storageunit.Generation, n int) {
	t.Helper()
	h := backing.Handle()
	for i := 0; i < n; i++ {
		gu := storageunit.NewGenUnit(g, storageunit.UnitID(i))
		be, err := h.OpenUnit(gu, 1)
		if err != nil {
			t.Fatalf("seed open %s: %v", gu, err)
		}
		if err := be.Put([]byte("k"), []byte("v")); err != nil {
			t.Fatalf("seed put %s: %v", gu, err)
		}
		if err := h.CloseUnit(gu); err != nil {
			t.Fatalf("seed close %s: %v", gu, err)
		}
	}
}

// TestFounderDerivesLiveGenFromDurableState: a founder started with DESIRED=4
// against a backing already at gen-1/count-4 (a completed reshard, with the
// retired gen-0 units still in the backing) recovers gen-1/count-4 instead of
// keeping the gen-0/desired default. (Here desired happens to equal the live
// count, which is the steady-state restart; the orphan-prevention property is
// covered by the next test where desired != live count.)
func TestFounderDerivesLiveGenFromDurableState(t *testing.T) {
	backing := sharedfactory.NewBacking()
	// A prior cluster life: gen-0/count-2 then a reshard to gen-1/count-4. Both
	// unit sets are durable (CloseUnit does not delete bytes).
	seedBackingAtGen(t, backing, 0, 2)
	seedBackingAtGen(t, backing, 1, 4)

	c := founderCluster("founder", 4, backing.Handle())
	if err := c.deriveGenerationFromDurableState(); err != nil {
		t.Fatalf("derive: %v", err)
	}
	gs := c.genSnapshot()
	if gs.gen != 1 {
		t.Fatalf("derived gen = %d, want 1", gs.gen)
	}
	if gs.count.N() != 4 {
		t.Fatalf("derived count = %d, want 4", gs.count.N())
	}
}

// TestFounderDoesNotOrphanWhenDesiredAboveLive is the core correctness test: a
// founder restarting with DESIRED=4 a cluster whose durable state is still at
// gen-0/count-2 must NOT re-init at the desired (4) - that would orphan the
// count-2 data. The reconcile loop (a later phase) drives toward the desired;
// the founder boots at the LIVE count-2.
func TestFounderDoesNotOrphanWhenDesiredAboveLive(t *testing.T) {
	backing := sharedfactory.NewBacking()
	seedBackingAtGen(t, backing, 0, 2) // live state is gen-0/count-2

	c := founderCluster("founder", 4, backing.Handle()) // desired bumped to 4
	if err := c.deriveGenerationFromDurableState(); err != nil {
		t.Fatalf("derive: %v", err)
	}
	gs := c.genSnapshot()
	if gs.gen != 0 || gs.count.N() != 2 {
		t.Fatalf("founder booted at gen=%d count=%d, want gen=0 count=2 (the live state, not the desired 4)",
			gs.gen, gs.count.N())
	}
	// The configured unitCount (desired) is unchanged: the reconcile loop reads
	// it later as the target.
	if c.unitCount.N() != 4 {
		t.Fatalf("unitCount (desired) = %d, want 4", c.unitCount.N())
	}
}

// TestFreshFounderEstablishesAtDesired: a founder against an EMPTY backing keeps
// the gen-0/desired default (the only place the desired seeds the live count).
func TestFreshFounderEstablishesAtDesired(t *testing.T) {
	backing := sharedfactory.NewBacking() // empty: no units durable
	c := founderCluster("founder", 4, backing.Handle())
	if err := c.deriveGenerationFromDurableState(); err != nil {
		t.Fatalf("derive: %v", err)
	}
	gs := c.genSnapshot()
	if gs.gen != 0 || gs.count.N() != 4 {
		t.Fatalf("fresh founder at gen=%d count=%d, want gen=0 count=4 (the desired)", gs.gen, gs.count.N())
	}
}

// nonEnumerableFactory is a BackendFactory that does NOT implement
// presentUnitsProvider, to pin the fall-through-to-default branch.
type nonEnumerableFactory struct{}

func (nonEnumerableFactory) OpenUnit(storageunit.GenUnit, storageunit.Epoch) (backend.Backend, error) {
	return nil, nil
}
func (nonEnumerableFactory) CloseUnit(storageunit.GenUnit) error { return nil }
func (nonEnumerableFactory) CurrentEpoch(storageunit.GenUnit) (storageunit.Epoch, bool) {
	return 0, false
}
func (nonEnumerableFactory) OpenUnits() []storageunit.GenUnit { return nil }

// TestNonEnumerableFactoryKeepsDefault: a factory without PresentUnits keeps the
// gen-0/desired default rather than failing (it cannot have founded a resharded
// cluster, so there is no durable state to recover).
func TestNonEnumerableFactoryKeepsDefault(t *testing.T) {
	c := founderCluster("founder", 4, nonEnumerableFactory{})
	if err := c.deriveGenerationFromDurableState(); err != nil {
		t.Fatalf("derive: %v", err)
	}
	gs := c.genSnapshot()
	if gs.gen != 0 || gs.count.N() != 4 {
		t.Fatalf("non-enumerable founder at gen=%d count=%d, want gen=0 count=4", gs.gen, gs.count.N())
	}
}
