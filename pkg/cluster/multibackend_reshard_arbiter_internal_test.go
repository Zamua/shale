package cluster

// White-box tests for the decentralized reshard Arbiter wiring (v0.9 Phase C1).
// They drive initReshardArbiter directly against the in-process
// MemConditionalStore double, no membership / gRPC, so the construct + seed
// seam is pinned in isolation. The full split protocol the Arbiter drives is
// covered by the lossless-split oracle (tests/integration) in later phases.

import (
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/reshard"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestInitReshardArbiter_SeedsOnR2 pins that an R>1 multi-backend cluster with a
// ConditionalStore configured constructs + seeds the Arbiter at the configured
// base count (epoch 0, count == target == N, no plan).
func TestInitReshardArbiter_SeedsOnR2(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	c.cfg.ConditionalStore = storageunit.NewMemConditionalStore()

	if err := c.initReshardArbiter(); err != nil {
		t.Fatalf("initReshardArbiter: %v", err)
	}
	if c.arbiter == nil {
		t.Fatalf("R>1 cluster with a ConditionalStore should construct an arbiter")
	}
	st, _, err := c.arbiter.Read()
	if err != nil {
		t.Fatalf("read seeded state: %v", err)
	}
	if st.Epoch != 0 || st.Count.N() != 8 || st.Target.N() != 8 || st.Plan != reshard.PlanNone {
		t.Fatalf("seeded state = %+v, want epoch0 count8 target8 none", st)
	}
}

// TestInitReshardArbiter_NilStoreNoArbiter pins that without a ConditionalStore
// the Arbiter is left unconstructed, so the cluster stays byte-for-byte on the
// existing static / coordinated-freeze reshard paths.
func TestInitReshardArbiter_NilStoreNoArbiter(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	if err := c.initReshardArbiter(); err != nil {
		t.Fatalf("initReshardArbiter: %v", err)
	}
	if c.arbiter != nil {
		t.Fatalf("no ConditionalStore should leave arbiter nil")
	}
}

// TestInitReshardArbiter_R1NoArbiter pins that even WITH a ConditionalStore an
// R=1 multi-backend cluster does NOT construct an Arbiter: the decentralized
// online reshard is R>1 only and R=1 keeps the coordinated freeze barrier.
func TestInitReshardArbiter_R1NoArbiter(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 1, backing, "n1", "n2")
	c.cfg.ConditionalStore = storageunit.NewMemConditionalStore()
	if err := c.initReshardArbiter(); err != nil {
		t.Fatalf("initReshardArbiter: %v", err)
	}
	if c.arbiter != nil {
		t.Fatalf("R=1 cluster should not construct an arbiter (R>1-only feature)")
	}
}

// TestInitReshardArbiter_JoinerAdoptsExisting pins the idempotent-seed contract:
// a second node seeding the SAME store adopts the already-seeded State rather
// than overwriting it, even when configured at a different base count. This is
// what keeps the single durable epoch object the sole cluster authority.
func TestInitReshardArbiter_JoinerAdoptsExisting(t *testing.T) {
	backing := sharedfactory.NewBacking()
	store := storageunit.NewMemConditionalStore()

	founder := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	founder.cfg.ConditionalStore = store
	if err := founder.initReshardArbiter(); err != nil {
		t.Fatalf("founder seed: %v", err)
	}

	// A joiner configured at a DIFFERENT base count (16) adopts the founder's 8.
	joiner := newReplicatedCluster(t, "n2", 16, 2, backing, "n1", "n2", "n3")
	joiner.cfg.ConditionalStore = store
	if err := joiner.initReshardArbiter(); err != nil {
		t.Fatalf("joiner seed: %v", err)
	}
	st, _, err := joiner.arbiter.Read()
	if err != nil {
		t.Fatalf("joiner read: %v", err)
	}
	if st.Count.N() != 8 || st.Target.N() != 8 {
		t.Fatalf("joiner adopted %+v, want the founder's seeded count8 target8", st)
	}
}

// gs builds a genState for the controller table tests. cutOver lists the old
// unit ids already flipped.
func gs(gen storageunit.Generation, count, nextCount int, cutOver ...storageunit.UnitID) genState {
	g := genState{
		gen:     gen,
		count:   storageunit.MustUnitCount(count),
		cutOver: make(map[storageunit.UnitID]struct{}),
	}
	if nextCount != 0 {
		g.nextCount = storageunit.MustUnitCount(nextCount)
	}
	for _, u := range cutOver {
		g.cutOver[u] = struct{}{}
	}
	return g
}

// st builds a reshard.State for the controller table tests.
func stt(epoch uint64, count, target int, plan reshard.Plan) reshard.State {
	return reshard.State{
		Epoch:  epoch,
		Count:  storageunit.MustUnitCount(count),
		Target: storageunit.MustUnitCount(target),
		Plan:   plan,
	}
}

func TestReshardGenStep(t *testing.T) {
	cases := []struct {
		name        string
		local       genState
		S           reshard.State
		wantChanged bool
		wantGen     storageunit.Generation
		wantCount   int
		wantNext    int // 0 == not in flight
	}{
		{
			name:        "steady, agreed count higher: enter split one step",
			local:       gs(0, 2, 0),
			S:           stt(1, 4, 4, reshard.PlanSplit),
			wantChanged: true, wantGen: 0, wantCount: 2, wantNext: 4,
		},
		{
			name:        "non-contiguous: agreed count two steps ahead, still steps ONE gen",
			local:       gs(0, 2, 0),
			S:           stt(2, 8, 8, reshard.PlanSplit),
			wantChanged: true, wantGen: 0, wantCount: 2, wantNext: 4,
		},
		{
			name:        "mid-split, not all cut over: no change",
			local:       gs(0, 2, 4, 0), // unit 0 flipped, unit 1 not
			S:           stt(1, 4, 4, reshard.PlanSplit),
			wantChanged: false, wantGen: 0, wantCount: 2, wantNext: 4,
		},
		{
			name:        "all cut over: finalize to next generation",
			local:       gs(0, 2, 4, 0, 1), // both old units flipped
			S:           stt(1, 4, 4, reshard.PlanSplit),
			wantChanged: true, wantGen: 1, wantCount: 4, wantNext: 0,
		},
		{
			name:        "steady at agreed count: no change",
			local:       gs(1, 4, 0),
			S:           stt(1, 4, 4, reshard.PlanNone),
			wantChanged: false, wantGen: 1, wantCount: 4, wantNext: 0,
		},
		{
			name:        "steady, agreed count lower: enter merge one step",
			local:       gs(2, 8, 0),
			S:           stt(3, 4, 2, reshard.PlanMerge),
			wantChanged: true, wantGen: 2, wantCount: 8, wantNext: 4,
		},
		{
			name:        "non-contiguous merge: agreed two steps lower, still steps ONE gen",
			local:       gs(3, 8, 0),
			S:           stt(5, 2, 2, reshard.PlanMerge),
			wantChanged: true, wantGen: 3, wantCount: 8, wantNext: 4,
		},
		{
			name:        "merge all cut over: finalize to the halved generation",
			local:       gs(2, 8, 4, 0, 1, 2, 3, 4, 5, 6, 7), // all 8 old units flipped
			S:           stt(3, 4, 4, reshard.PlanMerge),
			wantChanged: true, wantGen: 3, wantCount: 4, wantNext: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next, changed := reshardGenStep(tc.local, tc.S)
			if changed != tc.wantChanged {
				t.Fatalf("changed = %v, want %v", changed, tc.wantChanged)
			}
			if next.gen != tc.wantGen {
				t.Fatalf("gen = %d, want %d", next.gen, tc.wantGen)
			}
			if int(next.count.N()) != tc.wantCount {
				t.Fatalf("count = %d, want %d", next.count.N(), tc.wantCount)
			}
			gotNext := 0
			if !next.nextCount.IsZero() {
				gotNext = int(next.nextCount.N())
			}
			if gotNext != tc.wantNext {
				t.Fatalf("nextCount = %d, want %d", gotNext, tc.wantNext)
			}
		})
	}
}

func TestAllCutOver(t *testing.T) {
	if allCutOver(gs(0, 4, 0)) {
		t.Fatalf("not in flight should not be allCutOver")
	}
	if allCutOver(gs(0, 4, 8, 0, 1)) {
		t.Fatalf("2 of 4 cut over should not be allCutOver")
	}
	if !allCutOver(gs(0, 4, 8, 0, 1, 2, 3)) {
		t.Fatalf("4 of 4 cut over should be allCutOver")
	}
}

func TestShouldAdvanceArbiter(t *testing.T) {
	if !shouldAdvanceArbiter(gs(1, 4, 0), stt(1, 4, 8, reshard.PlanNone)) {
		t.Fatalf("steady at applied count with a farther target should advance")
	}
	if shouldAdvanceArbiter(gs(1, 4, 0), stt(1, 4, 4, reshard.PlanNone)) {
		t.Fatalf("steady at target should NOT advance")
	}
	if shouldAdvanceArbiter(gs(0, 2, 4), stt(1, 4, 8, reshard.PlanSplit)) {
		t.Fatalf("mid-split should NOT advance (non-contiguous safeguard)")
	}
	if shouldAdvanceArbiter(gs(0, 2, 0), stt(1, 4, 8, reshard.PlanSplit)) {
		t.Fatalf("a node BEHIND the agreed count should NOT advance")
	}
}

// enterSplit puts the cluster into an in-flight split to 2x the current count
// (sets genState.nextCount), the state the desired-set / router machinery keys
// off. White-box helper: it commits a cloned genState the way the controller
// would once the per-unit machinery is wired.
func enterSplit(t *testing.T, c *Cluster) {
	t.Helper()
	gs := c.genSnapshot().clone()
	nc, err := gs.count.Double()
	if err != nil {
		t.Fatalf("double: %v", err)
	}
	gs.nextCount = nc
	c.commitGenState(gs)
}

// enterMerge puts the cluster into an in-flight merge to half the current count
// (sets genState.nextCount = count.Halve()).
func enterMerge(t *testing.T, c *Cluster) {
	t.Helper()
	gs := c.genSnapshot().clone()
	nc, err := gs.count.Halve()
	if err != nil {
		t.Fatalf("halve: %v", err)
	}
	gs.nextCount = nc
	c.commitGenState(gs)
}

// TestDesiredReplicaUnits_MergeSurvivors pins that during a merge the desired set
// adds this node's gen-(g+1) SURVIVOR units at their own gen-(g+1) ring home (no
// co-location - the two-source merge copy is cross-node), alongside the gen-g
// parents.
func TestDesiredReplicaUnits_MergeSurvivors(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")

	for _, ru := range c.desiredReplicaUnits() {
		if ru.Unit.Gen != 0 {
			t.Fatalf("steady state should desire only gen-0 units, got %v", ru)
		}
	}

	enterMerge(t, c) // 8 -> 4
	survivors := map[storageunit.UnitID]bool{}
	for _, ru := range c.desiredReplicaUnits() {
		switch ru.Unit.Gen {
		case 0: // parent, fine
		case 1:
			survivors[ru.Unit.ID] = true
		default:
			t.Fatalf("unexpected generation %d in desired set: %v", ru.Unit.Gen, ru)
		}
	}
	if len(survivors) == 0 {
		t.Fatalf("in-flight merge should desire gen-1 survivors, got none")
	}
	// Every desired survivor must be a gen-1 unit (count 4) this node owns on the
	// gen-1 ring - its final home, not a co-located slot.
	for s := range survivors {
		if int(s) >= 4 {
			t.Fatalf("survivor %d out of range for the halved count 4", s)
		}
		owns := false
		for _, m := range c.unitReplicas(storageunit.NewGenUnit(1, s)) {
			if m.ID == "n1" {
				owns = true
			}
		}
		if !owns {
			t.Fatalf("survivor %d desired but n1 is not in its gen-1 ring replica set", s)
		}
	}
}

// assertSplitChildrenColocated checks that desired is a steady gen-g set PLUS,
// during a split, the gen-(g+1) children co-located at their parent's slot:
// every gen-(g+1) child maps (ParentUnit) to a desired gen-g unit at the SAME
// replica slot, and each held parent contributes BOTH of its children.
func assertSplitChildrenColocated(t *testing.T, desired []storageunit.ReplicaUnit, newCount int) {
	t.Helper()
	parents := map[storageunit.ReplicaUnit]bool{}
	var children []storageunit.ReplicaUnit
	for _, ru := range desired {
		switch ru.Unit.Gen {
		case 0:
			parents[ru] = true
		case 1:
			children = append(children, ru)
		default:
			t.Fatalf("unexpected generation %d in desired set: %v", ru.Unit.Gen, ru)
		}
	}
	if len(children) == 0 {
		t.Fatalf("in-flight split should desire gen-1 children, got none (parents=%d)", len(parents))
	}
	for _, ch := range children {
		parent, err := storageunit.ParentUnit(ch.Unit.ID, storageunit.MustUnitCount(newCount))
		if err != nil {
			t.Fatalf("ParentUnit(%d): %v", ch.Unit.ID, err)
		}
		want := storageunit.NewReplicaUnit(storageunit.NewGenUnit(0, parent), ch.Replica)
		if !parents[want] {
			t.Fatalf("child %v not co-located: want a desired parent %v at slot %d", ch, want, ch.Replica)
		}
	}
	if len(children) != 2*len(parents) {
		t.Fatalf("got %d children for %d parents, want both children of each (=%d)", len(children), len(parents), 2*len(parents))
	}
}

// TestDesiredReplicaUnits_SplitChildren pins that the current-view desired set is
// the steady gen-g units in steady state, and adds the co-located gen-(g+1)
// children when a split is in flight.
func TestDesiredReplicaUnits_SplitChildren(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")

	for _, ru := range c.desiredReplicaUnits() {
		if ru.Unit.Gen != 0 {
			t.Fatalf("steady state should desire only gen-0 units, got %v", ru)
		}
	}

	enterSplit(t, c)
	assertSplitChildrenColocated(t, c.desiredReplicaUnits(), 8)
}

// TestDesiredPendingReplicaUnits_SplitChildren pins that the PENDING-view desired
// set stays in lockstep: it also surfaces the co-located children during a split
// (computed under the draining-excluded ring), so the overlap reconcile never
// sees a child as a spurious transition.
func TestDesiredPendingReplicaUnits_SplitChildren(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")
	enterSplit(t, c)

	// A draining member exercises the pending (draining-excluded) resolver.
	pending := c.desiredPendingReplicaUnits(map[storageunit.NodeID]struct{}{"n3": {}})
	assertSplitChildrenColocated(t, pending, 8)
}

// TestReshardController_MarchesSplit_2to8 drives the real Arbiter together with
// the pure controller functions through a full 2 -> 8 split, simulating the
// per-unit cut-over the (not-yet-built) unit machinery performs. It proves the
// state machine converges: each generation's split is entered, completed, and
// finalized in order before the arbiter advances to the next.
func TestReshardController_MarchesSplit_2to8(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	a := reshard.NewArbiter(store)
	if _, err := a.Seed(storageunit.MustUnitCount(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Retarget(storageunit.MustUnitCount(8)); err != nil {
		t.Fatal(err)
	}

	g := gs(0, 2, 0)
	var splits int
	for range 200 {
		S, _, err := a.Read()
		if err != nil {
			t.Fatal(err)
		}
		// Converged?
		if g.count.N() == 8 && g.nextCount.IsZero() && S.Count.N() == 8 {
			if splits != 2 {
				t.Fatalf("2 -> 8 took %d split generations, want 2 (2->4->8)", splits)
			}
			return
		}
		// 1) Advance the agreed epoch when settled + behind target.
		if shouldAdvanceArbiter(g, S) {
			if _, _, err := a.Advance(); err != nil {
				t.Fatal(err)
			}
			continue
		}
		// 2) Reflect the agreed State into genState (enter in-flight / finalize).
		if next, changed := reshardGenStep(g, S); changed {
			if next.gen == g.gen+1 {
				splits++
			}
			g = next
			continue
		}
		// 3) Simulate the per-unit machinery completing every cut-over.
		if !g.nextCount.IsZero() && !allCutOver(g) {
			for _, u := range g.count.IDs() {
				g.cutOver[u] = struct{}{}
			}
			continue
		}
		t.Fatalf("stuck: g=%+v S=%+v", g, S)
	}
	t.Fatalf("did not converge: g=%+v", g)
}

// TestReshardController_MarchesMerge_8to2 is the merge-direction analogue: the
// controller + real Arbiter converge 8 -> 2 in two halving generations
// (8 -> 4 -> 2), one generation entered, completed, and finalized before the
// next.
func TestReshardController_MarchesMerge_8to2(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	a := reshard.NewArbiter(store)
	if _, err := a.Seed(storageunit.MustUnitCount(8)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Retarget(storageunit.MustUnitCount(2)); err != nil {
		t.Fatal(err)
	}

	g := gs(0, 8, 0)
	var merges int
	for range 200 {
		s, _, err := a.Read()
		if err != nil {
			t.Fatal(err)
		}
		if g.count.N() == 2 && g.nextCount.IsZero() && s.Count.N() == 2 {
			if merges != 2 {
				t.Fatalf("8 -> 2 took %d merge generations, want 2 (8->4->2)", merges)
			}
			return
		}
		if shouldAdvanceArbiter(g, s) {
			if _, _, err := a.Advance(); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if next, changed := reshardGenStep(g, s); changed {
			if next.gen == g.gen+1 {
				merges++
			}
			g = next
			continue
		}
		if !g.nextCount.IsZero() && !allCutOver(g) {
			for _, u := range g.count.IDs() {
				g.cutOver[u] = struct{}{}
			}
			continue
		}
		t.Fatalf("stuck: g=%+v s=%+v", g, s)
	}
	t.Fatalf("did not converge: g=%+v", g)
}
