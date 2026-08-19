// Package coordcontract is the shared contract harness for shale's
// coordination port (pkg/coord): one set of tests pinning the properties
// EVERY adapter must satisfy, run by each adapter's own suite through
// RunContract.
//
// Why a shared package: the contract tests grew up inside the removed
// gossip adapter's suite, which made them look adapter-specific when they
// are port-specific. Every adapter has exactly the same bar to clear -
// Populated tracking View, TransitionSets matching View's roles, the
// PlacementMembers basis, Locate determinism, the genuinely reduced
// placement under exclusion - and hand-copying the tests is how two suites
// drift apart (internal/clustertest exists for the same reason).
// Adapter-SPECIFIC behavior stays in each adapter's own tests: the CAS
// suite owns lease expiry and document GC; the gossip suite owned its
// ring-drop heal, reconcile idempotence and bootstrap-discovery tests.
//
// The harness reaches an adapter only through the Adapter hooks and the
// port itself, so it can never depend on one implementation's interior.
package coordcontract

import (
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/storageunit"
)

// DefaultRoleVisibilityTimeout bounds how long RunContract waits for a
// role flip to reach the query methods when Adapter.RoleVisibilityTimeout
// is zero. It matches the waits the removed gossip suite always used.
const DefaultRoleVisibilityTimeout = 2 * time.Second

// contractUnitCount is the unit universe the Locate tests sweep. 64 units
// over <= 5 members gives every placement shape (including the ones where
// exclusion genuinely moves a unit) without a slow sweep.
const contractUnitCount = 64

// Adapter is how one coordinator implementation plugs into RunContract.
// All three hooks are required.
type Adapter struct {
	// NewUnstarted returns a CONSTRUCTED but not STARTED Coordinator.
	//
	// The port's letter says Start precedes every other method, but the
	// contract holds adapters to the stricter bar the original suite
	// pinned: pre-Start, View() must be empty, Populated() false, Locate
	// nil, and Close a safe no-op - never a panic on nil internals.
	NewUnstarted func(t testing.TB) coord.Coordinator

	// New returns a STARTED Coordinator whose View ALREADY holds exactly
	// members (self included), all in steady state (no roles), and whose
	// membership stays exactly members for the life of the test: the
	// wiring picks whatever knobs (static construction, parked reconcile,
	// long lease/poll intervals) guarantee no member appears or expires
	// mid-run. members may arrive in any order; the view must come back
	// sorted regardless. The harness registers Close on t.Cleanup; the
	// wiring may register additional cleanup of its own (Close is
	// idempotent by port contract, so both closing is fine).
	New func(t testing.TB, self coord.Node, members []coord.Node) coord.Coordinator

	// SetMemberRoles makes member id advertise exactly roles (roles == 0
	// retracts them all), through whatever mechanism the adapter has: a
	// test seam on a static coordinator (coordstatic's facts override), or
	// the real SetRole driven on the peer instance that owns id (a CAS
	// cluster). Visibility may lag delivery by the adapter's own refresh
	// cadence; the harness waits up to RoleVisibilityTimeout for the flip
	// to land, then asserts exactly.
	SetMemberRoles func(t testing.TB, co coord.Coordinator, id storageunit.NodeID, roles coord.Role)

	// RoleVisibilityTimeout bounds how long a role flip may take to reach
	// the query methods; zero means DefaultRoleVisibilityTimeout. The wait
	// exists because the port lets role delivery ride the adapter's own
	// refresh mechanism (a CAS poll, a broadcast in a fork's adapter). What it
	// must NEVER ride is the change hint - which is why the wait loop
	// never reads Changed(): an adapter that refreshed roles only when
	// the caller consumed a hint would time out here.
	RoleVisibilityTimeout time.Duration
}

// RunContract runs every port-contract test against the adapter. Call it
// once from the adapter's test suite; adapter-specific tests stay in that
// suite alongside it.
func RunContract(t *testing.T, a Adapter) {
	if a.NewUnstarted == nil || a.New == nil || a.SetMemberRoles == nil {
		t.Fatal("coordcontract: Adapter requires NewUnstarted, New and SetMemberRoles")
	}
	t.Run("Populated_TracksViewEmpty", a.testPopulatedTracksViewEmpty)
	t.Run("View_SelfAndSortedMembers", a.testViewSelfAndSortedMembers)
	t.Run("TransitionSets_MatchViewRoles", a.testTransitionSetsMatchViewRoles)
	t.Run("PlacementMembers_MatchViewBasis", a.testPlacementMembersMatchViewBasis)
	t.Run("Locate_DeterministicAcrossInstances", a.testLocateDeterministicAcrossInstances)
	t.Run("Locate_PrimaryIsStableAcrossN", a.testLocatePrimaryIsStableAcrossN)
	t.Run("Locate_ClampsAndRefusesDegenerateN", a.testLocateClampsAndRefusesDegenerateN)
	t.Run("Locate_ExcludeIsGenuineReducedPlacement", a.testLocateExcludeIsGenuineReducedPlacement)
	t.Run("Locate_ExcludingEveryoneReturnsNil", a.testLocateExcludingEveryoneReturnsNil)
}

// open constructs a started coordinator through the adapter and owns its
// shutdown.
func (a Adapter) open(t *testing.T, self coord.Node, members []coord.Node) coord.Coordinator {
	t.Helper()
	co := a.New(t, self, members)
	t.Cleanup(func() { _ = co.Close() })
	return co
}

// nodes builds the harness's member sets: stable IDs, "id:0" addresses.
func nodes(ids ...string) []coord.Node {
	out := make([]coord.Node, 0, len(ids))
	for _, id := range ids {
		out = append(out, coord.Node{ID: storageunit.NodeID(id), Addr: id + ":0"})
	}
	return out
}

// testPopulatedTracksViewEmpty pins the port contract: Populated() is
// equivalent to !View().Empty() at all times - including before Start,
// where both must report an empty view (and Locate, asked over that empty
// eligible set, must return nil rather than panic).
func (a Adapter) testPopulatedTracksViewEmpty(t *testing.T) {
	un := a.NewUnstarted(t)
	t.Cleanup(func() { _ = un.Close() })
	if un.Populated() {
		t.Fatal("unstarted coordinator reports Populated; View is empty")
	}
	if !un.View().Empty() {
		t.Fatal("unstarted coordinator's View is non-empty")
	}
	if got := un.Locate(storageunit.NewGenUnit(0, 1), 1, coord.Placement{}); got != nil {
		t.Fatalf("unstarted coordinator's Locate returned %+v; an empty eligible set must return nil", got)
	}

	members := nodes("a", "b", "c")
	co := a.open(t, members[0], members)
	if !co.Populated() {
		t.Fatal("started 3-member coordinator reports unpopulated")
	}
	if co.View().Empty() {
		t.Fatal("started 3-member coordinator's View is empty")
	}
}

// testViewSelfAndSortedMembers pins the View shape the port documents:
// Self is this node's advertised identity, Members always contains it once
// started, and Members is sorted by ID whatever order the membership was
// assembled in (two nodes must log, diff and compare views the same way).
func (a Adapter) testViewSelfAndSortedMembers(t *testing.T) {
	members := nodes("c", "a", "b") // deliberately unsorted input
	co := a.open(t, members[0], members)

	v := co.View()
	if v.Self.ID != "c" {
		t.Fatalf("View.Self = %+v, want the started identity c", v.Self)
	}
	if len(v.Members) != len(members) {
		t.Fatalf("View holds %d members, want %d: %+v", len(v.Members), len(members), v.Members)
	}
	for i := 1; i < len(v.Members); i++ {
		if v.Members[i-1].ID >= v.Members[i].ID {
			t.Fatalf("View.Members is not sorted by ID: %+v", v.Members)
		}
	}
	m, ok := v.Member("c")
	if !ok || m.Addr != "c:0" {
		t.Fatalf("View must contain self with its address, got %+v ok=%v", m, ok)
	}
	if _, ok := v.Member("zzz"); ok {
		t.Fatal("View reported an absent id as found")
	}
}

// testTransitionSetsMatchViewRoles pins the port contract against View as
// the reference: the sets contain exactly the members whose view roles
// carry the corresponding bit; a role absent from every member comes back
// as a NIL map (callers rely on nil for the steady-state fast path, so an
// empty non-nil map is a failure); retraction restores the nil steady
// state; and the returned maps are the caller's to keep - mutating one
// must not bleed into the adapter's next answer.
func (a Adapter) testTransitionSetsMatchViewRoles(t *testing.T) {
	members := nodes("a", "b", "c", "d")
	co := a.open(t, members[0], members)

	j, d := co.TransitionSets()
	if j != nil || d != nil {
		t.Fatalf("steady state must return nil sets, got joining=%v draining=%v", j, d)
	}

	// Flip one member joining and one joining+draining.
	idJoin := storageunit.NodeID("b")
	idBoth := storageunit.NodeID("c")
	a.SetMemberRoles(t, co, idJoin, coord.RoleJoining)
	a.SetMemberRoles(t, co, idBoth, coord.RoleJoining|coord.RoleDraining)

	wantJ := map[storageunit.NodeID]struct{}{idJoin: {}, idBoth: {}}
	wantD := map[storageunit.NodeID]struct{}{idBoth: {}}
	j, d = a.waitTransitionSets(t, co, wantJ, wantD)

	// Cross-check against View: same members, same bits.
	viewJ := map[storageunit.NodeID]struct{}{}
	viewD := map[storageunit.NodeID]struct{}{}
	for _, m := range co.View().Members {
		if m.Joining() {
			viewJ[m.ID] = struct{}{}
		}
		if m.Draining() {
			viewD[m.ID] = struct{}{}
		}
	}
	assertSameSet(t, "joining-vs-view", j, viewJ)
	assertSameSet(t, "draining-vs-view", d, viewD)

	// The returned maps are the caller's to keep: the port forbids the
	// adapter retaining or mutating them, so a caller-side mutation must
	// not surface in a fresh query.
	delete(j, idJoin)
	delete(d, idBoth)
	j2, d2 := co.TransitionSets()
	assertSameSet(t, "joining-after-caller-mutation", j2, wantJ)
	assertSameSet(t, "draining-after-caller-mutation", d2, wantD)

	// Retraction restores the NIL steady state, not empty non-nil maps.
	a.SetMemberRoles(t, co, idJoin, 0)
	a.SetMemberRoles(t, co, idBoth, 0)
	a.waitTransitionSets(t, co, nil, nil)
}

// waitTransitionSets polls until TransitionSets reports exactly wantJ /
// wantD (nil meaning "must be nil"), failing at the visibility deadline.
// The loop never reads Changed(): role liveness must not depend on the
// caller consuming hints.
func (a Adapter) waitTransitionSets(t *testing.T, co coord.Coordinator, wantJ, wantD map[storageunit.NodeID]struct{}) (j, d map[storageunit.NodeID]struct{}) {
	t.Helper()
	timeout := a.RoleVisibilityTimeout
	if timeout == 0 {
		timeout = DefaultRoleVisibilityTimeout
	}
	deadline := time.Now().Add(timeout)
	for {
		j, d = co.TransitionSets()
		if sameSet(j, wantJ) && sameSet(d, wantD) {
			return j, d
		}
		if time.Now().After(deadline) {
			t.Fatalf("role flip never reached TransitionSets: joining=%v draining=%v, want %v / %v",
				j, d, wantJ, wantD)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// testPlacementMembersMatchViewBasis pins the steady-state agreement of
// the two bases: with a stable membership, PlacementMembers returns
// exactly View's member set, in the same (ID-sorted) order. An adapter
// whose bases can transiently diverge (as the removed gossip adapter's
// ring did from its snapshot) pins that divergence in its OWN suite; a
// single-basis adapter (CAS) never diverges at all. The contract here is
// the point both agree on.
func (a Adapter) testPlacementMembersMatchViewBasis(t *testing.T) {
	members := nodes("a", "b", "c")
	co := a.open(t, members[0], members)

	pm := co.PlacementMembers()
	vm := co.View().Members
	if len(pm) != len(vm) {
		t.Fatalf("placement holds %d members, view %d - the steady-state bases must agree", len(pm), len(vm))
	}
	for i := range pm {
		if pm[i].ID != vm[i].ID {
			t.Fatalf("placement[%d]=%s view[%d]=%s - PlacementMembers must be View's member set, sorted by ID",
				i, pm[i].ID, i, vm[i].ID)
		}
	}
}

// testLocateDeterministicAcrossInstances pins the routing-correctness
// contract directly: two coordinators holding the same view - and NOT the
// same self, since the answer must not depend on who asks - compute the
// same slice, in the same order, for every unit and every n. A forwarded
// op that disagrees with its receiver about the primary loops.
func (a Adapter) testLocateDeterministicAcrossInstances(t *testing.T) {
	members := nodes("a", "b", "c", "d", "e")
	c1 := a.open(t, members[0], members)
	c2 := a.open(t, members[1], members)

	for _, u := range storageunit.MustUnitCount(contractUnitCount).IDs() {
		gu := storageunit.NewGenUnit(0, u)
		for _, n := range []int{1, 2, 3} {
			g1 := c1.Locate(gu, n, coord.Placement{})
			g2 := c2.Locate(gu, n, coord.Placement{})
			if len(g1) != len(g2) {
				t.Fatalf("unit %d n=%d: instance sizes %d vs %d", u, n, len(g1), len(g2))
			}
			for i := range g1 {
				if g1[i].ID != g2[i].ID {
					t.Fatalf("unit %d n=%d index %d: %s vs %s - two nodes with the same view must compute the same placement in the same order",
						u, n, i, g1[i].ID, g2[i].ID)
				}
			}
		}
	}
}

// testLocatePrimaryIsStableAcrossN pins that Locate(u, 1) and
// Locate(u, n)[0] name the SAME node. Routing resolves an owner with n=1
// and a replica set with n=R against the same view; if the primary
// disagreed between them, a forwarded op and its receiver would disagree
// about who owns the unit and loop.
func (a Adapter) testLocatePrimaryIsStableAcrossN(t *testing.T) {
	members := nodes("a", "b", "c")
	co := a.open(t, members[0], members)
	for _, u := range storageunit.MustUnitCount(contractUnitCount).IDs() {
		gu := storageunit.NewGenUnit(0, u)
		one := co.Locate(gu, 1, coord.Placement{})
		many := co.Locate(gu, 3, coord.Placement{})
		if len(one) != 1 || len(many) != 3 {
			t.Fatalf("unit %d: sizes %d / %d, want 1 / 3", u, len(one), len(many))
		}
		if one[0].ID != many[0].ID {
			t.Fatalf("unit %d: Locate(1)=%s but Locate(3)[0]=%s - the primary must not depend on n",
				u, one[0].ID, many[0].ID)
		}
	}
}

// testLocateClampsAndRefusesDegenerateN pins the size contract: n larger
// than the membership returns every distinct member (never a duplicate,
// never a phantom), and n <= 0 returns nothing.
func (a Adapter) testLocateClampsAndRefusesDegenerateN(t *testing.T) {
	members := nodes("a", "b")
	co := a.open(t, members[0], members)
	gu := storageunit.NewGenUnit(0, 3)

	got := co.Locate(gu, 5, coord.Placement{})
	if len(got) != 2 {
		t.Fatalf("n beyond the membership returned %d nodes, want 2 (clamped)", len(got))
	}
	if got[0].ID == got[1].ID {
		t.Fatalf("clamped result duplicated a node: %+v", got)
	}
	if n := co.Locate(gu, 0, coord.Placement{}); n != nil {
		t.Fatalf("n=0 returned %+v, want nil", n)
	}
	if n := co.Locate(gu, -1, coord.Placement{}); n != nil {
		t.Fatalf("n<0 returned %+v, want nil", n)
	}
}

// testLocateExcludeIsGenuineReducedPlacement pins the ONE property that
// makes the current/pending routing split exact: excluding a node must
// answer "where would this unit sit if that node were not a member", NOT
// "locate over everyone and drop it".
//
// Bounded-load consistent hashing is not removal-invariant, so those two
// answers genuinely differ. The test proves it by comparing an exclusion
// against a coordinator BUILT without the excluded node: they must agree
// exactly, unit for unit and index for index. The filtered-then-dropped
// approximation does not, which is what stranded a full-move unit's
// handoff on nodes that would never own it. An assignment-table adapter
// meets the same bar by reading its coordinated post-departure placement.
func (a Adapter) testLocateExcludeIsGenuineReducedPlacement(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e"}
	all := nodes(ids...)
	survivors := make([]coord.Node, 0, len(all)-1)
	for _, n := range all {
		if n.ID != "a" {
			survivors = append(survivors, n)
		}
	}
	full := a.open(t, all[0], all)
	without := a.open(t, survivors[0], survivors)
	exclude := map[storageunit.NodeID]struct{}{"a": {}}

	for _, u := range storageunit.MustUnitCount(contractUnitCount).IDs() {
		gu := storageunit.NewGenUnit(0, u)
		got := full.Locate(gu, 2, coord.Placement{Exclude: exclude})
		want := without.Locate(gu, 2, coord.Placement{})
		if len(got) != len(want) {
			t.Fatalf("unit %d: exclusion returned %d nodes, the genuine placement %d", u, len(got), len(want))
		}
		for i := range want {
			if got[i].ID != want[i].ID {
				t.Fatalf("unit %d index %d: exclusion=%s, placement-without-a=%s "+
					"(the exclusion must reconstruct the real post-removal placement, not a filtered one)",
					u, i, got[i].ID, want[i].ID)
			}
		}
	}
}

// testLocateExcludingEveryoneReturnsNil pins the caller's fallback signal:
// when the exclusion set swallows the whole membership there is no
// hypothetical placement to answer with, and a nil result is what tells
// the storage layer to fall back to the real one (the safe wedge) rather
// than route nowhere.
func (a Adapter) testLocateExcludingEveryoneReturnsNil(t *testing.T) {
	members := nodes("a", "b")
	co := a.open(t, members[0], members)
	got := co.Locate(storageunit.NewGenUnit(0, 1), 2, coord.Placement{
		Exclude: map[storageunit.NodeID]struct{}{"a": {}, "b": {}},
	})
	if got != nil {
		t.Fatalf("excluding every member returned %+v, want nil", got)
	}
}

// sameSet reports whether got is exactly want, where a nil want DEMANDS a
// nil got: nil-for-none is the port contract (the allocation-free steady
// state), not a convention an empty map may substitute for.
func sameSet(got, want map[storageunit.NodeID]struct{}) bool {
	if want == nil {
		return got == nil
	}
	if got == nil || len(got) != len(want) {
		return false
	}
	for id := range want {
		if _, ok := got[id]; !ok {
			return false
		}
	}
	return true
}

func assertSameSet(t *testing.T, name string, got, want map[storageunit.NodeID]struct{}) {
	t.Helper()
	if !sameSet(got, want) {
		t.Fatalf("%s: got %v, want %v", name, got, want)
	}
}
