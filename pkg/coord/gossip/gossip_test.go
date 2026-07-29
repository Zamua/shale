package gossip_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/storageunit"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// openSolo starts a single-node gossip coordinator (no seeds), parking the ring
// reconcile so a background tick cannot heal a divergence a test induces.
func openSolo(t *testing.T, id string, reconcile time.Duration) *gossip.Coordinator {
	t.Helper()
	co := gossip.New(gossip.Config{
		BindAddr:            "127.0.0.1:" + strconv.Itoa(freePort(t)),
		LogOutput:           io.Discard,
		ReconcileInterval:   reconcile,
		RejoinInterval:      -1,
		MetaRefreshInterval: -1,
	})
	boot, err := co.Start(coord.Params{
		Self: coord.Node{ID: storageunit.NodeID(id), Addr: "127.0.0.1:1"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if boot != coord.BootstrapFounded {
		t.Fatalf("a seedless node must report BootstrapFounded, got %v", boot)
	}
	t.Cleanup(func() { _ = co.Close() })
	return co
}

// waitForMember polls the view until it holds id.
func waitForMember(co *gossip.Coordinator, id storageunit.NodeID, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := co.View().Member(id); ok {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestReconcileRing_RestoresMissingMember pins the anti-entropy guarantee: the
// periodic reconcile must re-add a member gossip knows about but the placement
// ring has lost.
//
// The event channel drops on backpressure BY DESIGN (a blocking send would
// stall memberlist's callback goroutine), so without this heal a single dropped
// join leaves the ring permanently short a member and every unit it should own
// routes somewhere else forever. The test induces exactly that divergence -
// drop the member from the ring only - and drives one reconcile pass.
//
// Why it would catch a regression: any change that turns the reconcile into a
// no-op (dropping the snapshot read, guarding it with an inverted nil check)
// leaves the member missing and the assertion below fires.
func TestReconcileRing_RestoresMissingMember(t *testing.T) {
	co := openSolo(t, "solo", time.Hour)

	if !waitForMember(co, "solo", 2*time.Second) {
		t.Fatalf("local member never landed in the view; view=%+v", co.View().Members)
	}

	co.TestingRemoveFromRing("solo")
	if got := co.View().Members; len(got) != 0 {
		t.Fatalf("post-remove view should be empty, got %+v", got)
	}

	if !co.TestingReconcileRing() {
		t.Fatal("reconcile reported no change after the member was dropped from the ring")
	}
	members := co.View().Members
	if len(members) != 1 || members[0].ID != "solo" {
		t.Fatalf("reconcile did not restore the local member; view=%+v", members)
	}
}

// TestReconcileRing_IdempotentWhenInSync pins the other half: a reconcile that
// finds nothing to do must report NO change, so the storage layer is not woken
// by a hint every tick in steady state.
func TestReconcileRing_IdempotentWhenInSync(t *testing.T) {
	co := openSolo(t, "solo", time.Hour)
	if !waitForMember(co, "solo", 2*time.Second) {
		t.Fatal("local member never landed in the view")
	}
	if co.TestingReconcileRing() {
		t.Fatal("a reconcile against an already-synced ring reported a change")
	}
}

// TestChanged_IsCoalescing pins the hint contract: the channel has depth 1 and
// a send that would block is DROPPED. A caller that treated one receive as one
// change would miss changes; the contract says re-read the view instead.
func TestChanged_IsCoalescing(t *testing.T) {
	co := gossip.NewStatic(coord.Node{ID: "self", Addr: "self:0"}, nil)
	for range 5 {
		co.TestingSetMembers([]coord.Node{{ID: "self", Addr: "self:0"}})
	}
	got := 0
	for {
		select {
		case <-co.Changed():
			got++
			continue
		default:
		}
		break
	}
	if got != 1 {
		t.Fatalf("5 changes delivered %d hints, want 1 (the hint must coalesce, not queue)", got)
	}
}

// TestLocate_ExcludeIsHypotheticalPlacement pins the ONE property that makes
// the current/pending routing split exact: excluding a node must answer "where
// would this unit sit if that node were not a member", NOT "locate over
// everyone and drop it".
//
// Bounded-load consistent hashing is not removal-invariant, so those two
// answers genuinely differ. The test proves it by comparing an exclusion
// against a coordinator BUILT without the excluded node: they must agree
// exactly, unit for unit and index for index. The filtered-then-dropped
// approximation does not, which is what stranded a full-move unit's handoff on
// nodes that would never own it.
func TestLocate_ExcludeIsHypotheticalPlacement(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e"}
	all := make([]coord.Node, 0, len(ids))
	survivors := make([]coord.Node, 0, len(ids)-1)
	for _, id := range ids {
		n := coord.Node{ID: storageunit.NodeID(id), Addr: id + ":0"}
		all = append(all, n)
		if id != "a" {
			survivors = append(survivors, n)
		}
	}
	full := gossip.NewStatic(all[0], all)
	without := gossip.NewStatic(survivors[0], survivors)
	exclude := map[storageunit.NodeID]struct{}{"a": {}}

	count := storageunit.MustUnitCount(64)
	for _, u := range count.IDs() {
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

// TestLocate_PrimaryIsStableAcrossN pins that Locate(u, 1) and Locate(u, n)[0]
// name the SAME node. Routing resolves an owner with n=1 and a replica set with
// n=R against the same view; if the primary disagreed between them, a forwarded
// op and its receiver would disagree about who owns the unit and loop.
func TestLocate_PrimaryIsStableAcrossN(t *testing.T) {
	nodes := []coord.Node{{ID: "a", Addr: "a:0"}, {ID: "b", Addr: "b:0"}, {ID: "c", Addr: "c:0"}}
	co := gossip.NewStatic(nodes[0], nodes)
	for _, u := range storageunit.MustUnitCount(64).IDs() {
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

// TestLocate_ClampsAndRefusesDegenerateN pins the size contract: n larger than
// the membership returns every distinct member (never a duplicate, never a
// phantom), and n <= 0 returns nothing.
func TestLocate_ClampsAndRefusesDegenerateN(t *testing.T) {
	nodes := []coord.Node{{ID: "a", Addr: "a:0"}, {ID: "b", Addr: "b:0"}}
	co := gossip.NewStatic(nodes[0], nodes)
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

// TestLocate_ExcludingEveryoneReturnsNothing pins the caller's fallback signal:
// when the exclusion set swallows the whole membership there is no hypothetical
// placement to answer with, and an empty result is what tells the storage layer
// to fall back to the real one (the safe wedge) rather than route nowhere.
func TestLocate_ExcludingEveryoneReturnsNothing(t *testing.T) {
	nodes := []coord.Node{{ID: "a", Addr: "a:0"}, {ID: "b", Addr: "b:0"}}
	co := gossip.NewStatic(nodes[0], nodes)
	got := co.Locate(storageunit.NewGenUnit(0, 1), 2, coord.Placement{
		Exclude: map[storageunit.NodeID]struct{}{"a": {}, "b": {}},
	})
	if got != nil {
		t.Fatalf("excluding every member returned %+v, want nil", got)
	}
}

// TestGenUnitBytes_StableEncoding pins the fixed-width big-endian ring key: 8
// bytes of Generation then 4 of UnitID. A drift silently re-routes every unit
// in the cluster, so the test nails down concrete bytes. The generation prefix
// is what makes a gen-g unit K and a gen-(g+1) unit K land on potentially
// different nodes, which is what a doubling reshard depends on.
func TestGenUnitBytes_StableEncoding(t *testing.T) {
	cases := []struct {
		gu   storageunit.GenUnit
		want []byte
	}{
		{storageunit.NewGenUnit(0, 0), []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{storageunit.NewGenUnit(0, 1), []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}},
		{storageunit.NewGenUnit(1, 1), []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1}},
		{storageunit.NewGenUnit(0, 256), []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0}},
		{storageunit.NewGenUnit(0, 0xDEADBEEF), []byte{0, 0, 0, 0, 0, 0, 0, 0, 0xDE, 0xAD, 0xBE, 0xEF}},
	}
	for _, tc := range cases {
		if got := gossip.GenUnitBytes(tc.gu); !bytes.Equal(got, tc.want) {
			t.Fatalf("GenUnitBytes(%s) = %v, want %v", tc.gu, got, tc.want)
		}
	}
	if bytes.Equal(gossip.GenUnitBytes(storageunit.NewGenUnit(0, 5)), gossip.GenUnitBytes(storageunit.NewGenUnit(1, 5))) {
		t.Fatal("gen-0 and gen-1 of unit 5 encoded identically; generations would collide on the ring")
	}
}

// TestSetRole_DedupsAndCombines pins the declarative contract: SetRole
// publishes the COMPLETE set, so setting the same set twice is a no-op and
// setting both bits leaves both advertised (a partial update must never clear
// the other role).
func TestSetRole_DedupsAndCombines(t *testing.T) {
	co := openSolo(t, "roles", time.Hour)
	if !waitForMember(co, "roles", 2*time.Second) {
		t.Fatal("local member never landed in the view")
	}

	if err := co.SetRole(coord.RoleJoining | coord.RoleDraining); err != nil {
		t.Fatalf("SetRole both: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m, ok := co.View().Member("roles"); ok && m.Joining() && m.Draining() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	m, _ := co.View().Member("roles")
	if !m.Joining() || !m.Draining() {
		t.Fatalf("both roles set, view reports joining=%v draining=%v", m.Joining(), m.Draining())
	}

	// Repeating the same set must not error and must not change anything.
	if err := co.SetRole(coord.RoleJoining | coord.RoleDraining); err != nil {
		t.Fatalf("SetRole repeat: %v", err)
	}

	// Clearing one leaves the other.
	if err := co.SetRole(coord.RoleDraining); err != nil {
		t.Fatalf("SetRole drop joining: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m, ok := co.View().Member("roles"); ok && !m.Joining() && m.Draining() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	m, _ = co.View().Member("roles")
	t.Fatalf("after dropping joining, view reports joining=%v draining=%v (want false/true)", m.Joining(), m.Draining())
}

// TestGeneration_IsUnsupported pins that this adapter holds no opinion on
// storage generations: it answers 0 and refuses a proposal with the typed
// sentinel, so the storage layer keeps driving generations through its own
// agreement instead of silently believing a coordinator that guessed.
func TestGeneration_IsUnsupported(t *testing.T) {
	co := gossip.NewStatic(coord.Node{ID: "a", Addr: "a:0"}, nil)
	if g := co.Generation(); g != 0 {
		t.Fatalf("Generation = %d, want 0 (no opinion)", g)
	}
	err := co.ProposeGeneration(7)
	if !errors.Is(err, coord.ErrGenerationUnsupported) {
		t.Fatalf("ProposeGeneration error = %v, want ErrGenerationUnsupported", err)
	}
}

// TestStart_RequiresBindAddr pins that a coordinator with nothing to bind fails
// LOUDLY rather than coming up with a view of one and silently forking.
func TestStart_RequiresBindAddr(t *testing.T) {
	co := gossip.New(gossip.Config{})
	if _, err := co.Start(coord.Params{Self: coord.Node{ID: "a", Addr: "a:1"}}); err == nil {
		t.Fatal("Start with no BindAddr returned nil; it must refuse")
	}
}

// TestStart_FounderDropsJoiningRole pins the founder guard. A node with no
// seeds forms the cluster: there is no incumbent serving the positions it is
// about to mount, so advertising RoleJoining would make it exclude itself from
// its own CURRENT set with nobody left to be current - a cluster that can never
// ack a write. The request is honored only for a node that actually joined one.
func TestStart_FounderDropsJoiningRole(t *testing.T) {
	co := gossip.New(gossip.Config{
		BindAddr:            "127.0.0.1:" + strconv.Itoa(freePort(t)),
		LogOutput:           io.Discard,
		ReconcileInterval:   time.Hour,
		RejoinInterval:      -1,
		MetaRefreshInterval: -1,
	})
	boot, err := co.Start(coord.Params{
		Self:         coord.Node{ID: "founder", Addr: "127.0.0.1:1"},
		InitialRoles: coord.RoleJoining,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = co.Close() })
	if boot != coord.BootstrapFounded {
		t.Fatalf("Bootstrap = %v, want BootstrapFounded", boot)
	}
	if !waitForMember(co, "founder", 2*time.Second) {
		t.Fatal("founder never landed in its own view")
	}
	if m, _ := co.View().Member("founder"); m.Joining() {
		t.Fatal("a founder advertised RoleJoining; it must be dropped (no incumbent to warm against)")
	}
}

// TestStart_JoinerKeepsJoiningRoleAndReportsJoined pins the other side of the
// founder guard: a node that reaches a seed IS joining an existing cluster, so
// the requested role survives and Bootstrap says so (which is what tells the
// storage layer to learn the cluster generation instead of defining it).
func TestStart_JoinerKeepsJoiningRoleAndReportsJoined(t *testing.T) {
	seedPort := freePort(t)
	seed := gossip.New(gossip.Config{
		BindAddr:            "127.0.0.1:" + strconv.Itoa(seedPort),
		LogOutput:           io.Discard,
		ReconcileInterval:   time.Hour,
		RejoinInterval:      -1,
		MetaRefreshInterval: -1,
	})
	if _, err := seed.Start(coord.Params{Self: coord.Node{ID: "seed", Addr: "127.0.0.1:1"}}); err != nil {
		t.Fatalf("seed Start: %v", err)
	}
	t.Cleanup(func() { _ = seed.Close() })

	joiner := gossip.New(gossip.Config{
		BindAddr:            "127.0.0.1:" + strconv.Itoa(freePort(t)),
		Seeds:               []string{"127.0.0.1:" + strconv.Itoa(seedPort)},
		LogOutput:           io.Discard,
		ReconcileInterval:   time.Hour,
		RejoinInterval:      -1,
		MetaRefreshInterval: -1,
	})
	boot, err := joiner.Start(coord.Params{
		Self:         coord.Node{ID: "joiner", Addr: "127.0.0.1:2"},
		InitialRoles: coord.RoleJoining,
	})
	if err != nil {
		t.Fatalf("joiner Start: %v", err)
	}
	t.Cleanup(func() { _ = joiner.Close() })
	if boot != coord.BootstrapJoined {
		t.Fatalf("Bootstrap = %v, want BootstrapJoined", boot)
	}
	if !waitForMember(joiner, "joiner", 2*time.Second) {
		t.Fatal("joiner never landed in its own view")
	}
	if m, _ := joiner.View().Member("joiner"); !m.Joining() {
		t.Fatal("a joiner dropped RoleJoining; the warming stance must ride the first announcement")
	}
}

// TestClose_IsIdempotent pins that Close can be called twice (the storage
// layer's Close path can race a hard-kill seam) without panicking on a
// double-close of the stop channel.
func TestClose_IsIdempotent(t *testing.T) {
	co := gossip.New(gossip.Config{
		BindAddr:            "127.0.0.1:" + strconv.Itoa(freePort(t)),
		LogOutput:           io.Discard,
		ReconcileInterval:   time.Hour,
		RejoinInterval:      -1,
		MetaRefreshInterval: -1,
	})
	if _, err := co.Start(coord.Params{Self: coord.Node{ID: "c", Addr: "127.0.0.1:1"}}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := co.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := co.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
