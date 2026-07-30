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

func freePort(t testing.TB) int {
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

// waitForRingMember polls the PLACEMENT basis until it holds id. Tests that
// manipulate the ring (TestingRemoveFromRing + reconcile-heal assertions) MUST
// gate on this, not on the view: the view answers from the membership
// snapshot, which is populated at Open BEFORE the events loop has mirrored the
// join into the ring - so a view-gated test can remove the member while its
// join event is still in flight and watch the event re-add it immediately.
func waitForRingMember(co *gossip.Coordinator, id storageunit.NodeID, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range co.PlacementMembers() {
			if m.ID == id {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
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

	if !waitForRingMember(co, "solo", 2*time.Second) {
		t.Fatalf("local member never landed in the ring; placement=%+v", co.PlacementMembers())
	}

	// Induce the divergence STABLY: a queued membership event (the initial
	// join, an incarnation refresh) delivered between the removal and the
	// probe re-adds the member through the EVENT path, which is a different
	// heal than the one under test. Re-remove until the drop sticks - the
	// event queue is finite and quiesced (meta refresh and rejoin disabled),
	// so this converges within a few attempts.
	// Drop-then-probe as ONE retryable cycle: a queued membership event (the
	// initial join, an incarnation refresh) can heal the ring through the
	// EVENT path at any point before the reconcile probe runs - including
	// between "the drop stuck" and the probe itself (the interleaving a
	// drop-only retry loop still lost to under -race). The cycle
	// distinguishes the two no-change outcomes: ring repopulated = an event
	// healed it first (not the path under test - retry the cycle); ring
	// still empty = the reconcile heal is genuinely broken (the regression
	// this test exists to catch).
	healed := false
	for attempt := 0; attempt < 50 && !healed; attempt++ {
		co.TestingRemoveFromRing("solo")
		if len(co.TestingRingMembers()) != 0 {
			// An event re-added the member before the drop even stuck.
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// The ring drop is a PLACEMENT divergence only: the View answers
		// from the membership snapshot, which still (correctly) knows the
		// member - a dropped ring event must never make a live member vanish
		// from stance questions.
		if got := co.View().Members; len(got) != 1 || got[0].ID != "solo" {
			t.Fatalf("post-remove view must still show the snapshot member, got %+v", got)
		}
		if co.TestingReconcileRing() {
			healed = true
			break
		}
		if len(co.TestingRingMembers()) == 0 {
			t.Fatal("reconcile reported no change while the member was still missing from the ring")
		}
		// No-change AND ring repopulated: an event healed it between our
		// emptiness check and the probe. Retry the cycle.
	}
	if !healed {
		t.Fatalf("never completed a clean drop-then-reconcile cycle; events healed the ring every time (ring=%+v)", co.TestingRingMembers())
	}
	ringMembers := co.TestingRingMembers()
	if len(ringMembers) != 1 || ringMembers[0].ID != "solo" {
		t.Fatalf("reconcile did not restore the local member to the ring; ring=%+v", ringMembers)
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

// The Locate contract tests (exclusion as a genuine reduced placement,
// primary-stable-across-N, clamps + degenerate n, exclude-everyone) moved
// to the shared port-contract harness: internal/coordcontract, run here by
// TestPortContract in contract_test.go.

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
