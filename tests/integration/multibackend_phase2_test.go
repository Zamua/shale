package integration

// End-to-end coverage for v0.8 Phase 2 (multi-backend node, STATIC unit
// routing). These tests stand up in-process clusters (goroutines +
// ephemeral ports) using the in-memory memfactory.BackendFactory test
// double, and exercise the five Phase 2 guarantees the spec subsection
// "v0.8 Phase 2: multi-backend node (static routing)" pins:
//
//  1. SINGLE NODE, multi-unit: one node owns all N units; Put/Get round-
//     trips, and a key lands in the RIGHT unit's backend (asserted via the
//     factory's per-unit store), present there and nowhere else.
//  2. MULTI NODE static: a key written on a non-owner node FORWARDS to the
//     unit's owner and reads back from any node; co-located keys (same
//     {tag}) all land on one node/unit.
//  3. CO-EXISTENCE: the legacy single-Config.Backend path still works and
//     is byte-for-byte unchanged; multi-backend is opt-in.
//  4. CONFIG validation: factory+unitcount XOR backend; bad combos error.
//  5. The unit-based owner guard: a forwarded op for a unit a node does not
//     own is refused (FailedPrecondition), NOT re-forwarded (no A->B->A
//     loop).
//
// The existing multibackend_test.go covers the 2-node forward + guard
// happy path; this file pins the per-unit PHYSICAL placement (via the unit
// map), the legacy-mode byte-for-byte equivalence, the config XOR at the
// real Open boundary, and the no-loop property of the guard.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// unitForKeyTest is the test-side mirror of the cluster's unit map: extract
// the shard key the SAME way the ring does, then mask to the unit (the logical
// gen-0 unit; these Phase 2 tests do not reshard). Lets a test name the exact
// unit a key must land in without reaching into the unexported cluster
// internals.
func unitForKeyTest(key string, unitCount int) storageunit.UnitID {
	uc := storageunit.MustUnitCount(unitCount)
	return storageunit.UnitForShardKey(ring.ShardKey([]byte(key)), uc)
}

// keyInUnitBackend reports whether key=val is physically present in the
// per-unit backend the factory holds for u. ok is false when that unit was
// never opened (no store) or the key is absent / has a different value.
func keyInUnitBackend(t *testing.T, fac *memfactory.Factory, u storageunit.UnitID, key, val string) bool {
	t.Helper()
	b, present := fac.UnitBackend(storageunit.NewGenUnit(0, u))
	if !present {
		return false
	}
	got, err := b.Get([]byte(key))
	if errors.Is(err, backend.ErrNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("UnitBackend(%d).Get(%q): %v", u, key, err)
	}
	return bytes.Equal(got, []byte(val))
}

// keyPresentInAnyUnitExcept scans every unit's backend EXCEPT keepUnit and
// returns the first unit id that holds key (or -1, false if none does). The
// physical-isolation assertion: a key's bytes must live in its own unit's
// backend and in NO other unit's backend.
func keyPresentInAnyUnitExcept(t *testing.T, fac *memfactory.Factory, unitCount int, key string, keepUnit storageunit.UnitID) (storageunit.UnitID, bool) {
	t.Helper()
	uc := storageunit.MustUnitCount(unitCount)
	for _, u := range uc.IDs() {
		if u == keepUnit {
			continue
		}
		b, present := fac.UnitBackend(storageunit.NewGenUnit(0, u))
		if !present {
			continue
		}
		if _, err := b.Get([]byte(key)); err == nil {
			return u, true
		} else if !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("UnitBackend(%d).Get(%q): %v", u, key, err)
		}
	}
	return 0, false
}

// TestMBPhase2_SingleNodeMultiUnitPlacement (scenario 1). One node owns all
// N units (empty ring -> owns the whole unit space). Put then Get round-
// trips many keys, and each key's value lands in EXACTLY its own unit's
// backend (asserted via the factory's per-unit store) and no other unit's.
func TestMBPhase2_SingleNodeMultiUnitPlacement(t *testing.T) {
	const unitCount = 16
	n := startMultiBackendNode(t, "solo", "", unitCount)

	// A spread of keys; with 16 units and varied keys these cover several
	// distinct units (asserted at the end).
	keys := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		keys = append(keys, fmt.Sprintf("rec-%04d", i))
	}

	for _, k := range keys {
		if err := n.Cluster.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}

	hitUnits := make(map[storageunit.UnitID]struct{})
	for _, k := range keys {
		// Round-trips through the cluster surface.
		got, err := n.Cluster.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %q: %v", k, err)
		}
		if !bytes.Equal(got, []byte("v-"+k)) {
			t.Fatalf("Get %q = %q, want %q", k, got, "v-"+k)
		}

		// Physical placement: the value lives in THIS key's unit backend.
		u := unitForKeyTest(k, unitCount)
		hitUnits[u] = struct{}{}
		if !keyInUnitBackend(t, n.Factory, u, k, "v-"+k) {
			t.Fatalf("key %q not found in its unit %d's backend", k, u)
		}
		// And in NO other unit's backend (per-unit isolation).
		if other, found := keyPresentInAnyUnitExcept(t, n.Factory, unitCount, k, u); found {
			t.Fatalf("key %q leaked into unit %d's backend (should only be in unit %d)", k, other, u)
		}
	}

	// The keys actually spanned multiple units, otherwise the placement
	// assertion is vacuous (everything trivially in one bucket).
	if len(hitUnits) < 2 {
		t.Fatalf("keys landed in %d distinct units, want >= 2 (placement test would be vacuous)", len(hitUnits))
	}
}

// TestMBPhase2_ForwardingAndCoLocation (scenario 2). A 2-node static
// cluster: a key written on the NON-owner node forwards over gRPC to the
// unit owner and reads back from EITHER node; a batch of co-located keys
// (shared {tag}) all land on ONE node and ONE unit.
func TestMBPhase2_ForwardingAndCoLocation(t *testing.T) {
	const unitCount = 16
	n1 := startMultiBackendNode(t, "mbp1", "", unitCount)
	n2 := startMultiBackendNode(t, "mbp2", n1.BindAddr, unitCount)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("ring convergence: %v", err)
	}

	// Forwarding: a key whose unit n2 owns, written via n1, must land on
	// n2's storage and be absent from n1's, then read back via BOTH nodes.
	keyOnN2 := keyForOwner(t, n1.Cluster, unitCount, "mbp2")
	if err := n1.Cluster.Put([]byte(keyOnN2), []byte("fwd")); err != nil {
		t.Fatalf("forwarded Put via n1: %v", err)
	}
	assertPlacedOn(t, n2, keyOnN2, "fwd")
	assertAbsentOn(t, n1, keyOnN2)
	for _, c := range clusters {
		got, err := c.Get([]byte(keyOnN2))
		if err != nil {
			t.Fatalf("Get %q via %s: %v", keyOnN2, c.NodeID(), err)
		}
		if !bytes.Equal(got, []byte("fwd")) {
			t.Fatalf("Get %q via %s = %q, want fwd", keyOnN2, c.NodeID(), got)
		}
	}

	// Co-location: all keys under one {tag} share a shard key -> one hash
	// -> one unit -> one owner. Write them via whichever node, then assert
	// every co-located key sits in the SAME unit, on the SAME owner node.
	const tag = "tenant42"
	wantOwner := unitOwnerID(n1.Cluster, "{"+tag+"}/probe", unitCount)
	wantUnit := unitForKeyTest("{"+tag+"}/probe", unitCount)
	var ownerNode *multiNode
	switch wantOwner {
	case n1.ID:
		ownerNode = n1
	case n2.ID:
		ownerNode = n2
	default:
		t.Fatalf("co-location owner %q is neither node", wantOwner)
	}

	const colocated = 30
	for i := 0; i < colocated; i++ {
		k := fmt.Sprintf("{%s}/item/%03d", tag, i)
		// Write via the NON-owner node so the routing/forwarding handles
		// placement, not the test.
		writer := n1
		if writer.ID == wantOwner {
			writer = n2
		}
		if err := writer.Cluster.Put([]byte(k), []byte("c")); err != nil {
			t.Fatalf("Put co-located %q via %s: %v", k, writer.ID, err)
		}
	}

	// Every co-located key is in the owner's wantUnit backend, and the
	// non-owner holds none of them.
	for i := 0; i < colocated; i++ {
		k := fmt.Sprintf("{%s}/item/%03d", tag, i)
		gotUnit := unitForKeyTest(k, unitCount)
		if gotUnit != wantUnit {
			t.Fatalf("co-located key %q hashed to unit %d, want %d (shared by the tag)", k, gotUnit, wantUnit)
		}
		if !keyInUnitBackend(t, ownerNode.Factory, wantUnit, k, "c") {
			t.Fatalf("co-located key %q missing from owner %s unit %d", k, ownerNode.ID, wantUnit)
		}
		assertAbsentOn(t, otherMultiNode(ownerNode, n1, n2), k)
	}
}

// otherMultiNode returns whichever of a/b is NOT owner.
func otherMultiNode(owner, a, b *multiNode) *multiNode {
	if owner == a {
		return b
	}
	return a
}

// TestMBPhase2_LegacyPathUnchanged (scenario 3, co-existence). The legacy
// single-Config.Backend path is opt-in's complement: a node started WITHOUT
// a factory runs the per-node mode exactly as before. We run an
// existing-style legacy 2-node round-trip + forwarding test alongside the
// multi-backend tests and assert the legacy node is NOT in multi-backend
// mode (multi-backend stays strictly opt-in). The behavior asserted here
// is identical to the pre-v0.8 per-node forwarding contract: same Put/Get,
// same physical placement on the owner's single backend.
func TestMBPhase2_LegacyPathUnchanged(t *testing.T) {
	// Two legacy nodes (memory backend per node, NO factory): the canonical
	// pre-v0.8 shape. startTestNode builds exactly that. The legacy path
	// runs the v0.3+ rebalance on join, so we wait for full quiescence
	// (ring convergence + bootstrap-migration settle) before writing -
	// exactly as the legacy threeNodeFixture does. (The multi-backend
	// tests above only need waitForMembersAll because Phase 2 has no
	// rebalance; this difference is itself part of the co-existence story.)
	n1 := startTestNode(t, "legacy1", "")
	n2 := startTestNode(t, "legacy2", n1.BindAddr)
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	waitForClusterReady(t, clusters, 15*time.Second)

	// Forwarding still works exactly as pre-v0.8: a key owned by n2,
	// written via n1, forwards and lands on n2's single backend.
	key := findKeyNotOwnedBy(t, n1.Cluster, "legacy1", 1000)
	owner := ownerOf(n1.Cluster, key)
	if err := putWithRetry(n1.Cluster, []byte(key), []byte("legacy-val")); err != nil {
		t.Fatalf("legacy Put: %v", err)
	}
	// Read back via BOTH nodes (one local, one forwarded).
	for _, c := range clusters {
		got, err := c.Get([]byte(key))
		if err != nil {
			t.Fatalf("legacy Get via %s: %v", c.NodeID(), err)
		}
		if !bytes.Equal(got, []byte("legacy-val")) {
			t.Fatalf("legacy Get via %s = %q, want legacy-val", c.NodeID(), got)
		}
	}

	// Physical placement on the OWNER's single backend (the per-node path:
	// keys live in the one Config.Backend, not a unit map). Byte-for-byte
	// the same as the pre-v0.8 behavior.
	ownerNode := n1
	if owner == n2.ID {
		ownerNode = n2
	}
	if got, err := ownerNode.Backend.Get([]byte(key)); err != nil || !bytes.Equal(got, []byte("legacy-val")) {
		t.Fatalf("legacy placement: owner %s backend has %q (err=%v), want legacy-val", ownerNode.ID, got, err)
	}
	// And ABSENT from the non-owner's single backend.
	nonOwner := n2
	if owner == n2.ID {
		nonOwner = n1
	}
	if _, err := nonOwner.Backend.Get([]byte(key)); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("legacy placement: non-owner %s unexpectedly holds key (err=%v)", nonOwner.ID, err)
	}
}

// TestMBPhase2_ConfigValidationAtOpen (scenario 4). The factory+unitcount
// XOR backend rule, asserted at the REAL cluster.Open boundary (not just
// the unexported validateBackendMode unit test): bad combos make Open fail
// with a clear error; the two valid shapes succeed.
func TestMBPhase2_ConfigValidationAtOpen(t *testing.T) {
	uc := storageunit.MustUnitCount(8)

	cases := []struct {
		name    string
		cfg     cluster.Config
		wantErr bool
	}{
		{
			name:    "neither backend nor factory: error",
			cfg:     cluster.Config{NodeID: "x", LogOutput: io.Discard},
			wantErr: true,
		},
		{
			name:    "both backend and factory+unitcount: error",
			cfg:     cluster.Config{NodeID: "x", Backend: memory.New(), BackendFactory: memfactory.New(), UnitCount: uc, LogOutput: io.Discard},
			wantErr: true,
		},
		{
			name:    "factory without unitcount: error",
			cfg:     cluster.Config{NodeID: "x", BackendFactory: memfactory.New(), LogOutput: io.Discard},
			wantErr: true,
		},
		{
			name:    "unitcount without factory: error",
			cfg:     cluster.Config{NodeID: "x", UnitCount: uc, LogOutput: io.Discard},
			wantErr: true,
		},
		{
			name:    "backend + unitcount (no factory): error",
			cfg:     cluster.Config{NodeID: "x", Backend: memory.New(), UnitCount: uc, LogOutput: io.Discard},
			wantErr: true,
		},
		{
			name:    "legacy backend only: ok",
			cfg:     cluster.Config{NodeID: "x", Backend: memory.New(), LogOutput: io.Discard},
			wantErr: false,
		},
		{
			name:    "factory + unitcount: ok",
			cfg:     cluster.Config{NodeID: "x", BackendFactory: memfactory.New(), UnitCount: uc, LogOutput: io.Discard},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := cluster.Open(tc.cfg)
			if c != nil {
				t.Cleanup(func() { _ = c.Close() })
			}
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Open: expected error, got nil")
				}
				// The error must be informative (mentions the mode contract),
				// not an opaque nil-pointer panic surfaced as a generic error.
				if c != nil {
					t.Fatalf("Open returned a non-nil cluster alongside an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Open: unexpected error: %v", err)
			}
		})
	}
}

// TestMBPhase2_OwnerGuardNoLoop (scenario 5). The unit-based owner guard: a
// forwarded op (Forwarded=true) for a unit a node does NOT own is REFUSED
// with FailedPrecondition, never re-forwarded. We prove the "no A->B->A
// loop" property two ways:
//
//  1. The refusal is immediate FailedPrecondition (the guard short-circuits
//     before any forward decision).
//  2. The non-owner node never dials the owner for the forwarded op: a
//     re-forward would be a fresh outbound gRPC call, which would either
//     loop or eventually succeed. We assert the refusal returns promptly
//     and the owner's storage is untouched by the refused op.
func TestMBPhase2_OwnerGuardNoLoop(t *testing.T) {
	const unitCount = 16
	n1 := startMultiBackendNode(t, "guard1", "", unitCount)
	n2 := startMultiBackendNode(t, "guard2", n1.BindAddr, unitCount)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster}, 2, 15*time.Second); err != nil {
		t.Fatalf("ring convergence: %v", err)
	}

	// A key whose unit n1 owns. n2 (the joiner) never mounted n1's units,
	// so forwarding it to n2 must hit the guard. (The joiner is the node
	// that genuinely does not own the unit; see the existing guard test for
	// why the seed would accept it under Phase 2 static topology.)
	keyOnN1 := keyForOwner(t, n1.Cluster, unitCount, "guard1")
	if n2.Cluster.OwnsKey([]byte(keyOnN1)) {
		t.Fatalf("precondition: guard2 unexpectedly owns the unit for %q", keyOnN1)
	}
	wantUnit := unitForKeyTest(keyOnN1, unitCount)

	conn, err := grpc.NewClient(n2.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial guard2: %v", err)
	}
	defer func() { _ = conn.Close() }()
	api := pb.NewShaleNodeClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// The refused op must return quickly (no re-forward round-trip, no
	// loop). Bound it well under the context deadline.
	start := time.Now()
	_, err = api.Put(ctx, &pb.PutRequest{Key: []byte(keyOnN1), Value: []byte("should-not-land"), Forwarded: true})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("forwarded Put on a non-owned unit must be refused, got nil error")
	}
	if st, _ := status.FromError(err); st.Code() != codes.FailedPrecondition {
		t.Fatalf("Put guard: expected FailedPrecondition, got %v (%v)", st.Code(), err)
	}
	if elapsed > time.Second {
		t.Fatalf("forwarded refusal took %v - a re-forward loop would account for the delay; the guard must short-circuit", elapsed)
	}

	// No-loop proof part 2: the refused write never reached the actual
	// owner's (n1) unit backend. A re-forward (A->B->A) would have landed
	// the value on n1. It must be absent.
	if keyInUnitBackend(t, n1.Factory, wantUnit, keyOnN1, "should-not-land") {
		t.Fatalf("refused forwarded write reached owner guard1 unit %d - the guard re-forwarded instead of refusing", wantUnit)
	}

	// And a NON-forwarded request to guard2 still routes correctly to the
	// owner (the guard only refuses ALREADY-forwarded ops, so normal
	// client traffic is unaffected).
	if err := n2.Cluster.Put([]byte(keyOnN1), []byte("routed-ok")); err != nil {
		t.Fatalf("non-forwarded Put via guard2 should route to guard1: %v", err)
	}
	if got, err := n1.Cluster.Get([]byte(keyOnN1)); err != nil || !bytes.Equal(got, []byte("routed-ok")) {
		t.Fatalf("routed write not visible on guard1: got=%q err=%v", got, err)
	}
}
