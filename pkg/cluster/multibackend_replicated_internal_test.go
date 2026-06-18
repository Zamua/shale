package cluster

// White-box tests for v0.8 Phase 2b per-UNIT replica routing. These live in
// package cluster so they can drive the unexported helpers (unitReplicas,
// desiredReplicaUnits, multiReplicated) against a real ring + a per-replica
// shared-backing factory, without standing up membership / gRPC. The
// wired-together cross-node fan-out + read quorum is covered end to end in
// tests/integration/lossless_multibackend_r2_gate_test.go.

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// newReplicatedCluster builds a minimal R>1 multi-backend Cluster wired to a
// per-replica shared-backing handle + a real ring containing memberIDs, with
// no membership / gRPC. self is this node's id.
func newReplicatedCluster(t *testing.T, self string, n, r int, backing *sharedfactory.Backing, memberIDs ...string) *Cluster {
	t.Helper()
	h := backing.Handle()
	rg := ring.New()
	for _, id := range memberIDs {
		rg.Add(ring.Member{ID: id, Addr: id + ":0"})
	}
	c := &Cluster{
		cfg:             Config{NodeID: self, ReplicationFactor: r},
		multi:           true,
		factory:         h,
		unitCount:       storageunit.MustUnitCount(n),
		mountMap:        make(map[storageunit.ReplicaUnit]backend.Backend),
		handoffPhase:    make(map[storageunit.ReplicaUnit]storageunit.HandoffState),
		acquireInFlight: make(map[storageunit.ReplicaUnit]struct{}),
		pauseUnits:      make(map[storageunit.UnitID]*sync.RWMutex),
		ring:            rg,
		closeCh:         make(chan struct{}),
	}
	c.genOwner = c.genUnitOwner
	c.initGenState()
	c.initReplicatedFactory()
	return c
}

func TestMultiReplicated_PredicateGating(t *testing.T) {
	backing := sharedfactory.NewBacking()

	// R=2 with a populated 3-member ring: replicated.
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	if !c.multiReplicated() {
		t.Fatalf("R=2 multi with populated ring should be replicated")
	}
	if c.replicaFactory == nil {
		t.Fatalf("R>1 should wire replicaFactory")
	}

	// R=1 multi: NOT replicated (single-mount path).
	c1 := newReplicatedCluster(t, "n1", 8, 1, backing, "n1", "n2")
	if c1.multiReplicated() {
		t.Fatalf("R=1 multi should not be replicated")
	}
	if c1.replicaFactory != nil {
		t.Fatalf("R=1 should not wire replicaFactory")
	}
}

func TestUnitReplicas_ReturnsRDistinctNodes(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 16, 2, backing, "n1", "n2", "n3")

	for _, u := range c.unitCount.IDs() {
		gu := storageunit.NewGenUnit(0, u)
		set := c.unitReplicas(gu)
		if len(set) != 2 {
			t.Fatalf("unit %d: replica set size %d, want 2", u, len(set))
		}
		if set[0].ID == set[1].ID {
			t.Fatalf("unit %d: replicas are the same node %q (must be distinct)", u, set[0].ID)
		}
	}
}

// TestUnitReplicas_AgreesAcrossNodes: the replica set for a unit is identical
// regardless of which node computes it (same ring, same hashing).
func TestUnitReplicas_AgreesAcrossNodes(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c1 := newReplicatedCluster(t, "n1", 16, 2, backing, "n1", "n2", "n3")
	c2 := newReplicatedCluster(t, "n2", 16, 2, backing, "n1", "n2", "n3")

	for _, u := range c1.unitCount.IDs() {
		gu := storageunit.NewGenUnit(0, u)
		s1 := c1.unitReplicas(gu)
		s2 := c2.unitReplicas(gu)
		if len(s1) != len(s2) {
			t.Fatalf("unit %d: replica set sizes differ %d vs %d", u, len(s1), len(s2))
		}
		for i := range s1 {
			if s1[i].ID != s2[i].ID {
				t.Fatalf("unit %d pos %d: %q vs %q (must agree)", u, i, s1[i].ID, s2[i].ID)
			}
		}
	}
}

// TestDesiredReplicaUnits_UnionCoversEveryUnitRTimes: across the 3 nodes,
// every unit is desired by exactly R=2 nodes (its replica set), and each node
// records the position it holds.
func TestDesiredReplicaUnits_UnionCoversEveryUnitRTimes(t *testing.T) {
	const n, r = 16, 2
	backing := sharedfactory.NewBacking()
	ids := []string{"n1", "n2", "n3"}

	count := map[storageunit.UnitID]int{}
	for _, self := range ids {
		c := newReplicatedCluster(t, self, n, r, backing, ids...)
		for _, ru := range c.desiredReplicaUnits() {
			count[ru.Unit.ID]++
			// The recorded position must match self's index in the replica set.
			set := c.unitReplicas(ru.Unit)
			if int(ru.Replica) >= len(set) || set[ru.Replica].ID != self {
				t.Fatalf("unit %d: recorded replica pos %d does not point at %q", ru.Unit.ID, ru.Replica, self)
			}
		}
	}
	for _, u := range storageunit.MustUnitCount(n).IDs() {
		if count[u] != r {
			t.Fatalf("unit %d desired by %d nodes, want R=%d", u, count[u], r)
		}
	}
}

// TestDesiredReplicaUnits_Characterization pins the EXACT ordered
// []ReplicaUnit that desiredReplicaUnits returns for a fixed small topology
// (a deterministic 3-node ring, R=2, N=8, self=n1). It is a refactor guard:
// desiredReplicaUnits was reworked to derive its result from the pure
// storageunit.OwnedReplicaUnits (via a ReplicaLookupFunc adapter) instead of
// an inline ring loop, and this test pins that the observable output is
// byte-for-byte unchanged across that refactor. The golden slice below was
// captured from the pre-refactor inline implementation; if the ring hashing
// ever changes, regenerate it deliberately (do not loosen the assertion).
//
// Because the refactored desiredReplicaUnits now routes through
// storageunit.OwnedReplicaUnits, this test also exercises the pure-domain
// derivation end to end (the cluster supplies the ring-backed ReplicaLookup,
// the pure function enumerates + positions, the cluster qualifies with the
// live generation).
func TestDesiredReplicaUnits_Characterization(t *testing.T) {
	const n, r = 8, 2
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", n, r, backing, "n1", "n2", "n3")

	got := c.desiredReplicaUnits()

	// Golden: the exact ordered (gen, unitID, replicaPos) n1 holds at gen 0.
	type want struct {
		unit storageunit.UnitID
		pos  uint8
	}
	golden := []want{
		{0, 1},
		{3, 1},
		{5, 0},
		{6, 1},
	}
	if len(got) != len(golden) {
		t.Fatalf("desiredReplicaUnits len = %d, want %d; got = %v", len(got), len(golden), got)
	}
	for i, g := range got {
		if g.Unit.Gen != 0 {
			t.Fatalf("entry %d: gen = %d, want 0 (static topology)", i, g.Unit.Gen)
		}
		if g.Unit.ID != golden[i].unit || g.Replica != golden[i].pos {
			t.Fatalf("entry %d: got (u%d r%d), want (u%d r%d)",
				i, g.Unit.ID, g.Replica, golden[i].unit, golden[i].pos)
		}
	}
}

// TestReplicasForKey_CoLocatedKeysShareReplicaSet: keys in the same {tag} set
// resolve to one unit and therefore one replica set.
func TestReplicasForKey_CoLocatedKeysShareReplicaSet(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 16, 2, backing, "n1", "n2", "n3")

	a := c.replicasForKey([]byte("{acct42}:balance"))
	b := c.replicasForKey([]byte("{acct42}:name"))
	if len(a) != len(b) {
		t.Fatalf("co-located keys got different replica-set sizes %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			t.Fatalf("co-located keys diverge at pos %d: %q vs %q", i, a[i].ID, b[i].ID)
		}
	}
}

// TestMountReplicaUnits_DegradedBoot pins v0.8 Phase 2f: a replica position
// whose backing store cannot be opened is SKIPPED at boot (recorded +
// observable), not fatal. The node mounts every HEALTHY position and Open
// succeeds, instead of the whole node bricking on one bad replica.
//
// BREAK DEMONSTRATION: the pre-fix mountReplicaUnits returned the open error +
// closeMountedUnits() here, so on that code this test fails - mountReplicaUnits
// returns non-nil and mounts nothing. The skip is what holds the node up.
//
// It also proves the skipped position RE-MOUNTS once the damage is cleared (the
// self-heal the periodic reconcile drives in production).
func TestMountReplicaUnits_DegradedBoot(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")

	desired := c.desiredReplicaUnits()
	if len(desired) < 2 {
		t.Fatalf("test needs n1 to own >=2 replica positions, got %d", len(desired))
	}
	bad := desired[0]

	// Poison ONE of n1's replica positions: its backing store is un-openable,
	// modeling a corrupt/truncated durable database (slatedb "empty SSTable").
	injected := errors.New("Data error: empty SSTable (injected)")
	backing.SetOpenReplicaFault(bad, injected)

	// DEGRADED BOOT: the Open-time mount must NOT abort on the one bad replica.
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits must NOT fail on one un-openable replica (degraded boot); got %v", err)
	}

	// The poisoned position is SKIPPED: unmounted, but recorded for /debug/shale/state.
	if _, ok := c.mountMap[bad]; ok {
		t.Fatalf("poisoned replica %s must NOT be mounted", bad)
	}
	v, ok := c.lastAcquireErr.Load(bad)
	if !ok {
		t.Fatalf("poisoned replica %s must be recorded in lastAcquireErr", bad)
	}
	if s, _ := v.(string); !strings.Contains(s, "empty SSTable") {
		t.Fatalf("lastAcquireErr for %s = %q, want it to carry the open error", bad, s)
	}

	// Every OTHER desired position IS mounted: the healthy units keep full service.
	for _, ru := range desired[1:] {
		if _, ok := c.mountMap[ru]; !ok {
			t.Fatalf("healthy replica %s must be mounted on a degraded boot", ru)
		}
	}

	// SELF-HEAL: once the backing is repaired, re-acquiring the position mounts
	// it and clears the recorded error (the periodic reconcile drives this in
	// production; here we drive the acquire primitive directly).
	backing.SetOpenReplicaFault(bad, nil)
	c.reconcileMu.Lock()
	c.acquireReplicaUnit(bad)
	c.reconcileMu.Unlock()
	if _, ok := c.mountMap[bad]; !ok {
		t.Fatalf("repaired replica %s must mount on re-acquire", bad)
	}
	if _, ok := c.lastAcquireErr.Load(bad); ok {
		t.Fatalf("lastAcquireErr for %s must be cleared after a successful re-acquire", bad)
	}
}
