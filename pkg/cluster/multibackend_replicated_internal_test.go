package cluster

// White-box tests for v0.8 Phase 2b per-UNIT replica routing. These live in
// package cluster so they can drive the unexported helpers (unitReplicas,
// desiredReplicaUnits, multiReplicated) against a real ring + a per-replica
// shared-backing factory, without standing up membership / gRPC. The
// wired-together cross-node fan-out + read quorum is covered end to end in
// tests/integration/lossless_multibackend_r2_gate_test.go.

import (
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
		cfg:        Config{NodeID: self, ReplicationFactor: r},
		multi:      true,
		factory:    h,
		unitCount:  storageunit.MustUnitCount(n),
		mountMap:   make(map[storageunit.GenUnit]backend.Backend),
		pauseUnits: make(map[storageunit.UnitID]*sync.RWMutex),
		ring:       rg,
		closeCh:    make(chan struct{}),
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
