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
