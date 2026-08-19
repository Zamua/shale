package integration

// Half-purged consistency for the mount-time tombstone purge (docs/SPEC.md
// "Tombstone purge", "Consistency while half-purged"): replicas purge
// independently, so between their passes one replica holds a NATIVE absence
// where the other still holds the tombstone envelope - and reads through
// either node must answer deleted throughout.
//
// The fixture distinguishes the states it claims (a purge that silently
// no-ops would also read deleted everywhere): node A's local copy must be
// NATIVELY absent (LocalGet -> ErrNotFound) while node B's still holds the
// envelope bytes (LocalGet -> a decodable empty-payload envelope). Only then
// are the cluster-level reads evidence about the half-purged state.

import (
	"errors"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
)

func TestTombstonePurge_HalfPurgedClusterReadsDeletedConsistently(t *testing.T) {
	const grace = 300 * time.Millisecond
	backing := sharedfactory.NewBacking()

	// Purge is enabled ONLY on node A (B's grace stays zero = disabled), so
	// the half-purged state is constructed deterministically: no reconcile
	// remount on B can purge its copy mid-test.
	na := startReplicatedNodeCfg(t, "tp-a", "", 4, 2, backing, func(c *cluster.Config) {
		c.TombstoneGracePeriod = grace
	})
	nb := startReplicatedNodeCfg(t, "tp-b", na.ClusterToken, 4, 2, backing, nil)
	if err := waitForMembersAll([]*cluster.Cluster{na.Cluster, nb.Cluster}, 2, 30*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond) // let the initial mounts settle.

	key := []byte("tp-key")
	if err := na.Cluster.Put(key, []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := nb.Cluster.Get(key); err != nil {
		t.Fatalf("pre-delete Get via B: %v", err)
	}
	if err := na.Cluster.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	time.Sleep(2 * grace) // age the tombstone past A's grace.
	na.Cluster.TestingRunTombstonePurge()

	// The distinguishing assertions: A natively absent, B still enveloped.
	if _, err := na.Cluster.LocalGet(key); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("A's local copy after purge: err = %v, want native ErrNotFound", err)
	}
	rawB, err := nb.Cluster.LocalGet(key)
	if err != nil {
		t.Fatalf("B's local copy vanished; the fixture is not half-purged: %v", err)
	}
	envB, derr := cluster.Decode(rawB)
	if derr != nil || len(envB.Payload) != 0 {
		t.Fatalf("B's local copy is not a tombstone envelope (payload %d bytes, err %v)", len(envB.Payload), derr)
	}

	// The property: both nodes read deleted, consistently, while half-purged.
	for _, n := range []*sharedNode{na, nb} {
		if _, err := n.Cluster.Get(key); !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("half-purged Get via %s: err = %v, want ErrNotFound", n.ID, err)
		}
	}

	// Post-purge writes on the same key work everywhere.
	if err := nb.Cluster.Put(key, []byte("v2")); err != nil {
		t.Fatalf("re-create after purge: %v", err)
	}
	for _, n := range []*sharedNode{na, nb} {
		v, err := n.Cluster.Get(key)
		if err != nil || string(v) != "v2" {
			t.Fatalf("re-created read via %s: %q, %v", n.ID, v, err)
		}
	}
}
