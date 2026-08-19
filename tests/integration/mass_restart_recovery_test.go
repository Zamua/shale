package integration

// Mass-restart auto-recovery (v0.8 Phase 2e, abrupt-termination regime). A
// graceful single-node leave is one thing; a MASS restart (every node of an R=2
// multi-backend cluster goes down near-simultaneously, then all come back on the
// SAME durable backing) is the abrupt-termination case. The contract: durability
// is preserved (every acked key survives) AND the cluster AUTO-RECOVERS to a
// serving state without intervention - an availability blip during the restart
// is acceptable, a permanently-wedged Acquiring position is not.
//
// This pins the property a real-MinIO staging mass-rollout exposed as missing:
// some per-position slatedb units stayed wedged in Acquiring after a mass
// restart and the cluster never re-converged to serving.

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMassRestart_R2_AutoRecoversAndDurable(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()

	// Phase 1: stand up a 3-node R=2 cluster and write a recorded dataset.
	nodes := start3NodeR2(t, unitCount, backing)
	want, _ := writeRecordedDataset(t, nodes[0].Cluster)
	t.Logf("mass-restart: wrote %d recorded keys, killing all nodes", len(want))

	// Phase 2: MASS KILL - tear every node down near-simultaneously. The durable
	// backing persists (the object-storage analogue); the processes do not.
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n *sharedNode) {
			defer wg.Done()
			n.stop()
		}(n)
	}
	wg.Wait()
	t.Logf("mass-restart: all nodes down; restarting on the same backing")

	// Phase 3: RESTART all nodes on the SAME backing, same IDs (persistent
	// storage, fresh processes). The founder re-establishes; joiners re-seed.
	r1 := startReplicatedNode(t, "r2a", "", unitCount, 2, backing)
	r2 := startReplicatedNode(t, "r2b", r1.ClusterToken, unitCount, 2, backing)
	r3 := startReplicatedNode(t, "r2c", r1.ClusterToken, unitCount, 2, backing)
	restarted := []*sharedNode{r1, r2, r3}
	cs := []*cluster.Cluster{r1.Cluster, r2.Cluster, r3.Cluster}
	if err := waitForMembersAll(cs, 3, 30*time.Second); err != nil {
		t.Fatalf("post-restart membership convergence: %v", err)
	}

	// Phase 4: AUTO-RECOVERY - the cluster must accept writes again WITHOUT any
	// manual nudge. Poll a canary write (across all entry nodes) until it
	// succeeds. A permanently-wedged unit makes this time out = the bug.
	if !pollWriteRecovers(t, restarted, 90*time.Second) {
		t.Fatalf("cluster did NOT auto-recover writes within 90s after mass restart")
	}
	t.Logf("mass-restart: writes auto-recovered")

	// Phase 5: DURABILITY - every recorded (acked) key still readable + correct.
	lost, corrupt := 0, 0
	for k, v := range want {
		got, err := getWithConvergenceRetry(r1.Cluster, k, 20*time.Second)
		if err != nil {
			lost++
			if lost <= 5 {
				t.Errorf("DURABILITY: acked key %q unreadable after mass restart: %v", k, err)
			}
			continue
		}
		if !bytes.Equal(got, v) {
			corrupt++
			if corrupt <= 5 {
				t.Errorf("DURABILITY: acked key %q corrupt: got %q want %q", k, got, v)
			}
		}
	}
	if lost > 0 || corrupt > 0 {
		t.Fatalf("DURABILITY VIOLATED after mass restart: %d lost, %d corrupt of %d", lost, corrupt, len(want))
	}
	t.Logf("mass-restart: durability held (%d keys all readable + correct)", len(want))
}

// pollWriteRecovers writes a canary across every entry node, returning true as
// soon as ANY entry accepts a write (the cluster is serving again), or false on
// timeout. Rotates entries so a single wedged owner does not mask a recovered one.
func pollWriteRecovers(t *testing.T, nodes []*sharedNode, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for i := 0; time.Now().Before(deadline); i++ {
		entry := nodes[i%len(nodes)]
		k := fmt.Sprintf("postrestart-canary-%d", i)
		if err := entry.Cluster.Put([]byte(k), []byte("alive")); err == nil {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

// getWithConvergenceRetry reads key back, retrying past the post-restart
// convergence transients (Unavailable while a position is mid-acquire) until a
// definitive answer or the deadline. A genuinely-lost key stays unreadable past
// the deadline; the generous window cannot hide a real loss behind a transient.
func getWithConvergenceRetry(c *cluster.Cluster, key string, timeout time.Duration) ([]byte, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		got, err := c.Get([]byte(key))
		if err == nil {
			return got, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, lastErr
		}
		if code := status.Code(err); code != codes.Unavailable {
			return nil, err
		}
		time.Sleep(50 * time.Millisecond)
	}
}
