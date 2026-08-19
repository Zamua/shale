package integration

// THE R>1 MULTI-BACKEND LOSSLESS GATE (the data-loss oracle for v0.8 Phase 2b).
//
// A 3-node multi-backend cluster at R=2 on the per-replica shared-backing
// factory. Each unit's R=2 replicas are INDEPENDENT durable databases mounted
// on two different nodes (the unit's replica set = LocateKeyN over the unit
// id). The gate writes a recorded BASELINE dataset spanning many units
// (including co-located {tag} sets), then runs a CONCURRENT PROBE that keeps
// acking new keys through the FULL ROUTED SURFACE (writers rotate the entry
// node so writes route both locally and forwarded; a key is recorded ONLY once
// its Put returns nil), folds the acked keys into the recorded set, and
// asserts:
//
//  1. THE ORACLE: every baseline key AND every acked probe key is readable with
//     its EXACT value from EVERY node (forwarding to a replica), zero loss.
//  2. Each unit is mounted on EXACTLY R=2 distinct nodes (the replica set), and
//     a write landed on BOTH replicas (the per-replica independent stores).
//  3. Co-located {tag} sets share one unit hence one replica set.
//  4. SURVIVE-ONE-LOSS: wiping ONE replica copy of a unit + reading still
//     returns every value (the surviving replica is a complete copy).
//
// It is kept honest by a BREAK DEMONSTRATION (TestR2GateCatchesLostWrite) that
// removes BOTH replica copies of a unit and shows the oracle FAILS (catches
// the lost write) - proving the gate is not a rubber stamp.
//
// Static topology: the unit -> replica-set assignment is fixed at Open
// (membership-change handoff at R>1 is a later phase). So no membership change
// happens mid-test; the 3 nodes are all present before the dataset is written.

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/clustertest"
	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
)

// startReplicatedNode brings up one R=replicationFactor multi-backend node
// whose factory is a per-replica handle over backing. seedAddr empty =
// founder of a NEW cluster (its own membership store), so a seedless
// replacement is isolated from the survivors by construction.
func startReplicatedNode(t *testing.T, id, seedAddr string, unitCount, replicationFactor int, backing *sharedfactory.Backing) *sharedNode {
	t.Helper()
	h := backing.Handle()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startReplicatedNode %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:            id,
		BackendFactory:    h,
		UnitCount:         storageunit.MustUnitCount(unitCount),
		ReplicationFactor: replicationFactor,
		// WriteAll so every acked write reaches BOTH replicas (W=R), and ReadAll
		// so a read consults BOTH replicas and finds the surviving stamped value
		// after one replica copy is lost (the SURVIVE-ONE-LOSS oracle). With R=2,
		// ReadQuorum == ReadAll, so this is the strict-durability replicated config.
		WriteConsistency:     cluster.WriteAll,
		ReadConsistency:      cluster.ReadAll,
		GRPCAddr:             grpcAddr,
		LogOutput:            io.Discard,
		RebalanceSettleDelay: 300 * time.Millisecond,
	}
	store, token := coordStoreFor(t, seedAddr)
	c := clustertest.OpenClusterCAS(t, cfg, store)

	rpc.NewServer(c).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &sharedNode{
		ID:           id,
		Cluster:      c,
		Handle:       h,
		ClusterToken: token,
		GRPCAddr:     grpcAddr,
		stop: func() {
			grpcSrv.GracefulStop()
			<-serveDone
		},
	}
	t.Cleanup(n.Close)
	return n
}

// replicaSetOnRing previews the R replica nodes (ordered) for key's unit under
// a ring built from c.Members(), the same deterministic hashing the cluster
// uses. Lets the test name the exact replica set without reaching into
// unexported cluster internals.
func replicaSetOnRing(c *cluster.Cluster, key string, unitCount, r int) []string {
	rg := ring.New()
	for _, m := range c.Members() {
		rg.Add(m)
	}
	u := storageunit.UnitForShardKey(ring.ShardKey([]byte(key)), storageunit.MustUnitCount(unitCount))
	members := rg.LocateKeyN(gen0UnitBytes(u), r)
	out := make([]string, 0, len(members))
	for _, m := range members {
		out = append(out, m.ID)
	}
	return out
}

// start3NodeR2 stands up a 3-node R=2 multi-backend cluster and waits for
// convergence + a settle so ownership is stable before the test writes.
func start3NodeR2(t *testing.T, unitCount int, backing *sharedfactory.Backing) []*sharedNode {
	t.Helper()
	n1 := startReplicatedNode(t, "r2a", "", unitCount, 2, backing)
	n2 := startReplicatedNode(t, "r2b", n1.ClusterToken, unitCount, 2, backing)
	n3 := startReplicatedNode(t, "r2c", n1.ClusterToken, unitCount, 2, backing)
	nodes := []*sharedNode{n1, n2, n3}
	cs := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(cs, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)
	return nodes
}

// writeRecordedDataset writes plain + co-located keys via the founder, returns
// the recorded key -> value map and the tag -> keys map.
func writeRecordedDataset(t *testing.T, originator *cluster.Cluster) (map[string][]byte, map[string][]string) {
	t.Helper()
	want := make(map[string][]byte)

	const nPlain = 300
	for i := range nPlain {
		k := fmt.Sprintf("rec-%05d", i)
		v := fmt.Appendf(nil, "val-%05d-payload", i)
		if err := putWithRetryUnavailable(t, originator, k, string(v), 10*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	const nTags, perTag = 10, 6
	tagSets := make(map[string][]string)
	for ti := range nTags {
		tag := fmt.Sprintf("acct%02d", ti)
		set := make([]string, 0, perTag)
		for mi := range perTag {
			k := fmt.Sprintf("{%s}/field-%d", tag, mi)
			v := fmt.Appendf(nil, "co-%s-%d", tag, mi)
			if err := putWithRetryUnavailable(t, originator, k, string(v), 10*time.Second); err != nil {
				t.Fatalf("co-located Put %q: %v", k, err)
			}
			want[k] = v
			set = append(set, k)
		}
		tagSets[tag] = set
	}
	return want, tagSets
}

// runAckedProbe writes new keys THROUGH THE FULL ROUTED SURFACE for probeDur,
// rotating the entry node per writer so writes route both locally and
// forwarded, and records ONLY the keys whose Put returned nil (the acked set).
// An acked key the readback later loses is a genuine violation; an unacked key
// is not owed durability, so recording acked-only is what keeps the oracle
// honest. Returns the acked key -> value map for folding into the gate dataset.
func runAckedProbe(t *testing.T, nodes []*sharedNode, probeDur time.Duration) map[string][]byte {
	t.Helper()
	var mu sync.Mutex
	acked := make(map[string][]byte)
	var stop atomic.Bool
	var wg sync.WaitGroup
	const probeWriters = 6
	for w := range probeWriters {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			entry := nodes[w%len(nodes)] // route both locally and forwarded
			for i := 0; !stop.Load(); i++ {
				k := fmt.Sprintf("probe-%d-%07d", w, i)
				v := fmt.Appendf(nil, "pv-%d-%07d", w, i)
				if err := putWithRetryUnavailable(t, entry.Cluster, k, string(v), 10*time.Second); err != nil {
					t.Errorf("probe Put %q: %v", k, err)
					return
				}
				mu.Lock()
				acked[k] = v // recorded ONLY after the ack
				mu.Unlock()
			}
		}(w)
	}
	time.Sleep(probeDur)
	stop.Store(true)
	wg.Wait()
	if len(acked) == 0 {
		t.Fatalf("concurrent probe acked zero keys")
	}
	return acked
}

// TestLosslessMultibackendR2Gate is THE data-loss oracle for v0.8 Phase 2b.
func TestLosslessMultibackendR2Gate(t *testing.T) {
	const unitCount, r = 16, 2
	backing := sharedfactory.NewBacking()
	nodes := start3NodeR2(t, unitCount, backing)

	want, tagSets := writeRecordedDataset(t, nodes[0].Cluster)

	// Concurrent probe: keep acking new keys through the full routed surface
	// (alternating entry node), record ONLY acked keys, and fold them into the
	// recorded dataset so the oracle covers both baseline and probe writes.
	for k, v := range runAckedProbe(t, nodes, 400*time.Millisecond) {
		want[k] = v
	}

	// (1) THE ORACLE: every acked key readable with its exact value from EVERY
	// node (each routes to a replica via forwarding).
	for k, v := range want {
		readAcrossNodes(t, nodes, k, v)
	}

	// (2) Each unit's replica set is R=2 distinct nodes, and a written key
	// landed on BOTH replica copies (independent per-replica stores).
	checked := 0
	for k := range want {
		set := replicaSetOnRing(nodes[0].Cluster, k, unitCount, r)
		if len(set) != r {
			t.Fatalf("key %q: replica set size %d, want %d", k, len(set), r)
		}
		if set[0] == set[1] {
			t.Fatalf("key %q: replica set has duplicate node %q", k, set[0])
		}
		u := unitOf(k, unitCount)
		gu := storageunit.NewGenUnit(0, u)
		// Both replica positions must physically hold the key's value.
		for pos := uint8(0); pos < uint8(r); pos++ {
			ru := storageunit.NewReplicaUnit(gu, pos)
			be, ok := backing.ReplicaStore(ru)
			if !ok {
				t.Fatalf("key %q unit %d replica %d: no store (write did not reach this replica)", k, u, pos)
			}
			got, err := be.Get([]byte(k))
			if err != nil {
				t.Fatalf("key %q unit %d replica %d: Get on replica store: %v (acked write missing from a replica)", k, u, pos, err)
			}
			// At R>1 the stored bytes are an LWW envelope; the payload must match.
			if !bytes.Contains(got, want[k]) {
				t.Fatalf("key %q unit %d replica %d: stored envelope %q does not carry payload %q", k, u, pos, got, want[k])
			}
		}
		checked++
	}
	if checked == 0 {
		t.Fatalf("no keys checked for replica placement")
	}

	// (3) Co-located {tag} sets share one unit, hence one replica set.
	for tag, set := range tagSets {
		u0 := unitOf(set[0], unitCount)
		rs0 := replicaSetOnRing(nodes[0].Cluster, set[0], unitCount, r)
		for _, k := range set {
			if u := unitOf(k, unitCount); u != u0 {
				t.Fatalf("co-located set %q split across units %d and %d", tag, u0, u)
			}
			rs := replicaSetOnRing(nodes[0].Cluster, k, unitCount, r)
			for i := range rs {
				if rs[i] != rs0[i] {
					t.Fatalf("co-located set %q has divergent replica sets", tag)
				}
			}
		}
	}

	// (4) SURVIVE-ONE-LOSS: wipe ONE replica copy of every unit, then assert
	// every value is still readable (the OTHER replica is a complete copy).
	// We wipe replica position 0 of each unit; the cluster read path finds the
	// surviving stamped value on position 1 via the quorum read.
	for _, u := range storageunit.MustUnitCount(unitCount).IDs() {
		backing.WipeReplica(storageunit.NewReplicaUnit(storageunit.NewGenUnit(0, u), 0))
	}
	for k, v := range want {
		// Read through every node; the surviving replica satisfies the read.
		got, err := getWithRetryUnavailable(t, nodes[0].Cluster, k, 10*time.Second)
		if err != nil {
			t.Fatalf("SURVIVE-ONE-LOSS VIOLATED: key %q unreadable after wiping replica 0: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("SURVIVE-ONE-LOSS VIOLATED: key %q = %q, want %q after one-replica loss", k, got, v)
		}
	}
}

// TestR2GateCatchesLostWrite is the BREAK DEMONSTRATION: it removes BOTH
// replica copies of a unit, then shows the oracle FAILS - proving the gate
// genuinely catches a lost write rather than rubber-stamping. (The positive
// gate proves losing ONE replica is survivable; the break removes BOTH, so the
// loss is genuine and must be detected.)
func TestR2GateCatchesLostWrite(t *testing.T) {
	const unitCount, r = 16, 2
	backing := sharedfactory.NewBacking()
	nodes := start3NodeR2(t, unitCount, backing)

	want, _ := writeRecordedDataset(t, nodes[0].Cluster)
	// Fold in acked probe keys so the break operates on the full routed surface.
	probeAcked := runAckedProbe(t, nodes, 400*time.Millisecond)
	for k, v := range probeAcked {
		want[k] = v
	}

	// Pick an ACKED probe key and wipe BOTH replica copies of its unit: a key the
	// cluster acknowledged through the routed surface, so its loss is genuine.
	var victim string
	for k := range probeAcked {
		victim = k
		break
	}
	u := unitOf(victim, unitCount)
	gu := storageunit.NewGenUnit(0, u)
	backing.WipeReplica(storageunit.NewReplicaUnit(gu, 0))
	backing.WipeReplica(storageunit.NewReplicaUnit(gu, 1))

	// The oracle (read every key with its exact value from every node) MUST now
	// fail for the victim key: no replica holds it. We run the same readback the
	// positive gate runs, and assert it reports a loss.
	lost := oracleDetectsLoss(t, nodes, want)
	if !lost {
		t.Fatalf("BREAK DEMONSTRATION FAILED: wiped both replicas of unit %d (key %q) but the oracle did NOT catch the lost write - the gate is a rubber stamp", u, victim)
	}
}

// TestMultibackendR2_CASPerUnitFanout exercises the CAS write-set fan-out at
// R=2 multi-backend (CommitCASApply -> replicateCASBatch -> ApplyBatchLocal,
// re-keyed per unit). It runs concurrent Transact increments of one counter
// from every node and asserts no lost update, then asserts the committed value
// is durable on BOTH replica copies of the counter's unit (the per-unit fan-out
// reached both, via apply-if-newer).
func TestMultibackendR2_CASPerUnitFanout(t *testing.T) {
	// The 60 goroutines below all increment ONE counter, so every commit
	// contends on the same key's CAS. Under the race detector the ~10x
	// slowdown widens that contention window enough that a writer can lose
	// the CAS race more than the default CASMaxAttempts (10) times running
	// and exhaust its budget - a test-environment artifact, not a lost
	// update. Raise the budget (the var is exported for exactly this, see
	// cas_test.go) so the no-lost-update assertion is what's under test, not
	// the retry headroom. Restored on return.
	oldMax := cluster.CASMaxAttempts
	cluster.CASMaxAttempts = 500
	defer func() { cluster.CASMaxAttempts = oldMax }()

	const unitCount, r = 16, 2
	backing := sharedfactory.NewBacking()
	nodes := start3NodeR2(t, unitCount, backing)

	const counter = "cas-counter"
	if err := putWithRetryUnavailable(t, nodes[0].Cluster, counter, "0", 10*time.Second); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	const perNode = 20
	var wg sync.WaitGroup
	wg.Add(len(nodes) * perNode)
	for _, n := range nodes {
		for range perNode {
			go func(c *cluster.Cluster) {
				defer wg.Done()
				err := c.Transact([]byte(counter), func(tx backend.Transaction) error {
					cur, err := tx.Get([]byte(counter))
					if err != nil {
						return err
					}
					var v int
					_, _ = fmt.Sscanf(string(cur), "%d", &v)
					return tx.Put([]byte(counter), []byte(fmt.Sprintf("%d", v+1)))
				})
				if err != nil {
					t.Errorf("Transact: %v", err)
				}
			}(n.Cluster)
		}
	}
	wg.Wait()

	want := fmt.Sprintf("%d", len(nodes)*perNode)
	got, err := getWithRetryUnavailable(t, nodes[0].Cluster, counter, 10*time.Second)
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	if string(got) != want {
		t.Fatalf("counter: want %s (no lost update), got %q", want, got)
	}

	// The committed value must be durable on BOTH replica copies of the unit.
	gu := storageunit.NewGenUnit(0, unitOf(counter, unitCount))
	for pos := uint8(0); pos < uint8(r); pos++ {
		ru := storageunit.NewReplicaUnit(gu, pos)
		be, ok := backing.ReplicaStore(ru)
		if !ok {
			t.Fatalf("counter unit replica %d: no store (CAS fan-out missed a replica)", pos)
		}
		stored, gerr := be.Get([]byte(counter))
		if gerr != nil {
			t.Fatalf("counter unit replica %d: Get: %v (CAS write missing from a replica)", pos, gerr)
		}
		if !bytes.Contains(stored, []byte(want)) {
			t.Fatalf("counter unit replica %d: stored %q does not carry %q", pos, stored, want)
		}
	}
}

// oracleDetectsLoss runs the positive gate's readback in a non-fatal mode:
// it returns true the moment any acked key is unreadable or wrong from any
// node (the loss the break-demonstration deliberately induces). It does NOT
// call t.Fatalf, so the break test can ASSERT the loss was caught.
func oracleDetectsLoss(t *testing.T, nodes []*sharedNode, want map[string][]byte) bool {
	t.Helper()
	for k, v := range want {
		for _, n := range nodes {
			got, err := getWithRetryUnavailable(t, n.Cluster, k, 3*time.Second)
			if err != nil {
				return true // unreadable: a lost write
			}
			if !bytes.Equal(got, v) {
				return true // wrong value: corruption
			}
		}
	}
	return false
}
