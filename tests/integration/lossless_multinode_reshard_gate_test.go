package integration

// THE LOSSLESS MULTI-NODE R=1 RESHARD GATE (the data-loss oracle for the
// arbiter-driven multi-node doubling).
//
// This is the multi-node analogue of the single-node Phase 4 gate
// (lossless_reshard_gate_test.go). The safety net the multi-node
// doubling-resharder is built to satisfy:
//
//	NO ACKED WRITE IS LOST DURING A MULTI-NODE RESHARD.
//
// A cross-node doubling cannot be made safe by the plain single-node
// node-LOCAL write-pause: each node advances its generation independently, so
// a concurrent write could route at the OLD generation on one node while
// another has already flipped, and be lost. The arbiter-driven R=1 reshard
// removes that hazard with PARENT-ANCHORED PLACEMENT plus the per-unit
// pause-held cut-over (pkg/cluster/multibackend_reshard_r1.go): while a split
// is in flight, every node forwards a splitting unit's key-space to the SAME
// node (its gen-g owner), and that owner's clear+copy+flip under the unit's
// write-pause WRITE side is the single, proven cut-over boundary. Cross-node
// ordering comes from the durable cut-over markers (one per unit, create-only
// CAS); each node finalizes (retires parents + advances its generation) only
// once it observes EVERY unit's marker. Reshard() on a multi-node cluster
// delegates to this flow: it retargets the shared CAS arbiter and waits for
// the local generation to advance.
//
// The gate below stands up an in-process 3-node R=1 cluster (gRPC-registered +
// forwarding, sharing ONE shared-backing factory AND one MemConditionalStore)
// and asserts the full set of properties across a live delegated reshard, WITH
// A CONCURRENT WRITER hammering acked keys through the whole reshard:
//
//  1. THE ORACLE (zero loss). Every key written before the reshard AND every
//     key the concurrent probe got an ACK for during the reshard is still
//     readable, with its EXACT recorded value, from ANY node, after the
//     cluster reaches gen g+1 (2N units). Hundreds of baseline keys including
//     co-located {tag} sets, plus a stream of probe writes (a write refused by
//     a cut-over pause / acquiring window gets the retryable error and is
//     retried, so it is only counted acked once Put returns nil).
//  2. GENERATION ADVANCED + PARTITIONED. The cluster reached gen g+1 (2N
//     units): every gen-g unit store was retired and every gen-(g+1) child
//     store exists in the shared backing, and the 2N units are correctly
//     partitioned across the nodes by the redistribution (each node mounts
//     exactly the gen-(g+1) units the ring assigns it; their union is all 2N
//     units; no node still mounts a gen-g unit).
//  3. CO-LOCATION SURVIVES. Every key in a {tag} set lands on ONE gen-(g+1)
//     unit and is served by ONE owner node after the bisect (a co-located set
//     is never split), because the whole set shares one ShardHash and one
//     ShardHash bisects to exactly one child.
//  4. READS RETRYABLE-AVAILABLE ACROSS THE WHOLE RESHARD. Generations stagger
//     across nodes during the split (one node finalized, a peer still in
//     flight), so a read may briefly hit the retryable acquiring-window /
//     ring-refresh error; a reader running through the WHOLE reshard with the
//     standard retry must always eventually read the correct value
//     (retryable-available, never lost, never wrong).
//  5. BREAK DEMONSTRATION (TestMultiNodeReshardGateCatchesLostWrite, below).
//     Config.TestingForceUncleanReshard flips + retires parents WITHOUT
//     copying their data into the children; the oracle must FAIL (catch the
//     loss), proving the gate is not a rubber stamp.
//
// If this test ever passes while a write is silently lost across a multi-node
// reshard, the gate is broken; the break-demonstration keeps it honest.

import (
	"bytes"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// genUnitOwnerAmong previews which node owns a generation-qualified unit on the
// ring built from members - the same deterministic genUnitBytes encoding the
// cluster routes units with. Used to assert the 2N gen-(g+1) units partition
// correctly across the nodes after the redistribution.
func genUnitOwnerAmong(members []ring.Member, gu storageunit.GenUnit) string {
	r := ring.New()
	for _, m := range members {
		r.Add(m)
	}
	return r.LocateKey(genUnitBytesForTest(gu)).ID
}

// nodeMountsExpectedGen1 waits until node n's handle mounts EXACTLY the gen-1
// units the ring assigns it (and no gen-0 unit remains). Returns the set it
// settled on for the union assertion. The redistribution is asynchronous (the
// reconcile cadence after finalize), so we poll.
func nodeMountsExpectedGen1(t *testing.T, n *sharedNode, members []ring.Member, newCount int) map[storageunit.UnitID]struct{} {
	t.Helper()
	newUC := storageunit.MustUnitCount(newCount)
	expected := make(map[storageunit.UnitID]struct{})
	for _, u := range newUC.IDs() {
		gu := storageunit.NewGenUnit(1, u)
		if genUnitOwnerAmong(members, gu) == n.ID {
			expected[u] = struct{}{}
		}
	}
	if !waitUntil(12*time.Second, func() bool {
		open := n.Handle.OpenUnits()
		got := make(map[storageunit.UnitID]struct{})
		for _, m := range open {
			if m.Unit().Gen != 1 {
				return false // a gen-0 unit still mounted: not settled / not retired
			}
			got[m.Unit().ID] = struct{}{}
		}
		if len(got) != len(expected) {
			return false
		}
		for u := range expected {
			if _, ok := got[u]; !ok {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("node %s did not settle to its expected gen-1 unit set: want %v, open=%v", n.ID, expected, n.Handle.OpenUnits())
	}
	return expected
}

// getWithRetryReshard is the reshard-aware READ retry: like
// getWithRetryUnavailable it retries codes.Unavailable (acquiring / cut-over
// windows), and it ALSO retries codes.FailedPrecondition - the forwarding
// loop-guard a read hits during the staggered finalize window, when the
// originating node (still in flight, parent-anchored) forwards to a node that
// already finalized and re-keyed. A real SDK refreshes its ring view and
// retries; the window closes as the origin observes the markers and finalizes.
func getWithRetryReshard(t *testing.T, c *cluster.Cluster, key string, timeout time.Duration) ([]byte, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got, err := c.Get([]byte(key))
		if err == nil {
			return got, nil
		}
		code := status.Code(err)
		retryable := code == codes.Unavailable || code == codes.FailedPrecondition
		if !retryable || time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startR1StoreGateCluster stands up a 3-node R=1 cluster over one shared
// backing + one shared MemConditionalStore (the arbiter-driven reshard's
// agreement store), with an optional Config mutator, and converges it.
func startR1StoreGateCluster(t *testing.T, idPrefix string, unitCount int, backing *sharedfactory.Backing, store storageunit.ConditionalStore, mutate func(*cluster.Config)) []*sharedNode {
	t.Helper()
	mk := func(cfg *cluster.Config) {
		cfg.ConditionalStore = store
		if mutate != nil {
			mutate(cfg)
		}
	}
	n1 := startReplicatedNodeCfg(t, idPrefix+"1", "", unitCount, 1, backing, mk)
	n2 := startReplicatedNodeCfg(t, idPrefix+"2", n1.ClusterToken, unitCount, 1, backing, mk)
	n3 := startReplicatedNodeCfg(t, idPrefix+"3", n1.ClusterToken, unitCount, 1, backing, mk)
	nodes := []*sharedNode{n1, n2, n3}
	cs := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(cs, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	// Let the join-driven reconcile settle so ownership is stable before the
	// reshard begins.
	time.Sleep(900 * time.Millisecond)
	return nodes
}

// TestLosslessMultiNodeReshardGate is THE data-loss oracle for the multi-node
// R=1 arbiter-driven reshard. See the file header for the properties it pins.
// It stands up a 3-node shared-backing + shared-store cluster, writes a
// recorded dataset spanning every unit plus co-located {tag} sets, starts a
// concurrent probe that keeps acking new keys through the full routed surface,
// triggers a delegated Reshard() that doubles N -> 2N cluster-wide, and
// asserts: the whole dataset (baseline + probe) survived with exact values
// from every node, the cluster advanced to gen 1 with the 2N units partitioned
// correctly across nodes, co-location held, and reads stayed
// retryable-available throughout.
func TestLosslessMultiNodeReshardGate(t *testing.T) {
	const unitCount = 8 // N; doubles to 16
	const newCount = 2 * unitCount
	backing := sharedfactory.NewBacking()
	store := storageunit.NewMemConditionalStore()

	// === Step 1a: stand up 3 nodes, R=1 multi-backend, sharing one backing +
	// one conditional store. ===
	nodes := startR1StoreGateCluster(t, "mngate", unitCount, backing, store, nil)
	n1, n2 := nodes[0], nodes[1]

	// === Step 1b: write a known dataset spanning MANY units, recording
	// key -> value. plainKeys spread across all N units; coLocated {tag} sets
	// each share one tag so they all land on ONE unit. ===
	want := make(map[string][]byte)

	const nPlain = 400
	for i := range nPlain {
		k := fmt.Sprintf("rec-%05d", i)
		v := fmt.Appendf(nil, "val-%05d-payload", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 10*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	// Co-located {tag} sets: 12 tags x 8 members each. Every member of a tag
	// MUST share a unit before AND after the bisect.
	const nTags, perTag = 12, 8
	tagSets := make(map[string][]string)
	for ti := range nTags {
		tag := fmt.Sprintf("acct%02d", ti)
		set := make([]string, 0, perTag)
		for mi := range perTag {
			k := fmt.Sprintf("{%s}/field-%d", tag, mi)
			v := fmt.Appendf(nil, "co-%s-%d", tag, mi)
			if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 10*time.Second); err != nil {
				t.Fatalf("co-located Put %q: %v", k, err)
			}
			want[k] = v
			set = append(set, k)
		}
		tagSets[tag] = set
	}

	// Pre-condition: every co-located set is on ONE gen-0 unit, and the dataset
	// spans the WHOLE unit space (so the bisect is exercised on every unit).
	oldUC := storageunit.MustUnitCount(unitCount)
	tagUnit := make(map[string]storageunit.UnitID, nTags)
	for tag, set := range tagSets {
		u0 := storageunit.UnitForShardKey(ring.ShardKey([]byte(set[0])), oldUC)
		for _, k := range set {
			if u := storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), oldUC); u != u0 {
				t.Fatalf("precondition: co-located set %q split across units %d and %d", tag, u0, u)
			}
		}
		tagUnit[tag] = u0
	}
	spanned := make(map[storageunit.UnitID]struct{})
	for k := range want {
		spanned[storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), oldUC)] = struct{}{}
	}
	if len(spanned) != unitCount {
		t.Fatalf("precondition: baseline dataset spans only %d/%d units; widen the key set", len(spanned), unitCount)
	}

	// Sanity: everything readable from every node BEFORE the reshard.
	for k, v := range want {
		readAcrossNodes(t, nodes, k, v)
	}

	// === Step 2: start a CONCURRENT background probe. It writes THROUGH the full
	// routed surface (alternating entry node so writes route both locally and
	// forwarded), retries the reshard-window retryable errors, and records ONLY
	// keys it got an ACK for (Put returned nil). ===
	var probeMu sync.Mutex
	probeAcked := make(map[string][]byte)
	var stop atomic.Bool
	var wg sync.WaitGroup
	const probeWriters = 6
	// Each probe writer closes its own ready channel after its FIRST acked write
	// so the barrier below can hold the reshard until every writer is in flight.
	probeReady := make([]chan struct{}, probeWriters)
	for w := range probeReady {
		probeReady[w] = make(chan struct{})
	}
	for w := range probeWriters {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			entry := nodes[w%len(nodes)]
			i := 0
			for !stop.Load() {
				k := fmt.Sprintf("probe-%d-%07d", w, i)
				v := fmt.Appendf(nil, "pv-%d-%07d", w, i)
				// Retry the cut-over + handoff-window retryable signals; any
				// other error is a real failure. Record only after Put returns nil.
				if err := putWithRetryReshard(t, entry.Cluster, k, string(v), 15*time.Second); err != nil {
					t.Errorf("probe Put %q during reshard: %v", k, err)
					return
				}
				probeMu.Lock()
				probeAcked[k] = v
				probeMu.Unlock()
				if i == 0 {
					close(probeReady[w])
				}
				i++
			}
		}(w)
	}
	// BARRIER: hold the reshard until every probe writer has provably landed an
	// acked write through its entry node. The writers keep looping through the
	// reshard that follows, so the acked set spans it by construction rather
	// than by timing luck.
	awaitFirstAckBarrier(t, probeReady, 30*time.Second, &stop, &wg)

	// === Step 2b (req 4, the staggered-window read model): RETRYABLE-AVAILABLE
	// ACROSS THE WHOLE RESHARD. Generations stagger across nodes during the
	// split (a peer-mid-flight forwards to a node that already finalized, which
	// may briefly return the retryable acquiring-window error or the
	// ring-refresh loop-guard). A reader running through the whole reshard
	// must, WITH the standard retry, always eventually read the correct value -
	// never a permanent failure, never a wrong value. We rotate the entry node
	// so the read routes both locally and forwarded. ===
	const readProbeKey = "rec-00000"
	readProbeVal := want[readProbeKey]
	var readViolation atomic.Value // stores a string describing the first violation
	var readWg sync.WaitGroup
	readWg.Go(func() {
		i := 0
		for !stop.Load() {
			entry := nodes[i%len(nodes)]
			got, err := getWithRetryReshard(t, entry.Cluster, readProbeKey, 8*time.Second)
			if err != nil {
				readViolation.CompareAndSwap(nil, fmt.Sprintf("read of %q via %s failed across the reshard even with retry: %v", readProbeKey, entry.ID, err))
				return
			}
			if !bytes.Equal(got, readProbeVal) {
				readViolation.CompareAndSwap(nil, fmt.Sprintf("read of %q via %s returned %q, want %q (wrong value mid-reshard)", readProbeKey, entry.ID, got, readProbeVal))
				return
			}
			i++
			time.Sleep(2 * time.Millisecond)
		}
	})

	// === Step 3: trigger the delegated multi-node reshard on n1. It retargets
	// the shared arbiter; the reconcile pump (standing in for the production
	// cadence) drives every node's share of the split to convergence. ===
	stopPump := startReconcilePump([]*cluster.Cluster{nodes[0].Cluster, nodes[1].Cluster, nodes[2].Cluster})
	defer stopPump()
	if err := n1.Cluster.Reshard(); err != nil {
		stop.Store(true)
		wg.Wait()
		readWg.Wait()
		t.Fatalf("multi-node Reshard (delegated, n1): %v", err)
	}

	// === Step 4: keep writing briefly past the local convergence (peers may
	// still be finalizing), then stop the probe + reader and let the
	// redistribution settle. ===
	time.Sleep(400 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	readWg.Wait()
	time.Sleep(900 * time.Millisecond)

	probeMu.Lock()
	if len(probeAcked) < probeWriters {
		probeMu.Unlock()
		t.Fatalf("only %d probe writes acked; the concurrent writer did not run", len(probeAcked))
	}
	t.Logf("concurrent probe acked %d keys across the multi-node reshard", len(probeAcked))
	probeSnapshot := make(map[string][]byte, len(probeAcked))
	maps.Copy(probeSnapshot, probeAcked)
	probeMu.Unlock()

	// === ASSERTION (req 4): a reader running across the WHOLE reshard, WITH
	// the standard retry, never hit a permanent failure or a wrong value. ===
	if v := readViolation.Load(); v != nil {
		t.Fatalf("READ-AVAILABILITY VIOLATED: %s (reads must be retryable-available across the whole reshard)", v.(string))
	}

	// === ASSERTION (req 1, THE ORACLE): EVERY baseline key AND every acked probe
	// key is readable with its EXACT value, from EVERY node. ZERO loss. ===
	assertOracle := func(label string, ds map[string][]byte) {
		for k, v := range ds {
			for _, n := range nodes {
				got, err := getWithRetryUnavailable(t, n.Cluster, k, 12*time.Second)
				if err != nil {
					t.Fatalf("ORACLE VIOLATED (%s): key %q unreadable via %s after reshard: %v (NO ACKED WRITE LOST)", label, k, n.ID, err)
				}
				if !bytes.Equal(got, v) {
					t.Fatalf("ORACLE VIOLATED (%s): key %q via %s = %q, want %q (value corrupted across reshard)", label, k, n.ID, got, v)
				}
			}
		}
	}
	assertOracle("baseline", want)
	assertOracle("probe", probeSnapshot)

	// === ASSERTION (req 2): GENERATION ADVANCED. Every node reports gen 1 at
	// 2N units, and every gen-(g+1) child store exists in the shared backing
	// and holds its keys. ===
	if !waitUntil(15*time.Second, func() bool { return allAtGeneration(nodes, 1, uint32(newCount)) }) {
		t.Fatalf("GENERATION VIOLATED: not every node reached gen 1 / %d units: %s", newCount, genStatesOf(nodes))
	}
	newUC := storageunit.MustUnitCount(newCount)
	for k, v := range want {
		newU := storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), newUC)
		newGU := storageunit.NewGenUnit(1, newU)
		found, ok := sharedStoreHas(backing, newGU, k, v)
		if !ok {
			t.Fatalf("GENERATION VIOLATED: key %q - shared backing has no gen-1 store for child %s (data did not move to the new generation)", k, newGU)
		}
		if !found {
			t.Fatalf("GENERATION VIOLATED: key %q absent (or wrong) in its gen-1 child store %s (data did not physically move)", k, newGU)
		}
	}

	// === ASSERTION (req 2): PARTITIONED across nodes by the redistribution.
	// Each node settles to EXACTLY the gen-1 units the ring assigns it; their
	// union is all 2N gen-1 units; no node still mounts a gen-0 unit. ===
	members := n1.Cluster.Members()
	union := make(map[storageunit.UnitID]struct{})
	for _, n := range nodes {
		got := nodeMountsExpectedGen1(t, n, members, newCount)
		for u := range got {
			if _, dup := union[u]; dup {
				t.Fatalf("PARTITION VIOLATED: gen-1 unit %d mounted on more than one node (overlap)", u)
			}
			union[u] = struct{}{}
		}
	}
	if len(union) != newCount {
		t.Fatalf("PARTITION VIOLATED: the 2N gen-1 units do not cover the whole space: got %d distinct, want %d", len(union), newCount)
	}

	// === ASSERTION (req 2): ROUTING advanced to gen 1. A fresh key whose unit at
	// 2N is >= N (a unit that does not exist before the doubling) must round-trip
	// through every node and physically land in its gen-1 store. ===
	var highKey string
	for i := range 1_000_000 {
		k := fmt.Sprintf("hi-%d", i)
		u := storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), newUC)
		if uint32(u) >= unitCount {
			highKey = k
			break
		}
	}
	if highKey == "" {
		t.Fatal("could not find a key mapping to a doubled-only unit (>= N at 2N)")
	}
	freshVal := []byte("served-at-gen-1")
	if err := putWithRetryUnavailable(t, n2.Cluster, highKey, string(freshVal), 12*time.Second); err != nil {
		t.Fatalf("ROUTING: fresh Put of doubled-only-unit key %q after reshard: %v", highKey, err)
	}
	readAcrossNodes(t, nodes, highKey, freshVal)
	highU := storageunit.UnitForShardKey(ring.ShardKey([]byte(highKey)), newUC)
	highGU := storageunit.NewGenUnit(1, highU)
	if found, ok := sharedStoreHas(backing, highGU, highKey, freshVal); !ok || !found {
		t.Fatalf("ROUTING VIOLATED: doubled-only-unit key %q did not land in its gen-1 unit %s: ok=%v found=%v", highKey, highGU, ok, found)
	}

	// === ASSERTION (req 3): CO-LOCATION SURVIVES. Each {tag} set is still
	// entirely on ONE gen-1 unit (a set shares one ShardHash, which bisects to
	// exactly one child), that child is a bisect of the recorded gen-0 unit, AND
	// the whole set routes to ONE owner node on the live ring. ===
	for tag, set := range tagSets {
		childOf := childUnitForKey(t, set[0], unitCount)
		owners := make(map[string]struct{})
		for _, k := range set {
			if c := childUnitForKey(t, k, unitCount); c != childOf {
				t.Fatalf("CO-LOCATION VIOLATED: set %q split across gen-1 children %d and %d after bisect", tag, childOf, c)
			}
			owners[genUnitOwnerAmong(members, storageunit.NewGenUnit(1, childOf))] = struct{}{}
		}
		parent := tagUnit[tag]
		if childOf != parent && childOf != parent+storageunit.UnitID(unitCount) {
			t.Fatalf("CO-LOCATION VIOLATED: set %q child %d is not a bisect of recorded gen-0 unit %d", tag, childOf, parent)
		}
		if len(owners) != 1 {
			t.Fatalf("CO-LOCATION VIOLATED: set %q routes to %d owners %v after reshard (must be exactly one)", tag, len(owners), owners)
		}
	}
}

// --- BREAK DEMONSTRATION (req 5) ----------------------------------------

// TestMultiNodeReshardGateCatchesLostWrite is the BREAK DEMONSTRATION for the
// multi-node gate. Config.TestingForceUncleanReshard disables the R=1 drive's
// copy passes: units flip + publish their durable markers and finalize retires
// the parents WITHOUT the children ever receiving the parents' data - exactly
// the "flip without capturing acked writes" failure the copy machinery exists
// to prevent. The oracle must CATCH the resulting loss. It PASSES iff the
// broken reshard is caught; if the break produced no detectable loss, the test
// fails loudly - that would mean the oracle is blind to a write lost to a
// broken multi-node reshard.
func TestMultiNodeReshardGateCatchesLostWrite(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	store := storageunit.NewMemConditionalStore()
	nodes := startR1StoreGateCluster(t, "mnbrk", unitCount, backing, store, func(cfg *cluster.Config) {
		cfg.TestingForceUncleanReshard = true
	})
	n1 := nodes[0]

	// Seed so every unit has gen-g data the (skipped) bisect copy should have
	// carried into the children.
	want := make(map[string][]byte)
	for i := range 200 {
		k := fmt.Sprintf("seed-%05d", i)
		v := fmt.Appendf(nil, "sv-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 10*time.Second); err != nil {
			t.Fatalf("seed Put %q: %v", k, err)
		}
		want[k] = v
	}

	// Delegated reshard with the copy disabled: it CONVERGES (markers publish,
	// parents retire, the generation advances) but the children are empty.
	stopPump := startReconcilePump([]*cluster.Cluster{nodes[0].Cluster, nodes[1].Cluster, nodes[2].Cluster})
	defer stopPump()
	if err := n1.Cluster.Reshard(); err != nil {
		t.Fatalf("unclean Reshard should still converge (the break is silent): %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// THE ORACLE, in detection mode: the seeded keys were never copied into the
	// gen-1 children, so they must now be lost.
	lost := 0
	for k, v := range want {
		got, err := getWithRetryUnavailable(t, n1.Cluster, k, 5*time.Second)
		if err != nil || !bytes.Equal(got, v) {
			lost++
		}
	}
	if lost == 0 {
		t.Fatalf("BREAK NOT CAUGHT: an unclean reshard (no copy) lost no acked write - the oracle would rubber-stamp a broken multi-node reshard")
	}
	t.Logf("break demonstration: unclean reshard lost %d/%d acked keys; the oracle catches it", lost, len(want))
}
