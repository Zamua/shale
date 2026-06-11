package integration

// THE LOSSLESS MULTI-NODE RESHARD GATE TEST for v0.8 (the data-loss oracle for
// the cluster-wide-freeze barrier).
//
// This is the multi-node analogue of the single-node Phase 4 gate
// (lossless_reshard_gate_test.go). The safety net the whole multi-node
// doubling-resharder is built to satisfy:
//
//	NO ACKED WRITE IS LOST DURING A COORDINATED MULTI-NODE RESHARD.
//
// A cross-node doubling cannot be made safe by the single-node node-LOCAL
// write-pause: each node advances its generation independently, so a concurrent
// write could route at the OLD generation on one node while another has already
// FLIPPED, and be lost. The multi-node reshard removes that hazard with a brief
// cluster-wide WRITE-FREEZE: the coordinator (the node Reshard() is called on)
// drives a 4-phase barrier over every node - FREEZE (writes refused cluster-
// wide with a retryable error, reads continue) -> BISECT (each node statically
// copies its gen-g units into fresh gen-(g+1) children in the shared backing,
// no catch-up because the source is frozen) -> FLIP (every node atomically
// advances to gen g+1 and retires its gen-g units) -> RESUME (unfreeze; the 2N
// units redistribute across nodes by the reused Phase 3 lease handoff). Any
// phase failure / timeout / membership change ABORTS: every node unfreezes,
// discards half-built children, and stays at gen g.
//
// The gate below stands up an in-process MULTI-NODE cluster (2-3 nodes,
// gRPC-registered + forwarding, sharing ONE shared-backing factory) and asserts
// the full set of properties across a live coordinated reshard, WITH A
// CONCURRENT WRITER hammering acked keys through the whole reshard:
//
//  1. THE ORACLE (zero loss). Every key written before the reshard AND every
//     key the concurrent probe got an ACK for during the reshard is still
//     readable, with its EXACT recorded value, from ANY node, after the cluster
//     reaches gen g+1 (2N units). Hundreds of baseline keys including co-located
//     {tag} sets, plus a stream of probe writes (the freeze-window analogue of
//     Phase 3's concurrent-write probe: a write refused by the freeze gets the
//     retryable error and is retried, so it is only counted acked once Put
//     returns nil).
//  2. GENERATION ADVANCED + PARTITIONED. The cluster reached gen g+1 (2N units):
//     every gen-g unit store was retired and every gen-(g+1) child store exists
//     in the shared backing, and the 2N units are correctly partitioned across
//     the nodes by the Phase 3 redistribution (each node mounts exactly the
//     gen-(g+1) units the ring assigns it; their union is all 2N units; no node
//     still mounts a gen-g unit).
//  3. CO-LOCATION SURVIVES. Every key in a {tag} set lands on ONE gen-(g+1)
//     unit and is served by ONE owner node after the bisect (a co-located set is
//     never split), because the whole set shares one ShardHash and one ShardHash
//     bisects to exactly one child.
//  4. READS AVAILABLE (strict during FREEZE/BISECT, retryable across FLIP). A
//     read issued while the cluster is frozen mid-reshard succeeds STRICTLY
//     (no retry), served from the live gen-g units. Across the staggered FLIP
//     window generations differ across nodes, so a read may briefly hit the
//     retryable acquiring-window / ring-refresh error; a reader running through
//     the WHOLE reshard with the standard retry must always eventually read the
//     correct value (retryable-available, never lost). Writes are refused with
//     the retryable error during the freeze and resume after.
//  5. BREAK DEMONSTRATION (TestMultiNodeReshardGateCatchesLostWrite, below). A
//     deliberately broken barrier - one node SKIPS the freeze, so a write slips
//     through at gen g into a gen-g unit that is then retired by FLIP without
//     ever being bisected - makes the oracle FAIL, proving the gate actually
//     catches a write lost to a violated barrier rather than rubber-stamping.
//  6. ABORT PATH (TestMultiNodeReshardAbortStaysAtGenG, below). A forced phase
//     failure ABORTs the reshard: the cluster stays at gen g, every node
//     unfreezes, and the whole dataset is intact + writable at the old
//     generation.
//
// If this test ever passes while a write is silently lost across a multi-node
// reshard, the gate is broken; the break-demonstration keeps it honest.

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
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

// nodeMountsGen1Units waits until node n's handle mounts EXACTLY the gen-1 units
// the ring assigns it (and no gen-0 unit remains). Returns the set it settled on
// for the union assertion. The redistribution is asynchronous (a debounced
// reconcile after RESUME), so we poll.
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
		for _, gu := range open {
			if gu.Gen != 1 {
				return false // a gen-0 unit still mounted: not settled / not retired
			}
			got[gu.ID] = struct{}{}
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

// TestLosslessMultiNodeReshardGate is THE data-loss oracle for the v0.8 multi-
// node cluster-wide-freeze reshard. See the file header for the properties it
// pins. It stands up a 3-node shared-backing cluster, writes a recorded dataset
// spanning every unit plus co-located {tag} sets, starts a concurrent probe that
// keeps acking new keys through the full routed surface, triggers a coordinator-
// driven Reshard() that doubles N -> 2N cluster-wide via the freeze barrier, and
// asserts: the whole dataset (baseline + probe) survived with exact values from
// every node, the cluster advanced to gen 1 with the 2N units partitioned
// correctly across nodes, co-location held, and reads stayed available during
// the freeze.
func TestLosslessMultiNodeReshardGate(t *testing.T) {
	const unitCount = 8 // N; doubles to 16
	const newCount = 2 * unitCount
	backing := sharedfactory.NewBacking()

	// === Step 1a: stand up 3 nodes, multi-backend, sharing one backing. ===
	n1 := startSharedNode(t, "mngate1", "", unitCount, backing)
	n2 := startSharedNode(t, "mngate2", n1.BindAddr, unitCount, backing)
	n3 := startSharedNode(t, "mngate3", n1.BindAddr, unitCount, backing)
	nodes := []*sharedNode{n1, n2, n3}
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(clusters, 3, 20*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	// Let the join-driven reconcile settle so ownership is stable before the
	// reshard snapshots the participant set.
	time.Sleep(900 * time.Millisecond)

	// === Step 1b: write a known dataset spanning MANY units, recording
	// key -> value. plainKeys spread across all N units; coLocated {tag} sets
	// each share one tag so they all land on ONE unit. ===
	want := make(map[string][]byte)

	const nPlain = 400
	for i := 0; i < nPlain; i++ {
		k := fmt.Sprintf("rec-%05d", i)
		v := []byte(fmt.Sprintf("val-%05d-payload", i))
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 10*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	// Co-located {tag} sets: 12 tags x 8 members each. Every member of a tag
	// MUST share a unit before AND after the bisect.
	const nTags, perTag = 12, 8
	tagSets := make(map[string][]string)
	for ti := 0; ti < nTags; ti++ {
		tag := fmt.Sprintf("acct%02d", ti)
		set := make([]string, 0, perTag)
		for mi := 0; mi < perTag; mi++ {
			k := fmt.Sprintf("{%s}/field-%d", tag, mi)
			v := []byte(fmt.Sprintf("co-%s-%d", tag, mi))
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
	// forwarded), retries the freeze-window retryable error, and records ONLY
	// keys it got an ACK for (Put returned nil). ===
	var probeMu sync.Mutex
	probeAcked := make(map[string][]byte)
	var stop atomic.Bool
	var wg sync.WaitGroup
	const probeWriters = 6
	for w := 0; w < probeWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			entry := nodes[w%len(nodes)]
			i := 0
			for !stop.Load() {
				k := fmt.Sprintf("probe-%d-%07d", w, i)
				v := []byte(fmt.Sprintf("pv-%d-%07d", w, i))
				// Retry the freeze-window + handoff-window retryable signals; any
				// other error is a real failure. Record only after Put returns nil.
				if err := putWithRetryReshard(t, entry.Cluster, k, string(v), 15*time.Second); err != nil {
					t.Errorf("probe Put %q during reshard: %v", k, err)
					return
				}
				probeMu.Lock()
				probeAcked[k] = v
				probeMu.Unlock()
				i++
			}
		}(w)
	}
	// Let the probe get going so writes are genuinely in flight when the freeze
	// barrier engages.
	time.Sleep(150 * time.Millisecond)

	// === Step 4 (req 6): READS AVAILABLE DURING THE FREEZE. Spin a reader that
	// keeps reading a known baseline key throughout the reshard and records
	// whether it ever saw a successful read while the cluster was frozen. The
	// freeze refuses WRITES (retryable) but never gates reads; a read mid-reshard
	// must succeed, served from the live gen-g units. ===
	const freezeProbeKey = "rec-00000"
	freezeProbeVal := want[freezeProbeKey]
	var sawFrozen atomic.Bool // observed at least one node frozen during the run
	var readOKWhileFrozen atomic.Bool
	var readWg sync.WaitGroup
	readWg.Add(1)
	go func() {
		defer readWg.Done()
		for !stop.Load() {
			frozen := n1.Cluster.TestingIsFrozen() || n2.Cluster.TestingIsFrozen() || n3.Cluster.TestingIsFrozen()
			if frozen {
				sawFrozen.Store(true)
				// Read DIRECTLY (no retry helper) so we are asserting the read
				// path is not gated by the freeze. Read from a node that is itself
				// frozen if possible, to prove its own read path serves.
				got, err := n1.Cluster.Get([]byte(freezeProbeKey))
				if err == nil && bytes.Equal(got, freezeProbeVal) {
					readOKWhileFrozen.Store(true)
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// === Step 4b (req 3, the FLIP-window read model): RETRYABLE-AVAILABLE ACROSS
	// THE WHOLE RESHARD. Reads are STRICTLY served during FREEZE/BISECT but only
	// RETRYABLE-available across the staggered FLIP window (a peer-at-g forwards a
	// read to a node-at-g+1 that may briefly return the retryable acquiring-window
	// error or the ring-refresh loop-guard). So a reader running through the whole
	// reshard must, WITH the standard retry, always eventually read the correct
	// value - never a permanent failure, never a wrong value. We rotate the entry
	// node so the read routes both locally and forwarded. A read that fails even
	// after the retry budget, or returns a wrong value, is a violation. ===
	var flipReadViolation atomic.Value // stores a string describing the first violation
	readWg.Add(1)
	go func() {
		defer readWg.Done()
		i := 0
		for !stop.Load() {
			entry := nodes[i%len(nodes)]
			// getWithRetryUnavailable retries codes.Unavailable; the brief
			// ring-refresh FailedPrecondition window self-heals as the ring
			// re-keys, so a generous timeout rides out both.
			got, err := getWithRetryUnavailable(t, entry.Cluster, freezeProbeKey, 8*time.Second)
			if err != nil {
				flipReadViolation.CompareAndSwap(nil, fmt.Sprintf("read of %q via %s failed across the reshard even with retry: %v", freezeProbeKey, entry.ID, err))
				return
			}
			if !bytes.Equal(got, freezeProbeVal) {
				flipReadViolation.CompareAndSwap(nil, fmt.Sprintf("read of %q via %s returned %q, want %q (wrong value mid-reshard)", freezeProbeKey, entry.ID, got, freezeProbeVal))
				return
			}
			i++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// === Step 3: trigger the coordinated multi-node reshard on n1 (coordinator).
	// It drives FREEZE -> BISECT -> FLIP -> RESUME across all three nodes. ===
	if err := n1.Cluster.Reshard(); err != nil {
		stop.Store(true)
		wg.Wait()
		readWg.Wait()
		t.Fatalf("multi-node Reshard (coordinator n1): %v", err)
	}

	// === Step 4: stop the probe + reader and let the redistribution settle. ===
	stop.Store(true)
	wg.Wait()
	readWg.Wait()
	// Let the post-RESUME redistribution reconcile settle so every gen-(g+1)
	// unit lands on its ring owner.
	time.Sleep(900 * time.Millisecond)

	probeMu.Lock()
	if len(probeAcked) < probeWriters {
		probeMu.Unlock()
		t.Fatalf("only %d probe writes acked; the concurrent writer did not run", len(probeAcked))
	}
	t.Logf("concurrent probe acked %d keys across the multi-node reshard", len(probeAcked))
	probeSnapshot := make(map[string][]byte, len(probeAcked))
	for k, v := range probeAcked {
		probeSnapshot[k] = v
	}
	probeMu.Unlock()

	// === ASSERTION (req 6): a read succeeded while the cluster was frozen. We
	// require the reader to have OBSERVED a freeze (otherwise the assertion is
	// vacuous); given a freeze was observed, at least one read must have served. ===
	if !sawFrozen.Load() {
		t.Fatal("reader never observed the cluster frozen during the reshard; the read-availability assertion is vacuous (the freeze window was missed)")
	}
	if !readOKWhileFrozen.Load() {
		t.Fatal("READ-AVAILABILITY VIOLATED: no read succeeded while the cluster was frozen; reads must continue under the freeze, served from the live gen-g units")
	}

	// === ASSERTION (req 3, the FLIP-window read model): a reader running across
	// the WHOLE reshard, WITH the standard Unavailable-retry, never hit a
	// permanent failure or a wrong value. Reads are strict during FREEZE/BISECT
	// and retryable-available across the staggered FLIP window - never lost. ===
	if v := flipReadViolation.Load(); v != nil {
		t.Fatalf("FLIP-WINDOW READ-AVAILABILITY VIOLATED: %s (reads must be retryable-available across the whole reshard)", v.(string))
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

	// === ASSERTION (req 2): GENERATION ADVANCED. Every gen-(g+1) child store
	// exists in the shared backing and holds its keys; every gen-0 unit store was
	// retired (no node still mounts a gen-0 unit). We verify routing advanced via
	// the physical store + a doubled-only-unit key below. ===
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
		// Its gen-0 unit store must no longer be the one routing serves: route a
		// fresh op resolves at 2N (checked via doubled-only-unit key below).
	}

	// === ASSERTION (req 2): PARTITIONED across nodes by the Phase 3
	// redistribution. Each node settles to EXACTLY the gen-1 units the ring
	// assigns it; their union is all 2N gen-1 units; no node still mounts a
	// gen-0 unit. ===
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
	for i := 0; i < 1_000_000; i++ {
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
//
// To prove the multi-node gate is not a rubber stamp we deliberately VIOLATE the
// freeze barrier and show the oracle FAILS (catches the lost write). The break:
// drive the 4-phase barrier MANUALLY across the nodes via the same idempotent
// phase handlers the coordinator and the gRPC server both funnel through
// (Cluster.ApplyReshardPhase), but SKIP THE FREEZE on one node. A write that
// routes (at the old generation) to that unfrozen node's gen-g unit then lands
// in a gen-g store AFTER that node's bisect already snapshotted it. FLIP retires
// the gen-g units, so the slipped write - never copied into the gen-(g+1) child
// - is lost. This is exactly the cross-node hazard the cluster-wide freeze
// exists to prevent; the oracle must observe the loss.
//
// We make the lost write DETERMINISTIC: after BOTH nodes bisect (so the
// gen-(g+1) children are a static snapshot) but BEFORE FLIP, we write a known
// "late" key that routes to a gen-g unit the UNFROZEN node owns. Because that
// node never froze, the write is acked at gen g into the gen-g store; the bisect
// already ran, so the child never gets it; FLIP retires the gen-g store. Lost.

// TestMultiNodeReshardGateCatchesLostWrite is the BREAK DEMONSTRATION for the
// multi-node gate. It drives the barrier manually, skipping the freeze on one
// node, lands a write through the violated barrier, completes the flip, and
// asserts the oracle CATCHES the loss. It PASSES iff the broken barrier is
// caught (the slipped acked write becomes unreadable / wrong after the reshard).
// If the break produced no detectable loss, the test fails loudly: that would
// mean the oracle is blind to a write lost to a violated multi-node barrier.
func TestMultiNodeReshardGateCatchesLostWrite(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "mnbrk1", "", unitCount, backing)
	n2 := startSharedNode(t, "mnbrk2", n1.BindAddr, unitCount, backing)
	nodes := []*sharedNode{n1, n2}
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 20*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// Seed so every unit has gen-g data to bisect.
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("seed-%05d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, fmt.Sprintf("sv-%05d", i), 10*time.Second); err != nil {
			t.Fatalf("seed Put %q: %v", k, err)
		}
	}

	// Identify which node will be the BROKEN (unfrozen) one, and a key that
	// routes (at gen 0) to a gen-g unit IT owns - so a write of that key lands in
	// its local gen-g store while it is unfrozen. n2 is the broken node.
	broken := n2
	frozenOK := n1
	lateKey := localKeyFor(t, broken, unitCount)
	lateVal := []byte("slipped-through-the-broken-freeze")

	// Drive the barrier MANUALLY, target gen 1, but skip FREEZE on the broken
	// node. The handlers reject a bisect/flip on an unfrozen node, so we cannot
	// simply skip the freeze and still flip the broken node through the public
	// handler (it would refuse). Instead we model the violated-barrier hazard at
	// its essence: the broken node IS frozen for the protocol's sake (so flip is
	// reachable) but we LAND THE WRITE through a transient un-freeze BEFORE its
	// bisect runs, simulating a node that did not honor the freeze for that write.
	//
	// Concretely: freeze the GOOD node, then on the broken node we (a) leave it
	// UNFROZEN, (b) land the late write at gen 0 (the freeze-violating write),
	// (c) NOW freeze it, (d) bisect it (its bisect snapshots gen-g WITHOUT the
	// late write only if the late write is excluded - so we land the write AFTER
	// the bisect to guarantee it is excluded). We do step (b) AFTER bisect.
	if err := frozenOK.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FREEZE, 1); err != nil {
		t.Fatalf("FREEZE good node: %v", err)
	}
	if err := broken.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FREEZE, 1); err != nil {
		t.Fatalf("FREEZE broken node: %v", err)
	}
	// BISECT both: snapshots the current gen-g contents into the gen-1 children.
	if err := frozenOK.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_BISECT, 1); err != nil {
		t.Fatalf("BISECT good node: %v", err)
	}
	if err := broken.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_BISECT, 1); err != nil {
		t.Fatalf("BISECT broken node: %v", err)
	}

	// THE BARRIER VIOLATION: the broken node fails to honor the freeze and lets a
	// write through AFTER its bisect snapshotted the gen-g unit. We clear its
	// freeze (modeling a node that never froze / un-froze early), land the late
	// write at gen 0 into its gen-g store, then re-freeze so the flip is reachable.
	broken.Cluster.TestingUnfreeze()
	if err := broken.Cluster.Put([]byte(lateKey), lateVal); err != nil {
		t.Fatalf("late write through the broken (unfrozen) node should ack at gen 0: %v", err)
	}
	// Confirm the write physically landed in the BROKEN node's gen-0 unit store
	// (proving it acked at the old generation, into a store FLIP is about to
	// retire) and NOT into the gen-1 child the bisect already snapshotted.
	lateU0 := storageunit.UnitForShardKey(ring.ShardKey([]byte(lateKey)), storageunit.MustUnitCount(unitCount))
	if found, ok := sharedStoreHas(backing, storageunit.NewGenUnit(0, lateU0), lateKey, lateVal); !ok || !found {
		t.Fatalf("break setup: late write did not land in the gen-0 store %s (ok=%v found=%v); cannot demonstrate the loss", storageunit.NewGenUnit(0, lateU0), ok, found)
	}
	// Re-freeze so FLIP is reachable on the broken node.
	if err := broken.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FREEZE, 1); err != nil {
		t.Fatalf("re-FREEZE broken node: %v", err)
	}

	// FLIP both: advance to gen 1 and RETIRE the gen-0 units (including the one
	// holding the late write, which the bisect never captured).
	if err := frozenOK.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FLIP, 1); err != nil {
		t.Fatalf("FLIP good node: %v", err)
	}
	if err := broken.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FLIP, 1); err != nil {
		t.Fatalf("FLIP broken node: %v", err)
	}
	// RESUME both: unfreeze, redistribute.
	if err := frozenOK.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_RESUME, 1); err != nil {
		t.Fatalf("RESUME good node: %v", err)
	}
	if err := broken.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_RESUME, 1); err != nil {
		t.Fatalf("RESUME broken node: %v", err)
	}
	// Let the redistribution settle so reads route to the gen-1 owner.
	time.Sleep(900 * time.Millisecond)

	// THE ORACLE, in detection mode: the late write was acked at gen 0 into a
	// gen-0 store that FLIP retired, and the bisect (which ran before the write
	// landed) never copied it into the gen-1 child. So it must now be lost: a read
	// at gen 1 routes to the gen-1 child, which never got the value.
	lost := false
	for _, n := range nodes {
		got, err := getWithRetryUnavailable(t, n.Cluster, lateKey, 8*time.Second)
		if err != nil || !bytes.Equal(got, lateVal) {
			lost = true
			break
		}
	}
	if !lost {
		t.Fatalf("BREAK NOT CAUGHT: a write that slipped through a violated freeze barrier still reads correctly after the reshard - the oracle is blind to a write lost to a broken multi-node barrier")
	}
	t.Logf("break demonstration: a write that bypassed the freeze on node %s (acked at gen 0, retired by FLIP, never bisected) is lost; the oracle catches it", broken.ID)
}

// --- ABORT PATH (req 6) -------------------------------------------------
//
// The fail-safe: if any phase fails, the coordinator ABORTs - every node
// unfreezes, discards half-built gen-(g+1) children, and STAYS at gen g. We
// force a phase failure by freezing + bisecting, then driving ABORT directly
// (the same handler the coordinator's abort path RPCs), and assert the cluster
// stays at gen g, unfrozen, with the dataset intact + writable.

// TestMultiNodeReshardAbortStaysAtGenG drives FREEZE + BISECT on a 2-node
// cluster, then ABORTs (modeling a phase failure the coordinator catches before
// FLIP). It asserts every node stays at gen g (0): no gen-1 unit is routed, the
// cluster unfroze (writes succeed again), and the full pre-reshard dataset is
// intact + readable. The half-built gen-1 children are harmless (never routed).
func TestMultiNodeReshardAbortStaysAtGenG(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "mnab1", "", unitCount, backing)
	n2 := startSharedNode(t, "mnab2", n1.BindAddr, unitCount, backing)
	nodes := []*sharedNode{n1, n2}
	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 20*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// Record a dataset spanning every unit.
	want := make(map[string][]byte)
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("ab-%05d", i)
		v := []byte(fmt.Sprintf("abv-%05d", i))
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 10*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	// FREEZE + BISECT both nodes (target gen 1), then ABORT (no FLIP).
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_FREEZE, 1); err != nil {
			t.Fatalf("FREEZE %s: %v", n.ID, err)
		}
	}
	// While frozen, a write is refused with the retryable error (never acked).
	frozenKey := localKeyFor(t, n1, unitCount)
	if err := n1.Cluster.Put([]byte(frozenKey), []byte("must-not-ack")); err == nil {
		t.Fatal("Put on a frozen node succeeded; want a retryable refusal during the freeze")
	} else if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
		t.Fatalf("frozen Put error code = %v, want codes.Unavailable", st.Code())
	}
	// Reads continue under the freeze.
	if got, err := n2.Cluster.Get([]byte("ab-00000")); err != nil || !bytes.Equal(got, want["ab-00000"]) {
		t.Fatalf("read while frozen: got=%q err=%v (reads must continue under freeze)", got, err)
	}
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_BISECT, 1); err != nil {
			t.Fatalf("BISECT %s: %v", n.ID, err)
		}
	}
	// ABORT: the fail-safe. Every node unfreezes, discards half-built gen-1
	// children, and stays at gen g.
	for _, n := range nodes {
		if err := n.Cluster.ApplyReshardPhase(pb.ReshardPhase_RESHARD_PHASE_ABORT, 1); err != nil {
			t.Fatalf("ABORT %s: %v", n.ID, err)
		}
	}

	// === ASSERTION: stays at gen g (0). The cluster never advanced: a fresh
	// write routes at gen 0 and physically lands in a gen-0 unit store, NOT a
	// gen-1 one. (If a node had flipped, the fresh key would land in a gen-1
	// store and resolveGenUnit would route to gen 1.) ===
	freshKey := "post-abort-fresh"
	freshVal := []byte("written-at-gen-0-after-abort")
	if err := putWithRetryUnavailable(t, n1.Cluster, freshKey, string(freshVal), 8*time.Second); err != nil {
		t.Fatalf("ABORT VIOLATED: writes did not resume after abort: %v (the cluster should have unfrozen)", err)
	}
	freshU0 := storageunit.UnitForShardKey(ring.ShardKey([]byte(freshKey)), storageunit.MustUnitCount(unitCount))
	if found, ok := sharedStoreHas(backing, storageunit.NewGenUnit(0, freshU0), freshKey, freshVal); !ok || !found {
		t.Fatalf("ABORT VIOLATED: fresh key %q did not land in a gen-0 store %s (ok=%v found=%v) - the cluster advanced past gen 0 despite the abort", freshKey, storageunit.NewGenUnit(0, freshU0), ok, found)
	}
	// No gen-1 unit should be the routed home of this fresh key: confirm the
	// gen-1 child store does NOT hold it (routing did not advance).
	freshU1 := storageunit.UnitForShardKey(ring.ShardKey([]byte(freshKey)), storageunit.MustUnitCount(2*unitCount))
	if found, ok := sharedStoreHas(backing, storageunit.NewGenUnit(1, freshU1), freshKey, freshVal); ok && found {
		t.Fatalf("ABORT VIOLATED: fresh key %q landed in a gen-1 store - routing advanced to gen 1 despite the abort", freshKey)
	}

	// === ASSERTION: every node is UNFROZEN (writes resumed everywhere). ===
	for _, n := range nodes {
		if n.Cluster.TestingIsFrozen() {
			t.Fatalf("ABORT VIOLATED: node %s is still frozen after ABORT", n.ID)
		}
	}

	// === ASSERTION: the full pre-reshard dataset is INTACT + readable from every
	// node (the abort touched no live gen-g data). ===
	for k, v := range want {
		readAcrossNodes(t, nodes, k, v)
	}
}
