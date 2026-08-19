package integration

// THE LOSSLESS-HANDOFF GATE TEST for v0.8 Phase 3 (the data-loss oracle).
//
// This is the safety net the whole lease-handoff phase is built to satisfy:
//
//	NO ACKED WRITE MAY BE LOST.
//
// Phase 3 hands a storage unit off COPY-FREE: when the ring re-assigns unit
// U from node A to node B, A flushes + releases the lease and B opens the
// SAME shared object-store bytes at a strictly higher epoch (fencing A).
// Bytes never move; only the writer lease moves. The gate below stands up an
// in-process multi-backend cluster on the SHARED-BACKING factory
// (internal/sharedfactory - one Backing per cluster, per-node Handles share
// it, exactly mirroring slatedb's shared object storage + per-unit durable
// writer-epoch) and asserts FIVE things across a real membership change:
//
//  1. THE ORACLE (zero loss). Every acked key written before the membership
//     change is still readable, with its EXACT recorded value, from ANY node
//     (via gRPC forwarding) after the ring re-assigns units and the reconcile
//     settles. Hundreds of keys, including co-located {tag} sets that must
//     stay together on one unit.
//  2. COPY-FREE. A handed-off unit's backend on the NEW owner is the SAME
//     shared underlying store, not a node-to-node copy: a write made DIRECTLY
//     to the shared backing's UnitStore (bypassing the cluster) is instantly
//     visible through the new owner's local read. A private copy could never
//     see that.
//  3. FENCING. After the handoff, the OLD owner's stale handle (opened at the
//     pre-move epoch) cannot write the handed-off unit: its Put returns
//     ErrFenced. The single-writer guarantee holds even if the old owner has
//     not yet observed the membership change.
//  4. CO-LOCATION SURVIVES. Every key in a {tag} set lands on ONE unit before
//     AND after the handoff, so a co-located set is never split across owners.
//  5. BREAK DEMONSTRATION (TestGateCatchesLostWrite, below). A deliberately
//     broken handoff (a unit dropped from the shared backing, simulating a
//     release that fails to flush / a lost copy) makes the oracle FAIL,
//     proving the gate actually catches a lost write rather than rubber-
//     stamping.
//
// If this test ever passes while a write is silently lost, the gate is
// broken; the break-demonstration test exists to keep it honest.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/clustertest"
	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
)

// startSharedNodeWithFactory brings up a multi-backend node like
// startSharedNode but with an arbitrary BackendFactory (e.g. a brokenHandle
// for the break-demonstration). The factory MUST embed a *sharedfactory.Handle
// so the returned sharedNode.Handle can expose OpenUnits() for mount-state
// introspection; we pull that handle back out via the unwrapper interface.
func startSharedNodeWithFactory(t *testing.T, id, seedAddr string, unitCount int, factory storageunit.BackendFactory) *sharedNode {
	t.Helper()

	h := unwrapHandle(factory)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startSharedNodeWithFactory %s: listen gRPC: %v", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	cfg := cluster.Config{
		NodeID:               id,
		BackendFactory:       factory,
		UnitCount:            storageunit.MustUnitCount(unitCount),
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
		ID:       id,
		Cluster:  c,
		Handle:   h,
		BindAddr: token,
		GRPCAddr: grpcAddr,
		stop: func() {
			grpcSrv.GracefulStop()
			<-serveDone
		},
	}
	t.Cleanup(n.Close)
	return n
}

// unwrapHandle extracts the embedded *sharedfactory.Handle from a factory so
// the test can introspect OpenUnits(). A plain *sharedfactory.Handle returns
// itself; a brokenHandle returns its embedded one.
func unwrapHandle(factory storageunit.BackendFactory) *sharedfactory.Handle {
	switch f := factory.(type) {
	case *sharedfactory.Handle:
		return f
	case *brokenHandle:
		return f.Handle
	case *skipCatchupHandle:
		return f.Handle
	default:
		panic(fmt.Sprintf("unwrapHandle: unsupported factory type %T", factory))
	}
}

// unitOf computes the storage unit a key routes to under unitCount, using the
// SAME shard-key extraction ({tag} co-location) the cluster routes with. Two
// keys with the same {tag} return the same unit.
func unitOf(key string, unitCount int) storageunit.UnitID {
	return storageunit.UnitForShardKey(ring.ShardKey([]byte(key)), storageunit.MustUnitCount(unitCount))
}

// readAcrossNodes reads key through EVERY node's Cluster surface (each routes
// to the current owner via forwarding) and asserts each returns want. It
// retries the retryable handoff-window error per node. A single node
// disagreeing is a forwarding / routing bug; all nodes must agree on the
// committed value.
func readAcrossNodes(t *testing.T, nodes []*sharedNode, key string, want []byte) {
	t.Helper()
	for _, n := range nodes {
		got, err := getWithRetryUnavailable(t, n.Cluster, key, 8*time.Second)
		if err != nil {
			t.Fatalf("ORACLE VIOLATED: key %q unreadable via node %s after handoff: %v (NO ACKED WRITE LOST)", key, n.ID, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ORACLE VIOLATED: key %q via node %s = %q, want %q (value corrupted across handoff)", key, n.ID, got, want)
		}
	}
}

// TestLosslessHandoffGate is THE data-loss oracle for v0.8 Phase 3. See the
// file header for the five properties it pins. It is intentionally heavier
// than the focused lease_handoff_test.go cases: it writes a large recorded
// dataset spanning many units (including co-located {tag} sets), then forces
// a real 2 -> 3 membership change and asserts the whole dataset survives,
// the handoff was copy-free, the old owner is fenced, and co-location held.
func TestLosslessHandoffGate(t *testing.T) {
	const unitCount = 16
	backing := sharedfactory.NewBacking()

	// --- Stand up 2 nodes, multi-backend, N=16 units. ---
	n1 := startSharedNode(t, "gate1", "", unitCount, backing)
	n2 := startSharedNode(t, "gate2", n1.BindAddr, unitCount, backing)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster}, 2, 15*time.Second); err != nil {
		t.Fatalf("initial 2-node convergence: %v", err)
	}
	// Let the join-driven reconcile settle so ownership is stable before we
	// record the baseline dataset.
	time.Sleep(700 * time.Millisecond)

	// --- Write a known dataset spanning MANY units, recording key -> value. ---
	// Two shapes:
	//   - plainKeys: hundreds of distinct keys, each routed by its whole key,
	//     spreading across the unit space.
	//   - coLocated: several {tag} sets, every key in a set sharing one tag so
	//     they all land on ONE unit (Redis-cluster hash-tag co-location). The
	//     oracle later asserts each set stayed on a single unit/owner.
	want := make(map[string][]byte) // the recorded dataset: key -> exact value

	const nPlain = 400
	for i := range nPlain {
		k := fmt.Sprintf("rec-%05d", i)
		v := fmt.Appendf(nil, "val-%05d-payload", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 8*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	// Co-located {tag} sets: 12 tags x 8 members each. Every member of a tag
	// MUST share a unit. Use enough tags that several land on units the 3rd
	// node will later own.
	const nTags, perTag = 12, 8
	tagSets := make(map[string][]string) // tag -> its keys
	for ti := range nTags {
		tag := fmt.Sprintf("acct%02d", ti)
		set := make([]string, 0, perTag)
		for mi := range perTag {
			k := fmt.Sprintf("{%s}/field-%d", tag, mi)
			v := fmt.Appendf(nil, "co-%s-%d", tag, mi)
			if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 8*time.Second); err != nil {
				t.Fatalf("co-located Put %q: %v", k, err)
			}
			want[k] = v
			set = append(set, k)
		}
		tagSets[tag] = set
	}

	// Pre-condition: every co-located set is on ONE unit (co-location holds
	// before any handoff). Record each tag's unit so we can prove it does not
	// split after the handoff.
	tagUnit := make(map[string]storageunit.UnitID, nTags)
	for tag, set := range tagSets {
		u0 := unitOf(set[0], unitCount)
		for _, k := range set {
			if u := unitOf(k, unitCount); u != u0 {
				t.Fatalf("precondition: co-located set %q split across units %d and %d", tag, u0, u)
			}
		}
		tagUnit[tag] = u0
	}

	// Sanity: confirm everything is readable BEFORE the membership change,
	// from both nodes (proves the baseline is genuinely committed + routable).
	nodes2 := []*sharedNode{n1, n2}
	keysSorted := make([]string, 0, len(want))
	for k := range want {
		keysSorted = append(keysSorted, k)
	}
	for _, k := range keysSorted {
		readAcrossNodes(t, nodes2, k, want[k])
	}

	// --- Trigger a membership change: add a 3rd node. ---
	// The ring re-assigns roughly a third of the units to it; the reconcile
	// hands off the moved units COPY-FREE (close-on-old / open-at-epoch+1-on-
	// new). Record which units the 3rd node will own so we can assert at least
	// one real handoff happened and probe it specifically.
	n3 := startSharedNode(t, "gate3", n1.BindAddr, unitCount, backing)
	nodes3 := []*sharedNode{n1, n2, n3}
	all3 := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(all3, 3, 15*time.Second); err != nil {
		t.Fatalf("3-node convergence after add: %v", err)
	}

	// Units the 3rd node now owns on the converged ring. Compute via the
	// cluster's own deterministic ring so we agree with routing.
	movedUnits := unitsOwnedBy(n1.Cluster, n3.ID, unitCount)
	if len(movedUnits) == 0 {
		t.Skip("3rd node owns no units under this ring; handoff direction not exercised")
	}

	// --- Wait for reconcile to settle: the 3rd node must mount its owned
	// units (the acquire half lands behind the 300ms settle timer). ---
	if !waitUntil(12*time.Second, func() bool {
		open := n3.Handle.OpenUnits()
		if len(open) < len(movedUnits) {
			return false
		}
		have := make(map[storageunit.UnitID]bool, len(open))
		for _, m := range open {
			have[m.Unit().ID] = true
		}
		for _, u := range movedUnits {
			if !have[u] {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("3rd node never mounted all its owned units: owned=%v open=%v", movedUnits, n3.Handle.OpenUnits())
	}

	// === ASSERTION 1: THE ORACLE. Every recorded key is still readable with
	// its EXACT value, from EVERY node (forwarding to the current owner). ZERO
	// loss. ===
	for _, k := range keysSorted {
		readAcrossNodes(t, nodes3, k, want[k])
	}

	// === ASSERTION 4: CO-LOCATION SURVIVES. Each {tag} set must STILL be (a)
	// entirely on its single recorded unit AND (b) served by a SINGLE owner
	// node after the handoff - never split across owners. We check both: the
	// per-key unit is stable (hash(tag) & (N-1) cannot move under a fixed N),
	// and the WHOLE set routes to one owner on the live 3-node ring. A split
	// would mean a transaction over the set could not commit in one engine. ===
	for tag, set := range tagSets {
		owners := make(map[string]struct{})
		for _, k := range set {
			if u := unitOf(k, unitCount); u != tagUnit[tag] {
				t.Fatalf("CO-LOCATION VIOLATED: set %q key %q now on unit %d, recorded unit %d", tag, k, u, tagUnit[tag])
			}
			owners[unitOwnerOnRing(n1.Cluster, k, unitCount)] = struct{}{}
		}
		if len(owners) != 1 {
			t.Fatalf("CO-LOCATION VIOLATED: set %q is split across %d owners %v after handoff (must be exactly one)", tag, len(owners), owners)
		}
	}

	// === ASSERTION 2: COPY-FREE. Pick a unit that handed off to n3. Write a
	// sentinel key DIRECTLY into the shared backing's store for that unit,
	// bypassing the cluster entirely (this models a write landing in the
	// shared object-store bytes). The new owner (n3) must see it through its
	// LOCAL read - which is only possible if n3 mounted the SAME shared store,
	// not a private copy. A node-to-node copy could never observe a write made
	// straight to the shared backing after the copy completed. ===
	probeUnit := movedUnits[0]
	sharedStore, ok := backing.UnitStore(gu0(probeUnit))
	if !ok {
		t.Fatalf("COPY-FREE check: shared backing has no store for handed-off unit %d", probeUnit)
	}
	// Both probe keys must HASH into probeUnit so the cluster's local-backend
	// resolution (unit = hash(shardKey) & (N-1)) lands them in probeUnit's
	// store. We synthesize a {tag} that maps to probeUnit and use it for both.
	tagForProbe := tagWithUnit(probeUnit, unitCount)
	sentinelKey := fmt.Appendf(nil, "{%s}/copyfree-direct", tagForProbe)
	sentinelVal := []byte("written-straight-to-shared-backing")
	if err := sharedStore.Put(sentinelKey, sentinelVal); err != nil {
		t.Fatalf("COPY-FREE check: direct write to shared backing for unit %d: %v", probeUnit, err)
	}
	// n3 must observe the sentinel via a LOCAL read (it owns + mounts the
	// unit). If it copied bytes instead of sharing, this read would miss.
	var got []byte
	if !waitUntil(8*time.Second, func() bool {
		var err error
		got, err = n3.Cluster.LocalGet(sentinelKey)
		return err == nil && bytes.Equal(got, sentinelVal)
	}) {
		t.Fatalf("COPY-FREE VIOLATED: new owner %s did not observe a write made directly to the shared backing for unit %d (it holds a COPY, not the shared store): got=%q", n3.ID, probeUnit, got)
	}
	// Symmetric direction: a write THROUGH the new owner shows up in the shared
	// backing store (the same underlying bytes), confirming bidirectional
	// identity rather than n3 holding a write-through to somewhere private.
	throughKey := fmt.Sprintf("{%s}/copyfree-through", tagForProbe)
	throughVal := []byte("written-through-new-owner")
	if err := putWithRetryUnavailable(t, n3.Cluster, throughKey, string(throughVal), 8*time.Second); err != nil {
		t.Fatalf("COPY-FREE check: Put through new owner: %v", err)
	}
	if gv, err := sharedStore.Get([]byte(throughKey)); err != nil || !bytes.Equal(gv, throughVal) {
		t.Fatalf("COPY-FREE VIOLATED: write through new owner %s did not land in the shared backing store for unit %d: got=%q err=%v", n3.ID, probeUnit, gv, err)
	}

	// === ASSERTION 3: FENCING. After the handoff, a STALE writer for the
	// handed-off unit - one whose captured epoch is below the unit's current
	// durable epoch (because the new owner acquired at a higher one) - cannot
	// write it: its Put fails with ErrFenced. This is the single-writer
	// guarantee the handoff depends on; it holds even if the prior owner has
	// not yet observed the membership change.
	//
	// We resolve the OLD owner of probeUnit (its owner in the 2-node ring) for
	// the failure message, then drive the fence against the SAME backing the
	// cluster uses (not a mock): we open a writer at the current durable epoch,
	// confirm it can write, then have a higher-epoch owner acquire (modeling
	// the next handoff) and confirm the first writer is now fenced. Because
	// n3's real acquire already advanced the durable epoch, any handle the old
	// owner still held for this unit at the pre-handoff epoch is, by the same
	// rule, already fenced.
	oldOwnerID := ownerOfUnitIn2NodeRing(probeUnit, n1.ID, n2.ID)
	if backing.DurableEpoch(gu0(probeUnit)) == 0 {
		t.Fatalf("FENCING check: handed-off unit %d (old owner %s) has durable epoch 0 - the acquire never fenced", probeUnit, oldOwnerID)
	}
	assertStaleHandleFenced(t, backing, probeUnit)

	// Belt + suspenders on the oracle: re-read the ENTIRE recorded dataset one
	// more time from a node that is NOT the original founder (n3, the joiner),
	// proving forwarding from the newest member reaches every key.
	for _, k := range keysSorted {
		got, err := getWithRetryUnavailable(t, n3.Cluster, k, 8*time.Second)
		if err != nil {
			t.Fatalf("ORACLE VIOLATED (re-read via joiner %s): key %q: %v", n3.ID, k, err)
		}
		if !bytes.Equal(got, want[k]) {
			t.Fatalf("ORACLE VIOLATED (re-read via joiner %s): key %q = %q, want %q", n3.ID, k, got, want[k])
		}
	}
}

// unitsOwnedBy returns the units the given node owns under c's current ring,
// computed via a ring rebuilt from c.Members() (deterministic consistent
// hashing on the same unit-id encoding the cluster uses).
func unitsOwnedBy(c *cluster.Cluster, nodeID string, unitCount int) []storageunit.UnitID {
	r := ring.New()
	for _, m := range c.Members() {
		r.Add(m)
	}
	uc := storageunit.MustUnitCount(unitCount)
	out := make([]storageunit.UnitID, 0)
	for _, u := range uc.IDs() {
		if r.LocateKey(unitIDBytesForTest(u)).ID == nodeID {
			out = append(out, u)
		}
	}
	return out
}

// ownerOfUnitIn2NodeRing recomputes which of the two original nodes owned a
// unit BEFORE the 3rd node joined, by building the 2-node ring from scratch.
func ownerOfUnitIn2NodeRing(u storageunit.UnitID, id1, id2 string) string {
	r := ring.New()
	r.Add(ring.Member{ID: id1})
	r.Add(ring.Member{ID: id2})
	return r.LocateKey(unitIDBytesForTest(u)).ID
}

// unitsOwnedInTwoNodeRing returns the units `target` owns in a ring built from
// exactly {founderID, target}. Lets a test PREDICT a handoff before the second
// node actually joins, so it can arm side effects (like the break-demo's wipe)
// before the asynchronous reconcile fires.
func unitsOwnedInTwoNodeRing(target, founderID string, unitCount int) []storageunit.UnitID {
	r := ring.New()
	r.Add(ring.Member{ID: founderID})
	r.Add(ring.Member{ID: target})
	uc := storageunit.MustUnitCount(unitCount)
	out := make([]storageunit.UnitID, 0)
	for _, u := range uc.IDs() {
		if r.LocateKey(unitIDBytesForTest(u)).ID == target {
			out = append(out, u)
		}
	}
	return out
}

// tagWithUnit returns a tag string whose hash maps to the given unit under
// unitCount. Brute-force search over a small label space (always succeeds for
// a power-of-two unit count well below the label space size).
func tagWithUnit(target storageunit.UnitID, unitCount int) string {
	uc := storageunit.MustUnitCount(unitCount)
	for i := range 1_000_000 {
		tag := fmt.Sprintf("probe%d", i)
		if storageunit.UnitForShardKey([]byte(tag), uc) == target {
			return tag
		}
	}
	panic(fmt.Sprintf("tagWithUnit: no tag maps to unit %d under %d units", target, unitCount))
}

// gu0 builds a generation-0 GenUnit. These lease-handoff gate tests do not
// reshard, so every unit they touch is at generation 0.
func gu0(u storageunit.UnitID) storageunit.GenUnit {
	return storageunit.NewGenUnit(0, u)
}

// unitIDBytesForTest mirrors the cluster's internal genUnitBytes (8-byte
// big-endian generation followed by 4-byte big-endian unit id) so a test-side
// ring placement agrees with routing. These tests place gen-0 units.
func unitIDBytesForTest(u storageunit.UnitID) []byte {
	var b [12]byte
	// Generation 0 occupies bytes 0..7 (all zero); the unit id is bytes 8..11.
	b[8] = byte(uint32(u) >> 24)
	b[9] = byte(uint32(u) >> 16)
	b[10] = byte(uint32(u) >> 8)
	b[11] = byte(uint32(u))
	return b[:]
}

// assertStaleHandleFenced proves the unit-level single-writer fence: a handle
// that opened unit u at the CURRENT durable epoch is then superseded by a
// higher-epoch acquire (modeling the next handoff), after which the first
// handle's writes fail with ErrFenced. This is the primitive the cluster's
// acquireUnit relies on to lock out a prior owner that has not yet observed
// the membership change. It runs against the SAME backing the cluster uses,
// so it is exercising the real fence, not a mock.
func assertStaleHandleFenced(t *testing.T, backing *sharedfactory.Backing, u storageunit.UnitID) {
	t.Helper()
	gu := gu0(u)
	// "stale" handle: open u at the current durable epoch via a fresh handle.
	stale := backing.Handle()
	staleBk, _, err := stale.OpenUnit(storageunit.SoleMount(gu), backing.DurableEpoch(gu))
	if err != nil {
		t.Fatalf("FENCING setup: stale handle OpenUnit(%d): %v", u, err)
	}
	// Before the next acquire, the stale handle is the current writer.
	if err := staleBk.Put([]byte("__fence_pre__"), []byte("ok")); err != nil {
		t.Fatalf("FENCING setup: stale handle should write before being superseded: %v", err)
	}
	// A higher-epoch owner acquires (the next handoff): durable epoch advances.
	newer := backing.Handle()
	if _, _, err := newer.OpenUnit(storageunit.SoleMount(gu), backing.DurableEpoch(gu)+1); err != nil {
		t.Fatalf("FENCING setup: newer handle OpenUnit(%d): %v", u, err)
	}
	// The stale handle is now fenced: its write MUST fail.
	if err := staleBk.Put([]byte("__fence_post__"), []byte("should-not-land")); err == nil {
		t.Fatalf("FENCING VIOLATED: stale handle for unit %d still writes after a higher-epoch owner acquired (double-writer)", u)
	} else if !errors.Is(err, sharedfactory.ErrFenced) {
		t.Fatalf("FENCING: stale-handle write failed with %v, want ErrFenced for unit %d", err, u)
	}
}

// brokenHandle wraps a real sharedfactory.Handle and DROPS one unit's bytes
// from the shared backing on release, simulating a handoff bug: the old
// owner's CloseUnit fails to flush (or the bytes are otherwise lost) so the
// new owner opens an EMPTY store instead of the shared bytes. It is the
// deliberate break the gate must catch. A real release never does this; the
// wrapper exists only to prove the oracle is not a rubber stamp. It satisfies
// storageunit.BackendFactory by embedding *sharedfactory.Handle and overriding
// only CloseUnit.
type brokenHandle struct {
	*sharedfactory.Handle
	backing   *sharedfactory.Backing
	breakUnit *atomicUnit
}

// atomicUnit is a tiny mutable holder for the unit to wipe on close, set after
// we learn which unit hands off. Guarded by its own mutex so the reconcile
// goroutine (which calls CloseUnit) reads it race-free against the test
// goroutine that arms it.
type atomicUnit struct {
	mu  sync.Mutex
	u   storageunit.UnitID
	set bool
}

func (a *atomicUnit) arm(u storageunit.UnitID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.u, a.set = u, true
}

func (a *atomicUnit) get() (storageunit.UnitID, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.u, a.set
}

// CloseUnit releases normally, then (the bug) wipes the shared bytes for the
// armed unit, so the next OpenUnit lands an empty store and every acked write
// in that unit is lost. This models the data-loss failure mode the gate is the
// safety net for.
func (h *brokenHandle) CloseUnit(m storageunit.MountRef) error {
	err := h.Handle.CloseUnit(m)
	if target, ok := h.breakUnit.get(); ok && m.Unit().ID == target {
		h.backing.WipeUnit(m.Unit())
	}
	return err
}

// sharedStoreKeyCount counts the keys currently in the shared backing store
// for unit u (0 if the unit has no store yet). The break-demo waits on this
// hitting zero as the deterministic signal that the broken release wiped the
// unit's bytes.
func sharedStoreKeyCount(t *testing.T, backing *sharedfactory.Backing, u storageunit.UnitID) int {
	t.Helper()
	s, ok := backing.UnitStore(gu0(u))
	if !ok {
		return 0
	}
	it, err := s.ScanPrefix(nil)
	if err != nil {
		t.Fatalf("sharedStoreKeyCount: scan unit %d: %v", u, err)
	}
	defer func() { _ = it.Close() }()
	n := 0
	for {
		k, _, err := it.Next()
		if err != nil {
			t.Fatalf("sharedStoreKeyCount: iterate unit %d: %v", u, err)
		}
		if k == nil {
			break
		}
		n++
	}
	return n
}

// TestGateCatchesLostWrite is the BREAK DEMONSTRATION. It re-runs the lossless
// gate's core oracle (write acked keys on 2 nodes, hand off on a join) but
// DELIBERATELY breaks the handoff: the old owner's CloseUnit wipes the
// handed-off unit's shared bytes, so the new owner mounts an empty store and
// every acked write in that unit is lost. The oracle MUST then observe the
// loss.
//
// This test PASSES iff the broken handoff is CAUGHT (a key in the wiped unit
// becomes unreadable / wrong). If the broken handoff somehow did NOT cause a
// detectable loss, the test fails loudly: that would mean the oracle is blind
// to a wiped unit, which is exactly what the gate must never be.
func TestGateCatchesLostWrite(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()

	bh1 := &brokenHandle{Handle: backing.Handle(), backing: backing, breakUnit: &atomicUnit{}}
	n1 := startSharedNodeWithFactory(t, "brk1", "", unitCount, bh1)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster}, 1, 10*time.Second); err != nil {
		t.Fatalf("founder up: %v", err)
	}

	// Write acked keys spread across units. Record key -> value.
	want := make(map[string][]byte)
	const nKeys = 200
	for i := range nKeys {
		k := fmt.Sprintf("brk-%04d", i)
		v := fmt.Appendf(nil, "bval-%04d", i)
		if err := putWithRetryUnavailable(t, n1.Cluster, k, string(v), 8*time.Second); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
		want[k] = v
	}

	// PREDICT which unit will hand off to the 2nd node, and ARM the break on
	// the founder for that unit BEFORE the node joins. Arming after join races
	// the reconcile: the founder might release (and our break would no-op,
	// since CloseUnit fires only once) before we arm. Consistent hashing is
	// deterministic on node IDs, so we compute n2's units from the predicted
	// 2-node ring up front. Once the founder's reconcile releases the armed
	// unit, its broken CloseUnit wipes the shared bytes IN PLACE - and because
	// the wipe targets the shared store object itself, the new owner sees the
	// loss whether it acquired before or after the wipe.
	const n2ID = "brk2"
	predicted := unitsOwnedInTwoNodeRing(n2ID, n1.ID, unitCount)
	if len(predicted) == 0 {
		t.Skip("no unit predicted to hand off to n2 under this ring; break not exercised")
	}
	breakUnit := predicted[0]
	bh1.breakUnit.arm(breakUnit)

	// Now add the 2nd node. The ring hands `predicted` units off; the founder
	// releases breakUnit (wiping it) and n2 acquires it.
	bh2 := &brokenHandle{Handle: backing.Handle(), backing: backing, breakUnit: &atomicUnit{}}
	n2 := startSharedNodeWithFactory(t, n2ID, n1.BindAddr, unitCount, bh2)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster}, 2, 15*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}

	// The reconcile (behind the settle timer) releases the broken unit on the
	// founder (wiping its bytes) and acquires it on n2. Wait for BOTH the new
	// owner to mount it AND the founder's broken release to actually wipe the
	// shared bytes. The wipe is the signal the break landed; the new owner may
	// mount before OR after the wipe, so we wait on the wipe specifically
	// (the shared store for breakUnit drained to zero keys).
	if !waitUntil(12*time.Second, func() bool {
		mounted := false
		for _, m := range n2.Handle.OpenUnits() {
			if m.Unit().ID == breakUnit {
				mounted = true
				break
			}
		}
		return mounted && sharedStoreKeyCount(t, backing, breakUnit) == 0
	}) {
		t.Fatalf("break setup: n2 never acquired the broken unit %d or founder never wiped it (store keys=%d)", breakUnit, sharedStoreKeyCount(t, backing, breakUnit))
	}

	// THE ORACLE, run in detection mode: at least one acked key whose unit is
	// the wiped one MUST now be lost (unreadable or wrong). The gate is proven
	// to CATCH the break iff we observe such a loss.
	lostObserved := false
	var lostKey string
	for k, v := range want {
		if unitOf(k, unitCount) != breakUnit {
			continue
		}
		got, err := getWithRetryUnavailable(t, n1.Cluster, k, 3*time.Second)
		if err != nil || !bytes.Equal(got, v) {
			lostObserved = true
			lostKey = k
			break
		}
	}
	if !lostObserved {
		// If we get here, the broken handoff did NOT produce a detectable loss,
		// which would mean the oracle is blind to a wiped unit. That is the
		// failure the break-demonstration exists to surface.
		t.Fatalf("BREAK NOT CAUGHT: wiped unit %d but every acked key in it still reads correctly - the oracle is blind to a lost write", breakUnit)
	}
	t.Logf("break demonstration: wiping handed-off unit %d on release made acked key %q unreadable/wrong; the oracle catches it", breakUnit, lostKey)
}
