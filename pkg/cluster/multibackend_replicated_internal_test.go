package cluster

// White-box tests for v0.8 Phase 2b per-UNIT replica routing. These live in
// package cluster so they can drive the unexported helpers (unitReplicas,
// desiredReplicaUnits, multiReplicated) against a real ring + a per-replica
// shared-backing factory, without standing up membership / gRPC. The
// wired-together cross-node fan-out + read quorum is covered end to end in
// tests/integration/lossless_multibackend_r2_gate_test.go.

import (
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/storageunit"
)

// newReplicatedCluster builds a minimal R>1 multi-backend Cluster wired to a
// per-replica shared-backing handle + a transport-free coordinator over
// memberIDs, with no gossip / gRPC. self is this node's id.
func newReplicatedCluster(t *testing.T, self string, n, r int, backing *sharedfactory.Backing, memberIDs ...string) *Cluster {
	t.Helper()
	h := backing.Handle()
	c := &Cluster{
		// LogOutput io.Discard: the acquire/mount transition logging added for
		// the handoff observability would otherwise write to os.Stderr (logf's
		// deliberate fallback) in every white-box test.
		cfg:        Config{NodeID: self, ReplicationFactor: r, LogOutput: io.Discard},
		multi:      true,
		factory:    h,
		unitCount:  storageunit.MustUnitCount(n),
		pauseUnits: make(map[storageunit.UnitID]*sync.RWMutex),
		coord:      staticCoord(self, nodesFor(memberIDs...)),
		closeCh:    make(chan struct{}),
	}
	c.mounts.init(c)
	c.genOwner = c.genUnitOwner
	c.initGenState()
	// Terminate the fixture's background goroutines at test end exactly as a
	// real Close does: flip closed, close closeCh, JOIN loopWG. The
	// displaced-drain poller a beginDrain arms exits on closeCh (production
	// wiring); without this cleanup the never-closed fixture leaves the poller
	// running to its 30s lifetime cap, which outlives a FAST SCOPED test run
	// and fails goleak at TestMain (the CI shard redness) while full runs
	// outlast the cap and hide it. The join is BOUNDED so a test bug (e.g. a
	// HangOpenReplica left armed) surfaces as an attributable failure here
	// instead of wedging the package to its timeout; hang-arming tests release
	// via their own t.Cleanup, which runs before this one (LIFO).
	t.Cleanup(func() {
		c.closed.Store(true)
		c.closeOnce.Do(func() { close(c.closeCh) })
		done := make(chan struct{})
		go func() { c.loopWG.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("fixture cleanup: background goroutines did not exit within 10s of close")
		}
	})
	return c
}

func TestMultiReplicated_PredicateGating(t *testing.T) {
	backing := sharedfactory.NewBacking()

	// R=2 with a populated 3-member ring: replicated.
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	if !c.multiReplicated() {
		t.Fatalf("R=2 multi with populated ring should be replicated")
	}
	if !c.replicaLayout() {
		t.Fatalf("R>1 should address storage by replica position")
	}

	// R=1 multi: NOT replicated (single-mount path).
	c1 := newReplicatedCluster(t, "n1", 8, 1, backing, "n1", "n2")
	if c1.multiReplicated() {
		t.Fatalf("R=1 multi should not be replicated")
	}
	if c1.replicaLayout() {
		t.Fatalf("R=1 should address storage by sole mount, not replica position")
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
			if int(ru.Replica) >= len(set) || string(set[ru.Replica].ID) != self {
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

// TestRoutedReplicasForKey_CoLocatedKeysShareReplicaSet: keys in the same
// {tag} set resolve to one unit and therefore one replica set. Asserted
// against routedReplicasForKey, the live resolver every replicated fan-out
// consults, so co-location is pinned on the path ops actually take. With no
// member joining or draining the routed set IS the stable set, so this also
// pins the steady-state stableR the ack bar is computed over.
func TestRoutedReplicasForKey_CoLocatedKeysShareReplicaSet(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 16, 2, backing, "n1", "n2", "n3")

	a, stableA := c.routedReplicasForKey([]byte("{acct42}:balance"))
	b, stableB := c.routedReplicasForKey([]byte("{acct42}:name"))
	if len(a) != len(b) {
		t.Fatalf("co-located keys got different replica-set sizes %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			t.Fatalf("co-located keys diverge at pos %d: %q vs %q", i, a[i].ID, b[i].ID)
		}
	}
	// Steady state (no joining / draining member): routed == the stable set.
	if stableA != len(a) || stableB != len(b) {
		t.Fatalf("steady state: stableR should equal the routed size, got (%d,%d) for routed sizes (%d,%d)",
			stableA, stableB, len(a), len(b))
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
	if _, ok := c.mounts.backendFor(bad); ok {
		t.Fatalf("poisoned replica %s must NOT be mounted", bad)
	}
	s, ok := c.mounts.acquireErrOf(bad)
	if !ok {
		t.Fatalf("poisoned replica %s must be recorded as a failed acquire", bad)
	}
	if !strings.Contains(s, "empty SSTable") {
		t.Fatalf("the acquire record for %s = %q, want it to carry the open error", bad, s)
	}

	// Every OTHER desired position IS mounted: the healthy units keep full service.
	for _, ru := range desired[1:] {
		if _, ok := c.mounts.backendFor(ru); !ok {
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
	if _, ok := c.mounts.backendFor(bad); !ok {
		t.Fatalf("repaired replica %s must mount on re-acquire", bad)
	}
	if _, ok := c.mounts.acquireErrOf(bad); ok {
		t.Fatalf("the acquire record for %s must be cleared after a successful re-acquire", bad)
	}
}

// TestMountReplicaUnits_BoundedConcurrency pins v0.8 Phase 2g: the boot mount
// opens owned positions IN PARALLEL up to Config.OpenConcurrency, and NEVER
// beyond it. A single-member ring makes n1 own one position for every unit (the
// worst-case founder-alone cold start). Each open is widened with an artificial
// delay so the pool's workers genuinely overlap, and the factory's high-water
// mark of simultaneously in-flight opens is read back. limit=4 must reach
// exactly 4 concurrent opens; limit=1 must degenerate to the exact sequential
// mount (high-water 1), proving the knob is a strict generalization. Both must
// still mount EVERY owned position (no faults, no markers).
func TestMountReplicaUnits_BoundedConcurrency(t *testing.T) {
	run := func(t *testing.T, limit int, wantMax int64) {
		t.Helper()
		backing := sharedfactory.NewBacking()
		// Single-member ring => n1 owns a position for every unit (16 positions).
		c := newReplicatedCluster(t, "n1", 16, 2, backing, "n1")
		owned := len(c.desiredReplicaUnits())
		if owned < 8 {
			t.Fatalf("test needs n1 to own >=8 positions to exercise concurrency, got %d", owned)
		}
		h, ok := c.factory.(*sharedfactory.Handle)
		if !ok {
			t.Fatalf("factory is %T, want *sharedfactory.Handle", c.factory)
		}
		// Widen each open so concurrent workers genuinely overlap (without a delay
		// an open can finish before the next worker is scheduled, hiding the
		// concurrency the bound permits).
		h.SetAcquireDelay(40 * time.Millisecond)
		c.cfg.OpenConcurrency = limit

		if err := c.mountReplicaUnits(); err != nil {
			t.Fatalf("mountReplicaUnits (limit %d): %v", limit, err)
		}
		if got := c.mounts.mountedCount(); got != owned {
			t.Fatalf("limit %d: mounted %d of %d owned positions", limit, got, owned)
		}
		if peak := h.MaxConcurrentOpens(); peak != wantMax {
			t.Fatalf("limit %d: max concurrent opens = %d, want exactly %d (owned=%d)", limit, peak, wantMax, owned)
		}
	}

	// Bounded parallel: opens run up to the bound, never beyond.
	t.Run("limit4_runs_4_in_parallel", func(t *testing.T) { run(t, 4, 4) })
	// Limit 1 degenerates to the exact strictly-sequential mount.
	t.Run("limit1_is_sequential", func(t *testing.T) { run(t, 1, 1) })
}

// TestMountReplicaUnits_DoesNotFenceServingPeer pins the v0.8 Phase 2f fencing-
// safety refinement: the boot mount must NOT open (hence must NOT fence) a
// position a live peer is already serving. It reads the fence-free serving
// marker first and DEFERS such positions to reconcile. This is the fix for the
// prod incident where a re-bootstrapping seed opened every position and fenced
// every serving peer cluster-wide.
//
// BREAK DEMONSTRATION: without the serving-marker pre-check, mountReplicaUnits
// would OpenReplicaUnit(served) at durable+1, fencing the peer, and the peer's
// second Put below would fail with ErrFenced - which is exactly what this test
// asserts must NOT happen.
func TestMountReplicaUnits_DoesNotFenceServingPeer(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 8, 2, backing, "n1", "n2", "n3")
	desired := c.desiredReplicaUnits()
	if len(desired) < 2 {
		t.Fatalf("test needs n1 to desire >=2 positions, got %d", len(desired))
	}
	served := desired[0] // a position a peer is already serving

	// A PEER opens this position and writes its serving marker (it reached Ready).
	peer := backing.Handle()
	pb, peerEpoch, err := peer.OpenReplicaUnit(served, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("peer open: %v", err)
	}
	if err := peer.WriteServingMarker(storageunit.ReplicaMount(served), peerEpoch); err != nil {
		t.Fatalf("peer write serving marker: %v", err)
	}
	if err := pb.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("peer put (pre-boot): %v", err)
	}

	// Founder boot-mount: it desires `served` too, but a peer is serving it.
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}

	// `served` is DEFERRED: not mounted, recorded, and - crucially - NOT fenced.
	if _, ok := c.mounts.backendFor(served); ok {
		t.Fatalf("position %s a peer is serving must NOT be mounted at boot", served)
	}
	if v, ok := c.mounts.acquireErrOf(served); !ok || !strings.Contains(v, "serving") {
		t.Fatalf("served position should be recorded boot-deferred; ok=%v v=%v", ok, v)
	}
	// THE FIX: the peer was NOT fenced - its writes still succeed.
	if err := pb.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("FENCE REGRESSION: boot mount fenced the serving peer (%v); the serving-marker pre-check failed", err)
	}

	// Cold-mount still works: the other desired positions (no marker) ARE mounted.
	for _, ru := range desired[1:] {
		if _, ok := c.mounts.backendFor(ru); !ok {
			t.Fatalf("un-marked position %s should mount normally at boot", ru)
		}
	}
}

// closeRacingFactory flips the cluster's closed flag as each open RETURNS, so
// every boot mount reaches the serving transition with the table already
// closing: the Open/Close overlap (a cancelled startup, a supervisor tearing
// the node down mid-boot), made deterministic.
type closeRacingFactory struct {
	storageunit.BackendFactory
	closed *atomic.Bool
}

func (f *closeRacingFactory) OpenUnit(m storageunit.MountRef, e storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	b, opened, err := f.BackendFactory.OpenUnit(m, e)
	if err == nil {
		f.closed.Store(true)
	}
	return b, opened, err
}

// TestMountReplicaUnits_SupersededMountRecordsWhyItIsUnmounted pins the BOOT
// caller's handling of a superseded serving transition. The position is opened
// and released rather than mounted, and mountReplicaUnits still returns nil, so
// Open succeeds holding fewer positions than it desires. The recorded acquire
// reason is the only account of that gap - /debug/shale/state has nothing else
// to read, and the mount count alone does not say which positions are missing
// or why.
func TestMountReplicaUnits_SupersededMountRecordsWhyItIsUnmounted(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1")
	desired := c.desiredReplicaUnits()
	if len(desired) == 0 {
		t.Fatalf("n1 desires no position; the test proves nothing")
	}
	c.factory = &closeRacingFactory{BackendFactory: c.factory, closed: &c.closed}

	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}

	if n := c.mounts.mountedCount(); n != 0 {
		t.Fatalf("a boot that lost the race with Close mounted %d positions, want 0", n)
	}
	for _, ru := range desired {
		msg, ok := c.mounts.acquireErrOf(ru)
		if !ok {
			t.Fatalf("boot opened %s, did not mount it, and recorded nothing: the missing mount is "+
				"unexplainable in /debug/shale/state", ru)
		}
		if !strings.Contains(msg, "superseded") {
			t.Fatalf("boot recorded %q for %s, want a reason naming the superseding close", msg, ru)
		}
	}
}

// roleHookCoord runs a hook when this node PUBLISHES its role set, so a test
// can move the coordination view inside that round trip. On a CAS coordinator
// the publish is an object-store read-modify-write and the poll loop refreshes
// the snapshot underneath it; the hook stands in for that.
type roleHookCoord struct {
	coord.Coordinator
	onSetRole func()
}

func (c *roleHookCoord) SetRole(r coord.Role) error {
	err := c.Coordinator.SetRole(r)
	if c.onSetRole != nil {
		c.onSetRole()
	}
	return err
}

// syncLog is an io.Writer safe to read while the reconcile the boot warm-up
// arms is still logging.
type syncLog struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// TestBootWarmUp_PublishesTheJoiningBitBeforeSamplingTheView pins the boot
// warm-up's READ ORDER against the founder-with-deferred-positions shape, the
// only one where it is observable: the node founds membership carrying no
// Joining bit, defers to a marker from the generation before it, and its sole
// peer clears its own bit during the publish. Sampling the view BEFORE
// publishing counts that peer as still warming and debounces, spending a full
// settle delay inside the deferred-position write-quorum gap the prompt exists
// to close.
func TestBootWarmUp_PublishesTheJoiningBitBeforeSamplingTheView(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2")
	logs := &syncLog{}
	c.cfg.LogOutput = logs

	// The peer advertises Joining until this node publishes its own bit, at
	// which point it is established.
	peerFacts := func(roles coord.Role) coord.Member {
		return coord.Member{Node: coord.Node{ID: "n2", Addr: "n2:0"}, Roles: roles}
	}
	inner := c.coord.(*gossip.Coordinator)
	inner.TestingSetFacts(peerFacts(coord.RoleJoining))
	c.coord = &roleHookCoord{
		Coordinator: inner,
		onSetRole:   func() { inner.TestingSetFacts(peerFacts(0)) },
	}

	// Defer a position so the warm-up runs at all: the peer is serving it.
	desired := c.desiredReplicaUnits()
	if len(desired) == 0 {
		t.Fatalf("n1 desires no position; the test proves nothing")
	}
	peer := backing.Handle()
	_, peerEpoch, err := peer.OpenReplicaUnit(desired[0], storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("peer open: %v", err)
	}
	if err := peer.WriteServingMarker(storageunit.ReplicaMount(desired[0]), peerEpoch); err != nil {
		t.Fatalf("peer write serving marker: %v", err)
	}

	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}

	if got := logs.String(); !strings.Contains(got, "PROMPT") {
		t.Fatalf("boot warm-up did not prompt into a peer that became established during the "+
			"role publish; the view was sampled before the publish. log:\n%s", got)
	}
}
