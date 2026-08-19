package integration

// The FAN-OUT half of the cluster.ErrAcquiring gate.
//
// Aggregate is the entry point with the MOST exposure to a handoff window, not
// the least: every other call routes ONE key to ONE position, so it meets an
// acquiring window only when that single position is mid-mount. Aggregate fans
// out across ALL shards, so it meets one when ANY node holds ANY owned position
// unmounted.
//
// It also has an error surface no point op has. A point op returns (value,
// error) and the consumer matches the error. Aggregate returns per-peer
// AggregateResult values, so a refusal can arrive through EITHER of two doors:
//
//	AggregateResult.Err  - shale could not run fn for that peer at all
//	the scan fn          - fn ran, and the refusal surfaced from the iterator
//	                       it was handed, crossing the fan-out boundary as
//	                       whatever fn RETURNS (an `any`, not an error)
//
// The second door is the dangerous one, and these tests exist mostly for it.
//
// WHY THIS IS DATA-LOSS-ADJACENT RATHER THAN AN ERGONOMICS COMPLAINT. Fan-out
// consumers build SETS and then act on what is ABSENT from them (a garbage
// collector deletes what nothing in its referenced set names). For such a
// consumer a silently-partial scan is not a smaller answer, it is a WRONG one
// that deletes live data, and re-running afterwards cannot undo it. So the bar
// here is HIGHER than "the refusal is matchable": a partial result must never
// be MISTAKABLE for a complete one. Matchability is how the consumer retries;
// non-mistakability is what makes silence impossible.
//
// The pre-fix behaviour these pin, measured: a node holding an owned position
// unmounted simply OMITTED it from its local scan. Both doors stayed shut - no
// AggregateResult.Err, no error from the scan fn - and the fan-out returned a
// short key set with a clean end-of-iteration. That is the exact shape a
// consumer cannot defend against, because a shorter answer is a legitimate one.

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// aggScan is what the test's scan fn returns for one peer. It mirrors the
// shape a real consumer uses: the scan's own result plus whatever error the
// iterator raised, carried back through Aggregate's `any` channel. Keeping the
// error as a FIELD (rather than returning it bare) is deliberate: it is the
// consumer's type-switch case, and it is where a refusal that arrived as a
// bare string or a rebuilt chain would stop being matchable.
type aggScan struct {
	keys    map[string]struct{}
	scanErr error
}

// aggOutcome is one fan-out's collapsed result across BOTH doors.
//
// Completeness is measured as the DISTINCT key union, not a running total,
// because that is what the consumers at risk actually build: a referenced-blob
// SET, a keygate count of distinct holders. It is also the only measure that is
// meaningful on this path - a unit mid-handoff can be mounted on both its old
// and its new owner at once, so the same key legitimately arrives twice. A
// duplicate is harmless to a set; a MISSING key is the one that deletes data.
type aggOutcome struct {
	keys      map[string]struct{} // distinct keys the fan-out delivered
	errs      []error             // AggregateResult.Err values
	scanErrs  []error             // errors that came back THROUGH the scan fn
	badValues []string
}

func (o aggOutcome) refused() bool { return len(o.errs) > 0 || len(o.scanErrs) > 0 }

// legBarrier holds every fan-out leg inside the scan fn until ALL of them have
// arrived, runs onFull exactly once at that point, then releases them together.
//
// The ordering is what makes the scan-fn test deterministic, and it has to be
// exactly this way round. The legs are NOT symmetric in cost: the local leg
// snapshots in-process and reaches fn almost immediately, while a peer leg
// dials gRPC, waits for READY, opens a stream and primes it first. Opening the
// window from whichever leg happened to arrive first therefore lands BEFORE the
// peer has been snapshotted, and the refusal comes back in AggregateResult.Err
// instead of through the scan fn - the channel under test never fires, and the
// test passes or fails on scheduling luck. Waiting for every leg to arrive
// proves every snapshot is already taken, which is the precondition that leaves
// the scan fn as the only door.
//
// A leg that never arrives (its snapshot was refused, so fn was never invoked
// for it) must not deadlock the fan-out, so arrive() gives up after a bounded
// wait. onFull does not run in that case, and the caller's assertion reports
// the window never opened rather than hanging.
type legBarrier struct {
	mu     sync.Mutex
	seen   int
	want   int
	onFull func()
	open   chan struct{}
}

func newLegBarrier(want int, onFull func()) *legBarrier {
	return &legBarrier{want: want, onFull: onFull, open: make(chan struct{})}
}

func (b *legBarrier) arrive() {
	b.mu.Lock()
	b.seen++
	if b.seen == b.want {
		if b.onFull != nil {
			b.onFull()
		}
		close(b.open)
	}
	b.mu.Unlock()
	select {
	case <-b.open:
	case <-time.After(5 * time.Second):
	}
}

func (o aggOutcome) allRefusals() []error {
	return append(append([]error{}, o.errs...), o.scanErrs...)
}

// runAggregate fans out a prefix-scoped key count and collapses both channels.
// The scan is PREFIXED on purpose: a prefixed scan opens a fresh LocalScan
// stream per peer rather than reusing the one snapshotPeer primed, which is
// what routes a mid-fan-out refusal through the scan fn instead of through
// AggregateResult.Err. It is also the shape the real consumers use (their
// referenced-blob set scans a pointer prefix).
func runAggregate(c *cluster.Cluster, prefix string, before func()) aggOutcome {
	barrier := newLegBarrier(len(c.Members()), before)
	results := c.Aggregate(func(b backend.Backend) any {
		if before != nil {
			// Hold here until every leg is inside fn, i.e. until every peer has
			// been snapshotted. `before` then opens the acquiring window from
			// the barrier itself, so no leg can be refused at snapshot time and
			// the scan fn is the only channel the refusal can arrive through.
			barrier.arrive()
		}
		seen := make(map[string]struct{})
		it, err := b.ScanPrefix([]byte(prefix))
		if err != nil {
			return aggScan{keys: seen, scanErr: err}
		}
		defer func() { _ = it.Close() }()
		for {
			k, _, err := it.Next()
			if err != nil {
				return aggScan{keys: seen, scanErr: err}
			}
			if k == nil {
				return aggScan{keys: seen}
			}
			seen[string(k)] = struct{}{}
		}
	})

	out := aggOutcome{keys: make(map[string]struct{})}
	for i, r := range results {
		if r.Err != nil {
			out.errs = append(out.errs, r.Err)
			continue
		}
		s, ok := r.Value.(aggScan)
		if !ok {
			out.badValues = append(out.badValues,
				fmt.Sprintf("result[%d].Value is %T (%v), not the scan fn's return type", i, r.Value, r.Value))
			continue
		}
		if s.scanErr != nil {
			out.scanErrs = append(out.scanErrs, s.scanErr)
		}
		for k := range s.keys {
			out.keys[k] = struct{}{}
		}
	}
	return out
}

// requireMatchableRefusals is the MATCHABILITY half: every refusal the fan-out
// produced, on EITHER channel, must be a real error carrying ErrAcquiring and
// must keep the client-facing code the existing retry shapes read.
//
// A refusal that fails this is worse than a hard error: the consumer wraps both
// channels with %w and gates on errors.Is, so an unmatchable refusal means their
// retry never fires and the refusal is consumed as data.
func requireMatchableRefusals(t *testing.T, label string, o aggOutcome) {
	t.Helper()
	for _, bad := range o.badValues {
		t.Fatalf("%s: %s", label, bad)
	}
	for _, err := range o.errs {
		if !errors.Is(err, cluster.ErrAcquiring) {
			t.Fatalf("%s: AggregateResult.Err does not match cluster.ErrAcquiring: %v (%T)", label, err, err)
		}
		if code := status.Code(err); code != codes.Unavailable {
			t.Fatalf("%s: AggregateResult.Err code moved: got %v want Unavailable. err=%v", label, code, err)
		}
	}
	for _, err := range o.scanErrs {
		if !errors.Is(err, cluster.ErrAcquiring) {
			t.Fatalf("%s: refusal reaching the SCAN FN does not match cluster.ErrAcquiring: %v (%T). "+
				"This is the channel that crosses the fan-out boundary as a value; unmatchable here "+
				"means the consumer's type-switch never fires and a partial set is consumed as complete.",
				label, err, err)
		}
		if code := status.Code(err); code != codes.Unavailable {
			t.Fatalf("%s: scan-fn refusal code moved: got %v want Unavailable. err=%v", label, code, err)
		}
	}
}

// requireNoSilentPartial is the STRONGER half, and the one that makes silence
// impossible rather than merely recoverable: a fan-out that delivered FEWER
// keys than the settled cluster holds must have said so on some channel. A
// short result with both doors shut is the finding.
func requireNoSilentPartial(t *testing.T, label string, o aggOutcome, want int) {
	t.Helper()
	if len(o.keys) >= want {
		return
	}
	if !o.refused() {
		t.Fatalf("%s: SILENT PARTIAL. Aggregate delivered %d of %d distinct keys and reported NOTHING on "+
			"either channel (no AggregateResult.Err, no error through the scan fn). A consumer building a "+
			"referenced-blob set from this deletes live data: the missing keys read as unreferenced, and "+
			"re-running afterwards cannot undo the deletion.",
			label, len(o.keys), want)
	}
	t.Logf("%s: partial (%d of %d distinct keys) and correctly refused: %v", label, len(o.keys), want, o.allRefusals())
}

// aggregatePair brings up a settled 2-node R=1 cluster over shared backing and
// seeds `n` keys under prefix, returning the two nodes and the settled total
// the fan-out delivers when nothing is mid-acquire (the ground truth a partial
// is measured against).
func aggregatePair(t *testing.T, unitCount, n int, prefix string) (n1, n2 *sharedNode, settled int) {
	t.Helper()

	// A short PeerConnectTimeout keeps the peer-down leg from sitting out the
	// 30s default before reporting; the acquiring cases never reach it.
	shortPeerConnect := func(cfg *cluster.Config) { cfg.PeerConnectTimeout = 750 * time.Millisecond }
	backing := sharedfactory.NewBacking()
	n1 = startReplicatedNodeCfg(t, "agr1", "", unitCount, 1, backing, shortPeerConnect)
	n2 = startReplicatedNodeCfg(t, "agr2", n1.ClusterToken, unitCount, 1, backing, shortPeerConnect)

	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("ring convergence: %v", err)
	}
	if !waitUntil(10*time.Second, func() bool { return len(n2.Handle.OpenUnits()) > 0 }) {
		t.Fatalf("no unit ever handed off to agr2: n2 open=%v", n2.Handle.OpenUnits())
	}

	for i := range n {
		k := fmt.Sprintf("%s%04d", prefix, i)
		if err := n1.Cluster.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("seed Put %s: %v", k, err)
		}
	}

	// The baseline must see every seeded key, else a later "partial" assertion
	// would be measuring an unsettled fixture rather than the handoff window.
	// A unit mid-handoff can be mounted on its old AND new owner at once, so
	// the union is the settled measure; the raw sum is not.
	var base aggOutcome
	if !waitUntil(15*time.Second, func() bool {
		base = runAggregate(n1.Cluster, prefix, nil)
		return !base.refused() && len(base.keys) == n
	}) {
		t.Fatalf("baseline fan-out never delivered the %d seeded keys (got %d distinct, errs=%v scanErrs=%v): "+
			"the fixture is not settled", n, len(base.keys), base.errs, base.scanErrs)
	}
	return n1, n2, len(base.keys)
}

// ownedUnitOn returns a unit that node `owner` owns on a ring both nodes agree
// about, so clearing its mount leaves that node owner-but-unmounted: the real
// handoff state, not a synthesized error.
func ownedUnitOn(t *testing.T, probe *cluster.Cluster, peer *cluster.Cluster, ownerID string, unitCount, n int, prefix string) storageunit.UnitID {
	t.Helper()
	uc := storageunit.MustUnitCount(unitCount)
	for i := range n {
		k := fmt.Sprintf("%s%04d", prefix, i)
		o1 := unitOwnerOnRing(probe, k, unitCount)
		o2 := unitOwnerOnRing(peer, k, unitCount)
		if o1 == "" || o1 != o2 || o1 != ownerID {
			continue
		}
		return storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), uc)
	}
	t.Fatalf("no unit owned by %s found across %d candidate keys", ownerID, n)
	return 0
}

// TestAcquiringReason_AggregateRefusalIsMatchableAndNotSilent is the deliverable.
//
// A position that is owned-but-unmounted is invisible to a mount-map walk, so
// before the fix BOTH doors stayed shut and the fan-out returned a short set
// with a clean end-of-iteration (measured: 325 keys -> 300 with a peer
// mid-acquire, zero errors on either channel). Now it refuses, and the refusal
// carries ErrAcquiring whichever node is mid-acquire and whichever door it
// comes through.
//
// Both directions are driven: a REMOTE peer mid-acquire exercises the wire
// (the refusal is minted on the peer, encoded as a status detail, decoded by
// the originator's stream interceptor), and the LOCAL node mid-acquire
// exercises the in-process snapshot path where no wire round trip happens.
func TestAcquiringReason_AggregateRefusalIsMatchableAndNotSilent(t *testing.T) {
	const (
		unitCount = 8
		n         = 200
		prefix    = "agg-ar-"
	)

	t.Run("remote-peer-mid-acquire", func(t *testing.T) {
		n1, n2, settled := aggregatePair(t, unitCount, n, prefix)
		u := ownedUnitOn(t, n1.Cluster, n2.Cluster, n2.ID, unitCount, n, prefix)

		n2.Cluster.TestingClearMount(u)
		got := runAggregate(n1.Cluster, prefix, nil)

		if !got.refused() {
			t.Fatalf("peer held owner-but-unmounted, fan-out reported nothing: delivered %d of %d keys",
				len(got.keys), settled)
		}
		requireMatchableRefusals(t, "remote-peer-mid-acquire", got)
		requireNoSilentPartial(t, "remote-peer-mid-acquire", got, settled)
	})

	t.Run("local-node-mid-acquire", func(t *testing.T) {
		n1, n2, settled := aggregatePair(t, unitCount, n, prefix)
		u := ownedUnitOn(t, n1.Cluster, n2.Cluster, n1.ID, unitCount, n, prefix)

		// The fan-out is issued ON the mid-acquire node, so its own leg takes
		// the in-process snapshot path rather than the wire.
		n1.Cluster.TestingClearMount(u)
		got := runAggregate(n1.Cluster, prefix, nil)

		if !got.refused() {
			t.Fatalf("local node held owner-but-unmounted, fan-out reported nothing: delivered %d of %d keys",
				len(got.keys), settled)
		}
		requireMatchableRefusals(t, "local-node-mid-acquire", got)
		requireNoSilentPartial(t, "local-node-mid-acquire", got, settled)
	})
}

// TestAcquiringReason_AggregateScanFnChannelMatchesErrAcquiring isolates the
// door the spec calls out as data-loss-adjacent: the refusal that crosses the
// fan-out boundary THROUGH the scan fn.
//
// The window is opened AFTER every peer has been snapshotted (from inside fn
// itself, with every leg held at a barrier until it is open), so the refusal
// CANNOT arrive in AggregateResult.Err - shale already ran fn for that peer. It
// has to surface from the iterator fn is holding, and it has to still be a real
// error carrying the sentinel when it gets there.
//
// Measured before the fix, this exact construction returned keys=121 with
// scanErr=nil for the peer's leg: a truncated peer scan delivered as an
// ordinary success value, through the one door that could have reported it.
// The union across legs still looked complete (the local node's stale overlap
// mount happened to cover the same keys), which is exactly why this test asserts
// on the CHANNEL rather than on the total: a consumer that got lucky about
// coverage is not a consumer that was told the truth.
//
// A refusal arriving here as a bare string, a non-error type, or an error whose
// chain was rebuilt would leave the consumer's type-switch inert, their
// whole-call retry unfired, and their partial set consumed as complete.
func TestAcquiringReason_AggregateScanFnChannelMatchesErrAcquiring(t *testing.T) {
	const (
		unitCount = 8
		n         = 200
		prefix    = "agg-sf-"
	)
	n1, n2, settled := aggregatePair(t, unitCount, n, prefix)
	u := ownedUnitOn(t, n1.Cluster, n2.Cluster, n2.ID, unitCount, n, prefix)

	// Hold the window open for the whole fan-out rather than clearing once, so
	// a reconcile re-mounting between the clear and the peer's scan cannot turn
	// a real gap into a flaky pass.
	var stop func()
	defer func() {
		if stop != nil {
			stop()
		}
	}()
	got := runAggregate(n1.Cluster, prefix, func() {
		stop = holdAcquiringWindow([]*sharedNode{n2}, u)
	})

	if len(got.scanErrs) == 0 {
		t.Fatalf("the peer was held owner-but-unmounted for the whole scan and NOTHING crossed the "+
			"scan-fn channel (delivered %d of %d distinct keys; AggregateResult.Err=%v). fn had already "+
			"been invoked for that peer, so AggregateResult.Err cannot carry this refusal: the scan fn "+
			"is the only door, and it stayed shut. A consumer type-switching on the value sees a normal "+
			"result and consumes a partial set as complete.", len(got.keys), settled, got.errs)
	}
	requireMatchableRefusals(t, "scan-fn-channel", got)
	requireNoSilentPartial(t, "scan-fn-channel", got, settled)
}

// TestAcquiringReason_AggregatePeerDownDoesNotMatchErrAcquiring is the negative
// control, and the reason the whole mechanism exists. A downed peer fails the
// fan-out with the SAME codes.Unavailable an acquiring refusal carries, so a
// consumer keying off the code would retry a real outage on its handoff budget.
// Only the reason separates them, and a fan-out refusal must not blur that
// distinction just because it travelled a different path than a point op's.
func TestAcquiringReason_AggregatePeerDownDoesNotMatchErrAcquiring(t *testing.T) {
	const (
		unitCount = 8
		n         = 200
		prefix    = "agg-down-"
	)
	n1, n2, _ := aggregatePair(t, unitCount, n, prefix)

	// Kill ONLY the peer's gRPC listener; its coordinator keeps running so the
	// ring still fans out to it and the leg fails at a real dial, not a
	// synthesized error.
	n2.stop()
	n2.stop = nil

	var got aggOutcome
	ok := waitUntil(10*time.Second, func() bool {
		got = runAggregate(n1.Cluster, prefix, nil)
		return got.refused()
	})
	if !ok {
		t.Fatalf("fan-out to a downed peer never reported a failure: delivered %d keys", len(got.keys))
	}
	for _, err := range got.allRefusals() {
		if errors.Is(err, cluster.ErrAcquiring) {
			t.Fatalf("a genuine peer-down fan-out failure matched cluster.ErrAcquiring; "+
				"the acquiring/outage distinction is broken on the fan-out path: %v (%T)", err, err)
		}
	}
}

// TestAcquiringReason_R2AggregateMatchesErrAcquiring is the R>1 half. The
// contract is replication-independent, so a consumer running replicated - which
// is the point of running shale - must get the same match a single-copy one
// does, on the fan-out path as on the point-op paths.
//
// The window is held open by the same background re-clear loop the other R=2
// gates use, so the reconcile cannot close it mid-call. Both entry directions
// are driven: whichever node holds replica-0, one makes the mid-acquire leg
// in-process and the other makes it a peer reached over real gRPC.
//
// It asserts the contract WITHOUT first demanding an unrefused baseline. On
// this R=2 shared-backing fixture the co-replicas fence each other's handles,
// so a fan-out here legitimately refuses through the pre-existing fence recode
// (fenceToTransient -> errUnitAcquiring) whether or not a window is held open.
// That is measured, pre-existing behaviour; requiring a quiet baseline would
// only skip the test. What must hold either way is what is asserted: every
// refusal, through either door, matches ErrAcquiring, and a short result is
// never silent.
func TestAcquiringReason_R2AggregateMatchesErrAcquiring(t *testing.T) {
	const unitCount = 8
	n1, n2, key, u := r2AcquiringPair(t, unitCount)
	nodes := []*sharedNode{n1, n2}
	prefix := key[:len("r2-ar-key-")]

	// Seed a set worth measuring a partial against. Writes need both replicas
	// at W=2, so a seed may be refused mid-transition; only the keys that were
	// actually acked become ground truth.
	seeded := map[string]struct{}{key: {}}
	for i := 1; i < 40; i++ {
		k := fmt.Sprintf("%s%03d", prefix, i)
		if err := n1.Cluster.Put([]byte(k), []byte("v")); err == nil {
			seeded[k] = struct{}{}
		}
	}

	for _, entry := range []struct {
		name string
		node *sharedNode
	}{{"entry=arr1", n1}, {"entry=arr2", n2}} {
		t.Run("Aggregate/"+entry.name, func(t *testing.T) {
			label := "Aggregate/" + entry.name

			stop := holdAcquiringWindow(nodes, u)
			defer stop()

			got := runAggregate(entry.node.Cluster, prefix, nil)
			requireMatchableRefusals(t, label, got)
			requireNoSilentPartial(t, label, got, len(seeded))
			if !got.refused() {
				t.Logf("%s: slipped through the window (served %d of %d keys)", label, len(got.keys), len(seeded))
			}
		})
	}
}
