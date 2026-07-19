package integration

// The EXHAUSTION half of the cross-shard scan refusal.
//
// localScanMounted refuses UP FRONT when this node's mount map does not cover
// the positions it owns. That check is what the other Aggregate tests exercise:
// they open the acquiring window BEFORE the peer's scan starts, so the peer
// refuses while opening the stream and the refusal reaches the originator on
// the stream's very first Recv.
//
// This test covers the OTHER check - the one the chained iterator performs when
// it runs out of units:
//
//	return nil, nil, it.c.scanCoverageErr("LocalScan")   // mountedIterator.Next
//
// That line is the ONLY thing standing between "this node lost coverage while
// streaming" and a clean end-of-iteration. On the PEER path it is also the only
// thing that carries the refusal across the fan-out boundary once fn is already
// running: the peer's LocalScan handler returns it, gRPC turns it into a stream
// error, and the originator's iterator hands it to the caller's scan fn. Delete
// the line and the handler returns nil instead, the stream ends with io.EOF, and
// the scan fn sees an ordinary exhausted iterator.
//
// The window therefore has to open AFTER the peer's scan has begun. A barrier on
// the fan-out legs (the technique
// TestAcquiringReason_AggregateScanFnChannelMatchesErrAcquiring uses) is not
// late enough: it releases before fn issues its prefixed ScanPrefix, so the
// peer's UP-FRONT check still catches it and the exhaustion re-check is never
// consulted. This test pauses the peer INSIDE its own scan instead - the first
// Next of the first mounted unit's iterator blocks - which proves the up-front
// check has already run and passed, opens the window while the peer is held
// there, then lets the peer drain to exhaustion.
//
// WHAT THIS TEST DOES AND DOES NOT MEASURE. It asserts on the CHANNEL, not on
// the key count, and that is deliberate rather than a weakening. The pause seam
// cannot manufacture a genuinely truncated peer scan: mountedIterator snapshots
// its unit list (c.mountedUnits()) when it is built, so clearing a mount
// mid-stream does not take those keys away from the scan already in flight, and
// the fan-out here delivers a complete set. What the seam DOES reproduce
// faithfully is the state the guard exists for - a node that no longer covers
// the positions it owns, reaching the end of its iteration - and the assertion
// is the guard's whole job: at that moment the peer must raise a matchable
// error through the scan fn rather than signal a clean end. In production the
// gap that state implies is real (a position acquired concurrently is absent
// from the snapshot entirely, so its keys were never in the stream); the guard
// is deliberately conservative and refuses on the evidence, not on proof that
// keys were lost.

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

// scanPause is one armed pause: the next unit-level ScanPrefix for the armed
// prefix is intercepted and its first Next blocks, announcing arrival on
// entered and resuming when release closes.
//
// It is a TIMING seam only. It never injects an error, never alters what the
// scan returns, and never touches the mount map - it just holds the peer's
// scan still long enough for the test to change the world underneath it. The
// outcome under test is produced entirely by production code.
type scanPause struct {
	prefix  []byte
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	relOnce sync.Once
}

func newScanPause(prefix string) *scanPause {
	p := &scanPause{
		prefix:  []byte(prefix),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	p.armed.Store(true)
	return p
}

// claim consumes the arm for the first scan of the armed prefix, so exactly one
// scan is paused and every later scan (the reconcile's, a retry's) runs free.
func (p *scanPause) claim(prefix []byte) bool {
	if !bytes.Equal(prefix, p.prefix) {
		return false
	}
	return p.armed.CompareAndSwap(true, false)
}

// hold blocks the calling (peer-side) scan goroutine until releaseScan. The
// bounded wait keeps a mis-sequenced test from wedging a node goroutine.
func (p *scanPause) hold() {
	p.once.Do(func() {
		close(p.entered)
		select {
		case <-p.release:
		case <-time.After(20 * time.Second):
		}
	})
}

func (p *scanPause) releaseScan() { p.relOnce.Do(func() { close(p.release) }) }

func (p *scanPause) fired() bool {
	select {
	case <-p.entered:
		return true
	default:
		return false
	}
}

// scanPauser holds the pause currently installed on a node's factory. Fresh
// pauses are installed per attempt, so a retry gets a clean pair of channels.
type scanPauser struct {
	mu     sync.Mutex
	active *scanPause
}

func (s *scanPauser) install(p *scanPause) {
	s.mu.Lock()
	s.active = p
	s.mu.Unlock()
}

func (s *scanPauser) claim(prefix []byte) *scanPause {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil || !s.active.claim(prefix) {
		return nil
	}
	return s.active
}

// pausingFactory wraps a node's BackendFactory so the unit backends it mounts
// can pause a scan mid-flight. It wraps the ONE storage port, so it is
// layout-agnostic: whichever MountRef the cluster opens is passed straight
// through and the wrapper never has to know whether this fixture is R=1 or
// R>1.
type pausingFactory struct {
	storageunit.BackendFactory
	pauser *scanPauser
}

func (f *pausingFactory) OpenUnit(m storageunit.MountRef, epoch storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	b, opened, err := f.BackendFactory.OpenUnit(m, epoch)
	if err != nil {
		return nil, 0, err
	}
	return &pausingBackend{Backend: b, pauser: f.pauser}, opened, nil
}

type pausingBackend struct {
	backend.Backend
	pauser *scanPauser
}

func (b *pausingBackend) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	it, err := b.Backend.ScanPrefix(prefix)
	if err != nil {
		return nil, err
	}
	if p := b.pauser.claim(prefix); p != nil {
		return &pausingIterator{Iterator: it, pause: p}, nil
	}
	return it, nil
}

// Flush forwards the optional Flusher capability the wrapper would otherwise
// hide from the cluster's displacement-flush type assertion. A backend that
// does not implement it is skipped silently by that caller anyway, so
// reporting success for one is the same no-op.
func (b *pausingBackend) Flush() error {
	if fl, ok := b.Backend.(backend.Flusher); ok {
		return fl.Flush()
	}
	return nil
}

type pausingIterator struct {
	backend.Iterator
	pause *scanPause
}

func (it *pausingIterator) Next() ([]byte, []byte, error) {
	// Blocking on the FIRST Next (not on ScanPrefix) is what makes the premise
	// checkable: reaching here means the peer's LocalScan handler already ran
	// localScanMounted's up-front scanCoverageErr and it PASSED, so any refusal
	// that follows can only have come from the exhaustion re-check.
	it.pause.hold()
	return it.Iterator.Next()
}

// gatedAggregatePair is aggregatePair with the SECOND node's factory wrapped in
// a pause seam, so the peer leg of a fan-out can be held still mid-scan. It
// returns the pauser, the peer's owned unit, and the settled distinct-key total
// the fan-out delivers when nothing is mid-acquire.
func gatedAggregatePair(t *testing.T, unitCount, n int, prefix string) (n1, n2 *sharedNode, pauser *scanPauser, u storageunit.UnitID, settled int) {
	t.Helper()

	pauser = &scanPauser{}
	shortPeerConnect := func(cfg *cluster.Config) { cfg.PeerConnectTimeout = 750 * time.Millisecond }
	withPause := func(cfg *cluster.Config) {
		shortPeerConnect(cfg)
		cfg.BackendFactory = &pausingFactory{BackendFactory: cfg.BackendFactory, pauser: pauser}
	}

	backing := sharedfactory.NewBacking()
	n1 = startReplicatedNodeCfg(t, "agx1", "", unitCount, 1, backing, shortPeerConnect)
	n2 = startReplicatedNodeCfg(t, "agx2", n1.BindAddr, unitCount, 1, backing, withPause)

	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("ring convergence: %v", err)
	}
	if !waitUntil(10*time.Second, func() bool { return len(n2.Handle.OpenUnits()) > 0 }) {
		t.Fatalf("no unit ever handed off to agx2: n2 open=%v", n2.Handle.OpenUnits())
	}

	for i := range n {
		// Same %04d shape ownedUnitOn re-derives its candidate keys with.
		k := fmt.Sprintf("%s%04d", prefix, i)
		if err := n1.Cluster.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("seed Put %s: %v", k, err)
		}
	}

	var base aggOutcome
	if !waitUntil(15*time.Second, func() bool {
		base = runAggregate(n1.Cluster, prefix, nil)
		return !base.refused() && len(base.keys) == n
	}) {
		t.Fatalf("baseline fan-out never delivered the %d seeded keys (got %d distinct, errs=%v scanErrs=%v): "+
			"the fixture is not settled", n, len(base.keys), base.errs, base.scanErrs)
	}

	u = ownedUnitOn(t, n1.Cluster, n2.Cluster, n2.ID, unitCount, n, prefix)
	return n1, n2, pauser, u, len(base.keys)
}

// TestAcquiringReason_PeerScanExhaustionRefusalCrossesScanFn pins the chained
// iterator's exhaustion re-check on the PEER path.
//
// Sequence, all of it forced rather than raced:
//
//  1. The fan-out's scan fn issues a prefixed ScanPrefix against the peer. The
//     peer's LocalScan handler runs localScanMounted, whose up-front coverage
//     check PASSES (nothing is mid-acquire yet) and hands back a chained
//     iterator over the units mounted at that instant.
//  2. The peer's first Next blocks in the pause seam. The scan is now
//     unambiguously in flight and past its up-front check.
//  3. The test clears the peer's mount for a unit it OWNS, putting it in the
//     real owner-but-unmounted state, and keeps re-clearing so the reconcile
//     cannot close the window.
//  4. The peer resumes, streams what it holds, and runs out of units.
//
// At step 4 the only remaining opportunity to tell the truth is the exhaustion
// re-check. It must convert the end of the iteration into a stream error that
// arrives INSIDE the caller's scan fn still matching cluster.ErrAcquiring.
//
// Removing that one line (returning a clean nil,nil,nil instead) leaves every
// other ErrAcquiring test green and makes this one fail: the peer's leg comes
// back as a perfectly ordinary aggScan with scanErr == nil.
func TestAcquiringReason_PeerScanExhaustionRefusalCrossesScanFn(t *testing.T) {
	const (
		unitCount = 8
		n         = 200
		prefix    = "agg-ex-"
		attempts  = 5
	)

	n1, n2, pauser, u, settled := gatedAggregatePair(t, unitCount, n, prefix)

	// The reconcile can re-mount between the clear and the peer's exhaustion,
	// which closes the window early and produces a legitimate clean end. That is
	// a HARNESS race, not the behaviour under test, so a few attempts are
	// allowed. It cannot launder a real regression: with the exhaustion re-check
	// removed, NO attempt can ever produce a scan-fn refusal.
	var everFired bool
	for attempt := 1; attempt <= attempts; attempt++ {
		// Every attempt must START settled. A previous attempt leaves the peer
		// owner-but-unmounted, and a peer in that state refuses at
		// localScanMounted's UP-FRONT check - it never reaches a unit backend, so
		// the seam never fires and the attempt would measure the wrong check.
		if !waitUntil(15*time.Second, func() bool { return n2.Cluster.MountReadiness().PendingUnits == 0 }) {
			t.Fatalf("attempt %d: the peer never re-mounted its owned positions; cannot start from a "+
				"settled fixture (readiness=%+v)", attempt, n2.Cluster.MountReadiness())
		}

		pause := newScanPause(prefix)
		pauser.install(pause)

		var stopWindow func()
		opened := make(chan struct{})
		abort := make(chan struct{})
		go func() {
			defer close(opened)
			select {
			case <-pause.entered:
			case <-abort:
				// The fan-out finished without the peer entering the seam; release
				// so nothing wedges and let the premise check below report it.
				pause.releaseScan()
				return
			}
			// Clear ONCE synchronously before resuming the peer, so the window is
			// provably open when the scan continues, then hold it open against the
			// reconcile for the rest of the call.
			n2.Cluster.TestingClearMount(u)
			stopWindow = holdAcquiringWindow([]*sharedNode{n2}, u)
			pause.releaseScan()
		}()

		got := runAggregate(n1.Cluster, prefix, nil)
		close(abort)
		<-opened
		if stopWindow != nil {
			stopWindow()
		}

		// PREMISE: the peer must have been paused INSIDE its own scan, which is
		// what proves its up-front coverage check already ran and passed. An
		// attempt that never reached the seam measured nothing, so it is retried
		// rather than asserted on (and never counted as a pass).
		if !pause.fired() {
			t.Logf("attempt %d: the peer's scan never entered the pause seam (delivered %d of %d keys, "+
				"AggregateResult.Err=%v); the window would have been opened at an unknown point relative "+
				"to the up-front check, so this attempt proves nothing; retrying",
				attempt, len(got.keys), settled, got.errs)
			continue
		}
		everFired = true

		if len(got.scanErrs) == 0 {
			t.Logf("attempt %d: window closed before the peer exhausted (delivered %d of %d distinct keys, "+
				"AggregateResult.Err=%v); retrying", attempt, len(got.keys), settled, got.errs)
			continue
		}

		// Whatever came back through the scan fn must be a real, matchable error
		// carrying the sentinel and the client-facing code.
		requireMatchableRefusals(t, "peer-scan-exhaustion", got)
		t.Logf("attempt %d: the peer refused at exhaustion and the refusal reached the scan fn: %v",
			attempt, got.scanErrs)
		return
	}

	if !everFired {
		t.Fatalf("the peer's scan never once entered the pause seam across %d attempts, so this test "+
			"never established its precondition (a scan held mid-flight, past localScanMounted's up-front "+
			"coverage check). The exhaustion re-check was NOT exercised either way; fix the fixture rather "+
			"than reading this as a pass or a regression.", attempts)
	}
	t.Fatalf("the peer lost coverage while its scan was ALREADY STREAMING (past localScanMounted's "+
		"up-front check) on %d attempts and NOTHING ever crossed the scan-fn channel. The chained "+
		"iterator signalled a clean end of iteration instead of re-checking coverage at exhaustion, so a "+
		"fan-out consumer receives an ordinary exhausted iterator from a node that no longer covers the "+
		"positions it owns - the partial-looks-complete shape a referenced-set consumer cannot defend "+
		"against.", attempts)
}
