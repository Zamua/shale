package cluster

// White-box pins for the property that makes "mounted here but never marked"
// inexpressible: the durable serving marker is published BY the mount table's
// serving transition, not by each mount path. A path that mounts without
// publishing serves normally, so the omission has no local symptom - it
// surfaces only as a predecessor elsewhere that stays Draining forever.
//
// Each production path that reaches the transition is pinned here, plus the
// transition's own contract (publishes once, at the caller's open epoch, with
// the mount visible and the table lock released).

import (
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// capturedPublish is one recorded serving-marker publish.
type capturedPublish struct {
	ru    storageunit.ReplicaUnit
	epoch storageunit.Epoch
	site  string
}

// TestMountServing_PublishIsPartOfTheTransition pins the transition contract:
// a mounted outcome publishes exactly once, at the epoch the caller mounted at,
// with the mount already visible and the table lock RELEASED (marker I/O under
// the mount lock would stall every routed op's mount lookup).
func TestMountServing_PublishIsPartOfTheTransition(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")
	target := ru(0, 0, 0)
	const opened = storageunit.Epoch(7)

	var (
		seen         []capturedPublish
		mountVisible bool
		lockFree     bool
	)
	c.mounts.publish = func(p storageunit.ReplicaUnit, epoch storageunit.Epoch, site string) {
		seen = append(seen, capturedPublish{p, epoch, site})
		// Read the table from ANOTHER goroutine: if the publish ran under mu,
		// this read blocks until the deadline instead of returning.
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, mountVisible = c.mounts.backendFor(p)
		}()
		select {
		case <-done:
			lockFree = true
		case <-time.After(2 * time.Second):
		}
	}

	if out := c.mounts.mountServing(target, memory.New(), opened, "unit-test"); out == mountSuperseded {
		t.Fatalf("mountServing on a live cluster must mount, got mountSuperseded")
	}

	if len(seen) != 1 {
		t.Fatalf("publishes = %d, want exactly 1", len(seen))
	}
	if seen[0].ru != target || seen[0].epoch != opened || seen[0].site != "unit-test" {
		t.Fatalf("published %+v, want {%v %d unit-test}", seen[0], target, opened)
	}
	if !lockFree {
		t.Fatalf("the publish ran while the mount-table lock was held: marker I/O must not run under it")
	}
	if !mountVisible {
		t.Fatalf("the publish ran before the mount was installed: a predecessor could release onto an absent mount")
	}
	if got, ok := c.mounts.openEpochOf(target); !ok || got != opened {
		t.Fatalf("recorded open epoch = (%d, %v), want (%d, true)", got, ok, opened)
	}
}

// TestMountServing_SupersededPublishesNothing: Close ran before the transition,
// so nothing was mounted - publishing then would announce a serving owner that
// does not exist and release a predecessor onto no one.
func TestMountServing_SupersededPublishesNothing(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")
	target := ru(0, 0, 0)

	var seen []capturedPublish
	c.mounts.publish = func(p storageunit.ReplicaUnit, epoch storageunit.Epoch, site string) {
		seen = append(seen, capturedPublish{p, epoch, site})
	}
	c.closed.Store(true)

	if out := c.mounts.mountServing(target, memory.New(), 7, "unit-test"); out != mountSuperseded {
		t.Fatalf("mountServing on a closing cluster = %v, want mountSuperseded", out)
	}
	if len(seen) != 0 {
		t.Fatalf("a superseded transition published %+v, want nothing", seen)
	}
	if _, mounted := c.mounts.backendFor(target); mounted {
		t.Fatalf("a superseded transition must install no mount")
	}
	if _, ok := c.mounts.openEpochOf(target); ok {
		t.Fatalf("a superseded transition must leave no recorded open epoch")
	}
}

// TestBootMount_PublishesServingMarker pins the boot path through the
// PRODUCTION mount (not a re-implementation of its steps). Where pods are
// REPLACED rather than restarted in place, boot is the ordinary way a position
// is established, so the reconcile-time acquire may never run: a predecessor
// draining the position would then poll for a marker no path ever writes.
func TestBootMount_PublishesServingMarker(t *testing.T) {
	backing := sharedfactory.NewBacking()
	// Single-member ring: n1 owns a position for every unit.
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1")

	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}
	mounted := c.mounts.mountedList()
	if len(mounted) == 0 {
		t.Fatalf("boot mounted nothing; the test proves nothing")
	}
	for _, target := range mounted {
		opened, ok := c.mounts.openEpochOf(target)
		if !ok {
			t.Fatalf("boot mounted %v without recording an open epoch", target)
		}
		marker, ok := backing.ServingMarker(target)
		if !ok {
			t.Fatalf("boot mounted %v and published NO serving marker: a predecessor draining it polls forever", target)
		}
		// A marker BELOW the open epoch is worse than none: the drain gate is
		// strictly greater, so it never releases while looking satisfiable.
		if marker != opened {
			t.Fatalf("serving marker for %v = %d, want this node's exact open epoch %d", target, marker, opened)
		}
	}
}

// TestFinishStuckFlip_PublishesServingMarker: a MOUNTED position left in a
// gainer phase by a raced flip becomes locally serving when the phase is
// dropped, so that transition publishes too - nothing else will, the mount
// having already happened.
func TestFinishStuckFlip_PublishesServingMarker(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")
	target := ru(0, 0, 0)
	const opened = storageunit.Epoch(3)

	// The half-done flip: mounted + open epoch recorded + still Acquiring, with
	// no acquire goroutine in flight.
	c.mounts.mountUndecorated(target, memory.New())
	c.mounts.recordOpenEpoch(target, opened)
	c.beginAcquire(target)

	c.finishStuckFlipIfNeeded(target)

	if st := c.handoffPhaseOf(target); st.Phase != 0 {
		t.Fatalf("finishing a stuck flip must converge to Owned (no phase), got %v", st.Phase)
	}
	marker, ok := backing.ServingMarker(target)
	if !ok {
		t.Fatalf("finishing a stuck flip must publish the serving marker")
	}
	if marker != opened {
		t.Fatalf("serving marker = %d, want this node's exact open epoch %d", marker, opened)
	}
}

// -- the bug-class pin: every serving path, driven end to end ---------------

// openedEpochRecorder wraps the shared-backing factory to capture, per replica
// position, the epoch OpenUnit RETURNED to this node and every serving-marker
// epoch published for it. Wrapping the FACTORY rather than the table's publish
// hook is what makes the epoch assertion meaningful: the two values compared
// come from the two ends of the contract (what the open handed back, what the
// marker carries) with the whole production path in between.
type openedEpochRecorder struct {
	storageunit.BackendFactory
	mu        sync.Mutex
	opened    map[storageunit.ReplicaUnit]storageunit.Epoch
	published map[storageunit.ReplicaUnit][]storageunit.Epoch
}

func newOpenedEpochRecorder(inner storageunit.BackendFactory) *openedEpochRecorder {
	return &openedEpochRecorder{
		BackendFactory: inner,
		opened:         make(map[storageunit.ReplicaUnit]storageunit.Epoch),
		published:      make(map[storageunit.ReplicaUnit][]storageunit.Epoch),
	}
}

func (r *openedEpochRecorder) OpenUnit(m storageunit.MountRef, epoch storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	b, opened, err := r.BackendFactory.OpenUnit(m, epoch)
	if err == nil && m.Replicated() {
		r.mu.Lock()
		r.opened[m.ReplicaUnit()] = opened
		r.mu.Unlock()
	}
	return b, opened, err
}

func (r *openedEpochRecorder) WriteServingMarker(m storageunit.MountRef, epoch storageunit.Epoch) error {
	r.mu.Lock()
	r.published[m.ReplicaUnit()] = append(r.published[m.ReplicaUnit()], epoch)
	r.mu.Unlock()
	return r.BackendFactory.WriteServingMarker(m, epoch)
}

func (r *openedEpochRecorder) openedAt(ru storageunit.ReplicaUnit) (storageunit.Epoch, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.opened[ru]
	return e, ok
}

func (r *openedEpochRecorder) publishedFor(ru storageunit.ReplicaUnit) []storageunit.Epoch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]storageunit.Epoch(nil), r.published[ru]...)
}

// newRecordingReplicatedCluster is newReplicatedCluster with the factory
// wrapped in an openedEpochRecorder. Swapping the factory after construction is
// safe: the table's publish hook resolves c.factory at call time.
func newRecordingReplicatedCluster(t *testing.T, self string, n, r int, backing *sharedfactory.Backing, memberIDs ...string) (*Cluster, *openedEpochRecorder) {
	t.Helper()
	c := newReplicatedCluster(t, self, n, r, backing, memberIDs...)
	rec := newOpenedEpochRecorder(c.factory)
	c.factory = rec
	return c, rec
}

// TestServingMount_EveryPathPublishesItsOwnOpenEpoch is THE BUG-CLASS PIN: for
// every production route to "locally serving at a replica position", the
// position ends up mounted AND marked, at the epoch its own open returned.
//
// The list of rows is what makes it exhaustive, so a NEW serving route must add
// one. The structural half (a route cannot reach serving through a
// non-publishing mount helper in the first place) is pinned in
// serving_publish_structure_internal_test.go.
func TestServingMount_EveryPathPublishesItsOwnOpenEpoch(t *testing.T) {
	paths := []struct {
		name    string
		self    string
		members []string
		// drive runs the production entry point and returns the positions it
		// made locally serving.
		drive func(t *testing.T, c *Cluster) []storageunit.ReplicaUnit
	}{
		{
			// Boot: where pods are REPLACED rather than restarted in place, this
			// is the ordinary way a position is established, so a predecessor
			// draining it may have no other route that could satisfy its poll.
			name: "boot", self: "n1", members: []string{"n1"},
			drive: func(t *testing.T, c *Cluster) []storageunit.ReplicaUnit {
				if err := c.mountReplicaUnits(); err != nil {
					t.Fatalf("mountReplicaUnits: %v", err)
				}
				return c.mounts.mountedList()
			},
		},
		{
			// Clean-cut acquire: the reconcile's RELEASE-then-ACQUIRE branch.
			name: "acquire", self: "self", members: []string{"self", "n2", "n3"},
			drive: func(_ *testing.T, c *Cluster) []storageunit.ReplicaUnit {
				target := ru(0, 0, 0)
				c.acquireReplicaUnit(target)
				return []storageunit.ReplicaUnit{target}
			},
		},
		{
			// Overlap gainer: the background bounded acquire every real
			// successor mount routes through.
			name: "overlap", self: "self", members: []string{"self", "n2", "n3"},
			drive: func(_ *testing.T, c *Cluster) []storageunit.ReplicaUnit {
				target := ru(0, 0, 0)
				c.beginAcquire(target)
				c.acquireReplicaUnitOverlap(target)
				c.loopWG.Wait()
				return []storageunit.ReplicaUnit{target}
			},
		},
		{
			// Overlap rearm: a MOUNTED position left in a gainer phase by a
			// raced flip becomes serving when the phase drops. The mount already
			// happened, so nothing else can publish for it.
			name: "overlap-rearm", self: "self", members: []string{"self", "n2", "n3"},
			drive: func(t *testing.T, c *Cluster) []storageunit.ReplicaUnit {
				target := ru(0, 0, 0)
				b, opened, err := c.factory.OpenUnit(storageunit.ReplicaMount(target), acquireBaseEpoch)
				if err != nil {
					t.Fatalf("seed open: %v", err)
				}
				// The half-done flip: mounted + epoch recorded + still Acquiring,
				// with no acquire goroutine in flight.
				c.mounts.mountUndecorated(target, b)
				c.mounts.recordOpenEpoch(target, opened)
				c.beginAcquire(target)
				c.finishStuckFlipIfNeeded(target)
				return []storageunit.ReplicaUnit{target}
			},
		},
	}

	for _, p := range paths {
		t.Run(p.name, func(t *testing.T) {
			backing := sharedfactory.NewBacking()
			c, rec := newRecordingReplicatedCluster(t, p.self, 4, 2, backing, p.members...)

			serving := p.drive(t, c)
			if len(serving) == 0 {
				t.Fatalf("the %s row made nothing serving; it proves nothing", p.name)
			}
			for _, target := range serving {
				assertServingAndMarked(t, c, rec, backing, target)
			}
		})
	}
}

// assertServingAndMarked pins the whole contract for one position that just
// became locally serving: mounted, marked exactly once, and marked at the epoch
// THIS node's open returned (which is both its drain gate and what a
// predecessor's strict > release gate is measured against).
func assertServingAndMarked(t *testing.T, c *Cluster, rec *openedEpochRecorder, backing *sharedfactory.Backing, target storageunit.ReplicaUnit) {
	t.Helper()
	if _, mounted := c.mounts.backendFor(target); !mounted {
		t.Fatalf("%v is not mounted: no serving transition was driven", target)
	}
	opened, ok := rec.openedAt(target)
	if !ok {
		t.Fatalf("no OpenUnit recorded for %v: nothing to compare the marker epoch against", target)
	}
	marks := rec.publishedFor(target)
	if len(marks) == 0 {
		t.Fatalf("%v became locally serving and published NO serving marker: a predecessor "+
			"draining it polls for a signal that is never coming and stays Draining forever", target)
	}
	if len(marks) != 1 {
		t.Fatalf("%v published %d serving markers %v, want exactly 1", target, len(marks), marks)
	}
	if marks[0] != opened {
		t.Fatalf("%v published marker epoch %d, want THIS node's open epoch %d "+
			"(a lower epoch never satisfies a predecessor's strict > gate; a higher one "+
			"can release a predecessor onto an owner that is not this mount)", target, marks[0], opened)
	}
	if got, ok := c.mounts.openEpochOf(target); !ok || got != opened {
		t.Fatalf("%v recorded open epoch = (%d, %v), want (%d, true): the marker epoch and the "+
			"drain gate must be the same value", target, got, ok, opened)
	}
	durable, ok := backing.ServingMarker(target)
	if !ok {
		t.Fatalf("%v published a marker that never reached durable storage", target)
	}
	if durable != opened {
		t.Fatalf("%v durable marker = %d, want %d", target, durable, opened)
	}
}

// -- the regression check: the marker epoch is still the right one ----------

// TestServingMarker_CarriesOwnOpenEpoch_NotTheClimbingDurable discriminates the
// two candidate epochs at a moment they differ: this node opened at 3, a peer
// has since opened at 9 (the shared durable epoch climbs on ANY node's open).
// The marker must carry 3. Publishing the durable instead would announce an
// epoch this node never served at, releasing a predecessor sitting between the
// two onto an owner that has not reached serving.
func TestServingMarker_CarriesOwnOpenEpoch_NotTheClimbingDurable(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")
	target := ru(0, 0, 0)

	b, ownEpoch, err := c.factory.OpenUnit(storageunit.ReplicaMount(target), 3)
	if err != nil {
		t.Fatalf("own open: %v", err)
	}
	if ownEpoch != 3 {
		t.Fatalf("own open epoch = %d, want 3", ownEpoch)
	}
	c.mounts.mountUndecorated(target, b)
	c.mounts.recordOpenEpoch(target, ownEpoch)
	c.beginAcquire(target)

	// A peer opens the same position: the shared durable epoch climbs well above
	// this node's open epoch while this node's mount is untouched.
	peer := backing.Handle()
	if _, peerEpoch, err := peer.OpenReplicaUnit(target, 9); err != nil {
		t.Fatalf("peer open: %v", err)
	} else if peerEpoch != 9 {
		t.Fatalf("peer open epoch = %d, want 9", peerEpoch)
	}

	c.finishStuckFlipIfNeeded(target)

	marker, ok := backing.ServingMarker(target)
	if !ok {
		t.Fatalf("the serving transition published no marker")
	}
	if marker != ownEpoch {
		t.Fatalf("serving marker = %d, want this node's own open epoch %d (the climbing durable was %d)",
			marker, ownEpoch, 9)
	}
}

// TestServingMarker_ReleasesADrainingPredecessor is the epoch regression check
// in the terms that matter: a real predecessor Draining at its own open epoch
// releases when, and only when, the successor's mount publishes. A marker at a
// wrong (0, stale, or predecessor-equal) epoch leaves the drain hanging with no
// local symptom on either node.
func TestServingMarker_ReleasesADrainingPredecessor(t *testing.T) {
	backing := sharedfactory.NewBacking()
	pred := newReplicatedCluster(t, "pred", 4, 2, backing, "pred", "succ", "n3")
	succ := newReplicatedCluster(t, "succ", 4, 2, backing, "pred", "succ", "n3")
	target := ru(0, 0, 0)

	b, predEpoch, err := pred.factory.OpenUnit(storageunit.ReplicaMount(target), acquireBaseEpoch)
	if err != nil {
		t.Fatalf("predecessor open: %v", err)
	}
	pred.mounts.mountUndecorated(target, b)
	pred.mounts.recordOpenEpoch(target, predEpoch)
	pred.beginDrain(target)

	st := pred.handoffPhaseOf(target)
	if st.Phase != storageunit.PhaseDraining {
		t.Fatalf("predecessor phase = %v, want Draining", st.Phase)
	}
	if st.OpenEpoch != predEpoch {
		t.Fatalf("drain gate = %d, want the predecessor's own open epoch %d", st.OpenEpoch, predEpoch)
	}

	// No successor yet: the predecessor keeps serving.
	pred.drainCheck(target)
	if _, mounted := pred.mounts.backendFor(target); !mounted {
		t.Fatalf("the predecessor released with no successor marker")
	}

	succ.acquireReplicaUnit(target)

	marker, ok := backing.ServingMarker(target)
	if !ok {
		t.Fatalf("the successor mounted and published no serving marker")
	}
	if marker <= predEpoch {
		t.Fatalf("successor marker = %d, must be STRICTLY above the predecessor's open epoch %d "+
			"or the drain never releases", marker, predEpoch)
	}

	pred.drainCheck(target)
	if _, mounted := pred.mounts.backendFor(target); mounted {
		t.Fatalf("the predecessor did not release on the successor's marker")
	}
	if st := pred.handoffPhaseOf(target); st.Phase != 0 {
		t.Fatalf("after release the phase entry must be dropped, got %v", st.Phase)
	}
}
