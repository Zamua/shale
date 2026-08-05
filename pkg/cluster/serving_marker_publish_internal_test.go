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
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
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
