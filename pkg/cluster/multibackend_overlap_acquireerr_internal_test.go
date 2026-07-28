package cluster

import (
	"errors"
	"strings"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
)

// These tests pin the ACQUIRE RESULT CHANNEL: acquireReplicaUnitOverlapBlocking
// reports success/failure through its ERROR RETURN, and the acquire record is only a
// diagnostic mirror of that outcome.
//
// The structure they guard: the re-drive loop in acquireReplicaUnitOverlap used
// to decide whether to retry by re-reading the acquire record for ru after the
// call. That map is shared by every mount site on the node - the mount seam clears
// the entry at the mount choke point, and the boot path writes a NON-error
// "boot-deferred:" string under the same key - so an unrelated path's write
// could decide this loop's branch: a concurrent mount of ru clearing the record
// would end the retry of a still-failing open, and a boot-defer record would
// make a successful open look failed. Routing the outcome through a return value
// makes the signal private to the call, so neither confusion is expressible.
//
// Compile-level guard: on the pre-fix void signature, the `err := c.acquire...`
// assignments below do not build at all.

// TestAcquireOverlapBlocking_ReturnsOpenError pins the FAILURE direction: an
// un-openable position surfaces the open error as the RETURN VALUE (the retry
// loop's branch condition), and mirrors the same text into the acquire record for
// /debug/shale/state + MountReadiness.
func TestAcquireOverlapBlocking_ReturnsOpenError(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	// Pick a position this node actually DESIRES, so the MountReadiness
	// assertion below (which counts over the desired set) sees it.
	desired := c.desiredReplicaUnits()
	if len(desired) == 0 {
		t.Fatalf("test needs this node to own at least one replica position")
	}
	target := desired[0]

	injected := errors.New("Data error: empty SSTable (injected)")
	backing.SetOpenReplicaFault(target, injected)

	err := c.acquireReplicaUnitOverlapBlocking(target)
	if err == nil {
		t.Fatalf("a failing open must RETURN the error (it is the re-drive loop's branch condition), got nil")
	}
	if !strings.Contains(err.Error(), "empty SSTable") {
		t.Fatalf("returned error = %v, want it to carry the injected open failure", err)
	}
	if _, mounted := c.localBackendForReplicaUnit(target); mounted {
		t.Fatalf("a failed open must not install a mount for %s", target)
	}

	// The diagnostic mirror is still populated: MountReadiness counts
	// FailedOpenUnits off it and DebugState prints it.
	s, ok := c.mounts.acquireErrOf(target)
	if !ok {
		t.Fatalf("the acquire record must still be populated for %s (MountReadiness reads it)", target)
	}
	if !strings.Contains(s, "empty SSTable") {
		t.Fatalf("the acquire record for %s = %q, want the open error", target, s)
	}
	if r := c.MountReadiness(); r.FailedOpenUnits == 0 || !strings.Contains(r.LastAcquireError, "empty SSTable") {
		t.Fatalf("MountReadiness must surface the failed open: %+v", r)
	}
}

// TestAcquireOverlapBlocking_ReturnsNilOnMount pins the SUCCESS direction: a
// healthy open returns nil (the loop stops retrying) and CLEARS the diagnostic
// record, so a later unmount cannot resurface a stale error.
//
// The pre-existing record stored below is the shape the boot path writes
// ("boot-deferred: ..." is not an acquire failure at all). The acquire must
// still report success.
func TestAcquireOverlapBlocking_ReturnsNilOnMount(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	target := ru(0, 0, 0)
	c.mounts.recordAcquireErr(target, "boot-deferred: a peer is serving this position (serving marker epoch 3)")

	if err := c.acquireReplicaUnitOverlapBlocking(target); err != nil {
		t.Fatalf("a successful mount must return nil, got %v", err)
	}
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("%s should be mounted after a successful acquire", target)
	}
	if v, ok := c.mounts.acquireErrOf(target); ok {
		t.Fatalf("a successful mount must clear the diagnostic record, still holds %q", v)
	}
}

// TestAcquireOverlapBlocking_ReturnsErrorAfterSelfHealRetry pins that the
// SELF-HEAL reopen (close the stale factory handle, reopen once) still reports
// its final outcome through the return value: a fault that outlives the reopen
// returns non-nil, and clearing the fault makes the next call return nil and
// mount - the transient-failure self-heal the re-drive loop exists to drive.
func TestAcquireOverlapBlocking_ReturnsErrorAfterSelfHealRetry(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	target := ru(0, 0, 0)
	backing.SetOpenReplicaFault(target, errors.New("transient store blip (injected)"))
	if err := c.acquireReplicaUnitOverlapBlocking(target); err == nil {
		t.Fatalf("a persistent fault must survive the self-heal reopen and return non-nil")
	}

	// Repair: the very next attempt (what the re-drive loop makes after its
	// backoff) must succeed and report nil.
	backing.SetOpenReplicaFault(target, nil)
	if err := c.acquireReplicaUnitOverlapBlocking(target); err != nil {
		t.Fatalf("after repair the re-drive attempt must return nil, got %v", err)
	}
	if _, mounted := c.localBackendForReplicaUnit(target); !mounted {
		t.Fatalf("%s should be mounted after the repaired retry", target)
	}
}
