package rebalance_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/rebalance"
)

// TestReconcile_RepairsOwnedButMissing pins the founder-grows
// data-loss path: a joiner pins a self-only bootstrap ring snapshot
// under slow gossip, so the ring-vs-ring ComputePlan emits no Receive
// for partitions it owns in both the (self-only) old and the
// (converged) new snapshot. The keys physically live on the founder.
//
// The reconcile pass keyed on physical placement must detect each
// such partition (owned by self, zero keys held locally) and pull it
// from its prior owner (the founder), self-healing the orphan.
//
// We drive the Coordinator directly with a self-only old ring and a
// {founder, joiner} new ring, seed the founder's backend with keys the
// new ring assigns to the joiner, and assert those keys land on the
// joiner's backend after Evaluate + WaitForIdle.
func TestReconcile_RepairsOwnedButMissing(t *testing.T) {
	founder := "founder"
	joiner := "joiner"

	// The converged ring both nodes eventually see.
	newR := buildRing(founder, joiner)
	// The un-converged bootstrap snapshot the joiner pinned: self only.
	selfOnly := buildRing(joiner)

	// Partitions the converged ring assigns to the joiner. These are
	// the ones whose keys must end up on the joiner's backend.
	joinerParts := partitionsOwnedBy(newR, joiner)
	if len(joinerParts) == 0 {
		t.Fatal("expected the converged ring to assign some partitions to the joiner")
	}

	founderBE := memory.New()
	defer func() { _ = founderBE.Close() }()
	joinerBE := memory.New()
	defer func() { _ = joinerBE.Close() }()

	// Seed the founder with keys that the converged ring routes to the
	// joiner. In the real bug these were written while the founder was
	// alone (it owned everything); after the joiner joins they belong
	// to the joiner but physically still live on the founder.
	seeded := seedKeysOwnedBy(t, founderBE, newR, joiner, 25)

	// The founder's source the joiner pulls from. PartitionFn matches
	// the converged ring so the per-key filter agrees with what the
	// joiner asks for.
	partFn := func(k []byte) uint64 { return newR.PartitionID(k) }
	founderSource := rebalance.NewLocalSource(founderBE, partFn)

	// In-process destination: resolve any peer to the founder's source.
	dest := rebalance.NewInProcessDestination(joinerBE, func(_ rebalance.Member) (rebalance.MigrateSource, error) {
		return founderSource, nil
	})

	self := rebalance.Member{ID: joiner, Addr: "addr:" + joiner}
	opts := rebalance.DefaultOptions()
	opts.Destination = dest
	c := rebalance.New(self, joinerBE, opts)
	defer c.Stop()

	// Evaluate the founder-grows transition AS THE JOINER WOULD UNDER
	// SLOW GOSSIP: old = self-only snapshot, new = converged ring. The
	// ring-vs-ring diff sees old-owner == new-owner == self for the
	// joiner's partitions and emits no Receive. Only the reconcile pass
	// (physical placement) can repair them.
	c.Evaluate(selfOnly, newR, 1)

	// Poll the joiner's backend: the reconcile pass pulls the
	// owned-but-missing partitions asynchronously via runReceive. We
	// poll rather than WaitForIdle because the self-only old snapshot
	// also produces spurious empty Sends (partitions the converged ring
	// hands to the founder), which are a harmless side effect of the
	// un-converged baseline and not what this test asserts.
	deadline := time.Now().Add(3 * time.Second)
	missing := len(seeded)
	for time.Now().Before(deadline) && missing > 0 {
		missing = 0
		for _, kv := range seeded {
			if _, err := joinerBE.Get(kv[0]); err != nil {
				missing++
			}
		}
		if missing > 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if missing > 0 {
		t.Fatalf("%d/%d owned-but-missing keys not repaired by reconcile pass; snapshot: %+v",
			missing, len(seeded), c.Snapshot())
	}
}

// TestReconcile_QuiescesAfterFirstPass verifies the reconcile pass
// converges: once it has drained every owned partition (whether by
// pulling real keys or by a harmless empty pull of a never-written
// partition), a SUBSEQUENT Evaluate on the same stable ring issues NO
// further FetchRange calls. This is the spec's "pass quiesces once
// every owned partition is physically held" property and the guard
// against re-pulling empty partitions on every settle tick.
//
// The first Evaluate may legitimately empty-pull owned partitions that
// no key has ever hashed into (the spec accepts those as harmless);
// what must NOT happen is that the pass keeps re-issuing them forever.
func TestReconcile_QuiescesAfterFirstPass(t *testing.T) {
	r := buildRing("n1", "n2")
	self := rebalance.Member{ID: "n1", Addr: "addr:n1"}

	be := memory.New()
	defer func() { _ = be.Close() }()

	// Seed n1's backend with the keys it owns, so it physically holds
	// every NON-EMPTY partition the ring assigns to it. The empty ones
	// (no key ever hashed in) are what the quiescence guard covers.
	seedKeysOwnedBy(t, be, r, "n1", 30)

	dest := &fetchSpy{}
	opts := rebalance.DefaultOptions()
	opts.Destination = dest
	c := rebalance.New(self, be, opts)
	defer c.Stop()

	// First pass: may empty-pull some owned-but-never-written
	// partitions. Drain to terminal.
	c.Evaluate(r, r, 1)
	if err := c.WaitForIdle(timeout(t, 2*time.Second)); err != nil {
		t.Fatalf("WaitForIdle pass 1: %v", err)
	}
	firstCalls := dest.calls.Load()

	// Second pass on the identical stable ring: the pass must now be
	// idle. Zero NEW FetchRange calls.
	c.Evaluate(r, r, 2)
	if err := c.WaitForIdle(timeout(t, 2*time.Second)); err != nil {
		t.Fatalf("WaitForIdle pass 2: %v", err)
	}
	if newCalls := dest.calls.Load() - firstCalls; newCalls != 0 {
		t.Fatalf("reconcile pass re-issued %d FetchRange call(s) on a settled ring; want 0 (pass must quiesce)", newCalls)
	}
}

// fetchSpy counts FetchRange invocations + always succeeds with zero
// keys. Used to assert the reconcile pass does or does not pull.
type fetchSpy struct {
	calls atomic.Int64
}

func (f *fetchSpy) FetchRange(_ context.Context, _ rebalance.Member, _ []uint64, _ uint64) (int, error) {
	f.calls.Add(1)
	return 0, nil
}
