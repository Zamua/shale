package integration

// The R>1 half of the cluster.ErrAcquiring path-independence gate.
//
// At R=1 a refused op returns the acquiring-window refusal itself, so the
// identity is simply propagated. At R>1 it is NOT: a write is a FAN-OUT, and
// falling short of W collapses the legs into ONE freshly-minted status
// (classifyWriteAttempt). Any consumer running replicated - which is the point
// of running shale - therefore exercises a completely different terminal from
// the one the R=1 tests cover, and a gate that works at R=1 can silently fail
// there. These tests drive the PUBLIC API at R=2 so that gap cannot reopen.
//
// The window is held open deterministically: a background loop re-clears the
// replica-0 mount faster than the reconcile can restore it, leaving that node
// owner-but-unmounted (the real handoff state) for the whole call.
//
// Each entry point is driven from BOTH nodes. Whichever node holds replica-0,
// one direction makes the mid-acquire leg IN-PROCESS and the other makes it a
// leg forwarded over real gRPC, so the pair covers both halves of the contract
// without the test having to know which node ended up with the position.

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

// r2AcquiringPair brings up a settled 2-node R=2 cluster over shared backing,
// with short read/write budgets so a held-open handoff window surfaces its
// bounded terminal quickly instead of burning the default multi-second budget.
func r2AcquiringPair(t *testing.T, unitCount int) (n1, n2 *sharedNode, key string, u storageunit.UnitID) {
	t.Helper()

	shortBudgets := func(cfg *cluster.Config) {
		cfg.WriteTimeout = 600 * time.Millisecond
		cfg.ReadTimeout = 600 * time.Millisecond
	}
	backing := sharedfactory.NewBacking()
	n1 = startReplicatedNodeCfg(t, "arr1", "", unitCount, 2, backing, shortBudgets)
	n2 = startReplicatedNodeCfg(t, "arr2", n1.BindAddr, unitCount, 2, backing, shortBudgets)

	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("ring convergence: %v", err)
	}
	// Any key works at R=2 on a 2-node ring (both nodes are replicas of every
	// unit). A key that BOTH nodes can write is the settle condition worth
	// waiting on: it proves the replica positions are mounted on both sides and
	// W=2 is reachable, which is exactly the steady state the window is then
	// constructed against. (Mounts open lazily, so an OpenUnits poll would race.)
	uc := storageunit.MustUnitCount(unitCount)
	for i := range 64 {
		k := fmt.Sprintf("r2-ar-key-%03d", i)
		var wrote bool
		if !waitUntil(15*time.Second, func() bool {
			wrote = n1.Cluster.Put([]byte(k), []byte("seed")) == nil &&
				n2.Cluster.Put([]byte(k), []byte("seed")) == nil
			return wrote
		}) {
			continue
		}
		return n1, n2, k, storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), uc)
	}
	t.Fatal("no key could be seeded from both nodes on the settled R=2 pair")
	return nil, nil, "", 0
}

// holdAcquiringWindow keeps unit u's replica-0 mount cleared on both nodes
// until the returned stop func is called. Only the node that actually holds
// replica-0 has anything to clear; the other clear is a harmless no-op, so the
// caller does not need to know which is which. The co-replica keeps acking
// throughout, which is what makes the shortfall a PURE acquiring shortfall
// (acks below W with zero hard failures) rather than an outage.
func holdAcquiringWindow(nodes []*sharedNode, u storageunit.UnitID) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			for _, n := range nodes {
				n.Cluster.TestingClearMount(u)
			}
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	return func() {
		close(done)
		wg.Wait()
	}
}

// requireAcquiringOrServed is the harness discipline that keeps a race from
// being mistaken for a pass: an op that SLIPPED THROUGH the window (nil error)
// is reported and tolerated, but any non-nil error MUST match ErrAcquiring and
// MUST still be codes.Unavailable. A false negative therefore cannot hide
// behind "the window closed early".
func requireAcquiringOrServed(t *testing.T, label string, err error) {
	t.Helper()
	if err == nil {
		t.Logf("%s: slipped through the window (served); no error to classify", label)
		return
	}
	if !errors.Is(err, cluster.ErrAcquiring) {
		t.Fatalf("%s: R=2 acquiring-window refusal does NOT match cluster.ErrAcquiring; "+
			"a replicated consumer cannot gate on it: %v (%T)", label, err, err)
	}
	if code := status.Code(err); code != codes.Unavailable {
		t.Fatalf("%s: client-facing code moved: got %v want Unavailable. err=%v", label, code, err)
	}
	t.Logf("%s: OK: %v", label, err)
}

// TestAcquiringReason_R2WriteMatchesErrAcquiring is the blocking-gap regression.
// Before the fix, an R=2 Put into the handoff window returned a bare
// "shale: write needed 2 acks, got 1 (replicas mid-acquire)" status that wrapped
// nothing, so errors.Is(err, cluster.ErrAcquiring) was FALSE on both paths - a
// false negative on precisely the configuration a replicated consumer runs.
func TestAcquiringReason_R2WriteMatchesErrAcquiring(t *testing.T) {
	const unitCount = 8
	n1, n2, key, u := r2AcquiringPair(t, unitCount)
	nodes := []*sharedNode{n1, n2}

	// Both directions: one of them makes the mid-acquire leg in-process, the
	// other makes it a leg forwarded over gRPC.
	for _, entry := range []struct {
		name string
		node *sharedNode
	}{{"entry=arr1", n1}, {"entry=arr2", n2}} {
		t.Run("Put/"+entry.name, func(t *testing.T) {
			stop := holdAcquiringWindow(nodes, u)
			defer stop()
			err := entry.node.Cluster.Put([]byte(key), []byte("v"))
			if err == nil {
				t.Fatal("R=2 Put with a replica held mid-acquire: got an ack, want a refusal")
			}
			requireAcquiringOrServed(t, "Put/"+entry.name, err)
		})

		t.Run("Delete/"+entry.name, func(t *testing.T) {
			stop := holdAcquiringWindow(nodes, u)
			defer stop()
			err := entry.node.Cluster.Delete([]byte(key))
			if err == nil {
				t.Fatal("R=2 Delete with a replica held mid-acquire: got an ack, want a refusal")
			}
			requireAcquiringOrServed(t, "Delete/"+entry.name, err)
		})
	}
}

// TestAcquiringReason_R2AllEntryPointsMatchErrAcquiring drives the whole
// surface the consumer calls. Reads may legitimately be SERVED at R=2 (the
// co-replica still holds the data), so they are asserted with the
// slipped-through-vs-mismatch discipline rather than being required to fail;
// what must never happen is a non-nil error that does not match.
//
// Reads have no SYNTHESIZED terminal to lose the reason in, which is why they
// need no R>1-specific fix: when every union leg is mid-handoff at once, the
// read path returns errUnitAcquiring VERBATIM (readReplicatedUnit's
// all-legs-transient branch) rather than minting a fresh status the way the
// write fan-out's collapse did. The R=1 gate covers that refusal directly.
func TestAcquiringReason_R2AllEntryPointsMatchErrAcquiring(t *testing.T) {
	const unitCount = 8
	n1, n2, key, u := r2AcquiringPair(t, unitCount)
	nodes := []*sharedNode{n1, n2}

	for _, entry := range []struct {
		name string
		node *sharedNode
	}{{"entry=arr1", n1}, {"entry=arr2", n2}} {
		t.Run("Get/"+entry.name, func(t *testing.T) {
			stop := holdAcquiringWindow(nodes, u)
			defer stop()
			_, err := entry.node.Cluster.Get([]byte(key))
			requireAcquiringOrServed(t, "Get/"+entry.name, err)
		})

		t.Run("ScanPrefix/"+entry.name, func(t *testing.T) {
			stop := holdAcquiringWindow(nodes, u)
			defer stop()
			_, err := entry.node.Cluster.ScanPrefix([]byte(key))
			requireAcquiringOrServed(t, "ScanPrefix/"+entry.name, err)
		})

		t.Run("Transact/"+entry.name, func(t *testing.T) {
			stop := holdAcquiringWindow(nodes, u)
			defer stop()
			err := entry.node.Cluster.Transact([]byte(key), func(tx backend.Transaction) error {
				return tx.Put([]byte(key), []byte("txv"))
			})
			requireAcquiringOrServed(t, "Transact/"+entry.name, err)
		})
	}
}

// TestAcquiringReason_R2PeerDownDoesNotMatchErrAcquiring is the negative half at
// R=2, and the constraint the fix had to respect while closing the gap: the
// terminal now inherits its reason from the legs, so a terminal whose shortfall
// was caused by a genuinely-down peer must still NOT match. Otherwise a
// consumer's bounded retry would fire against a real outage.
func TestAcquiringReason_R2PeerDownDoesNotMatchErrAcquiring(t *testing.T) {
	const unitCount = 8
	n1, n2, key, _ := r2AcquiringPair(t, unitCount)

	// Kill ONLY n2's gRPC listener; memberlist keeps running so the ring still
	// routes a replica leg to it and the write fails at the dial - a real
	// transport failure, not a synthesized one. At W=2 that shortfall is fatal.
	n2.stop()
	n2.stop = nil

	var err error
	ok := waitUntil(10*time.Second, func() bool {
		err = n1.Cluster.Put([]byte(key), []byte("v"))
		return err != nil
	})
	if !ok {
		t.Fatalf("R=2 Put with a downed replica kept acking; err=%v", err)
	}
	if code := status.Code(err); code != codes.Unavailable {
		t.Fatalf("peer-down R=2 Put: code = %v, want Unavailable (the code acquiring shares). err=%v", code, err)
	}
	if errors.Is(err, cluster.ErrAcquiring) {
		t.Fatalf("an R=2 write terminal caused by a genuinely-down replica matched "+
			"cluster.ErrAcquiring; the acquiring/outage distinction is broken: %v", err)
	}
}
