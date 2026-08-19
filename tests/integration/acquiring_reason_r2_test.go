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
// The window is HELD OPEN, not raced: the fixture arms an acquire delay far
// beyond any bounded wait here, clears the replica-0 mount ONCE, and proves the
// position is observably owner-but-unmounted (the real handoff state) before it
// issues the op. Releasing the delay closes the window.
//
// Each entry point is driven from BOTH nodes. Whichever node holds replica-0,
// one direction makes the mid-acquire leg IN-PROCESS and the other makes it a
// leg forwarded over real gRPC, so the pair covers both halves of the contract
// without the test having to know which node ended up with the position.

import (
	"errors"
	"fmt"
	"strings"
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
	n2 = startReplicatedNodeCfg(t, "arr2", n1.ClusterToken, unitCount, 2, backing, shortBudgets)

	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("ring convergence: %v", err)
	}
	// Registered AFTER each node's own t.Cleanup(Close), so it runs BEFORE them
	// (cleanups are LIFO): no teardown ever waits out an armed acquire delay, on
	// ANY exit path including a t.Fatalf from inside a held window. This is what
	// makes arming a 5-minute delay affordable - the cost of a wide window must
	// not be paid by a runner that never needed it.
	t.Cleanup(func() {
		n1.Handle.SetAcquireDelay(0)
		n2.Handle.SetAcquireDelay(0)
	})

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

// replicaLineState parses DebugState's per-position line for ru on one node and
// reports that node's desired/mounted view of it. parsed is false when the node
// has no line for ru at all (it does not hold the position), which is a
// different thing from holding it and having it mounted - folding the two
// together would let "the wrong node was inspected" read as "the window is
// open".
func replicaLineState(c *cluster.Cluster, ru storageunit.ReplicaUnit) (desired, mounted, parsed bool) {
	prefix := ru.String() + " "
	for _, line := range strings.Split(c.DebugState(), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		return strings.Contains(line, "desired=true"),
			strings.Contains(line, "mounted=true"),
			true
	}
	return false, false, false
}

// waitAcquiringWindowOpen blocks until some node reports ru as
// DESIRED-BUT-UNMOUNTED: the MEASURED form of "a node owns this replica
// position and cannot serve it", which is the state the op below has to be
// issued in for its result to mean anything. It returns that node's ID.
//
// It exists because the previous fixture ASSUMED the window instead of
// observing it. That version re-cleared the mount on a 1ms loop and raced the
// periodic reconcile for it; when the reconcile won, both replicas really were
// mounted, W=2 really was satisfiable, and the Put's ack was CORRECT - but the
// test read that ack as a contract violation and failed. Locally the race was
// invisible (400/400 refusals), while a loaded 2-vCPU CI runner lost it. The
// window has to be a thing this test opens and closes, not one it out-runs.
func waitAcquiringWindowOpen(t *testing.T, nodes []*sharedNode, ru storageunit.ReplicaUnit, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, n := range nodes {
			desired, mounted, parsed := replicaLineState(n.Cluster, ru)
			if parsed && desired && !mounted {
				return n.ID
			}
		}
		if !time.Now().Before(deadline) {
			var dumps strings.Builder
			for _, n := range nodes {
				fmt.Fprintf(&dumps, "\n--- %s ---\n%s", n.ID, n.Cluster.DebugState())
			}
			t.Fatalf("no node reported %s as desired-but-unmounted within %s, so the acquiring window "+
				"this gate probes in was never open and the op below would be issued against a fully "+
				"mounted replica set. This is a fixture-window miss, not a contract result. State:%s",
				ru, timeout, dumps.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// holdAcquiringWindowDelay is armed past any bounded wait in this file so the
// window's width is never sampled from the runner's speed. It is NEVER slept:
// every caller releases it, and r2AcquiringPair's cleanup releases it again on
// any t.Fatalf path, so a runner only ever pays the time the probe itself takes.
const holdAcquiringWindowDelay = 5 * time.Minute

// holdProvenAcquiringWindow puts unit u's replica-0 holder into the
// owner-but-unmounted state, PROVES it is in that state, and HOLDS IT THERE
// until the returned stop func is called.
//
// The hold is the armed acquire delay, not a race: every path that re-mounts a
// replica position goes through the factory's OpenReplicaUnit, so with the delay
// armed BEFORE the mount is cleared, the periodic reconcile's restoring open
// blocks in the delay and the mount cannot land. SetAcquireDelay re-reads as it
// sleeps, so stop() releases the open already in flight rather than only later
// ones. Only the node that actually holds replica-0 has anything to clear; the
// other clear is a harmless no-op, so the caller does not need to know which is
// which. The co-replica stays mounted and keeps acking throughout, which is what
// makes the shortfall a PURE acquiring shortfall (acks below W with zero hard
// failures) rather than an outage.
//
// It is separate from holdAcquiringWindow rather than replacing it because that
// one is also called from an Aggregate fan-out goroutine, where t.Fatalf (which
// the window proof needs, and which must run on the test goroutine) is not
// legal. Callers that CAN prove their window should prefer this one.
func holdProvenAcquiringWindow(t *testing.T, nodes []*sharedNode, u storageunit.UnitID) (stop func()) {
	t.Helper()
	for _, n := range nodes {
		n.Handle.SetAcquireDelay(holdAcquiringWindowDelay)
	}
	for _, n := range nodes {
		n.Cluster.TestingClearMount(u)
	}
	ru := storageunit.NewReplicaUnit(storageunit.NewGenUnit(0, u), 0)
	holder := waitAcquiringWindowOpen(t, nodes, ru, 30*time.Second)
	t.Logf("acquiring window held open on %s: %s desired-but-unmounted under a %s acquire delay",
		holder, ru, holdAcquiringWindowDelay)
	return func() {
		for _, n := range nodes {
			n.Handle.SetAcquireDelay(0)
		}
	}
}

// holdAcquiringWindow is the ORIGINAL racing hold: a background loop re-clears
// unit u's replica-0 mount, betting it can out-run the periodic reconcile that
// restores it. It cannot prove the window was open when the op ran, so a
// reconcile that wins the race leaves the op looking at a fully mounted replica
// set. Retained ONLY for the Aggregate callers, which invoke it from a fan-out
// goroutine and so cannot use the proving variant above.
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
			stop := holdProvenAcquiringWindow(t, nodes, u)
			defer stop()
			err := entry.node.Cluster.Put([]byte(key), []byte("v"))
			if err == nil {
				t.Fatal("R=2 Put with a replica held mid-acquire: got an ack, want a refusal")
			}
			requireAcquiringOrServed(t, "Put/"+entry.name, err)
		})

		t.Run("Delete/"+entry.name, func(t *testing.T) {
			stop := holdProvenAcquiringWindow(t, nodes, u)
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
			stop := holdProvenAcquiringWindow(t, nodes, u)
			defer stop()
			_, err := entry.node.Cluster.Get([]byte(key))
			requireAcquiringOrServed(t, "Get/"+entry.name, err)
		})

		t.Run("ScanPrefix/"+entry.name, func(t *testing.T) {
			stop := holdProvenAcquiringWindow(t, nodes, u)
			defer stop()
			_, err := entry.node.Cluster.ScanPrefix([]byte(key))
			requireAcquiringOrServed(t, "ScanPrefix/"+entry.name, err)
		})

		t.Run("Transact/"+entry.name, func(t *testing.T) {
			stop := holdProvenAcquiringWindow(t, nodes, u)
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

	// Kill ONLY n2's gRPC listener; its coordinator keeps running so the ring
	// still routes a replica leg to it and the write fails at the dial - a real
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
