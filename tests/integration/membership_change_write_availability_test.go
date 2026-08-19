package integration

// THE R>1 WRITE-AVAILABILITY-THROUGH-MEMBERSHIP-CHANGE GATE (v0.8 Phase 2d).
//
// During an R>1 multi-backend MEMBERSHIP CHANGE (scale up: more nodes join),
// reconcileReplicaUnits re-mounts the units whose replica assignment moved. A
// write that lands on a replica mid-mount used to ERROR (the acquiring-window
// refusal counted against the R=2 W=2 failure budget), so during a big churn a
// large fraction of writes failed even though ZERO acked writes were ever lost.
// Measured on the live staging chaos (scale 3 -> 12 at R=2): only ~52-64% of
// writes acked.
//
// Phase 2d's retry-on-acquiring turns the handoff-window refusal into bounded
// LATENCY: layer 1 makes the acquiring refusal a true fan-out transient (it
// consumes neither budget); layer 2 retries the whole write under WriteTimeout
// when W is momentarily unreachable. So the ack rate should climb to ~100% with
// zero acked loss.
//
// This gate WRITES CONTINUOUSLY THROUGH A MEMBERSHIP CHANGE and asserts BOTH:
//
//	(a) THE ORACLE: every baseline key and every ACKED probe key is readable
//	    with its exact value from every node. ZERO acked loss (durability).
//	(b) THE ACK RATE: acked / attempted is HIGH (> 95%). This is the assertion
//	    that FAILS on the pre-fix code (the membership change drives the ack rate
//	    to ~50-64%) and PASSES after the fix - proving the gate is not a rubber
//	    stamp. The continuous writer calls Put DIRECTLY (no external retry) so it
//	    measures the CLUSTER'S internal availability, not the test's patience.
//
// To make the acquiring window wide enough to reproduce the pre-fix drop
// deterministically (the in-process sharedfactory mounts instantly otherwise),
// every node's handle is armed with an artificial OpenReplicaUnit delay
// (SetAcquireDelay) before the membership change - the analogue of a real
// slatedb manifest open from object storage. The fix's WriteTimeout-bounded
// retry rides out that window; the pre-fix budget bug does not.
//
// NO slatedb tag, NO MinIO: in-process sharedfactory + memory backend only.

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
)

func TestMembershipChangeWriteAvailabilityGate(t *testing.T) {
	const unitCount = 32
	backing := sharedfactory.NewBacking()

	// Start a 3-node R=2 cluster and record a baseline so the oracle covers
	// pre-churn writes too.
	nodes := start3NodeR2(t, unitCount, backing)
	want := make(map[string][]byte)
	const nBaseline = 150
	for i := range nBaseline {
		k := fmt.Sprintf("base-%05d", i)
		v := fmt.Appendf(nil, "baseval-%05d", i)
		if err := putWithRetryUnavailable(t, nodes[0].Cluster, k, string(v), 10*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	// CONTINUOUS WRITER: keep writing a tracked keyset through the routed
	// surface, recording a key ONLY once its Put returns nil, counting
	// attempted-vs-acked. Calls Put DIRECTLY (no external retry) so the ack rate
	// reflects the cluster's internal availability through the churn. The entry
	// node is read from a slice updated as joiners arrive, so writes route to
	// the growing membership too.
	var (
		mu        sync.Mutex
		ackedKeys = make(map[string][]byte)
		attempted atomic.Int64
		acked     atomic.Int64
		stop      atomic.Bool
		wg        sync.WaitGroup

		entryMu  sync.RWMutex
		entryFor = append([]*sharedNode{}, nodes...) // grows as joiners arrive
	)
	pickEntry := func(w int) *sharedNode {
		entryMu.RLock()
		defer entryMu.RUnlock()
		return entryFor[w%len(entryFor)]
	}
	const writers = 6
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				entry := pickEntry(w) // route both locally and forwarded
				k := fmt.Sprintf("probe-%d-%07d", w, i)
				v := fmt.Appendf(nil, "pv-%d-%07d", w, i)
				attempted.Add(1)
				err := entry.Cluster.Put([]byte(k), []byte(v))
				if err == nil {
					acked.Add(1)
					mu.Lock()
					ackedKeys[k] = v
					mu.Unlock()
				}
				// Tiny pace so the writers don't pure-spin a single core.
				time.Sleep(time.Millisecond)
			}
		}(w)
	}

	// Let the writers warm up, THEN scale 3 -> 4 (one joiner). The join makes
	// the ring re-assign replica positions across many units, so reconcile re-
	// mounts fire on several nodes. We arm a wide acquiring window on the
	// EXISTING nodes BEFORE the join so those reconcile re-mounts genuinely
	// stall and the handoff window overlaps the in-flight writes (the pre-fix
	// drop needs that). The delay only affects OpenReplicaUnit, NOT the
	// coordinator, so
	// membership still converges. 120ms per acquire: comfortably under the 5s
	// WriteTimeout the retry rides, but long enough that without the retry a
	// write landing on a re-mounting replica fails.
	time.Sleep(200 * time.Millisecond)
	for _, n := range nodes {
		n.Handle.SetAcquireDelay(120 * time.Millisecond)
	}
	n4 := startReplicatedNode(t, "r2d", nodes[0].ClusterToken, unitCount, 2, backing)
	n4.Handle.SetAcquireDelay(120 * time.Millisecond)
	all := append(append([]*sharedNode{}, nodes...), n4)

	cs := make([]*cluster.Cluster, 0, len(all))
	for _, n := range all {
		cs = append(cs, n.Cluster)
	}
	if err := waitForMembersAll(cs, 4, 30*time.Second); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("4-node convergence: %v", err)
	}

	// Route writes to the joiner too now that it is a member.
	entryMu.Lock()
	entryFor = append([]*sharedNode{}, all...)
	entryMu.Unlock()

	// Keep writing across the reconcile settle + the acquiring windows, then a
	// little longer so the post-churn state is steady before we measure.
	time.Sleep(2 * time.Second)
	stop.Store(true)
	wg.Wait()

	// Drop the acquire delay so the readback below is not itself slowed by the
	// artificial latency (the membership change is over; this is pure cleanup).
	for _, n := range all {
		n.Handle.SetAcquireDelay(0)
	}

	att := attempted.Load()
	ack := acked.Load()
	if att == 0 {
		t.Fatalf("continuous writer attempted zero writes")
	}
	rate := float64(ack) / float64(att)
	t.Logf("write availability through 3->4 membership change: acked %d / attempted %d = %.1f%%",
		ack, att, rate*100)

	// Fold the acked probe keys into the oracle dataset.
	mu.Lock()
	for k, v := range ackedKeys {
		want[k] = v
	}
	mu.Unlock()

	// (a) THE ORACLE: every baseline key and every ACKED probe key is readable
	// with its exact value from EVERY node (each routes to a replica via
	// forwarding). ZERO acked loss - this must hold even at a low ack rate.
	for k, v := range want {
		got, err := getWithRetryUnavailable(t, all[0].Cluster, k, 10*time.Second)
		if err != nil {
			t.Fatalf("ACKED WRITE LOST: key %q unreadable after membership change: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("ACKED WRITE CORRUPTED: key %q = %q, want %q", k, got, v)
		}
	}
	// Spot-check the oracle from every node for a sample of keys (full cross-node
	// readback of every key is O(keys * nodes) and the single-node readback above
	// already proves durability via the quorum read; this catches a node that
	// diverged).
	sample := 0
	for k, v := range want {
		for _, n := range all {
			got, err := getWithRetryUnavailable(t, n.Cluster, k, 10*time.Second)
			if err != nil {
				t.Fatalf("ACKED WRITE LOST on node %s: key %q: %v", n.ID, k, err)
			}
			if !bytes.Equal(got, v) {
				t.Fatalf("node %s: key %q = %q, want %q", n.ID, k, got, v)
			}
		}
		if sample++; sample >= 40 {
			break
		}
	}

	// (b) THE ACK RATE: > 95%. This FAILS on the pre-fix code (the acquiring
	// refusal counts against the failure budget, dropping the rate to ~50-64%)
	// and PASSES after the fix (retry-on-acquiring rides the window out).
	const threshold = 0.95
	if rate < threshold {
		t.Fatalf("WRITE AVAILABILITY TOO LOW: acked %d / attempted %d = %.1f%% < %.0f%% "+
			"(the membership change dropped the ack rate; retry-on-acquiring is not riding out the handoff window)",
			ack, att, rate*100, threshold*100)
	}
}
