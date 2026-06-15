package integration

// THE OVERLAP-HANDOFF ACCEPTANCE GATE (v0.8 Phase 2e, Option B).
//
// Phase 2d (Option A, the retry-on-acquiring belt) keeps R>1 writes available
// across a membership change by RETRYING through the acquiring window, BOUNDED BY
// WriteTimeout. So Option A's availability is a function of mount-time-vs-timeout:
// when a single replica mount takes LONGER than the WriteTimeout budget, the
// retry exhausts and the write fails even though no acked write is ever lost.
//
// Phase 2e (Option B, overlap handoff) removes that bound: the OLD owner of a
// moving ReplicaUnit KEEPS SERVING until the NEW owner has fully acquired, and
// during the overlap the NEW owner FORWARDS routed ops to the OLD owner (the
// position-addressed forward). Availability no longer depends on mount time - a
// multi-second mount is a non-event.
//
// This gate makes the mount delay (7s) LARGER than the WriteTimeout (the 5s
// default), the regime where Option A ALONE collapses (the retry budget cannot
// outlast the mount). It writes continuously through a 3 -> 4 R=2 membership
// change and asserts BOTH:
//
//	(a) THE ORACLE: every baseline + acked probe key is readable with its exact
//	    value from every node. ZERO acked loss.
//	(b) THE ACK RATE stays high DESPITE the mount being LONGER than the write
//	    budget - which is only possible because the old owner serves via the new
//	    owner's forward throughout the long Acquiring window. With overlap OFF
//	    (clean-cut + Option-A retry only) this rate collapses, because the retry
//	    cannot ride out a mount longer than its budget.
//
// NO slatedb tag, NO MinIO: in-process sharedfactory (the SetAcquireDelay
// slow-mount injection is the object-store-latency analogue) + memory backend.

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

func TestOverlapHandoffGate_MountSlowerThanWriteBudget(t *testing.T) {
	const unitCount = 32
	// The slatedb default WriteTimeout is 5s; the mount delay below is LONGER, so
	// Option A's retry budget cannot outlast a single mount and overlap
	// forwarding is the only thing that keeps writes available.
	const mountDelay = 7 * time.Second
	backing := sharedfactory.NewBacking()

	// Start a 3-node R=2 cluster (default 5s WriteTimeout via startReplicatedNode)
	// and let it fully settle before the baseline so the initial-convergence
	// per-replica fencing is resolved (no artificial mount delay armed yet).
	nodes := start3NodeR2(t, unitCount, backing)
	time.Sleep(700 * time.Millisecond)

	want := make(map[string][]byte)
	for i := range 120 {
		k := fmt.Sprintf("ovbase-%05d", i)
		v := fmt.Appendf(nil, "ovbaseval-%05d", i)
		if err := putWithRetryUnavailable(t, nodes[0].Cluster, k, string(v), 15*time.Second); err != nil {
			t.Fatalf("baseline Put %q: %v", k, err)
		}
		want[k] = v
	}

	var (
		mu        sync.Mutex
		ackedKeys = make(map[string][]byte)
		attempted atomic.Int64
		acked     atomic.Int64
		stop      atomic.Bool
		wg        sync.WaitGroup

		entryMu  sync.RWMutex
		entryFor = append([]*sharedNode{}, nodes...)
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
				entry := pickEntry(w)
				k := fmt.Sprintf("ovprobe-%d-%07d", w, i)
				v := fmt.Appendf(nil, "ovpv-%d-%07d", w, i)
				attempted.Add(1)
				// Call Put DIRECTLY (no external retry) so the ack rate reflects the
				// cluster's internal availability through the long mount.
				if err := entry.Cluster.Put([]byte(k), []byte(v)); err == nil {
					acked.Add(1)
					mu.Lock()
					ackedKeys[k] = v
					mu.Unlock()
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}

	// Warm up, then arm the LONG mount delay on the existing nodes and join a
	// 4th. The join shuffles replica positions across many units, so reconcile
	// re-mounts fire; each mount now stalls 7s (> the 5s write budget), the
	// regime where Option-A retry alone collapses. Overlap forwarding keeps the
	// old owner serving throughout, so the ack rate must stay high.
	time.Sleep(300 * time.Millisecond)
	for _, n := range nodes {
		n.Handle.SetAcquireDelay(mountDelay)
	}
	n4 := startReplicatedNode(t, "ovd", nodes[0].BindAddr, unitCount, 2, backing)
	n4.Handle.SetAcquireDelay(mountDelay)
	all := append(append([]*sharedNode{}, nodes...), n4)

	csAll := make([]*cluster.Cluster, 0, len(all))
	for _, n := range all {
		csAll = append(csAll, n.Cluster)
	}
	if err := waitForMembersAll(csAll, 4, 30*time.Second); err != nil {
		stop.Store(true)
		wg.Wait()
		t.Fatalf("4-node convergence: %v", err)
	}
	entryMu.Lock()
	entryFor = append([]*sharedNode{}, all...)
	entryMu.Unlock()

	// Keep writing across the full mount window (a couple of mount-delays) so the
	// overlap window is genuinely exercised by in-flight writes.
	time.Sleep(16 * time.Second)
	stop.Store(true)
	wg.Wait()

	// Drop the delay so the readback is not itself slowed, then let any still-
	// Acquiring positions finish their (now instant) mount + the drains release.
	for _, n := range all {
		n.Handle.SetAcquireDelay(0)
	}
	time.Sleep(2 * time.Second)

	att := attempted.Load()
	ack := acked.Load()
	if att == 0 {
		t.Fatalf("continuous writer attempted zero writes")
	}
	rate := float64(ack) / float64(att)
	t.Logf("overlap-handoff availability (mount %v > writeTimeout 5s) through 3->4: acked %d / attempted %d = %.1f%%",
		mountDelay, ack, att, rate*100)

	mu.Lock()
	for k, v := range ackedKeys {
		want[k] = v
	}
	mu.Unlock()

	// (a) THE ORACLE: zero acked loss, readable + exact from every node.
	for k, v := range want {
		got, err := getWithRetryUnavailable(t, all[0].Cluster, k, 15*time.Second)
		if err != nil {
			t.Fatalf("ACKED WRITE LOST: key %q unreadable after overlap handoff: %v", k, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("ACKED WRITE CORRUPTED: key %q = %q, want %q", k, got, v)
		}
	}
	sample := 0
	for k, v := range want {
		for _, n := range all {
			got, err := getWithRetryUnavailable(t, n.Cluster, k, 15*time.Second)
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

	// (b) THE ACK RATE stays high DESPITE the mount being LONGER than the write
	// budget. This is the Option-B-only assertion: Option A's retry cannot
	// outlast a mount longer than WriteTimeout, so without overlap forwarding the
	// rate would collapse. Overlap forwarding keeps the old owner serving, so the
	// rate stays near 100% (a few writes can still blip on the pure-new-mount /
	// ambiguous-predecessor fallback to Option A).
	const threshold = 0.90
	if rate < threshold {
		t.Fatalf("OVERLAP AVAILABILITY TOO LOW: acked %d / attempted %d = %.1f%% < %.0f%% "+
			"(a mount %v > the 5s write budget collapsed availability; overlap forwarding is not keeping the old owner serving)",
			ack, att, rate*100, threshold*100, mountDelay)
	}
}
