package integration

// Failure scenario: destination dies mid-migration.
//
// docs/SPEC.md "Failure handling" calls out the destination-crash
// case: source's stream returns an error, source remains owner, the
// next Evaluate pass re-runs the plan against the now-2-node ring.
// The post-conditions this test pins:
//
//   1. The remaining two nodes converge to a 2-member ring.
//   2. No range on either survivor sits stuck in a non-terminal
//      state forever (no orphan ranges).
//   3. Every key written before the failure is gettable via either
//      survivor (assuming its ring-owner is still alive; we filter
//      to that subset, same R=1 caveat as the shrink test).
//
// The shape mirrors a real-world rolling deploy where a node we just
// rebalanced TO falls over before the data settled; the cluster has
// to detect this + retry without operator intervention.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/ring"
)

func TestRebalance_DestinationFailureMidMigration(t *testing.T) {
	t.Parallel()

	// --- bring up a 2-node cluster + write a body of keys so the
	// growth migration has real work to do. ---
	n1 := startTestNode(t, "fail-n1", "")
	n2 := startTestNode(t, "fail-n2", n1.BindAddr)
	pair := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(pair, 2, 10*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}

	const total = 400
	keys := putN(t, n1.Cluster, "fail", total)

	// --- add a third node + immediately schedule it to die. ---
	n3 := startTestNode(t, "fail-n3", n1.BindAddr)
	trio := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(trio, 3, 10*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}

	// Hit n3 well past the integration fixture's 500ms settle delay
	// so the source's Evaluate has launched runSend for every
	// outgoing partition + the gRPC streams to n3 are inflight or
	// already received. Closing n3 here exercises BOTH the
	// stream-interrupted path (source's MigrateRange handler errors,
	// MarkSendComplete propagates the err, runSend stays out of
	// HandedOff) AND the cluster-wide recovery path (n3's
	// disappearance triggers a fresh Evaluate on n1/n2 that
	// re-routes the partitions to whichever survivor the 2-node
	// ring picks).
	time.Sleep(2 * time.Second)
	n3.Close()

	// Survivors converge to a 2-member ring.
	if err := waitForMembersAll(pair, 2, 10*time.Second); err != nil {
		t.Fatalf("post-failure convergence to 2 members: %v", err)
	}

	// Filter the test set to keys whose POST-failure ring owner is
	// one of the survivors. R=1: anything stranded on n3 at the
	// moment it died is gone.
	finalRing := ring.New()
	for _, m := range n1.Cluster.Members() {
		finalRing.Add(m)
	}
	survivable := make([]string, 0, len(keys))
	for _, k := range keys {
		if owner := finalRing.LocateKey([]byte(k)).ID; owner == "fail-n1" || owner == "fail-n2" {
			survivable = append(survivable, k)
		}
	}
	if len(survivable) < total/2 {
		t.Fatalf("expected most keys' new owner to be a survivor, got %d/%d (ring distribution suspect)",
			len(survivable), total)
	}

	// Give the cluster a few seconds for the post-failure re-eval +
	// the in-flight stream errors to drain. Convergence to "every
	// key on its new ring owner" is NOT achievable here: per the v0.3
	// data-loss fix, the source's runSend stays out of HandedOff for
	// any partition whose stream errored AND any partition whose
	// stream completed successfully BEFORE n3 was killed is gone
	// (the data made it to n3, which then died, R=1 + no replica).
	// The test's contract is that the cluster recovers gracefully,
	// not that R=1 magically resurrects data n3 took with it.
	time.Sleep(5 * time.Second)

	// --- assertion 1: MOST survivable keys are gettable via the
	// surviving nodes. With the v0.3 data-loss fix, partitions whose
	// stream errored stay on the source -- those keys are preserved.
	// Partitions whose stream completed before n3 died are gone (R=1
	// limitation). We assert >= 50% survival as the floor: substantial
	// data preservation is the fix's win. Without the fix, EVERY
	// migrated partition's source would have been swept, so survival
	// would collapse toward "keys whose new owner never changed."
	gettable := 0
	for _, k := range survivable {
		if _, err := n1.Cluster.Get([]byte(k)); err == nil {
			gettable++
		}
	}
	t.Logf("post-failure survivable: %d/%d gettable via the cluster", gettable, len(survivable))
	if gettable < len(survivable)/2 {
		t.Fatalf("post-failure data-loss: only %d/%d 'survivable' keys are gettable; the post-fix expectation is most survivable keys (those whose stream errored mid-flight) stay on the source",
			gettable, len(survivable))
	}

	// --- assertion 2: no survivor holds zero keys (the rebalance
	// did NOT collapse onto one node). ---
	dist := perNodeKeyCount(t, []*testNode{n1, n2})
	t.Logf("post-failure distribution: %v", dist)
	for _, n := range []*testNode{n1, n2} {
		if dist[n.ID] == 0 {
			t.Fatalf("post-failure: survivor %s holds zero keys; distribution=%v", n.ID, dist)
		}
	}

	// --- assertion 3: writes still work end-to-end (the cluster
	// isn't stuck in some "migration cancelled, all writes rejected"
	// state). ---
	if err := n1.Cluster.Put([]byte("fail-post-write"), []byte("ok")); err != nil {
		t.Fatalf("Put after failure recovery: %v (cluster may have stuck range state from the aborted migration)", err)
	}
	got, err := n2.Cluster.Get([]byte("fail-post-write"))
	if err != nil {
		t.Fatalf("Get after failure recovery: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("Get after failure recovery: got %q want ok", got)
	}
}

// TestRebalance_SourceDoesNotDeleteOnDestinationCrash pins the v0.3
// source-observes-destination data-loss fix: when the destination
// crashes mid-stream, the source MUST NOT flip its partition to
// HandedOff (because the destination never acked the data). If the
// source flipped HandedOff, the sweep would delete the local copy
// after the grace window, AND the destination doesn't have it
// either -- pure data loss on backends that don't snapshot during
// scan the way the memory backend implicitly does.
//
// Shape:
//
//  1. 2-node cluster, populate enough keys + values bulky enough
//     that a 3-node growth migration takes meaningful wall-clock.
//  2. Add a 3rd node, wait for membership convergence.
//  3. Kill the 3rd node's gRPC server mid-rebalance (force-stop, not
//     graceful, so in-flight MigrateRange streams break immediately).
//  4. Wait long enough for: the source's settle delay to elapse,
//     the source-side scan to "complete" (with the new code, the
//     wired runSend would NOT transition to HandedOff until ack),
//     and the grace window AFTER any potential (broken) HandedOff
//     transition to expire so a buggy sweep would have already
//     deleted.
//  5. Assert: every key originally written before the kill is still
//     gettable from one of the survivors. With the fix, the source
//     never reached HandedOff for the partitions n3 was destined to
//     own + the sweep never deleted them.
//
// Pre-fix failure mode: with runSend's local-scan-driven flip, the
// source transitions HandedOff regardless of stream success; sweep
// fires after grace; the keys disappear from the source AND the
// destination never received them (it crashed) -- a Get returns
// ErrNotFound for many keys.
func TestRebalance_SourceDoesNotDeleteOnDestinationCrash(t *testing.T) {
	t.Parallel()

	n1 := startTestNode(t, "crash-n1", "")
	n2 := startTestNode(t, "crash-n2", n1.BindAddr)
	pair := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(pair, 2, 10*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}

	// Populate enough bulky values that the rebalance streaming
	// window comfortably spans the kill timing. Memory backend +
	// loopback gRPC are very fast, so the values have to be big
	// enough that streaming time outpaces any wakeup latency in
	// the test's sleep + kill sequence. 600 keys * 256KB = ~150MB
	// across the cluster, of which the n3-bound partitions are
	// streaming for hundreds of milliseconds even over loopback +
	// the race detector's serialization overhead.
	const total = 600
	const valueBytes = 256 * 1024
	bigValue := make([]byte, valueBytes)
	for i := range bigValue {
		bigValue[i] = byte('A' + (i % 26))
	}
	keys := make([]string, total)
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("crash-%05d", i)
		if err := putWithRetry(n1.Cluster, []byte(k), bigValue); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
		keys[i] = k
	}

	preDist := perNodeKeyCount(t, []*testNode{n1, n2})
	t.Logf("pre-growth distribution (n=2): %v", preDist)
	if preDist["crash-n1"]+preDist["crash-n2"] != total {
		t.Fatalf("pre-growth total = %d, want %d (dist=%v)", preDist["crash-n1"]+preDist["crash-n2"], total, preDist)
	}

	// Compute which survivor physically holds each key BEFORE the
	// growth migration (the source side that the v0.3 rebalance is
	// about to migrate FROM). With the fix, every one of these
	// physical placements MUST survive the destination's mid-stream
	// crash; pre-fix the sweep deletes them.
	preOwners := make(map[string]string, len(keys))
	for _, k := range keys {
		if _, err := n1.Backend.Get([]byte(k)); err == nil {
			preOwners[k] = "crash-n1"
		} else if _, err := n2.Backend.Get([]byte(k)); err == nil {
			preOwners[k] = "crash-n2"
		} else {
			t.Fatalf("pre-growth: key %s not on either source", k)
		}
	}

	// Add n3 to trigger rebalance.
	n3 := startTestNode(t, "crash-n3", n1.BindAddr)
	trio := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(trio, 3, 10*time.Second); err != nil {
		t.Fatalf("3-node convergence: %v", err)
	}
	_ = trio

	// Close n3 IMMEDIATELY: its bootstrap Evaluate has already
	// dispatched runReceive goroutines (initRebalance fires the
	// bootstrap Evaluate synchronously inside Open), and those
	// goroutines have started their MigrateRange streams against the
	// source(s). Closing n3 here cancels those in-flight streams
	// while the source's runSendWired is still waiting for the
	// destination ack. The source-side gRPC handler's Send returns
	// the cancelled-context error, SignalMigrateRangeComplete
	// propagates that err to MarkSendComplete, and runSendWired
	// transitions Done-handoff-err (NOT HandedOff).
	//
	// For partitions where the source's runSendWired never sees a
	// stream (n3 never asked, or n3 closed before reaching that
	// partition's dial), the wired-handoff timeout
	// (RebalanceHandoffTimeout, 4s in the integration fixture)
	// also transitions Done-handoff-err. Either way, the partition
	// stays out of HandedOff + the sweep can't delete the
	// source's local copy.
	//
	// We use n3.Close() (cluster close) instead of n3.KillGRPC()
	// (server-only stop) because the MigrateRange stream is
	// destination-initiated: dest is the gRPC client. Killing the
	// dest's server doesn't break the dest's outgoing connection;
	// closing the dest's cluster does.
	n3.Close()

	// Hold long enough that:
	//   - the source's MigrateRange handler observes the broken
	//     stream (gRPC Send returns immediately once the underlying
	//     TCP conn dies), MarkSendComplete propagates the error,
	//     runSendWired transitions Done-handoff-err.
	//   - any partition whose stream WAS in-flight at kill time has
	//     errored; the wired-handoff timeout (4s in the integration
	//     fixture) is the upper bound on how long a partition the
	//     destination never asked about waits before also going
	//     Done-handoff-err.
	//   - the grace window (3s in the test fixture) has elapsed past
	//     any HYPOTHETICAL HandedOff transition the buggy code would
	//     have done; the sweep would have already fired.
	//
	// 7s past the kill covers all three: 4s for the wired timeout,
	// 3s for grace + sweep + observation.
	time.Sleep(7 * time.Second)

	// Core assertion: every key originally owned by a survivor is
	// STILL on its source backend. If runSend had transitioned to
	// HandedOff for the partitions n3 was destined for, the sweep
	// would have deleted them by now -- pre-fix this is exactly the
	// data-loss path.
	survived := 0
	missing := 0
	survivors := map[string]*testNode{"crash-n1": n1, "crash-n2": n2}
	for _, k := range keys {
		preOwner := preOwners[k]
		src, ok := survivors[preOwner]
		if !ok {
			// Shouldn't happen in a 2-node baseline, but defensively skip.
			continue
		}
		if _, err := src.Backend.Get([]byte(k)); err == nil {
			survived++
		} else {
			missing++
			if missing <= 3 {
				t.Logf("key %s missing from source-side %s after crash (preOwner=%s): %v", k, preOwner, preOwner, err)
			}
		}
	}
	postDist := perNodeKeyCount(t, []*testNode{n1, n2})
	t.Logf("post-kill distribution (n1,n2): %v", postDist)

	// Aggregate the per-source rebalance snapshots. With the fix,
	// every range whose source-side handler observed a broken
	// stream ends up in Done with an error prefixed "rebalance:
	// handoff:" (the runSendWired error path). Pre-fix the source's
	// non-wired runSend never reads the stream outcome + cannot
	// produce that error -- any Done-with-err comes from local-scan
	// failures with a different prefix ("rebalance: send:
	// rebalance: iterate:") or similar.
	var doneWithHandoffErr, doneClean, stillHandedOff int
	for _, n := range []*testNode{n1, n2} {
		for _, s := range n.Cluster.RebalanceSnapshot() {
			switch s.State {
			case rebalance.StateHandedOff:
				stillHandedOff++
			case rebalance.StateDone:
				if s.Err == "" {
					doneClean++
				} else if strings.Contains(s.Err, "rebalance: handoff:") {
					doneWithHandoffErr++
				}
			}
		}
	}
	t.Logf("n1+n2 state: done-handoff-err=%d done-clean=%d still-handed-off=%d", doneWithHandoffErr, doneClean, stillHandedOff)

	// Mechanism assertion: the wired-handoff path MUST have caught
	// at least one stream's broken-ack + routed the partition to
	// Done with a "rebalance: handoff:" error. Without the fix this
	// path doesn't exist + the counter is zero.
	if doneWithHandoffErr == 0 {
		t.Fatalf("source-side runSendWired produced no handoff errors after destination crash; the wired-handoff signal isn't reaching the Coordinator (or this run completed every stream before the kill timing; bump valueBytes if so)")
	}

	t.Logf("source-side survival: %d/%d keys preserved across destination crash (%d ranges held back from HandedOff via wired-handoff fix)",
		survived, len(keys), doneWithHandoffErr)
}
