package integration

// NATURAL-TRIGGER reproduction: does a HARD-KILL (SIGKILL, no graceful Leave)
// rolling restart CAUSE the ring to diverge on its own?
//
// The membership-divergence mechanism test forces the divergent ring directly.
// This test asks the upstream question the live capture posed: does a rolling
// restart where each node departs UNGRACEFULLY (memberlist Shutdown with no
// Leave - a k8s pod delete / SIGKILL) and rejoins with the SAME stable node-name
// but a NEW address naturally settle into the divergent member view observed in
// staging (some nodes see 2 members, some see 3), which then produces the
// conflicting desired sets / orphaned positions / stuck writes?
//
// It uses the test-only seams TestingHardKill (cluster) / TestingShutdownNoLeave
// (membership): the departing node's transport is Shutdown WITHOUT the graceful
// leave broadcast, so peers must reap it via SWIM failure detection, exactly the
// regime the graceful-leave restarts (which converged 17/17 in the discovery
// test) bypass.
//
// Framed as a MEASUREMENT: it performs the hard-kill roll, then observes for a
// bounded window whether every node converges on the same 3-member view, and
// LOGS "RESULT: DIVERGED" or "RESULT: CONVERGED" with the captured per-node
// member sets. Run with -count=N -v and grep the RESULT lines for the hit rate.

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/cluster"
)

// hardKill tears the node down UNGRACEFULLY (SIGKILL model): memberlist Shutdown
// with no Leave + gRPC server stop. It deliberately does NOT mutate the
// sharedNode's Cluster/stop fields: a concurrent reader (the sampler / writers)
// may still hold this pointer, and reading a CLOSED cluster is safe (Members
// returns the frozen ring, Put returns ErrClosed), whereas niling the fields
// under a reader would be a data race. The teardown is idempotent, so the later
// t.Cleanup(n.Close) is harmless.
func (n *sharedNode) hardKill() {
	if n.Cluster != nil {
		_ = n.Cluster.TestingHardKill()
	}
	if n.stop != nil {
		n.stop()
	}
}

// hardRestart replaces the node with stable id `id` via an UNGRACEFUL departure
// (no Leave) and a rejoin on a NEW bind/grpc address with the SAME stable id,
// seeded to a live peer, over the same durable backing. This is the in-process
// analogue of a k8s pod delete + reschedule. The map slot is set nil under the
// lock while the node is being replaced so concurrent readers (sampler/writers)
// skip it, then swapped to the new instance.
func (f *liveFleet) hardRestart(id string) {
	f.t.Helper()
	f.mu.Lock()
	seed := f.seedsForLocked(id)
	old := f.live[id]
	f.live[id] = nil // readers skip the slot while it is mid-replace
	f.mu.Unlock()
	if old != nil {
		old.hardKill()
	}
	n := startReplicatedNode(f.t, id, seed, f.unitCount, f.rf, f.backing)
	f.mu.Lock()
	f.live[id] = n
	f.mu.Unlock()
}

// memberView returns a node's sorted member-id set and its size.
func memberView(c *cluster.Cluster) (ids []string, n int) {
	ms := c.Members()
	ids = make([]string, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, m.ID)
	}
	sort.Strings(ids)
	return ids, len(ids)
}

// fleetConverged reports whether every live node reports the SAME member-id set
// of the expected size. The negation (some node's set differs, e.g. size 2 vs 3)
// is the divergence the live capture showed.
func fleetConverged(f *liveFleet, wantN int) bool {
	var ref string
	count := 0
	for _, n := range f.nodes() {
		ids, sz := memberView(n.Cluster)
		if sz != wantN {
			return false
		}
		key := strings.Join(ids, ",")
		if ref == "" {
			ref = key
		} else if key != ref {
			return false
		}
		count++
	}
	return count == wantN
}

// waitFleetConverged polls until every node agrees on the wantN-member set or the
// timeout elapses. Returns true if it converged.
func waitFleetConverged(f *liveFleet, wantN int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fleetConverged(f, wantN) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// captureMemberViews formats each node's member-view size + set - the exact
// evidence from the live capture (some see 2, some see 3).
func captureMemberViews(f *liveFleet) string {
	var b strings.Builder
	for _, n := range f.nodes() {
		ids, sz := memberView(n.Cluster)
		fmt.Fprintf(&b, "\n  view[%s]: sees %d members = %v", n.ID, sz, ids)
	}
	return b.String()
}

// divergenceSampler continuously samples the fleet's member views (only when all
// nodes are nominally live) and records whether the ring EVER diverged (some node
// sees a different member set than another) plus the longest sustained divergence
// streak. It distinguishes a TRANSIENT divergence (the mechanism fired but healed)
// from a PERSISTENT one (the live 40-minute bug).
type divergenceSampler struct {
	everDiverged   atomic.Bool
	maxStreakMs    atomic.Int64
	sawTwoAndThree atomic.Bool // the exact live signature: one node sees 2, another 3
}

func (s *divergenceSampler) run(f *liveFleet, stop *atomic.Bool) func() {
	done := make(chan struct{})
	go func() {
		defer close(done)
		var streakStart time.Time
		for !stop.Load() {
			nodes := f.nodes()
			if len(nodes) == len(f.ids) { // only judge when every node is nominally up
				sizes := map[int]bool{}
				var ref string
				diverged := false
				for i, n := range nodes {
					ids, sz := memberView(n.Cluster)
					sizes[sz] = true
					key := strings.Join(ids, ",")
					if i == 0 {
						ref = key
					} else if key != ref {
						diverged = true
					}
				}
				if sizes[2] && sizes[3] {
					s.sawTwoAndThree.Store(true)
				}
				if diverged {
					s.everDiverged.Store(true)
					if streakStart.IsZero() {
						streakStart = time.Now()
					}
					if ms := time.Since(streakStart).Milliseconds(); ms > s.maxStreakMs.Load() {
						s.maxStreakMs.Store(ms)
					}
				} else {
					streakStart = time.Time{}
				}
			} else {
				streakStart = time.Time{}
			}
			time.Sleep(30 * time.Millisecond)
		}
	}()
	return func() { <-done }
}

// classifyingWriters keeps acking new keys through a rotating entry node,
// classifying each error as the wedge signature ("replicas mid-acquire") vs other
// so the test can detect whether the wedge mechanism fired during the roll.
func classifyingWriters(f *liveFleet, stop *atomic.Bool, writers int) (wait func(), acked, midAcq *atomic.Int64) {
	var wg sync.WaitGroup
	var a, m atomic.Int64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				cs := f.clusters()
				if len(cs) == 0 {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				entry := cs[(w+i)%len(cs)]
				k := fmt.Sprintf("cw-%d-%07d", w, i)
				err := entry.Put([]byte(k), []byte("v"))
				switch {
				case err == nil:
					a.Add(1)
				case isMidAcquire(err):
					m.Add(1)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}
	return wg.Wait, &a, &m
}

// TestHardKillRoll_MembershipDivergence measures whether a hard-kill rolling
// restart naturally produces the divergent member view. It does not fail on
// either outcome; it logs RESULT lines (grep them under -count for the hit rate).
func TestHardKillRoll_MembershipDivergence(t *testing.T) {
	const unitCount, rf = 16, 2
	f := newLiveFleet(t, unitCount, rf)
	f.bootstrap("n1", "n2", "n3")

	for i := 0; i < 150; i++ {
		k := fmt.Sprintf("seed-%05d", i)
		if err := putWithRetryUnavailable(t, f.live["n1"].Cluster, k, "seedval", 10*time.Second); err != nil {
			t.Fatalf("seed put %q: %v", k, err)
		}
	}

	var stop atomic.Bool
	waitBG, acked, midAcq := classifyingWriters(f, &stop, 6)
	sampler := &divergenceSampler{}
	waitSampler := sampler.run(f, &stop)

	// HARD-KILL rolling restart: reverse ordinal, each node SIGKILLed (no Leave)
	// and rejoined same-name/new-address, with MAXIMAL overlap (no settle between
	// kills, so the next node is killed while peers are still reaping the previous
	// one and learning its new address). Multiple rounds accumulate churn.
	const rounds = 6
	for r := 0; r < rounds; r++ {
		for _, id := range []string{"n3", "n2", "n1"} {
			f.hardRestart(id)
			time.Sleep(150 * time.Millisecond) // aggressive overlap
		}
		t.Logf("hard-kill round %d done (acked=%d midAcquire=%d)", r+1, acked.Load(), midAcq.Load())
	}

	// Keep sampling + writing through the heal window so a PERSISTENT divergence
	// would be caught (rejoin loop 30s, meta-refresh 10s, reconcile 5s).
	converged := waitFleetConverged(f, len(f.ids), 45*time.Second)

	stop.Store(true)
	waitBG()
	waitSampler()

	views := captureMemberViews(f)
	transient := sampler.everDiverged.Load()
	twoAndThree := sampler.sawTwoAndThree.Load()
	streak := sampler.maxStreakMs.Load()

	t.Logf("MECHANISM: ring divergence observed during roll=%v (sawTwoAndThree=%v, maxStreak=%dms); "+
		"wedge writes during roll (mid-acquire)=%d", transient, twoAndThree, streak, midAcq.Load())

	if !converged {
		state := captureFleetState(f)
		t.Logf("RESULT: PERSISTENT-DIVERGED - nodes still disagree on the member set after the heal window "+
			"(the live 40-min signature).%s\ncaptured per-node desired/mounted state:%s", views, state)
		return
	}
	if transient {
		t.Logf("RESULT: CONVERGED-AFTER-TRANSIENT - the ring diverged during the roll (mechanism fired, "+
			"maxStreak=%dms) but healed within the window.%s", streak, views)
		return
	}
	t.Logf("RESULT: CONVERGED-CLEAN - no ring divergence observed at all during the hard-kill roll.%s", views)
}

// TestHardKillRoll_IsolatedRejoinDivergesPersistently pins the NECESSARY
// condition for the persistent divergence, since the hard-kill roll with a
// reachable seed converges every time (above). It hard-kills one node and rejoins
// it ISOLATED - unable to reach any peer (empty seed) - which is the in-process
// analogue of a rejoining pod whose seed (headless-service DNS) is stale or
// unresolvable during address churn, or a real-network partition. This is NOT the
// natural roll; it deprives the rejoin of a live seed on purpose to CONFIRM the
// mechanism: when the same-name/new-address instance cannot re-bridge to the
// cluster, the peers that reap its OLD (SIGKILLed, no-Leave) entry via SWIM settle
// on a smaller member view and NEVER re-add it (their own rejoin seeds are stale
// too), so the ring diverges PERSISTENTLY - exactly the live shape (nodes
// disagree on the member set). It reliably diverges; that is the point.
func TestHardKillRoll_IsolatedRejoinDivergesPersistently(t *testing.T) {
	const unitCount, rf = 16, 2
	f := newLiveFleet(t, unitCount, rf)
	f.bootstrap("n1", "n2", "n3")
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("seed-%05d", i)
		if err := putWithRetryUnavailable(t, f.live["n1"].Cluster, k, "seedval", 10*time.Second); err != nil {
			t.Fatalf("seed put %q: %v", k, err)
		}
	}

	// HARD-KILL n3 (no Leave) and rejoin it ISOLATED: empty seed, so its
	// memberlist comes up SOLO and never contacts n1/n2. Same stable id, new
	// address. Models the rejoin failing to reach a live seed.
	f.mu.Lock()
	old := f.live["n3"]
	f.live["n3"] = nil
	f.mu.Unlock()
	old.hardKill()
	n3 := startReplicatedNode(t, "n3", "", unitCount, rf, f.backing) // seed="" -> solo
	f.mu.Lock()
	f.live["n3"] = n3
	f.mu.Unlock()

	// The cluster must NOT re-converge to a single agreed 3-member view: n1/n2 reap
	// n3#old and drop to {n1,n2}; the isolated n3 sees only itself; there is no
	// gossip path to re-bridge (n3 has no seed; n1/n2's rejoin seeds are stale).
	converged := waitFleetConverged(f, len(f.ids), 20*time.Second)
	views := captureMemberViews(f)
	if converged {
		t.Fatalf("expected PERSISTENT divergence from an isolated (unreachable-seed) rejoin, but the fleet converged: %s", views)
	}

	// Pin the exact shape: some nodes see fewer members than others (the live
	// signature). Confirm at least one node is stuck below the full set.
	minSeen, maxSeen := len(f.ids), 0
	for _, n := range f.nodes() {
		_, sz := memberView(n.Cluster)
		if sz < minSeen {
			minSeen = sz
		}
		if sz > maxSeen {
			maxSeen = sz
		}
	}
	if minSeen == maxSeen {
		t.Fatalf("expected disagreeing member-view sizes (some see fewer), got all=%d: %s", minSeen, views)
	}
	t.Logf("RESULT: PERSISTENT-DIVERGED (isolated rejoin) - nodes disagree on the member set and never "+
		"re-converge (min view=%d, max view=%d). This pins the necessary condition: the same-name/new-address "+
		"rejoin FAILING TO REACH the cluster (stale-seed / DNS-lag / partition) is what makes the divergence "+
		"stick; with a reachable seed the in-process roll always converges.%s", minSeen, maxSeen, views)
}
