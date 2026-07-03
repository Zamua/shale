package integration

// ROLLING-RESTART CONVERGENCE WEDGE reproduction (investigation harness).
//
// The live failure this models: a 3-node, RF=2, 16-unit, homogeneous shale
// cluster, after a rolling restart, gets stuck with many units running at
// 1-of-2 replicas, so writes fail "shale: write needed 2 acks, got 1 (replicas
// mid-acquire)" (codes.Unavailable) and do NOT recover for a long time
// (~40 min observed live). Two ingredients are believed involved:
//
//  1. BOOT-DEFER-ALL: on reboot a node reads back its OWN serving marker (a
//     plain epoch object with no node identity) and defers opening ALL its
//     units. shale's periodic self-heal reconcile re-acquires deferred
//     positions LOCALLY (no quorum), so on a CONSISTENT ring it heals fast.
//  2. RING / MEMBERSHIP CONVERGENCE RACE (believed to be why it STICKS): if the
//     3 nodes do not converge to a consistent member view, each node computes a
//     DIFFERENT replica pair per unit, so a write's fan-out targets a node that
//     does not own (and never mounts) that position -> perpetual 1-ack.
//
// This file is the DISCOVERY arm: it stands up the real-gossip harness, churns
// the cluster with repeated restarts (accumulating stale marker/membership
// state the way the live cluster did), performs a rolling restart, then polls
// for write recovery. If writes do NOT recover it dumps each node's member view
// (do the nodes AGREE on the ring?) and each node's per-unit desired/mounted
// state (the /debug/shale/state equivalent), so a wedge tells us WHICH arm it
// is: rings DISAGREE -> membership race; rings AGREE but units unmounted ->
// storage transition stall. The companion deterministic mechanism tests live in
// rolling_restart_wedge_mechanism_test.go.
//
// It reuses the R=2 shared-backing harness (start3NodeR2 / startReplicatedNode /
// sharedfactory.Backing): the Backing is the durable object-storage analogue and
// SURVIVES a node restart (its serving markers + per-replica epochs persist),
// which is exactly what makes boot-defer-all and epoch churn observable
// in-process.

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// liveFleet tracks the currently-live node per stable id across restarts, plus
// the shared durable backing every instance mounts. Restarting a node replaces
// its entry in place (same id, same backing, a fresh process = fresh bind/grpc
// address), modeling a k8s pod that comes back on a new IP with the SAME stable
// identity while its durable object storage persists.
type liveFleet struct {
	t         *testing.T
	backing   *sharedfactory.Backing
	unitCount int
	rf        int
	ids       []string

	mu   sync.Mutex // guards live against the background writers reading it mid-restart
	live map[string]*sharedNode
}

func newLiveFleet(t *testing.T, unitCount, rf int) *liveFleet {
	return &liveFleet{
		t:         t,
		backing:   sharedfactory.NewBacking(),
		unitCount: unitCount,
		rf:        rf,
		live:      map[string]*sharedNode{},
	}
}

// bootstrap stands up ids[0] as founder and the rest seeded to it, then waits
// for the whole cluster to converge on membership.
func (f *liveFleet) bootstrap(ids ...string) {
	f.t.Helper()
	f.ids = append([]string(nil), ids...)
	founder := startReplicatedNode(f.t, ids[0], "", f.unitCount, f.rf, f.backing)
	f.mu.Lock()
	f.live[ids[0]] = founder
	f.mu.Unlock()
	for _, id := range ids[1:] {
		n := startReplicatedNode(f.t, id, founder.BindAddr, f.unitCount, f.rf, f.backing)
		f.mu.Lock()
		f.live[id] = n
		f.mu.Unlock()
	}
	f.waitConverged(20 * time.Second)
}

// seedsFor returns the CURRENT bind address of some live node other than
// `except` (the faithful "reachable headless-service seed" model: a restarted
// pod always reaches the pods that are up, so we never manufacture a harness-only
// split by seeding to a stale address). Caller holds f.mu.
func (f *liveFleet) seedsForLocked(except string) string {
	for _, id := range f.ids {
		if id == except {
			continue
		}
		if n := f.live[id]; n != nil && n.Cluster != nil {
			return n.BindAddr
		}
	}
	return ""
}

// restart replaces the node with stable id `id`: it gracefully closes the old
// instance (best case for peer convergence) and brings a fresh instance up on a
// new bind/grpc address, seeded to a live peer, over the SAME durable backing.
func (f *liveFleet) restart(id string) {
	f.t.Helper()
	f.mu.Lock()
	seed := f.seedsForLocked(id)
	old := f.live[id]
	f.mu.Unlock()
	if old != nil {
		old.Close() // graceful membership Leave + gRPC stop
	}
	n := startReplicatedNode(f.t, id, seed, f.unitCount, f.rf, f.backing)
	f.mu.Lock()
	f.live[id] = n
	f.mu.Unlock()
}

// clusters returns the live *cluster.Cluster handles in stable-id order,
// skipping any node mid-restart (nil Cluster).
func (f *liveFleet) clusters() []*cluster.Cluster {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*cluster.Cluster, 0, len(f.ids))
	for _, id := range f.ids {
		if n := f.live[id]; n != nil && n.Cluster != nil {
			out = append(out, n.Cluster)
		}
	}
	return out
}

func (f *liveFleet) nodes() []*sharedNode {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*sharedNode, 0, len(f.ids))
	for _, id := range f.ids {
		if n := f.live[id]; n != nil && n.Cluster != nil {
			out = append(out, n)
		}
	}
	return out
}

func (f *liveFleet) waitConverged(timeout time.Duration) {
	f.t.Helper()
	if err := waitForMembersAll(f.clusters(), len(f.ids), timeout); err != nil {
		f.t.Fatalf("membership did not converge to %d: %v", len(f.ids), err)
	}
}

// unitKeys returns one representative key per unit id (0..unitCount-1), so a
// probe can attempt a write that routes to EVERY unit.
func unitKeys(unitCount int) map[storageunit.UnitID]string {
	out := make(map[storageunit.UnitID]string, unitCount)
	for i := 0; len(out) < unitCount; i++ {
		k := fmt.Sprintf("uk-%06d", i)
		u := unitOf(k, unitCount)
		if _, ok := out[u]; !ok {
			out[u] = k
		}
	}
	return out
}

// backgroundWriters keeps acking new keys through a rotating entry node until
// stop is set, returning the wait func + a pointer to the acked-count. Errors
// are tolerated (a restart-in-progress is expected to blip); the point is to
// keep write traffic flowing across every unit during churn, not to gate on it.
func backgroundWriters(f *liveFleet, stop *atomic.Bool, writers int) (wait func(), acked *atomic.Int64) {
	var wg sync.WaitGroup
	var n atomic.Int64
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
				k := fmt.Sprintf("bg-%d-%07d", w, i)
				if err := entry.Put([]byte(k), []byte("v")); err == nil {
					n.Add(1)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}
	return wg.Wait, &n
}

// wedgeReport is the captured internal state at the moment recovery timed out.
type wedgeReport struct {
	membersAgree bool
	perNode      string // formatted member views + DebugState per node
	failingUnits []storageunit.UnitID
}

// pollUnitWritesRecover attempts, for up to timeout, a write to every unit
// through a rotating entry node. It returns nil once EVERY unit accepts a write
// in a single clean pass (the cluster is back to serving all positions). On
// timeout it returns a wedgeReport with the units that never recovered plus the
// captured cross-node state. A per-attempt write is bounded so a wedged unit
// (perpetual "mid-acquire") does not consume the whole budget on one key.
func pollUnitWritesRecover(f *liveFleet, timeout time.Duration) *wedgeReport {
	t := f.t
	t.Helper()
	keys := unitKeys(f.unitCount)
	deadline := time.Now().Add(timeout)
	var lastFailing []storageunit.UnitID
	for time.Now().Before(deadline) {
		cs := f.clusters()
		if len(cs) == 0 {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		failing := make([]storageunit.UnitID, 0)
		i := 0
		for u, k := range keys {
			entry := cs[i%len(cs)]
			i++
			// Bounded single write (no long retry): we want to observe whether the
			// unit is serving NOW, and let the outer loop re-probe.
			if err := putBounded(entry, k, "probe", 200*time.Millisecond); err != nil {
				failing = append(failing, u)
			}
		}
		if len(failing) == 0 {
			return nil // clean sweep: fully recovered.
		}
		lastFailing = failing
		time.Sleep(200 * time.Millisecond)
	}
	sort.Slice(lastFailing, func(a, b int) bool { return lastFailing[a] < lastFailing[b] })
	return &wedgeReport{
		membersAgree: membersAgree(f),
		perNode:      captureFleetState(f),
		failingUnits: lastFailing,
	}
}

// putBounded issues a single Put and, on the retryable acquiring-window error,
// re-tries only within a short bound (so a genuinely-wedged unit fails fast for
// the recovery poll rather than blocking the whole sweep on one key).
func putBounded(c *cluster.Cluster, key, val string, bound time.Duration) error {
	deadline := time.Now().Add(bound)
	for {
		err := c.Put([]byte(key), []byte(val))
		if err == nil {
			return nil
		}
		if st, _ := status.FromError(err); st.Code() != codes.Unavailable || time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// membersAgree reports whether every live node sees the SAME set of member IDs.
// Divergence here is the membership-race signature.
func membersAgree(f *liveFleet) bool {
	var ref []string
	for _, n := range f.nodes() {
		ids := memberIDs(n.Cluster)
		if ref == nil {
			ref = ids
			continue
		}
		if strings.Join(ids, ",") != strings.Join(ref, ",") {
			return false
		}
	}
	return true
}

func memberIDs(c *cluster.Cluster) []string {
	ms := c.Members()
	ids := make([]string, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, m.ID)
	}
	sort.Strings(ids)
	return ids
}

// captureFleetState formats, per live node: its member view (id -> addr) and its
// DebugState() per-unit dump (desired/pending/mounted/phase, flagging
// desired-but-unmounted). This is the deliverable that tells us WHICH arm.
func captureFleetState(f *liveFleet) string {
	var b strings.Builder
	for _, n := range f.nodes() {
		fmt.Fprintf(&b, "\n===== node %s =====\n", n.ID)
		fmt.Fprintf(&b, "member-view: %s\n", formatMembers(n.Cluster))
		fmt.Fprintf(&b, "%s", n.Cluster.DebugState())
	}
	return b.String()
}

func formatMembers(c *cluster.Cluster) string {
	ms := c.Members()
	sort.Slice(ms, func(i, j int) bool { return ms[i].ID < ms[j].ID })
	parts := make([]string, 0, len(ms))
	for _, m := range ms {
		parts = append(parts, fmt.Sprintf("%s@%s", m.ID, m.Addr))
	}
	return strings.Join(parts, " ")
}

// replicaSetOn previews the ordered replica node ids for a key on cluster c's
// OWN ring (the deterministic hashing the cluster routes with), so a wedge dump
// can show which nodes each viewer thinks own a failing unit.
func replicaSetOn(c *cluster.Cluster, key string, unitCount, rf int) []string {
	rg := ring.New()
	for _, m := range c.Members() {
		rg.Add(m)
	}
	u := storageunit.UnitForShardKey(ring.ShardKey([]byte(key)), storageunit.MustUnitCount(unitCount))
	set := rg.LocateKeyN(gen0UnitBytes(u), rf)
	out := make([]string, 0, len(set))
	for _, m := range set {
		out = append(out, m.ID)
	}
	return out
}

// TestRollingRestartWedge_Churned is the discovery test. It churns a 3-node R=2
// cluster with repeated restarts, does a final rolling restart, and asserts
// writes RECOVER on every unit within the budget. A failure here is the wedge
// reproduced; the failure message carries the captured cross-node state that
// identifies the arm.
func TestRollingRestartWedge_Churned(t *testing.T) {
	const unitCount, rf = 16, 2
	f := newLiveFleet(t, unitCount, rf)
	f.bootstrap("n1", "n2", "n3")

	// Seed a small recorded dataset so there are real serving markers + epochs on
	// disk before we start churning.
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("seed-%05d", i)
		if err := putWithRetryUnavailable(t, f.live["n1"].Cluster, k, "seedval", 10*time.Second); err != nil {
			t.Fatalf("seed put %q: %v", k, err)
		}
	}

	// CHURN: repeated rolling restarts (reverse ordinal), each node coming back on
	// a new address over the same durable backing, with background writers running
	// throughout so every unit accrues writes + climbing epochs. The rolls OVERLAP
	// convergence: only a SHORT settle between kills (well under the SWIM
	// convergence + reconcile cadence), modeling the "hard-kill exactly as the
	// previous node rejoins" interleaving. We do NOT wait for full convergence
	// between individual kills - only once per round - so a node can be killed
	// while a peer is still mid-rejoin.
	var stop atomic.Bool
	waitBG, acked := backgroundWriters(f, &stop, 6)

	const churnRounds = 6
	for r := 0; r < churnRounds; r++ {
		for _, id := range []string{"n3", "n2", "n1"} {
			f.restart(id)
			time.Sleep(400 * time.Millisecond) // short: overlaps convergence
		}
		f.waitConverged(25 * time.Second)
		t.Logf("churn round %d complete (acked so far=%d)", r+1, acked.Load())
	}

	stop.Store(true)
	waitBG()
	t.Logf("churn done: %d background writes acked across %d rounds", acked.Load(), churnRounds)

	// FINAL recovery gate: every unit must accept a write within the budget.
	if rep := pollUnitWritesRecover(f, 45*time.Second); rep != nil {
		var failKeys []string
		uk := unitKeys(unitCount)
		for _, u := range rep.failingUnits {
			failKeys = append(failKeys, fmt.Sprintf("unit%d(key=%s)", u, uk[u]))
		}
		// Per failing unit, show each viewer's computed replica set: divergence is
		// the membership-race fingerprint.
		var rs strings.Builder
		for _, u := range rep.failingUnits {
			k := uk[u]
			fmt.Fprintf(&rs, "\nunit %d (key %s) replica-set-per-viewer:", u, k)
			for _, n := range f.nodes() {
				fmt.Fprintf(&rs, "\n  view[%s] = %v", n.ID, replicaSetOn(n.Cluster, k, unitCount, rf))
			}
		}
		t.Fatalf("WEDGE REPRODUCED: %d/%d units never accepted a write in 45s.\n"+
			"members-agree=%v (false => MEMBERSHIP-RACE arm; true => STORAGE-STALL arm)\n"+
			"failing units: %v%s\n"+
			"captured per-node state:%s",
			len(rep.failingUnits), unitCount, rep.membersAgree, failKeys, rs.String(), rep.perNode)
	}
	t.Logf("RECOVERED: all %d units accept writes after churn + rolling restart", unitCount)
}
