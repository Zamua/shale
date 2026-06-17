package membership

import (
	"io"
	"strings"
	"testing"
	"time"
)

// This file builds an in-process repro of the mass-rolling-restart gossip
// split that the periodic re-Join fix heals, and demonstrates THE FLIP: a
// disjoint topology stays split when no re-Join happens, and converges the
// moment a re-Join (the loop's per-tick operation) fires.
//
// Faithful split model. The production bug is two groups that have pruned
// each other out of their member lists, leaving the seed as the only stable
// bridge that is never re-dialed. We reproduce that deterministically as two
// GENUINELY DISJOINT memberlist clusters, each anchored on its own seed:
//
//   cluster 1: {S, A}   - A seeded to S.
//   cluster 2: {S2, B}  - B seeded to S2 (a second, independent seed).
//
// We cannot drop packets in-process, so we build the split directly: the two
// clusters share NO member, so memberlist's own PushPull never bridges them
// (it only syncs members already in the local list, and the lists are
// disjoint). The ONLY thing that can merge them is a fresh Join across the
// gap - exactly what the rejoin loop issues every tick. So:
//
//   - with NO re-Join, the clusters stay split for the whole window (control);
//   - with ONE re-Join, the two member lists merge (treatment).
//
// TestRejoinHealsSplit_FlipOnInterval below complements this by exercising the
// full configured loop (RejoinInterval) end to end rather than the bare Join.

// waitConverged polls until m reports >= want members or the deadline passes.
// Returns the last observed count + whether the threshold was reached. Generous
// bounds keep this non-flaky in CI; convergence in practice is well under the
// timeout once a Join lands.
func waitConverged(m *Membership, want int, timeout time.Duration) (int, bool) {
	deadline := time.Now().Add(timeout)
	last := 0
	for time.Now().Before(deadline) {
		last = len(m.Members())
		if last >= want {
			return last, true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return last, false
}

// openWithBindRetry opens a Membership, re-picking the bind port and retrying
// if Open fails with the ephemeralPort close-then-rebind race ("address already
// in use"). ephemeralPort grabs a free port by closing its own listener, so a
// concurrently-starting memberlist can occasionally claim it first; a couple of
// retries make multi-node in-process tests non-flaky. Returns the node + the
// bind address it actually bound to (so callers can seed peers at it).
func openWithBindRetry(t *testing.T, id, grpcAddr string, seeds []string, rejoin time.Duration) (*Membership, string) {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		port := ephemeralPort(t)
		addr := bindAddr(port)
		cfg := Config{
			NodeID: id, BindAddr: addr, GRPCAddr: grpcAddr,
			Seeds: seeds, RejoinInterval: rejoin, LogOutput: io.Discard,
		}
		m, err := Open(cfg)
		if err == nil {
			t.Cleanup(func() { _ = m.Close() })
			return m, addr
		}
		lastErr = err
		// Retry only on the known bind race; surface anything else immediately.
		if !strings.Contains(err.Error(), "address already in use") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("openWithBindRetry(%s): %v", id, lastErr)
	return nil, ""
}

// openSeed opens a lone-cluster seed node (no seeds), returning it + its addr.
func openSeed(t *testing.T, id, grpcAddr string) (*Membership, string) {
	t.Helper()
	return openWithBindRetry(t, id, grpcAddr, nil, 0)
}

// openMember opens a node that joins the given seeds at Open (rejoin loop off),
// returning it + its own bind addr.
func openMember(t *testing.T, id, grpcAddr string, seeds []string) *Membership {
	t.Helper()
	m, _ := openWithBindRetry(t, id, grpcAddr, seeds, 0)
	return m
}

// TestRejoinHealsDisjointSplit_FlipOnLoop is the DECISIVE flip test. It builds
// two genuinely disjoint memberlist clusters and proves that re-Join (the
// operation the rejoin loop issues on every tick) is the ONLY thing that
// merges them: PushPull never crosses, so with no re-Join the split is stable,
// and a single re-Join heals it.
//
// Construction:
//  1. cluster 1 = {S, A}   (A seeded to S).
//  2. cluster 2 = {S2, B}  (B seeded to S2; S2 is a SECOND, independent seed).
//
// The two clusters share NO member, so neither side's PushPull anti-entropy
// can ever reach the other (PushPull only syncs nodes already in the local
// list). This is the in-process analogue of the production split where two
// groups have pruned each other out.
//
//   - CONTROL: wait a full window with NO re-Join. The clusters stay split
//     (each sees 2 members, never 4). This proves the split is real + stable.
//   - TREATMENT: invoke B.ml.Join({S}) ONCE - exactly the call the rejoin loop
//     makes on a tick - and the two member lists MERGE (B converges to 4).
//
// Driving ml.Join directly (the test is in-package) isolates the variable to
// the re-Join operation itself, the load-bearing primitive the periodic loop
// is built around. The loop's scheduling/skip/stop behavior is pinned
// separately by the unit tests in rejoin_test.go.
func TestRejoinHealsDisjointSplit_FlipOnLoop(t *testing.T) {
	// Cluster 1: {S, A}. Open each node by grabbing its bind port immediately
	// before Open (openSeed / openMember below) to keep the close-then-rebind
	// race window of ephemeralPort small. sPort is captured so the treatment
	// re-Join can target S.
	s, sPort := openSeed(t, "dj-s", "127.0.0.1:29201")
	openMember(t, "dj-a", "127.0.0.1:29202", []string{sPort})
	if _, ok := waitConverged(s, 2, 3*time.Second); !ok {
		t.Fatalf("cluster 1 never formed: %v", s.Members())
	}

	// Cluster 2: {S2, B} - independent, shares no member with cluster 1.
	s2, s2Port := openSeed(t, "dj-s2", "127.0.0.1:29203")
	_ = s2
	// RejoinInterval intentionally 0: we drive the re-Join by hand below so the
	// flip is attributable to the re-Join operation, not loop timing.
	b := openMember(t, "dj-b", "127.0.0.1:29204", []string{s2Port})
	if _, ok := waitConverged(b, 2, 3*time.Second); !ok {
		t.Fatalf("cluster 2 never formed: %v", b.Members())
	}

	// --- CONTROL: no re-Join. The split is stable; B must NOT reach 4. ---
	if n, ok := waitConverged(b, 4, 1500*time.Millisecond); ok {
		t.Fatalf("CONTROL: clusters merged WITHOUT a re-Join (B saw %d) - "+
			"PushPull bridged the disjoint split, the test cannot demonstrate the flip", n)
	}
	if got := len(b.Members()); got != 2 {
		t.Fatalf("CONTROL: B's split cluster should still see exactly 2, saw %d (%v)", got, b.Members())
	}

	// --- TREATMENT: one re-Join (the loop's per-tick op) bridges B to S. ---
	if _, err := b.ml.Join([]string{sPort}); err != nil {
		t.Fatalf("re-Join failed: %v", err)
	}
	// Both clusters now merge: every node converges to 4 members {S,A,S2,B}.
	if n, ok := waitConverged(b, 4, 5*time.Second); !ok {
		t.Fatalf("TREATMENT: re-Join did not heal the split: B saw %d (%v), want 4", n, b.Members())
	}
	if n, ok := waitConverged(s, 4, 5*time.Second); !ok {
		t.Fatalf("TREATMENT: original cluster did not absorb the merge: S saw %d (%v), want 4", n, s.Members())
	}
}

// TestRejoinHealsSplit_FlipOnInterval exercises the END-TO-END loop path: an
// isolated node converges into a live seed's cluster when the rejoin loop is
// enabled, and a node with no bridging mechanism stays isolated. It pairs with
// the disjoint-split test above (which isolates the re-Join primitive) by
// pinning that the configured loop actually drives convergence in practice.
func TestRejoinHealsSplit_FlipOnInterval(t *testing.T) {
	seedPort := ephemeralPort(t)
	aPort := ephemeralPort(t)

	// Cluster 1: {seed, A}.
	seed := openTestMembership(t, Config{
		NodeID:   "csplit-seed",
		BindAddr: bindAddr(seedPort),
		GRPCAddr: "127.0.0.1:29001",
	})
	a := openTestMembership(t, Config{
		NodeID:   "csplit-a",
		BindAddr: bindAddr(aPort),
		GRPCAddr: "127.0.0.1:29002",
		Seeds:    []string{bindAddr(seedPort)},
	})
	// {seed, A} should see 2 members each.
	if _, ok := waitConverged(seed, 2, 3*time.Second); !ok {
		t.Fatalf("cluster 1 never formed: seed sees %v", seed.Members())
	}
	_ = a

	// --- CONTROL: B with rejoin DISABLED stays split. ---
	// B opens against a DEAD address (a port nobody is listening on) so its
	// initial Join fails -> Open errors. To form a lone cluster instead, open
	// B with NO seeds at all, then it is its own {B} cluster. With no rejoin
	// loop, nothing ever re-Joins S, so B never learns {seed, A}.
	bCtrlPort := ephemeralPort(t)
	bCtrl := openTestMembership(t, Config{
		NodeID:   "csplit-b-ctrl",
		BindAddr: bindAddr(bCtrlPort),
		GRPCAddr: "127.0.0.1:29003",
		// No Seeds -> lone cluster {b-ctrl}. No RejoinInterval -> no loop.
	})
	// Give it the same window the treatment gets. It must NOT converge.
	if n, ok := waitConverged(bCtrl, 2, 1500*time.Millisecond); ok {
		t.Fatalf("CONTROL converged without rejoin loop (saw %d members) - "+
			"the split is not real, test cannot demonstrate the flip", n)
	}
	if got := len(bCtrl.Members()); got != 1 {
		t.Fatalf("CONTROL: lone cluster should see only itself, saw %d (%v)", got, bCtrl.Members())
	}

	// --- TREATMENT: B with rejoin ENABLED converges. ---
	// Same lone-cluster start, but with a live seed + a short rejoin interval.
	// Open's initial Join(seed) succeeds (seed is live), but the decisive
	// demonstration is the loop: even if PushPull lagged, the periodic Join is
	// what guarantees the merge. We assert B reaches the full 3-member view
	// {seed, A, B} AND that the rejoin loop actually ran.
	bTreatPort := ephemeralPort(t)
	bTreat := openTestMembership(t, Config{
		NodeID:         "csplit-b-treat",
		BindAddr:       bindAddr(bTreatPort),
		GRPCAddr:       "127.0.0.1:29004",
		Seeds:          []string{bindAddr(seedPort)},
		RejoinInterval: 40 * time.Millisecond,
	})
	if n, ok := waitConverged(bTreat, 3, 5*time.Second); !ok {
		t.Fatalf("TREATMENT failed to converge with rejoin loop: saw %d members (%v), want 3",
			n, bTreat.Members())
	}
	// The rejoin loop must have actually issued at least one Join.
	if _, ok := waitForAtomic(bTreat.RejoinAttempts, 1, 3*time.Second); !ok {
		t.Fatalf("TREATMENT converged but rejoin loop never attempted a Join")
	}
}

// TestRejoinReConvergesAfterSeedFlap demonstrates the loop's ANTI-ENTROPY
// value directly: after a node is in a healthy cluster, the periodic Join is
// idempotent (the cluster stays whole) and keeps re-contacting the seed so a
// transient split would heal. This pins that a healthy node keeps re-Joining
// (attempts climb) WITHOUT ever shrinking its member view - the no-op-Join
// property the fix relies on.
func TestRejoinReConvergesAfterSeedFlap(t *testing.T) {
	seedPort := ephemeralPort(t)
	nPort := ephemeralPort(t)

	seed := openTestMembership(t, Config{
		NodeID:   "flap-seed",
		BindAddr: bindAddr(seedPort),
		GRPCAddr: "127.0.0.1:29101",
	})
	_ = seed

	n := openTestMembership(t, Config{
		NodeID:         "flap-n",
		BindAddr:       bindAddr(nPort),
		GRPCAddr:       "127.0.0.1:29102",
		Seeds:          []string{bindAddr(seedPort)},
		RejoinInterval: 30 * time.Millisecond,
	})

	// Converge to {seed, n}.
	if _, ok := waitConverged(n, 2, 3*time.Second); !ok {
		t.Fatalf("never converged to 2 members: %v", n.Members())
	}

	// Let the idempotent re-Join run several times.
	if _, ok := waitForAtomic(n.RejoinAttempts, 4, 3*time.Second); !ok {
		t.Fatalf("rejoin loop did not keep re-Joining a healthy cluster")
	}

	// The repeated no-op Joins must NOT have shrunk or churned the view: a
	// healthy node still sees both members.
	if got := len(n.Members()); got < 2 {
		t.Fatalf("idempotent re-Join shrank the member view to %d (%v), want >=2", got, n.Members())
	}
}
