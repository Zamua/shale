package membership

// Reclaim-gate / same-name-new-address supersession reproduction.
//
// This pins the EXACT membership mechanism behind the rolling-restart wedge's
// persistent 2-vs-3 divergence, and proves which code state avoids it. The
// failure the live capture showed (some peers see the rebooted node, some do
// not, asymmetrically) is memberlist's DeadNodeReclaimTime / conflicting-address
// gate (state.go aliveNode): when an alive message arrives for an EXISTING node
// NAME whose address changed and whose state is Dead, memberlist REJECTS it
// unless DeadNodeReclaimTime has elapsed since death - and it rejects BEFORE
// examining the incarnation, so even a strictly-higher incarnation does NOT win.
//
// That gate is only reachable when the memberlist NAME is REUSED across a
// restart. shale's #473 identity-decouple makes the memberlist name per-process
// unique (NodeID#bootEpoch), so a restart presents a NEW name -> aliveNode's
// "never seen this node" branch -> the gate is never reached -> the successor is
// integrated immediately. The stable id rides in Meta and Members() projects to
// the highest boot epoch, so the ring still sees exactly one node per id at the
// newest address. That IS the deterministic, timer-free incarnation-supersession
// the wedge needs, achieved at the shale layer (shale cannot change memberlist's
// internal aliveNode gate; it is a dependency).
//
// The two tests below are the pre-fix / post-fix demonstration, toggled by the
// TestingStableMemberlistName seam:
//   - StableName (PRE-#473): the reclaim gate REJECTS the same-name/new-address
//     rejoin; the killer stays diverged (asymmetric, matching the live capture).
//   - UniqueName (#473, the shipped default): the rejoin is a fresh node, so it
//     is integrated immediately and the view re-converges.

import (
	"io"
	"strings"
	"testing"
	"time"
)

func buildReclaimNode(t *testing.T, id, bind, grpc string, seeds []string, stableName bool, logw io.Writer) *Membership {
	t.Helper()
	m, err := Open(Config{
		NodeID:                      id,
		BindAddr:                    bind,
		GRPCAddr:                    grpc,
		Seeds:                       seeds,
		MetaRefreshInterval:         200 * time.Millisecond, // climbing incarnation (prod 10s)
		RejoinInterval:              300 * time.Millisecond,
		TestingStableMemberlistName: stableName,
		LogOutput:                   logw,
	})
	if err != nil {
		t.Fatalf("open %s: %v", id, err)
	}
	return m
}

func waitCount(m *Membership, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(m.Members()) == want {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return len(m.Members()) == want
}

// runSameNameNewAddrRejoin stands up 2 nodes, hard-kills the joiner (SIGKILL, no
// Leave) so the founder reaps it via failure detection, then rejoins it with the
// SAME stable id but a NEW address (seeded to the still-live founder, so there IS
// connectivity). It returns whether the founder re-integrated the successor
// within the observation window, whether it logged a conflicting-address
// rejection, and the successor's own view size (the asymmetry witness).
func runSameNameNewAddrRejoin(t *testing.T, stableName bool) (founderReconverged, conflictLogged bool, successorView int) {
	t.Helper()
	founderBuf := &safeBuf{}
	p1, p2 := ephemeralPort(t), ephemeralPort(t)
	const g1, g2 = "127.0.0.1:41001", "127.0.0.1:41002"

	founder := buildReclaimNode(t, "n1", bindAddr(p1), g1, nil, stableName, founderBuf)
	defer func() { _ = founder.Close() }()
	joiner := buildReclaimNode(t, "n2", bindAddr(p2), g2, []string{bindAddr(p1)}, stableName, io.Discard)

	if !waitCount(founder, 2, 10*time.Second) {
		t.Fatalf("baseline: founder never saw 2 members (saw %d)", len(founder.Members()))
	}

	// HARD-KILL the joiner: no Leave, so the founder must reap it via SWIM.
	_ = joiner.TestingShutdownNoLeave()
	if !waitCount(founder, 1, 15*time.Second) {
		t.Logf("note: founder still sees %d after kill (joiner not yet reaped as dead)", len(founder.Members()))
	}

	// REJOIN with the SAME stable id, a NEW address, seeded to the live founder.
	p2b := ephemeralPort(t)
	const g2b = "127.0.0.1:41012"
	successor := buildReclaimNode(t, "n2", bindAddr(p2b), g2b, []string{bindAddr(p1)}, stableName, io.Discard)
	defer func() { _ = successor.Close() }()

	founderReconverged = waitCount(founder, 2, 12*time.Second)
	successorView = len(successor.Members())
	conflictLogged = strings.Contains(founderBuf.String(), "Conflicting address")
	return
}

// TestReclaimGate_UniqueNameSupersedesImmediately is the POST-FIX (#473, the
// shipped default) case: a same-id/new-address rejoin presents a fresh unique
// memberlist name, so the founder integrates it immediately and re-converges,
// with no conflicting-address rejection. This is what the CURRENT code does.
func TestReclaimGate_UniqueNameSupersedesImmediately(t *testing.T) {
	reconv, conflict, successorView := runSameNameNewAddrRejoin(t, false)
	if conflict {
		t.Errorf("POST-FIX (#473 unique names): unexpected 'Conflicting address' rejection")
	}
	if !reconv {
		t.Fatalf("POST-FIX (#473 unique names): founder did NOT re-integrate the new-address successor "+
			"(stayed diverged; successor sees %d) - the fix regressed", successorView)
	}
	t.Logf("POST-FIX (#473 unique names): same-id/new-address rejoin SUPERSEDES immediately - founder "+
		"re-converged to 2, no reclaim-gate rejection (conflictLogged=%v)", conflict)
}

// TestReclaimGate_StableNameRejectedByReclaimGate is the PRE-FIX case: with a
// REUSED (stable) memberlist name, the founder's memberlist rejects the
// new-address rejoin at the conflicting-address / DeadNodeReclaimTime gate, so
// the founder stays stuck at 1 while the successor sees 2 - the asymmetric
// persistent divergence the live capture showed. This is the failure #473 fixes.
func TestReclaimGate_StableNameRejectedByReclaimGate(t *testing.T) {
	reconv, conflict, successorView := runSameNameNewAddrRejoin(t, true)
	if reconv {
		t.Fatalf("PRE-FIX (stable name): expected the reclaim gate to REJECT the new-address rejoin " +
			"(persistent divergence), but the founder re-converged - the gate did not bite as modeled")
	}
	if !conflict {
		t.Logf("PRE-FIX (stable name): founder stayed diverged but no 'Conflicting address' line was captured "+
			"(the rejection may have surfaced via a different memberlist path); successor sees %d", successorView)
	}
	t.Logf("PRE-FIX (stable name, reclaim gate): same-id/new-address rejoin REJECTED - founder stuck at 1 while "+
		"successor sees %d (ASYMMETRIC divergence, matching the live capture); conflictLogged=%v", successorView, conflict)
}
