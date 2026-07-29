package integration

// THE QUORUM-FLOOR DATA-SAFETY GATE (the mirror's dangerous edge).
//
// Join write-transparency (TestJoinIsWriteTransparent) excludes a warming
// newcomer from the CURRENT set so the still-mounted displaced owner stays the
// stable holder. The NAIVE version of that exclusion is UNSAFE when 2+ of a
// unit's replicas warm AT ONCE (a mass restart / mass boot): excluding all of
// them shrinks CURRENT below R, in the limit to zero, so stableR = 0 and
// requiredWriteAcks(WriteAll, 0) = 0 - a write would ACK with ZERO durable
// applies (a lost acked write). The graceful-leave path never hits this (a
// draining node stays a stable MOUNTED current owner, so stableR = R by
// construction), but the JOIN signal fires AUTOMATICALLY on every
// boot-with-deferred-positions, mass restarts included.
//
// The QUORUM FLOOR is the guard: never let CURRENT shrink below R. When excluding
// the joining members would drop CURRENT below R, current FALLS BACK to the full
// ring for that unit - the unit reverts to the pre-Joining-bit behavior:
// unavailable-but-SAFE (the write WEDGES with the mid-acquire transient) with
// stableR = R, so a write can NEVER ack below R durable copies.
//
// This test drives the exact scenario: a 2-node R=2 cluster where BOTH nodes are
// mass-restarted with a slow mount, so for EVERY unit both replicas are warming at
// once. It asserts (a) NO write ever false-acks during the warm window (a single
// nil ack below RF durable copies is a data-safety violation the floor must
// prevent) - every write degrades to the SAFE wedge; and (b) NO acked write is
// lost - every recorded key survives the mass restart and is readable with its
// exact value once the cluster heals, and fresh writes then ack with R durable
// copies.

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestJoinMassBoot_QuorumFloorNeverFalseAcks is the data-safety gate: a 2-node
// R=2 cluster mass-restarted with a slow mount (BOTH replicas of every unit warm
// at once) must degrade every unit to the SAFE wedge and NEVER ack below R durable
// copies, and no recorded write may be lost across the restart.
func TestJoinMassBoot_QuorumFloorNeverFalseAcks(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	mutate := func(cfg *cluster.Config) { cfg.WriteTimeout = 300 * time.Millisecond }

	n1 := startReplicatedNodeCfg(t, "mb-a", "", uc, rf, backing, mutate)
	n2 := startReplicatedNodeCfg(t, "mb-b", n1.BindAddr, uc, rf, backing, mutate)
	if err := waitForMembersAll([]*cluster.Cluster{n1.Cluster, n2.Cluster}, 2, 20*time.Second); err != nil {
		t.Fatalf("2-node convergence: %v", err)
	}
	time.Sleep(800 * time.Millisecond)

	// Seed one recorded key per unit (a real value on both replicas), then plant a
	// serving marker for every position so BOTH nodes boot-defer ALL of them on
	// restart (the mass-boot precondition).
	uk := oneKeyPerUnit(t, n1.Cluster, uc)
	want := make(map[string]string, len(uk))
	for _, k := range uk {
		want[k] = "seed"
	}
	plantAllServingMarkers(t, n1.Handle, uc, rf)

	// MASS RESTART both nodes over the SAME backing with a slow mount armed BEFORE
	// Open, so both boot-defer everything and warm slowly: for the warm window EVERY
	// unit has BOTH replicas freshly-booted (owned-but-unmounted). The delay is
	// moderate because the warm-up now runs through the background bounded
	// acquire (concurrent under the open-permit pool, since v0.14.2); a very long
	// delay would only stretch the heal, not strengthen the safety claim (the
	// fully-unmounted instant already exercises the floor).
	n1.Close()
	n2.Close()
	const mountDelay = 3 * time.Second
	r1 := startReplicatedNodeSlowAcquire(t, "mb-a", "", uc, rf, backing, mountDelay, 300*time.Millisecond)
	r2 := startReplicatedNodeSlowAcquire(t, "mb-b", r1.BindAddr, uc, rf, backing, mountDelay, 300*time.Millisecond)
	if err := waitForMembersAll([]*cluster.Cluster{r1.Cluster, r2.Cluster}, 2, 30*time.Second); err != nil {
		t.Fatalf("post-mass-restart convergence: %v", err)
	}

	units := make([]storageunit.UnitID, 0, len(uk))
	for u := range uk {
		units = append(units, u)
	}
	sort.Slice(units, func(i, j int) bool { return units[i] < units[j] })

	// WARM WINDOW SAFETY PROBE: hammer every unit with a UNIQUE value while both
	// replicas are warming. Two outcomes are safe: the mid-acquire wedge (a unit
	// with < R mounted copies degrades to unavailable, the quorum floor at work), OR
	// a genuine ack from a unit whose BOTH copies have mounted (staggered mounting
	// makes some units available late in the window). The DATA-SAFETY invariant is
	// that EVERY ack is backed by R durable copies - so we record each acked
	// (key,value) and read it back after heal: a write that acked below R copies (a
	// floor failure) would be LOST and caught there. We also require MANY wedges, so
	// the run provably exercised the floor rather than being trivially available.
	var probes, wedged, acked, otherErr int
	var casProbes, casWedgedN, casAcked, casOther int
	acks := make(map[string]string, len(units)*4)
	casAcks := make(map[string]string, len(units)*2)
	for round := 0; round < 8; round++ {
		for _, u := range units {
			probes++
			val := fmt.Sprintf("mb-probe-%d-%d", u, round)
			err := r1.Cluster.Put([]byte(uk[u]), []byte(val))
			switch {
			case err == nil:
				acked++
				acks[uk[u]] = val // last acked value for this key must survive
			case isMidAcquire(err) || status.Code(err) == codes.Unavailable:
				wedged++
			default:
				otherErr++
			}
		}
		// CAS PROBES (workstream B): the CAS surface must obey the SAME floor
		// contract - under an all-warming unit either the SAFE wedge (retryable
		// refusal) or a genuine ack backed by R durable copies (verified by the
		// durability sweep below). A false CAS ack below R copies would be lost
		// there. Probe a few units per round with fresh If-None-Match commits.
		for i := 0; i < 4 && i < len(units); i++ {
			u := units[(round*4+i)%len(units)]
			casProbes++
			k := casKeysForUnit(u, uc, 1, fmt.Sprintf("mb-%d-%d", u, round))[0]
			err := casProbe(r1.Cluster, k, "mbcas")
			switch {
			case err == nil:
				casAcked++
				casAcks[k] = "mbcas"
			case casWedged(err) || status.Code(err) == codes.DeadlineExceeded:
				casWedgedN++
			default:
				casOther++
				t.Logf("mass-boot CAS probe unexpected error on unit %d: %v", u, err)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Logf("MASS-BOOT warm window: probes=%d wedged(safe)=%d acked=%d other=%d; CAS probes=%d wedged(safe)=%d acked=%d other=%d",
		probes, wedged, acked, otherErr, casProbes, casWedgedN, casAcked, casOther)

	// SAFETY ASSERTION (a): the mass boot degraded to the SAFE wedge for the
	// fully-warming units (many probes wedged, unavailable-but-not-lost), and no
	// probe returned a non-wedge hard error.
	if wedged == 0 {
		t.Fatalf("expected the mass boot to WEDGE (the safe unavailable-but-not-lost degradation) while both replicas "+
			"warm, but no probe wedged (probes=%d acked=%d) - the scenario did not exercise the quorum floor", probes, acked)
	}
	if otherErr > 0 {
		t.Fatalf("unexpected non-wedge hard errors during the mass boot: %d (want only mid-acquire/Unavailable wedges)", otherErr)
	}
	if casOther > 0 {
		t.Fatalf("unexpected non-wedge hard errors from CAS probes during the mass boot: %d (the CAS surface must "+
			"degrade to the same safe wedge)", casOther)
	}

	// HEAL: drop the slow mount so both nodes finish warming, and let the reconcile
	// mount every deferred position before the durability sweep (the warm-up is
	// asynchronous/bounded since v0.14.2; the poll below waits for it, so the
	// settle is a head start rather than a completion guarantee).
	for _, n := range []*sharedNode{r1, r2} {
		n.Handle.SetAcquireDelay(0)
	}
	// Wait for both nodes to fully warm (every owned position mounted) so the
	// durability read is not racing an in-progress mount.
	healed := false
	for deadline := time.Now().Add(40 * time.Second); time.Now().Before(deadline); {
		if desiredButUnmounted(r1.Cluster) == 0 && desiredButUnmounted(r2.Cluster) == 0 {
			healed = true
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !healed {
		t.Fatalf("cluster did not finish warming within 40s of dropping the mount delay (r1 unmounted=%d r2 unmounted=%d)",
			desiredButUnmounted(r1.Cluster), desiredButUnmounted(r2.Cluster))
	}

	// SAFETY ASSERTION (b): NO acked write is lost. Every value that ACKED during
	// the warm window must be readable with its exact value once the cluster heals.
	// A write that acked below R durable copies (the floor failing) would be LOST.
	lost, mismatched := 0, 0
	var lostKeys []string
	for k, v := range acks {
		got, err := getWithRetryUnavailable(t, r1.Cluster, k, 15*time.Second)
		if err != nil {
			lost++
			lostKeys = append(lostKeys, fmt.Sprintf("%s(err:%v)", k, err))
			continue
		}
		if string(got) != v {
			mismatched++
			lostKeys = append(lostKeys, fmt.Sprintf("%s(got %q want %q)", k, got, v))
		}
	}
	// CAS acks obey the same contract: an acked If-None-Match commit below R
	// durable copies would be lost here.
	for k, v := range casAcks {
		got, err := getWithRetryUnavailable(t, r1.Cluster, k, 15*time.Second)
		if err != nil || string(got) != v {
			lost++
			lostKeys = append(lostKeys, fmt.Sprintf("cas:%s(got %q want %q err %v)", k, got, v, err))
		}
	}
	// Seed keys not overwritten by a later acked probe must also survive.
	for k, v := range want {
		if acks[k] != "" {
			continue // overwritten by an acked probe (checked above)
		}
		got, err := getWithRetryUnavailable(t, r1.Cluster, k, 15*time.Second)
		if err != nil || string(got) != v {
			lost++
			lostKeys = append(lostKeys, fmt.Sprintf("seed:%s(got %q want %q err %v)", k, got, v, err))
		}
	}
	if lost > 0 || mismatched > 0 {
		t.Fatalf("DATA-SAFETY VIOLATION: %d acked writes unreadable and %d mismatched after the mass restart + heal - "+
			"an acked write was lost (the quorum floor must guarantee every ack is backed by R durable copies).\nlost: %v",
			lost, mismatched, lostKeys)
	}
	t.Logf("DURABILITY held: all %d seed keys + all %d warm-window acked values survived with exact values", len(want), len(acks))

	// POSITIVE ack durability: after heal, a fresh write must ACK and be read back
	// (proving the ack is backed by R durable copies now that the nodes are mounted).
	freshOK := 0
	for _, u := range units {
		k := "mb-fresh-" + fmt.Sprintf("%d", u)
		if err := putWithRetryUnavailable(t, r1.Cluster, k, "fresh", 10*time.Second); err != nil {
			t.Fatalf("post-heal write %q did not ack: %v", k, err)
		}
		got, err := getWithRetryUnavailable(t, r2.Cluster, k, 10*time.Second)
		if err != nil || string(got) != "fresh" {
			t.Fatalf("post-heal write %q not durably readable from the peer (got %q, err %v) - ack not backed by a durable copy", k, got, err)
		}
		freshOK++
	}
	t.Logf("CONFIRMED (quorum floor SAFE): mass-booting both R=2 replicas at once degraded every unit to the SAFE wedge "+
		"(%d/%d probes wedged, 0 false-acks), NO recorded write was lost (%d/%d keys survived), and post-heal writes ack "+
		"with a durable copy (%d/%d). The floor held stableR >= R so no write ever acked below R durable copies.",
		wedged, probes, len(want), len(want), freshOK, len(units))
}
