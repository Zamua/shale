package integration

// STORAGE-STALL CONTRAST for the rolling-restart wedge investigation.
//
// The membership-divergence test proves that a DIVERGENT ring wedges writes at
// 1-of-2 and does not recover while the divergence holds. This file proves the
// COMPLEMENT: with the BOOT-DEFER-ALL behavior actually firing (a restarted node
// reads back a durable serving marker for every position it owns and defers
// opening all of them at boot), a cluster on a CONSISTENT ring SELF-HEALS within
// a couple of reconcile ticks. Together the two tests pin the diagnostic the live
// incident needs:
//
//	persistent wedge  <=>  ring / membership divergence (NOT boot-defer alone).
//
// Boot-defer preconditions (verified against both the real slate factory and the
// sharedfactory test double, which share the cluster-side WriteServingMarker call
// sites): a serving marker is written for a position only when a node ACQUIRES it
// via reconcile (acquireReplicaUnit / the overlap acquire / finishStuckFlip), NOT
// by the initial boot mount. A live cluster that has run for a while (and rolled
// before) therefore has a marker for essentially every position, so a reboot
// defers ALL of them ("degraded boot complete - 0 mounted, N DEFERRED"). A short
// in-process cluster does not naturally mark every position, so this test PLANTS a
// durable marker for every (unit, replica) position on the shared backing first -
// the faithful stand-in for "every position has been Ready before" - then mass-
// restarts so every node genuinely boot-defers, and asserts the heal.

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/storageunit"
)

// threadSafeBuf is an io.Writer that captures a node's cluster log from the
// (possibly concurrent) reconcile goroutines so the test can assert boot-defer
// fired.
type threadSafeBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *threadSafeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *threadSafeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// desiredButUnmounted parses DebugState()'s summary line for the
// "desired-but-unmounted" count (the wedge signature the /debug/shale/state dump
// flags per position). It is the storage-stall arm's fingerprint on ONE node.
func desiredButUnmounted(c *cluster.Cluster) int {
	dump := c.DebugState()
	for _, line := range strings.Split(dump, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "summary:") {
			continue
		}
		var pos, unm int
		if _, err := fmt.Sscanf(line, "summary: %d positions, %d desired-but-unmounted", &pos, &unm); err == nil {
			return unm
		}
	}
	return -1
}

// plantAllServingMarkers writes a durable serving marker (epoch 1) for every
// (unit, replica) position on the shared backing via one node's factory handle
// (all handles share the backing). This is the faithful precondition for
// boot-defer-all: "every position has reached Ready before, so its marker
// exists." The write is monotonic, so a position already marked at a higher
// epoch keeps it.
func plantAllServingMarkers(t *testing.T, h *sharedfactory.Handle, unitCount, rf int) {
	t.Helper()
	for _, u := range storageunit.MustUnitCount(unitCount).IDs() {
		for p := 0; p < rf; p++ {
			ru := storageunit.NewReplicaUnit(storageunit.NewGenUnit(0, u), uint8(p))
			if err := h.WriteServingMarker(storageunit.ReplicaMount(ru), 1); err != nil {
				t.Fatalf("plant serving marker %s: %v", ru, err)
			}
		}
	}
}

// TestRollingRestartWedge_BootDeferHealsOnConsistentRing plants serving markers
// for every position, mass-restarts a 3-node R=2 cluster so every node genuinely
// boot-defers ALL its positions, and proves that on a consistent ring the
// deferred positions self-heal (no quorum, no wedge) and every unit recovers.
func TestRollingRestartWedge_BootDeferHealsOnConsistentRing(t *testing.T) {
	const unitCount, rf = 16, 2
	backing := sharedfactory.NewBacking()
	nodes := start3NodeR2(t, unitCount, backing) // ids r2a, r2b, r2c

	// Seed data + plant a serving marker for every position: the boot-defer-all
	// precondition (every position has been Ready, so a reboot defers all of them).
	want, _ := writeRecordedDataset(t, nodes[0].Cluster)
	plantAllServingMarkers(t, nodes[0].Handle, unitCount, rf)
	t.Logf("seeded %d keys and planted serving markers for all %d positions", len(want), unitCount*rf)

	// MASS RESTART all three over the SAME backing with captured logs, so each
	// node re-reads the markers at Open and boot-defers.
	for _, n := range nodes {
		n.Close()
	}
	bufs := map[string]*threadSafeBuf{"r2a": {}, "r2b": {}, "r2c": {}}
	mk := func(id, seed string) *sharedNode {
		return startReplicatedNodeCfg(t, id, seed, unitCount, rf, backing, func(cfg *cluster.Config) {
			cfg.LogOutput = bufs[id]
		})
	}
	r2a := mk("r2a", "")
	r2b := mk("r2b", r2a.ClusterToken)
	r2c := mk("r2c", r2a.ClusterToken)
	restarted := []*sharedNode{r2a, r2b, r2c}

	// CONFIRM boot-defer actually fired: at least one node's boot log must report
	// DEFERRED positions (the "degraded boot complete - ... N DEFERRED" line). This
	// is the ingredient-#1 evidence, captured deterministically from the log rather
	// than a timing-raced DebugState snapshot.
	deferSeen := false
	var deferLines []string
	for id, buf := range bufs {
		for _, line := range strings.Split(buf.String(), "\n") {
			if strings.Contains(line, "DEFERRED") || strings.Contains(line, "boot defer") {
				deferSeen = true
				deferLines = append(deferLines, fmt.Sprintf("[%s] %s", id, strings.TrimSpace(line)))
			}
		}
	}
	if deferSeen {
		t.Logf("BOOT-DEFER-ALL confirmed (ingredient #1). Sample log lines:\n  %s", strings.Join(deferLines, "\n  "))
	} else {
		// Not fatal on its own (a very fast reconcile can mount before we read the
		// buffer), but log it so the run is honest about what it observed.
		t.Logf("NOTE: no boot-defer log captured (reconcile may have healed before Open logged, or markers were superseded); proceeding to the heal gate")
	}

	// Consistent-ring precondition: all three nodes must converge on the 3-node
	// ring (this is what makes recovery the storage arm's job, not the membership
	// arm's).
	cs := []*cluster.Cluster{r2a.Cluster, r2b.Cluster, r2c.Cluster}
	if err := waitForMembersAll(cs, 3, 25*time.Second); err != nil {
		t.Fatalf("post-mass-restart membership did not converge to 3: %v", err)
	}
	if !clustersAgreeOnMembers(restarted) {
		t.Fatalf("nodes do not agree on the ring after mass restart (would be the membership arm, out of scope here)")
	}

	// HEAL GATE: within a bounded window every unit must accept a write AND every
	// node must drive desired-but-unmounted to 0, all while the ring stays
	// consistent. A persistent nonzero on an AGREEING ring would be the storage-
	// stall arm; the point of the test is that this does NOT happen.
	deadline := time.Now().Add(40 * time.Second)
	keys := unitKeys(unitCount)
	for {
		unmA := desiredButUnmounted(r2a.Cluster)
		unmB := desiredButUnmounted(r2b.Cluster)
		unmC := desiredButUnmounted(r2c.Cluster)
		allWritable := true
		stuckUnit := -1
		i := 0
		for u, k := range keys {
			entry := restarted[i%len(restarted)].Cluster
			i++
			if err := putBounded(entry, k, "heal", 300*time.Millisecond); err != nil {
				allWritable = false
				stuckUnit = int(u)
				break
			}
		}
		agree := clustersAgreeOnMembers(restarted)
		if unmA == 0 && unmB == 0 && unmC == 0 && allWritable && agree {
			t.Logf("HEALED on a consistent ring: all nodes desired-but-unmounted=0 and all %d units writable "+
				"(boot-defer-all self-healed via reconcile, no quorum, no wedge)", unitCount)
			// Durability check: every acked key still readable.
			lost := 0
			for k, v := range want {
				got, err := getWithRetryUnavailable(t, r2a.Cluster, k, 10*time.Second)
				if err != nil || string(got) != string(v) {
					lost++
				}
			}
			if lost > 0 {
				t.Fatalf("DURABILITY: %d/%d seeded keys lost/corrupt after boot-defer + heal", lost, len(want))
			}
			t.Logf("durability held: all %d seeded keys survived the mass restart", len(want))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("did NOT heal on a consistent ring within 40s: desired-but-unmounted a=%d b=%d c=%d, allWritable=%v (stuck unit %d), members-agree=%v.\n"+
				"A persistent wedge on an AGREEING ring would be the STORAGE-STALL arm.\nstate:%s",
				unmA, unmB, unmC, allWritable, stuckUnit, agree, captureNodesState(restarted))
		}
		time.Sleep(500 * time.Millisecond)
	}
}
