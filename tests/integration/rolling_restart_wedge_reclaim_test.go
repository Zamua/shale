package integration

// FULL-CLUSTER verification of same-identity supersession: a node expired as
// dead at its old address, then a same-stable-id successor appearing at a NEW
// address (with connectivity), must be re-integrated so the ring re-converges
// and writes recover.

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
)

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *logBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *logBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestReclaimGate_FullClusterRejoinRecovers pins that a same-stable-id/
// new-address rejoin after the old instance was expired as dead re-integrates:
// the ring re-converges and writes recover.
func TestReclaimGate_FullClusterRejoinRecovers(t *testing.T) {
	const uc, rf = 16, 2
	backing := sharedfactory.NewBacking()
	b1, b2 := &logBuf{}, &logBuf{}
	mk := func(id, seed string, buf *logBuf) *sharedNode {
		return startReplicatedNodeCfg(t, id, seed, uc, rf, backing, func(cfg *cluster.Config) {
			if buf != nil {
				cfg.LogOutput = buf // captured for failure diagnostics
			}
		})
	}
	n1 := mk("n1", "", b1)
	n2 := mk("n2", n1.ClusterToken, b2)
	n3 := mk("n3", n1.ClusterToken, nil)
	cs := []*cluster.Cluster{n1.Cluster, n2.Cluster, n3.Cluster}
	if err := waitForMembersAll(cs, 3, 20*time.Second); err != nil {
		t.Fatalf("baseline convergence: %v", err)
	}

	// Seed data + a baseline write so we can prove write recovery afterward.
	want, _ := writeRecordedDataset(t, n1.Cluster)

	// HARD-KILL n3 (SIGKILL, no Leave), then WAIT until n1 AND n2 reap it as DEAD
	// (drop to a 2-member view) - the precondition for the reclaim gate.
	_ = n3.Cluster.TestingHardKill()
	n3.stop()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if len(n1.Cluster.Members()) == 2 && len(n2.Cluster.Members()) == 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(n1.Cluster.Members()) != 2 || len(n2.Cluster.Members()) != 2 {
		t.Fatalf("n3 not reaped dead: n1 sees %d, n2 sees %d (want 2,2)", len(n1.Cluster.Members()), len(n2.Cluster.Members()))
	}
	t.Logf("n3 reaped DEAD: n1 + n2 both see 2 members")

	// REJOIN n3 with the SAME stable id, a NEW address, seeded to the live n1.
	n3b := mk("n3", n1.ClusterToken, nil)
	cs = []*cluster.Cluster{n1.Cluster, n2.Cluster, n3b.Cluster}

	// The ring must re-converge to 3 on EVERY node (the successor is superseded in,
	// not rejected by the reclaim gate).
	if err := waitForMembersAll(cs, 3, 25*time.Second); err != nil {
		t.Fatalf("RECLAIM-GATE WEDGE: ring did NOT re-converge after same-id/new-address rejoin: %v\n"+
			"n1 log tail:\n%s", err, tail(b1.String(), 1500))
	}
	t.Logf("ring re-converged to 3 after same-id/new-address rejoin")

	// Writes must recover on every unit, and the seeded dataset must survive.
	keys := unitKeys(uc)
	writeDeadline := time.Now().Add(30 * time.Second)
	for {
		stuck := -1
		i := 0
		for u, k := range keys {
			entry := cs[i%len(cs)]
			i++
			if err := putBounded(entry, k, "recover", 300*time.Millisecond); err != nil {
				stuck = int(u)
				break
			}
		}
		if stuck == -1 {
			break
		}
		if time.Now().After(writeDeadline) {
			t.Fatalf("writes did NOT recover after rejoin: unit %d still unwritable", stuck)
		}
		time.Sleep(300 * time.Millisecond)
	}
	lost := 0
	for k, v := range want {
		got, err := getWithRetryUnavailable(t, n1.Cluster, k, 10*time.Second)
		if err != nil || string(got) != string(v) {
			lost++
		}
	}
	if lost > 0 {
		t.Fatalf("durability: %d/%d seeded keys lost after rejoin", lost, len(want))
	}
	t.Logf("RECOVERED: ring re-converged, all %d units writable, %d seeded keys durable after the "+
		"same-id/new-address rejoin", uc, len(want))
}

// tail returns the last n bytes of s (a log tail for failure diagnostics).
func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}
