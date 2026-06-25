package membership

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// safeBuf is a concurrency-safe io.Writer for capturing memberlist's internal
// log from multiple goroutines.
type safeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func mkNode(t *testing.T, id, bind, grpc string, seeds []string, declared uint32, logw io.Writer) *Membership {
	t.Helper()
	m, err := Open(Config{
		NodeID:              id,
		BindAddr:            bind,
		GRPCAddr:            grpc,
		Seeds:               seeds,
		DeclaredUnitCount:   declared,
		MetaRefreshInterval: 200 * time.Millisecond, // fast re-broadcast (prod is 10s)
		RejoinInterval:      300 * time.Millisecond,
		LogOutput:           logw,
	})
	if err != nil {
		t.Fatalf("open %s: %v", id, err)
	}
	return m
}

// TestRestartedNodeChangedMetaPropagates_Graceful reproduces the rolling-restart
// scenario where a node restarts with the SAME node id, a NEW bind address, a
// RESET memberlist incarnation, and a CHANGED declared unit count. Peers that
// remember the old instance MUST update their view to the new declared count -
// this is the property the declarative-reshard unanimity gate depends on.
//
// Graceful variant: the old instance does a clean Leave (StateLeft) before the
// new instance joins.
func TestRestartedNodeChangedMetaPropagates_Graceful(t *testing.T) {
	n1Buf, n2Buf := &safeBuf{}, &safeBuf{}
	n1Port, n2Port, n3Port := ephemeralPort(t), ephemeralPort(t), ephemeralPort(t)
	const n1G, n2G, n3G = "127.0.0.1:31001", "127.0.0.1:31002", "127.0.0.1:31003"

	m1 := mkNode(t, "n1", bindAddr(n1Port), n1G, nil, 8, n1Buf)
	defer func() { _ = m1.Close() }()
	m2 := mkNode(t, "n2", bindAddr(n2Port), n2G, []string{bindAddr(n1Port)}, 8, n2Buf)
	defer func() { _ = m2.Close() }()
	m3 := mkNode(t, "n3", bindAddr(n3Port), n3G, []string{bindAddr(n1Port)}, 8, io.Discard)

	want := map[string]string{"n1": n1G, "n2": n2G, "n3": n3G}
	for _, m := range []*Membership{m1, m2} {
		if err := waitForMembers(m, want, 5*time.Second); err != nil {
			t.Fatalf("converge: %v", err)
		}
	}
	if err := waitForDeclaredCount(m1, "n3", 8, 3*time.Second); err != nil {
		t.Fatalf("baseline n1: %v", err)
	}
	if err := waitForDeclaredCount(m2, "n3", 8, 3*time.Second); err != nil {
		t.Fatalf("baseline n2: %v", err)
	}
	t.Logf("baseline OK: both peers see n3 declared=8")

	// RESTART n3: graceful Leave, then a fresh instance with a NEW bind port,
	// a NEW grpc addr, and declared=16.
	if err := m3.Close(); err != nil {
		t.Logf("n3 graceful close: %v", err)
	}
	n3PortNew := ephemeralPort(t)
	const n3Gnew = "127.0.0.1:31013"
	m3b := mkNode(t, "n3", bindAddr(n3PortNew), n3Gnew, []string{bindAddr(n1Port)}, 16, io.Discard)
	defer func() { _ = m3b.Close() }()
	t.Logf("restarted n3: new bind=%s new grpc=%s declared=16", bindAddr(n3PortNew), n3Gnew)

	err1 := waitForDeclaredCount(m1, "n3", 16, 40*time.Second)
	err2 := waitForDeclaredCount(m2, "n3", 16, 40*time.Second)
	m1n3, _ := findMember(m1, "n3")
	m2n3, _ := findMember(m2, "n3")
	t.Logf("FINAL: n1 sees n3 declared=%d addr=%s (err=%v)", m1n3.DeclaredUnitCount, m1n3.Addr, err1)
	t.Logf("FINAL: n2 sees n3 declared=%d addr=%s (err=%v)", m2n3.DeclaredUnitCount, m2n3.Addr, err2)
	if err1 != nil || err2 != nil {
		t.Logf("=== n1 memberlist log ===\n%s", n1Buf.String())
		t.Logf("=== n2 memberlist log ===\n%s", n2Buf.String())
		t.Fatalf("REPRODUCED (graceful): peers did not converge to n3 declared=16")
	}
	t.Logf("PASS (graceful): peers converged to n3 declared=16")
}

// TestRestartedNodeChangedMetaPropagates_HardKill is the same scenario but the
// old n3 instance is killed WITHOUT a graceful Leave (its memberlist transport
// is shut down abruptly). Peers detect it dead via probe failure (StateDead),
// which exercises the DeadNodeReclaimTime reclaim path for the new bind address.
// This is the faithful model of a k8s pod that is SIGKILLed (or whose Leave does
// not propagate before the new pod joins).
func TestRestartedNodeChangedMetaPropagates_HardKill(t *testing.T) {
	n1Buf, n2Buf := &safeBuf{}, &safeBuf{}
	n1Port, n2Port, n3Port := ephemeralPort(t), ephemeralPort(t), ephemeralPort(t)
	const n1G, n2G, n3G = "127.0.0.1:32001", "127.0.0.1:32002", "127.0.0.1:32003"

	m1 := mkNode(t, "n1", bindAddr(n1Port), n1G, nil, 8, n1Buf)
	defer func() { _ = m1.Close() }()
	m2 := mkNode(t, "n2", bindAddr(n2Port), n2G, []string{bindAddr(n1Port)}, 8, n2Buf)
	defer func() { _ = m2.Close() }()
	m3 := mkNode(t, "n3", bindAddr(n3Port), n3G, []string{bindAddr(n1Port)}, 8, io.Discard)

	want := map[string]string{"n1": n1G, "n2": n2G, "n3": n3G}
	for _, m := range []*Membership{m1, m2} {
		if err := waitForMembers(m, want, 5*time.Second); err != nil {
			t.Fatalf("converge: %v", err)
		}
	}
	if err := waitForDeclaredCount(m1, "n3", 8, 3*time.Second); err != nil {
		t.Fatalf("baseline n1: %v", err)
	}
	t.Logf("baseline OK: both peers see n3 declared=8")

	// HARD KILL n3: abruptly shut down its memberlist transport with NO Leave
	// (models a SIGKILLed pod). We deliberately do NOT call m3.Close() afterwards
	// because memberlist.Leave PANICS after Shutdown; m3's shale loops are left to
	// no-op against the dead transport (harmless goroutine leak for the repro).
	_ = m3.ml.Shutdown()
	t.Logf("hard-killed n3 (no graceful Leave)")

	n3PortNew := ephemeralPort(t)
	const n3Gnew = "127.0.0.1:32013"
	m3b := mkNode(t, "n3", bindAddr(n3PortNew), n3Gnew, []string{bindAddr(n1Port)}, 16, io.Discard)
	defer func() { _ = m3b.Close() }()
	t.Logf("restarted n3: new bind=%s new grpc=%s declared=16", bindAddr(n3PortNew), n3Gnew)

	// Allow 50s: 30s DeadNodeReclaimTime + probe-detection + margin.
	err1 := waitForDeclaredCount(m1, "n3", 16, 50*time.Second)
	err2 := waitForDeclaredCount(m2, "n3", 16, 50*time.Second)
	m1n3, _ := findMember(m1, "n3")
	m2n3, _ := findMember(m2, "n3")
	t.Logf("FINAL: n1 sees n3 declared=%d addr=%s (err=%v)", m1n3.DeclaredUnitCount, m1n3.Addr, err1)
	t.Logf("FINAL: n2 sees n3 declared=%d addr=%s (err=%v)", m2n3.DeclaredUnitCount, m2n3.Addr, err2)
	if err1 != nil || err2 != nil {
		t.Logf("=== n1 memberlist log ===\n%s", n1Buf.String())
		t.Logf("=== n2 memberlist log ===\n%s", n2Buf.String())
		t.Fatalf("REPRODUCED (hard-kill): peers did not converge to n3 declared=16")
	}
	t.Logf("PASS (hard-kill): peers converged to n3 declared=16")
}

// TestStaggeredRollChangedMetaPropagates models a StatefulSet rolling restart:
// all three nodes restart ONE AT A TIME (reverse ordinal), each with a graceful
// Leave, a NEW bind address, and a CHANGED declared count (8 -> 16). This is the
// case the single-node graceful test cannot show: when peers are THEMSELVES
// cycling, a departing node's Leave can be missed, leaving its old entry
// un-reclaimed so the next-rolled node's new address conflicts. Faithful to the
// live staging failure (the #473 reshard wedge). Runs with the current #473 fix
// active (MetaRefreshInterval + DeadNodeReclaimTime via mkNode).
func TestStaggeredRollChangedMetaPropagates(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	ports := map[string]int{}
	grpcs := map[string]string{"n1": "127.0.0.1:33001", "n2": "127.0.0.1:33002", "n3": "127.0.0.1:33003"}
	bufs := map[string]*safeBuf{}
	live := map[string]*Membership{}
	for _, id := range ids {
		ports[id] = ephemeralPort(t)
		bufs[id] = &safeBuf{}
	}
	seedsExcept := func(except string) []string {
		var s []string
		for _, id := range ids {
			if id != except {
				s = append(s, bindAddr(ports[id]))
			}
		}
		return s
	}

	live["n1"] = mkNode(t, "n1", bindAddr(ports["n1"]), grpcs["n1"], nil, 8, bufs["n1"])
	live["n2"] = mkNode(t, "n2", bindAddr(ports["n2"]), grpcs["n2"], []string{bindAddr(ports["n1"])}, 8, bufs["n2"])
	live["n3"] = mkNode(t, "n3", bindAddr(ports["n3"]), grpcs["n3"], []string{bindAddr(ports["n1"])}, 8, bufs["n3"])
	defer func() {
		for _, m := range live {
			_ = m.Close()
		}
	}()

	want := map[string]string{"n1": grpcs["n1"], "n2": grpcs["n2"], "n3": grpcs["n3"]}
	for _, id := range ids {
		if err := waitForMembers(live[id], want, 5*time.Second); err != nil {
			t.Fatalf("initial converge %s: %v", id, err)
		}
	}
	t.Logf("converged at declared=8")

	// Roll reverse-ordinal, one at a time, graceful Leave, declared 16.
	for _, id := range []string{"n3", "n2", "n1"} {
		_ = live[id].Close() // graceful Leave
		newPort := ephemeralPort(t)
		ports[id] = newPort
		live[id] = mkNode(t, id, bindAddr(newPort), grpcs[id], seedsExcept(id), 16, bufs[id])
		time.Sleep(2 * time.Second) // model the per-pod readiness gate between rolls
		t.Logf("rolled %s -> new bind=%s declared=16", id, bindAddr(newPort))
	}

	// Require: every node sees every member declared=16 (the unanimity the
	// declarative-reshard gate needs). Poll up to 40s.
	deadline := time.Now().Add(40 * time.Second)
	var bad string
	for time.Now().Before(deadline) {
		bad = ""
		for _, viewer := range ids {
			for _, peer := range ids {
				mem, ok := findMember(live[viewer], peer)
				if !ok || mem.DeclaredUnitCount != 16 {
					bad = fmt.Sprintf("%s sees %s declared=%d ok=%v", viewer, peer, mem.DeclaredUnitCount, ok)
				}
			}
		}
		if bad == "" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	for _, viewer := range ids {
		var sb strings.Builder
		for _, peer := range ids {
			mem, _ := findMember(live[viewer], peer)
			fmt.Fprintf(&sb, "%s=%d ", peer, mem.DeclaredUnitCount)
		}
		t.Logf("view[%s]: %s", viewer, strings.TrimSpace(sb.String()))
	}
	if bad != "" {
		t.Errorf("REPRODUCED (staggered roll): never unanimous on declared=16; last bad: %s", bad)
	} else {
		t.Logf("PASS (staggered roll): all nodes converged to declared=16")
	}
}

// TestStaggeredRollHardKill is the most faithful model of the live staging
// failure: all three nodes restart ONE AT A TIME (reverse ordinal), each via a
// HARD KILL (m.ml.Shutdown() with NO graceful Leave, modeling a SIGKILLed pod),
// each rejoining with a NEW bind address and a CHANGED declared count (8 -> 16).
// Without the #473 identity-decouple, a hard-killed node's old memberlist entry
// (same stable name, old address) never reclaims in time, so the next-rolled
// node's new address conflicts and the changed Meta is rejected, wedging the
// declarative-reshard unanimity gate. With the fix (per-process unique memberlist
// name, stable id in Meta), every restart is a clean new-node join, so all nodes
// MUST converge to declared=16. We deliberately do NOT call Close() on the killed
// instance: memberlist.Leave panics after Shutdown, and its shale loops harmlessly
// no-op against the dead transport (a goroutine leak acceptable for the repro).
func TestStaggeredRollHardKill(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	ports := map[string]int{}
	grpcs := map[string]string{"n1": "127.0.0.1:34001", "n2": "127.0.0.1:34002", "n3": "127.0.0.1:34003"}
	bufs := map[string]*safeBuf{}
	live := map[string]*Membership{}
	for _, id := range ids {
		ports[id] = ephemeralPort(t)
		bufs[id] = &safeBuf{}
	}
	seedsExcept := func(except string) []string {
		var s []string
		for _, id := range ids {
			if id != except {
				s = append(s, bindAddr(ports[id]))
			}
		}
		return s
	}

	live["n1"] = mkNode(t, "n1", bindAddr(ports["n1"]), grpcs["n1"], nil, 8, bufs["n1"])
	live["n2"] = mkNode(t, "n2", bindAddr(ports["n2"]), grpcs["n2"], []string{bindAddr(ports["n1"])}, 8, bufs["n2"])
	live["n3"] = mkNode(t, "n3", bindAddr(ports["n3"]), grpcs["n3"], []string{bindAddr(ports["n1"])}, 8, bufs["n3"])
	// Track which instances are still gracefully closable: a hard-killed old
	// instance must NOT be Close()d (Leave panics after Shutdown). The map always
	// holds the CURRENTLY-LIVE instance per id, which is closable; the cleanup
	// closes those.
	defer func() {
		for _, m := range live {
			_ = m.Close()
		}
	}()

	want := map[string]string{"n1": grpcs["n1"], "n2": grpcs["n2"], "n3": grpcs["n3"]}
	for _, id := range ids {
		if err := waitForMembers(live[id], want, 5*time.Second); err != nil {
			t.Fatalf("initial converge %s: %v", id, err)
		}
	}
	t.Logf("converged at declared=8")

	// Roll reverse-ordinal, one at a time, HARD KILL (no Leave), declared 16.
	for _, id := range []string{"n3", "n2", "n1"} {
		_ = live[id].ml.Shutdown() // hard kill: NO graceful Leave (models SIGKILL)
		t.Logf("hard-killed %s (no graceful Leave)", id)
		newPort := ephemeralPort(t)
		ports[id] = newPort
		live[id] = mkNode(t, id, bindAddr(newPort), grpcs[id], seedsExcept(id), 16, bufs[id])
		time.Sleep(2 * time.Second) // model the per-pod readiness gate between rolls
		t.Logf("rolled %s -> new bind=%s declared=16", id, bindAddr(newPort))
	}

	// Require: every node sees every member declared=16 (the unanimity the
	// declarative-reshard gate needs). Poll up to 45s (generous; the fix should
	// converge fast since there is no same-name address conflict to wait out).
	deadline := time.Now().Add(45 * time.Second)
	var bad string
	for time.Now().Before(deadline) {
		bad = ""
		for _, viewer := range ids {
			for _, peer := range ids {
				mem, ok := findMember(live[viewer], peer)
				if !ok || mem.DeclaredUnitCount != 16 {
					bad = fmt.Sprintf("%s sees %s declared=%d ok=%v", viewer, peer, mem.DeclaredUnitCount, ok)
				}
			}
		}
		if bad == "" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	for _, viewer := range ids {
		var sb strings.Builder
		for _, peer := range ids {
			mem, _ := findMember(live[viewer], peer)
			fmt.Fprintf(&sb, "%s=%d ", peer, mem.DeclaredUnitCount)
		}
		t.Logf("view[%s]: %s", viewer, strings.TrimSpace(sb.String()))
	}
	if bad != "" {
		for _, viewer := range ids {
			t.Logf("=== %s memberlist log ===\n%s", viewer, bufs[viewer].String())
		}
		t.Errorf("REPRODUCED (staggered roll, hard kill): never unanimous on declared=16; last bad: %s", bad)
	} else {
		t.Logf("PASS (staggered roll, hard kill): all nodes converged to declared=16")
	}
}
