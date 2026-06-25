package cluster

// White-box tests for the declarative reshard trigger: the unanimity helper
// (pure) and observeDeclaredReshardTarget's gating + retarget wiring (against a
// real single-node membership + arbiter). The full multi-node online
// convergence is covered by the lossless split/merge gate (tests/integration);
// here we pin that a unanimous declared count moves the arbiter target, and
// that a non-steady arbiter or a disagreeing/unknown member does NOT.

import (
	"io"
	"net"
	"strconv"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/membership"
	"github.com/Zamua/shale/pkg/storageunit"
)

func TestUnanimousDeclaredCount(t *testing.T) {
	mk := func(counts ...uint32) []membership.Member {
		ms := make([]membership.Member, len(counts))
		for i, c := range counts {
			ms[i] = membership.Member{ID: "n" + strconv.Itoa(i), DeclaredUnitCount: c}
		}
		return ms
	}
	cases := []struct {
		name    string
		members []membership.Member
		wantD   uint32
		wantOK  bool
	}{
		{"single", mk(8), 8, true},
		{"unanimous", mk(16, 16, 16), 16, true},
		{"disagreement (mid-roll)", mk(8, 8, 16), 0, false},
		{"one unknown vetoes", mk(16, 16, 0), 0, false},
		{"all unknown", mk(0, 0, 0), 0, false},
		{"empty set", mk(), 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, ok := unanimousDeclaredCount(tc.members)
			if d != tc.wantD || ok != tc.wantOK {
				t.Fatalf("unanimousDeclaredCount = (%d, %v), want (%d, %v)", d, ok, tc.wantD, tc.wantOK)
			}
		})
	}
}

// openDeclaringMembership opens a single-node Membership advertising the given
// declared unit count, registering a Cleanup to close it.
func openDeclaringMembership(t *testing.T, nodeID string, declared uint32) *membership.Membership {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	mem, err := membership.Open(membership.Config{
		NodeID:            nodeID,
		BindAddr:          "127.0.0.1:" + strconv.Itoa(port),
		GRPCAddr:          "127.0.0.1:1",
		DeclaredUnitCount: declared,
		LogOutput:         io.Discard,
	})
	if err != nil {
		t.Fatalf("membership.Open: %v", err)
	}
	t.Cleanup(func() { _ = mem.Close() })
	return mem
}

// newDeclaredReshardCluster builds an R=2 multi-backend cluster founded at
// `founded` units with a wired arbiter (seeded count==target==founded) and a
// real single-node membership advertising `declared` units.
func newDeclaredReshardCluster(t *testing.T, founded int, declared uint32) *Cluster {
	t.Helper()
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", founded, 2, backing, "n1", "n2", "n3")
	c.cfg.ConditionalStore = storageunit.NewMemConditionalStore()
	c.cfg.DeclarativeReshard = true
	if err := c.initReshardArbiter(); err != nil {
		t.Fatal(err)
	}
	c.membership = openDeclaringMembership(t, "n1", declared)
	return c
}

// TestObserveDeclaredReshardTarget_RetargetsOnUnanimity: a steady arbiter
// (count==target==4) and a membership unanimously declaring 8 retargets the
// arbiter to 8 (the operator re-declared the count; the cluster now converges).
func TestObserveDeclaredReshardTarget_RetargetsOnUnanimity(t *testing.T) {
	c := newDeclaredReshardCluster(t, 4, 8)

	c.observeDeclaredReshardTarget()

	s, _, err := c.arbiter.Read()
	if err != nil {
		t.Fatal(err)
	}
	if s.Target.N() != 8 {
		t.Fatalf("arbiter target = %d, want 8 (retargeted to the declared count)", s.Target.N())
	}
	// Count is still 4: the retarget only declares the goal; observeReshard
	// (not called here) is what advances count toward it.
	if s.Count.N() != 4 {
		t.Fatalf("arbiter count = %d, want 4 (retarget does not itself advance)", s.Count.N())
	}
}

// TestObserveDeclaredReshardTarget_NoopWhenAlreadyDeclared: when the declared
// count already equals the agreed target, the tick is a no-op (no churn).
func TestObserveDeclaredReshardTarget_NoopWhenAlreadyDeclared(t *testing.T) {
	c := newDeclaredReshardCluster(t, 8, 8) // declared == seeded target

	before, _, _ := c.arbiter.Read()
	c.observeDeclaredReshardTarget()
	after, _, _ := c.arbiter.Read()

	if after.Target.N() != 8 || after.Epoch != before.Epoch {
		t.Fatalf("steady-at-declared should be a no-op: before %+v, after %+v", before, after)
	}
}

// TestObserveDeclaredReshardTarget_DefersWhileConverging: while the arbiter is
// still converging toward an EARLIER target (count != target), a newly-declared
// count must NOT move the target again - the cluster finishes the in-flight
// generation step first. Pins the steadiness gate.
func TestObserveDeclaredReshardTarget_DefersWhileConverging(t *testing.T) {
	c := newDeclaredReshardCluster(t, 4, 8)
	// Force the arbiter mid-converge: target ahead of count (as if a 4->16
	// reshard is in flight) so it is NOT steady.
	if _, err := c.arbiter.Retarget(storageunit.MustUnitCount(16)); err != nil {
		t.Fatal(err)
	}

	c.observeDeclaredReshardTarget() // declared is 8, but arbiter is mid-converge

	s, _, _ := c.arbiter.Read()
	if s.Target.N() != 16 {
		t.Fatalf("target = %d, want 16 unchanged (must not retarget while converging)", s.Target.N())
	}
}

// TestObserveDeclaredReshardTarget_NoArbiterIsNoop: without a wired arbiter
// (R=1 / legacy / no ConditionalStore) the tick is an inert no-op (no panic).
func TestObserveDeclaredReshardTarget_NoArbiterIsNoop(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")
	c.membership = openDeclaringMembership(t, "n1", 8)
	// c.arbiter is nil (initReshardArbiter never called).
	c.observeDeclaredReshardTarget() // must not panic
}
