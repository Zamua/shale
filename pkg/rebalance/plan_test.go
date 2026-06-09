package rebalance_test

import (
	"testing"

	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/ring"
)

// buildRing is a small helper so the tests read like ring transitions
// instead of like ring-construction noise.
func buildRing(ids ...string) *ring.Ring {
	r := ring.New()
	for _, id := range ids {
		r.Add(ring.Member{ID: id, Addr: "addr:" + id})
	}
	return r
}

func TestComputePlan_FirstRebalance_NilOldRing(t *testing.T) {
	// Joining as the only node: every partition is a Receive for
	// self, with an empty From (no prior owner).
	self := rebalance.Member{ID: "n1", Addr: "addr:n1"}
	newR := buildRing("n1")

	plan := rebalance.ComputePlan(self, nil, newR, 1)

	if plan.Generation != 1 {
		t.Fatalf("Generation = %d, want 1", plan.Generation)
	}
	if len(plan.Sends) != 0 {
		t.Fatalf("Sends should be empty on first rebalance, got %d", len(plan.Sends))
	}
	if len(plan.Receives) != len(newR.Partitions()) {
		t.Fatalf("Receives = %d, want %d (every partition)", len(plan.Receives), len(newR.Partitions()))
	}
	for _, mv := range plan.Receives {
		if mv.To.ID != "n1" {
			t.Fatalf("Receive.To = %+v, want n1", mv.To)
		}
		if mv.From.ID != "" {
			t.Fatalf("Receive.From should be zero on first rebalance, got %+v", mv.From)
		}
	}
}

func TestComputePlan_OneNodeToTwoNodes_SplitsPartitions(t *testing.T) {
	// n1 is the only node before; n1 + n2 after. n1 keeps ~half its
	// partitions and ships ~half to n2. n2's plan is the mirror:
	// it receives those partitions from n1.
	old := buildRing("n1")
	new := buildRing("n1", "n2")

	n1 := rebalance.Member{ID: "n1", Addr: "addr:n1"}
	n2 := rebalance.Member{ID: "n2", Addr: "addr:n2"}

	n1Plan := rebalance.ComputePlan(n1, old, new, 2)
	n2Plan := rebalance.ComputePlan(n2, old, new, 2)

	if len(n1Plan.Receives) != 0 {
		t.Fatalf("n1 should receive nothing in a 1->2 transition (it was the sole owner), got %d", len(n1Plan.Receives))
	}
	if len(n2Plan.Sends) != 0 {
		t.Fatalf("n2 should send nothing (it had no data before), got %d", len(n2Plan.Sends))
	}
	if len(n1Plan.Sends) == 0 {
		t.Fatalf("n1 should send some partitions to n2; got 0")
	}
	if len(n1Plan.Sends) != len(n2Plan.Receives) {
		t.Fatalf("n1.Sends (%d) must equal n2.Receives (%d): both sides derive the same plan",
			len(n1Plan.Sends), len(n2Plan.Receives))
	}

	// Per partition: n1's Send.To == n2; n2's Receive.From == n1.
	for _, mv := range n1Plan.Sends {
		if mv.From.ID != "n1" || mv.To.ID != "n2" {
			t.Fatalf("n1 send malformed: %+v", mv)
		}
	}
	for _, mv := range n2Plan.Receives {
		if mv.From.ID != "n1" || mv.To.ID != "n2" {
			t.Fatalf("n2 receive malformed: %+v", mv)
		}
	}

	// Sanity: every partition n1 sends is one n2 receives (same
	// partition id), no duplicates.
	sent := make(map[uint64]struct{}, len(n1Plan.Sends))
	for _, mv := range n1Plan.Sends {
		sent[mv.PartitionID] = struct{}{}
	}
	for _, mv := range n2Plan.Receives {
		if _, ok := sent[mv.PartitionID]; !ok {
			t.Fatalf("n2 receives partition %d that n1 does not send", mv.PartitionID)
		}
	}
}

func TestComputePlan_TwoNodesToOneNode_NodeLeaves(t *testing.T) {
	// n2 leaves. n1 picks up every partition n2 used to own. From
	// n1's view: no Sends, Receives = (the partitions n2 used to own).
	old := buildRing("n1", "n2")
	new := buildRing("n1")
	n1 := rebalance.Member{ID: "n1", Addr: "addr:n1"}

	plan := rebalance.ComputePlan(n1, old, new, 3)

	if len(plan.Sends) != 0 {
		t.Fatalf("n1 should send nothing when n2 leaves; got %d", len(plan.Sends))
	}
	if len(plan.Receives) == 0 {
		t.Fatalf("n1 should receive partitions previously owned by n2; got 0")
	}
	for _, mv := range plan.Receives {
		if mv.From.ID != "n2" {
			t.Fatalf("Receive.From should be n2, got %+v", mv.From)
		}
		if mv.To.ID != "n1" {
			t.Fatalf("Receive.To should be n1, got %+v", mv.To)
		}
	}
}

func TestComputePlan_NodeReplacesNode_IsNoopForUnrelatedSelf(t *testing.T) {
	// 3 nodes -> swap n2 out, n3 in (but with a different ID). n1's
	// view: some partitions move between n2 and the new node; none
	// of them involve n1, so n1's plan should be empty.
	old := buildRing("n1", "n2", "n4")
	new := buildRing("n1", "n3", "n4")
	n1 := rebalance.Member{ID: "n1", Addr: "addr:n1"}

	plan := rebalance.ComputePlan(n1, old, new, 4)

	// n1 may keep some partitions, send some it owned to n3, and
	// receive some that used to be on n2. The "no Move involves
	// self at all" assertion isn't quite right; what we want is:
	// every move in n1's plan has either From or To == n1.
	for _, mv := range plan.Sends {
		if mv.From.ID != "n1" {
			t.Fatalf("Sends entry without n1 as From: %+v", mv)
		}
		if mv.To.ID == "n1" {
			t.Fatalf("Sends entry with n1 as both From and To: %+v", mv)
		}
	}
	for _, mv := range plan.Receives {
		if mv.To.ID != "n1" {
			t.Fatalf("Receives entry without n1 as To: %+v", mv)
		}
		if mv.From.ID == "n1" {
			t.Fatalf("Receives entry with n1 as both From and To: %+v", mv)
		}
	}
}

func TestComputePlan_NoRingChange_IsEmpty(t *testing.T) {
	// Ring is the same on both sides. Nothing moves anywhere.
	r := buildRing("n1", "n2", "n3")
	n1 := rebalance.Member{ID: "n1", Addr: "addr:n1"}

	plan := rebalance.ComputePlan(n1, r, r, 5)

	if len(plan.Sends) != 0 || len(plan.Receives) != 0 {
		t.Fatalf("identical rings should produce empty plan; got Sends=%d Receives=%d",
			len(plan.Sends), len(plan.Receives))
	}
}

func TestComputePlan_EmptyNewRing_IsEmpty(t *testing.T) {
	// Defensive: never panic on an empty new ring. There is nothing
	// to assign, so the plan is empty.
	r := buildRing("n1")
	empty := ring.New()
	n1 := rebalance.Member{ID: "n1", Addr: "addr:n1"}

	plan := rebalance.ComputePlan(n1, r, empty, 6)
	if len(plan.Sends) != 0 || len(plan.Receives) != 0 {
		t.Fatalf("empty new ring should produce empty plan; got Sends=%d Receives=%d",
			len(plan.Sends), len(plan.Receives))
	}
}

func TestComputePlan_SendsSortedByPartitionID(t *testing.T) {
	old := buildRing("n1")
	new := buildRing("n1", "n2", "n3")
	n1 := rebalance.Member{ID: "n1", Addr: "addr:n1"}

	plan := rebalance.ComputePlan(n1, old, new, 7)
	for i := 1; i < len(plan.Sends); i++ {
		if plan.Sends[i-1].PartitionID >= plan.Sends[i].PartitionID {
			t.Fatalf("Sends not ascending at index %d: %d then %d",
				i, plan.Sends[i-1].PartitionID, plan.Sends[i].PartitionID)
		}
	}
}
