// Package rebalance owns the v0.3 data-migration protocol described
// in docs/SPEC.md "Rebalancing (v0.3+)".
//
// The bounded context is "given a ring transition, move the right
// keys from the right peers, without losing data or letting writes
// silently land on the wrong owner." It splits along three seams:
//
//   - plan.go      pure ComputePlan(oldRing, newRing) -> Plan
//   - state.go     per-range lifecycle Coordinator
//   - execute.go   gRPC stream source + destination adapters
//   - sweep.go     post-migration cleanup of handed-off ranges
//
// plan.go is the pure-domain layer: no I/O, no goroutines, no
// dependencies on backend, rpc, or cluster. ComputePlan is a function
// of two ring snapshots + the self identity, and it returns a Plan
// the Coordinator (state.go) can execute. Keeping this layer pure is
// what makes the v0.3 protocol trivially testable: every transition
// shape (1-node -> 2-node, node-replaces-node, etc.) is a single
// function call asserting on the returned Sends/Receives.
package rebalance

import (
	"sort"

	"github.com/Zamua/shale/pkg/ring"
)

// Member is the rebalance package's view of a cluster node. It is a
// copy of ring.Member's shape rather than an alias: the rebalance
// layer doesn't need (and shouldn't take) a hard dependency on the
// ring package's struct definition beyond ComputePlan's inputs. ID
// is the cluster-unique identity (used for self-comparison + log
// lines); Addr is the gRPC host:port the destination dials when
// opening a MigrateRange stream.
type Member struct {
	ID   string
	Addr string
}

// fromRing converts ring.Member -> Member without leaking the ring
// type through this package's public surface.
func fromRing(m ring.Member) Member { return Member{ID: m.ID, Addr: m.Addr} }

// Range is a single partition of the consistent-hash ring along with
// its current owner. v0.3 uses partition-id as the migration unit
// (the unit buraksezer/consistent already tracks ownership against),
// so "a range" is identified by exactly one uint64 PartitionID.
type Range struct {
	PartitionID uint64
	Owner       Member
}

// Move is one (from, to) ownership transition for a single partition.
// A Plan is built out of these. The Coordinator drives a Move forward
// by either streaming KVs out (if From == self) or pulling them in
// (if To == self).
type Move struct {
	PartitionID uint64
	From        Member
	To          Member
}

// Plan is one node's view of "what do I have to do to catch the data
// state up to the new ring." Generation is the ring generation the
// plan was computed against (callers stamp it onto the wire so the
// other side can reject a stream that was opened against a stale
// ring). Sends are moves where this node is the source; Receives are
// moves where this node is the destination. Moves where this node is
// neither side are omitted entirely (they are between two peers and
// do not concern us).
type Plan struct {
	Generation uint64
	Sends      []Move
	Receives   []Move
}

// ComputePlan returns the local Plan for the transition from oldRing
// to newRing, from self's perspective. Both rings must have been
// constructed with the same partitionCount (the case for every Ring
// produced by pkg/ring.New today) so that Partitions() returns the
// same id space on both sides.
//
// For each partition the new ring tracks:
//
//	oldOwner := oldRing.Owner(partitionID)
//	newOwner := newRing.Owner(partitionID)
//	if oldOwner == self && newOwner != self -> Sends    (push to new owner)
//	if oldOwner != self && newOwner == self -> Receives (pull from old owner)
//	else                                    -> no-op
//
// A nil oldRing is treated as the empty ring: every partition is a
// fresh assignment, so Sends is empty and Receives contains every
// partition whose newOwner is self. This is the first-rebalance case
// where the node has no prior snapshot to diff against.
//
// Partitions that exist on newRing but have no owner (because newRing
// is empty) are skipped entirely; ComputePlan never returns a Move
// involving the zero Member as a participant.
func ComputePlan(self Member, oldRing, newRing *ring.Ring, gen uint64) Plan {
	plan := Plan{Generation: gen}
	if newRing == nil || newRing.Empty() {
		return plan
	}

	for _, pid := range newRing.Partitions() {
		newOwner := fromRing(newRing.Owner(pid))
		if newOwner.ID == "" {
			// Should not happen on a non-empty ring; skip defensively.
			continue
		}

		var oldOwner Member
		if oldRing != nil && !oldRing.Empty() {
			oldOwner = fromRing(oldRing.Owner(pid))
		}

		switch {
		case oldOwner.ID == self.ID && newOwner.ID != self.ID:
			plan.Sends = append(plan.Sends, Move{
				PartitionID: pid,
				From:        self,
				To:          newOwner,
			})
		case oldOwner.ID != self.ID && newOwner.ID == self.ID:
			// oldOwner.ID may be "" (first rebalance or partition was
			// unowned in the previous ring); that's the natural way to
			// represent "no prior source," and the Coordinator's
			// execution path treats an empty From as "no source to
			// pull from, mark this range Done immediately."
			plan.Receives = append(plan.Receives, Move{
				PartitionID: pid,
				From:        oldOwner,
				To:          self,
			})
		default:
			// old == self && new == self: stable, no-op.
			// old != self && new != self: two peers handle it.
		}
	}

	// Sort for stable iteration: tests compare Plan values directly,
	// and the operator-facing --dry-run output is easier to read when
	// moves come out in partition order.
	sort.Slice(plan.Sends, func(i, j int) bool { return plan.Sends[i].PartitionID < plan.Sends[j].PartitionID })
	sort.Slice(plan.Receives, func(i, j int) bool { return plan.Receives[i].PartitionID < plan.Receives[j].PartitionID })

	return plan
}
