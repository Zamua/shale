// reconcile.go: owned-but-missing repair (anti-entropy pass).
//
// The ring-vs-ring ComputePlan (plan.go) is correct but incomplete:
// it can only see ownership transitions that show up as a DIFFERENCE
// between the two ring snapshots a node actually observed. It is blind
// to a partition this node owns but never physically received, because
// such a partition looks stable (old owner == new owner == self) in
// both snapshots. That blind spot is the founder-grows (1 -> 2)
// data-loss path: under slow gossip a joiner pins a self-only
// {joiner} bootstrap snapshot, so when the founder later becomes
// visible the ring-vs-ring diff emits no Receive for partitions the
// converged ring assigns to the joiner; the keys live on the founder
// and are never streamed across.
//
// reconcile is a second pass keyed on PHYSICAL PLACEMENT rather than
// on ring history. For every partition the new ring assigns to self,
// it asks "do I physically hold any of this partition's keys?" A
// partition assigned to self for which the local backend holds ZERO
// keys is owned-but-missing; reconcile schedules a Receive from the
// partition's prior owner (the node the current ring would have placed
// it on before self joined: the owner in the ring with self removed).
//
// The Receive flows through the IDENTICAL tryRegister -> runReceive ->
// FetchRange path the ring-vs-ring plan uses. There is no second
// migration mechanism, no new wire message, no persisted state.
//
// Safety (docs/SPEC.md "Why reconcile cannot itself lose data"):
// reconcile only ever schedules Receives (non-destructive pulls). It
// never schedules a Send and never deletes anything, so it cannot arm
// the grace sweep on any node and cannot reintroduce the
// copy-before-delete loss from the other direction. A failed pull
// leaves the destination untouched (FetchRange rolls back) and the
// prior owner still holds the data, so the next Evaluate re-detects
// and retries. The failure mode is "retry," never "loss."

package rebalance

import (
	"github.com/Zamua/shale/pkg/ring"
)

// reconcile repairs owned-but-missing partitions: those the new ring
// assigns to self for which the local backend holds zero keys. For
// each, it registers a Receive from the partition's prior owner (the
// owner in newRing with self removed) and launches runReceive through
// the same path the ring-vs-ring plan uses.
//
// Preconditions handled defensively:
//   - newRing nil/empty or single-member (only self): nothing to pull
//     from; return without scanning.
//   - dest nil (source-only test mode): a Receive can't advance, so
//     skip the pass entirely rather than register ranges that would
//     hang. The ring-vs-ring plan already short-circuits dest==nil by
//     flipping Receives straight to Done; reconcile simply does not
//     manufacture work the wired path can't complete.
//   - partFn nil: should not happen (Evaluate wires it before calling
//     reconcile), but guard so a misordered caller is a no-op, not a
//     panic.
func (c *Coordinator) reconcile(newRing *ring.Ring) {
	if newRing == nil || newRing.Empty() {
		return
	}
	// Need at least one peer besides self to have a prior owner to
	// pull from. A single-node ring owns everything itself; there is
	// nowhere a missing partition could have come from.
	if len(newRing.Members()) < 2 {
		return
	}

	c.mu.Lock()
	dest := c.dest
	partFn := c.partFn
	c.mu.Unlock()
	if dest == nil || partFn == nil {
		return
	}

	// The ring as it would have been before self joined: removing self
	// reveals which peer the consistent hash would have placed each of
	// self's partitions on, i.e. the prior owner to pull from.
	priorRing := ring.New()
	for _, m := range newRing.Members() {
		if m.ID == c.self.ID {
			continue
		}
		priorRing.Add(m)
	}
	if priorRing.Empty() {
		return
	}

	// Partitions the new ring assigns to self. These are the only
	// candidates for owned-but-missing repair.
	ownedBySelf := make(map[uint64]struct{})
	for _, pid := range newRing.Partitions() {
		if newRing.Owner(pid).ID == c.self.ID {
			ownedBySelf[pid] = struct{}{}
		}
	}
	if len(ownedBySelf) == 0 {
		return
	}

	// Single local scan to determine which of self's partitions we
	// physically hold at least one key for. Mirrors the sweep's
	// scan-once-then-bucket pattern (sweep.go). A held partition is
	// removed from the candidate set; whatever remains is missing.
	held, err := c.heldPartitions(partFn, ownedBySelf)
	if err != nil {
		// Could not scan (backend transient error). Surface nothing
		// here; the next Evaluate retries. Do not schedule pulls off a
		// failed scan -- we cannot tell missing from unread.
		return
	}

	for pid := range ownedBySelf {
		if _, ok := held[pid]; ok {
			continue // physically present; nothing to repair.
		}
		if c.reconciledClean(pid) {
			// Already pulled this partition successfully on a prior
			// tick and it came back empty (no key has ever hashed into
			// it). Re-pulling would be a harmless but wasteful empty
			// FetchRange every settle tick, so skip it. A pull that
			// FAILED is in StateDone WITH an error and is NOT skipped:
			// reconciledClean returns false for it, so the next tick
			// retries. A pull that returned keys would have flipped
			// held[pid] true above, so it is skipped there.
			continue
		}
		prior := priorRing.Owner(pid)
		if prior.ID == "" || prior.ID == c.self.ID {
			// No distinct prior owner to pull from. Skip; a routed Get
			// would have nowhere else to go either, so there is no data
			// to recover.
			continue
		}
		m := Move{
			PartitionID: pid,
			From:        Member{ID: prior.ID, Addr: prior.Addr},
			To:          c.self,
		}
		// tryRegister refuses partitions already in a non-terminal
		// state, so a partition the ring-vs-ring plan already put in
		// StateReceiving (or a prior reconcile pull still in flight) is
		// dropped here -- the two passes can name the same partition
		// without racing.
		if c.tryRegister(m, StateReceiving) {
			go c.runReceive(m, dest)
		}
	}
}

// reconciledClean reports whether partition pid is already tracked in a
// terminal StateDone with no recorded error. The reconcile pass uses
// this to avoid re-pulling a partition it already drained cleanly
// (notably an empty partition that no key has ever hashed into):
// without the guard, every settle tick would re-issue a harmless but
// wasteful empty FetchRange for each such partition. A partition whose
// last pull FAILED carries an error here, so reconciledClean returns
// false and the next tick retries it.
func (c *Coordinator) reconciledClean(pid uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.ranges[pid]
	if !ok {
		return false
	}
	return r.state == StateDone && r.err == nil
}

// heldPartitions scans the local backend once and returns the subset
// of candidate partitions for which at least one key physically lives
// on this node. Used by reconcile to distinguish owned-and-held from
// owned-but-missing.
//
// The scan stops contributing once every candidate is accounted for,
// but still drains the iterator so the backend can release it. Keys
// whose partition is not a candidate are ignored.
func (c *Coordinator) heldPartitions(partFn PartitionFn, candidates map[uint64]struct{}) (map[uint64]struct{}, error) {
	it, err := c.local.ScanPrefix(nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	held := make(map[uint64]struct{}, len(candidates))
	for {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		if k == nil {
			break
		}
		pid := partFn(k)
		if _, ok := candidates[pid]; ok {
			held[pid] = struct{}{}
		}
	}
	return held, nil
}
