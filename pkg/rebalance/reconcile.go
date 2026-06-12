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
	"time"

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
//
// reconcile runs one owned-but-missing repair pass. retryEmpty controls
// whether a partition that previously drained EMPTY (zero keys) is
// re-pulled: false on the settle-timer Evaluate path (so a stable ring
// quiesces - no re-pull of genuinely-empty partitions on every tick),
// true on the periodic sweep-tick path (so a source-not-ready empty pull
// on a shrink self-heals across a few ticks). The two share all other
// logic; only the empty-partition skip decision differs.
func (c *Coordinator) reconcile(newRing *ring.Ring, retryEmpty bool) {
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
	//
	// Replica-aware DELIVER side (docs/SPEC.md "Replica-set placement"):
	// at R>1 "self owns pid" means self is anywhere in the partition's
	// replica set (ReplicasForPartition(pid, R)), not just its PRIMARY.
	// Without this, a fresh joiner only backfills the partitions whose
	// PRIMARY moved to it and never backfills the partitions where it is
	// a SUCCESSOR replica (PRIMARY stayed on a peer): at N=2,R=2 that is
	// exactly the half of keys the joiner must also hold. selfIsReplica
	// below picks PRIMARY (legacy) at R<=1 and the full replica set at
	// R>1, so the R=1 candidate set is byte-for-byte the old one.
	ownedBySelf := make(map[uint64]struct{})
	for _, pid := range newRing.Partitions() {
		if c.selfIsPartitionReplica(pid) || newRing.Owner(pid).ID == c.self.ID {
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
		if c.reconciledClean(pid, retryEmpty) {
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
		prior := c.reconcileSource(newRing, priorRing, pid)
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

// evictStaleReplicas is the replica-aware DELETE-side counterpart to the
// reconcile DELIVER pass. The ring-vs-ring ComputePlan only schedules a
// Send (which leads to a sweep delete) when self loses the PRIMARY of a
// partition. At R>1 that misses a whole class of shed: a node can hold a
// partition it was a SUCCESSOR replica of (never the primary) and a ring
// change can drop it OUT of the partition's replica set without producing
// any handoff event. Those keys must be deleted so the cluster converges
// to exactly N*R physical copies; otherwise growth leaks copies (a node
// that left the replica set keeps the data forever = over-retention).
//
// The pass scans the local backend once, buckets keys by partition, and
// for every partition that (a) self physically holds at least one key for
// and (b) self is NOT in ReplicasForPartition(pid, R) of the current ring
// and (c) is not already mid-migration, it registers the partition
// straight into StateHandedOff with handedOffAt=now. The existing sweep
// then deletes it after the grace window (re-checking selfIsPartitionReplica,
// which is false here, so it deletes). Routing the delete through the
// sweep reuses the grace delay: the grace window gives any peer newly in
// the replica set time to pull its copy (pull-based reconcile) before the
// source drops it, and at R>1 the data is in any case still on the other
// R-1 replicas, so an eviction can never drop a key below its replica
// count.
//
// No-op unless replica-awareness is configured (repFactor > 1). At R<=1
// the single-owner plan's Sends already cover every shed, so this pass
// would only duplicate work; it short-circuits.
func (c *Coordinator) evictStaleReplicas(newRing *ring.Ring) {
	if newRing == nil || newRing.Empty() {
		return
	}
	c.mu.Lock()
	rf := c.repFactor
	partFn := c.partFn
	c.mu.Unlock()
	if rf <= 1 || partFn == nil {
		return
	}

	it, err := c.local.ScanPrefix(nil)
	if err != nil {
		// Transient backend error: the next Evaluate retries. Do not
		// evict off a failed scan.
		return
	}
	held := make(map[uint64]struct{})
	for {
		k, _, err := it.Next()
		if err != nil {
			_ = it.Close()
			return
		}
		if k == nil {
			break
		}
		held[partFn(k)] = struct{}{}
	}
	_ = it.Close()

	for pid := range held {
		if c.selfIsPartitionReplica(pid) {
			continue // still a replica-owner: RETAIN.
		}
		// Register the stale-replica partition for the grace-gated sweep
		// delete. tryRegister refuses partitions already in a
		// non-terminal state, so a Send/Receive the ring-vs-ring plan
		// already scheduled for this partition wins and we do not race
		// it. Target the partition's current primary purely for
		// Snapshot readability; the delete is self-driven via the sweep
		// and does not depend on the destination.
		to := newRing.Owner(pid)
		m := Move{
			PartitionID: pid,
			From:        c.self,
			To:          Member{ID: to.ID, Addr: to.Addr},
		}
		if c.tryRegister(m, StateHandedOff) {
			c.markEvicted(pid)
		}
	}
}

// markEvicted stamps handedOffAt=now on a partition just registered into
// StateHandedOff by evictStaleReplicas, so the sweep's grace check has a
// deadline to compare against. Separated from tryRegister because the
// general registration path (Send/Receive) sets handedOffAt only on the
// Sending->HandedOff transition, not at registration.
func (c *Coordinator) markEvicted(pid uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.ranges[pid]; ok && r.state == StateHandedOff {
		r.handedOffAt = time.Now()
	}
}

// reconcileSource picks the peer to pull partition pid from when self is
// owned-but-missing. At R<=1 there is exactly one prior owner (the
// reduced-ring primary, which is where the single copy lived), so it
// returns priorRing.Owner(pid) - byte-for-byte the legacy behavior.
//
// At R>1 the source must be a peer that ACTUALLY holds a replica of the
// partition, and on a SHRINK that is NOT always the reduced-ring primary:
// when a holder leaves and self newly enters the replica set, the other
// surviving holder may be a successor replica, not the primary the
// reduced ring would name (which might never have held the key). So we
// prefer a member of the partition's CURRENT replica set
// (ReplicasForPartition(pid, R) on newRing) other than self - those are
// exactly the nodes the ring expects to hold it - and fall back to the
// reduced-ring owner only if the replica set yields no distinct peer.
func (c *Coordinator) reconcileSource(newRing, priorRing *ring.Ring, pid uint64) ring.Member {
	c.mu.Lock()
	rf := c.repFactor
	c.mu.Unlock()
	if rf > 1 && newRing != nil && !newRing.Empty() {
		for _, m := range newRing.ReplicasForPartition(pid, rf) {
			if m.ID != "" && m.ID != c.self.ID {
				return m
			}
		}
	}
	return priorRing.Owner(pid)
}

// reconciledClean reports whether partition pid is already tracked in a
// terminal StateDone with no recorded error. The reconcile pass uses
// this to avoid re-pulling a partition it already drained cleanly
// (notably an empty partition that no key has ever hashed into):
// without the guard, every settle tick would re-issue a harmless but
// wasteful empty FetchRange for each such partition. A partition whose
// last pull FAILED carries an error here, so reconciledClean returns
// false and the next tick retries it.
func (c *Coordinator) reconciledClean(pid uint64, retryEmpty bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.ranges[pid]
	if !ok {
		return false
	}
	if r.state != StateDone || r.err != nil {
		return false
	}
	if r.keyCount > 0 {
		return true // delivered data; genuinely clean.
	}
	// Same-gen Done with zero keys: the pull drained empty. On the
	// settle-timer Evaluate path (retryEmpty == false) we treat that as
	// clean immediately so a stable ring quiesces - no re-pull of a
	// genuinely-empty partition on every tick. On the periodic
	// sweep-tick path (retryEmpty == true) the empty is ambiguous
	// (genuinely-empty vs source-not-ready race on a shrink), so we
	// retry a small bounded number of times before accepting it as
	// empty; each retry burns one of the budget.
	if !retryEmpty {
		return true
	}
	if r.reconcileEmptyTries >= reconcileEmptyRetryCap {
		return true // exhausted retries; treat as genuinely empty.
	}
	r.reconcileEmptyTries++
	return false // retry the empty pull (source-not-ready self-heal).
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
