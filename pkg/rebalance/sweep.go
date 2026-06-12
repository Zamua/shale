// sweep.go: background cleanup of post-handoff ranges.
//
// After a source completes a stream + flips to StateHandedOff, it
// still owns the local copy of the range's keys. The grace period
// (Options.GraceDuration, default 30s) gives peers + clients time to
// observe the new ring + retire stragglers that landed on the old
// owner. Once the grace window elapses, the sweep:
//
//  1. deletes the range's keys from the local Backend (they are
//     authoritative on the new owner now)
//  2. flips the range from StateHandedOff to StateDone
//
// The sweep runs on a single timer + holds no locks across Backend
// deletes (it batches partition ids first, then drops the
// Coordinator mutex while it calls into Backend). This matters
// because Backend.Delete may take real I/O time on slate / pebble.

package rebalance

import (
	"context"
	"time"
)

// sweepInterval is how often the sweep checks for expired
// hand-offs. Independent of GraceDuration so a long grace doesn't
// starve cleanup of ranges that have multiple expiries within the
// window. 10s is a balance between "responsive enough that operators
// don't notice stale state" and "cheap enough that the sweep itself
// is invisible in profiles." Exposed as a var so the cluster layer
// (and tests) can override it for faster feedback in integration
// fixtures; production stays at the package default.
var sweepInterval = 10 * time.Second

// SetSweepInterval overrides the package-wide sweep tick interval.
// Used by the cluster layer to keep integration tests under a few
// seconds of wall clock. Not goroutine-safe relative to RunSweep;
// call before any Coordinator starts its sweep loop, or call from
// tests where you control the lifecycle.
func SetSweepInterval(d time.Duration) {
	if d <= 0 {
		return
	}
	sweepInterval = d
}

// RunSweep starts the background cleanup loop. It runs until ctx is
// canceled or the Coordinator is Stop()ped. Callers (the cluster
// layer) typically launch this once per Coordinator from a goroutine.
func (c *Coordinator) RunSweep(ctx context.Context) {
	t := time.NewTicker(sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case now := <-t.C:
			c.sweepHandedOff(ctx, now)
			// Periodic reconcile retry. The settle-timer Evaluate fires
			// once per membership change (one ring generation), but a
			// reconcile Receive can come back empty when its source was
			// itself still settling (source-not-ready race): at R>1 on a
			// shrink, two survivors can each newly enter a partition's
			// replica set and briefly race who has the data first. A
			// single post-leave Evaluate is not enough to converge those.
			// Re-running reconcile on the sweep tick retries every
			// still-owned-but-missing partition on a bounded schedule
			// until the cluster reaches its replica count, with no extra
			// membership event required. No-op once settled (every owned
			// partition is held) and at R<=1 (the ring-vs-ring plan
			// already delivers single-owner moves). See docs/SPEC.md
			// "Reconcile (owned-but-missing repair)."
			c.reconcileTick()
		}
	}
}

// reconcileTick re-runs the reconcile + evict passes against the ring
// captured by the last Evaluate. It is the periodic, gen-independent
// retry the sweep loop drives so a source-not-ready reconcile pull (or a
// stale-replica eviction that could not register while a migration was
// in flight) is retried until the cluster converges, without waiting on
// another membership change. Guarded so it is a cheap no-op before the
// first Evaluate (no ring captured) and at R<=1 (single-owner placement
// is fully handled by the ring-vs-ring plan).
func (c *Coordinator) reconcileTick() {
	c.mu.Lock()
	r := c.ring
	rf := c.repFactor
	c.mu.Unlock()
	if r == nil || r.Empty() || rf <= 1 {
		return
	}
	c.reconcile(r, true)
	c.evictStaleReplicas(r)
}

// SweepOnce runs one cleanup pass at time now. Public so tests +
// operator tools can trigger a sweep without waiting on the timer.
// Production code SHOULD use RunSweep + let the ticker drive it.
func (c *Coordinator) SweepOnce(ctx context.Context, now time.Time) {
	c.sweepHandedOff(ctx, now)
}

// sweepHandedOff is the internal entry point; SweepOnce is the
// public, tick-free variant tests + operators call directly.
//
// For each range in StateHandedOff whose handedOffAt + GraceDuration
// is in the past as of now:
//   - find the partition's keys in the local Backend
//   - delete them
//   - flip the range to StateDone
//
// Keys are identified by re-running the same partition function the
// range was registered against (Coordinator.partFn). If partFn is
// nil (sweep racing against the first Evaluate), the sweep is a no-op
// for that tick.
func (c *Coordinator) sweepHandedOff(ctx context.Context, now time.Time) {
	c.mu.Lock()
	if c.partFn == nil {
		c.mu.Unlock()
		return
	}
	partFn := c.partFn
	grace := c.opts.GraceDuration
	// Snapshot the set of (partition, deadline) we care about so we
	// can drop the lock before doing any Backend I/O.
	type victim struct{ pid uint64 }
	var victims []victim
	for pid, r := range c.ranges {
		if r.state != StateHandedOff {
			continue
		}
		if now.Sub(r.handedOffAt) < grace {
			continue
		}
		victims = append(victims, victim{pid: pid})
	}
	c.mu.Unlock()

	if len(victims) == 0 {
		return
	}

	// Build the partition-id set once so the key-belongs-to-victim
	// check is a single map lookup.
	//
	// Replica-aware DELETE side (docs/SPEC.md "Replica-set placement"):
	// a handed-off partition's PRIMARY moved to a peer, but at R>1 this
	// node may still be a SUCCESSOR replica of that partition. If so it
	// must RETAIN the keys, not delete them: the replica set the current
	// ring assigns is ReplicasForPartition(pid, R), and this node is in
	// it. We therefore split victims into "delete" (self no longer a
	// replica - the legacy single-owner case, and every R=1 case) and
	// "retain" (self still a replica - keys stay, partition just flips to
	// Done). At R<=1 selfIsPartitionReplica is always false, so every
	// victim deletes exactly as before.
	want := make(map[uint64]struct{}, len(victims))
	for _, v := range victims {
		if c.selfIsPartitionReplica(v.pid) {
			// Still a replica-owner: keep the keys. The partition is
			// flipped to Done below alongside the deleting victims.
			continue
		}
		want[v.pid] = struct{}{}
	}

	// Every victim is a retained replica (nothing to delete): skip the
	// scan entirely and flip them straight to Done. Common at R>1 when a
	// node that held all keys hands off PRIMARY of some partitions but
	// stays a replica of them.
	if len(want) == 0 {
		c.mu.Lock()
		for _, v := range victims {
			if r, ok := c.ranges[v.pid]; ok && r.state == StateHandedOff {
				r.state = StateDone
			}
		}
		c.mu.Unlock()
		return
	}

	it, err := c.local.ScanPrefix(nil)
	if err != nil {
		// Surface the error onto every victim so operators can see
		// the failure in Snapshot; do not flip to Done since we
		// have not actually cleaned up.
		c.mu.Lock()
		for _, v := range victims {
			if r, ok := c.ranges[v.pid]; ok && r.state == StateHandedOff {
				r.err = err
			}
		}
		c.mu.Unlock()
		return
	}
	defer func() { _ = it.Close() }()

	var toDelete [][]byte
	for {
		if ctx.Err() != nil {
			return
		}
		k, _, err := it.Next()
		if err != nil {
			return
		}
		if k == nil {
			break
		}
		if _, ok := want[partFn(k)]; !ok {
			continue
		}
		toDelete = append(toDelete, append([]byte(nil), k...))
	}

	for _, k := range toDelete {
		if ctx.Err() != nil {
			return
		}
		_ = c.local.Delete(k)
	}

	c.mu.Lock()
	for _, v := range victims {
		if r, ok := c.ranges[v.pid]; ok && r.state == StateHandedOff {
			r.state = StateDone
		}
	}
	c.mu.Unlock()
}
