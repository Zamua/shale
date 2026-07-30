package cluster

// rebalance.go: the debounce + guards shared by the cluster's one
// rebalancing engine.
//
// shale has a single coordination model. A multi-node cluster is
// multi-backend: every key lives in a storage UNIT, and a unit moves
// between nodes by LEASE HANDOFF, not by copying keys. The old owner
// releases the unit (CloseUnit, flush) and the new owner acquires it
// (OpenUnit at a higher epoch, fencing the old). The bytes never travel
// through shale; they are already in the shared durable store the
// storageunit.BackendFactory opens against. That engine lives in
// multibackend_rebalance.go (R=1) and multibackend_overlap.go (R>1).
//
// What is left in THIS file is the part both of those share with the
// membership layer:
//
//   - the settle-timer debounce, so a burst of joins/leaves (a rolling
//     restart) collapses into one reconcile pass instead of thrashing
//     the cluster through every intermediate ring shape
//   - the ring generation counter every reconcile reasons from
//   - the transient-rejection error a KV op returns when it lands on a
//     unit that is mid-handoff
//
// Single-node mode reaches none of it: Open returns before membership
// is built, so there is no ring to change and nothing to debounce.

import (
	"context"
	"fmt"
	"time"
)

// defaultSettleDelay is the rebalance debounce. Exposed as a var so
// cluster_internal_test.go (and the in-process integration tests) can
// shrink it to keep wall-clock under a few hundred ms.
var defaultSettleDelay = 5 * time.Second

// settleDelay returns the configured settle delay, falling back to
// the package default if Config.RebalanceSettleDelay is zero.
func (c *Cluster) settleDelay() time.Duration {
	if c.cfg.RebalanceSettleDelay > 0 {
		return c.cfg.RebalanceSettleDelay
	}
	return defaultSettleDelay
}

// bumpRingGen records that the coordination view has changed + schedules the
// debounced response at settleDelay from now. Subsequent calls within the
// window reset the timer rather than fire multiple passes, so a burst of
// changes (or a coalesced hint that stood for several) still costs one pass.
//
// The debounced response is the COPY-FREE unit reconcile: acquire
// newly-owned units, release no-longer-owned ones (see
// multibackend_rebalance.go). Only a multi-node cluster reaches here, and
// every multi-node cluster is multi-backend, so there is exactly one
// response to arm.
func (c *Cluster) bumpRingGen() {
	c.ringGen.Add(1)
	c.scheduleReconcile()
}

// WaitForRebalanceIdle blocks until this node is rebalance-idle or ctx
// is canceled. Per docs/SPEC.md "Trigger", a node is rebalance-idle when
// no settle-timer reconcile is pending (settlePending == 0): nothing is
// armed-but-unrun and no reconcile callback is mid-flight.
//
// Including the pending half is what closes the race where a caller polls
// immediately after a membership change, BEFORE the debounce fires. The
// wait blocks through the debounce, so an observed idle guarantees the
// reconcile has run AND drained: scheduleReconcile holds the obligation
// from arm through the applied mounts (see multibackend_rebalance.go), so
// there is no gap between "pending" dropping to zero and the unit moves
// being visible.
//
// Single-node mode never schedules a reconcile, so settlePending stays 0
// and the call returns on the first poll.
func (c *Cluster) WaitForRebalanceIdle(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		// <= 0, not == 0: the counter is maintained to never go negative,
		// but if an accounting bug ever drifts it below zero the failure
		// must degrade to EARLY idle (a caller proceeds a beat soon), never
		// to PERMANENTLY not-idle (== 0 is unsatisfiable from -1, and every
		// idle-waiter times out forever after - the exact wedge the
		// fired-timer double-decrement produced).
		if c.settlePending.Load() <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			// A timeout here has ONE proximate cause - settlePending held
			// above zero - but several distinct mechanisms behind it (a
			// pending timer that has not fired, a reconcile pass blocked
			// mid-run, an accounting leak). An error that cannot say which
			// costs a debugging session; report the settle machinery's state.
			c.settleMu.Lock()
			armed := c.settleTimer != nil
			immediate := c.settleImmediate
			c.settleMu.Unlock()
			return fmt.Errorf("%w (settlePending=%d timerArmed=%v immediate=%v reconcileRunning=%v)",
				ctx.Err(), c.settlePending.Load(), armed, immediate, c.reconcileRunning.Load())
		case <-ticker.C:
		}
	}
}

// retryAfterMs reads the configured retry-after hint, defaulting to 50ms.
// It is the base of the Layer-2 handoff retry backoff as well as the hint
// handed to the client (see multibackend_handoff_retry.go).
func (c *Cluster) retryAfterMs() int {
	if c.cfg.RebalanceRetryAfterMs > 0 {
		return c.cfg.RebalanceRetryAfterMs
	}
	return 50
}

// RingGeneration returns this node's current ring generation counter.
// Exposed for observability tooling.
func (c *Cluster) RingGeneration() uint64 { return c.ringGen.Load() }
