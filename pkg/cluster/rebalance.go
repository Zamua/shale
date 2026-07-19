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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// bumpRingGen records that the ring shape has changed + schedules the
// debounced response at settleDelay from now. Subsequent calls within the
// window reset the timer rather than fire multiple passes. Called from the
// membership events loop AND from the reconcile loop.
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
		if c.settlePending.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// migrationGuardError builds the error returned to a Put/Delete that
// lands on a unit currently mid-handoff. Carries codes.ResourceExhausted
// + a retry-after-ms hint per docs/SPEC.md "Cutover" so clients know the
// failure is transient + how long to back off.
//
// Three reserved codes carry distinct retry semantics and must not be
// conflated:
//
//   - ResourceExhausted: in-flight handoff. Retry after the hinted
//     backoff; the unit is moving and the next attempt may land on a
//     different owner.
//   - FailedPrecondition: forwarding loop-guard (docs/SPEC.md
//     "Failure handling"). The receiving node disagrees with the
//     originator about ownership; client must refresh ring + retry.
//   - Unavailable: a peer's gRPC channel is gone (server killed,
//     connection refused, deadline canceled). The replica counts
//     against the fanout's failure budget so a genuinely-down node
//     short-circuits the call rather than blocking for every peer.
//
// Handoff rejections must be distinguishable from a real down peer so
// isTransientReplicaErr can treat them differently: handoff responses do
// NOT count against the failure budget, so the fanout keeps waiting on
// other replicas instead of failing the whole call when a single replica
// is mid-handoff. Conversely, Unavailable from a dead peer MUST count, so
// (R - W + 1) such failures fail-fast instead of waiting for every
// replica's transport timeout.
func migrationGuardError(retryAfterMs int) error {
	return status.Errorf(codes.ResourceExhausted,
		"shale: key is migrating out; retry after %dms", retryAfterMs)
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
