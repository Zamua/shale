package cluster

// rebalance.go: the cluster layer's integration with pkg/rebalance.
//
// The rebalance package owns the per-range state machine + the pure
// ComputePlan domain function (see docs/SPEC.md "Rebalancing"). This
// file is the adapter that wires those into the running Cluster:
//
//   - watches membership events, increments a monotonic ring
//     generation, and schedules a debounced Evaluate after the
//     settle delay
//   - snapshots the current ring on each Evaluate so ComputePlan
//     has both an "old" (last evaluated) and a "new" (current) ring
//   - implements rebalance.MigrateDestination by dialing peers over
//     gRPC + consuming their MigrateRange stream (the source side
//     lives in pkg/rpc/server.go)
//
// The settle-timer pattern is: every membership-event delivery into
// the cluster increments c.ringGen + (re)schedules a timer for
// settleDelay in the future. When that timer fires the cluster grabs
// the current ring snapshot, hands (lastEvalRing, currentRing) to
// the Coordinator, and stores currentRing as lastEvalRing for the
// next tick. This collapses bursts of joins/leaves (e.g. a rolling
// restart) into one Evaluate pass instead of thrashing the cluster
// through every intermediate ring shape.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/ring"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// defaultSettleDelay is the v0.3 rebalance debounce. Exposed as a var
// so cluster_internal_test.go (and the in-process integration tests)
// can shrink it to keep wall-clock under a few hundred ms.
var defaultSettleDelay = 5 * time.Second

// initRebalance constructs the Coordinator + sweep goroutine, pins
// lastEvalRing to the at-startup ring snapshot, and for joiners runs
// a SYNCHRONOUS bootstrap Evaluate so the new node's share of the
// existing keyspace is pulled from prior owners before Open returns.
//
// Two startup shapes need different handling, distinguished by
// whether Config.Seeds was set:
//
//   - Founder (Seeds empty): we are the first node. The at-startup
//     ring is {self}; pin lastEvalRing to that snapshot. Any future
//     events that add peers correctly compute Sends from us to them.
//
//   - Joiner (Seeds non-empty): we joined an existing cluster.
//     Memberlist's Join is synchronous so by the time Open returns,
//     the at-startup ring lists every peer + self. Synthesize old =
//     (current - self) - the "ring as it existed before I joined" -
//     and call rebalance.Evaluate(old, current) inline. This
//     registers the joiner's Receives from prior owners. We then
//     pin lastEvalRing = current so the next ring change diffs
//     properly. Blocking on WaitForIdle here would deadlock against
//     the sweep + put-during-grace test scenarios; we settle for
//     "registered" + let the sweep + retries race themselves out
//     before the operator-facing API needs the data settled.
//
// In single-node mode there are no peers, no plan, no need to run
// any of this.
func (c *Cluster) initRebalance() {
	self := rebalance.Member{ID: c.cfg.NodeID, Addr: c.cfg.GRPCAddr}
	opts := rebalance.DefaultOptions()
	opts.SettleDelay = c.settleDelay()
	if c.cfg.RebalanceGraceDuration > 0 {
		opts.GraceDuration = c.cfg.RebalanceGraceDuration
	}
	if c.cfg.RebalanceRetryAfterMs > 0 {
		opts.RetryAfterMs = c.cfg.RebalanceRetryAfterMs
	}
	// Destination is the cluster's own gRPC-backed puller. Source is
	// left nil so Coordinator.Evaluate auto-wires a NewLocalSource
	// against the backend on first run (we want the partition function
	// to come from the same ring snapshot the plan was computed
	// against).
	opts.Destination = &clusterDestination{c: c}
	// AwaitHandoffSignal gates the source's Sending -> HandedOff flip
	// on the destination acknowledging the stream. Without it, the
	// source's local scan independently triggers the flip + the
	// sweep deletes the keys on grace, even if the destination
	// crashed mid-stream. With it, MarkSendComplete (called by the
	// gRPC handler in pkg/rpc/server.go) carries the only signal that
	// flips the source state.
	opts.AwaitHandoffSignal = true
	opts.HandoffTimeout = c.handoffTimeout()
	rb := rebalance.New(self, c.backend, opts)
	c.rebalance.Store(rb)

	startup := c.snapshotRing()

	// Background sweep handles post-handoff cleanup. The Coordinator's
	// stop channel cuts this loop on Close.
	c.loopWG.Add(1)
	c.rebalanceCtx, c.rebalanceCancel = context.WithCancel(context.Background())
	go func() {
		defer c.loopWG.Done()
		rb.RunSweep(c.rebalanceCtx)
	}()

	// settleMu publishes c.lastEvalRing to the runEvaluate reader. We
	// hold it for the assignment even though Open hasn't spawned the
	// reader goroutine yet, so any future code that grows another
	// reader path can't trip the race detector on a now-unguarded
	// write. The bootstrap Evaluate below is fine to run outside the
	// lock: it touches Coordinator state only.
	if len(c.cfg.Seeds) > 0 && len(startup.Members()) > 1 {
		// Joiner: synthesize "ring before I joined" baseline +
		// fire the bootstrap Evaluate inline so the pull is
		// dispatched before Open returns. Receives proceed
		// asynchronously: callers that want to observe a fully
		// primed joiner use WaitForRebalanceIdle.
		//
		// We intentionally do NOT block Open here. Blocking
		// hides the StateReceiving window from observers + makes
		// it impossible for tests to assert the v0.3 cutover
		// behavior (destination returns try_other_owner on reads,
		// FailedPrecondition on writes) for the pull duration.
		// A later settle-timer Evaluate that arrives mid-receive
		// can't introduce a NEW Move for the same partition
		// (current ring is unchanged), so there's no
		// tryRegister conflict to worry about.
		//
		bootstrapOld := ringMinus(startup, c.cfg.NodeID)
		c.settleMu.Lock()
		c.lastEvalRing = startup
		c.settleMu.Unlock()
		rb.Evaluate(bootstrapOld, startup, c.ringGen.Load())
	} else {
		// Founder (or joiner with no peers yet visible): pin
		// lastEvalRing to the init-time snapshot. Subsequent
		// membership events diff against this baseline; if peers
		// only become visible later, that diff produces the
		// correct Sends.
		c.settleMu.Lock()
		c.lastEvalRing = startup
		c.settleMu.Unlock()
	}
}

// settleDelay returns the configured settle delay, falling back to
// the package default if Config.RebalanceSettleDelay is zero.
func (c *Cluster) settleDelay() time.Duration {
	if c.cfg.RebalanceSettleDelay > 0 {
		return c.cfg.RebalanceSettleDelay
	}
	return defaultSettleDelay
}

// handoffTimeout returns the configured handoff timeout, falling
// back to the rebalance package default (5 minutes) when
// Config.RebalanceHandoffTimeout is zero. The Coordinator's New()
// also applies its own default if we pass zero, but propagating the
// cluster-level config here lets the replaceCoordinator path honor
// the same value without re-reading rebalance defaults.
func (c *Cluster) handoffTimeout() time.Duration {
	if c.cfg.RebalanceHandoffTimeout > 0 {
		return c.cfg.RebalanceHandoffTimeout
	}
	return 5 * time.Minute
}

// bumpRingGen records that the ring shape has changed + schedules an
// Evaluate at settleDelay from now. Subsequent calls within the
// window reset the timer rather than fire multiple Evaluates. Called
// from the membership events loop AND from the reconcile loop AND
// from ProposeRebalance(apply) (the latter bypasses the timer; see
// runEvaluateNow).
func (c *Cluster) bumpRingGen() {
	c.ringGen.Add(1)
	c.scheduleEvaluate()
}

// scheduleEvaluate (re)arms the settle timer. Holds settleMu only
// long enough to swap the timer reference.
func (c *Cluster) scheduleEvaluate() {
	if c.rebalance.Load() == nil {
		return
	}
	c.settleMu.Lock()
	defer c.settleMu.Unlock()
	if c.settleTimer != nil {
		c.settleTimer.Stop()
	}
	d := c.settleDelay()
	c.settleTimer = time.AfterFunc(d, c.runEvaluate)
}

// runEvaluate fires when the settle timer elapses. Snapshots the
// current ring, computes the plan against lastEvalRing, hands it
// to the Coordinator, and stores the new snapshot as lastEvalRing
// for next time. lastEvalRing is pinned at init time (per the
// founder/joiner branches in initRebalance), so by the time
// runEvaluate first runs there is always a valid baseline.
func (c *Cluster) runEvaluate() {
	if c.closed.Load() {
		return
	}
	rb := c.rebalance.Load()
	if rb == nil {
		return
	}
	current := c.snapshotRing()
	gen := c.ringGen.Load()

	c.settleMu.Lock()
	old := c.lastEvalRing
	c.lastEvalRing = current
	c.settleMu.Unlock()

	rb.Evaluate(old, current, gen)
}

// ringMinus returns a fresh ring containing every member of src
// except the one with id == omit. Used by the bootstrap path so a
// new node Evaluating for the first time computes its plan against
// "ring without me" as the synthetic prior state, surfacing the
// correct From peers on Receives instead of the empty-Member
// first-rebalance sentinel.
func ringMinus(src *ring.Ring, omit string) *ring.Ring {
	out := ring.New()
	if src == nil {
		return out
	}
	for _, m := range src.Members() {
		if m.ID == omit {
			continue
		}
		out.Add(m)
	}
	return out
}

// runEvaluateNow bypasses the settle timer and Evaluates immediately.
// Used by the operator-facing ProposeRebalance(apply) path: the human
// has already decided they want the migration, no need to wait the
// debounce window.
func (c *Cluster) runEvaluateNow() {
	c.settleMu.Lock()
	if c.settleTimer != nil {
		c.settleTimer.Stop()
		c.settleTimer = nil
	}
	c.settleMu.Unlock()
	c.runEvaluate()
}

// snapshotRing returns a fresh *ring.Ring populated with the members
// the live ring currently knows about. ComputePlan diffs two rings;
// taking a snapshot lets the next Evaluate compare "what membership
// was at the last tick" vs "what it is now" without holding the live
// ring stable across the call.
func (c *Cluster) snapshotRing() *ring.Ring {
	r := ring.New()
	if c.ring == nil {
		return r
	}
	for _, m := range c.ring.Members() {
		r.Add(m)
	}
	return r
}

// WaitForRebalanceIdle blocks until every in-flight migration on this
// node has reached a terminal state (StateDone) or ctx is canceled.
// Used by tests + by the operator-facing --apply path so callers can
// report "rebalance complete" with confidence.
//
// In single-node mode there is no Coordinator; the call returns
// immediately with nil.
func (c *Cluster) WaitForRebalanceIdle(ctx context.Context) error {
	rb := c.rebalance.Load()
	if rb == nil {
		return nil
	}
	return rb.WaitForIdle(ctx)
}

// migrationGuardError builds the error returned to a Put/Delete that
// lands on a key whose partition is currently sending out. Carries
// codes.ResourceExhausted + a retry-after-ms hint per docs/SPEC.md
// "Cutover" so clients know the failure is transient + how long to
// back off.
//
// Three reserved codes carry distinct retry semantics in the v0.4+
// model and must not be conflated:
//
//   - ResourceExhausted: in-flight migration. Retry after the
//     hinted backoff; the partition is mid-handoff and the next
//     attempt may land on a different owner.
//   - FailedPrecondition: forwarding loop-guard (docs/SPEC.md
//     "Failure handling"). The receiving node disagrees with the
//     originator about ownership; client must refresh ring + retry.
//   - Unavailable: a peer's gRPC channel is gone (server killed,
//     connection refused, deadline canceled). The replica counts
//     against the fanout's failure budget so a genuinely-down node
//     short-circuits the call rather than blocking for every peer.
//
// Migration-guard rejections must be distinguishable from a real
// down peer so isTransientReplicaErr can treat them differently:
// migration-guard responses do NOT count against the failure budget,
// so the fanout keeps waiting on other replicas instead of failing
// the whole call when a single replica is mid-handoff. Conversely,
// Unavailable from a dead peer MUST count, so (R - W + 1) such
// failures fail-fast instead of waiting for every replica's
// transport timeout.
func migrationGuardError(retryAfterMs int) error {
	return status.Errorf(codes.ResourceExhausted,
		"shale: key is migrating out; retry after %dms", retryAfterMs)
}

// retryAfterMs reads the configured retry-after hint from the
// Coordinator's options. 50ms is the default in
// rebalance.DefaultOptions.
func (c *Cluster) retryAfterMs() int {
	if c.cfg.RebalanceRetryAfterMs > 0 {
		return c.cfg.RebalanceRetryAfterMs
	}
	return 50
}

// IsMigrating returns whether the given key's partition is currently
// migrating out from this node. The cluster layer calls this before
// dispatching a Put/Delete to the local backend; if true, the op is
// rejected with a transient FailedPrecondition.
//
// Safe in single-node mode: returns false (no Coordinator).
func (c *Cluster) IsMigrating(key []byte) bool {
	rb := c.rebalance.Load()
	if rb == nil {
		return false
	}
	return rb.IsMigrating(key)
}

// -- clusterDestination ----------------------------------------------
//
// clusterDestination implements rebalance.MigrateDestination. Each
// FetchRange call dials the source peer's gRPC service, opens a
// MigrateRange stream, and writes each KV to the local backend. The
// terminal MigrationDone message carries the source's CRC32; the
// destination compares and rolls back on mismatch.

type clusterDestination struct {
	c *Cluster
}

func (d *clusterDestination) FetchRange(ctx context.Context, peer rebalance.Member, partitionIDs []uint64, gen uint64) (int, error) {
	if peer.Addr == "" {
		// Empty From means "no prior source" (first-rebalance case
		// in ComputePlan). Nothing to fetch; treat as a zero-key
		// success so the Coordinator flips the range to Done.
		return 0, nil
	}
	cli, err := d.c.clientFor(peer.Addr)
	if err != nil {
		return 0, fmt.Errorf("rebalance: dial source %s: %w", peer.ID, err)
	}
	// Stamp the destination's CURRENT ringGen on the wire (not the
	// Coordinator-captured value, which may be stale by the time the
	// stream opens). The source's freshness check is "destination's
	// gen < source's own current"; using the dest's latest counter
	// avoids spurious rejection when ringGen on this node has caught
	// up to a fresher ring shape between Evaluate + the gRPC dial.
	if cur := d.c.ringGen.Load(); cur > gen {
		gen = cur
	}
	stream, err := cli.api.MigrateRange(ctx, &pb.RangeSpec{
		PartitionIds:   partitionIDs,
		RingGeneration: gen,
	})
	if err != nil {
		return 0, fmt.Errorf("rebalance: open MigrateRange: %w", err)
	}

	hasher := crc32.NewIEEE()
	applied := make([][]byte, 0)
	rollback := func() {
		for _, k := range applied {
			_ = d.c.backend.Delete(k)
		}
	}

	count := 0
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Source closed without a Done marker: treat as
				// failure + roll back. A clean termination uses
				// Done; an EOF before Done means the stream was
				// interrupted.
				rollback()
				return 0, errors.New("rebalance: source EOF before MigrationDone")
			}
			rollback()
			return 0, fmt.Errorf("rebalance: stream recv: %w", err)
		}
		switch body := msg.GetBody().(type) {
		case *pb.MigrateChunk_Kv:
			kv := body.Kv
			if err := d.c.backend.Put(kv.GetKey(), kv.GetValue()); err != nil {
				rollback()
				return 0, fmt.Errorf("rebalance: apply key: %w", err)
			}
			applied = append(applied, append([]byte(nil), kv.GetKey()...))
			hasher.Write(kv.GetKey())
			hasher.Write(kv.GetValue())
			count++
		case *pb.MigrateChunk_Done:
			if uint64(count) != body.Done.GetTotalKeys() {
				rollback()
				return 0, fmt.Errorf("rebalance: count mismatch: got %d, source reported %d",
					count, body.Done.GetTotalKeys())
			}
			got := hasher.Sum(nil)
			want := body.Done.GetChecksum()
			if len(want) > 0 && !bytes.Equal(got, want) {
				rollback()
				return 0, fmt.Errorf("rebalance: checksum mismatch: got %x, want %x", got, want)
			}
			return count, nil
		default:
			// Unknown oneof variant: defensive. Treat as protocol
			// error + roll back.
			rollback()
			return 0, errors.New("rebalance: unknown chunk body")
		}
	}
}

// -- ProposeRebalance -------------------------------------------------

// ProposeRebalance is the operator-facing planner / applier /
// canceller, invoked by the `shale rebalance --dry-run | --apply |
// --cancel` CLI subcommand via the ProposeRebalance gRPC method.
//
// Exactly one of dryRun / apply / cancel must be true.
//
//   - dryRun: compute the plan against the current ring + return it
//     without executing.
//   - apply:  Evaluate immediately (bypass the settle delay) +
//     return the plan that was just queued.
//   - cancel: stop in-flight migrations + return the plan we were
//     working on (now aborted).
//
// Returns an error if invariants are violated (e.g. multiple flags
// set, or called in single-node mode).
func (c *Cluster) ProposeRebalance(dryRun, apply, cancel bool) ([]RebalanceItem, error) {
	if c.closed.Load() {
		return nil, errors.New("cluster: closed")
	}
	if c.rebalance.Load() == nil {
		return nil, errors.New("cluster: rebalance not available in single-node mode")
	}
	flags := 0
	if dryRun {
		flags++
	}
	if apply {
		flags++
	}
	if cancel {
		flags++
	}
	if flags != 1 {
		return nil, errors.New("cluster: ProposeRebalance requires exactly one of dryRun/apply/cancel")
	}

	switch {
	case cancel:
		if rb := c.rebalance.Load(); rb != nil {
			rb.Stop()
		}
		// Replace the Coordinator so a subsequent ring change picks
		// up a fresh state machine. The cancel call is a "stop
		// everything in flight"; if the operator changes their mind
		// they can trigger another --apply.
		c.replaceCoordinator()
		return c.snapshotRebalanceItems(), nil

	case apply:
		c.runEvaluateNow()
		return c.snapshotRebalanceItems(), nil

	default: // dryRun
		return c.computeRebalanceItems(), nil
	}
}

// RebalanceItem is one row of the operator-facing plan: which
// partition would move, from whom to whom. The estimated key count
// is best-effort: zero for receives (we don't have a count until the
// stream completes) and the in-progress count for sends pulled from
// the Coordinator snapshot.
type RebalanceItem struct {
	PartitionID       uint64
	CurrentOwner      string
	ProposedOwner     string
	EstimatedKeyCount uint64
}

// computeRebalanceItems runs ComputePlan against the current ring +
// returns the plan as operator-readable rows. Used by --dry-run.
func (c *Cluster) computeRebalanceItems() []RebalanceItem {
	current := c.snapshotRing()
	c.settleMu.Lock()
	old := c.lastEvalRing
	c.settleMu.Unlock()
	self := rebalance.Member{ID: c.cfg.NodeID, Addr: c.cfg.GRPCAddr}
	plan := rebalance.ComputePlan(self, old, current, c.ringGen.Load())

	out := make([]RebalanceItem, 0, len(plan.Sends)+len(plan.Receives))
	for _, mv := range plan.Sends {
		out = append(out, RebalanceItem{
			PartitionID:   mv.PartitionID,
			CurrentOwner:  mv.From.ID,
			ProposedOwner: mv.To.ID,
		})
	}
	for _, mv := range plan.Receives {
		out = append(out, RebalanceItem{
			PartitionID:   mv.PartitionID,
			CurrentOwner:  mv.From.ID,
			ProposedOwner: mv.To.ID,
		})
	}
	return out
}

// snapshotRebalanceItems returns the Coordinator's current
// per-range view as operator-readable rows. Used by --apply +
// --cancel (both want to surface "this is what was queued / what was
// in flight"). Sorted by partition id for stable iteration.
func (c *Cluster) snapshotRebalanceItems() []RebalanceItem {
	rb := c.rebalance.Load()
	if rb == nil {
		return nil
	}
	snap := rb.Snapshot()
	out := make([]RebalanceItem, 0, len(snap))
	for _, s := range snap {
		out = append(out, RebalanceItem{
			PartitionID:       s.PartitionID,
			CurrentOwner:      s.From.ID,
			ProposedOwner:     s.To.ID,
			EstimatedKeyCount: uint64(s.KeyCount),
		})
	}
	return out
}

// replaceCoordinator stops the existing Coordinator + creates a
// fresh one bound to the same backend. Used by --cancel: callers
// want the in-flight migrations gone, but they may still trigger
// future Evaluates as the cluster topology evolves.
func (c *Cluster) replaceCoordinator() {
	self := rebalance.Member{ID: c.cfg.NodeID, Addr: c.cfg.GRPCAddr}
	opts := rebalance.DefaultOptions()
	opts.SettleDelay = c.settleDelay()
	if c.cfg.RebalanceGraceDuration > 0 {
		opts.GraceDuration = c.cfg.RebalanceGraceDuration
	}
	if c.cfg.RebalanceRetryAfterMs > 0 {
		opts.RetryAfterMs = c.cfg.RebalanceRetryAfterMs
	}
	opts.Destination = &clusterDestination{c: c}
	opts.AwaitHandoffSignal = true
	opts.HandoffTimeout = c.handoffTimeout()
	c.rebalance.Store(rebalance.New(self, c.backend, opts))

	// Reset the lastEvalRing baseline to the current ring so the
	// next Evaluate sees the current shape as the steady state. The
	// alternative ("keep the old lastEvalRing so the next Evaluate
	// re-issues the plan we just cancelled") would defeat the
	// purpose of --cancel.
	c.settleMu.Lock()
	c.lastEvalRing = c.snapshotRing()
	c.settleMu.Unlock()
}

// MigrateRangeSource exposes the Coordinator's source for the
// rpc.Server's MigrateRange handler to iterate. Returns the channel
// pair from the underlying MigrateSource. Held in this file rather
// than in cluster.go so the import of pkg/rebalance stays local to
// the integration adapter.
//
// Stale-generation guard: per docs/SPEC.md "Wire", ring_generation
// is per-node best-effort freshness, not a cluster-wide consistency
// check. v0.3 uses per-node monotonic counters that diverge during
// normal join/leave races (a node that joined later starts at 0 +
// has fewer NotifyJoin events to count). Enforcing a strict <
// comparison against the source's own counter spuriously rejects
// mid-bootstrap streams even after the FetchRange path stamps the
// dest's CURRENT ringGen on the wire. We accept the value as-is
// + lean on the forwarding loop-guard + per-key migration guards
// for the wrong-owner protection the spec ultimately calls for.
// A real freshness check lands when the gossip layer carries a
// cluster-wide generation (v0.4 or later).
func (c *Cluster) MigrateRangeSource(partitionIDs []uint64, ringGen uint64) (<-chan rebalance.KeyValue, <-chan error, error) {
	if c.rebalance.Load() == nil {
		return nil, nil, status.Error(codes.FailedPrecondition,
			"shale: rebalance not available in single-node mode")
	}

	// Use a freshly-built localSource bound to the local backend +
	// the current ring's partition function. We don't reuse the
	// Coordinator's auto-wired source because that source's
	// partition function is pinned to the ring snapshot used at
	// Evaluate time; the destination-asked partitions are talking
	// about THIS source's current ring view.
	partFn := func(k []byte) uint64 { return c.ring.PartitionID(k) }
	source := rebalance.NewLocalSource(c.backend, partFn)
	kvCh, errCh := source.OpenRange(partitionIDs, ringGen)
	return kvCh, errCh, nil
}

// SignalMigrateRangeComplete is the gRPC handler's callback after a
// MigrateRange stream returns. It threads the destination-observed
// outcome (totalKeys + terminal err) back to the Coordinator so the
// source-side runSend can flip Sending -> HandedOff only on a clean
// completion. The handler in pkg/rpc/server.go calls this in a defer
// with the stream's return value.
//
// One MigrateRange call may cover multiple partitions; the same
// signal is delivered to each, so a partial-stream failure rolls
// every partition in the call back to Done-with-error together.
//
// No-op in single-node mode (no Coordinator). Safe to call after
// Close: the Coordinator's stop channel cuts pending waiters; this
// just drops the signal.
func (c *Cluster) SignalMigrateRangeComplete(partitionIDs []uint64, totalKeys int, err error) {
	rb := c.rebalance.Load()
	if rb == nil {
		return
	}
	for _, pid := range partitionIDs {
		rb.MarkSendComplete(pid, totalKeys, err)
	}
}

// -- internals shared with cluster.go --------------------------------
//
// The fields below live on Cluster (see cluster.go). They are
// referenced from this file via c.foo.

// RingGeneration returns this node's current ring generation
// counter. Exposed for the rpc.Server's MigrateRange handler +
// for observability tooling.
func (c *Cluster) RingGeneration() uint64 { return c.ringGen.Load() }

// RebalanceSnapshot returns the Coordinator's full per-range view
// (every partition the Coordinator has ever tracked, with its
// current State). Exposed for observability tooling + tests.
// Returns nil in single-node mode.
func (c *Cluster) RebalanceSnapshot() []rebalance.RangeStatus {
	rb := c.rebalance.Load()
	if rb == nil {
		return nil
	}
	return rb.Snapshot()
}
