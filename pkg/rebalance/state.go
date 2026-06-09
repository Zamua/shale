// state.go: per-range lifecycle coordinator.
//
// The Coordinator owns one node's view of "what migrations are in
// flight." Evaluate(oldRing, newRing) computes a Plan, marks each
// outgoing range as Sending + each incoming range as Receiving,
// and launches a goroutine per range to drive it through to
// HandedOff (source) or Done (destination). A background sweep
// (sweep.go) flips HandedOff to Done after the grace duration and
// deletes the no-longer-owned keys from local storage.
//
// State is per-partition + lives only in memory. There is no
// persisted rebalance log: on restart, the next Evaluate sees the
// current ring + the keys actually held and re-issues whatever
// moves are needed. This is the same code path as the steady-state
// one (docs/SPEC.md "Plan").
package rebalance

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/ring"
)

// RangeState is the lifecycle position of a single partition on this
// node. Names match docs/SPEC.md "Cutover":
//
//	StatePreMigration -- assigned but not yet started
//	StateSending      -- source is streaming out
//	StateReceiving    -- destination is pulling in
//	StateHandedOff    -- source done; in grace period; reads still served
//	StateDone         -- destination finished, or source cleanup complete
type RangeState int

const (
	StatePreMigration RangeState = iota
	StateSending
	StateReceiving
	StateHandedOff
	StateDone
)

func (s RangeState) String() string {
	switch s {
	case StatePreMigration:
		return "pre-migration"
	case StateSending:
		return "sending"
	case StateReceiving:
		return "receiving"
	case StateHandedOff:
		return "handed-off"
	case StateDone:
		return "done"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// IsTerminal reports whether s is a steady state the Coordinator no
// longer needs to drive forward. StateDone is the only terminal
// state; HandedOff is non-terminal because the sweep still needs to
// run cleanup.
func (s RangeState) IsTerminal() bool { return s == StateDone }

// Options tune the Coordinator. The zero value is not useful; call
// DefaultOptions() or set every field explicitly.
type Options struct {
	// SettleDelay is the v0.3 rebalance trigger debounce. The
	// cluster layer (not the Coordinator itself) honors this when
	// scheduling Evaluate from membership events.
	SettleDelay time.Duration

	// GraceDuration is how long a Sending range stays HandedOff
	// before the sweep deletes its now-foreign keys. docs/SPEC.md
	// specifies T_drain at the protocol level (30s in the v0.3 spec
	// text); this package defaults to 60s to give peers extra time
	// to converge on the new ring before the source drops the data.
	GraceDuration time.Duration

	// ChunkSize is the target number of KV pairs per outgoing
	// stream message. Pure throughput knob, has no protocol effect.
	ChunkSize int

	// RetryAfterMs is the hint included in FailedPrecondition
	// responses when a write lands on a node whose partition is
	// mid-migration. Clients SHOULD retry after this many ms.
	RetryAfterMs int

	// Source builds outgoing streams. If nil, the Coordinator
	// auto-constructs a NewLocalSource(local, ringPartitionFn).
	Source MigrateSource

	// Destination dials peers + pulls incoming streams. If nil, the
	// Coordinator runs in "source-only" mode: incoming moves are
	// recorded in StateReceiving but never advance, which is useful
	// for tests that only care about source-side behavior.
	Destination MigrateDestination

	// AwaitHandoffSignal gates how runSend decides when to flip a
	// partition Sending -> HandedOff. When false (the test default),
	// runSend drains its local source scan + flips immediately on
	// scan completion, no destination involvement. When true (the
	// cluster wires this), runSend waits for an explicit
	// MarkSendComplete call before flipping: the source observes the
	// gRPC handler's return value before exposing the partition to
	// the sweep + deleting any keys.
	//
	// Without this, a destination crash mid-stream still flips the
	// source to HandedOff (because the source's local scan ran
	// independently of the stream), the grace timer expires, and the
	// sweep deletes the keys -- data loss on backends that don't
	// snapshot like the memory backend does. The wired path makes the
	// handler's success/error the load-bearing signal for the flip.
	AwaitHandoffSignal bool

	// HandoffTimeout bounds the runSend wait for MarkSendComplete
	// when AwaitHandoffSignal is true. If no signal arrives within
	// the window, runSend transitions to StateDone with an error so
	// the next Evaluate pass can re-register + retry. Defaults to
	// 5 minutes (DefaultOptions); cluster integration tests shrink
	// it. Zero falls back to a sensible default at New() time.
	HandoffTimeout time.Duration
}

// DefaultOptions returns the spec-aligned tunables. Callers that
// want to override a single field should still start here:
//
//	opts := rebalance.DefaultOptions()
//	opts.GraceDuration = 5 * time.Second   // test
//	c := rebalance.New(self, be, opts)
func DefaultOptions() Options {
	return Options{
		SettleDelay:    5 * time.Second,
		GraceDuration:  60 * time.Second,
		ChunkSize:      64,
		RetryAfterMs:   50,
		HandoffTimeout: 5 * time.Minute,
	}
}

// RangeStatus is the snapshot record returned by Coordinator.Snapshot.
// One struct per partition the Coordinator is tracking.
type RangeStatus struct {
	PartitionID uint64
	State       RangeState
	From        Member
	To          Member
	KeyCount    int
	StartedAt   time.Time
	HandedOffAt time.Time
	Err         string
}

// rangeRecord is the internal mutable state for one partition. Kept
// separate from RangeStatus so the public snapshot type stays a
// value (no locks, no pointers) callers can pass around freely.
type rangeRecord struct {
	move        Move
	state       RangeState
	keyCount    int
	startedAt   time.Time
	handedOffAt time.Time
	err         error
}

// Coordinator owns this node's per-range state + drives migrations
// to completion. Construct one per node; share it across all goroutines
// that care about migration state (the gRPC server's MigrateRange
// handler reads it; the cluster's Put/Get path consults IsMigrating).
type Coordinator struct {
	self    Member
	local   backend.Backend
	opts    Options
	source  MigrateSource
	dest    MigrateDestination
	partFn  PartitionFn
	stopCh  chan struct{}
	stopped bool

	mu       sync.Mutex
	ranges   map[uint64]*rangeRecord
	gen      uint64
	handoffs map[uint64]chan handoffSignal
}

// handoffSignal carries the destination-observed outcome of one
// outgoing range stream. The gRPC handler (cluster integration
// only) calls MarkSendComplete after the source-to-destination
// stream returns; runSend waits for the signal before flipping the
// partition Sending -> HandedOff. err == nil + totalKeys >= 0 means
// "destination acked the count + checksum"; err != nil means the
// stream errored (destination crashed mid-stream, checksum mismatch,
// etc.) + the partition must NOT advance to HandedOff so the sweep
// won't delete the source's local copy.
type handoffSignal struct {
	totalKeys int
	err       error
}

// New constructs a Coordinator. If opts.Source is nil, a default
// localSource is wired against the provided backend the first time
// Evaluate runs (we wait so the partition function can come from
// the same Ring the plan was computed against). The Coordinator
// does not start the background sweep automatically; call
// (*Coordinator).RunSweep(ctx) from the cluster layer to enable it.
func New(self Member, local backend.Backend, opts Options) *Coordinator {
	if opts.HandoffTimeout <= 0 {
		opts.HandoffTimeout = 5 * time.Minute
	}
	c := &Coordinator{
		self:     self,
		local:    local,
		opts:     opts,
		source:   opts.Source,
		dest:     opts.Destination,
		stopCh:   make(chan struct{}),
		ranges:   make(map[uint64]*rangeRecord),
		handoffs: make(map[uint64]chan handoffSignal),
	}
	return c
}

// Stop signals all running migrations to halt + closes the sweep
// loop if it is running. Idempotent.
func (c *Coordinator) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}
	c.stopped = true
	close(c.stopCh)
}

// Evaluate computes the local plan against (oldRing, newRing) and
// launches goroutines to drive each move. It is non-blocking: by
// the time it returns, every move has been registered in the
// Coordinator's range table + has a goroutine in flight. Callers
// wait for quiescence via WaitForIdle.
//
// Re-entering Evaluate while moves are in flight is safe: ranges
// already in StateSending or StateReceiving are not re-launched;
// new ranges (those not yet tracked) join the in-flight set.
func (c *Coordinator) Evaluate(oldRing, newRing *ring.Ring, gen uint64) {
	plan := ComputePlan(c.self, oldRing, newRing, gen)

	c.mu.Lock()
	c.gen = gen
	// Wire a default source on first use if the caller did not
	// inject one. We use newRing for the partition function so the
	// scan filter matches the ring the plan was computed against.
	if c.source == nil {
		c.partFn = ringPartitionFn(newRing)
		c.source = NewLocalSource(c.local, c.partFn)
	} else if c.partFn == nil {
		c.partFn = ringPartitionFn(newRing)
	}
	source := c.source
	dest := c.dest
	c.mu.Unlock()

	for _, m := range plan.Sends {
		if c.tryRegister(m, StateSending) {
			go c.runSend(m, source)
		}
	}
	for _, m := range plan.Receives {
		if c.tryRegister(m, StateReceiving) {
			if dest != nil {
				go c.runReceive(m, dest)
			} else {
				// No destination wired (source-only mode); mark the
				// range Done immediately so WaitForIdle does not
				// hang on it. This is the test/single-source case.
				c.transition(m.PartitionID, StateDone, nil, 0)
			}
		}
	}
}

// tryRegister adds a Move under state, IFF the partition is not
// already tracked in a non-terminal state. Returns true if the move
// was newly registered (caller should launch the goroutine), false
// if a move is already in flight.
func (c *Coordinator) tryRegister(m Move, state RangeState) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.ranges[m.PartitionID]; ok && !existing.state.IsTerminal() {
		return false
	}
	c.ranges[m.PartitionID] = &rangeRecord{
		move:      m,
		state:     state,
		startedAt: time.Now(),
	}
	return true
}

// transition flips one partition into a new state + records key
// count / error. Callers pass nil error + 0 keys when they do not
// apply.
func (c *Coordinator) transition(pid uint64, to RangeState, err error, keyCount int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.ranges[pid]
	if !ok {
		return
	}
	r.state = to
	if err != nil {
		r.err = err
	}
	if keyCount > 0 {
		r.keyCount = keyCount
	}
	if to == StateHandedOff {
		r.handedOffAt = time.Now()
	}
}

// runSend drives a Sending partition forward. Two execution shapes,
// selected by Options.AwaitHandoffSignal:
//
//  1. Local-scan mode (AwaitHandoffSignal=false, the test default):
//     drain the source once locally to count keys, then flip to
//     HandedOff. There is no destination involvement; the sweep
//     deletes after grace. Useful for tests that don't model the
//     full source-destination round trip.
//
//  2. Wired mode (AwaitHandoffSignal=true, the cluster sets this):
//     skip the local scan entirely. Wait on a per-partition signal
//     channel that MarkSendComplete will publish to when the gRPC
//     handler returns. A successful return flips to HandedOff; an
//     error keeps the partition out of HandedOff so the sweep won't
//     delete the source's local copy + the next Evaluate retries.
//     This is the fix that closes the v0.3 data-loss path where the
//     source was previously flipping HandedOff independently of the
//     destination's outcome.
func (c *Coordinator) runSend(m Move, source MigrateSource) {
	if c.opts.AwaitHandoffSignal {
		c.runSendWired(m)
		return
	}

	// Local-scan mode: count what we would have shipped so
	// RangeStatus.KeyCount is meaningful. The destination's FetchRange
	// call against the same MigrateSource will trigger an independent
	// scan in the in-process test; that's intentional (the scan is
	// read-only).
	kvCh, errCh := source.OpenRange([]uint64{m.PartitionID}, c.currentGen())
	count := 0
	for kv := range kvCh {
		_ = kv
		count++
	}
	if err := <-errCh; err != nil {
		c.transition(m.PartitionID, StateDone, fmt.Errorf("rebalance: send: %w", err), 0)
		return
	}
	c.transition(m.PartitionID, StateHandedOff, nil, count)
}

// runSendWired waits for the gRPC handler (the source-side server
// for MigrateRange) to signal completion via MarkSendComplete. The
// signal carries the destination-acked count + the stream's terminal
// error (nil on clean Done). The Coordinator then flips state:
//
//   - clean signal (err == nil): Sending -> HandedOff. Sweep will
//     delete after grace.
//   - error signal (err != nil): Sending -> Done with error attached.
//     Sweep skips the partition; next Evaluate sees the terminal
//     state + re-registers the Send if the new ring still warrants
//     one.
//   - timeout (no signal within Options.HandoffTimeout): treat as
//     "destination never even asked." Same Done-with-error path so
//     a stuck partition can be retried by the next Evaluate.
//   - Stop: return without transitioning; the cluster is shutting
//     down + further state changes would race with teardown.
func (c *Coordinator) runSendWired(m Move) {
	ch := c.registerHandoff(m.PartitionID)
	defer c.clearHandoff(m.PartitionID)

	timer := time.NewTimer(c.opts.HandoffTimeout)
	defer timer.Stop()

	select {
	case sig, ok := <-ch:
		if !ok {
			// Handoff channel was closed without a signal (Coordinator
			// stop / clear race). Don't transition.
			return
		}
		if sig.err != nil {
			c.transition(m.PartitionID, StateDone, fmt.Errorf("rebalance: handoff: %w", sig.err), 0)
			return
		}
		c.transition(m.PartitionID, StateHandedOff, nil, sig.totalKeys)
	case <-c.stopCh:
		return
	case <-timer.C:
		c.transition(m.PartitionID, StateDone,
			fmt.Errorf("rebalance: handoff: timed out after %s waiting for destination ack", c.opts.HandoffTimeout),
			0)
	}
}

// registerHandoff creates the per-partition signal channel runSendWired
// reads from, OR returns the existing one if a prior call already
// registered. A buffered channel of capacity 1 means MarkSendComplete
// never blocks (the latest signal wins). Held in c.handoffs so the
// gRPC handler -> Coordinator lookup is O(1).
func (c *Coordinator) registerHandoff(pid uint64) chan handoffSignal {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch, ok := c.handoffs[pid]; ok {
		return ch
	}
	ch := make(chan handoffSignal, 1)
	c.handoffs[pid] = ch
	return ch
}

// clearHandoff drops the per-partition signal channel after runSendWired
// has consumed (or timed out on) the signal. Idempotent; safe to call
// even if no entry exists.
func (c *Coordinator) clearHandoff(pid uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.handoffs, pid)
}

// MarkSendComplete publishes the destination's stream outcome for a
// single partition. The cluster's gRPC handler defers a call to this
// with the gRPC server-streaming return value as err (nil on clean
// completion, non-nil if Send() failed at any point or the source's
// scan errored). totalKeys is the count the source actually streamed.
//
// Non-blocking + create-on-demand: if runSendWired hasn't yet
// registered the per-partition channel (the destination's MigrateRange
// can fire before the source's settle timer elapses + Evaluate runs),
// MarkSendComplete creates the channel itself and parks the signal
// on it. The signal sits buffered until runSendWired arrives + reads
// it. This rendezvous is what makes the source-observes-destination
// guarantee hold even when the destination opens its stream before
// the source has computed its plan.
//
// Only meaningful when Options.AwaitHandoffSignal is true; with the
// default false, runSend never reads the channel + the parked signal
// is GC'd at clearHandoff time. The cluster integration sets the
// flag; tests that want the wired behavior should set it too.
func (c *Coordinator) MarkSendComplete(pid uint64, totalKeys int, err error) {
	c.mu.Lock()
	ch, ok := c.handoffs[pid]
	if !ok {
		ch = make(chan handoffSignal, 1)
		c.handoffs[pid] = ch
	}
	c.mu.Unlock()
	select {
	case ch <- handoffSignal{totalKeys: totalKeys, err: err}:
	default:
		// Channel was already signaled (rare; e.g. a re-fired stream
		// before clearHandoff). The first signal wins.
	}
}

// runReceive pulls one partition from its source. On success the
// range flips straight to Done; on failure it flips to Done with an
// error attached + the next Evaluate pass will retry.
func (c *Coordinator) runReceive(m Move, dest MigrateDestination) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// If Stop fires mid-receive, cancel the in-flight FetchRange.
	go func() {
		select {
		case <-c.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	n, err := dest.FetchRange(ctx, m.From, []uint64{m.PartitionID}, c.currentGen())
	if err != nil {
		c.transition(m.PartitionID, StateDone, fmt.Errorf("rebalance: receive: %w", err), 0)
		return
	}
	c.transition(m.PartitionID, StateDone, nil, n)
}

func (c *Coordinator) currentGen() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gen
}

// IsMigrating reports whether key's partition is mid-migration OUT
// of this node (StateSending or StateHandedOff). The cluster layer
// uses this on the source side to reject writes for the key (the
// data is being streamed out; accepting a write here would either
// be lost or arrive at the destination out of order).
//
// Returns false for any other state (or unknown partition).
func (c *Coordinator) IsMigrating(key []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partFn == nil {
		return false
	}
	pid := c.partFn(key)
	r, ok := c.ranges[pid]
	if !ok {
		return false
	}
	return r.state == StateSending || r.state == StateHandedOff
}

// IsReceiving reports whether key's partition is mid-migration INTO
// this node (StateReceiving). The cluster layer uses this on the
// destination side to hint "try other owner" on reads + to reject
// writes (the destination is not yet authoritative; the source is
// still serving reads + the stream is still in flight).
//
// Returns false for any other state (or unknown partition).
func (c *Coordinator) IsReceiving(key []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partFn == nil {
		return false
	}
	pid := c.partFn(key)
	r, ok := c.ranges[pid]
	if !ok {
		return false
	}
	return r.state == StateReceiving
}

// WaitForIdle blocks until every tracked range is in a terminal
// state (StateDone) or ctx is canceled. Used by tests + by the
// operator-facing --apply path so the CLI can report "rebalance
// complete" with confidence.
func (c *Coordinator) WaitForIdle(ctx context.Context) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if c.idle() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Coordinator) idle() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.ranges {
		if !r.state.IsTerminal() {
			return false
		}
	}
	return true
}

// Snapshot returns the current per-range state for observability:
// shale rebalance --dry-run, shale topology, structured-log dumps.
// Sorted by partition id for stable output.
func (c *Coordinator) Snapshot() []RangeStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]RangeStatus, 0, len(c.ranges))
	for pid, r := range c.ranges {
		st := RangeStatus{
			PartitionID: pid,
			State:       r.state,
			From:        r.move.From,
			To:          r.move.To,
			KeyCount:    r.keyCount,
			StartedAt:   r.startedAt,
			HandedOffAt: r.handedOffAt,
		}
		if r.err != nil {
			st.Err = r.err.Error()
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartitionID < out[j].PartitionID })
	return out
}

// PartitionFunc exposes the Coordinator's partition function for
// callers (the rpc server's MigrateRange handler, integration
// tests) that need to ask "what partition does this key live in"
// against the same ring snapshot the Coordinator is using.
// Returns nil before the first Evaluate.
func (c *Coordinator) PartitionFunc() PartitionFn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.partFn
}
