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
}

// DefaultOptions returns the spec-aligned tunables. Callers that
// want to override a single field should still start here:
//
//	opts := rebalance.DefaultOptions()
//	opts.GraceDuration = 5 * time.Second   // test
//	c := rebalance.New(self, be, opts)
func DefaultOptions() Options {
	return Options{
		SettleDelay:   5 * time.Second,
		GraceDuration: 60 * time.Second,
		ChunkSize:     64,
		RetryAfterMs:  50,
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

	mu     sync.Mutex
	ranges map[uint64]*rangeRecord
	gen    uint64
}

// New constructs a Coordinator. If opts.Source is nil, a default
// localSource is wired against the provided backend the first time
// Evaluate runs (we wait so the partition function can come from
// the same Ring the plan was computed against). The Coordinator
// does not start the background sweep automatically; call
// (*Coordinator).RunSweep(ctx) from the cluster layer to enable it.
func New(self Member, local backend.Backend, opts Options) *Coordinator {
	c := &Coordinator{
		self:   self,
		local:  local,
		opts:   opts,
		source: opts.Source,
		dest:   opts.Destination,
		stopCh: make(chan struct{}),
		ranges: make(map[uint64]*rangeRecord),
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

// runSend drains the local source for the move's partition. v0.3
// has the destination dial in to pull, not the source push, but at
// the Coordinator level we still need to mark the source-side
// progress: count the keys we would stream, flip Sending ->
// HandedOff once the destination is known to have completed. The
// cluster integration wires the actual stream-served bytes back
// here via FinishSend.
//
// In tests + in the in-process destination, the destination uses
// the same MigrateSource interface, so by the time the destination
// returns from FetchRange the source's scan has also drained. We
// model that by counting the source-side keys here (cheap with the
// memory backend, sized work with slate) and flipping straight to
// HandedOff. The grace timer is the sweep's responsibility.
func (c *Coordinator) runSend(m Move, source MigrateSource) {
	// Count what we would have shipped so RangeStatus.KeyCount is
	// meaningful. The destination's FetchRange call against the
	// same MigrateSource will trigger an independent scan in the
	// in-process test; that's intentional (the scan is read-only).
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

// IsMigrating reports whether key's partition is in StateSending or
// StateHandedOff on THIS node. The cluster layer uses this to:
//
//   - reject writes for the key (source side)
//   - hint try-other-owner on reads (destination side)
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
