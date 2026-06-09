// Replication fan-out (v0.4+).
//
// When ReplicationFactor > 1, Put / Delete fan out to R replicas in
// parallel and Get fetches from N replicas in parallel per the read
// consistency. The fanout helper here is the shared machinery: it
// dispatches op to each replica concurrently, collects acks + errors
// as they arrive, and returns once either requiredAcks succeed or
// enough failures have accumulated that requiredAcks is unreachable.
//
// Surplus successful ops above requiredAcks are NOT cancelled per
// docs/SPEC.md "Fan-out + ack accounting": they continue in the
// background so the eventual replica state matches the consistency
// setting's intent. Failures contribute to the per-call failure
// budget; once (len(replicas) - requiredAcks + 1) failures land,
// fanout returns the accumulated errors so the caller can map them to
// codes.Unavailable.
//
// The op callback is given the replica Member and a context that the
// caller can pass into a backend / gRPC call. The context is the one
// fanout was invoked with (the caller owns the timeout / cancellation
// budget; we don't impose one). Returning a nil error means "ack";
// returning any error means "failure" and counts against the budget.

package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/ring"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// replicaResult is the per-replica outcome forwarded onto the channel
// fanout returns. Used by Get's collector path so successful replies
// (envelope bytes) can be pulled out alongside the ack accounting
// fanout does.
type replicaResult struct {
	Member ring.Member
	Err    error
	// Value carries the per-replica returned bytes for Get; nil for
	// Put / Delete callbacks that have nothing to return. NotFound is
	// signaled by Err == backend.ErrNotFound; the caller's op callback
	// is responsible for translating gRPC NotFound into that error.
	Value []byte
}

// fanout dispatches op against every replica in parallel, waits for a
// decision (success or failure budget exhausted), and returns the
// (acks, errs, resultsCh) triple. The resultsCh produces one
// replicaResult per dispatched op (including surplus + transient), in
// arrival order, and is closed once every op has reported.
//
// Decision rule:
//   - acks >= requiredAcks (success), OR
//   - non-transient errs > (len(replicas) - requiredAcks)
//     i.e. requiredAcks acks are no longer reachable, OR
//   - every dispatched op has reported (we ran out of replicas).
//
// Transient errors (per isTransientReplicaErr) count neither as ack
// nor as failure budget; they flow through the channel for caller
// visibility but the accounting goroutine waits for more replicas to
// land before deciding. Migration-guard codes.Unavailable from a
// replica that is mid-handoff is the canonical transient.
func fanout(
	ctx context.Context,
	replicas []ring.Member,
	requiredAcks int,
	op func(ctx context.Context, replica ring.Member) ([]byte, error),
) (int, []error, <-chan replicaResult) {
	n := len(replicas)
	if n == 0 {
		empty := make(chan replicaResult)
		close(empty)
		return 0, nil, empty
	}
	if requiredAcks < 1 {
		requiredAcks = 1
	}
	if requiredAcks > n {
		requiredAcks = n
	}

	resultsCh := make(chan replicaResult, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for _, r := range replicas {
		r := r
		go func() {
			defer wg.Done()
			v, err := op(ctx, r)
			resultsCh <- replicaResult{Member: r, Err: err, Value: v}
		}()
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Tee resultsCh onto the caller's channel + drive the accounting
	// state behind it. The caller may read outCh at any pace (or not
	// at all if it doesn't care about read-repair); the accounting
	// goroutine signals "decided" on a separate channel so this call
	// can return as soon as the rule above fires.
	outCh := make(chan replicaResult, n)
	decided := make(chan struct{})
	var (
		mu        sync.Mutex
		acks      int
		errs      []error
		isDecided bool
	)
	markDecided := func() {
		if !isDecided {
			isDecided = true
			close(decided)
		}
	}
	go func() {
		defer close(outCh)
		for res := range resultsCh {
			outCh <- res
			mu.Lock()
			if res.Err == nil {
				acks++
			} else if !isTransientReplicaErr(res.Err) {
				errs = append(errs, res.Err)
			}
			if acks >= requiredAcks || len(errs) > n-requiredAcks {
				markDecided()
			}
			mu.Unlock()
		}
		// All replicas reported; if we never hit a rule above (e.g.
		// every replica returned transient), signal decided with
		// whatever we have so the caller proceeds.
		mu.Lock()
		markDecided()
		mu.Unlock()
	}()

	select {
	case <-decided:
	case <-ctx.Done():
		// Caller's context died before any decision; report what we
		// have. The accounting goroutine keeps draining resultsCh in
		// the background so we don't leak.
	}

	mu.Lock()
	a := acks
	e := append([]error(nil), errs...)
	mu.Unlock()
	return a, e, outCh
}

// isTransientReplicaErr reports whether err is the kind of replica
// outcome that should not count toward the success or failure budget.
// In v0.4 that's the migration-guard codes.Unavailable returned by
// Put / Delete when the receiving node's partition is mid-handoff;
// the spec mandates these be treated as transient failures that don't
// count toward acks (and shouldn't count toward the failure budget
// either; another replica may still respond before the budget exhausts).
func isTransientReplicaErr(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.Unavailable
	}
	return false
}

// requiredWriteAcks returns the number of acks fanout must observe to
// satisfy the configured WriteConsistency over R replicas.
//
//	WriteOne    -> 1
//	WriteQuorum -> floor(R/2) + 1
//	WriteAll    -> R
func requiredWriteAcks(wc WriteConsistency, r int) int {
	switch wc {
	case WriteOne:
		return 1
	case WriteAll:
		return r
	case WriteQuorum:
		fallthrough
	default:
		return r/2 + 1
	}
}

// requiredReadReplicas returns the number of replicas a Get should
// query to satisfy the configured ReadConsistency over R replicas.
//
//	ReadNearest -> 1
//	ReadQuorum  -> floor(R/2) + 1
//	ReadAll     -> R
func requiredReadReplicas(rc ReadConsistency, r int) int {
	switch rc {
	case ReadAll:
		return r
	case ReadQuorum:
		return r/2 + 1
	case ReadNearest:
		fallthrough
	default:
		return 1
	}
}

// replicationFactor returns the normalized R for this cluster. R=1 is
// the v0.3 behavior (single owner, no replicas, no envelope cost).
// Clamping against the live ring size happens at fan-out time inside
// ring.LocateKeyN, not here.
func (c *Cluster) replicationFactor() int {
	r := c.cfg.ReplicationFactor
	if r < 1 {
		r = 1
	}
	return r
}

// putReplicated stamps + envelopes + fans out a write to the R
// replicas LocateKeyN picks for key. value == nil is the Delete /
// tombstone shape: an empty-payload envelope still carrying a Stamp.
//
// Returns codes.Unavailable when fewer than W acks land (W per
// WriteConsistency). Migration-guard rejections from individual
// replicas are transient + don't count toward the success or failure
// budget per docs/SPEC.md "Fan-out + ack accounting".
func (c *Cluster) putReplicated(key, value []byte) error {
	replicas := c.ring.LocateKeyN(c.shardKey(key), c.replicationFactor())
	if len(replicas) == 0 {
		return status.Error(codes.Unavailable, "shale: no replicas available for key")
	}
	stamp := Stamp{
		TimestampNanos: uint64(time.Now().UnixNano()),
		WriterNodeID:   c.cfg.NodeID,
	}
	envBytes := Encode(Envelope{Stamp: stamp, Payload: append([]byte(nil), value...)})
	w := requiredWriteAcks(c.cfg.WriteConsistency, len(replicas))

	acks, errs, resultsCh := fanout(context.Background(), replicas, w,
		func(ctx context.Context, replica ring.Member) ([]byte, error) {
			return nil, c.dispatchReplicaPut(ctx, replica, key, envBytes)
		})

	// Drain surplus replicas in the background so the WaitGroup
	// finalizes + no goroutine leaks. We don't care about their
	// outcomes (v0.4 has no hinted handoff; lagging replicas catch
	// up via the next read-repair or future anti-entropy).
	go func() {
		for range resultsCh {
		}
	}()

	if acks < w {
		return status.Errorf(codes.Unavailable,
			"shale: write needed %d acks, got %d (%d failures: %v)",
			w, acks, len(errs), firstErr(errs))
	}
	return nil
}

// dispatchReplicaPut routes one replica's write to either the local
// backend (with the migration guard) or a peer's gRPC ReplicaPut.
// Returns the raw replica outcome; the migration-guard Unavailable is
// surfaced verbatim so fanout's isTransientReplicaErr can classify it.
func (c *Cluster) dispatchReplicaPut(ctx context.Context, replica ring.Member, key, envBytes []byte) error {
	if replica.ID == c.cfg.NodeID {
		if rb := c.rebalance.Load(); rb != nil && (rb.IsMigrating(key) || rb.IsReceiving(key)) {
			return migrationGuardError(c.retryAfterMs())
		}
		return c.backend.Put(key, envBytes)
	}
	cli, err := c.clientFor(replica.Addr)
	if err != nil {
		return err
	}
	return cli.PutForwarded(ctx, key, envBytes)
}

// getReplicated fetches the LWW winner across N replicas per
// ReadConsistency, returning the winner's payload (or
// backend.ErrNotFound if the winner is a tombstone / no replica had
// the key).
//
// On Quorum / All, after the winner is determined any replica that
// returned NotFound OR an older Stamp is read-repaired asynchronously
// (best-effort, errors swallowed). Read-repair is skipped on Nearest
// since there is nothing to compare against.
func (c *Cluster) getReplicated(key []byte) ([]byte, error) {
	allReplicas := c.ring.LocateKeyN(c.shardKey(key), c.replicationFactor())
	if len(allReplicas) == 0 {
		return nil, status.Error(codes.Unavailable, "shale: no replicas available for key")
	}
	rc := c.cfg.ReadConsistency
	n := requiredReadReplicas(rc, len(allReplicas))
	if n > len(allReplicas) {
		n = len(allReplicas)
	}
	queried := allReplicas[:n]

	// requiredAcks for the read is "enough replicas to satisfy the
	// consistency" which equals n itself: we want N responses (a
	// response is success OR NotFound, both are first-class outcomes
	// for LWW; only transport / migration errors are failures).
	// Failure budget = (queried - n) = 0, so any non-transient error
	// will trip the unreachable check. We forgive that by letting the
	// caller still pick a winner from whatever DID land - read paths
	// degrade more gracefully than writes since the surviving
	// replicas can still serve consistent data.
	_, _, resultsCh := fanout(context.Background(), queried, n,
		func(ctx context.Context, replica ring.Member) ([]byte, error) {
			return c.dispatchReplicaGet(ctx, replica, key)
		})

	// Collect first N usable responses (success or NotFound). For
	// Nearest (n=1) this is one envelope; for Quorum / All it's the
	// full set we asked for. Anything beyond N is surplus we read
	// only for read-repair.
	gathered := make([]collected, 0, n)
	var nonTransientErr error

	for res := range resultsCh {
		if res.Err != nil {
			if isTransientReplicaErr(res.Err) {
				// Skip transient; another replica may land.
				continue
			}
			if errors.Is(res.Err, backend.ErrNotFound) {
				if len(gathered) < n {
					gathered = append(gathered, collected{member: res.Member, env: Envelope{}, hadValue: false})
				}
				continue
			}
			if nonTransientErr == nil {
				nonTransientErr = res.Err
			}
			continue
		}
		env, err := Decode(res.Value)
		if err != nil {
			// Corrupt envelope: treat as a replica failure but keep
			// trying others.
			if nonTransientErr == nil {
				nonTransientErr = err
			}
			continue
		}
		if len(gathered) < n {
			gathered = append(gathered, collected{member: res.Member, env: env, hadValue: true})
		} else if rc != ReadNearest {
			// Past the consistency target: keep draining so read-
			// repair below sees every replica that responded after
			// we decided. fanout's resultsCh closes once every
			// dispatched op finishes.
		}
	}

	if len(gathered) == 0 {
		if nonTransientErr != nil {
			return nil, nonTransientErr
		}
		return nil, backend.ErrNotFound
	}

	// LWW winner: highest Stamp.Greater wins. NotFound participates
	// as a zero-Stamp envelope (loses every comparison against a real
	// write, matching v0.3 compat behavior).
	winner := gathered[0]
	for _, g := range gathered[1:] {
		if g.hadValue && (!winner.hadValue || g.env.Stamp.Greater(winner.env.Stamp)) {
			winner = g
		}
	}

	// Read-repair: on Quorum / All, push the winning envelope back to
	// any replica that returned NotFound OR a strictly-older Stamp.
	// Best-effort: errors swallowed, no retry. Skipped on Nearest
	// (only one replica queried; nothing to compare).
	if rc != ReadNearest && winner.hadValue {
		c.scheduleReadRepair(key, winner.env, gathered, queried)
	}

	if !winner.hadValue {
		return nil, backend.ErrNotFound
	}
	if len(winner.env.Payload) == 0 {
		// Tombstone wins: surface as NotFound.
		return nil, backend.ErrNotFound
	}
	return winner.env.Payload, nil
}

// dispatchReplicaGet routes one replica's read. Returns the raw
// envelope bytes (which the caller Decodes) on success, the canonical
// backend.ErrNotFound when the replica has no entry, or the gRPC
// status error otherwise. Migration-guard Unavailable is surfaced
// verbatim so the caller's classifier can skip it.
func (c *Cluster) dispatchReplicaGet(ctx context.Context, replica ring.Member, key []byte) ([]byte, error) {
	if replica.ID == c.cfg.NodeID {
		v, err := c.backend.Get(key)
		if err != nil {
			return nil, err
		}
		return v, nil
	}
	cli, err := c.clientFor(replica.Addr)
	if err != nil {
		return nil, err
	}
	v, found, err := cli.GetForwarded(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, backend.ErrNotFound
	}
	return v, nil
}

// collected is one replica's contribution to the Get fan-out, ready
// for LWW comparison + read-repair classification. hadValue is false
// when the replica explicitly returned NotFound (which itself can win
// LWW if no replica had a stamped value).
type collected struct {
	member   ring.Member
	env      Envelope
	hadValue bool
}

// scheduleReadRepair fires a best-effort async push of the winning
// envelope to every queried replica that returned NotFound or a
// strictly-older Stamp. Capped at len(queried) goroutines (one per
// lagging replica); errors are swallowed.
func (c *Cluster) scheduleReadRepair(key []byte, winnerEnv Envelope, gathered []collected, queried []ring.Member) {
	winnerBytes := Encode(winnerEnv)
	// Build a quick lookup: which gathered replicas saw an older or
	// missing value. Members we queried but never heard back from
	// (transient skip) are NOT repaired here - they'll catch up on
	// the next quorum read or future anti-entropy.
	laggers := make([]ring.Member, 0, len(gathered))
	for _, g := range gathered {
		if !g.hadValue {
			laggers = append(laggers, g.member)
			continue
		}
		if winnerEnv.Stamp.Greater(g.env.Stamp) {
			laggers = append(laggers, g.member)
		}
	}
	if len(laggers) == 0 {
		return
	}
	// Detached context: read-repair MUST outlive the caller's Get
	// (which has already returned by the time these fire), and we
	// don't want a caller cancel to kill the repair mid-flight.
	for _, m := range laggers {
		m := m
		go func() {
			_ = c.dispatchReplicaPut(context.Background(), m, key, winnerBytes)
		}()
	}
}

// LocalReplicaPut writes bytes directly to the local backend on
// behalf of a forwarded replica write. Used by rpc.Server.Put when
// Forwarded=true. At R>1 this node may be one of the R replicas
// (possibly NOT the primary - replication's whole point); the
// originator stamped + envelope-encoded the value once before the
// fan-out, so we just persist what arrived. At R=1 this is the same
// raw-bytes path the v0.3 forwarder used.
//
// The migration guard applies: if this node's partition is mid-
// handoff, returns the transient Unavailable so the originator's
// fanout classifies it as transient + doesn't count it against the
// success or failure budget.
//
// Caller (rpc.Server.Put) is responsible for the OwnsReplica check;
// LocalReplicaPut trusts that gate.
func (c *Cluster) LocalReplicaPut(key, bytesToWrite []byte) error {
	if c.closed.Load() || c.backend == nil {
		return backend.ErrClosed
	}
	if rb := c.rebalance.Load(); rb != nil && (rb.IsMigrating(key) || rb.IsReceiving(key)) {
		return migrationGuardError(c.retryAfterMs())
	}
	return c.backend.Put(key, bytesToWrite)
}

// LocalReplicaDelete clears key from the local backend on behalf of a
// forwarded Delete. At R>1 Delete is normally routed through Put with
// a tombstone envelope; this path covers the R=1 forwarded-Delete
// shape that mirrors v0.3 behavior.
func (c *Cluster) LocalReplicaDelete(key []byte) error {
	if c.closed.Load() || c.backend == nil {
		return backend.ErrClosed
	}
	if rb := c.rebalance.Load(); rb != nil && (rb.IsMigrating(key) || rb.IsReceiving(key)) {
		return migrationGuardError(c.retryAfterMs())
	}
	return c.backend.Delete(key)
}

// OwnsReplica reports whether the local node is one of the R replica
// owners of key per LocateKeyN. Exported for rpc.Server's forwarding
// loop-guard: with R>1, a forwarded Put may land on any of the R
// replicas, not just the primary. The classic OwnsKey check (primary
// only) would reject legitimate replica writes.
//
// At R=1, OwnsReplica == OwnsKey (LocateKeyN with n=1 == LocateKey).
func (c *Cluster) OwnsReplica(key []byte) bool {
	return c.ownsAsReplica(key)
}

// ownsAsReplica is the lower-case implementation. Returns true if
// the local node is among LocateKeyN's returned members for the key.
// Falls back to OwnsKey semantics (primary check) in single-node mode
// where there is no ring.
func (c *Cluster) ownsAsReplica(key []byte) bool {
	if c.ring == nil || c.ring.Empty() {
		return true
	}
	for _, m := range c.ring.LocateKeyN(c.shardKey(key), c.replicationFactor()) {
		if m.ID == c.cfg.NodeID {
			return true
		}
	}
	return false
}

// firstErr returns the first non-nil error or nil if errs is empty.
// Used in the Unavailable wrap so the operator sees one representative
// failure cause instead of "0 acks, no diagnostic".
func firstErr(errs []error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
