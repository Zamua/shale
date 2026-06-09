// Replication fan-out (v0.4+).
//
// When ReplicationFactor > 1, Put / Delete fan out to R replicas in
// parallel. The fanout helper here is the shared machinery: it
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
