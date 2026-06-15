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
// land before deciding. Migration-guard codes.ResourceExhausted from
// a replica that is mid-handoff is the canonical transient (v0.4
// distinguishes this from codes.Unavailable, which counts as failure).
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
// In v0.4 that's the migration-guard sentinel code (ResourceExhausted)
// returned by Put / Delete when the receiving node's partition is mid-
// handoff; the spec mandates these be treated as transient failures
// that don't count toward acks (and shouldn't count toward the failure
// budget either; another replica may still respond before the budget
// exhausts).
//
// codes.Unavailable is NOT classified as transient here. It is the
// canonical gRPC code for "the channel is gone" (server killed,
// connection refused, deadline canceled the call) -- a genuinely-down
// peer. Conflating the two breaks the failure-budget short-circuit:
// a real down peer would loop until every other replica also
// responded, instead of failing fast once (R - W + 1) such failures
// land. The migration guard therefore uses a less-common code
// (ResourceExhausted) so isTransientReplicaErr can distinguish "this
// replica is mid-handoff, try another" from "this replica is dead,
// count it as failure." See docs/SPEC.md "Cutover" for the rationale.
func isTransientReplicaErr(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.ResourceExhausted
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
	r := max(c.cfg.ReplicationFactor, 1)
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
//
// Each dispatch inherits a context.WithTimeout(WriteTimeout) deadline
// so a hung peer (blackhole, half-open TCP, server stuck before
// returning) cancels at deadline rather than blocking the call (and
// the surplus drainer goroutine) forever. Without this, the
// accounting goroutine never finalizes its sync.WaitGroup, the result
// channel never closes, and every Put against a hung peer
// permanently leaks two goroutines.
func (c *Cluster) putReplicated(key, value []byte) error {
	replicas := c.ring.LocateKeyN(c.shardKey(key), c.replicationFactor())
	if len(replicas) == 0 {
		return status.Error(codes.Unavailable, "shale: no replicas available for key")
	}
	stamp := Stamp{
		TimestampNanos: uint64(time.Now().UnixNano()),
		NodeID:         c.cfg.NodeID,
	}
	// Encode allocates its own buffer and copies env.Payload into it
	// (envelope.go: append(out, env.Payload...)); it never retains,
	// aliases, or mutates the input. The caller does not touch value
	// after this point. So an inner defensive copy here would be pure
	// waste (a second O(len) copy + alloc per Put). Pass value directly.
	envBytes := Encode(Envelope{Stamp: stamp, Payload: value})
	w := requiredWriteAcks(c.cfg.WriteConsistency, len(replicas))

	fanoutCtx, cancelFanout := context.WithTimeout(context.Background(), c.cfg.WriteTimeout)
	acks, errs, resultsCh := fanout(fanoutCtx, replicas, w,
		func(ctx context.Context, replica ring.Member) ([]byte, error) {
			return nil, c.dispatchReplicaPut(ctx, replica, key, envBytes)
		})

	// Drain surplus replicas in the background so the WaitGroup
	// finalizes + no goroutine leaks. The drainer also cancels the
	// fanout context once every replica reports (success, error, or
	// deadline) so any in-flight gRPC call that survived past the
	// decision point gets torn down rather than leaking.
	go func() {
		defer cancelFanout()
		// Drain the channel so the fanout goroutines can exit
		// cleanly; we have already collected the acks we need.
		//nolint:revive // empty-block: idiomatic channel drain.
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
// Returns the raw replica outcome; the migration-guard
// ResourceExhausted is surfaced verbatim so fanout's
// isTransientReplicaErr can classify it.
//
// dispatchReplicaPut is only reached at R>1 (putReplicated and
// scheduleReadRepair are the only callers; both are R>1-only), so
// envBytes is always an LWW Envelope. Both the local-self branch and
// the remote branch (peer's PutForwarded -> LocalReplicaPut) apply the
// envelope APPLY-IF-NEWER: a replica never overwrites a value it holds
// with one carrying an older-or-equal stamp. This is what makes a stale
// read-repair (which rides this path) a no-op instead of a clobber.
func (c *Cluster) dispatchReplicaPut(ctx context.Context, replica ring.Member, key, envBytes []byte) error {
	if replica.ID == c.cfg.NodeID {
		if rb := c.rebalance.Load(); rb != nil && (rb.IsMigrating(key) || rb.IsReceiving(key)) {
			return migrationGuardError(c.retryAfterMs())
		}
		return c.applyEnvelopeIfNewer(key, envBytes)
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
// Dispatch shape: we ALWAYS dispatch to all R replicas, not just the
// first N. That gives ReadNearest a fallback when the primary is
// down (a successor's response satisfies the n=1 target) and lets
// ReadQuorum tolerate up to (R - N) replica failures per the spec
// (docs/SPEC.md "Failure modes -> Replica down at Read time"). The
// fanout returns once N successful responses land OR the entire ring
// has reported. Surplus responses past N become read-repair feed.
//
// On Quorum / All, after the winner is determined any replica that
// returned NotFound OR an older Stamp is read-repaired asynchronously
// (best-effort, errors swallowed). Read-repair is skipped on Nearest
// since there is nothing to compare against.
//
// Each dispatch inherits a context.WithTimeout(ReadTimeout) deadline
// so a hung peer cancels its dispatched call instead of blocking the
// surplus drainer + accounting goroutines forever (same shape as
// putReplicated's WriteTimeout).
func (c *Cluster) getReplicated(key []byte) ([]byte, error) {
	allReplicas := c.ring.LocateKeyN(c.shardKey(key), c.replicationFactor())
	if len(allReplicas) == 0 {
		return nil, status.Error(codes.Unavailable, "shale: no replicas available for key")
	}
	rc := c.cfg.ReadConsistency
	n := min(requiredReadReplicas(rc, len(allReplicas)), len(allReplicas))
	// Dispatch to every replica, not just the first N. requiredAcks
	// stays at n so the fanout signals "decided" once we have enough
	// successful responses; surplus dispatches keep running so they
	// fill in as fallbacks if some of the first N never reply (down
	// primary, network drop, etc.). The failure-budget math
	// (allReplicas - n) gives us (R - N) tolerated transport
	// failures before the call gives up on collecting more.
	queried := allReplicas

	fanoutCtx, cancelFanout := context.WithTimeout(context.Background(), c.cfg.ReadTimeout)
	_, _, resultsCh := fanout(fanoutCtx, queried, n,
		func(ctx context.Context, replica ring.Member) ([]byte, error) {
			return c.dispatchReplicaGet(ctx, replica, key)
		})

	// Collect responses. Successes (including NotFound) up to N
	// satisfy the consistency target; anything beyond N is surplus
	// kept for read-repair classification. A transport / migration
	// error contributes to nonTransientErr (surfaced if NO replica
	// produces a usable response) but does NOT prevent us from
	// considering a later-arriving successor as one of the N.
	gathered := make([]collected, 0, len(queried))
	var nonTransientErr error
	usable := 0

	for res := range resultsCh {
		if res.Err != nil {
			if isTransientReplicaErr(res.Err) {
				// Skip transient; another replica may land.
				continue
			}
			if errors.Is(res.Err, backend.ErrNotFound) {
				if usable < n {
					gathered = append(gathered, collected{member: res.Member, env: Envelope{}, hadValue: false})
					usable++
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
		if usable < n {
			gathered = append(gathered, collected{member: res.Member, env: env, hadValue: true})
			usable++
		} else if rc != ReadNearest {
			// Past the consistency target: keep draining so read-
			// repair below sees every replica that responded after
			// we decided. fanout's resultsCh closes once every
			// dispatched op finishes.
			gathered = append(gathered, collected{member: res.Member, env: env, hadValue: true})
		}
	}
	cancelFanout()

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
// status error otherwise. Migration-guard ResourceExhausted is
// surfaced verbatim so the caller's classifier can skip it.
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
// strictly-older Stamp. Bounded at len(queried) goroutines (one per
// lagging replica); errors are swallowed.
//
// Lifecycle: each repair is registered against c.repairWG + uses
// c.repairCtx so Close cancels the context (the in-flight gRPC call
// returns) and waits for the goroutine to exit before tearing down
// the cached peerClient map. Without this, a repair mid-PutForwarded
// could race against c.clients teardown in Close, dialing through a
// torn-down client. If c.repairCtx is already cancelled (Close has
// already started), we drop the repair on the floor rather than
// spawning a doomed goroutine: read-repair is best-effort and the
// post-Close window has no consumer anyway.
func (c *Cluster) scheduleReadRepair(key []byte, winnerEnv Envelope, gathered []collected, _ []ring.Member) {
	if c.repairCtx == nil {
		return
	}
	if c.repairCtx.Err() != nil {
		return
	}
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
	for _, m := range laggers {
		c.repairWG.Go(func() {
			_ = c.dispatchReplicaPut(c.repairCtx, m, key, winnerBytes)
		})
	}
}

// LocalReplicaPut writes bytes directly to the local backend on
// behalf of a forwarded replica write. Used by rpc.Server.Put when
// Forwarded=true. At R>1 this node may be one of the R replicas
// (possibly NOT the primary - replication's whole point); the
// originator stamped + envelope-encoded the value once before the
// fan-out. At R=1 this is the same raw-bytes path the v0.3 forwarder
// used.
//
// At R>1 the incoming bytes are an LWW Envelope and are applied
// APPLY-IF-NEWER: persist only if the incoming stamp strictly beats the
// stored stamp (or there is no stored value). An older-or-equal
// forwarded write (e.g. a reordered fan-out or a stale read-repair that
// reached this node over the wire) becomes a no-op, so a replica never
// clobbers a value it already holds with a staler one. At R=1 there are
// no envelopes: the bytes are written verbatim, exactly as v0.3 did.
//
// The migration guard applies: if this node's partition is mid-
// handoff, returns the transient ResourceExhausted so the
// originator's fanout classifies it as transient + doesn't count it
// against the success or failure budget.
//
// Caller (rpc.Server.Put) is responsible for the OwnsReplica check;
// LocalReplicaPut trusts that gate.
func (c *Cluster) LocalReplicaPut(key, bytesToWrite []byte) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	if c.multi {
		// FREEZE gate (v0.8 multi-node reshard): a forwarded write that lands on
		// a frozen node is refused with the retryable error, same as a first-hop
		// write, so the originating node retries (and eventually re-forwards)
		// after RESUME. No forwarded write is acked during the static bisect.
		if c.isFrozen() {
			return errWriteFrozen("Put")
		}
		// Multi-backend mode at R>1 (replicated, v0.8 Phase 2b): the incoming
		// bytes are an LWW envelope; apply them APPLY-IF-NEWER against the
		// key's mounted unit so a reordered older fan-out or a stale read-
		// repair self-resolves to a never-clobber no-op. The replica set is
		// static, so the unit is mounted; an unmounted unit returns the
		// retryable acquiring-window error the originator's fan-out tolerates.
		if c.multiReplicated() {
			return c.applyEnvelopeIfNewerToUnit(key, bytesToWrite)
		}
		// Multi-backend mode (R=1): apply against the key's mounted unit,
		// resolved UNDER the reshard write-pause so a mid-flight cut-over
		// routes the write to the new gen-(g+1) child, not a retired unit
		// (Phase 4, NO ACKED WRITE LOST). The caller's OwnsKey guard already
		// confirmed this node owns the unit, so a missing mount means the
		// unit's lease is HANDING OFF to this node (Phase 3 window): return
		// the retryable acquiring-window error so the forwarder retries once
		// the reconcile has acquired, rather than re-forwarding or losing the
		// write.
		b, gu, unlock, ok := c.localWriteBackendForKey(key)
		defer unlock()
		if !ok {
			return errUnitAcquiring("Put")
		}
		if err := b.Put(key, bytesToWrite); err != nil {
			// Stale mount (lease moved during the reshard redistribution):
			// evict + retryable so the forwarder retries and lands on the
			// freshly re-acquired mount (never ack a write that did not land).
			c.evictStaleMount(gu, b)
			return errUnitAcquiring("Put")
		}
		return nil
	}
	if rb := c.rebalance.Load(); rb != nil && (rb.IsMigrating(key) || rb.IsReceiving(key)) {
		return migrationGuardError(c.retryAfterMs())
	}
	if c.casReplicated() {
		// R>1: envelope bytes, LWW-on-write.
		return c.applyEnvelopeIfNewer(key, bytesToWrite)
	}
	// R=1: raw bytes, no envelope, no LWW (unchanged v0.3 path).
	return c.backend.Put(key, bytesToWrite)
}

// LocalReplicaDelete clears key from the local backend on behalf of a
// forwarded Delete. At R>1 Delete is normally routed through Put with
// a tombstone envelope; this path covers the R=1 forwarded-Delete
// shape that mirrors v0.3 behavior.
func (c *Cluster) LocalReplicaDelete(key []byte) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	if c.multi {
		// FREEZE gate (v0.8 multi-node reshard): a forwarded delete on a frozen
		// node is refused with the retryable error, same as a forwarded Put.
		if c.isFrozen() {
			return errWriteFrozen("Delete")
		}
		// Resolve under the reshard write-pause (Phase 4). Owner-but-unmounted:
		// handoff landing on us. Retryable acquiring-window error (never lose
		// the forwarded delete).
		b, gu, unlock, ok := c.localWriteBackendForKey(key)
		defer unlock()
		if !ok {
			return errUnitAcquiring("Delete")
		}
		if err := b.Delete(key); err != nil {
			// Stale mount (lease moved): evict + retryable, same as Put.
			c.evictStaleMount(gu, b)
			return errUnitAcquiring("Delete")
		}
		return nil
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
//
// In multi-backend mode (v0.8 Phase 3, R=1) "replica" collapses to the
// single unit owner, so the guard is the unit-RING-ownership check OwnsKey
// uses (NOT the mount): a forwarded Put / Delete that lands on the ring
// owner of the key's unit passes even during the handoff window before the
// unit is mounted, so the local apply path can return the retryable
// acquiring-window error instead of the originator looping on a
// refresh-ring refusal. See OwnsKey for the full rationale.
func (c *Cluster) OwnsReplica(key []byte) bool {
	if c.multiReplicated() {
		// R>1 multi-backend (v0.8 Phase 2b): a forwarded replica write may land
		// on ANY of the unit's R replica nodes, not just the primary. Accept if
		// this node is anywhere in the unit's replica set.
		for _, m := range c.replicasForKey(key) {
			if m.ID == c.cfg.NodeID {
				return true
			}
		}
		return false
	}
	if c.multi {
		_, isLocal := c.unitOwnerOf(key)
		return isLocal
	}
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
