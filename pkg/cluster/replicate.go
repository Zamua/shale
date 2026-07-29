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

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/coord"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// replicaResult is the per-replica outcome forwarded onto the channel
// fanout returns. Used by Get's collector path so successful replies
// (envelope bytes) can be pulled out alongside the ack accounting
// fanout does.
type replicaResult struct {
	Member coord.Node
	Err    error
	// Value carries the per-replica returned bytes for Get; nil for
	// Put / Delete callbacks that have nothing to return. NotFound is
	// signaled by Err == backend.ErrNotFound; the caller's op callback
	// is responsible for translating gRPC NotFound into that error.
	Value []byte
}

// fanout dispatches op against every replica in parallel, waits for a
// decision (success or failure budget exhausted), and returns the
// (acks, errs, transient, resultsCh) tuple. The resultsCh produces one
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
// op receives idx, the position of replica within the replicas slice, so a
// caller that routes the SAME member to more than one target (the pending-ranges
// union routes a survivor that shifted replica indices to BOTH the position it is
// draining and the position it acquired) can recover which target this goroutine
// is for - a member-keyed lookup would collapse the duplicates onto one position.
// fanout also returns the TRANSIENT leg errors it kept out of errs. They are
// not budget input (that is exactly what makes them transient) - they are
// EVIDENCE, so a caller that has to collapse the pass into one terminal error
// can report WHY W was missed (a mid-acquire replica vs a migration guard)
// instead of inferring it from which branch it landed in. Discarding them here
// is what made the R>1 acquiring shortfall unattributable to its own callers.
func fanout(
	ctx context.Context,
	replicas []coord.Node,
	requiredAcks int,
	op func(ctx context.Context, idx int, replica coord.Node) ([]byte, error),
) (int, []error, []error, <-chan replicaResult) {
	n := len(replicas)
	if n == 0 {
		empty := make(chan replicaResult)
		close(empty)
		return 0, nil, nil, empty
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
	for i, r := range replicas {
		go func() {
			defer wg.Done()
			v, err := op(ctx, i, r)
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
		transient []error
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
			} else if isTransientReplicaErr(res.Err) {
				// Neither ack nor failure budget; retained only as evidence
				// (see the doc comment) so a collapsed terminal can name the
				// reason the legs actually carried.
				transient = append(transient, res.Err)
			} else {
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
	tr := append([]error(nil), transient...)
	mu.Unlock()
	return a, e, tr, outCh
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
//
// v0.8 Phase 2d: the multi-backend acquiring-window refusal
// (errUnitAcquiring) travels client-facing as codes.Unavailable but is
// tagged with errAcquiringSentinel. On the IN-PROCESS local-self fan-out
// leg the sentinel survives, so this classifier skips it (isAcquiringErr).
// On the cross-node forwarded leg the sentinel cannot cross gRPC, so that
// leg is re-coded to codes.ResourceExhausted (recodeForwardedReplicaErr)
// and skipped by the ResourceExhausted branch. Either way a mid-acquire
// replica no longer consumes the failure budget. See docs/SPEC.md
// "v0.8 Phase 2d".
// isTransientReadLegErr is the READ-leg HANDOFF-CLASS transient classification
// (docs/SPEC.md "Union reads" guard 2): everything isTransientReplicaErr
// skips, PLUS a CLOSED-MID-RELEASE mount. Handoff-class evidence earns the
// FULL-budget re-poll; the weaker unreachable-member class is classified
// separately (isUnreachableLegErr) and earns only the capped re-poll. A read leg that resolves a local handle in the
// same instant ordered removal's release closes it reads backend.ErrClosed;
// a leaving node's own shutdown surfaces the same to a forwarded leg, crossing
// gRPC as the bare codes.Unknown wrapping of the error text (gRPC wraps
// non-status errors that way, so the wire form is matched by message). On a
// union leg either form means "this copy just moved on" - another leg or the
// next re-poll serves - so it must count as neither ack nor failure and must
// be re-polled by the read retry, not surfaced. READ legs only: the write
// fan-out's accounting is unchanged, and a client op entering a genuinely
// closed cluster still gets backend.ErrClosed from the entry not-ready check.
func isTransientReadLegErr(err error) bool {
	if isTransientReplicaErr(err) {
		return true
	}
	if errors.Is(err, backend.ErrClosed) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		// EXACT-message match for the wire form: gRPC wraps the receiver's raw
		// non-status error verbatim, so the message is exactly ErrClosed's text.
		// A wrapped error merely EMBEDDING the text is some other failure and
		// must stay hard (a substring match would silently absorb it).
		if st.Code() == codes.Unknown && st.Message() == backend.ErrClosed.Error() {
			return true
		}
	}
	return false
}

// isUnreachableLegErr classifies a read leg's bare codes.Unavailable (a
// refused dial / transport failure - a just-departed member's dead address,
// or a receiver dying mid-request). Skipped within a sweep like the handoff
// classes, but weaker evidence of a transition: a sweep whose ONLY transient
// evidence is unreachable legs earns the CAPPED re-poll (docs/SPEC.md guard
// 2), so a genuinely all-down replica set surfaces its dial error after a
// sub-second capped re-poll instead of stalling to the full read budget.
// Checked AFTER isTransientReadLegErr at every call site, so an
// Unavailable-coded handoff refusal (the in-process acquiring sentinel)
// classifies as handoff, never as unreachable.
func isUnreachableLegErr(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable
}

// unreachableOnlyError marks a sweep whose only transient evidence was
// unreachable legs, carrying the FIRST dial error VERBATIM (its text is the
// diagnostic the operator needs). Internal to the read retry: the wrapper
// counts these sweeps against the cap and always returns the INNER error to
// callers, never the marker.
type unreachableOnlyError struct{ inner error }

func (e *unreachableOnlyError) Error() string { return e.inner.Error() }
func (e *unreachableOnlyError) Unwrap() error { return e.inner }

func isTransientReplicaErr(err error) bool {
	if err == nil {
		return false
	}
	if isAcquiringErr(err) {
		return true
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

// collected is one replica's contribution to the Get fan-out, ready
// for LWW comparison + read-repair classification. hadValue is false
// when the replica explicitly returned NotFound (which itself can win
// LWW if no replica had a stamped value).
type collected struct {
	member   coord.Node
	env      Envelope
	hadValue bool
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
			// FORWARDED replica leg (Phase 2d): re-code a local acquiring
			// refusal to the transient codes.ResourceExhausted before it
			// crosses gRPC back to the originator, since errAcquiringSentinel
			// cannot survive the wire. The originator's isTransientReplicaErr
			// then skips it instead of counting it as a down-peer failure.
			return recodeForwardedReplicaErr(c.applyEnvelopeIfNewerToUnit(key, bytesToWrite))
		}
		// Multi-backend mode (R=1): apply against the key's mounted unit,
		// resolved UNDER the reshard write-pause so a mid-flight cut-over
		// routes the write to the new gen-(g+1) child, not a retired unit
		// (Phase 4, NO ACKED WRITE LOST). The caller's OwnsKey guard already
		// confirmed this node owns the unit, so a missing mount means the
		// unit's lease is HANDING OFF to this node (Phase 3 window): return
		// the retryable acquiring-window error so the forwarder retries once
		// the reconcile has acquired, rather than re-forwarding or losing the
		// write. A failed write means the mount went stale mid-redistribution
		// and yields the same retryable error, so the forwarder retries and
		// lands on the freshly re-acquired mount (never ack a write that did
		// not land). See withLocalWriteBackend.
		return c.withLocalWriteBackend(key, "Put", func(b backend.Backend) error {
			return b.Put(key, bytesToWrite)
		})
	}
	// Single-node: the forwarded-write path collapses to a local write.
	// There is no ring, so there is no other replica to reconcile with
	// and no envelope to unwrap.
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
		// Apply under the reshard write-pause (Phase 4). Owner-but-unmounted
		// (handoff landing on us) and a stale mount (lease moved) both give
		// the retryable acquiring-window error, same as Put, so the forwarded
		// delete is never lost.
		return c.withLocalWriteBackend(key, "Delete", func(b backend.Backend) error {
			return b.Delete(key)
		})
	}
	// Single-node: no ring, so the forwarded delete is a local delete.
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
		// this node is anywhere in the unit's ROUTED replica set. v0.8 Phase 2e
		// (pending ranges): the routed set is the UNION of current + pending owners
		// during a transition, so a PENDING owner receiving a union dual-write is
		// accepted here (it is a legitimate routed replica) and the loop-guard does
		// NOT refuse it. In steady state routed == the stable replica set, so this
		// is unchanged.
		routed, _ := c.routedReplicasForKey(key)
		for _, m := range routed {
			if string(m.ID) == c.cfg.NodeID {
				return true
			}
		}
		return false
	}
	if c.multi {
		_, isLocal := c.unitOwnerOf(key)
		return isLocal
	}
	// Single-node: no ring, so this node owns every key unconditionally.
	return true
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
