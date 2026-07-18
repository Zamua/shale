// Refusal REASONS: the machine-readable classification of WHY a cluster op was
// refused, carried uniformly across the in-process dispatch path AND the gRPC
// peer-forwarding path.
//
// WHY THIS EXISTS. shale is embedded IN-PROCESS by library consumers (open a
// Cluster, then call Transact / Get / ScanPrefix / Aggregate). Routing and peer
// forwarding both happen INSIDE the cluster, BELOW that seam, so a refusal
// raised by a locally-mounted position and one forwarded from a peer come back
// through the SAME call site. Without a reason the consumer has nothing to
// discriminate on: the gRPC status code alone is far too coarse (an
// acquiring-window refusal and a genuinely-down peer are both
// codes.Unavailable), and matching on the message string is not a contract.
// A consumer that wants to retry ONLY the transient handoff blip, and NOT a
// real outage, therefore cannot be written. The reason closes that gap.
//
// THE SHAPE. Each reason has exactly one EXPORTED sentinel that consumers match
// with errors.Is. On the wire the reason rides as a google.rpc.ErrorInfo status
// detail (a stable machine-readable Reason string under a shale-owned domain),
// attached where the refusal is serialized and decoded by the peer client, so
// the decoded error wraps the SAME sentinel the local path returns. Both halves
// read the ONE reasonSentinels table below, so a reason cannot be encodable but
// undecodable.
//
// FIRST SLICE, NOT A ONE-OFF. Only ReasonAcquiring is implemented. Siblings
// (fenced, frozen, migration-guard, conflict) join by adding a constant, an
// exported sentinel, and one table row - no redesign, and no change to the
// encode/decode plumbing.
//
// TWO PLACES A REASON CAN BE LOST, and both are covered. The first is the NODE
// BOUNDARY, handled by the encode/decode halves below. The second is a FAN-OUT
// COLLAPSE: at R>1 a write is many legs summarized into one error, and minting
// that terminal fresh drops whatever the legs carried. Terminals built with
// reasonTerminal inherit the legs' reason instead (see classifyWriteAttempt), so
// the contract holds at R=1 and R>1 alike. A consumer's match must not depend on
// its replication factor.
//
// SCOPE. This contract covers the IN-PROCESS Cluster surface (the seam a library
// consumer embeds: Get / Put / Delete / ScanPrefix / Transact) and every
// cluster-internal peer RPC underneath it, because the decode interceptors are
// installed in peerDialOptions, which is the only dial site pkg/cluster uses. It
// does NOT cover pkg/rpc.Client, the out-of-process CLI client: that dials with
// plain credentials and installs no interceptors, so a refusal it receives keeps
// its code and message but arrives WITHOUT the sentinel. Wiring the same
// interceptors there is the change to make if that client ever needs to gate on
// a reason; until then, do not assume errors.Is works across it.
//
// WHAT THE REASON DOES NOT CHANGE. The gRPC status CODE is untouched: an
// acquiring refusal still travels client-facing as codes.Unavailable, because
// the existing retry and forwarding shapes key off it. The reason is strictly
// ADDITIVE identity layered on top of the code. Likewise, decoding attaches the
// exported sentinel ONLY - never the package-private classifier tags - so every
// internal classifier (isTransientReplicaErr, isAcquiringErr, isUnreachableLegErr)
// sees exactly what it saw before.

package cluster

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RefusalReason is the stable, machine-readable classification of a cluster
// refusal. The string values are part of the WIRE contract (they travel in the
// ErrorInfo detail), so they may be added to but never renamed.
type RefusalReason string

const (
	// ReasonAcquiring marks the handoff-window refusal: the addressed node IS
	// the ring owner of the key's position but has not finished mounting it.
	// Surfaces as ErrAcquiring.
	ReasonAcquiring RefusalReason = "ACQUIRING"
)

// reasonDomain namespaces the ErrorInfo details shale attaches, so a Reason
// string can never be confused with one minted by another service in a shared
// gRPC deployment. Part of the wire contract; do not change it.
const reasonDomain = "shale.dev"

// ErrAcquiring reports that a cluster op was refused because the position that
// serves the key is MID-ACQUIRE: the addressed node is the correct ring owner,
// but the position's mount (its lease handoff) has not completed yet.
//
// RETRY CONTRACT. This is TRANSIENT and SAFE TO RETRY. The refusal is explicit:
// the op was NOT applied, no partial state was written, and no stale or wrong
// result was served. The window is bounded by the handoff itself (a mount, not
// an outage), so a bounded retry with backoff will observe the acquire finish.
// Retrying is the correct response; surfacing it to an end user is not.
//
// NOT A PEER-DOWN SIGNAL. An acquiring refusal says the cluster is mid-
// transition and healthy, NOT that a node is unreachable. It shares
// codes.Unavailable with genuine peer-down errors, which is exactly why
// matching on the code (or on the message) is wrong: only errors.Is against
// this sentinel distinguishes the two.
//
// PATH-INDEPENDENT. errors.Is(err, ErrAcquiring) holds whether the position was
// mounted on the node the call entered (in-process) or the op was forwarded to
// a peer that was mid-acquire (over gRPC). Consumers neither know nor need to
// know which path produced the error.
//
// REPLICATION-INDEPENDENT. It holds at R=1 and at R>1 alike. At R>1 a write is a
// fan-out, and falling short of W collapses the legs into one error; that
// terminal inherits the reason its legs carried, so a replicated consumer gets
// the same match a single-copy one does.
//
//	if errors.Is(err, cluster.ErrAcquiring) {
//	    // bounded retry with backoff; the handoff will finish
//	}
//
// WHAT IT DOES NOT COVER, deliberately. A write that fell short of W because a
// replica was GENUINELY DOWN does not match, even when other replicas were
// simultaneously mid-acquire. The retry contract above rests on the window being
// bounded by a mount; once a real failure is in the mix it is bounded by whatever
// revives the peer instead, so reporting it as acquiring would send this bounded
// retry against an outage. A mixed shortfall surfaces as the plain
// codes.Unavailable failure it is.
var ErrAcquiring = errors.New("cluster: unit acquiring (handoff window)")

// reasonSentinels is the SINGLE source of truth binding each wire reason to the
// exported sentinel a decoded error wraps. Both the encode half (reasonFor) and
// the decode half (decodeRefusalReason) read it, so a reason cannot be emitted
// on the wire without a matching sentinel on the far side.
var reasonSentinels = map[RefusalReason]error{
	ReasonAcquiring: ErrAcquiring,
}

// reasonFor returns the wire reason to attach for sentinel, if it has one.
// Inverse of reasonSentinels; kept in this file so the two halves stay adjacent.
//
// Taxonomy scaffolding: it has no production caller today (encode sites name
// their reason directly). It earns its place by making the table's
// INVERTIBILITY testable - the round-trip test walks reasonSentinels through it,
// so a reason that is emittable but not decodable fails there rather than in a
// consumer's error handling.
func reasonFor(sentinel error) (RefusalReason, bool) {
	for r, s := range reasonSentinels {
		if s == sentinel {
			return r, true
		}
	}
	return "", false
}

// withReason attaches reason to st as a machine-readable ErrorInfo detail,
// leaving the status CODE and MESSAGE untouched. On any marshaling failure it
// returns st unchanged: the reason is additive identity, and losing it must
// degrade to today's behavior rather than corrupt the refusal.
func withReason(st *status.Status, reason RefusalReason) *status.Status {
	withDetail, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: string(reason),
		Domain: reasonDomain,
	})
	if err != nil {
		return st
	}
	return withDetail
}

// reasonOf extracts the shale refusal reason carried by err's gRPC status, if
// any. Details from other domains are ignored, so a reason string minted by an
// unrelated service can never be mistaken for one of ours.
func reasonOf(err error) (RefusalReason, bool) {
	if err == nil {
		return "", false
	}
	st, ok := status.FromError(err)
	if !ok || st == nil {
		return "", false
	}
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != reasonDomain {
			continue
		}
		r := RefusalReason(info.GetReason())
		if _, known := reasonSentinels[r]; known {
			return r, true
		}
	}
	return "", false
}

// reasonError attaches a reason's EXPORTED sentinel to an error, without
// disturbing anything else about it. It has two producers:
//
//  1. decodeRefusalReason, for a refusal that arrived over gRPC carrying an
//     ErrorInfo detail (the wire half of the contract), and
//  2. reasonTerminal, for a terminal error SYNTHESIZED in-process to summarize
//     legs that carried the reason (the R>1 write shortfall, where the fan-out
//     legs are collapsed into one error the caller sees).
//
// It is a pure IDENTITY wrapper: Error() and GRPCStatus() delegate to the
// wrapped error verbatim, so the message text and the status code (including
// codes.Unavailable) are exactly what they were, and every existing code-based
// classifier behaves identically. Unwrap returns BOTH the original error (so
// errors.Is/As against anything it already carried keeps working) and the
// exported sentinel (so errors.Is(err, ErrAcquiring) holds).
//
// It deliberately does NOT unwrap to any package-private tag such as
// errAcquiringSentinel: that tag means "a LOCAL in-process mid-acquire replica"
// to the fan-out and read-leg classifiers, and widening it to wire-arrived or
// synthesized errors would silently reclassify their budgets. The exported
// sentinel is the CONSUMER's identity; the private tag is the classifiers'. The
// two are deliberately not the same set, and this wrapper only ever grants the
// former.
type reasonError struct {
	err      error
	sentinel error
}

func (e *reasonError) Error() string { return e.err.Error() }

func (e *reasonError) GRPCStatus() *status.Status {
	st, _ := status.FromError(e.err)
	return st
}

func (e *reasonError) Unwrap() []error { return []error{e.err, e.sentinel} }

// reasonTerminal builds a terminal error for reason: a gRPC status carrying
// BOTH halves of the reason contract, so the identity holds no matter which way
// the caller reaches it.
//
//   - The wire half: the ErrorInfo detail rides on the status, so if this
//     terminal is produced on a node the client FORWARDED to, the originating
//     node's peer client decodes it straight back into the same sentinel.
//   - The in-process half: the returned value wraps the exported sentinel
//     directly, so a consumer holding the error on the node that minted it
//     matches without any wire round trip.
//
// code and msg are the caller's, untouched: the reason is additive identity
// layered on top of a status the existing retry/forwarding shapes already
// understand.
func reasonTerminal(code codes.Code, reason RefusalReason, msg string) error {
	sentinel, ok := reasonSentinels[reason]
	if !ok {
		// Unknown reason: degrade to the plain status rather than minting an
		// identity nothing can match.
		return status.Error(code, msg)
	}
	st := withReason(status.New(code, msg), reason)
	return &reasonError{err: st.Err(), sentinel: sentinel}
}

// legsCarryReason reports whether any of legs carries reason's exported
// sentinel. It is how a synthesized terminal inherits the reason of the
// underlying legs instead of guessing from the branch it was minted in: the
// terminal claims a reason only when a leg actually presented that evidence.
//
// It matches the EXPORTED sentinel, which is exactly the set of forms a leg can
// arrive in: an in-process refusal (whose private tag unwraps to the exported
// sentinel) and a wire-decoded peer refusal (which the client interceptor wrapped
// in the exported sentinel). One predicate covers both paths.
func legsCarryReason(legs []error, reason RefusalReason) bool {
	sentinel, ok := reasonSentinels[reason]
	if !ok {
		return false
	}
	for _, leg := range legs {
		if errors.Is(leg, sentinel) {
			return true
		}
	}
	return false
}

// decodeRefusalReason re-attaches the exported sentinel for any shale refusal
// reason carried by err, and returns every other error (including nil and
// io.EOF) completely untouched.
//
// This is the CLIENT half of the reason contract, applied by the peer client's
// interceptors so it covers every cluster-internal RPC (unary and streaming) in
// ONE place rather than per call site. An error with no shale ErrorInfo detail
// - notably a genuine peer-down codes.Unavailable, which carries no details at
// all - passes through unchanged and therefore does NOT match any reason
// sentinel. That is the distinction the whole mechanism exists to preserve.
func decodeRefusalReason(err error) error {
	if err == nil {
		return nil
	}
	r, ok := reasonOf(err)
	if !ok {
		return err
	}
	return &reasonError{err: err, sentinel: reasonSentinels[r]}
}

// localReason is the LOCAL (in-process) form of a refusal reason's package-
// private classifier tag. It prints as msg - preserving the exact text the tag
// had before reasons existed - and unwraps to the exported sentinel, so the
// private identity every internal classifier matches on is unchanged while
// errors.Is(err, <exported sentinel>) additionally holds.
type localReason struct {
	msg      string
	sentinel error
}

func (e *localReason) Error() string { return e.msg }
func (e *localReason) Unwrap() error { return e.sentinel }
