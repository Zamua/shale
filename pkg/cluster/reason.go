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
//	if errors.Is(err, cluster.ErrAcquiring) {
//	    // bounded retry with backoff; the handoff will finish
//	}
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

// remoteReasonError re-attaches a decoded reason's exported sentinel to an
// error that arrived over gRPC.
//
// It is a pure IDENTITY wrapper: Error() and GRPCStatus() delegate to the
// received error verbatim, so the message text and the status code (including
// codes.Unavailable) are exactly what they were before decoding, and every
// existing code-based classifier behaves identically. Unwrap returns BOTH the
// original error (so errors.Is/As against anything it already carried keeps
// working) and the exported sentinel (so errors.Is(err, ErrAcquiring) holds).
//
// It deliberately does NOT unwrap to any package-private tag such as
// errAcquiringSentinel: that tag means "a LOCAL in-process mid-acquire replica"
// to the fan-out and read-leg classifiers, and widening it to wire-arrived
// errors would silently reclassify their budgets.
type remoteReasonError struct {
	err      error
	sentinel error
}

func (e *remoteReasonError) Error() string { return e.err.Error() }

func (e *remoteReasonError) GRPCStatus() *status.Status {
	st, _ := status.FromError(e.err)
	return st
}

func (e *remoteReasonError) Unwrap() []error { return []error{e.err, e.sentinel} }

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
	return &remoteReasonError{err: err, sentinel: reasonSentinels[r]}
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
