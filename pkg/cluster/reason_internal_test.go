package cluster

// White-box guards on the refusal-reason plumbing. The integration test
// (tests/integration/acquiring_reason_test.go) proves the CONSUMER contract end
// to end; this file pins the internal invariants that contract must not have
// disturbed, because breaking any of them would move a classification budget
// rather than fail a visible assertion.

import (
	"errors"
	"io"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// remoteAcquiringErr models what the peer client receives: the acquiring
// refusal round-tripped through a gRPC status (the detail survives, the Go
// value does not), then decoded.
func remoteAcquiringErr(t *testing.T) error {
	t.Helper()
	st, ok := status.FromError(errUnitAcquiring("Get"))
	if !ok {
		t.Fatal("errUnitAcquiring did not carry a gRPC status")
	}
	// st.Err() is a plain *status.Error, exactly what crosses the wire: no Go
	// wrapping, only code + message + details.
	return decodeRefusalReason(st.Err())
}

// TestAcquiringReason_LocalTagIdentityUnchanged pins that exporting
// ErrAcquiring did not disturb the package-private sentinel every internal
// classifier matches on. errAcquiringSentinel now UNWRAPS to ErrAcquiring, and
// the direction matters: the private tag keeps its own identity and its exact
// message text.
func TestAcquiringReason_LocalTagIdentityUnchanged(t *testing.T) {
	err := errUnitAcquiring("Put")

	if !errors.Is(err, errAcquiringSentinel) {
		t.Fatal("local acquiring refusal no longer matches errAcquiringSentinel")
	}
	if !isAcquiringErr(err) {
		t.Fatal("local acquiring refusal no longer classified by isAcquiringErr")
	}
	if !isTransientReplicaErr(err) {
		t.Fatal("local acquiring refusal no longer transient to the fan-out classifier")
	}
	if !errors.Is(err, ErrAcquiring) {
		t.Fatal("local acquiring refusal does not match the exported ErrAcquiring")
	}
	if got, want := errAcquiringSentinel.Error(), "cluster: unit acquiring (handoff window)"; got != want {
		t.Fatalf("errAcquiringSentinel text drifted: got %q, want %q", got, want)
	}
	if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
		t.Fatalf("local acquiring refusal code = %v, want Unavailable", st.Code())
	}
}

// TestAcquiringReason_DecodedRemoteStaysOutOfLocalClassifiers is the
// no-regression guard with the sharpest teeth.
//
// errAcquiringSentinel means "a LOCAL in-process mid-acquire replica" to the
// fan-out ack-vs-failure budget (isTransientReplicaErr, the Phase 2d fix) and
// to the read-leg classes (isTransientReadLegErr vs isUnreachableLegErr). A
// wire-decoded acquiring error must therefore gain the EXPORTED sentinel only:
// if it also unwrapped to the private tag, every remote Unavailable acquiring
// refusal would silently change budget class - the fan-out would stop counting
// it, and a read leg would jump from the capped unreachable re-poll to the full
// handoff re-poll. Neither is in scope here.
func TestAcquiringReason_DecodedRemoteStaysOutOfLocalClassifiers(t *testing.T) {
	decoded := remoteAcquiringErr(t)

	// The consumer contract: the wire path matches the exported sentinel.
	if !errors.Is(decoded, ErrAcquiring) {
		t.Fatalf("decoded remote acquiring refusal does not match ErrAcquiring: %v", decoded)
	}
	// The no-regression half: it must NOT acquire the private local identity.
	if errors.Is(decoded, errAcquiringSentinel) {
		t.Fatal("decoded remote acquiring refusal leaked into errAcquiringSentinel; local-only classifiers would reclassify remote errors")
	}
	if isAcquiringErr(decoded) {
		t.Fatal("decoded remote acquiring refusal now passes isAcquiringErr; the read-retry budget class would move")
	}
	if isTransientReplicaErr(decoded) {
		t.Fatal("decoded remote acquiring refusal now passes isTransientReplicaErr; the fan-out ack-vs-failure budget would move")
	}
	// It must still be classified exactly as it was before reasons existed.
	if !isUnreachableLegErr(decoded) {
		t.Fatal("decoded remote acquiring refusal no longer reads as a bare Unavailable leg; its read-leg class moved")
	}
	if st, _ := status.FromError(decoded); st.Code() != codes.Unavailable {
		t.Fatalf("decoded remote acquiring refusal code = %v, want Unavailable (client-facing code must not change)", st.Code())
	}
}

// TestAcquiringReason_ForwardedReplicaRecodeUnchanged pins that the INTERNAL
// replica leg still re-codes to ResourceExhausted. That recode is what keeps a
// cross-node mid-acquire replica out of the failure budget; the reason detail
// is a parallel mechanism for the CLIENT-facing refusal and must not have
// replaced it.
func TestAcquiringReason_ForwardedReplicaRecodeUnchanged(t *testing.T) {
	recoded := recodeForwardedReplicaErr(errUnitAcquiring("Put"))

	st, ok := status.FromError(recoded)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("forwarded replica leg recode: code = %v, want ResourceExhausted", st.Code())
	}
	if !isTransientReplicaErr(recoded) {
		t.Fatal("recoded forwarded replica error is no longer transient to the fan-out classifier")
	}
	// A non-acquiring error still passes through untouched.
	other := status.Error(codes.Unavailable, "peer down")
	if got := recodeForwardedReplicaErr(other); got != other {
		t.Fatalf("recode altered a non-acquiring error: %v", got)
	}
}

// TestAcquiringReason_DecodeLeavesUnrelatedErrorsAlone pins that decoding is
// inert for everything that carries no shale reason detail. A bare Unavailable
// (peer down) is the case the consumer's whole retry decision hinges on; io.EOF
// is the streaming path's normal termination and must survive identity checks.
func TestAcquiringReason_DecodeLeavesUnrelatedErrorsAlone(t *testing.T) {
	peerDown := status.Error(codes.Unavailable, `connection error: desc = "transport: dial tcp: connect: connection refused"`)
	if got := decodeRefusalReason(peerDown); got != peerDown {
		t.Fatalf("decode altered a peer-down error: %v", got)
	}
	if errors.Is(decodeRefusalReason(peerDown), ErrAcquiring) {
		t.Fatal("a bare Unavailable matched ErrAcquiring; the acquiring/outage distinction is broken")
	}
	if got := decodeRefusalReason(io.EOF); got != io.EOF {
		t.Fatalf("decode altered io.EOF: %v", got)
	}
	if got := decodeRefusalReason(nil); got != nil {
		t.Fatalf("decode turned nil into %v", got)
	}
	// A detail from a FOREIGN domain must never be read as a shale reason.
	foreign := status.New(codes.Unavailable, "someone else's refusal")
	if _, ok := reasonOf(withForeignDomain(t, foreign).Err()); ok {
		t.Fatal("a foreign-domain ErrorInfo was read as a shale refusal reason")
	}
}

// withForeignDomain attaches an ErrorInfo whose Reason string collides with a
// shale reason but whose DOMAIN belongs to someone else, modeling shale sharing
// a gRPC deployment with another service.
func withForeignDomain(t *testing.T, st *status.Status) *status.Status {
	t.Helper()
	out, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: string(ReasonAcquiring),
		Domain: "someone-else.example",
	})
	if err != nil {
		t.Fatalf("attach foreign detail: %v", err)
	}
	return out
}

// TestAcquiringReason_SentinelTableRoundTrips pins the ONE table both halves
// read, so a reason can never be encodable but undecodable. The next reason
// added (fenced, frozen, migration-guard, conflict) is covered by this test the
// moment its row lands.
func TestAcquiringReason_SentinelTableRoundTrips(t *testing.T) {
	if len(reasonSentinels) == 0 {
		t.Fatal("reasonSentinels is empty")
	}
	for reason, sentinel := range reasonSentinels {
		st := withReason(status.New(codes.Unavailable, "refused"), reason)
		got, ok := reasonOf(st.Err())
		if !ok {
			t.Fatalf("reason %q did not survive an encode/decode round trip", reason)
		}
		if got != reason {
			t.Fatalf("reason round trip: got %q, want %q", got, reason)
		}
		if !errors.Is(decodeRefusalReason(st.Err()), sentinel) {
			t.Fatalf("decoded reason %q does not match its sentinel", reason)
		}
		if back, ok := reasonFor(sentinel); !ok || back != reason {
			t.Fatalf("reasonFor(%v) = %q,%v; want %q", sentinel, back, ok, reason)
		}
		// The reason is additive: it must not disturb code or message.
		if st.Code() != codes.Unavailable || st.Message() != "refused" {
			t.Fatalf("attaching reason %q altered the status: %v %q", reason, st.Code(), st.Message())
		}
	}
}
