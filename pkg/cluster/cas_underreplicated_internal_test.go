package cluster

// White-box tests for the tx-commit-to-error conversion (casResultToError):
// the single site that decides, from a backend.CASResult, whether Transact
// re-runs fn. The load-bearing discriminator is Committed: an owner-local
// commit that durably applied is a SUCCESS even when the R>1 replica fan-out
// missed W (outcome (c)), so it must map to nil and NOT be re-run. Only a
// NOT-committed retryable/backend failure (outcome (d) / a hard error) maps to
// an error. See docs/SPEC.md "The four commit outcomes".

import (
	"errors"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestCASResultToError_CommittedUnderReplicated_IsSuccess pins outcome (c):
// a commit that durably applied owner-locally but missed W on the fan-out
// (Committed=true, UnderReplicated=true, Err carrying the under-W
// codes.Unavailable) maps to nil. Before the fix this returned the raw
// codes.Unavailable, which Transact re-ran (amplification + false conflict).
func TestCASResultToError_CommittedUnderReplicated_IsSuccess(t *testing.T) {
	underW := status.Error(codes.Unavailable, "shale: write needed 3 acks, got 1 (2 failures)")
	res := backend.CASResult{Committed: true, UnderReplicated: true, Err: underW}

	if err := casResultToError(res); err != nil {
		t.Fatalf("committed-under-replicated must map to nil (success); got %v", err)
	}
}

// TestCASResultToError_CommittedReplicated_IsSuccess pins outcome (b): a fully
// replicated commit (the common case) maps to nil.
func TestCASResultToError_CommittedReplicated_IsSuccess(t *testing.T) {
	if err := casResultToError(backend.CASResult{Committed: true}); err != nil {
		t.Fatalf("a committed (fully replicated) result must map to nil; got %v", err)
	}
}

// TestCASResultToError_NotCommittedRetryable_PreservesError pins outcome (d):
// a PRE-commit retryable refusal (Committed=false, Err=codes.Unavailable)
// applied nothing, so it must surface the retryable error unchanged for
// Transact to re-run. The discriminator is Committed: same code, opposite
// disposition from (c).
func TestCASResultToError_NotCommittedRetryable_PreservesError(t *testing.T) {
	frozen := status.Error(codes.Unavailable, "shale: cluster write-frozen for reshard")
	err := casResultToError(backend.CASResult{Err: frozen})
	if err == nil {
		t.Fatalf("a not-committed retryable refusal must surface an error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("the not-committed error must keep codes.Unavailable so Transact re-runs; got ok=%v err=%v", ok, err)
	}
}

// TestCASResultToError_Conflict_Unchanged pins outcome (a): a pre-commit
// read-check failure maps to backend.ErrCASConflict, unchanged by this fix.
func TestCASResultToError_Conflict_Unchanged(t *testing.T) {
	err := casResultToError(backend.CASResult{Conflict: true})
	if !errors.Is(err, backend.ErrCASConflict) {
		t.Fatalf("conflict must map to ErrCASConflict; got %v", err)
	}
}

// TestCASResultToError_NotCommittedBackendError_Surfaces pins a plain
// not-committed backend failure (no gRPC status): it surfaces as-is (a hard
// error Transact does NOT retry). Committed==false is the discriminator.
func TestCASResultToError_NotCommittedBackendError_Surfaces(t *testing.T) {
	boom := errors.New("backend exploded")
	if err := casResultToError(backend.CASResult{Err: boom}); !errors.Is(err, boom) {
		t.Fatalf("a not-committed backend error must surface unchanged; got %v", err)
	}
}
