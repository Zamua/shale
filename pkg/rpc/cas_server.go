package rpc

import (
	"context"

	"github.com/Zamua/shale/pkg/backend"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CommitCAS is the owner-side handler for the v0.6 optimistic-concurrency
// commit (docs/SPEC.md "Owner-side validate-and-apply"). The client has
// already run the cross-shard guard, so every read-check and write-op key
// shards to the same owner as pin_key; this handler:
//
//  1. Verifies the local node owns pin_key. If not (the ring moved and we
//     are no longer the owner), returns FailedPrecondition WITHOUT opening
//     a backend transaction, so the client re-pins against the new ring
//     and retries rather than us applying to the wrong backend.
//  2. Delegates to Cluster.CommitCASApply, which opens ONE short local
//     backend.Transaction, validates the read-set, applies the write-set,
//     and commits, with a deferred Rollback guarding every non-committed
//     return path (validation conflict, op error, commit error, context
//     cancel).
//
// The outcome travels as typed booleans: a conflict is NOT a gRPC error
// (it is the expected OCC retry signal, same convention GetResponse uses
// for not-found), so it returns {conflict:true} with a nil error. A
// backend failure returns {error:<msg>} with a nil error too (the wire
// error string is the contract); only the ownership refusal in step 1
// uses a gRPC status code, because there is no committed/conflict/error
// triple to fill when we refuse to even open the transaction.
func (s *Server) CommitCAS(ctx context.Context, req *pb.CommitCASRequest) (*pb.CommitCASResponse, error) {
	// The gate is the DESIGNATED-CAS-owner predicate (OwnsCASPin), not the raw
	// ring head (OwnsKey): during a join transition the designation moves to the
	// displaced still-mounted current head while the full-ring head is a warming
	// Joining newcomer, and the gate must accept commits exactly where the client
	// dispatch (commitCAS) sends them - both evaluate the same pure function of
	// (ring, joining set), so at most one node accepts per converged view. In
	// steady state OwnsCASPin == OwnsKey. See docs/SPEC.md "CAS during a
	// transition: ONE designated owner".
	if !s.c.OwnsCASPin(req.GetPinKey()) {
		return nil, status.Error(codes.FailedPrecondition,
			"shale: CommitCAS: this node does not own pin_key; re-pin against the current ring")
	}

	reads := make([]backend.ReadCheck, len(req.GetReads()))
	for i, r := range req.GetReads() {
		reads[i] = backend.ReadCheck{
			Key:          r.GetKey(),
			ExpectedVal:  r.GetExpectedValue(),
			ExpectAbsent: r.GetExpectAbsent(),
		}
	}
	writes := make([]backend.WriteOp, len(req.GetWrites()))
	for i, w := range req.GetWrites() {
		writes[i] = backend.WriteOp{
			Key:   w.GetKey(),
			Value: w.GetValue(),
			Del:   w.GetDelete(),
		}
	}

	res := s.c.CommitCASApply(ctx, backend.IsolationLevel(req.GetIsolationLevel()), req.GetPinKey(), reads, writes)
	resp := &pb.CommitCASResponse{}
	switch {
	case res.Conflict:
		resp.Conflict = true
	case res.Err != nil:
		// A commit error that carries a gRPC status (the freeze-window /
		// acquiring-window retryable codes.Unavailable, the migration-guard
		// codes.ResourceExhausted) must reach the client AS a gRPC status error
		// so status.Code() classifies it - exactly the convention ApplyBatch
		// uses for its migration-guard rejection. Flattening it to resp.Error
		// would erase the code: the client would wrap the string with
		// status.Code()==Unknown, and a caller classifying a frozen CAS commit
		// as retryable would mis-read it as a hard failure. A plain backend
		// failure (no status) still travels as the response error string (the
		// wire convention for an unexpected apply error -> the owner rolled back).
		if _, ok := status.FromError(res.Err); ok {
			return nil, res.Err
		}
		resp.Error = res.Err.Error()
	default:
		resp.Committed = true
	}
	return resp, nil
}
