// Package rpc wires the shale node's public surface (pkg/cluster) to
// the wire protocol defined in proto/shale.proto. The Server type
// implements pb.ShaleNodeServer by delegating each RPC to a
// *cluster.Cluster, plus a small counter for Stats.
//
// In v0.1, exactly one Server fronts one Cluster. In v0.2+, the same
// Server is the inter-node forwarding endpoint: when node A's local
// Cluster needs to operate on a key owned by node B, A's rpc.Client
// dials B's rpc.Server and invokes the same RPCs the CLI uses.
package rpc

import (
	"context"
	"errors"
	"hash/crc32"
	"sync/atomic"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is the gRPC adapter on top of cluster.Cluster.
type Server struct {
	pb.UnimplementedShaleNodeServer

	c *cluster.Cluster

	// Lifetime request counters; surfaced by Stats. Maintained with
	// atomics so concurrent RPCs don't contend on a mutex.
	puts    atomic.Uint64
	gets    atomic.Uint64
	deletes atomic.Uint64
	scans   atomic.Uint64
}

// NewServer wraps c so it can be registered against a grpc.Server via
// Register. The caller owns c's lifecycle; Server holds a reference but
// does not close it.
func NewServer(c *cluster.Cluster) *Server {
	return &Server{c: c}
}

// Register installs s on the given grpc.Server. Convenience for callers
// who want a one-liner in main.go.
func (s *Server) Register(g *grpc.Server) {
	pb.RegisterShaleNodeServer(g, s)
}

// -- KV RPCs ---------------------------------------------------------

// errForwardLoop is returned when a forwarded=true request arrives at
// a node that does not own the key. The originating cluster's ring
// has drifted ahead of ours; bouncing it back would loop. The caller
// sees FailedPrecondition + a descriptive message so it can refresh
// its ring view + retry from scratch.
func errForwardLoop(reason string) error {
	return status.Error(codes.FailedPrecondition, "shale: forwarding loop refused: "+reason)
}

// Put handles the gRPC Put RPC. A non-Forwarded request goes through
// the cluster's full routing + replication; a Forwarded request is a
// peer-to-peer single-replica write that bypasses re-routing.
func (s *Server) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.puts.Add(1)
	if req.GetForwarded() {
		// Forwarded writes go straight to the local backend with the
		// migration guard; calling s.c.Put would re-enter the routing
		// + (at R>1) re-fan-out the same envelope, looping. The
		// receiving node must be a legitimate replica owner; if it
		// isn't, refuse with the loop-guard so the originator can
		// refresh its ring view + retry. (At R=1 OwnsReplica reduces
		// to OwnsKey, so the v0.3 single-owner check is preserved.)
		if !s.c.OwnsReplica(req.GetKey()) {
			return nil, errForwardLoop("Put: this node is not a replica for the key")
		}
		if err := s.c.LocalReplicaPut(req.GetKey(), req.GetValue()); err != nil {
			return nil, err
		}
		return &pb.PutResponse{}, nil
	}
	if err := s.c.Put(req.GetKey(), req.GetValue()); err != nil {
		return nil, err
	}
	return &pb.PutResponse{}, nil
}

// Get handles the gRPC Get RPC. It serves out of the local backend
// for in-flight migration reads and otherwise routes through the
// cluster's read path.
func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.gets.Add(1)
	if req.GetForwarded() && !s.c.OwnsKey(req.GetKey()) {
		// The forwarded request landed on a node that no longer
		// thinks it owns the key. Two possibilities:
		//
		//   1. Diverged-ring ping-pong: the destination's ring
		//      pointed to us but ours has moved on; the classic
		//      loop-guard refuses so we don't bounce back + forth.
		//   2. Receive-window read forwarder (docs/SPEC.md
		//      "Cutover"): the actual destination of an in-flight
		//      migration forwards a read here because we, the
		//      source, still hold the authoritative copy even
		//      though the shared ring now says the destination
		//      owns the partition.
		//
		// We distinguish the two by checking the LOCAL backend:
		// if we have the key, case 2 applies and we serve. If we
		// don't, case 1 applies and we refuse with the loop-guard
		// (re-forwarding from here would be the ping-pong the
		// guard exists to prevent).
		v, err := s.c.LocalGet(req.GetKey())
		if err == nil {
			return &pb.GetResponse{Value: v}, nil
		}
		if !errors.Is(err, backend.ErrNotFound) {
			return nil, err
		}
		return nil, errForwardLoop("Get: this node does not own the key")
	}
	// In multi-backend mode (v0.8 Phase 3) the guard above (OwnsKey) is RING
	// ownership of the key's unit, not mount-ness. So a forwarded Get that
	// reaches the ring owner during a lease handoff (owner assigned, unit
	// not yet mounted) passes the guard and falls through to c.Get, which
	// resolves the local unit: mounted -> serve; mid-handoff -> the
	// retryable acquiring-window error (codes.Unavailable) so the originator
	// backs off and retries once the reconcile mounts the unit. A
	// genuinely-diverged ring (this node is NOT the unit's ring owner) was
	// already refused above with the loop-guard. Either way the originator
	// makes progress without an A->B->A bounce.
	v, err := s.c.Get(req.GetKey())
	if errors.Is(err, backend.ErrNotFound) {
		return &pb.GetResponse{NotFound: true}, nil
	}
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Value: v}, nil
}

// Delete handles the gRPC Delete RPC. At R>1 originators issue
// Delete via the tombstone path; this method covers both that case
// and the R=1 single-owner clear.
func (s *Server) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.deletes.Add(1)
	if req.GetForwarded() {
		// See Put above for the rationale. At R=1 the destination
		// just clears its local copy; at R>1 the originator drove
		// Delete through putReplicated (tombstone envelope) so the
		// "Delete forwarded" path here only fires for the R=1 case +
		// the single-owner local-clear it represents. The
		// OwnsReplica check covers both (it reduces to OwnsKey at R=1).
		if !s.c.OwnsReplica(req.GetKey()) {
			return nil, errForwardLoop("Delete: this node is not a replica for the key")
		}
		if err := s.c.LocalReplicaDelete(req.GetKey()); err != nil {
			return nil, err
		}
		return &pb.DeleteResponse{}, nil
	}
	if err := s.c.Delete(req.GetKey()); err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{}, nil
}

// ScanPrefix streams every key/value pair whose key starts with
// req.Prefix. The stream ends when the iterator is exhausted or the
// client cancels.
func (s *Server) ScanPrefix(req *pb.ScanPrefixRequest, stream grpc.ServerStreamingServer[pb.ScanPrefixResponse]) error {
	s.scans.Add(1)
	if req.GetForwarded() && !s.c.OwnsKey(req.GetPrefix()) {
		return errForwardLoop("ScanPrefix: this node does not own the prefix")
	}
	it, err := s.c.ScanPrefix(req.GetPrefix())
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		if k == nil {
			return nil
		}
		if err := stream.Send(&pb.ScanPrefixResponse{Key: k, Value: v}); err != nil {
			return err
		}
	}
}

// LocalScan streams the local Backend's keys directly, bypassing the
// ring entirely. Used by sibling shale nodes for admin-style snapshot
// + counter operations (Aggregate, Stats.keys_held) where re-routing
// through ownerOf would hash an empty prefix to a single shard and
// undercount everything.
func (s *Server) LocalScan(req *pb.LocalScanRequest, stream grpc.ServerStreamingServer[pb.LocalScanResponse]) error {
	s.scans.Add(1)
	it, err := s.c.LocalScanPrefix(req.GetPrefix())
	if err != nil {
		return err
	}
	defer func() { _ = it.Close() }()
	for {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		if k == nil {
			return nil
		}
		if err := stream.Send(&pb.LocalScanResponse{Key: k, Value: v}); err != nil {
			return err
		}
	}
}

// ApplyBatch is the replica-side handler for the v0.6.x CAS write-set
// fan-out (docs/SPEC.md "ApplyBatch wire protocol"). The owner has already
// validated + stamped + committed these envelopes locally; this replica
// applies the whole batch (all writes) verbatim in ONE local backend
// transaction (apply-only, no decode, no re-validation, no re-stamp) and
// commits, rolling back on any error. It is cluster-internal: never called
// from outside the cluster.
//
// Outcome shape: a backend / apply failure travels as the response error
// string (non-empty => the replica rolled back), the same wire convention
// CommitCAS uses for backend failures. The migration-guard rejection is
// the exception: it travels as a gRPC codes.ResourceExhausted status error
// (NOT the response field) so the owner's fanout classifies it transient
// (mid-handoff replica, try another) rather than a hard failure. There is
// no ownership re-check beyond that guard: ApplyBatchLocal trusts the
// fan-out the same way LocalReplicaPut trusts OwnsReplica.
func (s *Server) ApplyBatch(_ context.Context, req *pb.ApplyBatchRequest) (*pb.ApplyBatchResponse, error) {
	writes := make([]cluster.EnvelopeWrite, len(req.GetWrites()))
	for i, w := range req.GetWrites() {
		writes[i] = cluster.EnvelopeWrite{Key: w.GetKey(), Envelope: w.GetEnvelope()}
	}
	if err := s.c.ApplyBatchLocal(writes); err != nil {
		// A migration-guard rejection must reach the owner as a gRPC
		// status code so its fanout sees ResourceExhausted and treats the
		// replica as transient; a non-status error is a hard apply failure
		// reported in the response string (the replica rolled back).
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return &pb.ApplyBatchResponse{Error: err.Error()}, nil
	}
	return &pb.ApplyBatchResponse{}, nil
}

// ReshardControl is the cluster-internal control RPC for the v0.8 multi-node
// doubling reshard (cluster-wide freeze barrier). The COORDINATOR (the node
// where Reshard() was called on a multi-node cluster) calls this once per
// barrier phase (FREEZE / BISECT / FLIP / RESUME / ABORT). The handler delegates
// to the cluster's idempotent phase handler; a phase that cannot be applied
// (wrong generation, not frozen, closed) travels back as the response error
// string so the coordinator ABORTS, rather than as a gRPC error code (matching
// the CommitCAS / ApplyBatch wire convention of carrying an expected control-
// flow outcome in the response field). Never called from outside the cluster.
func (s *Server) ReshardControl(_ context.Context, req *pb.ReshardControlRequest) (*pb.ReshardControlResponse, error) {
	if err := s.c.ApplyReshardPhase(req.GetPhase(), req.GetTargetGen()); err != nil {
		return &pb.ReshardControlResponse{Error: err.Error()}, nil
	}
	return &pb.ReshardControlResponse{}, nil
}

// GenState is the cluster-internal generation-propagation RPC for the v0.8
// join-after-reshard fix. A node opening in multi-backend mode WITH seeds (a
// JOINER) calls this on a live seed exactly once during Open - before it
// mounts any unit - to learn the cluster's live {generation, unit-count} and
// seed its own routing state from it (so it never routes / owns a key at gen 0
// after the cluster has resharded). The handler delegates to the cluster's
// snapshot accessor; the live value is coherent because a join only ever lands
// at a stable generation (a membership change mid-reshard ABORTS the reshard).
// Never called from outside the cluster.
func (s *Server) GenState(_ context.Context, _ *pb.GenStateRequest) (*pb.GenStateResponse, error) {
	gen, count := s.c.GenStateSnapshot()
	return &pb.GenStateResponse{Generation: gen, UnitCount: count}, nil
}

// -- Cluster RPCs ----------------------------------------------------

// Topology returns this node's current view of cluster membership.
func (s *Server) Topology(_ context.Context, _ *pb.TopologyRequest) (*pb.TopologyResponse, error) {
	id := s.c.NodeID()
	members := s.c.Members()
	nodes := make([]*pb.NodeInfo, 0, len(members))
	for _, m := range members {
		nodes = append(nodes, &pb.NodeInfo{NodeId: m.ID, GrpcAddr: m.Addr})
	}
	return &pb.TopologyResponse{
		NodeId:     id,
		SingleNode: len(nodes) <= 1,
		Nodes:      nodes,
	}, nil
}

// Stats returns the per-node counters this Server has accumulated.
func (s *Server) Stats(_ context.Context, _ *pb.StatsRequest) (*pb.StatsResponse, error) {
	keys, err := s.keysHeld()
	if err != nil {
		return nil, err
	}
	return &pb.StatsResponse{
		KeysHeld: keys,
		Puts:     s.puts.Load(),
		Gets:     s.gets.Load(),
		Deletes:  s.deletes.Load(),
		Scans:    s.scans.Load(),
		// Latency percentiles wire up in v0.5; placeholders for now.
		LatencyMsP50: 0,
		LatencyMsP99: 0,
	}, nil
}

// Ping is the liveness probe; it always returns an empty response.
func (s *Server) Ping(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{}, nil
}

// -- Rebalancing RPCs (v0.3 scaffold) --------------------------------
//
// MigrateRange and ProposeRebalance carry the wire surface for the
// v0.3 rebalancing protocol described in docs/SPEC.md. The handlers
// here are intentional stubs: they return Unimplemented so the proto
// registration is exercised by the existing build + test surface, and
// so the cluster + rebalance packages can dial these methods once the
// Core phase wires them up to real logic. Returning Unimplemented (not
// a panic, not a silent no-op) keeps any premature caller honest:
// they get the standard gRPC "this is registered but the body is not
// yet provided" signal and can fall through to the not-yet-rebalanced
// behavior path until the real implementation lands.

// MigrateRange is the source-side handler of the v0.3 range-transfer
// stream. The destination has sent a RangeSpec listing the partitions
// it claims ownership of + the ring generation it computed against;
// the source iterates its local Backend, streams every key whose
// shard key falls in any of those partitions, and ends with a
// MigrationDone marker carrying the CRC32-IEEE checksum + count.
//
// Stale-generation guard: if the destination's ring view is behind
// ours, we return FailedPrecondition so the destination's next
// Evaluate pass re-issues against the new ring rather than us
// streaming data that will be discarded.
//
// Handoff signaling: the handler's return value carries the
// destination-observed outcome of the stream. A nil return means
// the gRPC runtime delivered every chunk (the final Send(Done)
// succeeded and the destination read it before closing its end);
// non-nil means the destination disconnected or the source's scan
// errored. SignalMigrateRangeComplete threads that outcome back to
// the Coordinator's source-side runSend, which only flips Sending
// -> HandedOff on a clean return. Without this, the source flipped
// HandedOff based on its own local scan + the sweep deleted keys
// even when the destination crashed mid-stream. See
// docs/SPEC.md "Cutover" + "Failure handling".
func (s *Server) MigrateRange(req *pb.RangeSpec, stream grpc.ServerStreamingServer[pb.MigrateChunk]) (retErr error) {
	pids := req.GetPartitionIds()
	kvCh, errCh, err := s.c.MigrateRangeSource(pids, req.GetRingGeneration())
	if err != nil {
		s.c.SignalMigrateRangeComplete(pids, 0, err)
		return err
	}
	hasher := crc32.NewIEEE()
	var count uint64
	defer func() {
		// retErr captures the handler's return value. Nil iff every
		// stream.Send (including the terminal Done) succeeded + the
		// source scan finished without an error: that is the source
		// observing the destination acked the full transfer. Any
		// non-nil retErr keeps the partitions out of HandedOff at
		// the Coordinator level.
		s.c.SignalMigrateRangeComplete(pids, int(count), retErr)
	}()
	for kv := range kvCh {
		if err := stream.Send(&pb.MigrateChunk{
			Body: &pb.MigrateChunk_Kv{
				Kv: &pb.KeyValue{Key: kv.Key, Value: kv.Value},
			},
		}); err != nil {
			// Drain the source channel so the goroutine in
			// localSource exits cleanly instead of blocking on
			// an un-read send. errCh below will produce its
			// terminal value regardless.
			//nolint:revive // empty-block: idiomatic channel drain.
			for range kvCh {
			}
			<-errCh
			return err
		}
		hasher.Write(kv.Key)
		hasher.Write(kv.Value)
		count++
	}
	if scanErr := <-errCh; scanErr != nil {
		return status.Errorf(codes.Internal, "shale: MigrateRange scan: %v", scanErr)
	}
	return stream.Send(&pb.MigrateChunk{
		Body: &pb.MigrateChunk_Done{
			Done: &pb.MigrationDone{
				TotalKeys: count,
				Checksum:  hasher.Sum(nil),
			},
		},
	})
}

// ProposeRebalance is the operator-facing planner / applier / canceller.
// Exactly one of dry_run / apply / cancel must be set; otherwise the
// server returns InvalidArgument.
//
//   - dry_run: returns the plan the local node would execute against
//     the current ring, without doing anything.
//   - apply:   bypass the settle delay + Evaluate immediately; returns
//     the queued plan.
//   - cancel:  stop in-flight migrations; returns what was in flight.
func (s *Server) ProposeRebalance(_ context.Context, req *pb.ProposeRebalanceRequest) (*pb.ProposeRebalanceResponse, error) {
	items, err := s.c.ProposeRebalance(req.GetDryRun(), req.GetApply(), req.GetCancel())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "shale: %v", err)
	}
	resp := &pb.ProposeRebalanceResponse{Ranges: make([]*pb.RangePlanItem, 0, len(items))}
	for _, it := range items {
		resp.Ranges = append(resp.Ranges, &pb.RangePlanItem{
			PartitionId:       it.PartitionID,
			CurrentOwner:      it.CurrentOwner,
			ProposedOwner:     it.ProposedOwner,
			EstimatedKeyCount: it.EstimatedKeyCount,
		})
	}
	return resp, nil
}

// keysHeld counts via an empty-prefix scan of the LOCAL backend
// (Cluster.LocalScanPrefix), not Cluster.ScanPrefix. The latter routes
// the empty prefix through ownerOf, which in a multi-node cluster
// hashes nil to a single shard - so every node would report the same
// (wrong) count. LocalScanPrefix bypasses routing and gives us the
// true per-node key count. Cheap for the small memory-backed clusters
// we test against; a future revision will swap in a maintained
// counter on the Backend side.
func (s *Server) keysHeld() (uint64, error) {
	it, err := s.c.LocalScanPrefix(nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = it.Close() }()
	var n uint64
	for {
		k, _, err := it.Next()
		if err != nil {
			return 0, err
		}
		if k == nil {
			return n, nil
		}
		n++
	}
}
