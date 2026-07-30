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
		// v0.8 Phase 2e (pending ranges): a POSITION-ADDRESSED union write carries
		// the explicit ReplicaUnit (req.Ru). A PENDING owner targeted by the union
		// does not hold the position at its own current-set ring index, so the
		// OwnsReplica ring-index check below would wrongly refuse it. Instead
		// resolve mountMap[ru] DIRECTLY by the explicit ru. An unmounted ru (a
		// pending owner still mid-mount, or a released drain) returns the retryable
		// acquiring error so the originator's union fan-out tolerates it; no
		// re-forward, no loop.
		if ru := req.GetRu(); ru != nil {
			if err := s.c.LocalReplicaPutAtWire(ru.GetGen(), ru.GetUnit(), ru.GetReplica(), req.GetKey(), req.GetValue()); err != nil {
				return nil, err
			}
			return &pb.PutResponse{}, nil
		}
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
	if ru := req.GetRu(); req.GetForwarded() && ru != nil {
		// v0.8 Phase 2e (pending ranges, position-addressed read): resolve
		// mountMap[ru] DIRECTLY by the explicit ru, bypassing the ring-index
		// OwnsKey check (a pending owner is not at this position in its current-set
		// ring index). An unmounted ru returns the retryable acquiring error /
		// not-found so the originator's read fan-out skips this leg.
		v, err := s.c.LocalReplicaGetAtWire(ru.GetGen(), ru.GetUnit(), ru.GetReplica(), req.GetKey())
		if errors.Is(err, backend.ErrNotFound) {
			return &pb.GetResponse{NotFound: true}, nil
		}
		if err != nil {
			return nil, err
		}
		return &pb.GetResponse{Value: v}, nil
	}
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
		// v0.8 Phase 2e (pending ranges, position-addressed delete): resolve
		// mountMap[ru] DIRECTLY by the explicit ru. At R>1 a Delete is normally a
		// tombstone envelope routed through Put, so this path is the rare direct
		// position-addressed delete; the value carries the tombstone envelope bytes.
		if ru := req.GetRu(); ru != nil {
			if err := s.c.LocalReplicaDeleteAtWire(ru.GetGen(), ru.GetUnit(), ru.GetReplica(), req.GetKey(), nil); err != nil {
				return nil, err
			}
			return &pb.DeleteResponse{}, nil
		}
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
	if ru := req.GetRu(); req.GetForwarded() && ru != nil {
		// v0.8 Phase 2e (union scan leg, position-addressed): resolve the
		// explicit ru directly against the mount map (with the read-side
		// mounted-position fallback), bypassing the ring-ownership guard - the
		// union deliberately routes a scan to a member whose own ring view may
		// disagree mid-transition. An unmounted unit returns the retryable
		// acquiring recode so the originator's leg walk skips this member.
		it, err := s.c.LocalReplicaScanAtWire(ru.GetGen(), ru.GetUnit(), ru.GetReplica(), req.GetPrefix())
		if err != nil {
			return err
		}
		return streamScan(it, stream)
	}
	if req.GetForwarded() && !s.c.OwnsKey(req.GetPrefix()) {
		return errForwardLoop("ScanPrefix: this node does not own the prefix")
	}
	it, err := s.c.ScanPrefix(req.GetPrefix())
	if err != nil {
		return err
	}
	return streamScan(it, stream)
}

// streamScan drains it onto the ScanPrefix response stream, closing the
// iterator when done. Shared by the routed and the position-addressed
// (union scan leg) branches.
func streamScan(it backend.Iterator, stream grpc.ServerStreamingServer[pb.ScanPrefixResponse]) error {
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

// GenState is the cluster-internal generation-propagation RPC for the v0.8
// join-after-reshard fix. A node opening in multi-backend mode WITH seeds (a
// JOINER) calls this on a live seed during Open - before it mounts any unit -
// to learn the cluster's live {generation, unit-count} and seed its own
// routing state from it (so it never routes / owns a key at gen 0 after the
// cluster has resharded). The handler delegates to the cluster's snapshot
// accessor. The response additionally reports whether a reshard is IN FLIGHT
// on this node (nextCount set): an in-flight {gen, count} is about to change,
// so the joiner DEFERS (retries within its GenLearnBudget) rather than seed
// from it. Never called from outside the cluster.
func (s *Server) GenState(_ context.Context, _ *pb.GenStateRequest) (*pb.GenStateResponse, error) {
	gen, count := s.c.GenStateSnapshot()
	return &pb.GenStateResponse{Generation: gen, UnitCount: count, ReshardInFlight: s.c.ReshardInFlight()}, nil
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

// Stats returns the per-node counters this Server has accumulated, plus the
// node's mount-readiness counts (all zero in legacy single-backend mode).
func (s *Server) Stats(_ context.Context, _ *pb.StatsRequest) (*pb.StatsResponse, error) {
	keys, err := s.keysHeld()
	if err != nil {
		return nil, err
	}
	mr := s.c.MountReadiness()
	return &pb.StatsResponse{
		KeysHeld: keys,
		Puts:     s.puts.Load(),
		Gets:     s.gets.Load(),
		Deletes:  s.deletes.Load(),
		Scans:    s.scans.Load(),
		// Mount readiness (docs/SPEC.md "Mount readiness"): reported for
		// remote observability; the readiness DECISION stays in-process in
		// the embedding application via Cluster.Ready.
		DesiredUnits:     uint64(mr.DesiredUnits),
		MountedUnits:     uint64(mr.MountedUnits),
		PendingUnits:     uint64(mr.PendingUnits),
		FailedOpenUnits:  uint64(mr.FailedOpenUnits),
		LastAcquireError: mr.LastAcquireError,
	}, nil
}

// Ping is the liveness probe; it always returns an empty response.
func (s *Server) Ping(_ context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{}, nil
}

// -- Rebalancing RPCs (v0.3 scaffold) --------------------------------
//
// keysHeld counts via an empty-prefix scan of the LOCAL backend
// (Cluster.LocalScanPrefix), not Cluster.ScanPrefix. The latter routes
// the empty prefix through ownerOf, which in a multi-node cluster
// hashes nil to a single shard - so every node would report the same
// (wrong) count. LocalScanPrefix bypasses routing and gives us the
// true per-node key count. Cheap for the small memory-backed clusters
// we test against; a future revision will swap in a maintained
// counter on the Backend side.
//
// IT DELIBERATELY TOLERATES THE ACQUIRING REFUSAL, unlike every other consumer
// of LocalScanPrefix. That scan fails CLOSED when this node holds an owned
// position unmounted, because its callers build SETS and act on what is absent
// from them (a referenced-blob set drives GC, so a missing key deletes an
// object). This counter is not that: it is an approximate observability gauge,
// nothing decides deletion from it, and a partial count is strictly more useful
// than no answer. Propagating the refusal would take Stats DOWN during exactly
// the handoff window an operator is trying to observe - and Stats is the RPC
// that reports PendingUnits, the number explaining why the count is short. So
// the count is reported alongside that number and read as partial when it is
// non-zero, rather than the whole response failing.
func (s *Server) keysHeld() (uint64, error) {
	it, err := s.c.LocalScanPrefix(nil)
	if err != nil {
		if errors.Is(err, cluster.ErrAcquiring) {
			return 0, nil
		}
		return 0, err
	}
	defer func() { _ = it.Close() }()
	var n uint64
	for {
		k, _, err := it.Next()
		if err != nil {
			if errors.Is(err, cluster.ErrAcquiring) {
				return n, nil // partial; PendingUnits in the same response says so
			}
			return 0, err
		}
		if k == nil {
			return n, nil
		}
		n++
	}
}
