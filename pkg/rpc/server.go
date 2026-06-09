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

func (s *Server) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.puts.Add(1)
	if req.GetForwarded() && !s.c.OwnsKey(req.GetKey()) {
		return nil, errForwardLoop("Put: this node does not own the key")
	}
	if err := s.c.Put(req.GetKey(), req.GetValue()); err != nil {
		return nil, err
	}
	return &pb.PutResponse{}, nil
}

func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.gets.Add(1)
	if req.GetForwarded() && !s.c.OwnsKey(req.GetKey()) {
		return nil, errForwardLoop("Get: this node does not own the key")
	}
	v, err := s.c.Get(req.GetKey())
	if errors.Is(err, backend.ErrNotFound) {
		return &pb.GetResponse{NotFound: true}, nil
	}
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Value: v}, nil
}

func (s *Server) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.deletes.Add(1)
	if req.GetForwarded() && !s.c.OwnsKey(req.GetKey()) {
		return nil, errForwardLoop("Delete: this node does not own the key")
	}
	if err := s.c.Delete(req.GetKey()); err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{}, nil
}

func (s *Server) ScanPrefix(req *pb.ScanPrefixRequest, stream grpc.ServerStreamingServer[pb.ScanPrefixResponse]) error {
	s.scans.Add(1)
	if req.GetForwarded() && !s.c.OwnsKey(req.GetPrefix()) {
		return errForwardLoop("ScanPrefix: this node does not own the prefix")
	}
	it, err := s.c.ScanPrefix(req.GetPrefix())
	if err != nil {
		return err
	}
	defer it.Close()
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
	defer it.Close()
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

// -- Cluster RPCs ----------------------------------------------------

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
func (s *Server) MigrateRange(req *pb.RangeSpec, stream grpc.ServerStreamingServer[pb.MigrateChunk]) error {
	kvCh, errCh, err := s.c.MigrateRangeSource(req.GetPartitionIds(), req.GetRingGeneration())
	if err != nil {
		return err
	}
	hasher := crc32.NewIEEE()
	var count uint64
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
	defer it.Close()
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
