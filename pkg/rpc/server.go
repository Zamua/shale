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

func (s *Server) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.puts.Add(1)
	if err := s.c.Put(req.GetKey(), req.GetValue()); err != nil {
		return nil, err
	}
	return &pb.PutResponse{}, nil
}

func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.gets.Add(1)
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
	if err := s.c.Delete(req.GetKey()); err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{}, nil
}

func (s *Server) ScanPrefix(req *pb.ScanPrefixRequest, stream grpc.ServerStreamingServer[pb.ScanPrefixResponse]) error {
	s.scans.Add(1)
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

// keysHeld counts via an empty-prefix scan. Cheap for the small
// memory-backed clusters we test against; a future revision will swap
// in a maintained counter on the Backend side.
func (s *Server) keysHeld() (uint64, error) {
	it, err := s.c.ScanPrefix(nil)
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
