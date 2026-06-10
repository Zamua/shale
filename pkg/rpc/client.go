package rpc

import (
	"context"

	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is a thin wrapper around the generated ShaleNodeClient that
// also owns the underlying *grpc.ClientConn (so callers get one Close
// that tears down both). v0.1 connects over plaintext; mTLS is a v0.5
// concern.
type Client struct {
	conn *grpc.ClientConn
	api  pb.ShaleNodeClient
}

// NewClient dials addr ("host:port") and returns a ready Client. The
// dial is non-blocking; the first RPC surfaces any handshake failure.
func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, api: pb.NewShaleNodeClient(conn)}, nil
}

// Close tears down the underlying gRPC connection. Idempotent.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// -- KV ops ----------------------------------------------------------

// Put writes key=value through the server's Backend.
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	_, err := c.api.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	return err
}

// Get returns (value, found, error). found=false matches the Backend's
// ErrNotFound semantics without forcing callers to import that sentinel.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	resp, err := c.api.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}
	if resp.GetNotFound() {
		return nil, false, nil
	}
	return resp.GetValue(), true, nil
}

// Delete removes key. Deleting an absent key is not an error.
func (c *Client) Delete(ctx context.Context, key []byte) error {
	_, err := c.api.Delete(ctx, &pb.DeleteRequest{Key: key})
	return err
}

// ScanPrefix returns the raw streaming client; callers iterate via
// Recv until io.EOF. Kept thin so callers don't pay for an
// intermediate slice when streaming large prefixes.
func (c *Client) ScanPrefix(ctx context.Context, prefix []byte) (grpc.ServerStreamingClient[pb.ScanPrefixResponse], error) {
	return c.api.ScanPrefix(ctx, &pb.ScanPrefixRequest{Prefix: prefix})
}

// -- Cluster ops -----------------------------------------------------

// Topology returns the server's current view of cluster membership +
// ring shape.
func (c *Client) Topology(ctx context.Context) (*pb.TopologyResponse, error) {
	return c.api.Topology(ctx, &pb.TopologyRequest{})
}

// Stats returns the server's per-node stats snapshot.
func (c *Client) Stats(ctx context.Context) (*pb.StatsResponse, error) {
	return c.api.Stats(ctx, &pb.StatsRequest{})
}

// Ping is the round-trip liveness probe; it returns nil when the
// server is reachable.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.api.Ping(ctx, &pb.PingRequest{})
	return err
}

// -- Rebalancing ops (v0.3 scaffold) ---------------------------------

// MigrateRange opens the source-to-destination stream for the v0.3
// rebalancing protocol. The destination (this caller) sends the set
// of partition IDs it claims ownership of plus its ring generation;
// the source replies with a stream of MigrateChunk messages
// (KeyValue pairs followed by exactly one MigrationDone marker).
//
// Callers iterate the stream via Recv until io.EOF, branching on the
// oneof body to distinguish payload chunks from the terminal marker.
// Kept thin so callers don't pay for an intermediate buffer when
// streaming large ranges.
func (c *Client) MigrateRange(ctx context.Context, partitionIDs []uint64, ringGeneration uint64) (grpc.ServerStreamingClient[pb.MigrateChunk], error) {
	return c.api.MigrateRange(ctx, &pb.RangeSpec{
		PartitionIds:   partitionIDs,
		RingGeneration: ringGeneration,
	})
}

// ProposeRebalance is the CLI-side caller for
// `shale rebalance --dry-run | --apply | --cancel`. Exactly one of
// dryRun / apply / cancel must be true; the server returns
// InvalidArgument otherwise.
func (c *Client) ProposeRebalance(ctx context.Context, dryRun, apply, cancel bool) (*pb.ProposeRebalanceResponse, error) {
	return c.api.ProposeRebalance(ctx, &pb.ProposeRebalanceRequest{
		DryRun: dryRun,
		Apply:  apply,
		Cancel: cancel,
	})
}

// -- CAS transaction (v0.6) ------------------------------------------

// CommitCAS ships an optimistic-concurrency commit (read-set + write-set)
// to the owning node. The response carries the outcome as typed booleans
// (committed / conflict) plus an error string for backend / ownership
// failures; a conflict is the expected OCC retry signal, not a gRPC
// error. Exposed for symmetry with the inter-node peerClient; the cluster
// layer drives CommitCAS through its own private client.
func (c *Client) CommitCAS(ctx context.Context, req *pb.CommitCASRequest) (*pb.CommitCASResponse, error) {
	return c.api.CommitCAS(ctx, req)
}
