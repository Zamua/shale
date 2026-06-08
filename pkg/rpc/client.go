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

func (c *Client) Topology(ctx context.Context) (*pb.TopologyResponse, error) {
	return c.api.Topology(ctx, &pb.TopologyRequest{})
}

func (c *Client) Stats(ctx context.Context) (*pb.StatsResponse, error) {
	return c.api.Stats(ctx, &pb.StatsRequest{})
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.api.Ping(ctx, &pb.PingRequest{})
	return err
}
