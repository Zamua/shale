package cluster

// peerclient.go holds the cluster-internal gRPC CLIENT: the dial options every
// peer connection shares (including the refusal-reason decode interceptors),
// the peerClient wrapper itself, and the thin per-RPC methods (routed-forward,
// position-addressed, plain, admin, CAS, reshard-control, gen-state). It is
// transport plumbing only - no routing, no ownership, no transaction
// semantics. Callers live in multibackend*.go, cas.go and aggregate.go. See
// doc.go for the mode matrix.

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// peerDialOptions are the shared dial options for the cluster-internal peer
// gRPC client (and mirrored by pkg/rpc.Client). They harden the connection
// against the cold-start fail-fast trap (see Config.PeerConnectTimeout):
//   - a FAST reconnect backoff so a CACHED ClientConn that misses its first
//     connect (peer's gRPC momentarily not-ready) recovers in seconds, not the
//     gRPC default's 120s ceiling that wedges every later RPC on that client;
//   - keepalive so an idle cached connection stays warm and a half-open one is
//     detected and replaced rather than silently failing the next RPC.
//
// NOTE: these do NOT set WaitForReady - single-key Get/Put stay FAIL-FAST so a
// momentarily-unreachable replica fails over to another replica immediately
// rather than blocking the hot path. The cross-shard scan path instead waits
// EXPLICITLY (peerClient.waitReady, bounded by PeerConnectTimeout) before it
// opens its stream.
func peerDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  200 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		// Decode shale refusal REASONS (reason.go) back into their exported
		// sentinels. Installed as interceptors so EVERY cluster-internal RPC is
		// covered by ONE decode site rather than each peerClient wrapper having
		// to remember; a new RPC gets the behavior for free.
		grpc.WithChainUnaryInterceptor(reasonUnaryInterceptor),
		grpc.WithChainStreamInterceptor(reasonStreamInterceptor),
	}
}

// reasonUnaryInterceptor re-attaches the exported sentinel for any refusal
// reason a unary peer RPC came back with. Errors carrying no shale reason
// detail (a genuine peer-down Unavailable, a deadline, a context cancel) pass
// through byte-identical.
func reasonUnaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return decodeRefusalReason(invoker(ctx, method, req, reply, cc, opts...))
}

// reasonStreamInterceptor is the streaming counterpart. A refusal can surface
// EITHER when the stream is opened or on a later Recv (a server-streaming
// handler that rejects after the header), so both are decoded: the stream
// itself is wrapped so RecvMsg gets the same treatment. io.EOF (normal stream
// end) carries no reason detail and so passes through untouched.
func reasonStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	s, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return s, decodeRefusalReason(err)
	}
	return &reasonClientStream{ClientStream: s}, nil
}

// reasonClientStream decodes refusal reasons off a client stream's per-message
// errors. Only RecvMsg is overridden: SendMsg's error is a local transport
// fault, never a server-minted refusal.
type reasonClientStream struct{ grpc.ClientStream }

func (s *reasonClientStream) RecvMsg(m any) error {
	return decodeRefusalReason(s.ClientStream.RecvMsg(m))
}

// peerClient is the cluster-internal gRPC client used for inter-node
// forwarding. It is a deliberate mirror of pkg/rpc.Client (which the
// CLI uses), kept private here so pkg/cluster does not have to import
// pkg/rpc (which would create an import cycle, since pkg/rpc.Server
// already depends on pkg/cluster). The two clients share the same
// generated proto bindings + dial options.
type peerClient struct {
	conn *grpc.ClientConn
	api  pb.ShaleNodeClient
}

func newPeerClient(addr string) (*peerClient, error) {
	conn, err := grpc.NewClient(addr, peerDialOptions()...)
	if err != nil {
		return nil, err
	}
	return &peerClient{conn: conn, api: pb.NewShaleNodeClient(conn)}, nil
}

// waitReady blocks until this peer's gRPC ClientConn reaches READY or the
// context is done. grpc.NewClient is LAZY (it does not connect until the first
// RPC) and FAIL-FAST (a not-yet-ready connection fails an RPC immediately), so
// the cross-shard scan path (snapshotPeer) calls waitReady FIRST to absorb a
// peer that is momentarily not-serving-gRPC at cold-start: it nudges the
// connection out of IDLE (Connect) and waits for the connectivity state to
// climb to READY (the fast peerDialOptions backoff makes that quick once the
// peer is up). On ctx expiry it returns an error so the caller records the
// peer's AggregateResult.Err instead of blocking the whole fan-out forever.
func (c *peerClient) waitReady(ctx context.Context) error {
	if c.conn == nil {
		return errors.New("cluster: peer client closed")
	}
	c.conn.Connect() // nudge out of IDLE; no-op if already connecting/ready
	for {
		s := c.conn.GetState()
		if s == connectivity.Ready {
			return nil
		}
		if !c.conn.WaitForStateChange(ctx, s) {
			// ctx done before the state changed.
			return fmt.Errorf("cluster: peer connection not ready (state %s): %w", s, ctx.Err())
		}
	}
}

func (c *peerClient) Close() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

// The *Forwarded variants set Forwarded=true on the wire request so
// the receiving server can reject the op if it does not own the key
// (instead of bouncing it back, which would loop A->B->A on diverged
// rings). pkg/cluster only forwards via these variants; the plain
// ones below remain for tests + future non-routed call sites.

func (c *peerClient) PutForwarded(ctx context.Context, key, value []byte) error {
	_, err := c.api.Put(ctx, &pb.PutRequest{Key: key, Value: value, Forwarded: true})
	return err
}

func (c *peerClient) GetForwarded(ctx context.Context, key []byte) ([]byte, bool, error) {
	resp, err := c.api.Get(ctx, &pb.GetRequest{Key: key, Forwarded: true})
	if err != nil {
		return nil, false, err
	}
	if resp.GetNotFound() {
		return nil, false, nil
	}
	return resp.GetValue(), true, nil
}

func (c *peerClient) DeleteForwarded(ctx context.Context, key []byte) error {
	_, err := c.api.Delete(ctx, &pb.DeleteRequest{Key: key, Forwarded: true})
	return err
}

func (c *peerClient) ScanPrefixForwarded(ctx context.Context, prefix []byte) (grpc.ServerStreamingClient[pb.ScanPrefixResponse], error) {
	return c.api.ScanPrefix(ctx, &pb.ScanPrefixRequest{Prefix: prefix, Forwarded: true})
}

// The *AtReplica variants are the POSITION-ADDRESSED overlap forward (v0.8
// Phase 2e): they carry the EXPLICIT ReplicaUnit on the wire so the predecessor
// (which no longer holds the moving position at its own ring index) resolves
// the mount for ru DIRECTLY and serves the Draining entry, rather than re-resolving
// the position from the key against its live ring. The new (Acquiring) owner
// forwards routed ops here while it mounts. Forwarded stays true so the
// receiving server treats it as a peer-to-peer op (no re-routing); ru carries
// the position. See multibackend_overlap_forward.go.

func replicaUnitRef(ru storageunit.ReplicaUnit) *pb.ReplicaUnitRef {
	return &pb.ReplicaUnitRef{
		Gen:     uint64(ru.Unit.Gen),
		Unit:    uint32(ru.Unit.ID),
		Replica: uint32(ru.Replica),
	}
}

func (c *peerClient) PutAtReplica(ctx context.Context, ru storageunit.ReplicaUnit, key, value []byte) error {
	_, err := c.api.Put(ctx, &pb.PutRequest{Key: key, Value: value, Forwarded: true, Ru: replicaUnitRef(ru)})
	return err
}

func (c *peerClient) GetAtReplica(ctx context.Context, ru storageunit.ReplicaUnit, key []byte) ([]byte, bool, error) {
	resp, err := c.api.Get(ctx, &pb.GetRequest{Key: key, Forwarded: true, Ru: replicaUnitRef(ru)})
	if err != nil {
		return nil, false, err
	}
	if resp.GetNotFound() {
		return nil, false, nil
	}
	return resp.GetValue(), true, nil
}

func (c *peerClient) DeleteAtReplica(ctx context.Context, ru storageunit.ReplicaUnit, key []byte) error {
	_, err := c.api.Delete(ctx, &pb.DeleteRequest{Key: key, Forwarded: true, Ru: replicaUnitRef(ru)})
	return err
}

// ScanPrefixAtReplica opens a POSITION-ADDRESSED forwarded scan stream (the
// union scan leg): the receiver resolves the explicit ru against its own
// mount map (LocalReplicaScanAt) with no ring-ownership guard, the exact
// mirror of GetAtReplica.
func (c *peerClient) ScanPrefixAtReplica(ctx context.Context, ru storageunit.ReplicaUnit, prefix []byte) (grpc.ServerStreamingClient[pb.ScanPrefixResponse], error) {
	return c.api.ScanPrefix(ctx, &pb.ScanPrefixRequest{Prefix: prefix, Forwarded: true, Ru: replicaUnitRef(ru)})
}

// The plain (non-forwarded) variants are still here for tests +
// future non-routed call sites; they leave Forwarded=false so the
// server treats them as a first-hop request and routes normally.

func (c *peerClient) Put(ctx context.Context, key, value []byte) error {
	_, err := c.api.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	return err
}

func (c *peerClient) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	resp, err := c.api.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return nil, false, err
	}
	if resp.GetNotFound() {
		return nil, false, nil
	}
	return resp.GetValue(), true, nil
}

func (c *peerClient) Delete(ctx context.Context, key []byte) error {
	_, err := c.api.Delete(ctx, &pb.DeleteRequest{Key: key})
	return err
}

func (c *peerClient) ScanPrefix(ctx context.Context, prefix []byte) (grpc.ServerStreamingClient[pb.ScanPrefixResponse], error) {
	return c.api.ScanPrefix(ctx, &pb.ScanPrefixRequest{Prefix: prefix})
}

// LocalScan is the admin-path scan: bypasses ring routing on the
// receiving node + streams its raw backend keys back. Used by
// Aggregate's snapshotPeer.
func (c *peerClient) LocalScan(ctx context.Context, prefix []byte) (grpc.ServerStreamingClient[pb.LocalScanResponse], error) {
	return c.api.LocalScan(ctx, &pb.LocalScanRequest{Prefix: prefix})
}

// CommitCAS ships a CAS validate-and-apply to the owning peer. The wire
// response carries the outcome as typed booleans (committed / conflict)
// plus an error string for backend / ownership failures; the caller maps
// those back into a casResult.
func (c *peerClient) CommitCAS(ctx context.Context, req *pb.CommitCASRequest) (*pb.CommitCASResponse, error) {
	return c.api.CommitCAS(ctx, req)
}

// ApplyBatch ships an owner-committed CAS write-set to a replica peer for
// apply-only fan-out. The envelopes are already Encode()d by the owner;
// the replica writes them verbatim in one local transaction. A non-empty
// response error means the replica rolled the batch back; a migration-
// guard rejection arrives as a gRPC codes.ResourceExhausted error (the
// caller's fanout classifies it transient). Mirrors PutForwarded: a
// cluster-internal owner-to-replica call, never made from outside the
// cluster.
func (c *peerClient) ApplyBatch(ctx context.Context, writes []EnvelopeWrite) error {
	req := &pb.ApplyBatchRequest{Writes: make([]*pb.EnvelopeWrite, len(writes))}
	for i, w := range writes {
		req.Writes[i] = &pb.EnvelopeWrite{Key: w.Key, Envelope: w.Envelope}
	}
	resp, err := c.api.ApplyBatch(ctx, req)
	if err != nil {
		return err
	}
	if e := resp.GetError(); e != "" {
		return errors.New("cluster: ApplyBatch replica error: " + e)
	}
	return nil
}

// GenState asks a seed for its live {generation, unit-count}. A JOINER calls
// it during Open (before mounting any unit) to learn the cluster's live
// generation, so it never routes / owns a key at gen 0 after the cluster has
// resharded; an in-flight answer (reshard_in_flight) makes the joiner retry.
// Cluster-internal: only a multi-backend Open WITH seeds calls it; never from
// outside the cluster.
func (c *peerClient) GenState(ctx context.Context) (*pb.GenStateResponse, error) {
	return c.api.GenState(ctx, &pb.GenStateRequest{})
}
