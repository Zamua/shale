package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
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
// mountMap[ru] DIRECTLY and serves the Draining entry, rather than re-resolving
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

// ReshardControl drives one barrier phase of the v0.8 multi-node reshard
// (cluster-wide freeze) to a peer node. Cluster-internal: only the coordinator
// (the node where Reshard was called on a multi-node cluster) calls it, once
// per phase per peer. The receiving node's idempotent phase handler acks via an
// empty response error; a non-empty error string travels back as an ABORT
// trigger for the coordinator.
func (c *peerClient) ReshardControl(ctx context.Context, phase pb.ReshardPhase, targetGen uint64) (*pb.ReshardControlResponse, error) {
	return c.api.ReshardControl(ctx, &pb.ReshardControlRequest{Phase: phase, TargetGen: targetGen})
}

// GenState asks a seed for its live {generation, unit-count}. A JOINER calls
// it exactly once during Open (before mounting any unit) to learn the
// cluster's live generation, so it never routes / owns a key at gen 0 after
// the cluster has resharded. Cluster-internal: only a multi-backend Open WITH
// seeds calls it; never from outside the cluster.
func (c *peerClient) GenState(ctx context.Context) (*pb.GenStateResponse, error) {
	return c.api.GenState(ctx, &pb.GenStateRequest{})
}

// txRoutedGet performs the normal single-key routed Get the CAS read-set
// records against. It reuses Cluster.Get so a read inside a transaction
// sees exactly what a standalone Get would (same local/remote routing,
// same replication read path). Returns backend.ErrNotFound on absence.
func (c *Cluster) txRoutedGet(key []byte) ([]byte, error) {
	return c.Get(key)
}

// commitCAS dispatches a CAS commit to the pinned shard owner. When the
// owner is this node it is an in-process fast-path (CommitCASApply, no
// RPC); otherwise it serializes the read-set + write-set onto the wire
// and calls the peer's CommitCAS RPC. Either way a reported conflict maps
// to backend.ErrCASConflict; a backend / ownership failure (including the
// owner reporting it no longer owns the pin key) surfaces as that error.
func (c *Cluster) commitCAS(pinKey []byte, level backend.IsolationLevel, reads []backend.ReadCheck, writes []backend.WriteOp) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	// Re-resolve the DESIGNATED owner from the LIVE view in case it moved
	// between pin and commit. casDesignatedOwner is ownerOf in steady state; only
	// during a join transition whose full-ring head is a warming (Joining)
	// newcomer does it re-designate to the displaced still-mounted current head,
	// so the commit lands on a node that can actually serve the owner-local
	// transaction (see docs/SPEC.md "CAS during a transition"). When the
	// designated owner is us, take the in-process fast-path (CommitCASApply, no
	// RPC); otherwise serialize onto the wire and call the peer's CommitCAS RPC.
	curOwner, isLocal := c.casDesignatedOwner(pinKey)
	if isLocal {
		res := c.CommitCASApply(context.Background(), level, pinKey, reads, writes)
		if res.Committed && res.UnderReplicated {
			// Committed durably on this (owner) node but the fan-out missed W.
			// Log the degraded replication ONCE (res.Err is the under-W detail)
			// before casResultToError maps Committed==true to nil - a success, so
			// Transact does NOT re-run fn.
			detail := "replica fan-out missed W"
			if res.Err != nil {
				detail = res.Err.Error()
			}
			c.warnUnderReplicated(pinKey, detail)
		}
		return casResultToError(res)
	}
	cli, err := c.clientFor(curOwner.Addr)
	if err != nil {
		return err
	}
	req := &pb.CommitCASRequest{
		PinKey:         pinKey,
		IsolationLevel: int32(level),
		Reads:          make([]*pb.ReadCheck, len(reads)),
		Writes:         make([]*pb.WriteOp, len(writes)),
	}
	for i, r := range reads {
		req.Reads[i] = &pb.ReadCheck{Key: r.Key, ExpectedValue: r.ExpectedVal, ExpectAbsent: r.ExpectAbsent}
	}
	for i, w := range writes {
		req.Writes[i] = &pb.WriteOp{Key: w.Key, Value: w.Value, Delete: w.Del}
	}
	// Bound the forwarded commit so an unresponsive owner times out rather
	// than wedging the caller (same rationale as cluster.go's forwarded RPCs).
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.WriteTimeout)
	defer cancel()
	resp, err := cli.CommitCAS(ctx, req)
	if err != nil {
		return err
	}
	if resp.GetConflict() {
		return backend.ErrCASConflict
	}
	// Committed==true wins over any error string, mirroring casResultToError's
	// ordering: the remote owner's owner-local commit durably applied, so the
	// commit SUCCEEDED even when the fan-out missed W (under_replicated). The
	// forwarding node must return nil so Transact does NOT re-run fn and re-
	// commit an already-durable write. The owner already logged the under-W
	// detail; note the degraded replication on the forwarding node too.
	if resp.GetCommitted() {
		if resp.GetUnderReplicated() {
			c.warnUnderReplicated(pinKey, "owner reported W shortfall on the replica fan-out")
		}
		return nil
	}
	if e := resp.GetError(); e != "" {
		return errors.New("cluster: CommitCAS owner error: " + e)
	}
	return nil
}

// warnUnderReplicated writes a single WARN to the configured log sink when a
// CAS commit committed durably on the owner but replicated to fewer than W
// replicas (outcome (c), docs/SPEC.md "The four commit outcomes"). Best-effort
// observability, gated on cfg.LogOutput (nil sink => silent; the test default
// is io.Discard). Rate-reasonable by construction: a committed-under-
// replication result is now a SUCCESS and is NOT retried, so this fires at most
// once per under-replicated commit, never once per re-run. detail is the under-
// W fan-out error (owner path) or a short owner-reported note (forwarded path).
func (c *Cluster) warnUnderReplicated(pinKey []byte, detail string) {
	if c.cfg.LogOutput == nil {
		return
	}
	if c.multi {
		_, _ = fmt.Fprintf(c.cfg.LogOutput,
			"WARN cluster: CAS commit under-replicated (durable on owner, missed W): key=%x unit=%s: %s\n",
			pinKey, c.genUnitForKey(pinKey), detail)
		return
	}
	_, _ = fmt.Fprintf(c.cfg.LogOutput,
		"WARN cluster: CAS commit under-replicated (durable on owner, missed W): key=%x: %s\n",
		pinKey, detail)
}

// casResultToError maps a backend.CASResult into the error the cluster
// surface (clusterTx.Commit -> Transact) sees. It is the tx-commit-to-error
// conversion the retry loop keys off, so the ordering of the checks is
// load-bearing:
//
//   - Committed==true FIRST: the owner-local commit durably applied, so the
//     transaction SUCCEEDED even when the replica fan-out missed W
//     (UnderReplicated, with Err carrying the under-W detail for logging).
//     Returns nil so Transact does NOT re-run fn - re-running would re-commit
//     an already-durable write (amplification) and could make a retried
//     insert observe its own committed write as a false conflict. The caller
//     (commitCAS) logs the degraded replication once before this maps to nil.
//   - Conflict: the OCC retry signal, backend.ErrCASConflict.
//   - Err (with Committed==false): a genuine not-committed failure - a
//     backend / ownership error, or a pre-commit retryable freeze / fence /
//     reshard-cutover refusal that applied nothing and IS re-run by Transact.
//
// See docs/SPEC.md "The four commit outcomes".
func casResultToError(r backend.CASResult) error {
	if r.Committed {
		// Success, possibly under-replicated. r.Err (if any) is the under-W
		// detail, kept for the caller's WARN, and is deliberately NOT returned.
		return nil
	}
	if r.Conflict {
		return backend.ErrCASConflict
	}
	if r.Err != nil {
		return r.Err
	}
	return nil
}

// clusterTx is the CAS-buffered transaction returned by Cluster.Begin
// (and driven by Cluster.Transact). It is a BUFFER, not a live backend
// session: it never holds an open backend.Transaction across a network
// round-trip. See docs/SPEC.md "Single-shard transactions (CAS / OCC)".
//
// Lazy pinning: the shard is pinned on the first key touched (the first
// Get / Put / Delete), to whichever node the ring says owns that key's
// shard. Every subsequent key MUST shard to that same owner; a key that
// shards elsewhere returns backend.ErrCrossShard at the offending
// operation (the cross-shard guard, the load-bearing correctness
// property). This holds whether the pinned shard is local or remote: a
// genuinely cross-shard transaction STILL fails with ErrCrossShard.
//
// Buffering:
//   - Get does a real routed Get (the normal single-key read path,
//     local-or-remote) and records (key, value-seen) in the read-set; a
//     not-found records an expect_absent entry. A Get of a key the tx
//     itself already wrote is served from the write buffer (read-your-
//     writes) with no round-trip and adds NO read-check.
//   - Put / Delete buffer into the write-set; they do not hit the owner.
//   - Commit assembles the read-set + write-set and sends ONE CommitCAS
//     to the pinned owner (an in-process fast-path when the owner is this
//     node, no RPC). A reported conflict maps to backend.ErrCASConflict.
//   - Rollback / abandoning the tx is purely local: nothing was sent to
//     the owner, so there is nothing to undo.
type clusterTx struct {
	c     *Cluster
	level backend.IsolationLevel

	mu      sync.Mutex
	pinned  bool
	pinKey  []byte
	ownerID string // legacy mode: ring owner NODE of the pinned shard; the cross-shard guard compares against this
	// pinUnit is the generation-qualified storage UNIT of the pinned key in
	// multi-backend mode. The guard compares units (not owner nodes) there: a
	// node owns many units, so a single owner-node check would wrongly admit
	// two keys on different units of the same node into one transaction, which
	// commits against only the pin unit (a co-location split / silent loss).
	// Qualified by generation so a key resolved before a reshard cut-over and
	// one resolved after are correctly seen as different units.
	pinUnit storageunit.GenUnit
	done    bool

	// reads is the read-set: keys the tx READ from the cluster (and did
	// not itself write), in first-seen order, deduped by readSeen.
	reads    []backend.ReadCheck
	readSeen map[string]int // key -> index into reads (for dedupe)

	// writeBuf is the buffered write-set in order. writeIdx maps a key to
	// its latest entry so read-your-writes can serve the buffered value
	// and a re-write of the same key overwrites in place rather than
	// appending a duplicate.
	writeBuf []backend.WriteOp
	writeIdx map[string]int
}

// newCASTx constructs an empty CAS-buffered transaction.
func (c *Cluster) newCASTx(level backend.IsolationLevel) *clusterTx {
	return &clusterTx{
		c:        c,
		level:    level,
		readSeen: make(map[string]int),
		writeIdx: make(map[string]int),
	}
}

// pin records the pin key without performing an operation. Transact calls
// it so the shard is fixed to the caller-supplied pinKey even if fn's
// first touched key differs (they must still co-shard, enforced on that
// first op). A no-op if already pinned.
func (t *clusterTx) pin(key []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pinLocked(key)
}

// pinLocked pins the shard on key if not already pinned. Caller holds mu.
// Pinning is a pure ring lookup that cannot fail; the owner Addr is
// resolved fresh from the live ring at Commit time, so the tx records
// only the owner ID (for the cross-shard equality check) and the pin key.
func (t *clusterTx) pinLocked(key []byte) {
	if t.pinned {
		return
	}
	owner, _ := t.c.ownerOf(key)
	t.pinned = true
	t.pinKey = append([]byte(nil), key...)
	t.ownerID = owner.ID
	if t.c.multi {
		t.pinUnit = t.c.genUnitForKey(key)
	}
}

// guardShard pins on first touch + enforces the cross-shard guard on
// every subsequent key. Caller holds mu. Returns backend.ErrCrossShard
// if key shards to a different owner than the pinned one.
func (t *clusterTx) guardShard(key []byte) error {
	if t.done {
		return errors.New("cluster: transaction already finalized")
	}
	if !t.pinned {
		t.pinLocked(key)
		return nil
	}
	// Multi-backend: the transactable boundary is the storage UNIT (one
	// slatedb engine), not the owner node. A node owns many units, so
	// comparing owner nodes would admit a cross-unit transaction that then
	// commits against only the pin unit. Compare units.
	//
	// TODO(reshard-tx): pinUnit is captured at pin time and genUnitForKey here
	// reads the CURRENT genState. If a reshard cut-over for the pin key's old
	// unit commits between the pin and a later same-shard op, the same shard
	// key resolves to a different GenUnit and this returns a SPURIOUS
	// ErrCrossShard, failing a legitimate single-node transaction mid-reshard.
	// Transactions during a reshard are rare (reshard is an explicit op) and
	// this is not data-loss, but the fix is to resolve the pin unit and all
	// guardShard comparisons against the generation captured AT pin time (a
	// stable snapshot for the tx lifetime) so a mid-tx cut-over does not flip
	// the comparison. The reshard cut-over should also be made tx-aware.
	if t.c.multi {
		if t.c.genUnitForKey(key) != t.pinUnit {
			return backend.ErrCrossShard
		}
		return nil
	}
	owner, _ := t.c.ownerOf(key)
	if owner.ID != t.ownerID {
		return backend.ErrCrossShard
	}
	return nil
}

func (t *clusterTx) Get(key []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.guardShard(key); err != nil {
		return nil, err
	}
	// Read-your-writes: a key the tx already wrote is served from the
	// buffer and adds no read-check (validating it against pre-write
	// state would be wrong: the tx itself changed it).
	if idx, ok := t.writeIdx[string(key)]; ok {
		w := t.writeBuf[idx]
		if w.Del {
			return nil, backend.ErrNotFound
		}
		return append([]byte(nil), w.Value...), nil
	}
	// Routed Get against the cluster. Released the lock would let a
	// concurrent op race the read-set; the routed Get is a single network
	// op and clusterTx is not advertised as goroutine-safe across
	// concurrent ops on the SAME tx, so holding mu here is fine.
	val, err := t.c.txRoutedGet(key)
	if err != nil && !errors.Is(err, backend.ErrNotFound) {
		return nil, err
	}
	absent := errors.Is(err, backend.ErrNotFound)
	t.recordRead(key, val, absent)
	if absent {
		return nil, backend.ErrNotFound
	}
	return val, nil
}

// recordRead adds (or refreshes) a read-check for key. Caller holds mu.
// The FIRST observation of a key is the snapshot the OCC validate checks
// against; a later Get of the same key (without an intervening write)
// keeps the first-seen expectation rather than overwriting it, so the
// read-set stays a faithful record of what the client computed against.
func (t *clusterTx) recordRead(key, val []byte, absent bool) {
	if _, ok := t.readSeen[string(key)]; ok {
		return
	}
	rc := backend.ReadCheck{Key: append([]byte(nil), key...), ExpectAbsent: absent}
	if !absent {
		rc.ExpectedVal = append([]byte(nil), val...)
	}
	t.readSeen[string(key)] = len(t.reads)
	t.reads = append(t.reads, rc)
}

func (t *clusterTx) Put(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.guardShard(key); err != nil {
		return err
	}
	t.bufferWrite(backend.WriteOp{Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
	return nil
}

func (t *clusterTx) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.guardShard(key); err != nil {
		return err
	}
	t.bufferWrite(backend.WriteOp{Key: append([]byte(nil), key...), Del: true})
	return nil
}

// bufferWrite appends w to the write-set, or overwrites the prior entry
// for the same key in place (last-write-wins within the tx). Caller holds
// mu. The key is now "owned" by the tx, so subsequent Gets read-your-
// writes from here.
func (t *clusterTx) bufferWrite(w backend.WriteOp) {
	if idx, ok := t.writeIdx[string(w.Key)]; ok {
		t.writeBuf[idx] = w
		return
	}
	t.writeIdx[string(w.Key)] = len(t.writeBuf)
	t.writeBuf = append(t.writeBuf, w)
}

// ScanPrefix inside a CAS-buffered transaction is not supported in v0.6:
// a scanned range cannot be cheaply turned into a read-set of discrete
// key checks, and validating a range against concurrent inserts needs
// phantom protection the value-based read-set does not provide. Callers
// scan outside the transaction. See docs/SPEC.md "Begin vs Transact".
func (t *clusterTx) ScanPrefix(_ []byte) (backend.Iterator, error) {
	return nil, errors.New("cluster: ScanPrefix is not supported inside a CAS transaction; scan outside the transaction (see docs/SPEC.md)")
}

// Commit assembles the buffered read-set + write-set and ships ONE
// CommitCAS to the pinned shard owner. A nil-buffer tx (nothing touched,
// or a read-only tx with nothing to validate against) commits trivially.
// A reported conflict returns backend.ErrCASConflict; a backend /
// ownership failure returns that error.
func (t *clusterTx) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return errors.New("cluster: transaction already finalized")
	}
	t.done = true

	// Nothing to commit: no shard was pinned (the tx touched no key) or
	// the tx had no writes and no reads. Either way there is no state to
	// validate-and-apply; succeed trivially.
	if !t.pinned || (len(t.writeBuf) == 0 && len(t.reads) == 0) {
		return nil
	}

	return t.c.commitCAS(t.pinKey, t.level, t.reads, t.writeBuf)
}

// Rollback abandons the buffer. Purely local: nothing was sent to the
// owner (Commit is the only thing that ships), so there is nothing to
// undo. Idempotent with Commit via the done flag.
func (t *clusterTx) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done = true
	return nil
}

// remoteIterator adapts a server-streaming ScanPrefix RPC into the
// backend.Iterator interface so cluster.ScanPrefix can return a single
// iterator type regardless of whether the owning shard is local or
// remote. The wrapped stream is drained lazily on each Next() call;
// Close cancels the underlying context (which closes the stream) and
// is safe to call any number of times.
//
// End-of-stream contract normalization: when Close has been called
// locally, a subsequent Next that sees a gRPC Canceled error is the
// expected close path - we surface (nil, nil, nil), matching the
// memory backend's natural end-of-iteration. A Canceled error WITHOUT
// a local Close is a real failure (the remote side dropped the
// stream) + propagates up.
type remoteIterator struct {
	stream grpc.ServerStreamingClient[pb.ScanPrefixResponse]
	cancel context.CancelFunc

	closeOnce sync.Once
	closing   atomic.Bool // set by Close before cancel()
	done      bool
}

func (it *remoteIterator) Next() (key, value []byte, err error) {
	if it.done {
		return nil, nil, nil
	}
	msg, err := it.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			it.done = true
			return nil, nil, nil
		}
		// If the cancel came from a local Close, treat as natural EOI.
		if it.closing.Load() && isContextCanceled(err) {
			it.done = true
			return nil, nil, nil
		}
		return nil, nil, err
	}
	return msg.GetKey(), msg.GetValue(), nil
}

func (it *remoteIterator) Close() error {
	it.closeOnce.Do(func() {
		// Set closing BEFORE cancel so a Next racing on the stream
		// observes "we're closing" and turns Canceled into clean EOI.
		it.closing.Store(true)
		it.cancel()
	})
	return nil
}

// isContextCanceled reports whether err originated from a context
// cancellation (either context.Canceled or gRPC's Canceled status
// code wrapping the same). Used by remoteIterator.Next to decide
// whether a post-Close Recv error is benign end-of-stream or a real
// failure on the remote side.
func isContextCanceled(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.Canceled
	}
	return false
}

// snapshotBackend is the in-memory backend used by Aggregate to give
// peer fn invocations a read-only view of a peer's keyspace. Only
// Put + ScanPrefix are actually used by Aggregate's flow (Put to load
// the snapshot, ScanPrefix for the caller); the rest exist to satisfy
// the backend.Backend interface in case fn calls them.
type snapshotBackend struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newSnapshotBackend() *snapshotBackend {
	return &snapshotBackend{data: make(map[string][]byte)}
}

func (s *snapshotBackend) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (s *snapshotBackend) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[string(key)]
	if !ok {
		return nil, backend.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (s *snapshotBackend) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, string(key))
	return nil
}

func (s *snapshotBackend) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		if len(prefix) == 0 || hasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	entries := make([]snapshotEntry, len(keys))
	for i, k := range keys {
		// Borrow the stored value slice rather than copying it. The
		// snapshot is transient, read-only, and discarded after the
		// single fn invocation that owns it, so the standard read-only
		// backend.Iterator contract (don't mutate the returned bytes)
		// holds; a per-value defensive copy here is pure waste.
		entries[i] = snapshotEntry{key: []byte(k), value: s.data[k]}
	}
	return &snapshotIterator{entries: entries}, nil
}

func (s *snapshotBackend) Begin(backend.IsolationLevel) (backend.Transaction, error) {
	return nil, errors.New("cluster: snapshot backend does not support transactions")
}

func (s *snapshotBackend) Close() error { return nil }

func hasPrefix(s string, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i, b := range prefix {
		if s[i] != b {
			return false
		}
	}
	return true
}

type snapshotEntry struct {
	key, value []byte
}

type snapshotIterator struct {
	entries []snapshotEntry
	i       int
}

func (it *snapshotIterator) Next() ([]byte, []byte, error) {
	if it.i >= len(it.entries) {
		return nil, nil, nil
	}
	e := it.entries[it.i]
	it.i++
	return e.key, e.value, nil
}

func (it *snapshotIterator) Close() error { return nil }

// streamBackend is the streaming read-only view Aggregate hands to
// scan/count fns (the documented Stats / keys_held / cross-shard use).
// Unlike snapshotBackend it does NOT drain a peer's whole keyspace into
// memory first: each ScanPrefix opens a fresh LocalScan stream and the
// returned iterator pulls (key, value) pairs straight off stream.Recv,
// so peak memory is one in-flight pair rather than the peer's entire
// keyspace. The fn contract (func(backend.Backend) any) is preserved
// for any consumer that only scans/counts.
//
// Random-access Get is NOT supported (a stream cannot seek); callers
// that need Get must use the materializing snapshotPeer path instead.
// All existing Aggregate consumers scan via ScanPrefix only.
//
// The cancel func tears down every stream this view opened; Aggregate
// invokes it after fn returns (the streams must outlive fn, so the
// caller owns the cancel, not snapshotPeer).
type streamBackend struct {
	cli    *peerClient
	ctx    context.Context
	cancel context.CancelFunc

	// primed holds the full-keyspace stream snapshotPeer opened +
	// primed eagerly (first Recv already done, to surface transport
	// failure at snapshot time). The first full-keyspace ScanPrefix
	// (prefix nil) consumes it so the primed round-trip is not wasted;
	// once consumed, mu/primedUsed guard against a second consumer.
	mu         sync.Mutex
	primed     *primedStream
	primedUsed bool
}

func (s *streamBackend) Put([]byte, []byte) error {
	return errors.New("cluster: streaming snapshot backend is read-only")
}

func (s *streamBackend) Get([]byte) ([]byte, error) {
	return nil, errors.New("cluster: streaming snapshot backend does not support random-access Get")
}

func (s *streamBackend) Delete([]byte) error {
	return errors.New("cluster: streaming snapshot backend is read-only")
}

// ScanPrefix returns a streaming iterator over the peer's keys matching
// prefix. The first full-keyspace scan (prefix nil) reuses the stream
// snapshotPeer already opened + primed, so the priming round-trip is
// not thrown away; any later scan (or any prefixed scan) opens a fresh
// LocalScan stream scoped to prefix. The peer applies the prefix filter
// server-side and streams matching keys in the same key-ascending order
// the materializing path observed, so scan/count fns produce identical
// results.
func (s *streamBackend) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	if len(prefix) == 0 {
		s.mu.Lock()
		if !s.primedUsed {
			s.primedUsed = true
			p := s.primed
			s.mu.Unlock()
			return &streamIterator{primed: p}, nil
		}
		s.mu.Unlock()
	}
	stream, err := s.cli.LocalScan(s.ctx, prefix)
	if err != nil {
		return nil, err
	}
	return &streamIterator{stream: stream}, nil
}

func (s *streamBackend) Begin(backend.IsolationLevel) (backend.Transaction, error) {
	return nil, errors.New("cluster: streaming snapshot backend does not support transactions")
}

func (s *streamBackend) Close() error {
	s.cancel()
	return nil
}

// primedStream wraps a LocalScan stream whose first Recv was already
// performed eagerly by snapshotPeer (to surface transport failure at
// snapshot time). hasFirst/first carry that buffered first message;
// done means the eager Recv already hit EOF (empty keyspace).
type primedStream struct {
	stream   grpc.ServerStreamingClient[pb.LocalScanResponse]
	first    *pb.LocalScanResponse
	hasFirst bool
	done     bool
}

// streamIterator adapts a LocalScan gRPC stream to backend.Iterator.
// Next pulls one message per call; io.EOF maps to the (nil, nil, nil)
// exhausted signal the Iterator contract specifies. When backed by a
// primedStream the buffered first message is yielded before falling
// through to live Recv calls.
type streamIterator struct {
	stream grpc.ServerStreamingClient[pb.LocalScanResponse]
	primed *primedStream
}

func (it *streamIterator) Next() ([]byte, []byte, error) {
	if it.primed != nil {
		p := it.primed
		if p.done {
			return nil, nil, nil
		}
		if p.hasFirst {
			p.hasFirst = false
			return p.first.GetKey(), p.first.GetValue(), nil
		}
		// primed buffer drained; pull live from its underlying stream.
		it.stream = p.stream
		it.primed = nil
	}
	msg, err := it.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	return msg.GetKey(), msg.GetValue(), nil
}

func (it *streamIterator) Close() error { return nil }
