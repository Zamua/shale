package cluster

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/Zamua/shale/pkg/backend"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

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
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &peerClient{conn: conn, api: pb.NewShaleNodeClient(conn)}, nil
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
	if c.closed.Load() || c.backend == nil {
		return backend.ErrClosed
	}
	// Re-resolve the owner from the LIVE ring in case it moved between
	// pin and commit. When the owner is us, take the in-process fast-path
	// (CommitCASApply, no RPC); otherwise serialize onto the wire and
	// call the peer's CommitCAS RPC.
	curOwner, isLocal := c.ownerOf(pinKey)
	if isLocal {
		res := c.CommitCASApply(context.Background(), level, reads, writes)
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
	resp, err := cli.CommitCAS(context.Background(), req)
	if err != nil {
		return err
	}
	if resp.GetConflict() {
		return backend.ErrCASConflict
	}
	if e := resp.GetError(); e != "" {
		return errors.New("cluster: CommitCAS owner error: " + e)
	}
	return nil
}

// casResultToError maps a backend.CASResult into the error the cluster
// surface returns: nil on committed, backend.ErrCASConflict on conflict,
// the backend / ownership error otherwise.
func casResultToError(r backend.CASResult) error {
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
	ownerID string // ring owner of the pinned shard; the cross-shard guard compares against this
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
		entries[i] = snapshotEntry{key: []byte(k), value: append([]byte(nil), s.data[k]...)}
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
