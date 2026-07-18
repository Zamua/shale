package cluster

// aggregate_snapshot.go holds the ITERATOR + SNAPSHOT adapters that let a
// cross-node read present as one backend.Backend: remoteIterator (a
// server-streaming ScanPrefix RPC as a backend.Iterator), snapshotBackend (a
// materialized in-memory copy of a peer's keyspace) and streamBackend /
// primedStream (the lazy, streaming sibling that never materializes). Consumed
// by aggregate.go's fan-out. See doc.go.

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
	"google.golang.org/grpc/status"
)

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
