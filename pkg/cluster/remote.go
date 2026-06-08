package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/Zamua/shale/pkg/backend"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// clusterTx is the multi-node transaction wrapper returned by
// Cluster.Begin. It defers opening the underlying Backend transaction
// until the first key is touched: the shard pinning decision needs a
// key in hand, which cluster.Begin alone does not see.
//
// Once pinned, every subsequent key must shard to the same owner.
// Cross-shard touches return backend.ErrCrossShard so the caller sees
// the limitation at the offending operation, not later in Commit.
//
// In v0.2, only locally-pinned transactions execute; a remote pin
// returns an ErrCrossShard-wrapped error on first use until the gRPC
// transaction-proxy work lands. This is intentional: silently routing
// transactional work to the wrong backend would be a correctness bug,
// so we surface the gap at the call site.
type clusterTx struct {
	c     *Cluster
	level backend.IsolationLevel

	mu      sync.Mutex
	pinned  bool
	ownerID string
	tx      backend.Transaction
	done    bool
}

func (t *clusterTx) prepare(key []byte) (backend.Transaction, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return nil, errors.New("cluster: transaction already finalized")
	}
	owner, local := t.c.ownerOf(key)
	if !t.pinned {
		t.pinned = true
		t.ownerID = owner.ID
		if !local {
			return nil, fmt.Errorf("%w: v0.2 cannot proxy transactions to remote owner %s", backend.ErrCrossShard, owner.ID)
		}
		tx, err := t.c.backend.Begin(t.level)
		if err != nil {
			return nil, err
		}
		t.tx = tx
		return tx, nil
	}
	if owner.ID != t.ownerID {
		return nil, backend.ErrCrossShard
	}
	return t.tx, nil
}

func (t *clusterTx) Get(key []byte) ([]byte, error) {
	tx, err := t.prepare(key)
	if err != nil {
		return nil, err
	}
	return tx.Get(key)
}

func (t *clusterTx) Put(key, value []byte) error {
	tx, err := t.prepare(key)
	if err != nil {
		return err
	}
	return tx.Put(key, value)
}

func (t *clusterTx) Delete(key []byte) error {
	tx, err := t.prepare(key)
	if err != nil {
		return err
	}
	return tx.Delete(key)
}

func (t *clusterTx) ScanPrefix(prefix []byte) (backend.Iterator, error) {
	tx, err := t.prepare(prefix)
	if err != nil {
		return nil, err
	}
	return tx.ScanPrefix(prefix)
}

func (t *clusterTx) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done = true
	if t.tx == nil {
		return nil
	}
	return t.tx.Commit()
}

func (t *clusterTx) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done = true
	if t.tx == nil {
		return nil
	}
	return t.tx.Rollback()
}

// remoteIterator adapts a server-streaming ScanPrefix RPC into the
// backend.Iterator interface so cluster.ScanPrefix can return a single
// iterator type regardless of whether the owning shard is local or
// remote. The wrapped stream is drained lazily on each Next() call;
// Close cancels the underlying context (which closes the stream) and
// is safe to call any number of times.
type remoteIterator struct {
	stream grpc.ServerStreamingClient[pb.ScanPrefixResponse]
	cancel context.CancelFunc

	closeOnce sync.Once
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
		return nil, nil, err
	}
	return msg.GetKey(), msg.GetValue(), nil
}

func (it *remoteIterator) Close() error {
	it.closeOnce.Do(func() {
		it.cancel()
	})
	return nil
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
