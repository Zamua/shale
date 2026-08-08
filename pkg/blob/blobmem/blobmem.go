// Package blobmem is an in-memory blob.Store for tests and local dev: it holds
// each object's bytes in a buffer keyed by object key, with a SETTABLE per-object
// ModTime so the orphan-sweep age-gate can be driven deterministically.
//
// It is the blob-plane analogue of memfactory (the in-memory BackendFactory):
// a real adapter (blobstore.MinioBlobStore) streams to object storage; this one
// streams into an in-process map so the cluster's *BlobKV paths can be exercised
// without a MinIO. It lives under internal/ so it is reusable across the core
// module's test trees without widening the public API, and it depends only on
// the pure pkg/blob port (no minio-go, no cgo), so it builds with plain
// `go build` / `go test` (no -tags).
//
// The store is fully synchronous and goroutine-safe (a single mutex); PutStream
// copies the reader to completion before returning (durable-write analogue), and
// GetStream returns a reader over a SNAPSHOT of the bytes (a concurrent Delete
// does not truncate an in-flight read).
package blobmem

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/Zamua/shale/pkg/blob"
)

// object is one stored blob's bytes.
type object struct {
	data []byte
}

// Store is an in-memory blob.Store. The zero value is NOT ready; use New.
type Store struct {
	mu  sync.Mutex
	obj map[string]object
}

// compile-time check: the fake satisfies the core port.
var _ blob.Store = (*Store)(nil)

// New returns an empty in-memory Store.
func New() *Store {
	return &Store{obj: make(map[string]object)}
}

// PutStream copies r in full into a buffer at objkey and stamps its ModTime from
// the clock. size is accepted for signature parity (the port lets a caller pass
// the known length) but is not required: the bytes are read to EOF either way,
// matching the streaming-to-completion contract. An existing object at objkey is
// overwritten (and re-stamped), as an object store would on re-PUT.
func (s *Store) PutStream(_ context.Context, objkey string, r io.Reader, _ int64) error {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.obj[objkey] = object{data: buf.Bytes()}
	return nil
}

// GetStream returns a reader over a SNAPSHOT of objkey's bytes plus its size. A
// missing object returns blob.ErrNotFound, mirroring the real adapter. The
// returned reader is over a copy, so a concurrent Delete or overwrite does not
// affect an in-flight read; Close is a no-op but is still required by the port.
func (s *Store) GetStream(_ context.Context, objkey string) (io.ReadCloser, int64, error) {
	s.mu.Lock()
	o, ok := s.obj[objkey]
	s.mu.Unlock()
	if !ok {
		return nil, 0, blob.ErrNotFound
	}
	snap := make([]byte, len(o.data))
	copy(snap, o.data)
	return io.NopCloser(bytes.NewReader(snap)), int64(len(snap)), nil
}

// Delete removes objkey. Idempotent: deleting a missing object is a no-op
// returning nil, the property the orphan sweep relies on.
func (s *Store) Delete(_ context.Context, objkey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.obj, objkey)
	return nil
}

// Has reports whether objkey exists, without transferring bytes.
func (s *Store) Has(_ context.Context, objkey string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.obj[objkey]
	return ok, nil
}

// Len reports how many objects the store currently holds. Test convenience for
// asserting exactly the expected objects remain after a stage / unstage.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.obj)
}
