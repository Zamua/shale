// Package blobstore is the MinIO/S3 implementation of the core blob.Store
// port - the streaming byte plane of shale's value-separation model.
//
// It lives in a BACKEND module (next to the slate ConditionalStore, which it
// reuses the minio client pattern from) rather than in the light core module:
// minio-go is an object-store client the memory-backed reference binary must
// not pull in. The core pkg/blob package holds only the port + the pure domain
// types; this is the lone object-store adapter. It depends only on minio-go (no
// slatedb / cgo), so it builds and tests with plain `go build` / `go test` (no
// -tags slatedb).
//
// The streaming guarantee is load-bearing: PutStream hands the caller's
// io.Reader straight to PutObject (which streams it) and GetStream returns the
// GetObject reader directly - neither ever buffers a whole blob in memory.
package blobstore

import (
	"context"
	"fmt"
	"io"
	"iter"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Zamua/shale/pkg/blob"
)

// MinioBlobStore implements blob.Store over one bucket.
type MinioBlobStore struct {
	client *minio.Client
	bucket string
	prefix string // prepended to every objkey; may be empty
}

// Config configures a MinioBlobStore. It mirrors the slate
// MinioConditionalStoreConfig shape so an operator wiring both against the same
// object store uses one familiar config surface.
type Config struct {
	EndpointHost string // "host:port", no scheme
	AccessKey    string
	SecretKey    string
	UseSSL       bool
	Bucket       string
	KeyPrefix    string // namespaces this store's objects within the bucket; may be empty
}

// compile-time check: the adapter satisfies the core port.
var _ blob.Store = (*MinioBlobStore)(nil)

// New builds a MinioBlobStore against the bucket, reusing the exact minio.New +
// static-V4-credentials client pattern the slate ConditionalStore adapter uses.
// The bucket must already exist.
func New(cfg Config) (*MinioBlobStore, error) {
	mc, err := minio.New(cfg.EndpointHost, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("blobstore: minio client: %w", err)
	}
	return &MinioBlobStore{client: mc, bucket: cfg.Bucket, prefix: cfg.KeyPrefix}, nil
}

func (s *MinioBlobStore) keyFor(objkey string) string { return s.prefix + objkey }

// PutStream streams r to objkey. When size >= 0 it is passed to PutObject so the
// store can choose a single-shot or sized multipart PUT; when size is
// blob.SizeUnknown (-1) minio-go performs a streaming multipart upload that does
// not need the total length in advance (it buffers only one part at a time,
// never the whole blob). Either way r is streamed, never read fully into memory.
func (s *MinioBlobStore) PutStream(ctx context.Context, objkey string, r io.Reader, size int64) error {
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	if _, err := s.client.PutObject(ctx, s.bucket, s.keyFor(objkey), r, size, opts); err != nil {
		return fmt.Errorf("blobstore: PutStream %s: %w", objkey, err)
	}
	return nil
}

// GetStream opens objkey for streaming reads. It Stats first to resolve the
// object (a missing object surfaces here as NoSuchKey -> blob.ErrNotFound) and
// to learn the size, then returns the SAME object handle as the ReadCloser so
// the caller streams the bytes directly. It does NOT io.ReadAll the object.
//
// LIFETIME: minio-go's GetObject spawns a reader goroutine bound to ctx, and
// Close cancels it. So the passed ctx MUST outlive the returned reader (the
// whole duration of streaming the bytes downstream), not merely the GetStream
// call - a ctx scoped to "assemble the response" would truncate a large stream
// mid-flight. The caller MUST Close the returned reader (it releases that
// goroutine + the request context).
func (s *MinioBlobStore) GetStream(ctx context.Context, objkey string) (io.ReadCloser, int64, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.keyFor(objkey), minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, fmt.Errorf("blobstore: GetStream %s: %w", objkey, err)
	}
	// Stat resolves the underlying GET without reading the body; a missing
	// object surfaces here as NoSuchKey, which is blob.ErrNotFound.
	st, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		if isNotFound(err) {
			return nil, 0, blob.ErrNotFound
		}
		return nil, 0, fmt.Errorf("blobstore: GetStream stat %s: %w", objkey, err)
	}
	return obj, st.Size, nil
}

// Delete removes objkey. It is idempotent: RemoveObject against a missing object
// returns no error from S3/MinIO, so a delete of an already-reclaimed object is
// a no-op (the orphan sweep relies on this).
func (s *MinioBlobStore) Delete(ctx context.Context, objkey string) error {
	if err := s.client.RemoveObject(ctx, s.bucket, s.keyFor(objkey), minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("blobstore: Delete %s: %w", objkey, err)
	}
	return nil
}

// Has reports whether objkey exists, via StatObject (a HEAD - no bytes
// transferred). A NoSuchKey response means "no", not an error.
func (s *MinioBlobStore) Has(ctx context.Context, objkey string) (bool, error) {
	_, err := s.client.StatObject(ctx, s.bucket, s.keyFor(objkey), minio.StatObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("blobstore: Has %s: %w", objkey, err)
	}
	return true, nil
}

// List yields every object key under prefix (with this store's KeyPrefix
// stripped back off, so callers see the same key space they pass to the other
// methods). It is the input to the phase-2 same-shard orphan-bytes sweep.
func (s *MinioBlobStore) List(ctx context.Context, prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		opts := minio.ListObjectsOptions{Prefix: s.keyFor(prefix), Recursive: true}
		for obj := range s.client.ListObjects(ctx, s.bucket, opts) {
			if obj.Err != nil {
				yield("", fmt.Errorf("blobstore: List %s: %w", prefix, obj.Err))
				return
			}
			// TrimPrefix fails safe: every listed key starts with s.prefix
			// (ListObjects was given keyFor(prefix)), and if one ever did not it
			// is returned unchanged rather than blindly length-sliced.
			if !yield(strings.TrimPrefix(obj.Key, s.prefix), nil) {
				return
			}
		}
	}
}

// isNotFound maps the object store's NoSuchKey/NoSuchBucket response onto a
// not-found condition, mirroring the slate ConditionalStore adapter.
func isNotFound(err error) bool {
	code := minio.ToErrorResponse(err).Code
	return code == "NoSuchKey" || code == "NoSuchBucket"
}
