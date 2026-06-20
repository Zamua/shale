// Package slate provides, in condstore.go, the MinIO/S3 implementation of
// storageunit.ConditionalStore - the durable, decentralized arbiter the
// declarative reshard agreement runs on.
//
// It is deliberately TAGLESS (no slatedb / cgo): it depends only on minio-go,
// so it builds and its integration test runs without the slatedb native
// library. PutIfAbsent maps to a conditional PutObject with If-None-Match:*
// (create only if absent) and CompareAndSet maps to If-Match:<etag> (overwrite
// only if unchanged) - the same conditional-write primitive slatedb's manifest
// fencing relies on, so any object store that hosts slate also supports this.
// The version token returned is the object's S3 ETag.
package slate

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/Zamua/shale/pkg/storageunit"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinioConditionalStore implements storageunit.ConditionalStore over one bucket.
type MinioConditionalStore struct {
	client *minio.Client
	bucket string
	prefix string // prepended to every key; may be empty
}

// MinioConditionalStoreConfig configures a MinioConditionalStore.
type MinioConditionalStoreConfig struct {
	EndpointHost string // "host:port", no scheme
	AccessKey    string
	SecretKey    string
	UseSSL       bool
	Bucket       string
	KeyPrefix    string // namespaces the arbiter's objects within the bucket
}

// NewMinioConditionalStore builds a MinioConditionalStore against the bucket.
// The bucket must already exist.
func NewMinioConditionalStore(cfg MinioConditionalStoreConfig) (*MinioConditionalStore, error) {
	mc, err := minio.New(cfg.EndpointHost, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("slate: conditional store minio client: %w", err)
	}
	return &MinioConditionalStore{client: mc, bucket: cfg.Bucket, prefix: cfg.KeyPrefix}, nil
}

func (s *MinioConditionalStore) keyFor(key string) string { return s.prefix + key }

// PutIfAbsent implements storageunit.ConditionalStore via If-None-Match:* (the
// object is created only if it does not already exist). On a server that
// honours the conditional header, exactly one of many concurrent PutIfAbsent
// calls on the same key wins; the rest get ErrPrecondition.
func (s *MinioConditionalStore) PutIfAbsent(key string, data []byte) (string, error) {
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	opts.SetMatchETagExcept("*") // If-None-Match: *
	info, err := s.client.PutObject(context.Background(), s.bucket, s.keyFor(key),
		bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		if isPreconditionErr(err) {
			return "", storageunit.ErrPrecondition
		}
		return "", fmt.Errorf("slate: PutIfAbsent %s: %w", key, err)
	}
	return info.ETag, nil
}

// CompareAndSet implements storageunit.ConditionalStore via If-Match:<etag>
// (the object is overwritten only if its current ETag equals expectedVersion).
func (s *MinioConditionalStore) CompareAndSet(key string, data []byte, expectedVersion string) (string, error) {
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	opts.SetMatchETag(expectedVersion) // If-Match: "<etag>"
	info, err := s.client.PutObject(context.Background(), s.bucket, s.keyFor(key),
		bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		if isPreconditionErr(err) {
			return "", storageunit.ErrPrecondition
		}
		return "", fmt.Errorf("slate: CompareAndSet %s: %w", key, err)
	}
	return info.ETag, nil
}

// Get implements storageunit.ConditionalStore, returning the object's bytes and
// its current ETag (the version token), or ErrCondNotFound if absent.
func (s *MinioConditionalStore) Get(key string) ([]byte, string, error) {
	obj, err := s.client.GetObject(context.Background(), s.bucket, s.keyFor(key), minio.GetObjectOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("slate: conditional Get %s: %w", key, err)
	}
	defer func() { _ = obj.Close() }()
	// Stat resolves the underlying GET; a missing object surfaces here as
	// NoSuchKey, which is ErrCondNotFound, not a failure.
	st, err := obj.Stat()
	if err != nil {
		if isNotFoundErr(err) {
			return nil, "", storageunit.ErrCondNotFound
		}
		return nil, "", fmt.Errorf("slate: conditional Get stat %s: %w", key, err)
	}
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, "", fmt.Errorf("slate: conditional Get read %s: %w", key, err)
	}
	return data, st.ETag, nil
}

func isPreconditionErr(err error) bool {
	return minio.ToErrorResponse(err).Code == "PreconditionFailed"
}

func isNotFoundErr(err error) bool {
	code := minio.ToErrorResponse(err).Code
	return code == "NoSuchKey" || code == "NoSuchBucket"
}
