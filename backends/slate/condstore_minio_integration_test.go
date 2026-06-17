//go:build integration

// condstore_minio_integration_test.go pins the MinioConditionalStore adapter to
// REAL object-store conditional-write behaviour: that the deployed store honours
// If-None-Match:* (create-only) and If-Match:<etag> (compare-and-set), and that
// concurrent creates serialize to exactly one winner. This is the single
// non-fast test the whole decentralized arbiter depends on; everything above it
// uses the in-process MemConditionalStore double.
//
// Tagless (no slatedb): runs with just `-tags integration` against a MinIO.
//
//	SLATE_MINIO_ENDPOINT (default http://localhost:9000)
//	SLATE_MINIO_ACCESS / SLATE_MINIO_SECRET (default admin / supersecret)
package slate_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/backends/slate"
	"github.com/Zamua/shale/pkg/storageunit"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func condEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// newCondStore connects to a MinIO, creates a unique bucket (cleaned up on
// t.Cleanup), and returns a MinioConditionalStore against it.
func newCondStore(t *testing.T) *slate.MinioConditionalStore {
	t.Helper()
	endpoint := condEnv("SLATE_MINIO_ENDPOINT", "http://localhost:9000")
	access := condEnv("SLATE_MINIO_ACCESS", "admin")
	secret := condEnv("SLATE_MINIO_SECRET", "supersecret")
	secure := strings.HasPrefix(endpoint, "https://")
	host := strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")

	mc, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: secure,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bucket := fmt.Sprintf("shale-condstore-itest-%d", time.Now().UnixNano())
	if err := mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("create bucket (is MinIO up at %s?): %v", endpoint, err)
	}
	t.Cleanup(func() {
		cctx, cc := context.WithTimeout(context.Background(), 60*time.Second)
		defer cc()
		for o := range mc.ListObjects(cctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
			_ = mc.RemoveObject(cctx, bucket, o.Key, minio.RemoveObjectOptions{})
		}
		_ = mc.RemoveBucket(cctx, bucket)
	})

	store, err := slate.NewMinioConditionalStore(slate.MinioConditionalStoreConfig{
		EndpointHost: host, AccessKey: access, SecretKey: secret,
		UseSSL: secure, Bucket: bucket, KeyPrefix: "__reshard/",
	})
	if err != nil {
		t.Fatalf("new cond store: %v", err)
	}
	return store
}

func TestMinioConditionalStore_PutIfAbsentCreateOnly(t *testing.T) {
	s := newCondStore(t)
	v1, err := s.PutIfAbsent("epoch", []byte("g0"))
	if err != nil {
		t.Fatalf("first PutIfAbsent: %v", err)
	}
	if v1 == "" {
		t.Fatal("PutIfAbsent should return an etag version")
	}
	if _, err := s.PutIfAbsent("epoch", []byte("g1")); !errors.Is(err, storageunit.ErrPrecondition) {
		t.Fatalf("second PutIfAbsent = %v, want ErrPrecondition (store must honour If-None-Match:*)", err)
	}
	got, v, err := s.Get("epoch")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "g0" {
		t.Fatalf("Get = %q, want g0 (the failed create must not have overwritten)", got)
	}
	if v != v1 {
		t.Fatalf("Get version = %s, want %s", v, v1)
	}
}

func TestMinioConditionalStore_CompareAndSet(t *testing.T) {
	s := newCondStore(t)
	v0, err := s.PutIfAbsent("k", []byte("a"))
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := s.CompareAndSet("k", []byte("b"), "0badetagdeadbeef"); !errors.Is(err, storageunit.ErrPrecondition) {
		t.Fatalf("CAS with stale version = %v, want ErrPrecondition (store must honour If-Match)", err)
	}
	v1, err := s.CompareAndSet("k", []byte("b"), v0)
	if err != nil {
		t.Fatalf("CAS with current version: %v", err)
	}
	if v1 == v0 {
		t.Fatal("CompareAndSet must advance the etag")
	}
	got, _, _ := s.Get("k")
	if string(got) != "b" {
		t.Fatalf("after CAS Get = %q, want b", got)
	}
	if _, err := s.CompareAndSet("k", []byte("c"), v0); !errors.Is(err, storageunit.ErrPrecondition) {
		t.Fatalf("CAS with superseded version = %v, want ErrPrecondition", err)
	}
}

func TestMinioConditionalStore_GetAbsent(t *testing.T) {
	s := newCondStore(t)
	if _, _, err := s.Get("nope"); !errors.Is(err, storageunit.ErrCondNotFound) {
		t.Fatalf("Get(absent) = %v, want ErrCondNotFound", err)
	}
}

// TestMinioConditionalStore_RaceExactlyOneWinner is the property the arbiter
// rests on, proven against the REAL store: many nodes racing to create the same
// epoch object yield exactly one winner.
func TestMinioConditionalStore_RaceExactlyOneWinner(t *testing.T) {
	s := newCondStore(t)
	const n = 24
	var wins int64
	var start sync.WaitGroup
	var done sync.WaitGroup
	start.Add(1)
	for range n {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			if _, err := s.PutIfAbsent("epoch/1", []byte("x")); err == nil {
				atomic.AddInt64(&wins, 1)
			}
		}()
	}
	start.Done()
	done.Wait()
	if wins != 1 {
		t.Fatalf("concurrent PutIfAbsent on real MinIO: %d winners, want exactly 1 (If-None-Match must serialize)", wins)
	}
}
