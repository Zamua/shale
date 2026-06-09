//go:build slatedb && integration

// Real-MinIO end-to-end coverage for the SlateDB backend. These tests
// spin up a MinIO container via testcontainers-go, create a fresh
// bucket, and drive the public *Slate surface (Put/Get/Delete/
// ScanPrefix/Open/Close/reopen) against actual S3-compatible object
// storage. They exist because the in-process "memory:///" backend used
// by slate_test.go exercises the binding but skips the object-store
// round-trip that production deploys depend on. v0.6 (the hostthis
// migration to shale-with-slate) gates on this coverage.
//
// Gating: both `slatedb` (cgo binding, needed for any slate test) and
// `integration` (long-running, requires Docker). Default `go test ./...`
// skips both. Operator entry point is `make test-slate-minio`.
//
// Docker requirement: testcontainers-go talks to the local Docker
// daemon. On the development machine this is colima; in CI any Docker
// engine with the standard socket works. If Docker is unreachable, the
// test fails with a clear "failed to connect to docker" message rather
// than a hung process.

package slate_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/Zamua/shale/backends/slate"
	"github.com/Zamua/shale/pkg/backend"
)

// minioFixture owns a running MinIO container + a freshly-created
// bucket, and exposes the connection details a slate.Config needs.
type minioFixture struct {
	Endpoint  string // "http://127.0.0.1:32xx", no trailing slash
	Bucket    string
	AccessKey string
	SecretKey string
}

// startMinIO brings up a single-node MinIO via testcontainers, creates
// a unique bucket, and registers teardown on t.Cleanup. The bucket
// name is unique per test invocation so parallel runs of this file (or
// re-runs in the same container) don't collide.
//
// Failure mode worth calling out: if the local Docker daemon is not
// running, testcontainers fails with a connect error here. We surface
// that as t.Fatalf with the underlying error rather than try to skip,
// because this file is explicitly opt-in via the `integration` tag and
// the caller already declared "I have Docker." Skipping silently would
// hide real CI breakage.
func startMinIO(t *testing.T) *minioFixture {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		image    = "minio/minio:RELEASE.2024-01-16T16-07-38Z"
		user     = "minioadmin"
		password = "minioadmin"
	)

	container, err := tcminio.Run(ctx, image,
		tcminio.WithUsername(user),
		tcminio.WithPassword(password),
	)
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}
	t.Cleanup(func() {
		// Use a fresh context: t.Cleanup may run after the test's
		// own ctx has already been cancelled.
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		if err := container.Terminate(shutdownCtx); err != nil {
			t.Logf("terminate minio: %v", err)
		}
	})

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("get connection string: %v", err)
	}
	endpoint := "http://" + connStr

	bucket := fmt.Sprintf("shale-itest-%d", time.Now().UnixNano())

	// Use the official minio-go client to create the bucket up-front.
	// SlateDB's object_store crate expects the bucket to already exist;
	// it does not auto-create.
	mc, err := minio.New(connStr, &minio.Options{
		Creds:  credentials.NewStaticV4(user, password, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	if err := mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("create bucket %q: %v", bucket, err)
	}

	return &minioFixture{
		Endpoint:  endpoint,
		Bucket:    bucket,
		AccessKey: user,
		SecretKey: password,
	}
}

// openSlate opens a Slate against the fixture's bucket. Closes on
// t.Cleanup. dbName lets the caller carve sub-prefixes inside the same
// bucket for tests that need multiple logical instances.
func openSlate(t *testing.T, fx *minioFixture, dbName string) *slate.Slate {
	t.Helper()
	s, err := slate.New(slate.Config{
		Bucket:    fx.Bucket,
		DbName:    dbName,
		Endpoint:  fx.Endpoint,
		Region:    "us-east-1",
		AccessKey: fx.AccessKey,
		SecretKey: fx.SecretKey,
		UseSSL:    false,
	})
	if err != nil {
		t.Fatalf("open slate against minio: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestMinIO_BasicHappyPath drives Put/Get/Delete/ScanPrefix against a
// real MinIO bucket, then verifies the values survive a Close + reopen
// (durability across writer epochs).
func TestMinIO_BasicHappyPath(t *testing.T) {
	fx := startMinIO(t)

	s := openSlate(t, fx, "happy-path")

	// 1k small keys: round-trip every one and confirm both presence
	// and value. ScanPrefix on the shared prefix should see all 1000.
	const n = 1000
	prefix := []byte("k:")
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k:%05d", i))
		v := []byte(fmt.Sprintf("v-%d", i))
		if err := s.Put(k, v); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k:%05d", i))
		want := []byte(fmt.Sprintf("v-%d", i))
		got, err := s.Get(k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get %d: got %q want %q", i, got, want)
		}
	}

	it, err := s.ScanPrefix(prefix)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	count := 0
	for {
		k, _, err := it.Next()
		if err != nil {
			_ = it.Close()
			t.Fatalf("scan next: %v", err)
		}
		if k == nil {
			break
		}
		count++
	}
	_ = it.Close()
	if count != n {
		t.Fatalf("scan: got %d want %d", count, n)
	}

	// Delete a stripe and confirm:
	for i := 0; i < n; i += 7 {
		k := []byte(fmt.Sprintf("k:%05d", i))
		if err := s.Delete(k); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k:%05d", i))
		_, err := s.Get(k)
		if i%7 == 0 {
			if !errors.Is(err, backend.ErrNotFound) {
				t.Fatalf("expected ErrNotFound on deleted key %d, got %v", i, err)
			}
		} else if err != nil {
			t.Fatalf("get %d after partial delete: %v", i, err)
		}
	}

	// Close, reopen, verify the survivors are still there. This is the
	// real-MinIO durability claim: the bytes on disk in the container
	// outlive the writer process.
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := openSlate(t, fx, "happy-path")
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k:%05d", i))
		got, err := s2.Get(k)
		if i%7 == 0 {
			if !errors.Is(err, backend.ErrNotFound) {
				t.Fatalf("after reopen: deleted key %d should be absent, got %v", i, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("after reopen: get %d: %v", i, err)
		}
		want := []byte(fmt.Sprintf("v-%d", i))
		if !bytes.Equal(got, want) {
			t.Fatalf("after reopen: get %d: got %q want %q", i, got, want)
		}
	}
}

// TestMinIO_LargeBlobs writes 10 1MB blobs, reads them back, and
// confirms byte-for-byte equality. SlateDB's SST + WAL flushing path
// is the part being exercised here; the in-memory store doesn't catch
// alignment or chunking bugs in the S3 upload path.
func TestMinIO_LargeBlobs(t *testing.T) {
	fx := startMinIO(t)
	s := openSlate(t, fx, "large-blobs")

	const blobs = 10
	const blobSize = 1 << 20 // 1 MiB

	// Deterministic but uncompressible-ish payloads so we don't
	// accidentally test only zero-pages.
	rng := rand.New(rand.NewSource(1))
	want := make(map[string][]byte, blobs)
	for i := 0; i < blobs; i++ {
		buf := make([]byte, blobSize)
		_, _ = rng.Read(buf)
		key := fmt.Sprintf("blob:%02d", i)
		want[key] = buf
		if err := s.Put([]byte(key), buf); err != nil {
			t.Fatalf("put blob %d: %v", i, err)
		}
	}
	for k, w := range want {
		got, err := s.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if !bytes.Equal(got, w) {
			t.Fatalf("blob %s: %d bytes back, contents differ", k, len(got))
		}
	}
}

// TestMinIO_WriterEpochFencing exercises the single-writer guarantee:
// opening a second Slate against the same (bucket, dbName) should
// fence the first. The first writer's next operation must surface
// the underlying slatedb.ErrorClosed with CloseReasonFenced, NOT
// silently corrupt state by both writers continuing to commit.
//
// This is the property hostthis depends on: if two shaled processes
// race to take ownership of the same SlateDB instance, exactly one
// must end up writable.
func TestMinIO_WriterEpochFencing(t *testing.T) {
	fx := startMinIO(t)

	const db = "fenced"

	// Writer 1 opens + writes a known value.
	w1 := openSlate(t, fx, db)
	if err := w1.Put([]byte("owner"), []byte("w1")); err != nil {
		t.Fatalf("w1 put: %v", err)
	}

	// Writer 2 opens the SAME (bucket, dbName). The slatedb writer-
	// epoch protocol bumps the manifest, which means w1's subsequent
	// writes should fail.
	w2 := openSlate(t, fx, db)

	// Give the manifest stomp a beat to propagate through w1's
	// background tasks. The exact latency is implementation-defined
	// (slatedb's writer poll interval); a small bounded wait is fine.
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = w1.Put([]byte("owner"), []byte("w1-again"))
		if lastErr != nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastErr == nil {
		t.Fatalf("expected w1.Put to fail after w2 opened; got nil over 30s")
	}
	// We don't pin the exact error type because the slatedb binding
	// surfaces fencing via a few different paths (closed-handle,
	// internal). What matters is that the operation FAILS rather
	// than silently succeeding against a stale epoch.
	t.Logf("w1 fenced as expected: %v", lastErr)

	// w2 must be the live writer.
	if err := w2.Put([]byte("owner"), []byte("w2")); err != nil {
		t.Fatalf("w2 put after fencing w1: %v", err)
	}
	got, err := w2.Get([]byte("owner"))
	if err != nil {
		t.Fatalf("w2 get: %v", err)
	}
	if !bytes.Equal(got, []byte("w2")) {
		t.Fatalf("w2 should observe its own value; got %q", got)
	}
}
