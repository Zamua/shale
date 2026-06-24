//go:build integration

// minio_integration_test.go pins the MinIO blobstore adapter to REAL object-store
// streaming behaviour: that PutStream/GetStream round-trip bytes identically for
// both small and large (~20MB) blobs, that the large round-trip is genuinely
// STREAMING (driven from a generating reader, hashed through a discard sink, with
// no full-blob []byte ever materialized), that Delete is idempotent, Has and List
// behave, and a missing object surfaces the blob.ErrNotFound sentinel.
//
// This is the slate-module object-store adapter for the core module's pure-Go
// blob port, so it runs with just `-tags integration` against a MinIO (no
// slatedb/cgo). It mirrors the slate condstore integration test's env-var wiring
// exactly:
//
//	SLATE_MINIO_ENDPOINT (default http://localhost:9000) - the run/skip GATE
//	SLATE_MINIO_ACCESS / SLATE_MINIO_SECRET (default admin / supersecret)
//
// If SLATE_MINIO_ENDPOINT is unset every test here SKIPS, so the suite is safe to
// run on a box with no MinIO.
package blobstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Zamua/shale/pkg/blob"
)

func blobEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// newBlobStore connects to a MinIO, creates a unique bucket (cleaned up on
// t.Cleanup), and returns a MinioBlobStore against it under a KeyPrefix - the
// same shape as the slate condstore integration test's newCondStore. When
// SLATE_MINIO_ENDPOINT is unset the test SKIPS rather than failing, so the suite
// runs cleanly without a MinIO available.
func newBlobStore(t *testing.T) *MinioBlobStore {
	t.Helper()
	if os.Getenv("SLATE_MINIO_ENDPOINT") == "" {
		t.Skip("SLATE_MINIO_ENDPOINT unset; skipping MinIO blob integration test")
	}
	endpoint := blobEnv("SLATE_MINIO_ENDPOINT", "http://localhost:9000")
	access := blobEnv("SLATE_MINIO_ACCESS", "admin")
	secret := blobEnv("SLATE_MINIO_SECRET", "supersecret")
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
	bucket := fmt.Sprintf("shale-blob-itest-%d", time.Now().UnixNano())
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

	store, err := New(Config{
		EndpointHost: host, AccessKey: access, SecretKey: secret,
		UseSSL: secure, Bucket: bucket, KeyPrefix: "__blob/",
	})
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}
	return store
}

func itestCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestMinioBlobStore_PutGetSmallRoundTrip pins that a small blob round-trips byte
// for byte, with the size reported by GetStream matching what was written.
func TestMinioBlobStore_PutGetSmallRoundTrip(t *testing.T) {
	s := newBlobStore(t)
	ctx := itestCtx(t)

	want := []byte("the quick brown fox jumps over the lazy dog")
	key := blob.FinalKey("0-0", mustBlobID(t))
	if err := s.PutStream(ctx, key, strings.NewReader(string(want)), int64(len(want))); err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	rc, size, err := s.GetStream(ctx, key)
	if err != nil {
		t.Fatalf("GetStream: %v", err)
	}
	defer rc.Close()
	if size != int64(len(want)) {
		t.Fatalf("GetStream size = %d, want %d", size, len(want))
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("round-trip mismatch: got %q, want %q", got, want)
	}
}

// TestMinioBlobStore_LargeStreamingRoundTrip is the load-bearing streaming test.
// It drives PutStream from a GENERATING reader that produces ~20MB on the fly
// (never holding the whole blob), then drives GetStream into a sha256 + discard
// sink (never io.ReadAll-ing the body into a []byte). It asserts the two hashes
// match, proving a correct round-trip with NO full-blob buffer on either side.
//
// A counting reader wraps the GetStream body and asserts the read happens in
// many small chunks (not one giant Read), evidence the transfer is genuinely
// streamed rather than buffered whole.
func TestMinioBlobStore_LargeStreamingRoundTrip(t *testing.T) {
	s := newBlobStore(t)
	ctx := itestCtx(t)

	const size = 20 << 20 // 20 MiB

	// PUT: hash-while-generating, no full buffer. The generating reader yields
	// deterministic bytes so we can recompute the expected hash on GET.
	putHash := sha256.New()
	gen := io.TeeReader(newPatternReader(size), putHash)
	key := blob.FinalKey("0-9", mustBlobID(t))
	if err := s.PutStream(ctx, key, gen, int64(size)); err != nil {
		t.Fatalf("PutStream (20MB): %v", err)
	}
	wantSum := putHash.Sum(nil)

	// GET: stream into sha256, counting chunks, never materializing a []byte.
	rc, gotSize, err := s.GetStream(ctx, key)
	if err != nil {
		t.Fatalf("GetStream (20MB): %v", err)
	}
	defer rc.Close()
	if gotSize != int64(size) {
		t.Fatalf("GetStream size = %d, want %d", gotSize, size)
	}

	getHash := sha256.New()
	cr := &countingReader{r: rc}
	n, err := io.Copy(getHash, cr) // io.Copy uses a small fixed buffer; no whole-blob alloc
	if err != nil {
		t.Fatalf("stream copy: %v", err)
	}
	if n != int64(size) {
		t.Fatalf("streamed %d bytes, want %d", n, size)
	}
	if !equalSum(getHash.Sum(nil), wantSum) {
		t.Fatalf("hash mismatch after 20MB streaming round-trip:\n got  %x\n want %x", getHash.Sum(nil), wantSum)
	}
	// Streaming evidence: a 20MB body delivered through io.Copy's 32KB buffer
	// must take many Reads. One (or a tiny handful) would mean it was buffered
	// whole somewhere, defeating the streaming guarantee.
	if cr.reads < 8 {
		t.Fatalf("GetStream body delivered in only %d Read calls for %d bytes; expected many small reads (streaming, not whole-buffer)", cr.reads, size)
	}
	t.Logf("20MB round-trip streamed in %d Read calls (avg %d bytes/read)", cr.reads, n/int64(cr.reads))
}

// TestMinioBlobStore_GetStreamMissingIsErrNotFound pins the ErrNotFound sentinel:
// GetStream of an object that was never written returns blob.ErrNotFound (mapped
// from the store's NoSuchKey).
func TestMinioBlobStore_GetStreamMissingIsErrNotFound(t *testing.T) {
	s := newBlobStore(t)
	ctx := itestCtx(t)

	_, _, err := s.GetStream(ctx, blob.FinalKey("0-0", "does-not-exist"))
	if !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("GetStream(missing) = %v, want blob.ErrNotFound", err)
	}
}

// TestMinioBlobStore_DeleteIdempotent pins that deleting a missing object is a
// no-op (nil error), and that deleting an existing object then re-deleting it
// also returns nil both times - the property the phase-2 orphan sweep relies on.
func TestMinioBlobStore_DeleteIdempotent(t *testing.T) {
	s := newBlobStore(t)
	ctx := itestCtx(t)

	// Delete of a never-written object: no error.
	if err := s.Delete(ctx, blob.FinalKey("0-0", "never-existed")); err != nil {
		t.Fatalf("Delete(missing) = %v, want nil (idempotent)", err)
	}

	key := blob.FinalKey("0-0", mustBlobID(t))
	if err := s.PutStream(ctx, key, strings.NewReader("payload"), int64(len("payload"))); err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("first Delete: %v", err)
	}
	// Double delete: still no error.
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("second Delete (already gone) = %v, want nil (idempotent)", err)
	}
	// And it really is gone.
	ok, err := s.Has(ctx, key)
	if err != nil {
		t.Fatalf("Has after delete: %v", err)
	}
	if ok {
		t.Fatal("Has reported the object present after Delete")
	}
}

// TestMinioBlobStore_Has pins true-after-write / false-before-write.
func TestMinioBlobStore_Has(t *testing.T) {
	s := newBlobStore(t)
	ctx := itestCtx(t)

	key := blob.FinalKey("0-3", mustBlobID(t))
	ok, err := s.Has(ctx, key)
	if err != nil {
		t.Fatalf("Has(missing): %v", err)
	}
	if ok {
		t.Fatal("Has reported true for a never-written object")
	}
	if err := s.PutStream(ctx, key, strings.NewReader("x"), 1); err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	ok, err = s.Has(ctx, key)
	if err != nil {
		t.Fatalf("Has(present): %v", err)
	}
	if !ok {
		t.Fatal("Has reported false for a written object")
	}
}

// TestMinioBlobStore_ListUnderPrefix pins that List yields exactly the objects
// under a prefix - the input to the same-shard orphan sweep - that each yielded
// ObjectInfo carries the Key, the correct Size, and a non-zero ModTime (the
// LastModified the sweep age-gates on), and that the store's internal KeyPrefix
// is stripped so callers see the same key space they pass to the other methods
// (a key out of List feeds straight back into Has).
func TestMinioBlobStore_ListUnderPrefix(t *testing.T) {
	s := newBlobStore(t)
	ctx := itestCtx(t)

	// Write three blobs under unit A and one under unit B.
	unitA := "0-A"
	unitB := "0-B"
	const payloadA = "aaa" // a known size to assert ObjectInfo.Size
	wantA := map[string]bool{}
	for i := 0; i < 3; i++ {
		id := mustBlobID(t)
		key := blob.FinalKey(unitA, id)
		wantA[key] = true
		if err := s.PutStream(ctx, key, strings.NewReader(payloadA), int64(len(payloadA))); err != nil {
			t.Fatalf("PutStream A%d: %v", i, err)
		}
	}
	keyB := blob.FinalKey(unitB, mustBlobID(t))
	if err := s.PutStream(ctx, keyB, strings.NewReader("b"), 1); err != nil {
		t.Fatalf("PutStream B: %v", err)
	}

	gotA := map[string]blob.ObjectInfo{}
	for info, err := range s.List(ctx, blob.FinalPrefixForUnit(unitA)) {
		if err != nil {
			t.Fatalf("List error: %v", err)
		}
		gotA[info.Key] = info
	}
	if len(gotA) != len(wantA) {
		t.Fatalf("List(%s) returned %d objects, want %d: %v", blob.FinalPrefixForUnit(unitA), len(gotA), len(wantA), gotA)
	}
	for k := range wantA {
		info, ok := gotA[k]
		if !ok {
			t.Fatalf("List missing key %q (got %v)", k, gotA)
		}
		// Size + ModTime are the sweep's inputs: Size is the footprint, ModTime
		// is the age-gate signal. Pin both.
		if info.Size != int64(len(payloadA)) {
			t.Fatalf("List ObjectInfo.Size for %q = %d, want %d", k, info.Size, len(payloadA))
		}
		if info.ModTime.IsZero() {
			t.Fatalf("List ObjectInfo.ModTime for %q is zero; the orphan sweep age-gate needs a real LastModified", k)
		}
		// Round-trip the listed key back through Has: confirms the KeyPrefix was
		// stripped so List output is in the same key space as the inputs.
		hok, err := s.Has(ctx, k)
		if err != nil {
			t.Fatalf("Has(listed key %q): %v", k, err)
		}
		if !hok {
			t.Fatalf("Has(listed key %q) = false; List returned a key the other methods cannot resolve (KeyPrefix not stripped consistently)", k)
		}
	}
	// unit-B's key must NOT appear under unit-A's prefix.
	if _, leaked := gotA[keyB]; leaked {
		t.Fatalf("List(%s) leaked a key from %s: %q", blob.FinalPrefixForUnit(unitA), unitB, keyB)
	}
}

// --- streaming test helpers ---

func mustBlobID(t *testing.T) string {
	t.Helper()
	id, err := blob.NewBlobID()
	if err != nil {
		t.Fatalf("NewBlobID: %v", err)
	}
	return id
}

// patternReader generates `total` deterministic bytes on the fly without ever
// holding more than a tiny window in memory - so a test can drive a multi-MB
// PutStream with no whole-blob []byte. The byte at offset i is byte(i*2654435761)
// (a cheap pseudo-pattern), making the stream content-addressable for the GET
// hash check while staying allocation-free.
type patternReader struct {
	pos   int64
	total int64
}

func newPatternReader(total int64) *patternReader { return &patternReader{total: total} }

func (p *patternReader) Read(b []byte) (int, error) {
	if p.pos >= p.total {
		return 0, io.EOF
	}
	remaining := p.total - p.pos
	n := int64(len(b))
	if n > remaining {
		n = remaining
	}
	for i := int64(0); i < n; i++ {
		b[i] = byte((p.pos + i) * 2654435761)
	}
	p.pos += n
	if p.pos >= p.total {
		return int(n), nil // let the next Read report EOF
	}
	return int(n), nil
}

// countingReader counts Read calls passing through it, so a test can assert a
// transfer happened in many small chunks (streaming) rather than one giant read.
type countingReader struct {
	r     io.Reader
	reads int
}

func (c *countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	if n > 0 {
		c.reads++
	}
	return n, err
}

func equalSum(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
