package blobmem

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/Zamua/shale/pkg/blob"
)

// TestPutGetRoundTrip pins that bytes written via PutStream stream back
// identically via GetStream, with the reported size matching.
func TestPutGetRoundTrip(t *testing.T) {
	s := New()
	ctx := context.Background()
	want := "the quick brown fox"
	key := blob.FinalKey("0-3", "abc")
	if err := s.PutStream(ctx, key, strings.NewReader(want), int64(len(want))); err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	rc, size, err := s.GetStream(ctx, key)
	if err != nil {
		t.Fatalf("GetStream: %v", err)
	}
	defer func() { _ = rc.Close() }()
	if size != int64(len(want)) {
		t.Fatalf("size = %d, want %d", size, len(want))
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != want {
		t.Fatalf("round-trip = %q, want %q", got, want)
	}
}

// TestGetMissingIsErrNotFound pins the not-found sentinel.
func TestGetMissingIsErrNotFound(t *testing.T) {
	s := New()
	_, _, err := s.GetStream(context.Background(), blob.FinalKey("0-0", "nope"))
	if !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("GetStream(missing) = %v, want blob.ErrNotFound", err)
	}
}

// TestDeleteIdempotent pins that deleting a missing object is a no-op.
func TestDeleteIdempotent(t *testing.T) {
	s := New()
	ctx := context.Background()
	if err := s.Delete(ctx, blob.FinalKey("0-0", "never")); err != nil {
		t.Fatalf("Delete(missing) = %v, want nil", err)
	}
	key := blob.FinalKey("0-0", "x")
	if err := s.PutStream(ctx, key, strings.NewReader("x"), 1); err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("Delete (already gone) = %v, want nil", err)
	}
	if ok, _ := s.Has(ctx, key); ok {
		t.Fatal("Has reported present after Delete")
	}
}
