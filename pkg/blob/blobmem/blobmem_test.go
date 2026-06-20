package blobmem

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

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

// TestListUnderPrefix pins that List yields exactly the objects under a unit
// prefix, each with the right Size + ModTime, and that a different unit's object
// is excluded - the orphan-sweep input shape.
func TestListUnderPrefix(t *testing.T) {
	s := New()
	ctx := context.Background()
	fixed := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	s.SetClock(func() time.Time { return fixed })

	unitA, unitB := "0-A", "0-B"
	const payload = "abcd"
	want := map[string]bool{}
	for _, id := range []string{"a1", "a2", "a3"} {
		key := blob.FinalKey(unitA, id)
		want[key] = true
		if err := s.PutStream(ctx, key, strings.NewReader(payload), int64(len(payload))); err != nil {
			t.Fatalf("PutStream %s: %v", id, err)
		}
	}
	keyB := blob.FinalKey(unitB, "b1")
	if err := s.PutStream(ctx, keyB, strings.NewReader("z"), 1); err != nil {
		t.Fatalf("PutStream B: %v", err)
	}

	got := map[string]blob.ObjectInfo{}
	for info, err := range s.List(ctx, blob.FinalPrefixForUnit(unitA)) {
		if err != nil {
			t.Fatalf("List error: %v", err)
		}
		got[info.Key] = info
	}
	if len(got) != len(want) {
		t.Fatalf("List returned %d objects, want %d: %v", len(got), len(want), got)
	}
	for k := range want {
		info, ok := got[k]
		if !ok {
			t.Fatalf("List missing %q", k)
		}
		if info.Size != int64(len(payload)) {
			t.Fatalf("Size for %q = %d, want %d", k, info.Size, len(payload))
		}
		if !info.ModTime.Equal(fixed) {
			t.Fatalf("ModTime for %q = %v, want %v (the injected clock)", k, info.ModTime, fixed)
		}
	}
	if _, leaked := got[keyB]; leaked {
		t.Fatalf("List(%s) leaked unit-B object %q", blob.FinalPrefixForUnit(unitA), keyB)
	}
}

// TestSetModTime pins that SetModTime overrides an object's listed ModTime, so
// a test can age an object for the sweep's age-gate without sleeping.
func TestSetModTime(t *testing.T) {
	s := New()
	ctx := context.Background()
	key := blob.FinalKey("0-0", "x")
	if err := s.PutStream(ctx, key, strings.NewReader("x"), 1); err != nil {
		t.Fatalf("PutStream: %v", err)
	}
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if ok := s.SetModTime(key, old); !ok {
		t.Fatal("SetModTime(present) = false, want true")
	}
	if ok := s.SetModTime(blob.FinalKey("0-0", "absent"), old); ok {
		t.Fatal("SetModTime(absent) = true, want false")
	}
	for info, err := range s.List(ctx, blob.FinalPrefixForUnit("0-0")) {
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if !info.ModTime.Equal(old) {
			t.Fatalf("ModTime = %v, want %v after SetModTime", info.ModTime, old)
		}
	}
}

// TestListEarlyStop pins that a yield returning false stops the iteration (the
// caller can abandon a listing without draining it).
func TestListEarlyStop(t *testing.T) {
	s := New()
	ctx := context.Background()
	for _, id := range []string{"a1", "a2", "a3"} {
		if err := s.PutStream(ctx, blob.FinalKey("0-0", id), strings.NewReader("x"), 1); err != nil {
			t.Fatalf("PutStream %s: %v", id, err)
		}
	}
	seen := 0
	for range s.List(ctx, blob.FinalPrefixForUnit("0-0")) {
		seen++
		break
	}
	if seen != 1 {
		t.Fatalf("early-stop saw %d objects, want 1", seen)
	}
}
