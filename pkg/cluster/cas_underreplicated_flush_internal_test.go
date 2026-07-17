package cluster

// White-box pins for the under-W durability flush helper
// (flushOwnerUnderReplicated): the closing addition that makes a committed-
// under-replicated CAS write bucket-durable under a RELAXED backend
// (AwaitDurable=false). The normal W-reached path must NOT flush; only the
// under-W branch (the lone owner memtable copy) does, converting RAM-only to
// bucket-durable before success. A flush failure is a genuine durability
// failure surfaced terminally, not a false success. See docs/SPEC.md "The four
// commit outcomes" outcome (c).

import (
	"errors"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
)

// fakeFlushBackend implements backend.Flusher over a nil embedded Backend (the
// helper never touches the other methods), counting Flush calls and optionally
// returning an injected error. Instruments "flush called exactly once".
type fakeFlushBackend struct {
	backend.Backend
	calls int
	err   error
}

func (f *fakeFlushBackend) Flush() error {
	f.calls++
	return f.err
}

// plainBackend is a Backend WITHOUT the optional Flusher capability (the shape
// of a synchronously-durable backend like memory / pebble). The under-W path
// must skip the flush for it - there is no RAM-loss window to close.
type plainBackend struct{ backend.Backend }

// TestFlushOwnerUnderReplicated_FlushesCapableBackend pins the core behavior:
// a Flusher-capable owner backend is flushed exactly once and a successful
// flush returns nil (the lone copy is now bucket-durable).
func TestFlushOwnerUnderReplicated_FlushesCapableBackend(t *testing.T) {
	var c Cluster
	f := &fakeFlushBackend{}
	if err := c.flushOwnerUnderReplicated(f); err != nil {
		t.Fatalf("flush of a healthy Flusher must return nil; got %v", err)
	}
	if f.calls != 1 {
		t.Fatalf("under-W flush must call Flush exactly once; got %d", f.calls)
	}
}

// TestFlushOwnerUnderReplicated_PropagatesFlushError pins the terminal-failure
// contract: when the flush ITSELF errors (the lone copy could not be
// persisted), the helper propagates that error so the under-W branch surfaces
// it terminally instead of reporting a false durable success.
func TestFlushOwnerUnderReplicated_PropagatesFlushError(t *testing.T) {
	var c Cluster
	boom := errors.New("object store unreachable")
	f := &fakeFlushBackend{err: boom}
	err := c.flushOwnerUnderReplicated(f)
	if !errors.Is(err, boom) {
		t.Fatalf("a flush failure must propagate as a durability failure; got %v", err)
	}
	if f.calls != 1 {
		t.Fatalf("Flush must be attempted exactly once even when it fails; got %d", f.calls)
	}
}

// TestFlushOwnerUnderReplicated_SkipsNonFlusher pins the capability gate: a
// backend that does NOT implement backend.Flusher (already synchronously
// durable) is skipped, returning nil without any flush attempt.
func TestFlushOwnerUnderReplicated_SkipsNonFlusher(t *testing.T) {
	var c Cluster
	if err := c.flushOwnerUnderReplicated(plainBackend{}); err != nil {
		t.Fatalf("a non-Flusher backend must be skipped (nil); got %v", err)
	}
}

// TestFlushOwnerUnderReplicated_NilBackend pins the defensive nil guard (the
// legacy per-node fallback resolves c.backend, which a paranoid caller may find
// nil): a nil owner backend is a no-op, never a panic.
func TestFlushOwnerUnderReplicated_NilBackend(t *testing.T) {
	var c Cluster
	if err := c.flushOwnerUnderReplicated(nil); err != nil {
		t.Fatalf("nil owner backend must be a no-op; got %v", err)
	}
}
