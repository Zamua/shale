package cluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
)

// fenceAtCommitBackend mimics REAL slatedb's fence surface: Begin succeeds and
// buffered writes succeed, but the transaction's Commit returns backend.ErrFenced
// (a higher-epoch owner fenced this writer). The in-memory fencedReplicaBackend
// fences at Begin instead, so it can NEVER exercise the commit-fence recode - this
// double is the only in-process way to reach it.
type fenceAtCommitBackend struct{ backend.Backend }

func (f fenceAtCommitBackend) Begin(l backend.IsolationLevel) (backend.Transaction, error) {
	tx, err := f.Backend.Begin(l)
	if err != nil {
		return nil, err
	}
	return fenceAtCommitTx{tx}, nil
}

type fenceAtCommitTx struct{ backend.Transaction }

// Commit fences, wrapping the sentinel with %w exactly as the slate backend's
// fencedCommitErr does (so errors.Is(err, backend.ErrFenced) resolves through it).
func (t fenceAtCommitTx) Commit() error {
	return fmt.Errorf("slate: tx commit: %w", backend.ErrFenced)
}

// TestApply_FencedCommit_RecodedToTransient pins the prod-only fix (#410 residual):
// a write whose Commit is FENCED (real slatedb surfaces the fence here, after a
// successful Begin) must be recoded to the TRANSIENT acquiring-window error, NOT
// returned as the raw fence. The raw fence is a HARD failure that fast-fails the
// fan-out unretried; the transient form makes the leg non-acking + RETRYABLE so the
// fan-out re-sends onto the re-resolved union (the successor that fenced it serves
// the write once it finishes mounting). This is the during-leave write-availability
// fix that the in-memory tests (which fence at Begin) cannot reach.
func TestApply_FencedCommit_RecodedToTransient(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	target := ru(0, 0, 0)
	fb := fenceAtCommitBackend{Backend: memory.New()}
	env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1_000_000, NodeID: "w"}, Payload: []byte("v")})

	err := c.applyEnvelopeIfNewerToBackend(fb, target, []byte("k"), env)
	if err == nil {
		t.Fatal("a fenced commit must surface an error, got nil")
	}
	if errors.Is(err, backend.ErrFenced) {
		t.Fatalf("the RAW fence must NOT leak to the fan-out (it would fast-fail the write as a HARD error); got %v", err)
	}
	if !isAcquiringErr(err) {
		t.Fatalf("a fenced commit must be recoded to the TRANSIENT acquiring-window error so the fan-out retries onto the re-resolved union; got %v", err)
	}
	if !isTransientReplicaErr(err) {
		t.Fatalf("the recoded fence must be classified transient by the fan-out (neither ack nor hard failure); got %v", err)
	}
}
