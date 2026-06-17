package cluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
)

// fenceAtBackend mimics REAL slatedb's fence surface: Begin succeeds, but a chosen
// transaction op (Get, Put, or Commit) returns backend.ErrFenced - a higher-epoch
// owner fenced this now-stale writer. slatedb's background manifest-epoch poll
// lands the fence on whichever op runs next, so the apply sequence
// Begin->Get->Put->Commit can fence at ANY step; the in-memory fencedReplicaBackend
// fences only at Begin (already recoded by the Begin path) and cannot reach these
// surfaces.
type fenceAtBackend struct {
	backend.Backend
	at string // "get" | "put" | "commit"
}

func (f fenceAtBackend) Begin(l backend.IsolationLevel) (backend.Transaction, error) {
	tx, err := f.Backend.Begin(l)
	if err != nil {
		return nil, err
	}
	return fenceAtTx{Transaction: tx, at: f.at}, nil
}

type fenceAtTx struct {
	backend.Transaction
	at string
}

// each op wraps backend.ErrFenced with %w exactly as the slate backend's fenceTag
// does, so errors.Is(err, backend.ErrFenced) resolves through the wrap.
func (t fenceAtTx) Get(key []byte) ([]byte, error) {
	if t.at == "get" {
		return nil, fmt.Errorf("slate: tx get: %w", backend.ErrFenced)
	}
	return t.Transaction.Get(key)
}

func (t fenceAtTx) Put(key, value []byte) error {
	if t.at == "put" {
		return fmt.Errorf("slate: tx put: %w", backend.ErrFenced)
	}
	return t.Transaction.Put(key, value)
}

func (t fenceAtTx) Commit() error {
	if t.at == "commit" {
		return fmt.Errorf("slate: tx commit: %w", backend.ErrFenced)
	}
	return t.Transaction.Commit()
}

// TestApply_FencedAtAnyOp_RecodedToTransient pins the prod-only fix (#410 residual):
// a write whose Get, Put, OR Commit is FENCED (real slatedb surfaces the fence at
// whichever op runs first against a stale writer, AFTER a successful Begin) must be
// recoded to the TRANSIENT acquiring-window error, NOT returned as the raw fence.
// The raw fence is a HARD failure that fast-fails the fan-out unretried; the
// transient form makes the leg non-acking + RETRYABLE so the fan-out re-sends onto
// the re-resolved union (the successor that fenced it serves the write once it
// finishes mounting). The in-memory test double fences at Begin and cannot reach
// these surfaces.
func TestApply_FencedAtAnyOp_RecodedToTransient(t *testing.T) {
	for _, at := range []string{"get", "put", "commit"} {
		t.Run(at, func(t *testing.T) {
			backing := sharedfactory.NewBacking()
			c := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

			target := ru(0, 0, 0)
			fb := fenceAtBackend{Backend: memory.New(), at: at}
			env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1_000_000, NodeID: "w"}, Payload: []byte("v")})

			err := c.applyEnvelopeIfNewerToBackend(fb, target, []byte("k"), env)
			if err == nil {
				t.Fatalf("a fence at %s must surface an error, got nil", at)
			}
			if errors.Is(err, backend.ErrFenced) {
				t.Fatalf("a fence at %s leaked the RAW fence to the fan-out (a HARD fast-fail); got %v", at, err)
			}
			if !isAcquiringErr(err) {
				t.Fatalf("a fence at %s must recode to the TRANSIENT acquiring-window error so the fan-out retries; got %v", at, err)
			}
			if !isTransientReplicaErr(err) {
				t.Fatalf("a fence at %s must be classified transient by the fan-out (neither ack nor hard failure); got %v", at, err)
			}
		})
	}
}
