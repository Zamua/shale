package cluster_test

// End-to-end pins for the under-W durability flush (outcome (c), the closing
// addition). Under a RELAXED backend (slate AwaitDurable=false) a committed-
// under-replicated CAS write's only copy is the owner's memtable (RAM); before
// acking it as durable the owner SYNCHRONOUSLY FLUSHES its local backend to the
// object store. These drive the real 3-node replicated harness and instrument
// the owner's backend as a backend.Flusher to prove:
//
//   (a) the under-W path flushes the owner backend exactly once before success,
//   (b) the normal W-reached path does NOT flush (the performance invariant),
//   (c) a flush FAILURE on the under-W path is a TERMINAL error - Transact
//       surfaces it and does NOT re-run fn (no amplification).
//
// See docs/SPEC.md "The four commit outcomes".

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
)

// countingFlushBackend wraps a memory backend and adds the optional
// backend.Flusher capability so a test can observe the cluster's under-W flush.
// The underlying memory is already synchronously durable, so Flush is a pure
// instrument (count + optional injected failure), never a real persist. fail
// is toggled via an atomic so the race detector stays quiet if any background
// path ever calls Flush concurrently.
type countingFlushBackend struct {
	backend.Backend
	flushes atomic.Int64
	fail    atomic.Bool
}

var errFlushBoom = errors.New("under-W flush: object store unreachable")

func (b *countingFlushBackend) Flush() error {
	b.flushes.Add(1)
	if b.fail.Load() {
		return errFlushBoom
	}
	return nil
}

// startFlushInstrumentedCluster brings up the 3-node replicated harness with
// each node's backend wrapped in a countingFlushBackend, and returns the nodes
// plus the per-node wrappers (indexed by node). Counters are zeroed AFTER
// startup so any bootstrap-time activity does not pollute the assertions.
func startFlushInstrumentedCluster(t *testing.T, wc cluster.WriteConsistency) ([]*replicatedNode, []*countingFlushBackend) {
	t.Helper()
	wrappers := make([]*countingFlushBackend, 3)
	nodes := startThreeNodeReplicatedCluster(t, 3, wc, cluster.ReadNearest,
		withWrappedBackends(func(i int, mem *memory.Memory) backend.Backend {
			wrappers[i] = &countingFlushBackend{Backend: mem}
			return wrappers[i]
		}))
	for _, w := range wrappers {
		w.flushes.Store(0)
	}
	return nodes, wrappers
}

// insertIfAbsent is the canonical create-if-absent closure the CAS tests drive.
func insertIfAbsent(key, val []byte) func(tx backend.Transaction) error {
	return func(tx backend.Transaction) error {
		if _, gerr := tx.Get(key); gerr != nil && !errors.Is(gerr, backend.ErrNotFound) {
			return gerr
		}
		return tx.Put(key, val)
	}
}

// TestCASReplicate_UnderReplicated_FlushesOwnerOnceBeforeSuccess pins (a): an
// under-W commit (a co-replica down so the fan-out cannot reach W=3) flushes the
// OWNER's backend exactly once before returning success. The write's only copy
// under a relaxed backend is the owner's memtable; the flush makes it bucket-
// durable. Before the closing addition this flush did not happen (RED: flushes
// stays 0).
func TestCASReplicate_UnderReplicated_FlushesOwnerOnceBeforeSuccess(t *testing.T) {
	setUnderReplicatedFastFail(t)
	nodes, wrappers := startFlushInstrumentedCluster(t, cluster.WriteAll)

	key := []byte("flush:under-w")
	owner := ownsCASPinIndex(t, nodes, key)
	stopACoReplica(nodes, owner) // fan-out now reaches at most 2 acks < W=3

	if err := nodes[owner].cluster.Transact(key, insertIfAbsent(key, []byte("v"))); err != nil {
		t.Fatalf("under-W commit must SUCCEED (committed-under-replicated); got %v", err)
	}
	if got := wrappers[owner].flushes.Load(); got != 1 {
		t.Fatalf("under-W commit must flush the OWNER backend exactly once; got %d", got)
	}
	// The lone copy is durable on the owner's memory (the wrapper's flush is an
	// instrument; the real durability here is the in-memory write).
	env, ok := decodeReplica(t, nodes[owner], key)
	if !ok || string(env.Payload) != "v" {
		t.Fatalf("under-replicated write must be present on the owner; present=%v payload=%q", ok, env.Payload)
	}
}

// TestCASReplicate_WReached_DoesNotFlush pins (b), the PERFORMANCE invariant:
// the normal path (all replicas up, fan-out reaches W) must NOT flush any
// backend. Flushing on the W-reached path would defeat the whole relaxed-
// durability design (durability comes from the W in-RAM replicas).
func TestCASReplicate_WReached_DoesNotFlush(t *testing.T) {
	setUnderReplicatedFastFail(t)
	nodes, wrappers := startFlushInstrumentedCluster(t, cluster.WriteAll)

	key := []byte("flush:w-reached")
	owner := ownsCASPinIndex(t, nodes, key)

	if err := nodes[owner].cluster.Transact(key, insertIfAbsent(key, []byte("v"))); err != nil {
		t.Fatalf("fully-replicated commit must SUCCEED; got %v", err)
	}
	for i, w := range wrappers {
		if got := w.flushes.Load(); got != 0 {
			t.Fatalf("W-reached commit must NOT flush any backend; node %d flushed %d times", i, got)
		}
	}
}

// TestCASReplicate_UnderReplicated_FlushErrorIsTerminal pins (c): when the
// under-W flush ITSELF fails, the commit is a genuine durability failure -
// Transact surfaces the error and does NOT re-run fn. A flush I/O error is
// terminal (not the retryable under-W codes.Unavailable), so there is no
// amplification: fn runs exactly once.
func TestCASReplicate_UnderReplicated_FlushErrorIsTerminal(t *testing.T) {
	setUnderReplicatedFastFail(t)
	nodes, wrappers := startFlushInstrumentedCluster(t, cluster.WriteAll)

	key := []byte("flush:under-w-fails")
	owner := ownsCASPinIndex(t, nodes, key)
	stopACoReplica(nodes, owner)     // force under-W
	wrappers[owner].fail.Store(true) // the durability flush will fail

	fnRuns := 0
	err := nodes[owner].cluster.Transact(key, func(tx backend.Transaction) error {
		fnRuns++
		return insertIfAbsent(key, []byte("v"))(tx)
	})
	if !errors.Is(err, errFlushBoom) {
		t.Fatalf("a failed under-W durability flush must surface terminally; got %v", err)
	}
	if fnRuns != 1 {
		t.Fatalf("a terminal flush failure must NOT re-run fn (no amplification); ran %d", fnRuns)
	}
	if got := wrappers[owner].flushes.Load(); got != 1 {
		t.Fatalf("the flush must be attempted exactly once; got %d", got)
	}
}
