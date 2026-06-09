package rebalance_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/ring"
)

// seedKeysOwnedBy populates be with N keys whose hashes land on
// owner per the given ring. Returns the keys + values written so
// tests can re-read them on the destination.
func seedKeysOwnedBy(t *testing.T, be backend.Backend, r *ring.Ring, owner string, n int) [][2][]byte {
	t.Helper()
	var got [][2][]byte
	for i := 0; len(got) < n && i < 200000; i++ {
		k := []byte(fmt.Sprintf("seed-%d", i))
		if r.LocateKey(k).ID != owner {
			continue
		}
		v := []byte(fmt.Sprintf("value-%d", i))
		if err := be.Put(k, v); err != nil {
			t.Fatalf("seed put: %v", err)
		}
		got = append(got, [2][]byte{k, v})
	}
	if len(got) < n {
		t.Fatalf("could not seed %d keys for owner %s in 200000 samples; got %d", n, owner, len(got))
	}
	return got
}

func TestFetchRange_CopiesAllKeys_ChecksumMatches(t *testing.T) {
	// Single ring; we treat n1's keys as "the range" and have n2
	// pull them. PartitionFn is the ring's PartitionID so the
	// source's per-key filter matches what the destination expects.
	r := buildRing("n1", "n2")

	src := memory.New()
	defer src.Close()
	dst := memory.New()
	defer dst.Close()

	seeded := seedKeysOwnedBy(t, src, r, "n1", 50)

	// Collect every partition id n1 owns; that is the migration unit.
	parts := partitionsOwnedBy(r, "n1")

	partFn := func(k []byte) uint64 { return r.PartitionID(k) }
	source := rebalance.NewLocalSource(src, partFn)
	dest := rebalance.NewInProcessDestination(dst, func(_ rebalance.Member) (rebalance.MigrateSource, error) {
		return source, nil
	})

	n, err := dest.FetchRange(context.Background(), rebalance.Member{ID: "n1"}, parts, 1)
	if err != nil {
		t.Fatalf("FetchRange: %v", err)
	}
	if n != len(seeded) {
		t.Fatalf("FetchRange returned %d keys; seeded %d", n, len(seeded))
	}

	// Every seeded KV must now be present on dst with identical bytes.
	for _, kv := range seeded {
		got, err := dst.Get(kv[0])
		if err != nil {
			t.Fatalf("dst.Get(%q): %v", kv[0], err)
		}
		if !bytes.Equal(got, kv[1]) {
			t.Fatalf("dst.Get(%q) = %q, want %q", kv[0], got, kv[1])
		}
	}

	// Source-side checksum + destination's computed checksum must
	// match: same KVs, same hash function. SourceChecksum sorts
	// keys before hashing so the comparison is independent of scan
	// order on either side.
	srcSum, srcCount, err := rebalance.SourceChecksum(src, parts, partFn)
	if err != nil {
		t.Fatalf("SourceChecksum src: %v", err)
	}
	dstSum, dstCount, err := rebalance.SourceChecksum(dst, parts, partFn)
	if err != nil {
		t.Fatalf("SourceChecksum dst: %v", err)
	}
	if srcCount != dstCount {
		t.Fatalf("source count %d != dest count %d", srcCount, dstCount)
	}
	if !bytes.Equal(srcSum, dstSum) {
		t.Fatalf("checksum mismatch: src=%x dst=%x", srcSum, dstSum)
	}
}

func TestFetchRange_SourceErrorRollsBack(t *testing.T) {
	dst := memory.New()
	defer dst.Close()

	src := &failingSource{err: errors.New("simulated source failure")}
	dest := rebalance.NewInProcessDestination(dst, func(_ rebalance.Member) (rebalance.MigrateSource, error) {
		return src, nil
	})

	_, err := dest.FetchRange(context.Background(), rebalance.Member{ID: "n1"}, []uint64{0}, 1)
	if err == nil {
		t.Fatalf("FetchRange should have returned the source error")
	}

	// The failing source emits one KV before erroring; that KV must
	// be rolled back so dst is empty.
	if _, err := dst.Get([]byte("partial")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("partial write should have been rolled back; Get returned %v", err)
	}
}

func TestFetchRange_ContextCancelRollsBack(t *testing.T) {
	dst := memory.New()
	defer dst.Close()

	src := &blockingSource{}
	src.cond = sync.NewCond(&src.mu)
	dest := rebalance.NewInProcessDestination(dst, func(_ rebalance.Member) (rebalance.MigrateSource, error) {
		return src, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err := dest.FetchRange(ctx, rebalance.Member{ID: "n1"}, []uint64{0}, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("FetchRange should return context.Canceled, got %v", err)
	}

	// Release so the goroutine doesn't leak after the test exits.
	src.release()
}

// failingSource emits one KV then signals an error on errCh. Used to
// verify the destination's rollback path: the partial write must be
// undone before the error propagates back to the caller.
type failingSource struct {
	err error
}

func (s *failingSource) OpenRange(_ []uint64, _ uint64) (<-chan rebalance.KeyValue, <-chan error) {
	out := make(chan rebalance.KeyValue, 1)
	errCh := make(chan error, 1)
	out <- rebalance.KeyValue{Key: []byte("partial"), Value: []byte("v")}
	close(out)
	errCh <- s.err
	close(errCh)
	return out, errCh
}

// partitionsOwnedBy returns every partition id whose owner in r is
// the given member.
func partitionsOwnedBy(r *ring.Ring, owner string) []uint64 {
	var out []uint64
	for _, pid := range r.Partitions() {
		if r.Owner(pid).ID == owner {
			out = append(out, pid)
		}
	}
	return out
}
