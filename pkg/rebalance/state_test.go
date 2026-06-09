package rebalance_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/ring"
)

// blockingSource is a MigrateSource whose OpenRange does not return
// until release() is called. Tests use it to pin the Coordinator's
// runSend goroutine in StateSending long enough to observe state
// from the outside.
type blockingSource struct {
	mu       sync.Mutex
	released bool
	cond     *sync.Cond
}

func newBlockingSource() *blockingSource {
	b := &blockingSource{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *blockingSource) OpenRange(_ []uint64, _ uint64) (<-chan rebalance.KeyValue, <-chan error) {
	out := make(chan rebalance.KeyValue)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		b.mu.Lock()
		for !b.released {
			b.cond.Wait()
		}
		b.mu.Unlock()
		errCh <- nil
	}()
	return out, errCh
}

func (b *blockingSource) release() {
	b.mu.Lock()
	b.released = true
	b.cond.Broadcast()
	b.mu.Unlock()
}

// findSendingKey returns a key that hashes onto a partition n1 owns
// in oldR and n2 owns in newR, so the move shows up in n1's plan.
func findSendingKey(t *testing.T, oldR, newR *ring.Ring, sender, receiver string) []byte {
	t.Helper()
	for i := 0; i < 5000; i++ {
		k := []byte(fmt.Sprintf("k-%d", i))
		if oldR.LocateKey(k).ID == sender && newR.LocateKey(k).ID == receiver {
			return k
		}
	}
	t.Fatalf("could not find a key that moves from %s to %s in 5000 samples", sender, receiver)
	return nil
}

func TestIsMigrating_TrueDuringSending(t *testing.T) {
	old := buildRing("n1")
	new := buildRing("n1", "n2")
	self := rebalance.Member{ID: "n1", Addr: "addr:n1"}

	be := memory.New()
	defer be.Close()

	moving := findSendingKey(t, old, new, "n1", "n2")

	src := newBlockingSource()
	opts := rebalance.DefaultOptions()
	opts.Source = src
	c := rebalance.New(self, be, opts)
	defer c.Stop()
	// Release once we are done so runSend can shut down.
	defer src.release()

	c.Evaluate(old, new, 1)

	// runSend is now blocked waiting on src; the range is in
	// StateSending. IsMigrating should report true for the moving
	// key. We poll briefly because Evaluate launches runSend in a
	// goroutine.
	deadline := time.Now().Add(time.Second)
	for !c.IsMigrating(moving) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !c.IsMigrating(moving) {
		t.Fatalf("IsMigrating should be true for a key in StateSending; snapshot: %+v", c.Snapshot())
	}

	// Pick a key that never moves; IsMigrating should be false.
	for i := 0; i < 5000; i++ {
		k := []byte(fmt.Sprintf("stable-%d", i))
		if old.LocateKey(k).ID == new.LocateKey(k).ID {
			if c.IsMigrating(k) {
				t.Fatalf("IsMigrating should be false for stable key %q; snapshot: %+v", k, c.Snapshot())
			}
			return
		}
	}
	t.Fatal("did not find a stable key in 5000 samples")
}

func TestIsMigrating_FalseForStableRing(t *testing.T) {
	r := buildRing("n1", "n2")
	self := rebalance.Member{ID: "n1", Addr: "addr:n1"}
	be := memory.New()
	defer be.Close()

	c := rebalance.New(self, be, rebalance.DefaultOptions())
	defer c.Stop()

	c.Evaluate(r, r, 1)
	if err := c.WaitForIdle(timeout(t, time.Second)); err != nil {
		t.Fatalf("WaitForIdle: %v", err)
	}
	if c.IsMigrating([]byte("anything")) {
		t.Fatalf("IsMigrating should be false on a stable ring; snapshot: %+v", c.Snapshot())
	}
}

func TestSnapshot_OrderedByPartitionID(t *testing.T) {
	old := buildRing("n1")
	new := buildRing("n1", "n2")
	self := rebalance.Member{ID: "n1", Addr: "addr:n1"}

	be := memory.New()
	defer be.Close()

	src := newBlockingSource()
	opts := rebalance.DefaultOptions()
	opts.Source = src
	c := rebalance.New(self, be, opts)
	defer c.Stop()
	defer src.release()

	c.Evaluate(old, new, 1)

	// Let the Sends register; runSend will block in the source.
	time.Sleep(50 * time.Millisecond)

	snap := c.Snapshot()
	if len(snap) == 0 {
		t.Fatalf("snapshot should have entries; got 0")
	}
	for i := 1; i < len(snap); i++ {
		if snap[i-1].PartitionID > snap[i].PartitionID {
			t.Fatalf("Snapshot not ordered by partition id at %d: %d then %d",
				i, snap[i-1].PartitionID, snap[i].PartitionID)
		}
	}
}

func TestWaitForIdle_ReturnsImmediatelyWhenNothingPending(t *testing.T) {
	self := rebalance.Member{ID: "n1", Addr: "addr:n1"}
	be := memory.New()
	defer be.Close()

	c := rebalance.New(self, be, rebalance.DefaultOptions())
	defer c.Stop()

	if err := c.WaitForIdle(timeout(t, 200*time.Millisecond)); err != nil {
		t.Fatalf("WaitForIdle on empty Coordinator: %v", err)
	}
}

func TestWaitForIdle_RespectsContextCancel(t *testing.T) {
	self := rebalance.Member{ID: "n1", Addr: "addr:n1"}
	be := memory.New()
	defer be.Close()

	src := newBlockingSource()
	opts := rebalance.DefaultOptions()
	opts.Source = src
	c := rebalance.New(self, be, opts)
	defer c.Stop()
	defer src.release()

	old := buildRing("n1")
	new := buildRing("n1", "n2")
	c.Evaluate(old, new, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := c.WaitForIdle(ctx)
	if err == nil {
		t.Fatalf("WaitForIdle should return ctx error when migrations are still in flight")
	}
}

// timeout is a tiny ergonomic wrapper: returns a context that fails
// the test if it expires before the work completes. Each test gets a
// fresh context so a stuck goroutine does not leak into other cases.
func timeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
