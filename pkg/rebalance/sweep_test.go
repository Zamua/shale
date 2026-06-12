package rebalance_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/rebalance"
)

// runUntil polls fn until it returns true or d elapses. Returns
// whether fn ever returned true. Used by sweep tests because the
// transition Sending -> HandedOff -> Done crosses goroutine
// boundaries + we don't want a race-prone sleep.
func runUntil(d time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fn()
}

func TestSweep_DeletesHandedOffKeysAfterGrace(t *testing.T) {
	old := buildRing("n1")
	next := buildRing("n1", "n2")
	self := rebalance.Member{ID: "n1", Addr: "addr:n1"}

	be := memory.New()
	defer func() { _ = be.Close() }()

	// Seed both moving keys (partition shifts to n2) + stable keys
	// (partition stays on n1). After sweep, moving keys are gone +
	// stable keys remain.
	var moving, stable [][]byte
	for i := 0; len(moving) < 5 || len(stable) < 5; i++ {
		if i > 50000 {
			t.Fatalf("could not seed enough moving/stable keys")
		}
		k := fmt.Appendf(nil, "k-%d", i)
		oldOwner := old.LocateKey(k).ID
		newOwner := next.LocateKey(k).ID
		v := fmt.Appendf(nil, "v-%d", i)
		switch {
		case oldOwner == "n1" && newOwner == "n2" && len(moving) < 5:
			if err := be.Put(k, v); err != nil {
				t.Fatalf("be.Put: %v", err)
			}
			moving = append(moving, k)
		case oldOwner == "n1" && newOwner == "n1" && len(stable) < 5:
			if err := be.Put(k, v); err != nil {
				t.Fatalf("be.Put: %v", err)
			}
			stable = append(stable, k)
		}
	}

	// Source-only mode + short grace so the test stays under a sec.
	opts := rebalance.DefaultOptions()
	opts.GraceDuration = 30 * time.Millisecond
	c := rebalance.New(self, be, opts)
	defer c.Stop()

	c.Evaluate(old, next, 1)

	// Wait until every move has reached at least HandedOff.
	if !runUntil(2*time.Second, func() bool {
		for _, s := range c.Snapshot() {
			if s.State == rebalance.StatePreMigration || s.State == rebalance.StateSending {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("ranges did not reach HandedOff in time; snap=%+v", c.Snapshot())
	}

	// Give grace a chance to elapse, then run one sweep tick.
	time.Sleep(50 * time.Millisecond)
	c.SweepOnce(context.Background(), time.Now())

	// Moving keys must be gone from the local backend now.
	for _, k := range moving {
		if _, err := be.Get(k); !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("moving key %q should have been swept; Get returned %v", k, err)
		}
	}
	// Stable keys must still be present.
	for _, k := range stable {
		if _, err := be.Get(k); err != nil {
			t.Fatalf("stable key %q must remain; Get returned %v", k, err)
		}
	}
	// Every swept range is now StateDone.
	for _, s := range c.Snapshot() {
		if s.State != rebalance.StateDone {
			t.Fatalf("range %d in state %s, expected Done after sweep", s.PartitionID, s.State)
		}
	}
}

func TestSweep_PreGraceRangesUntouched(t *testing.T) {
	old := buildRing("n1")
	next := buildRing("n1", "n2")
	self := rebalance.Member{ID: "n1", Addr: "addr:n1"}

	be := memory.New()
	defer func() { _ = be.Close() }()

	// Seed a moving key so there is something the sweep could delete.
	var moving []byte
	for i := range 5000 {
		k := fmt.Appendf(nil, "k-%d", i)
		if old.LocateKey(k).ID == "n1" && next.LocateKey(k).ID == "n2" {
			moving = k
			if err := be.Put(k, []byte("v")); err != nil {
				t.Fatalf("be.Put: %v", err)
			}
			break
		}
	}
	if moving == nil {
		t.Fatal("could not find a moving key")
	}

	// Long grace; the sweep tick happens well before the deadline.
	opts := rebalance.DefaultOptions()
	opts.GraceDuration = 10 * time.Second
	c := rebalance.New(self, be, opts)
	defer c.Stop()

	c.Evaluate(old, next, 1)

	// Wait until at least one range reaches HandedOff.
	if !runUntil(2*time.Second, func() bool {
		for _, s := range c.Snapshot() {
			if s.State == rebalance.StateHandedOff {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("no range reached HandedOff; snap=%+v", c.Snapshot())
	}

	// Sweep with now == handoff time: grace has not elapsed.
	c.SweepOnce(context.Background(), time.Now())

	// Moving key must still be present.
	if _, err := be.Get(moving); err != nil {
		t.Fatalf("pre-grace sweep removed key %q (err=%v); should have left it alone", moving, err)
	}
	// HandedOff ranges must NOT have flipped to Done.
	sawHandedOff := false
	for _, s := range c.Snapshot() {
		if s.State == rebalance.StateHandedOff {
			sawHandedOff = true
		}
		if s.State == rebalance.StateDone {
			// Done ranges with KeyCount==0 are receive-side no-ops in
			// source-only mode; only Send ranges should still be
			// HandedOff. Filter on From == self to spot the failure.
			if s.From.ID == "n1" {
				t.Fatalf("range %d flipped to Done before grace elapsed", s.PartitionID)
			}
		}
	}
	if !sawHandedOff {
		t.Fatalf("expected at least one HandedOff range to verify pre-grace behavior")
	}
}
