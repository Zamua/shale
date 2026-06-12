package cluster

// White-box tests for TriggerReshard, the OPERATOR-facing reshard entrypoint
// (behind the gRPC Reshard RPC + `shale reshard`). Unlike Reshard, which BLOCKS
// on reshardMu, TriggerReshard REJECTS a concurrent reshard with
// ErrReshardInFlight. These tests pin: (1) a clean trigger advances the
// generation + reports the {from, to} counts; (2) a concurrent trigger is
// rejected (not queued); (3) legacy mode refuses without touching the
// generation; (4) sequential triggers double again (idempotent-safe retry shape).

import (
	"errors"
	"sync"
	"testing"

	"github.com/Zamua/shale/pkg/backend/memory"
)

// TestTriggerReshard_AdvancesGenerationAndReportsCounts: a single trigger on a
// single-node multi-backend cluster doubles the unit count, advances the
// generation, and returns from=N, to=2N.
func TestTriggerReshard_AdvancesGenerationAndReportsCounts(t *testing.T) {
	c, _ := openReshardCluster(t, 2)

	from, to, err := c.TriggerReshard()
	if err != nil {
		t.Fatalf("TriggerReshard: %v", err)
	}
	if from != 2 || to != 4 {
		t.Fatalf("TriggerReshard reported from=%d to=%d, want from=2 to=4", from, to)
	}
	if gs := c.genSnapshot(); gs.gen != 1 || gs.count.N() != 4 {
		t.Fatalf("after trigger: gen=%d count=%d, want gen=1 count=4", gs.gen, gs.count.N())
	}
}

// TestTriggerReshard_SequentialDoublesAgain: a second trigger AFTER the first
// returns cleanly doubles again (4 -> 8). This is the idempotent-safe retry
// shape: once the first finishes and releases reshardMu, the next is accepted
// and advances from the now-current generation.
func TestTriggerReshard_SequentialDoublesAgain(t *testing.T) {
	c, _ := openReshardCluster(t, 2)

	if from, to, err := c.TriggerReshard(); err != nil || from != 2 || to != 4 {
		t.Fatalf("first TriggerReshard: from=%d to=%d err=%v, want 2->4 nil", from, to, err)
	}
	from, to, err := c.TriggerReshard()
	if err != nil {
		t.Fatalf("second TriggerReshard: %v", err)
	}
	if from != 4 || to != 8 {
		t.Fatalf("second TriggerReshard reported from=%d to=%d, want from=4 to=8", from, to)
	}
}

// TestTriggerReshard_ConcurrentIsRejected: while one reshard is in flight (held
// open via the entry hook), a SECOND concurrent TriggerReshard is rejected with
// ErrReshardInFlight and does NOT queue behind the first. Deterministic: the
// hook blocks the first reshard until the second has observed the held lock.
func TestTriggerReshard_ConcurrentIsRejected(t *testing.T) {
	c, _ := openReshardCluster(t, 2)

	inReshard := make(chan struct{}) // closed once the first reshard is inside the body
	release := make(chan struct{})   // the test closes this to let the first reshard finish
	var once sync.Once
	c.TestingSetReshardEntryHook(func() {
		once.Do(func() { close(inReshard) })
		<-release
	})

	// First trigger runs in a goroutine; it blocks inside the body on the hook.
	firstDone := make(chan error, 1)
	go func() {
		_, _, err := c.TriggerReshard()
		firstDone <- err
	}()

	<-inReshard // the first reshard now holds reshardMu and is parked in the hook

	// Second trigger, while the first holds reshardMu: must be REJECTED, not
	// queued. A blocking implementation would hang here until release.
	_, _, err := c.TriggerReshard()
	if !errors.Is(err, ErrReshardInFlight) {
		t.Fatalf("concurrent TriggerReshard err = %v, want ErrReshardInFlight", err)
	}
	// The rejected call must not have advanced the generation.
	if gs := c.genSnapshot(); gs.gen != 0 {
		t.Fatalf("generation = %d after rejected concurrent trigger, want 0 (untouched)", gs.gen)
	}

	// Release the first reshard; it must finish cleanly and advance the gen.
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first TriggerReshard: %v", err)
	}
	if gs := c.genSnapshot(); gs.gen != 1 || gs.count.N() != 4 {
		t.Fatalf("after first finished: gen=%d count=%d, want gen=1 count=4", gs.gen, gs.count.N())
	}
}

// TestTriggerReshard_LegacyModeRefused: a legacy single-Backend cluster (no
// factory) refuses the trigger with the multi-backend-only error and never
// advances any generation (legacy mode has no generation state to move).
func TestTriggerReshard_LegacyModeRefused(t *testing.T) {
	c, err := Open(Config{NodeID: "legacy", Backend: memory.New()})
	if err != nil {
		t.Fatalf("Open legacy: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	from, to, err := c.TriggerReshard()
	if err == nil {
		t.Fatal("TriggerReshard on a legacy cluster returned nil error, want refusal")
	}
	if from != 0 || to != 0 {
		t.Fatalf("legacy refusal reported from=%d to=%d, want 0,0", from, to)
	}
}

// TestTriggerReshard_ClosedRefused: a trigger on a closed cluster refuses
// cleanly (no panic, no hang) rather than operating on torn-down state.
func TestTriggerReshard_ClosedRefused(t *testing.T) {
	c, _ := openReshardCluster(t, 2) // cleanup Close is idempotent; this Close is the first
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := c.TriggerReshard(); err == nil {
		t.Fatal("TriggerReshard on a closed cluster returned nil error, want refusal")
	}
}
