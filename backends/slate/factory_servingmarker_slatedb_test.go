//go:build slatedb && integration

// factory_servingmarker_slatedb_test.go: the v0.8 Phase 2e (Option B overlap
// handoff) REAL-SLATE RELEASE GATE.
//
// These two tests are the staging acceptance pins the spec ("v0.8 Phase 2e",
// the slate-manifest-seal section) requires before Phase 2e ships. They run
// against REAL slatedb instances in a REAL shared MinIO bucket and pin the ONE
// assumption the Go layer CANNOT prove from the slatedb-go v0.13.1 binding (an
// opaque uniffi FFI over the Rust core that exposes no WithEpoch setter and no
// fence-vs-recovery hook on DbBuilder): that the new owner's OpenReplicaUnit
// makes the manifest-epoch FENCE effective NO LATER THAN its WAL-recovery
// cutoff, so every write the OLD owner could still ack (directly before the
// fence, OR forwarded concurrently with the flip, both below epoch E) lands in
// the new owner's recovered tail and is visible after the handoff.
//
// They are GATED `slatedb && integration` (cgo binding + a running MinIO) and
// are NOT run in the RAM-constrained dev loop (the slatedb Rust+cgo build is
// heavy). They are the RELEASE GATE: Phase 2e is BLOCKED until BOTH pass on
// real slate. If either FAILS, the Rust core recovers-then-fences (or does not
// replay the full durable WAL tail on open), and OpenReplicaUnit must be
// adjusted to RE-SCAN the WAL tail after the fence (via the WalReader surface)
// before serving. See the FENCE <= RECOVERY ORDERING INVARIANT comment in
// factory.go OpenReplicaUnit and docs/SPEC.md "v0.8 Phase 2e".
//
// They reuse the harness (startFactoryMinIO / newBacking / gu) from
// factory_minio_integration_test.go (same package slate_test, same build tags).

package slate_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

// ru builds a generation-0 ReplicaUnit for the pin tests.
func ruSM(id storageunit.UnitID, replica uint8) storageunit.ReplicaUnit {
	return storageunit.NewReplicaUnit(gu(0, id), replica)
}

// TestServingMarker_WriteBeforeFenceVisibleAfterHandoff is PIN (1): a write
// ACKED on the OLD owner JUST BEFORE the fence must be readable on the NEW owner
// AFTER the handoff. This is the base seal: the new owner's open must recover
// the full durable WAL tail (an acked write durable-before-ack at T_write <
// T_fence may be in the WAL but not yet in the manifest snapshot the new owner
// read at fence time), AND the fence must be effective at-or-before the recovery
// cutoff so that write is inside the recovered tail.
//
// Sequence: OLD opens replica position P at epoch 1, writes + acks a recorded
// dataset (AwaitDurable=true makes each ack mean durable in the bucket). Then
// NEW opens the SAME position P at a higher epoch (the handoff fence). NEW must
// read EVERY key OLD acked. The serving marker is then written by NEW and read
// back by an OLD-side handle to prove the poll-only release signal round-trips
// over real shared storage at the open epoch.
func TestServingMarker_WriteBeforeFenceVisibleAfterHandoff(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBacking(t, fx)

	oldOwner := b.Handle()
	newOwner := b.Handle()
	p := ruSM(3, 1)

	// OLD acquires P at epoch 1 and writes a recorded, acked dataset.
	beOld, err := oldOwner.OpenReplicaUnit(p, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("OLD OpenReplicaUnit: %v", err)
	}
	const n = 200
	want := make(map[string]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k:%04d", i)
		v := fmt.Sprintf("v-%d", i)
		if err := beOld.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("OLD put %d: %v", i, err)
		}
		want[k] = v
	}

	// The new owner reads OLD's durable fence epoch and acquires at a strictly
	// higher epoch (the handoff). This fences OLD. The position's bytes are NOT
	// copied: NEW opens the SAME durable prefix dbNameReplica(P).
	durable, err := newOwner.DurableEpochReplica(p)
	if err != nil {
		t.Fatalf("DurableEpochReplica: %v", err)
	}
	openEpoch := durable + 1
	beNew, err := newOwner.OpenReplicaUnit(p, openEpoch)
	if err != nil {
		t.Fatalf("NEW OpenReplicaUnit: %v", err)
	}

	// THE SEAL ASSERTION: every key OLD acked before the fence is visible on NEW.
	for k, v := range want {
		got, err := beNew.Get([]byte(k))
		if err != nil {
			t.Fatalf("NEW Get %q (acked-before-fence write LOST across handoff): %v", k, err)
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Fatalf("NEW Get %q = %q, want %q", k, got, v)
		}
	}

	// The poll-only release signal round-trips: NEW writes the serving marker at
	// its open epoch (its mount flip), and an OLD-side read observes it.
	if err := newOwner.WriteServingMarker(p, openEpoch); err != nil {
		t.Fatalf("WriteServingMarker: %v", err)
	}
	gotEpoch, ok, err := oldOwner.ReadServingMarker(p)
	if err != nil {
		t.Fatalf("ReadServingMarker: %v", err)
	}
	if !ok || gotEpoch != openEpoch {
		t.Fatalf("ReadServingMarker = (%d,%v), want (%d,true)", gotEpoch, ok, openEpoch)
	}
}

// TestServingMarker_ForwardConcurrentWithFlipVisibleAfterHandoff is PIN (2):
// the harder case forwarding INTRODUCES. A write driven through the FORWARD path
// is acked on the OLD owner BELOW epoch E while the NEW owner's open/recovery
// RACES the flip. Because forwarding continues until the mount flip (strictly
// AFTER OpenReplicaUnit returns), such a write can be acked on OLD AFTER NEW's
// recovery snapshot but still below E. For the seal to hold, the fence must be
// effective no later than NEW's recovery cutoff, so the write is either inside
// NEW's recovered tail (acked, visible) or fenced (never acked) - never "acked
// on OLD, invisible to NEW."
//
// We model the forward at the durable layer (this is a factory-level pin, not a
// cluster-level forward-RPC test; the cluster's overlap controller is tested in
// the integration gate): a background writer keeps writing + acking through
// OLD's handle (each ack = durable below E) WHILE NEW opens the same position at
// epoch E. After NEW's open completes the writer stops; we then assert NEW reads
// every key the writer ACKED (an ack means OLD's fencedReplica accepted it below
// E and it is durable), and that once NEW is open OLD's further writes are
// FENCED (the single-writer guarantee at the cut). No acked write may be
// invisible to NEW.
func TestServingMarker_ForwardConcurrentWithFlipVisibleAfterHandoff(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBacking(t, fx)

	oldOwner := b.Handle()
	newOwner := b.Handle()
	p := ruSM(4, 0)

	beOld, err := oldOwner.OpenReplicaUnit(p, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("OLD OpenReplicaUnit: %v", err)
	}

	// Background writer: write + ack through OLD's handle (each ack durable below
	// E) and record exactly the keys whose Put RETURNED NIL (acked). A Put that
	// returns an error is a fenced/failed write the client would retry; it is NOT
	// recorded, mirroring the record-only-acked discipline of the availability
	// gate. The writer runs until told to stop (after NEW's open completes).
	var (
		mu      sync.Mutex
		acked   = make(map[string]string)
		stop    = make(chan struct{})
		writerD = make(chan struct{})
	)
	go func() {
		defer close(writerD)
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			k := fmt.Sprintf("fwd:%06d", i)
			v := fmt.Sprintf("fv-%d", i)
			if err := beOld.Put([]byte(k), []byte(v)); err == nil {
				mu.Lock()
				acked[k] = v
				mu.Unlock()
			}
			i++
		}
	}()

	// NEW opens the SAME position at E = durable+1, racing the forward writer.
	durable, err := newOwner.DurableEpochReplica(p)
	if err != nil {
		t.Fatalf("DurableEpochReplica: %v", err)
	}
	openEpoch := durable + 1
	beNew, err := newOwner.OpenReplicaUnit(p, openEpoch)
	if err != nil {
		t.Fatalf("NEW OpenReplicaUnit: %v", err)
	}

	// The mount flip: NEW serves locally + writes the serving marker. Stop the
	// forward writer (in a real handoff the new owner stops forwarding here).
	if err := newOwner.WriteServingMarker(p, openEpoch); err != nil {
		t.Fatalf("WriteServingMarker: %v", err)
	}
	close(stop)
	<-writerD

	// Snapshot the acked set (writer has stopped, no more mutation).
	mu.Lock()
	final := make(map[string]string, len(acked))
	for k, v := range acked {
		final[k] = v
	}
	mu.Unlock()
	if len(final) == 0 {
		t.Fatal("no forwarded writes acked; test did not exercise the concurrent window")
	}

	// THE SEAL ASSERTION (forwarding widened): every write ACKED on OLD below E
	// (whether before or during NEW's open/recovery race) is visible on NEW.
	for k, v := range final {
		got, err := beNew.Get([]byte(k))
		if err != nil {
			t.Fatalf("NEW Get %q (forwarded-acked-below-E write LOST across flip): %v", k, err)
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Fatalf("NEW Get %q = %q, want %q", k, got, v)
		}
	}

	// And OLD is fenced at/after E: a further OLD write must FAIL (single writer
	// at the cut; no acked-past-fence write can exist).
	if err := beOld.Put([]byte("post-fence"), []byte("x")); err == nil {
		t.Fatal("OLD write after NEW opened at E succeeded; the fence did not lock OLD out (split-brain)")
	}
}
