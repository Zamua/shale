package chaos

// Unit tests for the pure no-acked-write-loss oracle. These carry NO build tag,
// so they run under the normal `go test ./...` and keep the model honest even
// when the full soak is not run. The model is the safety net; a blind oracle
// would rubber-stamp a real loss, so we pin that it returns PASS for correct
// reads AND the right violation verdict for every loss shape.

import "testing"

func TestOraclePassOnLatestValue(t *testing.T) {
	o := NewOracle()
	val := EncodeValue("k1", 1, "payload")
	o.RecordPut("k1", val, 1)

	if vd, detail := o.Verify("k1", Observed{Value: val}); vd != VerdictPass {
		t.Fatalf("reading the latest acked value should PASS, got %s: %s", vd, detail)
	}
}

func TestOracleLostOnNotFoundForLiveKey(t *testing.T) {
	o := NewOracle()
	o.RecordPut("k1", EncodeValue("k1", 1, "p"), 1)

	// A key with an acked live value that reads back not-found is the canonical
	// lost write.
	vd, detail := o.Verify("k1", Observed{NotFound: true})
	if vd != VerdictLost {
		t.Fatalf("not-found for a live acked key should be LOST, got %s: %s", vd, detail)
	}
}

func TestOracleStaleOnOlderAckedValue(t *testing.T) {
	o := NewOracle()
	v1 := EncodeValue("k1", 1, "p")
	v2 := EncodeValue("k1", 2, "p")
	o.RecordPut("k1", v1, 1)
	o.RecordPut("k1", v2, 2)

	// Reading version 1 after version 2 was acked is a stale read: the newer acked
	// write is not visible where an older one is.
	vd, detail := o.Verify("k1", Observed{Value: v1})
	if vd != VerdictStale {
		t.Fatalf("older acked value after a newer ack should be STALE, got %s: %s", vd, detail)
	}
	// The latest still passes.
	if vd, _ := o.Verify("k1", Observed{Value: v2}); vd != VerdictPass {
		t.Fatalf("latest value should PASS, got %s", vd)
	}
}

func TestOracleCorruptOnNeverAckedValue(t *testing.T) {
	o := NewOracle()
	o.RecordPut("k1", EncodeValue("k1", 1, "p"), 1)

	vd, detail := o.Verify("k1", Observed{Value: "garbage-never-written"})
	if vd != VerdictCorrupt {
		t.Fatalf("a payload never acked for the key should be CORRUPT, got %s: %s", vd, detail)
	}
}

func TestOracleDeleteThenNotFoundPasses(t *testing.T) {
	o := NewOracle()
	o.RecordPut("k1", EncodeValue("k1", 1, "p"), 1)
	o.RecordDelete("k1", 2)

	if vd, detail := o.Verify("k1", Observed{NotFound: true}); vd != VerdictPass {
		t.Fatalf("not-found after an acked delete should PASS, got %s: %s", vd, detail)
	}
}

func TestOracleResurrectedOnValueForDeletedKey(t *testing.T) {
	o := NewOracle()
	v1 := EncodeValue("k1", 1, "p")
	o.RecordPut("k1", v1, 1)
	o.RecordDelete("k1", 2)

	// A value (even the pre-delete value) coming back for a deleted key means the
	// acked delete was lost / a stale value out-raced the tombstone.
	vd, detail := o.Verify("k1", Observed{Value: v1})
	if vd != VerdictResurrected {
		t.Fatalf("a value for a deleted key should be RESURRECTED, got %s: %s", vd, detail)
	}
}

func TestOracleVersionMustIncreasePerKey(t *testing.T) {
	o := NewOracle()
	o.RecordPut("k1", EncodeValue("k1", 5, "p"), 5)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("recording a non-increasing version for a key should panic (single-writer-per-key violated)")
		}
	}()
	o.RecordPut("k1", EncodeValue("k1", 3, "p"), 3) // 3 <= 5: must panic
}

func TestOracleInFlightTolerance(t *testing.T) {
	o := NewOracle()
	o.RecordPut("k1", EncodeValue("k1", 1, "p"), 1)

	// A delete is in flight: a reader that sees not-found mid-flight must NOT be
	// flagged (the tombstone may have landed durably before Delete returned).
	o.BeginOp("k1")
	if vd, _ := o.Verify("k1", Observed{NotFound: true}); vd != VerdictPass {
		t.Fatalf("not-found during an in-flight delete should be tolerated (PASS), got %s", vd)
	}
	// But a payload NEVER acked for the key is still CORRUPT even in flight.
	if vd, _ := o.Verify("k1", Observed{Value: "totally-bogus"}); vd != VerdictCorrupt {
		t.Fatalf("a never-acked payload during in-flight should still be CORRUPT, got %s", vd)
	}
	o.RecordDelete("k1", 2)
	o.EndOp("k1")

	// After the window closes, strictness returns: a value for the deleted key is
	// a resurrection.
	if vd, _ := o.Verify("k1", Observed{Value: EncodeValue("k1", 1, "p")}); vd != VerdictResurrected {
		t.Fatalf("after the in-flight window, a value for a deleted key should be RESURRECTED, got %s", vd)
	}
}

func TestOracleInFlightTolerangesPendingWriteLandingEarly(t *testing.T) {
	o := NewOracle()
	o.RecordPut("k1", EncodeValue("k1", 1, "p"), 1)

	// An overwrite to version 2 is in flight: the cluster made it durable+visible a
	// hair before Put returned + RecordPut(2) ran, so a reader can observe v2 while
	// the model is still at v1 and v2 is not yet in everValues. This is the pending
	// write landing early - it must PASS, not be flagged CORRUPT.
	o.BeginOp("k1")
	pending := EncodeValue("k1", 2, "p")
	if vd, detail := o.Verify("k1", Observed{Value: pending}); vd != VerdictPass {
		t.Fatalf("the in-flight write observed early should PASS, got %s: %s", vd, detail)
	}
	// A value addressed to a DIFFERENT key during in-flight is still CORRUPT.
	bogus := EncodeValue("other", 9, "p")
	if vd, _ := o.Verify("k1", Observed{Value: bogus}); vd != VerdictCorrupt {
		t.Fatalf("a value addressed to another key should be CORRUPT even in-flight, got %s", vd)
	}
	o.RecordPut("k1", pending, 2)
	o.EndOp("k1")
}

func TestDecodeValueRoundTrip(t *testing.T) {
	v := EncodeValue("w3-k17", 42, "payload")
	key, ver, ok := DecodeValue(v)
	if !ok || key != "w3-k17" || ver != 42 {
		t.Fatalf("DecodeValue(%q) = (%q, %d, %v), want (w3-k17, 42, true)", v, key, ver, ok)
	}
	if _, _, ok := DecodeValue("not-a-value"); ok {
		t.Fatal("DecodeValue should reject a malformed value")
	}
}

func TestOracleInFlightScopedToOneKey(t *testing.T) {
	o := NewOracle()
	o.RecordPut("k1", EncodeValue("k1", 1, "p"), 1)
	o.RecordPut("k2", EncodeValue("k2", 1, "p"), 1)

	// An in-flight op on k1 must NOT relax strictness on k2.
	o.BeginOp("k1")
	if vd, _ := o.Verify("k2", Observed{NotFound: true}); vd != VerdictLost {
		t.Fatalf("a loss on k2 must stay LOST even while k1 is in flight, got %s", vd)
	}
	o.EndOp("k1")
}

func TestOracleEverValuesBounded(t *testing.T) {
	o := NewOracle()
	// Overwrite one key far past the cap. everValues must stay bounded.
	total := everValuesCap * 4
	for v := uint64(1); v <= uint64(total); v++ {
		o.RecordPut("k", EncodeValue("k", v, "p"), v)
	}
	o.mu.RLock()
	e := o.model["k"]
	got := len(e.everValues)
	o.mu.RUnlock()
	if got > everValuesCap {
		t.Fatalf("everValues should be capped at %d, grew to %d", everValuesCap, got)
	}

	// The latest value still passes.
	latest := EncodeValue("k", uint64(total), "p")
	if vd, detail := o.Verify("k", Observed{Value: latest}); vd != VerdictPass {
		t.Fatalf("latest value should PASS after eviction, got %s: %s", vd, detail)
	}

	// A RETAINED recent-but-stale value is still STALE (a real violation).
	recentStale := EncodeValue("k", uint64(total-1), "p")
	if vd, _ := o.Verify("k", Observed{Value: recentStale}); vd != VerdictStale {
		t.Fatalf("a retained older acked value should be STALE, got %s", vd)
	}

	// An EVICTED very-old value (version 1, long past the cap) re-surfacing must
	// STILL be a violation - the cap must never turn a real loss into a pass. It
	// is well-formed + addressed to this key at a version below the retained
	// window, so it is flagged STALE rather than CORRUPT, but either way it is not
	// a PASS.
	evictedOld := EncodeValue("k", 1, "p")
	if vd, detail := o.Verify("k", Observed{Value: evictedOld}); vd == VerdictPass {
		t.Fatalf("an evicted very-old value re-surfacing must NOT pass (the cap excused a real loss): %s", detail)
	}
}

func TestOracleSnapshotReflectsLatest(t *testing.T) {
	o := NewOracle()
	o.RecordPut("a", EncodeValue("a", 1, "p"), 1)
	o.RecordPut("b", EncodeValue("b", 1, "p"), 1)
	o.RecordDelete("b", 2)

	snap := o.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot should have 2 keys, got %d", len(snap))
	}
	if snap["a"].NotFound {
		t.Fatal("key a is live; snapshot should not mark it not-found")
	}
	if !snap["b"].NotFound {
		t.Fatal("key b was deleted; snapshot should mark it not-found")
	}
}
