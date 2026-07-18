package cluster

// White-box pins for the stamp-clock Observe RATCHET SITES. The ratchet's
// contract (stampclock.go) is only as strong as its coverage: every place a
// node SEES a stamp must Observe it, or that node can later originate a
// write stamped below a value it has already read/applied and lose the LWW
// compare silently. The CAS validate site is pinned by
// TestCASReplicate_CommitSurvivesFutureStampedReadSet (and its blind-write
// variant); these tests pin the remaining sites, one each, by asserting on
// c.stamps DIRECTLY after driving the site with a future stamp - so
// stripping any single Observe call turns exactly its pin red:
//
//   - the LWW read-winner in the legacy replicated Get (replicate.go)
//   - the LWW read-winner in the multi-backend unit Get (multibackend_replicated.go)
//   - the INCOMING stamp on a replica-receiving apply (apply_if_newer.go)
//   - the STORED stamp the apply-if-newer compare ran against (apply_if_newer.go)
//
// Every future stamp is drawn ~minutes ahead of the wall clock, so a bare
// stamps.Next() (wall-clock based) can NEVER satisfy the assertion on its
// own; only the site's Observe can lift the clock above it.

import (
	"bytes"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// futureStamp returns a stamp d ahead of the wall clock.
func futureStamp(d time.Duration) uint64 {
	return uint64(time.Now().Add(d).UnixNano())
}

// TestGetReplicated_WinnerObserveRatchetsStampClock pins the legacy
// replicated read path's winner Observe (replicate.go, getReplicated): a
// node that READS a future-stamped LWW winner must never again issue a
// stamp at or below it.
func TestGetReplicated_WinnerObserveRatchetsStampClock(t *testing.T) {
	c, be := openSingleNodeForApply(t)

	key := []byte("ratchet-legacy-read-k")
	future := futureStamp(10 * time.Minute)
	planted := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: future, NodeID: "future-node"},
		Payload: []byte("v1"),
	})
	if err := be.Put(key, planted); err != nil {
		t.Fatalf("plant: %v", err)
	}

	got, err := c.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get = %q, want v1", got)
	}

	if next := c.stamps.Next(); next <= future {
		t.Fatalf("stamps.Next() = %d after reading winner stamped %d: the read-winner Observe did not ratchet "+
			"(this node's next originated write would lose LWW to the value it just read)", next, future)
	}
}

// TestGetReplicatedUnit_WinnerObserveRatchetsStampClock pins the
// multi-backend unit read path's winner Observe
// (multibackend_replicated.go, getReplicatedUnitOnce): the multi mirror of
// the legacy pin above.
func TestGetReplicatedUnit_WinnerObserveRatchetsStampClock(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "self", 4, 2, backing, "self")
	c.cfg.ReadTimeout = 3 * time.Second
	c.cfg.RebalanceSettleDelay = time.Hour

	key := []byte("ratchet-unit-read-k")
	ru := storageunit.NewReplicaUnit(c.genUnitForKey(key), 0)
	c.reconcileMu.Lock()
	c.acquireReplicaUnit(ru)
	c.reconcileMu.Unlock()
	c.mountMu.RLock()
	b := c.mountMap[ru]
	c.mountMu.RUnlock()
	if b == nil {
		t.Fatalf("fixture: position %v not mounted", ru)
	}

	future := futureStamp(10 * time.Minute)
	planted := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: future, NodeID: "future-node"},
		Payload: []byte("v1"),
	})
	if err := b.Put(key, planted); err != nil {
		t.Fatalf("plant: %v", err)
	}

	got, err := c.getReplicatedUnit(key)
	if err != nil {
		t.Fatalf("getReplicatedUnit: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("getReplicatedUnit = %q, want v1", got)
	}

	if next := c.stamps.Next(); next <= future {
		t.Fatalf("stamps.Next() = %d after reading winner stamped %d: the unit read-winner Observe did not ratchet", next, future)
	}
}

// TestApplyIfNewer_IncomingObserveRatchetsStampClock pins the
// replica-receiving apply's INCOMING-stamp Observe (apply_if_newer.go,
// applyEnvelopesTx): a replica that APPLIES a future-stamped envelope from
// a faster peer must never again issue a stamp at or below it. The key has
// no stored value, so the stored-stamp Observe below sees the zero stamp
// and cannot satisfy the assertion; only the incoming Observe can.
func TestApplyIfNewer_IncomingObserveRatchetsStampClock(t *testing.T) {
	c, _ := openSingleNodeForApply(t)

	key := []byte("ratchet-apply-incoming-k")
	future := futureStamp(10 * time.Minute)
	incoming := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: future, NodeID: "fast-peer"},
		Payload: []byte("x"),
	})

	applied, err := c.applyEnvelopeIfNewerReport(key, incoming)
	if err != nil {
		t.Fatalf("applyEnvelopeIfNewerReport: %v", err)
	}
	if !applied {
		t.Fatalf("apply of a fresh key reported applied=false")
	}

	if next := c.stamps.Next(); next <= future {
		t.Fatalf("stamps.Next() = %d after applying incoming stamp %d: the incoming-stamp Observe did not ratchet", next, future)
	}
}

// TestApplyIfNewer_StoredObserveRatchetsStampClock pins the
// replica-receiving apply's STORED-stamp Observe (apply_if_newer.go,
// applyEnvelopesTx): the apply-if-newer compare DECODES the stored stamp,
// so the node has seen it and must ratchet even when the apply is a no-op.
// The incoming stamp is far BELOW the stored one (though still ahead of
// the wall clock), so the incoming Observe cannot satisfy the assertion;
// only the stored-stamp Observe can.
func TestApplyIfNewer_StoredObserveRatchetsStampClock(t *testing.T) {
	c, be := openSingleNodeForApply(t)

	key := []byte("ratchet-apply-stored-k")
	futureIncoming := futureStamp(10 * time.Minute)
	futureStored := futureStamp(20 * time.Minute)
	stored := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: futureStored, NodeID: "faster-peer"},
		Payload: []byte("stored"),
	})
	if err := be.Put(key, stored); err != nil {
		t.Fatalf("plant stored: %v", err)
	}
	incoming := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: futureIncoming, NodeID: "fast-peer"},
		Payload: []byte("incoming"),
	})

	applied, err := c.applyEnvelopeIfNewerReport(key, incoming)
	if err != nil {
		t.Fatalf("applyEnvelopeIfNewerReport: %v", err)
	}
	if applied {
		t.Fatalf("older incoming stamp reported applied=true (apply-if-newer regression)")
	}

	if next := c.stamps.Next(); next <= futureStored {
		t.Fatalf("stamps.Next() = %d after a compare against stored stamp %d: the stored-stamp Observe did not ratchet", next, futureStored)
	}
}
