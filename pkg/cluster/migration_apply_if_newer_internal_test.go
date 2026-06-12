package cluster

// Deterministic regression for the P1 fix: the migration receive's
// per-key apply at R>1 routes through apply-if-newer (LWW-on-write),
// NOT a raw Put, so a reconcile stream that arrives out of order
// relative to a live concurrent replicated write loses the per-key
// Stamp compare and is a committed no-op, never a clobber.
//
// This drives the EXACT apply path the production gRPC destination
// uses: clusterDestination.FetchRange (pkg/cluster/rebalance.go) lands
// each streamed (key, value) at R>1 via c.applyEnvelopeIfNewerReport.
// We exercise that method directly rather than racing two goroutines
// through a live membership change, so the ordering is pinned and the
// test cannot flake.
//
// RED evidence: before the fix, FetchRange did d.c.backend.Put(...)
// unconditionally. Substituting a raw be.Put for applyEnvelopeIfNewer
// below (the "raw clobber" sub-case) overwrites the newer stored value
// with the older streamed one - the LWW violation. The apply-if-newer
// sub-case keeps the newer value. We assert both halves so the test
// is self-documenting about what the fix changes.

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
)

// openSingleNodeForApply stands up a minimal R>1 cluster whose local
// backend we can seed + apply against directly. Settle delay is parked
// so no bootstrap Evaluate fires; we never touch the network here.
func openSingleNodeForApply(t *testing.T) (*Cluster, *memory.Memory) {
	t.Helper()
	be := memory.New()
	c, err := Open(Config{
		NodeID:               "apply-local",
		Backend:              be,
		LogOutput:            io.Discard,
		ReplicationFactor:    2,
		WriteConsistency:     WriteOne,
		ReadConsistency:      ReadNearest,
		WriteTimeout:         200 * time.Millisecond,
		ReadTimeout:          200 * time.Millisecond,
		BindAddr:             hp(freeTCPPort(t)),
		GRPCAddr:             "127.0.0.1:1",
		RebalanceSettleDelay: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if !waitForLocalInRing(c, 2*time.Second) {
		t.Fatalf("local member never landed in ring")
	}
	return c, be
}

// TestMigrationApply_DoesNotClobberNewer is the P1 gate. It seeds the
// receiving node's local backend with a NEWER LWW envelope for key K,
// then applies an OLDER envelope for K through the SAME apply the
// migration receive routes through (applyEnvelopeIfNewerReport), and
// asserts K still holds the NEWER value and the apply reported
// applied==false (a committed no-op, so the rollback set stays honest).
func TestMigrationApply_DoesNotClobberNewer(t *testing.T) {
	c, be := openSingleNodeForApply(t)

	key := []byte("K")

	// The live concurrent putReplicated already landed the NEWER value.
	newer := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: 2_000_000, NodeID: "writer"},
		Payload: []byte("newer"),
	})
	if err := be.Put(key, newer); err != nil {
		t.Fatalf("seed newer: %v", err)
	}

	// The in-flight migration stream carries an OLDER value for K
	// (pulled from a writable replica before the newer write existed).
	older := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: 1_000_000, NodeID: "writer"},
		Payload: []byte("older"),
	})

	applied, err := c.applyEnvelopeIfNewerReport(key, older)
	if err != nil {
		t.Fatalf("applyEnvelopeIfNewerReport: %v", err)
	}
	if applied {
		t.Fatalf("older migration value reported applied=true; it must lose the Stamp compare and be a no-op")
	}

	// K must still hold the NEWER value: the raw Put would have
	// clobbered it here.
	got, err := be.Get(key)
	if err != nil {
		t.Fatalf("Get after apply: %v", err)
	}
	if !bytes.Equal(got, newer) {
		env, _ := Decode(got)
		t.Fatalf("K was clobbered by the older migration value: payload=%q want %q (raw-Put LWW violation)",
			env.Payload, "newer")
	}
}

// TestMigrationApply_AppliesNewerOverOlder is the symmetric green case:
// a migration value that is genuinely NEWER than the stored one DOES
// apply (apply-if-newer is not a blanket "never write" - it writes when
// the incoming Stamp wins) and reports applied==true so FetchRange
// records the key in its rollback set.
func TestMigrationApply_AppliesNewerOverOlder(t *testing.T) {
	c, be := openSingleNodeForApply(t)

	key := []byte("K")

	older := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: 1_000_000, NodeID: "writer"},
		Payload: []byte("older"),
	})
	if err := be.Put(key, older); err != nil {
		t.Fatalf("seed older: %v", err)
	}

	newer := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: 2_000_000, NodeID: "writer"},
		Payload: []byte("newer"),
	})

	applied, err := c.applyEnvelopeIfNewerReport(key, newer)
	if err != nil {
		t.Fatalf("applyEnvelopeIfNewerReport: %v", err)
	}
	if !applied {
		t.Fatalf("newer migration value reported applied=false; it must win the Stamp compare and be written")
	}

	got, err := be.Get(key)
	if err != nil {
		t.Fatalf("Get after apply: %v", err)
	}
	if !bytes.Equal(got, newer) {
		t.Fatalf("newer migration value did not land")
	}
}

// TestMigrationApply_RawPutWouldClobber is the RED witness baked into
// the suite: it demonstrates that the PRE-FIX apply (a raw backend.Put,
// what FetchRange did before this change) DOES clobber the newer value.
// This pins exactly what the fix prevents - if someone reverts
// FetchRange back to a raw Put, the contrast with
// TestMigrationApply_DoesNotClobberNewer makes the regression obvious.
func TestMigrationApply_RawPutWouldClobber(t *testing.T) {
	_, be := openSingleNodeForApply(t)

	key := []byte("K")
	newer := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: 2_000_000, NodeID: "writer"},
		Payload: []byte("newer"),
	})
	if err := be.Put(key, newer); err != nil {
		t.Fatalf("seed newer: %v", err)
	}
	older := Encode(Envelope{
		Stamp:   Stamp{TimestampNanos: 1_000_000, NodeID: "writer"},
		Payload: []byte("older"),
	})

	// The pre-fix path: a raw Put with no Stamp compare.
	if err := be.Put(key, older); err != nil {
		t.Fatalf("raw put: %v", err)
	}

	got, err := be.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if bytes.Equal(got, newer) {
		t.Fatalf("raw Put unexpectedly preserved the newer value; this test pins that a raw Put DOES clobber")
	}
	env, _ := Decode(got)
	if !bytes.Equal(env.Payload, []byte("older")) {
		t.Fatalf("raw Put left payload %q, expected the older clobber", env.Payload)
	}
}
