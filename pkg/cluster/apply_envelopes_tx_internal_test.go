package cluster

// Characterization pin for the single-sourced apply-if-newer transaction
// body (applyEnvelopesTx): all five apply sites - the legacy single-key
// report, the legacy CAS batch tail, the mounted-unit single-key apply,
// the explicit-backend batch apply, and the explicit-backend single-key
// report - make the IDENTICAL apply/no-op decision for an EQUAL-stamp
// incoming envelope: a committed no-op that leaves the stored value
// intact (Stamp.Greater is strict, so an equal stamp never applies).
// Before the body was single-sourced this held only by five-way copy
// discipline; this test keeps any future divergence loud.

import (
	"bytes"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/storageunit"
)

func TestApplySites_EqualStampIsCommittedNoOpAtAllFiveSites(t *testing.T) {
	stamp := Stamp{TimestampNanos: 5_000_000, NodeID: "w"}
	stored := Encode(Envelope{Stamp: stamp, Payload: []byte("stored")})
	incoming := Encode(Envelope{Stamp: stamp, Payload: []byte("incoming")})
	key := []byte("K")

	// Sites 1 + 2 run against the legacy single-backend cluster (c.backend).
	c, be := openSingleNodeForApply(t)
	if err := be.Put(key, stored); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}

	// Site 1: applyEnvelopeIfNewerReport (migration receive / legacy
	// replica put).
	applied, err := c.applyEnvelopeIfNewerReport(key, incoming)
	if err != nil {
		t.Fatalf("applyEnvelopeIfNewerReport: %v", err)
	}
	if applied {
		t.Fatalf("equal-stamp envelope reported applied=true at applyEnvelopeIfNewerReport")
	}
	assertStoredIntact(t, be, key, stored, "applyEnvelopeIfNewerReport")

	// Site 2: ApplyBatchLocal's legacy tail (CAS write-set fan-out at
	// legacy R>1).
	if err := c.ApplyBatchLocal([]EnvelopeWrite{{Key: key, Envelope: incoming}}); err != nil {
		t.Fatalf("ApplyBatchLocal: %v", err)
	}
	assertStoredIntact(t, be, key, stored, "ApplyBatchLocal")

	// Sites 3 + 4 + 5 run against a multi-backend replicated cluster.
	backing := sharedfactory.NewBacking()
	mc := newReplicatedCluster(t, "self", 4, 2, backing, "self", "n2", "n3")

	// Site 3: applyEnvelopeIfNewerToUnit resolves the key's MOUNTED unit;
	// mount a seeded backend at position 0 of the key's unit so the
	// resolve (indexed, or the lowest-mounted fallback) lands on it.
	unitBE := memory.New()
	if err := unitBE.Put(key, stored); err != nil {
		t.Fatalf("seed unit: %v", err)
	}
	gu := mc.genSnapshot().resolveGenUnit(storageunit.HashShardKey(mc.shardKey(key)))
	target := storageunit.NewReplicaUnit(gu, 0)
	mc.mountMu.Lock()
	mc.mountMap[target] = unitBE
	mc.mountMu.Unlock()
	if err := mc.applyEnvelopeIfNewerToUnit(key, incoming); err != nil {
		t.Fatalf("applyEnvelopeIfNewerToUnit: %v", err)
	}
	assertStoredIntact(t, unitBE, key, stored, "applyEnvelopeIfNewerToUnit")

	// Site 4: applyBatchToBackend against an explicit backend.
	batchBE := memory.New()
	if err := batchBE.Put(key, stored); err != nil {
		t.Fatalf("seed batch: %v", err)
	}
	if err := mc.applyBatchToBackend(batchBE, target, []EnvelopeWrite{{Key: key, Envelope: incoming}}); err != nil {
		t.Fatalf("applyBatchToBackend: %v", err)
	}
	assertStoredIntact(t, batchBE, key, stored, "applyBatchToBackend")

	// Site 5: applyEnvelopeIfNewerToBackendReport against an explicit
	// backend.
	reportBE := memory.New()
	if err := reportBE.Put(key, stored); err != nil {
		t.Fatalf("seed report: %v", err)
	}
	applied, err = mc.applyEnvelopeIfNewerToBackendReport(reportBE, target, key, incoming)
	if err != nil {
		t.Fatalf("applyEnvelopeIfNewerToBackendReport: %v", err)
	}
	if applied {
		t.Fatalf("equal-stamp envelope reported applied=true at applyEnvelopeIfNewerToBackendReport")
	}
	assertStoredIntact(t, reportBE, key, stored, "applyEnvelopeIfNewerToBackendReport")
}

// assertStoredIntact fails the test if key no longer holds the exact
// stored envelope bytes (i.e. an equal-stamp incoming envelope clobbered
// it through the named apply site).
func assertStoredIntact(t *testing.T, be backend.Backend, key, want []byte, site string) {
	t.Helper()
	got, err := be.Get(key)
	if err != nil {
		t.Fatalf("%s: Get after apply: %v", site, err)
	}
	if !bytes.Equal(got, want) {
		env, _ := Decode(got)
		t.Fatalf("%s: equal-stamp envelope clobbered the stored value (payload=%q, want %q)", site, env.Payload, "stored")
	}
}
