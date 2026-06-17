package cluster

// White-box tests for the v0.9 online split copy + durable markers. They drive
// the copy + marker mechanics directly against mounted backends and the
// in-process MemConditionalStore; the wired multi-node split is the
// lossless-split oracle (tests/integration, later phase).

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

func TestReshardMarkers(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")
	c.cfg.ConditionalStore = storageunit.NewMemConditionalStore()

	if p, _ := c.cutoverMarkerPresent(0, 0); p {
		t.Fatalf("cut-over marker should be absent initially")
	}
	if all, _ := c.allSlotsCaughtUp(0, 0, 2); all {
		t.Fatalf("no slots caught up yet")
	}

	if err := c.publishCaughtupMarker(0, 0, 0); err != nil {
		t.Fatalf("publish slot 0: %v", err)
	}
	if all, _ := c.allSlotsCaughtUp(0, 0, 2); all {
		t.Fatalf("1 of 2 slots caught up: allSlotsCaughtUp should be false")
	}
	// Idempotent re-publish of the same marker.
	if err := c.publishCaughtupMarker(0, 0, 0); err != nil {
		t.Fatalf("re-publish should be idempotent: %v", err)
	}
	if err := c.publishCaughtupMarker(0, 0, 1); err != nil {
		t.Fatalf("publish slot 1: %v", err)
	}
	if all, _ := c.allSlotsCaughtUp(0, 0, 2); !all {
		t.Fatalf("both slots caught up: allSlotsCaughtUp should be true")
	}

	if err := c.publishCutoverMarker(0, 0); err != nil {
		t.Fatalf("publish cut-over: %v", err)
	}
	if p, _ := c.cutoverMarkerPresent(0, 0); !p {
		t.Fatalf("cut-over marker should be present after publish")
	}
}

// keysInUnit returns n keys whose shard hash lands in unit `unit` at `count`.
func keysInUnit(t *testing.T, c *Cluster, unit storageunit.UnitID, count storageunit.UnitCount, n int) [][]byte {
	t.Helper()
	var out [][]byte
	for i := 0; len(out) < n; i++ {
		if i > 200000 {
			t.Fatalf("could not find %d keys in unit %d", n, unit)
		}
		key := []byte(fmt.Sprintf("key-%d", i))
		h := storageunit.HashShardKey(c.shardKey(key))
		if storageunit.UnitForHash(h, count) == unit {
			out = append(out, key)
		}
	}
	return out
}

// mountParentAndChildren opens parent unit K (slot 0) + its two gen-1 children and
// inserts them into the mount map, returning the parent ReplicaUnit.
func mountParentAndChildren(t *testing.T, c *Cluster, k storageunit.UnitID, oldCount storageunit.UnitCount) storageunit.ReplicaUnit {
	t.Helper()
	low, high, err := storageunit.ChildUnits(k, oldCount)
	if err != nil {
		t.Fatal(err)
	}
	parentRU := storageunit.NewReplicaUnit(storageunit.NewGenUnit(0, k), 0)
	rus := []storageunit.ReplicaUnit{
		parentRU,
		storageunit.NewReplicaUnit(storageunit.NewGenUnit(1, low), 0),
		storageunit.NewReplicaUnit(storageunit.NewGenUnit(1, high), 0),
	}
	for _, ru := range rus {
		b, _, err := c.replicaFactory.OpenReplicaUnit(ru, epochAtOpen)
		if err != nil {
			t.Fatalf("open %v: %v", ru, err)
		}
		c.mountMap[ru] = b
	}
	return parentRU
}

func TestCopyParentIntoChildren_RoutesAndLWW(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")
	enterSplit(t, c) // 4 -> 8
	gs := c.genSnapshot()
	parentRU := mountParentAndChildren(t, c, 0, storageunit.MustUnitCount(4))

	keys := keysInUnit(t, c, 0, storageunit.MustUnitCount(4), 8)
	pb := c.mountMap[parentRU]
	for _, k := range keys {
		env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 10, NodeID: "n1"}, Payload: append([]byte("v-"), k...)})
		if err := pb.Put(k, env); err != nil {
			t.Fatal(err)
		}
	}

	applied, err := c.copyParentIntoChildren(parentRU, gs)
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	if applied != len(keys) {
		t.Fatalf("applied %d, want %d", applied, len(keys))
	}

	// Each key landed in the correct child (by gen-1 hash bit), right payload.
	for _, k := range keys {
		h := storageunit.HashShardKey(c.shardKey(k))
		childID := storageunit.UnitForHash(h, storageunit.MustUnitCount(8))
		childRU := storageunit.NewReplicaUnit(storageunit.NewGenUnit(1, childID), 0)
		got, err := c.mountMap[childRU].Get(k)
		if err != nil {
			t.Fatalf("child %v missing key %q: %v", childRU, k, err)
		}
		dec, _ := Decode(got)
		if !bytes.Equal(dec.Payload, append([]byte("v-"), k...)) {
			t.Fatalf("key %q child payload = %q", k, dec.Payload)
		}
	}

	// Re-scan is clean (apply-if-newer no-ops equal stamps).
	if applied2, _ := c.copyParentIntoChildren(parentRU, gs); applied2 != 0 {
		t.Fatalf("re-scan applied %d, want 0 (clean pass)", applied2)
	}

	// LWW: a NEWER value already in the child is NOT clobbered by the stale copy.
	k0 := keys[0]
	h := storageunit.HashShardKey(c.shardKey(k0))
	childRU := storageunit.NewReplicaUnit(storageunit.NewGenUnit(1, storageunit.UnitForHash(h, storageunit.MustUnitCount(8))), 0)
	newer := Encode(Envelope{Stamp: Stamp{TimestampNanos: 99, NodeID: "n1"}, Payload: []byte("newer")})
	if _, err := c.applyEnvelopeIfNewerToBackendReport(c.mountMap[childRU], childRU, k0, newer); err != nil {
		t.Fatal(err)
	}
	if applied3, _ := c.copyParentIntoChildren(parentRU, gs); applied3 != 0 {
		t.Fatalf("stale copy applied %d, want 0 (LWW must protect the newer child value)", applied3)
	}
	got, _ := c.mountMap[childRU].Get(k0)
	dec, _ := Decode(got)
	if !bytes.Equal(dec.Payload, []byte("newer")) {
		t.Fatalf("LWW violated: child payload = %q, want newer", dec.Payload)
	}
}

func TestCopyParentUntilCaughtUp_Clean(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")
	enterSplit(t, c)
	gs := c.genSnapshot()
	parentRU := mountParentAndChildren(t, c, 0, storageunit.MustUnitCount(4))

	pb := c.mountMap[parentRU]
	for _, k := range keysInUnit(t, c, 0, storageunit.MustUnitCount(4), 6) {
		env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 5, NodeID: "n1"}, Payload: []byte("x")})
		if err := pb.Put(k, env); err != nil {
			t.Fatal(err)
		}
	}
	clean, err := c.copyParentUntilCaughtUp(parentRU, gs)
	if err != nil {
		t.Fatalf("caught-up: %v", err)
	}
	if !clean {
		t.Fatalf("expected a clean caught-up pass after a static copy")
	}
}
