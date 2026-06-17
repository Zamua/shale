package cluster

// White-box tests for the v0.9 two-generation dual-write router. They exercise
// routedReplicasForReshard (the auth/supp partition + co-location + cut-over
// flip) deterministically, and putReshardDualWrite landing a write in BOTH
// generations against mounted backends. The wired multi-node dual-write under
// concurrent load is the lossless-split oracle (tests/integration, later phase).

import (
	"bytes"
	"context"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

func TestRoutedReplicasForReshard(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")

	if _, ok := c.routedReplicasForReshard([]byte("k")); ok {
		t.Fatalf("no split in flight: want ok=false")
	}

	enterSplit(t, c) // 4 -> 8
	key := []byte("hello-reshard")
	legs, ok := c.routedReplicasForReshard(key)
	if !ok {
		t.Fatalf("split in flight: want ok=true")
	}
	if legs.stableR != 2 || len(legs.auth) != 2 || len(legs.supp) != 2 {
		t.Fatalf("stableR=%d auth=%d supp=%d, want 2/2/2", legs.stableR, len(legs.auth), len(legs.supp))
	}
	h := storageunit.HashShardKey(c.shardKey(key))
	parent := storageunit.UnitForHash(h, storageunit.MustUnitCount(4))
	child := storageunit.UnitForHash(h, storageunit.MustUnitCount(8))

	// Pre-cut-over: auth = parent (gen 0), supp = child (gen 1), co-located by slot.
	for s := 0; s < 2; s++ {
		if legs.auth[s].ru.Unit.Gen != 0 || legs.auth[s].ru.Unit.ID != parent {
			t.Fatalf("auth[%d] = %v, want gen0 unit %d", s, legs.auth[s].ru, parent)
		}
		if legs.supp[s].ru.Unit.Gen != 1 || legs.supp[s].ru.Unit.ID != child {
			t.Fatalf("supp[%d] = %v, want gen1 unit %d", s, legs.supp[s].ru, child)
		}
		if legs.auth[s].member.ID != legs.supp[s].member.ID {
			t.Fatalf("slot %d not co-located: parent on %s, child on %s", s, legs.auth[s].member.ID, legs.supp[s].member.ID)
		}
	}

	// Post-cut-over of the key's parent unit: auth flips to the child, parent
	// becomes supplementary.
	g := c.genSnapshot().clone()
	g.cutOver[parent] = struct{}{}
	c.commitGenState(g)
	legs2, _ := c.routedReplicasForReshard(key)
	for s := 0; s < 2; s++ {
		if legs2.auth[s].ru.Unit.Gen != 1 {
			t.Fatalf("post-cutover auth[%d] = %v, want gen1 child authoritative", s, legs2.auth[s].ru)
		}
		if legs2.supp[s].ru.Unit.Gen != 0 {
			t.Fatalf("post-cutover supp[%d] = %v, want gen0 parent supplementary", s, legs2.supp[s].ru)
		}
	}
}

func TestPutReshardDualWrite_LandsInBothGenerations(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2", "n3")
	enterSplit(t, c)

	key := []byte("dual-write-key")
	h := storageunit.HashShardKey(c.shardKey(key))
	parent := storageunit.UnitForHash(h, storageunit.MustUnitCount(4))
	child := storageunit.UnitForHash(h, storageunit.MustUnitCount(8))
	parentRU := storageunit.NewReplicaUnit(storageunit.NewGenUnit(0, parent), 0)
	childRU := storageunit.NewReplicaUnit(storageunit.NewGenUnit(1, child), 0)

	pb, _, err := c.replicaFactory.OpenReplicaUnit(parentRU, epochAtOpen)
	if err != nil {
		t.Fatalf("open parent: %v", err)
	}
	cb, _, err := c.replicaFactory.OpenReplicaUnit(childRU, epochAtOpen)
	if err != nil {
		t.Fatalf("open child: %v", err)
	}
	c.mountMap[parentRU] = pb
	c.mountMap[childRU] = cb

	// All legs local to self, so each dispatch applies to its mounted backend.
	legs := reshardLegs{
		auth:    []routedReplica{{member: ring.Member{ID: "n1"}, ru: parentRU}},
		supp:    []routedReplica{{member: ring.Member{ID: "n1"}, ru: childRU}},
		stableR: 1,
	}
	env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1, NodeID: "n1"}, Payload: []byte("v1")})
	if att := c.putReshardDualWrite(context.Background(), legs, key, env); att.err != nil {
		t.Fatalf("dual write returned error: %v", att.err)
	}

	for name, b := range map[string]backend.Backend{"parent (authoritative)": pb, "child (supplementary)": cb} {
		got, err := b.Get(key)
		if err != nil {
			t.Fatalf("%s Get: %v", name, err)
		}
		dec, err := Decode(got)
		if err != nil {
			t.Fatalf("%s decode: %v", name, err)
		}
		if !bytes.Equal(dec.Payload, []byte("v1")) {
			t.Fatalf("%s payload = %q, want v1 (dual write did not land)", name, dec.Payload)
		}
	}
}
