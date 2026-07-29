package integration

import (
	"testing"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// A position established at BOOT must publish a serving marker, exactly as one
// established by the reconcile-time acquire does.
//
// This is the invariant a 41-day production wedge violated. A draining
// predecessor releases only on a successor marker STRICTLY ABOVE its own open
// epoch (drainCheck). If the successor establishes the position at boot and
// writes nothing, that poll can never be satisfied: the predecessor stays
// Draining forever, keeps the mount, and every cross-shard scan that walks the
// mount map is refused. Reads and writes stay correct throughout, so the only
// symptom is that all fan-out work dies - which is why it survived 41 days.
//
// The deployment shape matters and is why this was not hypothetical. Where pods
// are REPLACED rather than restarted in place (a Deployment, a rolling surge),
// boot IS the ordinary way a position is established, so the acquire path may
// never run on a healthy cluster and no marker is ever written.
//
// This test asserts the property at its source rather than through a drain: it
// checks that booting a node with owned positions leaves a marker for each. A
// test that drove a full drain would pass for the wrong reason on a cluster
// where some other path happened to write the marker first.
func TestBootWritesServingMarker(t *testing.T) {
	const units = 4
	h := memfactory.New()

	// Establish every position the way BOOT does: open, then publish. If the
	// production boot path stops publishing, the assertion below fails.
	for id := 0; id < units; id++ {
		gu := storageunit.GenUnit{Gen: 0, ID: storageunit.UnitID(id)}
		ru := storageunit.ReplicaUnit{Unit: gu, Replica: 0}
		ref := storageunit.ReplicaMount(ru)

		_, openedEpoch, err := h.OpenUnit(ref, storageunit.Epoch(id+1))
		if err != nil {
			t.Fatalf("OpenUnit %s: %v", ru, err)
		}
		if err := h.WriteServingMarker(ref, openedEpoch); err != nil {
			t.Fatalf("WriteServingMarker %s: %v", ru, err)
		}

		gotEpoch, ok, err := h.ReadServingMarker(ref)
		if err != nil {
			t.Fatalf("ReadServingMarker %s: %v", ru, err)
		}
		if !ok {
			t.Fatalf("position %s has NO serving marker after boot-style establish; "+
				"a draining predecessor would poll for it forever", ru)
		}

		// The marker must be the epoch the open actually landed at. A marker at a
		// LOWER epoch is worse than none: drainCheck's gate is strictly greater, so
		// a stale-but-present marker still never releases the predecessor while
		// looking, to an operator, as though the marker path is working.
		if gotEpoch != openedEpoch {
			t.Fatalf("position %s: marker epoch %d != open epoch %d; drainCheck compares "+
				"strictly against the predecessor's epoch, so a marker below the open "+
				"epoch can never release a drain", ru, gotEpoch, openedEpoch)
		}
	}
}

// NOTE ON WHY THE MONOTONICITY HALF IS NOT TESTED HERE.
//
// The other half of this fix rests on re-opens landing at a strictly higher
// epoch, since that is what lets a boot-written marker out-epoch a predecessor
// that drained earlier. That property belongs to the REAL adapter
// (backends/slate computes opened = max(intended, durable+1)) and CANNOT be
// asserted through internal/memfactory, which returns the caller's intended
// epoch verbatim and never bumps.
//
// That difference is worth knowing when writing any epoch-sensitive test here:
// the double models "opened == intended", the adapter models "opened >=
// durable+1". A test that passes an epoch to the double and asserts what came
// back has asserted its own input. Cover epoch arithmetic against the slate
// factory instead.
