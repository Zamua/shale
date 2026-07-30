package memfactory

import (
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

// TestOpenUnit_DerivesMonotonicEpoch pins the double's fidelity to the real
// backing store's open semantics: opened = max(intended, durable+1). The
// verbatim behavior this replaces let a test's epoch assertion assert its own
// input, and let a stale writer re-open BELOW the historical fence after a
// close - a state the real store makes impossible. If this test starts
// failing because a caller "needs" the verbatim epoch back, that caller is
// depending on exactly the infidelity that hid the serving-marker wedge.
func TestOpenUnit_DerivesMonotonicEpoch(t *testing.T) {
	f := New()
	m := storageunit.SoleMount(storageunit.NewGenUnit(0, 3))

	// Fresh mount: intended stands (nothing durable to raise it past).
	_, opened, err := f.OpenUnit(m, 5)
	if err != nil || opened != 5 {
		t.Fatalf("fresh open: opened=%d err=%v, want 5", opened, err)
	}
	if err := f.CloseUnit(m); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Re-open BELOW the fence: the store raises past the durable floor, as
	// slate does - never honors the stale intent.
	_, opened, err = f.OpenUnit(m, 2)
	if err != nil || opened != 6 {
		t.Fatalf("below-fence reopen: opened=%d err=%v, want 6 (fence 5 + 1)", opened, err)
	}

	// Double-open at or below the CURRENT holder refuses even after
	// derivation (7 > 6 would fence; 3 derives to 7 which... intended 3
	// derives to fence 6 + 1 = 7 > current 6, so it FENCES the holder - the
	// derived epoch can never collide with the current one from below).
	_, opened2, err := f.OpenUnit(m, 3)
	if err != nil || opened2 != 7 {
		t.Fatalf("fencing reopen: opened=%d err=%v, want 7", opened2, err)
	}

	// Above-fence intent stands verbatim.
	if err := f.CloseUnit(m); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, opened, err = f.OpenUnit(m, 100)
	if err != nil || opened != 100 {
		t.Fatalf("above-fence open: opened=%d err=%v, want 100", opened, err)
	}
}
