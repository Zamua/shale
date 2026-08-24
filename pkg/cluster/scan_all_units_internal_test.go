package cluster

import (
	"fmt"
	"testing"

	"github.com/Zamua/shale/pkg/storageunit"
)

// The property: a prefix that names no shard token still enumerates the WHOLE
// keyspace.
//
// This is the bug it exists to prevent, and the reason the assertion compares
// against ScanPrefix rather than just counting. "pastes/" routes to one unit, so
// the ordinary scan returns that unit's share and reports no error - a caller
// enumerating a keyspace gets a silent subset. Asserting only "AllUnits found
// everything" would still pass if routing changed to make the plain scan global
// too, so the test also pins that the two DISAGREE, which is what makes the new
// call necessary rather than redundant.
func TestScanPrefixAllUnitsSeesEveryUnit(t *testing.T) {
	c := openStaticNode(t, "n1", func(cfg *Config) {
		cfg.UnitCount = storageunit.MustUnitCount(8)
	})

	const prefix = "rec/"
	want := map[string]bool{}
	for i := range 60 {
		k := fmt.Sprintf("%s%03d", prefix, i)
		if err := c.Put([]byte(k), []byte("v")); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
		want[k] = true
	}

	got := map[string]bool{}
	if err := c.ScanPrefixAllUnits([]byte(prefix), func(k, _ []byte) error {
		got[string(k)] = true
		return nil
	}); err != nil {
		t.Fatalf("ScanPrefixAllUnits: %v", err)
	}
	for k := range want {
		if !got[k] {
			t.Fatalf("ScanPrefixAllUnits missed %s: it found %d of %d keys, which is the "+
				"silent-subset failure this call exists to remove", k, len(got), len(want))
		}
	}

	// The routed scan sees ONE unit's share. If it ever sees everything, the
	// justification for ScanPrefixAllUnits has gone and this test should be the
	// thing that says so.
	routed := 0
	it, err := c.ScanPrefix([]byte(prefix))
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}
	for {
		k, _, err := it.Next()
		if err != nil {
			t.Fatalf("routed scan: %v", err)
		}
		if k == nil {
			break
		}
		routed++
	}
	_ = it.Close()
	if routed >= len(want) {
		t.Fatalf("the routed scan returned %d of %d keys: it is no longer shard-bound, so "+
			"ScanPrefixAllUnits may be redundant and this test should be revisited", routed, len(want))
	}
}
