package placement_test

import (
	"bytes"
	"testing"

	"github.com/Zamua/shale/internal/placement"
	"github.com/Zamua/shale/pkg/storageunit"
)

// GOLDEN VECTORS. This encoding is the ring's input for every coordinator
// adapter, so a change here silently relocates every unit in every existing
// cluster: the bytes are frozen, not an implementation detail. Byte-literal
// expectations (not a re-implementation of the encoder) are the point - a
// test that recomputed the value would agree with any drift.
func TestGenUnitBytes_StableEncoding(t *testing.T) {
	cases := []struct {
		gu   storageunit.GenUnit
		want []byte
	}{
		{storageunit.NewGenUnit(0, 0), []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{storageunit.NewGenUnit(0, 1), []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}},
		{storageunit.NewGenUnit(1, 1), []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1}},
		{storageunit.NewGenUnit(0, 256), []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0}},
		{storageunit.NewGenUnit(0, 0xDEADBEEF), []byte{0, 0, 0, 0, 0, 0, 0, 0, 0xDE, 0xAD, 0xBE, 0xEF}},
	}
	for _, tc := range cases {
		if got := placement.GenUnitBytes(tc.gu); !bytes.Equal(got, tc.want) {
			t.Fatalf("GenUnitBytes(%s) = %v, want %v", tc.gu, got, tc.want)
		}
	}
	if bytes.Equal(placement.GenUnitBytes(storageunit.NewGenUnit(0, 5)), placement.GenUnitBytes(storageunit.NewGenUnit(1, 5))) {
		t.Fatal("gen-0 and gen-1 of unit 5 encoded identically; generations would collide on the ring")
	}
}
