package reshard_test

import (
	"testing"

	"github.com/Zamua/shale/pkg/reshard"
	"github.com/Zamua/shale/pkg/storageunit"
)

func TestState_EncodeDecodeRoundTrip(t *testing.T) {
	cases := []reshard.State{
		{Epoch: 0, Count: storageunit.MustUnitCount(1), Target: storageunit.MustUnitCount(1), Plan: reshard.PlanNone},
		{Epoch: 1, Count: storageunit.MustUnitCount(2), Target: storageunit.MustUnitCount(8), Plan: reshard.PlanSplit},
		{Epoch: 7, Count: storageunit.MustUnitCount(256), Target: storageunit.MustUnitCount(256), Plan: reshard.PlanSplit},
		{Epoch: 9, Count: storageunit.MustUnitCount(4), Target: storageunit.MustUnitCount(2), Plan: reshard.PlanMerge},
	}
	for _, want := range cases {
		got, err := reshard.DecodeState(want.Encode())
		if err != nil {
			t.Fatalf("DecodeState(%+v): %v", want, err)
		}
		if got.Epoch != want.Epoch || got.Count.N() != want.Count.N() ||
			got.Target.N() != want.Target.N() || got.Plan != want.Plan {
			t.Fatalf("round-trip = %+v, want %+v", got, want)
		}
	}
}

func TestDecodeState_Rejects(t *testing.T) {
	for name, b := range map[string][]byte{
		"bad json":            []byte("not json"),
		"non-pow2 count":      []byte(`{"v":1,"epoch":1,"count":3,"target":4,"plan":"split"}`),
		"non-pow2 target":     []byte(`{"v":1,"epoch":1,"count":2,"target":5,"plan":"split"}`),
		"unknown plan":        []byte(`{"v":1,"epoch":1,"count":2,"target":2,"plan":"frobnicate"}`),
		"unknown schema vers": []byte(`{"v":99,"epoch":1,"count":2,"target":2,"plan":"split"}`),
	} {
		if _, err := reshard.DecodeState(b); err == nil {
			t.Fatalf("DecodeState(%s) should error (fail closed)", name)
		}
	}
}

func TestPlan_String(t *testing.T) {
	want := map[reshard.Plan]string{
		reshard.PlanNone:  "none",
		reshard.PlanSplit: "split",
		reshard.PlanMerge: "merge",
	}
	for p, w := range want {
		if got := p.String(); got != w {
			t.Fatalf("Plan(%d).String() = %q, want %q", p, got, w)
		}
	}
}
