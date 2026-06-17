package reshard_test

import (
	"testing"

	"github.com/Zamua/shale/pkg/reshard"
	"github.com/Zamua/shale/pkg/storageunit"
)

func TestState_EncodeDecodeRoundTrip(t *testing.T) {
	cases := []reshard.State{
		{Epoch: 0, Count: storageunit.MustUnitCount(1), Plan: reshard.PlanNone},
		{Epoch: 1, Count: storageunit.MustUnitCount(2), Plan: reshard.PlanSplit},
		{Epoch: 7, Count: storageunit.MustUnitCount(256), Plan: reshard.PlanSplit},
		{Epoch: 9, Count: storageunit.MustUnitCount(4), Plan: reshard.PlanMerge},
	}
	for _, want := range cases {
		got, err := reshard.DecodeState(want.Encode())
		if err != nil {
			t.Fatalf("DecodeState(%+v): %v", want, err)
		}
		if got.Epoch != want.Epoch || got.Count.N() != want.Count.N() || got.Plan != want.Plan {
			t.Fatalf("round-trip = %+v, want %+v", got, want)
		}
	}
}

func TestDecodeState_Rejects(t *testing.T) {
	if _, err := reshard.DecodeState([]byte("not json")); err == nil {
		t.Fatal("DecodeState(bad json) should error")
	}
	if _, err := reshard.DecodeState([]byte(`{"epoch":1,"count":3,"plan":"split"}`)); err == nil {
		t.Fatal("DecodeState(non-power-of-two count) should error")
	}
	if _, err := reshard.DecodeState([]byte(`{"epoch":1,"count":2,"plan":"frobnicate"}`)); err == nil {
		t.Fatal("DecodeState(unknown plan) should error")
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
