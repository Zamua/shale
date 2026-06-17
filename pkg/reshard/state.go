// Package reshard is the bounded context for shale's DECLARATIVE, DECENTRALIZED
// resharding agreement: the cluster changes its unit (shard) count online by
// agreeing on a monotonic reshard EPOCH stored as a single durable object that
// every node reads and that advances by a conditional-write race - no elected
// coordinator (see docs/decentralized-reshard-design.md).
//
// This package owns the agreement layer only (the epoch value object + the
// Arbiter that reads/seeds/advances it via a storageunit.ConditionalStore). The
// per-unit routing, copy, and cut-over machinery lives in pkg/cluster; keeping
// the agreement here, depending only on storageunit, keeps it small and
// testable in isolation against the in-process ConditionalStore double.
package reshard

import (
	"encoding/json"
	"fmt"

	"github.com/Zamua/shale/pkg/storageunit"
)

// Plan is the transition that produced a generation: a split (the count
// doubled), a merge (halved), or none (the seed generation).
type Plan uint8

const (
	// PlanNone is the seed generation: no transition produced it.
	PlanNone Plan = iota
	// PlanSplit doubled the unit count (N -> 2N) to grow.
	PlanSplit
	// PlanMerge halved the unit count (2N -> N) to shrink.
	PlanMerge
)

func (p Plan) String() string {
	switch p {
	case PlanNone:
		return "none"
	case PlanSplit:
		return "split"
	case PlanMerge:
		return "merge"
	default:
		return fmt.Sprintf("plan(%d)", uint8(p))
	}
}

// State is the cluster-agreed reshard generation: a monotonic Epoch, the live
// unit Count at that epoch, and the Plan that produced it from the previous
// epoch. It is stored as ONE durable object that every node reads; advancing it
// is a conditional-write race (Arbiter.Advance) that exactly one writer wins.
//
// It is a pure value object with a stable, operator-inspectable JSON encoding,
// so every node decodes the same bytes to the same state.
type State struct {
	Epoch uint64
	Count storageunit.UnitCount
	Plan  Plan
}

type stateWire struct {
	Epoch uint64 `json:"epoch"`
	Count int    `json:"count"`
	Plan  string `json:"plan"`
}

// Encode serializes the state to stable JSON.
func (s State) Encode() []byte {
	b, _ := json.Marshal(stateWire{
		Epoch: s.Epoch,
		Count: int(s.Count.N()),
		Plan:  s.Plan.String(),
	})
	return b
}

// DecodeState parses bytes produced by Encode, validating the count is a legal
// power-of-two UnitCount and the plan is known.
func DecodeState(b []byte) (State, error) {
	var w stateWire
	if err := json.Unmarshal(b, &w); err != nil {
		return State{}, fmt.Errorf("reshard: decode state: %w", err)
	}
	c, err := storageunit.NewUnitCount(w.Count)
	if err != nil {
		return State{}, fmt.Errorf("reshard: decode state count: %w", err)
	}
	var p Plan
	switch w.Plan {
	case "none":
		p = PlanNone
	case "split":
		p = PlanSplit
	case "merge":
		p = PlanMerge
	default:
		return State{}, fmt.Errorf("reshard: decode state: unknown plan %q", w.Plan)
	}
	return State{Epoch: w.Epoch, Count: c, Plan: p}, nil
}
