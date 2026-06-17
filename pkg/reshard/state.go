// Package reshard is the bounded context for shale's DECLARATIVE, DECENTRALIZED
// resharding agreement: the cluster changes its unit (shard) count online by
// agreeing on a monotonic reshard EPOCH stored as a single durable object that
// every node reads and that advances by a conditional-write race - no elected
// coordinator (see docs/decentralized-reshard-design.md).
//
// This package owns the agreement layer only (the epoch value object + the
// Arbiter that reads/seeds/retargets/advances it via a storageunit.
// ConditionalStore). The per-unit routing, copy, and cut-over machinery lives in
// pkg/cluster; keeping the agreement here, depending only on storageunit, keeps
// it small and testable in isolation against the in-process ConditionalStore
// double.
package reshard

import (
	"encoding/json"
	"fmt"

	"github.com/Zamua/shale/pkg/storageunit"
)

// stateSchemaVersion is the wire-format version of the durable epoch object. A
// node that reads a State at an UNKNOWN (higher) version fails closed rather
// than silently dropping a field it does not understand - the epoch object is
// THE cross-node authority, so a silently-misinterpreted field would be a silent
// divergence. Bump this only with a forward/backward-compatibility plan.
const stateSchemaVersion = 1

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

// State is the cluster-agreed reshard generation, stored as ONE durable object
// that every node reads. Advancing it is a conditional-write race (Arbiter.
// Advance) that exactly one writer wins.
//
//   - Epoch is a monotonic generation counter.
//   - Count is the LIVE unit count at this epoch.
//   - Target is the cluster-agreed FINAL count to converge to. It lives in the
//     durable State (not a per-node parameter) on purpose: if each node carried
//     its own desired count, a rolling config change would let one node split
//     while another merges it back, forever. With ONE agreed Target, every
//     node's Advance plans the same direction. Changing the Target is the
//     explicit, serialized Retarget operation.
//   - Plan is the transition that produced Count from the previous epoch. The
//     previous count is derivable from Plan (split => Count/2, merge => Count*2),
//     so it is not stored separately.
//
// It is a pure value object with a stable, versioned, operator-inspectable JSON
// encoding, so every node decodes the same bytes to the same state.
type State struct {
	Epoch  uint64
	Count  storageunit.UnitCount
	Target storageunit.UnitCount
	Plan   Plan
}

type stateWire struct {
	V      int    `json:"v"`
	Epoch  uint64 `json:"epoch"`
	Count  int    `json:"count"`
	Target int    `json:"target"`
	Plan   string `json:"plan"`
}

// Encode serializes the state to stable, versioned JSON.
func (s State) Encode() []byte {
	b, _ := json.Marshal(stateWire{
		V:      stateSchemaVersion,
		Epoch:  s.Epoch,
		Count:  int(s.Count.N()),
		Target: int(s.Target.N()),
		Plan:   s.Plan.String(),
	})
	return b
}

// DecodeState parses bytes produced by Encode. It fails closed on an unknown
// schema version (a newer writer), an illegal count/target (not a power of two),
// or an unknown plan.
func DecodeState(b []byte) (State, error) {
	var w stateWire
	if err := json.Unmarshal(b, &w); err != nil {
		return State{}, fmt.Errorf("reshard: decode state: %w", err)
	}
	if w.V != stateSchemaVersion {
		return State{}, fmt.Errorf("reshard: decode state: unsupported schema version %d (this node speaks %d)", w.V, stateSchemaVersion)
	}
	count, err := storageunit.NewUnitCount(w.Count)
	if err != nil {
		return State{}, fmt.Errorf("reshard: decode state count: %w", err)
	}
	target, err := storageunit.NewUnitCount(w.Target)
	if err != nil {
		return State{}, fmt.Errorf("reshard: decode state target: %w", err)
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
	return State{Epoch: w.Epoch, Count: count, Target: target, Plan: p}, nil
}
