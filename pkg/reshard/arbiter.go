package reshard

import (
	"errors"
	"fmt"

	"github.com/Zamua/shale/pkg/storageunit"
)

// EpochKey is the ConditionalStore key holding the cluster's agreed reshard
// State: a single object, read by every node, advanced by a conditional-write
// race.
const EpochKey = "epoch"

// retargetAttempts bounds how many times Retarget re-reads and retries the CAS
// when it loses a race to a concurrent Advance/Retarget. A handful is plenty:
// the contention is one operator-driven retarget against the reconcile cadence.
const retargetAttempts = 8

// Arbiter is the decentralized reshard agreement. It seeds, reads, retargets,
// and advances the cluster's reshard State on a storageunit.ConditionalStore
// using create-if-absent + compare-and-set, so determinism plus a CAS race
// replace an elected coordinator. It holds no cluster state; construct one per
// node.
type Arbiter struct {
	store storageunit.ConditionalStore
	key   string
}

// NewArbiter returns an Arbiter over store.
func NewArbiter(store storageunit.ConditionalStore) *Arbiter {
	return &Arbiter{store: store, key: EpochKey}
}

// Seed establishes epoch 0 at initial (Count == Target == initial) if no State
// exists yet. It is idempotent across racing nodes: exactly one PutIfAbsent
// wins, and the rest observe and adopt the seeded State. Seed MUST happen before
// any Advance/Retarget (Advance on an unseeded store returns
// storageunit.ErrCondNotFound, which the caller should treat as "retry after
// Seed").
func (a *Arbiter) Seed(initial storageunit.UnitCount) (State, error) {
	st := State{Epoch: 0, Count: initial, Target: initial, Plan: PlanNone}
	_, err := a.store.PutIfAbsent(a.key, st.Encode())
	if errors.Is(err, storageunit.ErrPrecondition) {
		got, _, rerr := a.Read() // someone else seeded; adopt it
		return got, rerr
	}
	if err != nil {
		return State{}, fmt.Errorf("reshard: seed: %w", err)
	}
	return st, nil
}

// Read returns the current agreed State and its version token (for a subsequent
// CompareAndSet). It surfaces storageunit.ErrCondNotFound if no State has been
// seeded yet.
func (a *Arbiter) Read() (State, string, error) {
	data, ver, err := a.store.Get(a.key)
	if err != nil {
		return State{}, "", err
	}
	st, err := DecodeState(data)
	if err != nil {
		return State{}, "", err
	}
	return st, ver, nil
}

// Retarget sets the cluster-agreed final Target (the declared shard count) via a
// compare-and-set, retrying a bounded number of times if it loses the race to a
// concurrent Advance or Retarget. This is the DECLARATIVE "I want N shards"
// operation - it changes only the Target, never Count/Epoch, so the reconcile
// then converges Count toward the new Target. Because the Target lives in the
// single durable State, every node plans toward the same value and no node can
// reverse another's reshard (the flap that a per-node desired would cause).
func (a *Arbiter) Retarget(target storageunit.UnitCount) (State, error) {
	var last State
	for range retargetAttempts {
		cur, ver, err := a.Read()
		if err != nil {
			return State{}, err
		}
		last = cur
		if cur.Target.N() == target.N() {
			return cur, nil // already the agreed target
		}
		next := State{Epoch: cur.Epoch, Count: cur.Count, Target: target, Plan: cur.Plan}
		if _, err := a.store.CompareAndSet(a.key, next.Encode(), ver); err != nil {
			if errors.Is(err, storageunit.ErrPrecondition) {
				continue // lost the race; re-read and retry
			}
			return State{}, fmt.Errorf("reshard: retarget: %w", err)
		}
		return next, nil
	}
	return last, fmt.Errorf("reshard: retarget: exhausted %d attempts under contention", retargetAttempts)
}

// Advance moves the cluster ONE generation toward the stored Target: split
// (double) if Target exceeds the live Count, merge (halve) if it is smaller,
// no-op if equal. It CAS-advances from the version it read; if it loses the race
// (another node advanced first) it returns that winner's State so the caller can
// re-evaluate and advance again if still not at Target. The bool reports whether
// THIS call performed the advance.
//
// CONTRACT for the caller (the Phase C reconcile): advance ONE generation at a
// time, and only call Advance again AFTER the current generation's reshard is
// fully complete (every unit cut over). A node must not skip a generation: if
// the epoch jumps g -> g+2 while a slow node was not looking, that node still
// owes generation g+1's split work. The arbiter cannot know cluster-wide
// completion, so the reconcile gates WHEN to advance; the arbiter only performs
// one agreed step.
//
// Reaching an arbitrary power-of-two Target is repeated Advance: e.g. 2 -> 16 is
// three split steps, each its own agreed epoch.
func (a *Arbiter) Advance() (State, bool, error) {
	cur, ver, err := a.Read()
	if err != nil {
		return State{}, false, err
	}
	next, step, err := planNext(cur)
	if err != nil {
		return State{}, false, err
	}
	if !step {
		return cur, false, nil // already at Target
	}
	if _, err := a.store.CompareAndSet(a.key, next.Encode(), ver); err != nil {
		if errors.Is(err, storageunit.ErrPrecondition) {
			winner, _, rerr := a.Read() // lost the race; adopt the winner
			if rerr != nil {
				return State{}, false, rerr
			}
			return winner, false, nil
		}
		return State{}, false, fmt.Errorf("reshard: advance: %w", err)
	}
	return next, true, nil
}

// planNext computes the single next generation from cur toward cur.Target. step
// is false when Count already equals Target. It is a pure function of cur: every
// node derives the identical next generation (the target is read from the shared
// State, never a per-node value), so the CAS race needs no tiebreak - the step
// they race to write is byte-identical.
func planNext(cur State) (next State, step bool, err error) {
	switch {
	case cur.Target.N() == cur.Count.N():
		return State{}, false, nil
	case cur.Target.N() > cur.Count.N():
		nc, derr := cur.Count.Double()
		if derr != nil {
			return State{}, false, derr
		}
		return State{Epoch: cur.Epoch + 1, Count: nc, Target: cur.Target, Plan: PlanSplit}, true, nil
	default:
		nc, herr := cur.Count.Halve()
		if herr != nil {
			return State{}, false, herr
		}
		return State{Epoch: cur.Epoch + 1, Count: nc, Target: cur.Target, Plan: PlanMerge}, true, nil
	}
}
