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

// Arbiter is the decentralized reshard agreement. It seeds, reads, and advances
// the cluster's reshard State on a storageunit.ConditionalStore using
// create-if-absent + compare-and-set, so determinism plus a CAS race replace an
// elected coordinator. It holds no cluster state; construct one per node.
type Arbiter struct {
	store storageunit.ConditionalStore
	key   string
}

// NewArbiter returns an Arbiter over store.
func NewArbiter(store storageunit.ConditionalStore) *Arbiter {
	return &Arbiter{store: store, key: EpochKey}
}

// Seed establishes epoch 0 at initial if no State exists yet. It is idempotent
// across racing nodes: exactly one PutIfAbsent wins, and the rest observe and
// adopt the seeded State. Returns the agreed seed State.
func (a *Arbiter) Seed(initial storageunit.UnitCount) (State, error) {
	st := State{Epoch: 0, Count: initial, Plan: PlanNone}
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

// Advance moves the cluster ONE generation toward desired: split (double) if
// desired exceeds the live count, merge (halve) if it is smaller, no-op if
// equal. It CAS-advances from the version it read; if it loses the race
// (another node advanced first) it returns that winner's State so the caller can
// re-evaluate against the new live count and advance again if still not at
// desired. The bool reports whether THIS call performed the advance.
//
// Reaching an arbitrary power-of-two target is repeated Advance: e.g. 2 -> 16 is
// three split steps, each its own agreed epoch.
func (a *Arbiter) Advance(desired storageunit.UnitCount) (State, bool, error) {
	cur, ver, err := a.Read()
	if err != nil {
		return State{}, false, err
	}
	next, step, err := planNext(cur, desired)
	if err != nil {
		return State{}, false, err
	}
	if !step {
		return cur, false, nil // already at desired
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

// planNext computes the single next generation toward desired from cur. step is
// false when cur is already at desired. It is a pure function of (cur, desired):
// every node derives the identical next generation, which is what lets the CAS
// race need no tiebreak for same-direction proposals (the step they race to
// write is byte-identical).
func planNext(cur State, desired storageunit.UnitCount) (next State, step bool, err error) {
	switch {
	case desired.N() == cur.Count.N():
		return State{}, false, nil
	case desired.N() > cur.Count.N():
		nc, derr := cur.Count.Double()
		if derr != nil {
			return State{}, false, derr
		}
		return State{Epoch: cur.Epoch + 1, Count: nc, Plan: PlanSplit}, true, nil
	default:
		nc, herr := cur.Count.Halve()
		if herr != nil {
			return State{}, false, herr
		}
		return State{Epoch: cur.Epoch + 1, Count: nc, Plan: PlanMerge}, true, nil
	}
}
