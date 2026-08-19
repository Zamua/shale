package cluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// A same-shard-key mismatch and a genuinely-different-shard mismatch are
// OPPOSITE conditions: one clears on retry (the unit layout moved under the
// transaction), the other never will (the caller's keys cannot be atomic).
// They must not share a sentinel, or every caller has to pick a default that
// is wrong for the other case - retry, and a key bug loops forever; fail, and
// a sub-second reshard cut-over becomes a client error.
//
// Both branches are exercised, because a test that only proves the new
// transient case would leave the permanent case free to have become transient
// too, which is the failure this split exists to prevent.
func TestGuardShard_DistinguishesLayoutMoveFromCrossShard(t *testing.T) {
	c, _ := openSingleNodeMulti(t, 8)

	// PERMANENT: two distinct shard keys in different units.
	k1 := []byte("nokey-0")
	var k2 []byte
	for i := 1; i < 10000; i++ {
		cand := fmt.Appendf(nil, "nokey-%d", i)
		if c.genUnitForKey(cand) != c.genUnitForKey(k1) {
			k2 = cand
			break
		}
	}
	if k2 == nil {
		t.Fatal("fixture: no two keys in different units")
	}
	tx := &clusterTx{c: c}
	tx.pinLocked(k1)
	if err := tx.guardShard(k2); !errors.Is(err, backend.ErrCrossShard) {
		t.Fatalf("different shard keys, different units: got %v, want ErrCrossShard (permanent)", err)
	}

	// TRANSIENT: the SAME shard key, with the pinned unit forced stale - the
	// state a reshard cut-over produces between the pin and a later op.
	tagged := []byte("thing/{shared-tag}/a")
	sibling := []byte("thing/{shared-tag}/b") // same tag => same unit, always
	tx2 := &clusterTx{c: c}
	tx2.pinLocked(tagged)
	tx2.pinUnit = storageunit.NewGenUnit(tx2.pinUnit.Gen+1, tx2.pinUnit.ID) // model the layout moving under us
	err := tx2.guardShard(sibling)
	if errors.Is(err, backend.ErrCrossShard) {
		t.Fatalf("same shard key after a layout move: got ErrCrossShard (permanent), want a retryable error")
	}
	if err == nil {
		t.Fatal("same shard key after a layout move: got nil, want a retryable error")
	}
	if !isAcquiringErr(err) {
		t.Fatalf("same shard key after a layout move: got %v, want the transient acquiring-window error", err)
	}
}
