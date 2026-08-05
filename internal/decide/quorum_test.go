package decide

import "testing"

// TestCurrentReplicaSet enumerates the quorum floor over the placement shapes a
// join transition produces, including the MASS BOOT (every holder of a unit
// joining at once) that the floor exists for.
func TestCurrentReplicaSet(t *testing.T) {
	cases := []struct {
		name           string
		fullRing       int
		joinerExcluded int
		r              int
		want           CurrentSetChoice
	}{
		{
			name: "steady state R=2: no joiner drops out, exclusion is CURRENT",
			// The transparency path: the newcomer leaves CURRENT and the
			// displaced owner stays the stable holder.
			fullRing: 2, joinerExcluded: 2, r: 2, want: CurrentJoinerExcluded,
		},
		{
			name:     "steady state R=3",
			fullRing: 3, joinerExcluded: 3, r: 3, want: CurrentJoinerExcluded,
		},
		{
			name:     "steady state R=1",
			fullRing: 1, joinerExcluded: 1, r: 1, want: CurrentJoinerExcluded,
		},
		{
			name: "MASS BOOT R=2: both holders joining, exclusion is empty",
			// Unfloored this is stableR=0 -> requiredWriteAcks(WriteAll, 0)=0:
			// a write acking with zero durable applies.
			fullRing: 2, joinerExcluded: 0, r: 2, want: CurrentFullRing,
		},
		{
			name:     "MASS BOOT R=1: the sole holder is joining",
			fullRing: 1, joinerExcluded: 0, r: 1, want: CurrentFullRing,
		},
		{
			name:     "partial mass boot R=2: exclusion leaves one holder, below R",
			fullRing: 2, joinerExcluded: 1, r: 2, want: CurrentFullRing,
		},
		{
			name:     "partial mass boot R=3: exclusion leaves two holders, below R",
			fullRing: 3, joinerExcluded: 2, r: 3, want: CurrentFullRing,
		},
		{
			name:     "undersized ring: fewer members than R, exclusion cannot reach R",
			fullRing: 2, joinerExcluded: 2, r: 3, want: CurrentFullRing,
		},
		{
			name: "exclusion shorter than the full ring floors even at R holders",
			// The exclusion must cost the unit no holder, not merely keep R.
			fullRing: 3, joinerExcluded: 2, r: 2, want: CurrentFullRing,
		},
		{
			name:     "exclusion longer than the full ring is not a shortfall",
			fullRing: 2, joinerExcluded: 3, r: 2, want: CurrentJoinerExcluded,
		},
		{
			name:     "no placement at all: nothing to reduce to",
			fullRing: 0, joinerExcluded: 0, r: 1, want: CurrentFullRing,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CurrentReplicaSet(tc.fullRing, tc.joinerExcluded, tc.r)
			if got != tc.want {
				t.Fatalf("CurrentReplicaSet(full=%d, excluded=%d, R=%d) = %v, want %v",
					tc.fullRing, tc.joinerExcluded, tc.r, got, tc.want)
			}
		})
	}
}

// TestCurrentReplicaSet_NeverAcksBelowR is the floor's reason for existing,
// stated as the property it guarantees: over every placement shape, the chosen
// set holds at least R members whenever the full ring itself does. The write ack
// bar is computed over len(CURRENT), so a shortfall here is a write acking on
// fewer than R durable copies.
func TestCurrentReplicaSet_NeverAcksBelowR(t *testing.T) {
	for r := 1; r <= 4; r++ {
		for full := 0; full <= 5; full++ {
			for excluded := 0; excluded <= full; excluded++ {
				chosen := full
				if CurrentReplicaSet(full, excluded, r) == CurrentJoinerExcluded {
					chosen = excluded
				}
				if full >= r && chosen < r {
					t.Fatalf("full=%d excluded=%d R=%d: CURRENT has %d members, below R", full, excluded, r, chosen)
				}
			}
		}
	}
}
