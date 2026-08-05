package decide

import "testing"

// TestBootWarmUp enumerates the boot gate over the shapes a replicated boot can
// end in: a founder and a joiner that deferred nothing, a staggered join, and
// the fleet-wide simultaneous boot the debounce path exists for.
func TestBootWarmUp(t *testing.T) {
	cases := []struct {
		name string
		in   BootState
		want BootAction
	}{
		{
			name: "founder boot: nothing deferred, no bit to take down",
			in:   BootState{},
			want: BootAction{Reason: BootWarm},
		},
		{
			name: "a joiner that deferred nothing drops the bit NOW, not on the first reconcile tick",
			// Held until that tick, the bit makes every peer booting into this
			// node read it as not established.
			in:   BootState{Peers: 2, SelfJoining: true},
			want: BootAction{Reason: BootStaleJoining},
		},
		{
			name: "staggered join: deferred positions and an established peer",
			in:   BootState{Deferred: 3, Peers: 2, SelfJoining: true},
			want: BootAction{Joining: true, Prompt: true, Reason: BootEstablishedPeer},
		},
		{
			name: "staggered join into a partly-warming cluster: one established peer is enough",
			in:   BootState{Deferred: 1, Peers: 3, JoiningPeers: 2, SelfJoining: true},
			want: BootAction{Joining: true, Prompt: true, Reason: BootEstablishedPeer},
		},
		{
			name: "MASS BOOT: deferred positions, every visible peer joining",
			// Acting on this partial view acquires positions this node will not
			// own once the ring converges, at epochs that fence booting siblings.
			in:   BootState{Deferred: 4, Peers: 2, JoiningPeers: 2, SelfJoining: true},
			want: BootAction{Joining: true, Reason: BootFleetJoining},
		},
		{
			name: "first node up in a mass boot: deferred positions, no peer visible at all",
			in:   BootState{Deferred: 4, Peers: 0, JoiningPeers: 0, SelfJoining: true},
			want: BootAction{Joining: true, Reason: BootFleetJoining},
		},
		{
			name: "founder that found serving markers raises the bit it never had",
			// A re-forming node founds membership (no Joining role accepted) and
			// still defers to the markers of the generation before it.
			in:   BootState{Deferred: 2, Peers: 1, SelfJoining: false},
			want: BootAction{Joining: true, Prompt: true, Reason: BootEstablishedPeer},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := BootWarmUp(tc.in); got != tc.want {
				t.Fatalf("BootWarmUp(%+v) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
}

// TestBootWarmUp_JoiningTracksTheOutcome pins the advertised bit to what the
// mount pass DID, never to what this node intended at membership-open: a node
// that deferred nothing lowers the bit whatever it was carrying, and a node
// that deferred anything raises it.
func TestBootWarmUp_JoiningTracksTheOutcome(t *testing.T) {
	for _, deferred := range []int{0, 1, 7} {
		for _, self := range []bool{false, true} {
			for peers := 0; peers <= 3; peers++ {
				for joining := 0; joining <= peers; joining++ {
					in := BootState{Deferred: deferred, Peers: peers, JoiningPeers: joining, SelfJoining: self}
					if got := BootWarmUp(in).Joining; got != (deferred > 0) {
						t.Fatalf("BootWarmUp(%+v).Joining = %v, want %v", in, got, deferred > 0)
					}
				}
			}
		}
	}
}

// TestBootWarmUp_NeverPromptsWithoutAnEstablishedPeer pins the mass-boot guard
// as a property over every view shape: prompting on a view that shows only
// joining peers is acting on a partial topology, which lost acked writes.
func TestBootWarmUp_NeverPromptsWithoutAnEstablishedPeer(t *testing.T) {
	for _, deferred := range []int{0, 1, 7} {
		for _, self := range []bool{false, true} {
			for peers := 0; peers <= 4; peers++ {
				in := BootState{Deferred: deferred, Peers: peers, JoiningPeers: peers, SelfJoining: self}
				if BootWarmUp(in).Prompt {
					t.Fatalf("BootWarmUp(%+v) prompted with no established peer", in)
				}
			}
		}
	}
}

// TestBootWarmUp_CleanBootMakesThisNodeAnEstablishedPeer composes the gate with
// itself across two nodes, which is where a stale bit costs anything: the bit
// node A publishes after its own clean boot is exactly what B's copy of the
// gate counts a second later. An A that kept the bit up costs B the prompt path.
func TestBootWarmUp_CleanBootMakesThisNodeAnEstablishedPeer(t *testing.T) {
	a := BootWarmUp(BootState{Deferred: 0, SelfJoining: true})
	joiningPeers := 0
	if a.Joining {
		joiningPeers = 1
	}
	b := BootWarmUp(BootState{Deferred: 2, Peers: 1, JoiningPeers: joiningPeers, SelfJoining: true})
	if !b.Prompt {
		t.Fatalf("a=%+v then b=%+v: B did not prompt into a cleanly-booted A", a, b)
	}
}
