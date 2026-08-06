package decide

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

// -- generators ---------------------------------------------------------

// role is a member's transitional stance. The three are EXCLUSIVE: a member
// cannot be warming into a position it has not mounted and yielding one it
// still serves at the same time.
type role int

const (
	roleSteady role = iota
	roleJoining
	roleDraining
)

func (r role) String() string {
	switch r {
	case roleJoining:
		return "joining"
	case roleDraining:
		return "draining"
	default:
		return "steady"
	}
}

// topology is one generated cluster shape: the members in RING ORDER, each
// member's role, and R.
type topology struct {
	members []string
	roles   []role
	r       int
}

func (tp topology) String() string {
	parts := make([]string, len(tp.members))
	for i, id := range tp.members {
		parts[i] = id + "=" + tp.roles[i].String()
	}
	return fmt.Sprintf("R=%d ring[%s]", tp.r, strings.Join(parts, " "))
}

func (tp topology) roleOf(id string) role {
	for i, m := range tp.members {
		if m == id {
			return tp.roles[i]
		}
	}
	return roleSteady
}

// firstR is the RANKED model of a placement: a unit sits on the first R members
// of the ring order that keep passes, so a placement computed AS IF a set of
// members were not members is the first R of the order with that set removed.
//
// The model is what makes the surviving-owner invariants below TRUE rather than
// assumed: removing members from an order does not reorder the ones that stay,
// so a member of the full placement that survives an exclusion cannot lose
// rank, and therefore cannot fall out of the excluded placement. A real
// bounded-load coordinator can violate that, which is why the invariants that
// depend on it are stated only over this generator and the ones that do not are
// also stated over the arbitrary generator below.
func (tp topology) firstR(keep func(role) bool) []string {
	out := make([]string, 0, tp.r)
	for i, id := range tp.members {
		if len(out) == tp.r {
			break
		}
		if keep(tp.roles[i]) {
			out = append(out, id)
		}
	}
	return out
}

func (tp topology) placements() Placements {
	return Placements{
		Full:            tp.firstR(func(role) bool { return true }),
		JoinerExcluded:  tp.firstR(func(rl role) bool { return rl != roleJoining }),
		DrainerExcluded: tp.firstR(func(rl role) bool { return rl != roleDraining }),
	}
}

// eachTopology enumerates the ring-model space EXHAUSTIVELY: 1 to 6 members, R
// of 1 to 3, and every assignment of {steady, joining, draining} to the members
// (3^n). 3276 topologies.
//
// Fixing the ring ORDER to the member order costs no generality. Permuting the
// ring is the same as permuting which members carry which role, and every role
// assignment is already enumerated, so a permuted ring is some enumerated
// topology under a renaming - and none of the properties below can tell members
// apart by name.
func eachTopology(fn func(topology)) {
	for n := 1; n <= 6; n++ {
		members := make([]string, n)
		for i := range members {
			members[i] = fmt.Sprintf("n%d", i)
		}
		assignments := 1
		for range n {
			assignments *= 3
		}
		for a := range assignments {
			roles := make([]role, n)
			for i, x := 0, a; i < n; i, x = i+1, x/3 {
				roles[i] = role(x % 3)
			}
			for r := 1; r <= 3; r++ {
				fn(topology{members: members, roles: roles, r: r})
			}
		}
	}
}

// eachArbitraryPlacements enumerates placements NO ring would produce: every
// triple of subsets of a four-member universe, with R of 1 to 3. 12288 cases.
//
// The exclusions here are unrelated to the full placement - disjoint from it,
// empty, larger than it, arbitrary. Route is not entitled to assume otherwise:
// the exclusions are separate coordinator LOCATE calls, and bounded-load
// consistent hashing is not removal-invariant, so a re-located placement is not
// obliged to be the full one minus anything. The invariants that are arithmetic
// over the inputs - the quorum floor, the shape of the routed set - must hold
// across all of it.
func eachArbitraryPlacements(fn func(Placements, int)) {
	universe := []string{"a", "b", "c", "d"}
	subsets := make([][]string, 0, 1<<len(universe))
	for m := range 1 << len(universe) {
		var s []string
		for i, id := range universe {
			if m&(1<<i) != 0 {
				s = append(s, id)
			}
		}
		subsets = append(subsets, s)
	}
	for _, full := range subsets {
		for _, joinerExcluded := range subsets {
			for _, drainerExcluded := range subsets {
				for r := 1; r <= 3; r++ {
					fn(Placements{Full: full, JoinerExcluded: joinerExcluded, DrainerExcluded: drainerExcluded}, r)
				}
			}
		}
	}
}

func render(p Placements, rt Routing) string {
	return fmt.Sprintf("full=%v joiner-excluded=%v drainer-excluded=%v -> current=%v(%v) pending=%v(%v) routed=%v stableR=%d inTransition=%v",
		p.Full, p.JoinerExcluded, p.DrainerExcluded,
		p.currentSet(rt.Current), rt.Current, p.pendingSet(rt.Pending), rt.Pending,
		rt.Routed, rt.StableR, rt.InTransition)
}

// -- named anchors ------------------------------------------------------

// TestRoute names the shapes the enumerations below cover anonymously, so the
// decision can be read as a table rather than reconstructed from properties.
func TestRoute(t *testing.T) {
	cases := []struct {
		name             string
		p                Placements
		r                int
		wantRouted       []string
		wantStableR      int
		wantInTransition bool
	}{
		{
			name: "steady state R=2: no transition anywhere, routed is the placement",
			p: Placements{
				Full: []string{"a", "b"}, JoinerExcluded: []string{"a", "b"}, DrainerExcluded: []string{"a", "b"},
			},
			r: 2, wantRouted: []string{"a", "b"}, wantStableR: 2,
		},
		{
			name: "LEAVE R=2: the leaver b is still current, its successor c joins the union",
			// b keeps serving until it leaves the ring, so it stays in CURRENT
			// and the bar stays at 2 - c is a bonus target while it mounts.
			p: Placements{
				Full: []string{"a", "b"}, JoinerExcluded: []string{"a", "b"}, DrainerExcluded: []string{"a", "c"},
			},
			r: 2, wantRouted: []string{"a", "b", "c"}, wantStableR: 2, wantInTransition: true,
		},
		{
			name: "JOIN R=2: the newcomer c is out of CURRENT, in the union",
			// The displaced owner b holds the bytes; c is warming.
			p: Placements{
				Full: []string{"a", "c"}, JoinerExcluded: []string{"a", "b"}, DrainerExcluded: []string{"a", "c"},
			},
			r: 2, wantRouted: []string{"a", "b", "c"}, wantStableR: 2, wantInTransition: true,
		},
		{
			name: "MASS BOOT R=2: both holders joining, the floor keeps the bar at 2",
			// Unfloored this is CURRENT empty and an ack bar of zero.
			p: Placements{
				Full: []string{"a", "b"}, JoinerExcluded: nil, DrainerExcluded: []string{"a", "b"},
			},
			r: 2, wantRouted: []string{"a", "b"}, wantStableR: 2,
		},
		{
			name: "EVERY member draining: no post-transition placement, route the stable set",
			p: Placements{
				Full: []string{"a"}, JoinerExcluded: []string{"a"}, DrainerExcluded: nil,
			},
			r: 1, wantRouted: []string{"a"}, wantStableR: 1,
		},
		{
			name: "a transition elsewhere in the cluster is not this unit's transition",
			// Some member carries a bit, but not one this unit's placement moves
			// over, so all three placements coincide.
			p: Placements{
				Full: []string{"a", "b"}, JoinerExcluded: []string{"a", "b"}, DrainerExcluded: []string{"a", "b"},
			},
			r: 2, wantRouted: []string{"a", "b"}, wantStableR: 2,
		},
		{
			name: "leave AND join at once: current drops the joiner, pending drops the drainer",
			p: Placements{
				Full: []string{"a", "d"}, JoinerExcluded: []string{"a", "b"}, DrainerExcluded: []string{"a", "c"},
			},
			r: 2, wantRouted: []string{"a", "b", "c"}, wantStableR: 2, wantInTransition: true,
		},
		{
			name: "undersized ring: fewer members than R floors, and the bar is what exists",
			p: Placements{
				Full: []string{"a"}, JoinerExcluded: []string{"a"}, DrainerExcluded: []string{"a"},
			},
			r: 3, wantRouted: []string{"a"}, wantStableR: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := Route(tc.p, tc.r)
			if !slices.Equal(rt.Routed, tc.wantRouted) {
				t.Fatalf("routed = %v, want %v\n%s", rt.Routed, tc.wantRouted, render(tc.p, rt))
			}
			if rt.StableR != tc.wantStableR {
				t.Fatalf("stableR = %d, want %d\n%s", rt.StableR, tc.wantStableR, render(tc.p, rt))
			}
			if rt.InTransition != tc.wantInTransition {
				t.Fatalf("inTransition = %v, want %v\n%s", rt.InTransition, tc.wantInTransition, render(tc.p, rt))
			}
		})
	}
}

// -- (a) the union never drops a node that is already serving -----------

// TestRoute_RoutedContainsEveryCurrentOwner pins the structural half over BOTH
// generators: whatever Route decides is CURRENT is routed in full.
//
// A current owner is by definition the member holding the position mounted
// right now. Dropping one leaves an op routed exclusively at members that are
// still opening, which is the retryable acquiring refusal for every caller
// until a mount lands - a unit with no mounted holder anywhere in its union.
func TestRoute_RoutedContainsEveryCurrentOwner(t *testing.T) {
	ring, arbitrary := 0, 0
	check := func(p Placements, r int) {
		rt := Route(p, r)
		for _, id := range p.currentSet(rt.Current) {
			if !slices.Contains(rt.Routed, id) {
				t.Fatalf("current owner %q is not routed\n%s", id, render(p, rt))
			}
		}
	}
	eachTopology(func(tp topology) { ring++; check(tp.placements(), tp.r) })
	eachArbitraryPlacements(func(p Placements, r int) { arbitrary++; check(p, r) })
	t.Logf("checked %d ring topologies and %d arbitrary placement triples", ring, arbitrary)
}

// TestRoute_UnionNeverDropsAServingOwner pins the invariant a leave turns on:
// every member of the PRE-TRANSITION placement that is not itself warming stays
// routed for the whole transition.
//
// It is stated over the ring generator because it needs the exclusions to be
// re-locations of the SAME ring (see topology.firstR): a member that survives an
// exclusion keeps its rank, so it cannot fall out of the excluded placement. A
// member is dropped from CURRENT only for being JOINING, and a joining member is
// not serving - it owns a position it has not mounted.
func TestRoute_UnionNeverDropsAServingOwner(t *testing.T) {
	cases, withDrainingOwner := 0, 0
	eachTopology(func(tp topology) {
		p := tp.placements()
		rt := Route(p, tp.r)
		cases++
		draining := false
		for _, id := range p.Full {
			if tp.roleOf(id) == roleDraining {
				draining = true
			}
			if tp.roleOf(id) == roleJoining {
				continue
			}
			if !slices.Contains(rt.Routed, id) {
				t.Fatalf("%s: %q was serving the unit before the transition and is not routed during it\n%s",
					tp, id, render(p, rt))
			}
		}
		if draining {
			withDrainingOwner++
		}
	})
	t.Logf("checked %d topologies, %d of them with a draining member in the unit's placement", cases, withDrainingOwner)
}

// -- (b) a draining member keeps serving until it leaves the ring -------

// TestRoute_DrainingOwnerStaysCurrent pins that a member yielding a position it
// still holds stays a CURRENT owner, not merely a routed one: it counts toward
// stableR, which is why the leave direction never needs the quorum floor. Only
// the joining exclusion can shrink CURRENT.
func TestRoute_DrainingOwnerStaysCurrent(t *testing.T) {
	owners := 0
	eachTopology(func(tp topology) {
		p := tp.placements()
		rt := Route(p, tp.r)
		for _, id := range p.Full {
			if tp.roleOf(id) != roleDraining {
				continue
			}
			owners++
			if !slices.Contains(p.currentSet(rt.Current), id) {
				t.Fatalf("%s: draining owner %q left CURRENT, so the ack bar no longer counts a member that is still serving\n%s",
					tp, id, render(p, rt))
			}
			if !slices.Contains(rt.Routed, id) {
				t.Fatalf("%s: draining owner %q is not routed, so ops skip the member that still holds the position\n%s",
					tp, id, render(p, rt))
			}
		}
	})
	t.Logf("checked %d draining owners across the ring topologies", owners)
}

// -- (c) the quorum floor -----------------------------------------------

// TestRoute_StableRNeverBelowQuorum is the floor's reason for existing at the
// routing level: a transition must never lower the write ack bar.
//
// The bar is requiredWriteAcks(consistency, stableR), so stableR is the number
// of durable copies an ack can be built on. It may only fall below R when the
// placement itself is smaller than R (an undersized ring has no R-th copy to
// require); a transition shrinking CURRENT must never take it there. Without
// the floor a mass boot computes CURRENT empty, and a bar of
// requiredWriteAcks(consistency, 0) is zero - a write acking with no durable
// copy at all.
func TestRoute_StableRNeverBelowQuorum(t *testing.T) {
	ring, arbitrary := 0, 0
	check := func(p Placements, r int) {
		rt := Route(p, r)
		if floor := min(r, len(p.Full)); rt.StableR < floor {
			t.Fatalf("stableR = %d, below the floor of %d: a write can ack on fewer durable copies than the normal bar\n%s",
				rt.StableR, floor, render(p, rt))
		}
	}
	eachTopology(func(tp topology) { ring++; check(tp.placements(), tp.r) })
	eachArbitraryPlacements(func(p Placements, r int) { arbitrary++; check(p, r) })
	t.Logf("checked %d ring topologies and %d arbitrary placement triples", ring, arbitrary)
}

// TestRoute_AckBarIsReachable pins the other side of the same number: stableR
// never exceeds the routed set. A bar higher than the number of replicas an op
// contacts is unsatisfiable, so every write to the unit would wedge to its
// timeout rather than fail on a quorum it could name.
func TestRoute_AckBarIsReachable(t *testing.T) {
	check := func(p Placements, r int) {
		rt := Route(p, r)
		if rt.StableR > len(rt.Routed) {
			t.Fatalf("stableR = %d over a routed set of %d: the ack bar cannot be reached\n%s",
				rt.StableR, len(rt.Routed), render(p, rt))
		}
	}
	eachTopology(func(tp topology) { check(tp.placements(), tp.r) })
	eachArbitraryPlacements(check)
}

// -- (d) no transition machinery in the common path ---------------------

// TestRoute_SteadyStateIsExactlyTheFullPlacement pins that a unit no transition
// touches routes its placement UNCHANGED - same members, same order, bar at the
// full count, not in transition.
//
// Two shapes qualify and both are covered: no member carries a bit at all, and
// a member carries one but sits outside this unit's placement (excluding a
// member ranked below R does not move the first R). The second is the common
// case during any real transition - one node moves, most units do not - so a
// union appearing there would put every unit in the cluster on the transition
// path for the duration.
func TestRoute_SteadyStateIsExactlyTheFullPlacement(t *testing.T) {
	quiet, elsewhere := 0, 0
	eachTopology(func(tp topology) {
		p := tp.placements()
		settled := true
		for _, id := range p.Full {
			if tp.roleOf(id) != roleSteady {
				settled = false
			}
		}
		if !settled {
			return
		}
		if slices.Contains(tp.roles, roleJoining) || slices.Contains(tp.roles, roleDraining) {
			elsewhere++
		} else {
			quiet++
		}
		rt := Route(p, tp.r)
		if rt.InTransition {
			t.Fatalf("%s: no member of the unit's placement is transitioning, yet the unit routes as in transition\n%s", tp, render(p, rt))
		}
		if !slices.Equal(rt.Routed, p.Full) {
			t.Fatalf("%s: routed = %v, want the placement itself %v (same order)\n%s", tp, rt.Routed, p.Full, render(p, rt))
		}
		if rt.StableR != len(p.Full) {
			t.Fatalf("%s: stableR = %d, want %d\n%s", tp, rt.StableR, len(p.Full), render(p, rt))
		}
	})
	t.Logf("checked %d fully quiet topologies and %d with a transition confined outside the unit's placement", quiet, elsewhere)
}

// -- (e) the routed set is well formed ----------------------------------

// TestRoute_RoutedIsWellFormed pins the routed set's shape: it is the union of
// CURRENT and PENDING and nothing else.
//
// A duplicate would double-count one replica's ack toward the bar, letting a
// write claim two durable copies it has one of. A member from outside every
// input placement would be an op sent to a node that owns nothing of the unit.
// A missing PENDING member would leave a successor un-dual-written, so it has to
// catch up from the shared db before it can serve.
func TestRoute_RoutedIsWellFormed(t *testing.T) {
	var inTransition int
	check := func(p Placements, r int) {
		rt := Route(p, r)
		seen := make(map[string]bool, len(rt.Routed))
		for _, id := range rt.Routed {
			if seen[id] {
				t.Fatalf("%q is routed twice\n%s", id, render(p, rt))
			}
			seen[id] = true
			if !slices.Contains(p.Full, id) && !slices.Contains(p.JoinerExcluded, id) && !slices.Contains(p.DrainerExcluded, id) {
				t.Fatalf("%q is routed but appears in none of the three placements\n%s", id, render(p, rt))
			}
		}
		current, pending := p.currentSet(rt.Current), p.pendingSet(rt.Pending)
		if !rt.InTransition {
			if !slices.Equal(rt.Routed, current) {
				t.Fatalf("off transition the routed set must be the CURRENT set itself, got %v\n%s", rt.Routed, render(p, rt))
			}
			return
		}
		inTransition++
		for _, id := range pending {
			if !slices.Contains(rt.Routed, id) {
				t.Fatalf("pending owner %q is not routed, so the transition's dual-write never reaches it\n%s", id, render(p, rt))
			}
		}
		want := len(current)
		for _, id := range pending {
			if !slices.Contains(current, id) {
				want++
			}
		}
		if len(rt.Routed) != want {
			t.Fatalf("routed set has %d members, want the union's %d\n%s", len(rt.Routed), want, render(p, rt))
		}
	}
	eachTopology(func(tp topology) { check(tp.placements(), tp.r) })
	ringUnions := inTransition
	eachArbitraryPlacements(check)
	t.Logf("routed a union in %d of 3276 ring topologies and %d of 12288 arbitrary placement triples",
		ringUnions, inTransition-ringUnions)
}
