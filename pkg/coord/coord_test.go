package coord_test

import (
	"testing"

	"github.com/Zamua/shale/pkg/coord"
)

// TestRole_IsABitSet pins that the two roles are INDEPENDENT bits and that Has
// tests for containment, not equality. The overlap protocol needs a node to be
// able to hold both at once (a node that joined, started warming, and is now
// being asked to yield), and a Has that compared equality would silently report
// "not draining" for a node advertising joining+draining.
func TestRole_IsABitSet(t *testing.T) {
	both := coord.RoleJoining | coord.RoleDraining
	if !both.Has(coord.RoleJoining) || !both.Has(coord.RoleDraining) {
		t.Fatalf("combined role %d does not report both bits", both)
	}
	if coord.RoleJoining.Has(coord.RoleDraining) {
		t.Fatal("joining alone reported draining")
	}
	var none coord.Role
	if none.Has(coord.RoleJoining) || none.Has(coord.RoleDraining) {
		t.Fatal("the zero role must be steady state (no bits set)")
	}
	if !none.Has(0) {
		t.Fatal("Has(0) must be true (every set contains the empty set)")
	}
}

// TestMember_RoleAccessors pins the two named predicates against the bit set,
// since the whole routing split is written in terms of them.
func TestMember_RoleAccessors(t *testing.T) {
	cases := []struct {
		roles    coord.Role
		joining  bool
		draining bool
	}{
		{0, false, false},
		{coord.RoleJoining, true, false},
		{coord.RoleDraining, false, true},
		{coord.RoleJoining | coord.RoleDraining, true, true},
	}
	for _, tc := range cases {
		m := coord.Member{Roles: tc.roles}
		if m.Joining() != tc.joining || m.Draining() != tc.draining {
			t.Fatalf("roles %d: joining=%v draining=%v, want %v/%v",
				tc.roles, m.Joining(), m.Draining(), tc.joining, tc.draining)
		}
	}
}

// TestView_EmptyAndMember pins the two view helpers callers gate on: Empty is
// what collapses a coordinator with no information to single-node behavior, and
// Member is the self / peer lookup.
func TestView_EmptyAndMember(t *testing.T) {
	var zero coord.View
	if !zero.Empty() {
		t.Fatal("the zero view must report Empty")
	}
	if _, ok := zero.Member("a"); ok {
		t.Fatal("the zero view reported a member")
	}

	v := coord.View{Members: []coord.Member{
		{Node: coord.Node{ID: "a", Addr: "a:1"}},
		{Node: coord.Node{ID: "b", Addr: "b:1"}, Roles: coord.RoleDraining},
	}}
	if v.Empty() {
		t.Fatal("a populated view reported Empty")
	}
	m, ok := v.Member("b")
	if !ok {
		t.Fatal("Member(b) not found in a view that holds it")
	}
	if !m.Draining() || m.Addr != "b:1" {
		t.Fatalf("Member(b) = %+v, want the draining b at b:1", m)
	}
	if _, ok := v.Member("zzz"); ok {
		t.Fatal("Member reported an absent id as found")
	}
}
