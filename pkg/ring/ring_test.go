package ring_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Zamua/shale/pkg/ring"
)

func TestNew_IsEmpty(t *testing.T) {
	r := ring.New()
	if !r.Empty() {
		t.Fatalf("fresh ring should be empty")
	}
	if got := r.Members(); len(got) != 0 {
		t.Fatalf("fresh ring should have no members, got %v", got)
	}
}

func TestAdd_LocateKeyReturnsAMember(t *testing.T) {
	r := ring.New()
	r.Add(ring.Member{ID: "n1", Addr: "127.0.0.1:7001"})
	r.Add(ring.Member{ID: "n2", Addr: "127.0.0.1:7002"})
	r.Add(ring.Member{ID: "n3", Addr: "127.0.0.1:7003"})

	if r.Empty() {
		t.Fatalf("ring with 3 members should not be Empty")
	}
	got := r.LocateKey([]byte("anything"))
	switch got.ID {
	case "n1", "n2", "n3":
		// ok, and Addr must have come along for the ride.
		if got.Addr == "" {
			t.Fatalf("LocateKey returned Member with empty Addr: %+v", got)
		}
	default:
		t.Fatalf("LocateKey returned unknown member: %+v", got)
	}
}

func TestLocateKey_Deterministic(t *testing.T) {
	r := ring.New()
	for _, id := range []string{"a", "b", "c", "d"} {
		r.Add(ring.Member{ID: id, Addr: "host:" + id})
	}
	first := r.LocateKey([]byte("repeatable-key"))
	for i := range 50 {
		got := r.LocateKey([]byte("repeatable-key"))
		if got != first {
			t.Fatalf("LocateKey is not deterministic: first=%+v iter %d=%+v", first, i, got)
		}
	}
}

func TestLocateKey_HashTagsCoLocate(t *testing.T) {
	r := ring.New()
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		r.Add(ring.Member{ID: id, Addr: "host:" + id})
	}
	a := r.LocateKey([]byte("{alice}/foo"))
	b := r.LocateKey([]byte("{alice}/bar"))
	c := r.LocateKey([]byte("{alice}/baz/qux"))
	if a != b || b != c {
		t.Fatalf("hash-tagged keys with same tag should co-locate: got %+v %+v %+v", a, b, c)
	}
}

func TestLocateKey_NoBracesUsesWholeKey(t *testing.T) {
	// Sanity-check the spec: the untagged key "foo" hashes on "foo"
	// while the tagged key "{foo}" hashes on "foo" as well, so those
	// two MUST land on the same Member (both hash the same bytes).
	// More importantly, an untagged key like "foo/bar" hashes on the
	// whole thing - independently of "{foo}/bar" which hashes on just
	// "foo". We verify the second claim: across a sample of keys, the
	// tagged + untagged forms diverge often enough that the routing
	// is plainly NOT coupled to the brace contents.
	r := ring.New()
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		r.Add(ring.Member{ID: id, Addr: "host:" + id})
	}

	divergences := 0
	for _, k := range []string{"foo/1", "foo/2", "foo/3", "foo/aaa", "foo/bbb", "foo/longer/path"} {
		untagged := r.LocateKey([]byte(k))
		tagged := r.LocateKey([]byte("{foo}/" + k[len("foo/"):]))
		if untagged != tagged {
			divergences++
		}
	}
	if divergences == 0 {
		t.Fatalf("expected untagged keys to hash independently of {foo}-tagged ones; got identical routing for all samples")
	}
}

func TestShardKey_Extraction(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"foo", "foo"},                 // no braces
		{"foo/bar", "foo/bar"},         // no braces
		{"{alice}/foo", "alice"},       // first balanced pair
		{"{alice}/foo/{bob}", "alice"}, // only first pair
		{"prefix{tag}suffix", "tag"},   // mid-key tag
		{"{}", "{}"},                   // empty tag, fall through
		{"{open", "{open"},             // unbalanced, fall through
		{"closed}", "closed}"},         // unbalanced, fall through
		{"{a}/{b}", "a"},               // first wins
		{"{}/foo", "{}/foo"},           // empty tag fall through
	}
	for _, c := range cases {
		got := ring.ShardKey([]byte(c.in))
		if !bytes.Equal(got, []byte(c.want)) {
			t.Errorf("ShardKey(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestRemove_DropsMember(t *testing.T) {
	r := ring.New()
	r.Add(ring.Member{ID: "n1", Addr: "h:1"})
	r.Add(ring.Member{ID: "n2", Addr: "h:2"})
	r.Add(ring.Member{ID: "n3", Addr: "h:3"})

	r.Remove("n2")
	members := r.Members()
	if len(members) != 2 {
		t.Fatalf("after Remove, expected 2 members, got %d (%+v)", len(members), members)
	}
	for _, m := range members {
		if m.ID == "n2" {
			t.Fatalf("n2 should have been removed, still present in %+v", members)
		}
	}

	// LocateKey must never return n2 for any sampled key.
	for i := range 1000 {
		got := r.LocateKey(fmt.Appendf(nil, "key-%d", i))
		if got.ID == "n2" {
			t.Fatalf("LocateKey returned removed member n2 for key-%d", i)
		}
	}

	// Removing a non-existent ID is a no-op.
	r.Remove("nope")
	if len(r.Members()) != 2 {
		t.Fatalf("Remove of unknown id should be a no-op")
	}
}

func TestMembers_SortedByID(t *testing.T) {
	r := ring.New()
	for _, id := range []string{"c", "a", "b"} {
		r.Add(ring.Member{ID: id, Addr: "h:" + id})
	}
	got := r.Members()
	want := []string{"a", "b", "c"}
	for i, m := range got {
		if m.ID != want[i] {
			t.Fatalf("Members not sorted: got %+v want order %v", got, want)
		}
	}
}

func TestAdd_ReplacesExistingMember(t *testing.T) {
	r := ring.New()
	r.Add(ring.Member{ID: "n1", Addr: "old:1"})
	r.Add(ring.Member{ID: "n1", Addr: "new:1"})
	if len(r.Members()) != 1 {
		t.Fatalf("expected 1 member after replace, got %d", len(r.Members()))
	}
	// LocateKey must hand back the NEW Addr.
	got := r.LocateKey([]byte("x"))
	if got.Addr != "new:1" {
		t.Fatalf("expected updated Addr 'new:1', got %q", got.Addr)
	}
}

func TestPartitions_FixedCount(t *testing.T) {
	r := ring.New()
	parts := r.Partitions()
	if len(parts) == 0 {
		t.Fatalf("Partitions should be non-empty even with no members")
	}
	// Partitions are ascending ids starting at 0; the count is the
	// ring's configured partitionCount (271 at the time of writing).
	for i := range parts {
		if parts[i] != uint64(i) {
			t.Fatalf("Partitions[%d] = %d, want %d", i, parts[i], i)
		}
	}
}

func TestOwner_EmptyRingReturnsZero(t *testing.T) {
	r := ring.New()
	got := r.Owner(0)
	if got.ID != "" || got.Addr != "" {
		t.Fatalf("Owner on empty ring should be zero Member, got %+v", got)
	}
}

func TestOwner_AgreesWithLocateKey(t *testing.T) {
	r := ring.New()
	for _, id := range []string{"a", "b", "c", "d"} {
		r.Add(ring.Member{ID: id, Addr: "h:" + id})
	}
	for i := range 50 {
		k := fmt.Appendf(nil, "k-%d", i)
		want := r.LocateKey(k)
		got := r.Owner(r.PartitionID(k))
		if got != want {
			t.Fatalf("Owner(PartitionID(%q)) = %+v, want LocateKey = %+v", k, got, want)
		}
	}
}

func TestOwner_OutOfRange(t *testing.T) {
	r := ring.New()
	r.Add(ring.Member{ID: "a", Addr: "h:a"})
	// Partition ids beyond the configured count must not panic and
	// must return the zero Member.
	got := r.Owner(1 << 30)
	if got.ID != "" || got.Addr != "" {
		t.Fatalf("Owner(huge) should be zero Member, got %+v", got)
	}
}

func TestDistribution_BoundedByLoadFactor(t *testing.T) {
	const (
		members  = 5
		keys     = 10000
		loadFact = 1.25 // matches ring.go's loadFactor
	)
	r := ring.New()
	for i := range members {
		id := fmt.Sprintf("n%d", i)
		r.Add(ring.Member{ID: id, Addr: "h:" + id})
	}

	counts := make(map[string]int, members)
	for i := range keys {
		m := r.LocateKey(fmt.Appendf(nil, "key-%d", i))
		counts[m.ID]++
	}
	if len(counts) != members {
		t.Fatalf("expected keys to land on all %d members, got %d distinct (%v)", members, len(counts), counts)
	}

	mean := float64(keys) / float64(members)
	// Bounded-loads guarantees no member exceeds load * mean by more
	// than one partition's worth. We allow a small absolute slack
	// (partition granularity is keys / 271, so a few-keys overshoot
	// is normal) on top of the multiplicative bound.
	limit := loadFact*mean + float64(keys)/271.0
	for id, c := range counts {
		if float64(c) > limit {
			t.Errorf("member %s got %d keys, exceeds bounded-load limit %.0f (mean=%.0f, factor=%.2f)",
				id, c, limit, mean, loadFact)
		}
	}
}

func TestLocateKeyN_OneMatchesLocateKey(t *testing.T) {
	r := ring.New()
	r.Add(ring.Member{ID: "n1", Addr: "h:n1"})
	r.Add(ring.Member{ID: "n2", Addr: "h:n2"})
	r.Add(ring.Member{ID: "n3", Addr: "h:n3"})

	for _, k := range [][]byte{
		[]byte("alpha"), []byte("beta"), []byte("gamma"),
		[]byte("user:42"), []byte("pastes/abc"),
	} {
		single := r.LocateKey(k)
		multi := r.LocateKeyN(k, 1)
		if len(multi) != 1 {
			t.Fatalf("LocateKeyN(k, 1) length = %d, want 1", len(multi))
		}
		if multi[0] != single {
			t.Errorf("LocateKeyN(k=%q, 1) primary %+v != LocateKey %+v", k, multi[0], single)
		}
	}
}

func TestLocateKeyN_AllMembersWhenNEqualsMembers(t *testing.T) {
	r := ring.New()
	r.Add(ring.Member{ID: "n1", Addr: "h:n1"})
	r.Add(ring.Member{ID: "n2", Addr: "h:n2"})
	r.Add(ring.Member{ID: "n3", Addr: "h:n3"})

	got := r.LocateKeyN([]byte("any-key"), 3)
	if len(got) != 3 {
		t.Fatalf("LocateKeyN(_, 3) on 3-member ring should return 3, got %d", len(got))
	}
	seen := map[string]bool{}
	for _, m := range got {
		if m.Addr == "" {
			t.Errorf("returned Member should carry Addr: %+v", m)
		}
		if seen[m.ID] {
			t.Errorf("LocateKeyN returned duplicate member %q", m.ID)
		}
		seen[m.ID] = true
	}
	if len(seen) != 3 {
		t.Errorf("expected 3 distinct members, got %d (%v)", len(seen), seen)
	}
}

func TestLocateKeyN_NGreaterThanMembersReturnsAll(t *testing.T) {
	r := ring.New()
	r.Add(ring.Member{ID: "n1", Addr: "h:n1"})
	r.Add(ring.Member{ID: "n2", Addr: "h:n2"})
	r.Add(ring.Member{ID: "n3", Addr: "h:n3"})

	// Caller asked for 5; ring has 3. No dupes, just every member.
	got := r.LocateKeyN([]byte("user:alice"), 5)
	if len(got) != 3 {
		t.Fatalf("LocateKeyN(_, 5) on 3-member ring should clamp to 3 distinct, got %d", len(got))
	}
	seen := map[string]bool{}
	for _, m := range got {
		if seen[m.ID] {
			t.Errorf("duplicate member %q in clamped result", m.ID)
		}
		seen[m.ID] = true
	}
}

func TestLocateKeyN_HashTagsRespected(t *testing.T) {
	// Per docs/SPEC.md, keys sharing a {tag} hash on the tag only +
	// must land on the same primary AND the same successor chain.
	r := ring.New()
	r.Add(ring.Member{ID: "n1", Addr: "h:n1"})
	r.Add(ring.Member{ID: "n2", Addr: "h:n2"})
	r.Add(ring.Member{ID: "n3", Addr: "h:n3"})

	a := r.LocateKeyN([]byte("{alice}/pastes/abc12345"), 3)
	b := r.LocateKeyN([]byte("{alice}/versions/abc12345/v2"), 3)
	if len(a) != len(b) {
		t.Fatalf("hash-tagged keys returned different replica-set sizes: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("hash-tagged keys must produce identical replica chains; position %d: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestLocateKeyN_EmptyRing(t *testing.T) {
	r := ring.New()
	if got := r.LocateKeyN([]byte("anything"), 3); len(got) != 0 {
		t.Errorf("LocateKeyN on empty ring should return nil/empty, got %v", got)
	}
}

func TestLocateKeyN_NonPositiveN(t *testing.T) {
	r := ring.New()
	r.Add(ring.Member{ID: "n1", Addr: "h:n1"})
	if got := r.LocateKeyN([]byte("k"), 0); len(got) != 0 {
		t.Errorf("LocateKeyN(_, 0) should return empty, got %v", got)
	}
	if got := r.LocateKeyN([]byte("k"), -1); len(got) != 0 {
		t.Errorf("LocateKeyN(_, -1) should return empty, got %v", got)
	}
}

// TestRing_DenseChurnNoPanic_466 pins the #466 fix: a dense membership churn
// (well past the 12-member count that crash-looped prod-staging) including the
// re-add path (Add of an already-present ID) must NOT panic out of the
// consistent-hash library, and must leave the ring consistent. Before the fix
// (incremental hash.Remove on re-add) this nil-deref'd in distributeWithLoad at
// ~12 members; the rebuild-from-member-set fix makes it safe.
func TestRing_DenseChurnNoPanic_466(t *testing.T) {
	r := ring.New()
	const N = 16
	add := func(i int) {
		r.Add(ring.Member{ID: fmt.Sprintf("n%02d", i), Addr: fmt.Sprintf("10.0.0.%d:7946", i)})
	}
	// scale up to N (past 12)
	for i := 0; i < N; i++ {
		add(i)
	}
	if got := len(r.Members()); got != N {
		t.Fatalf("after scale-up want %d members, got %d", N, got)
	}
	// re-add every member (the path that triggered #466) + a churn of
	// remove/re-add while the cluster is dense.
	for round := 0; round < 5; round++ {
		for i := 0; i < N; i++ {
			add(i) // re-add (existing ID)
		}
		r.Remove("n07")
		add(7) // re-add after remove
		r.Remove("n11")
		r.Remove("n12")
		add(11)
		add(12)
	}
	// the ring must still resolve keys to a live member and stay whole.
	if got := len(r.Members()); got != N {
		t.Fatalf("after churn want %d members, got %d", N, got)
	}
	if m := r.LocateKey([]byte("some-key")); m.ID == "" {
		t.Fatalf("LocateKey returned empty member after churn")
	}
	if p := r.RebuildPanics(); p != 0 {
		t.Fatalf("rebuild panicked %d times at N=%d (the library still faults); fix insufficient", p, N)
	}
}

// TestRing_RebuildOwnershipDeterministic pins that building the ring from the
// SAME member set in different insertion orders yields the SAME ownership (the
// rebuild-from-truth fix must not change which node owns a key vs the prior
// incremental build, or peers would disagree on routing).
func TestRing_RebuildOwnershipDeterministic(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"}
	mk := func(order []string) *ring.Ring {
		r := ring.New()
		for _, id := range order {
			r.Add(ring.Member{ID: id, Addr: "h:" + id})
		}
		return r
	}
	forward := mk(ids)
	rev := make([]string, len(ids))
	for i, id := range ids {
		rev[len(ids)-1-i] = id
	}
	reverse := mk(rev)
	for _, key := range []string{"k1", "k2", "alpha", "{tag}/x", "zzz", "0xdead"} {
		if a, b := forward.LocateKey([]byte(key)), reverse.LocateKey([]byte(key)); a.ID != b.ID {
			t.Fatalf("ownership of %q differs by insertion order: %s vs %s", key, a.ID, b.ID)
		}
	}
}
