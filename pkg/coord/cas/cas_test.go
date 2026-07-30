package cas_test

// Adapter-specific tests for the CAS coordinator: bootstrap discovery via
// the conditional write, observer-side lease expiry + GC, CAS conflict
// retries, role visibility on the poll cadence, and the graceful-leave vs
// hard-kill split. The port-contract properties live in the shared harness
// (contract_test.go); nothing here re-pins those.
//
// Every test drives the adapter's two cadences DETERMINISTICALLY: the loops
// are disabled (negative intervals) and the tests tick TestingPollOnce /
// TestingRenewOnce by hand, so there is no timing luck anywhere - a poll
// happens exactly when a test says it does.

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/cas"
	"github.com/Zamua/shale/pkg/storageunit"
)

// manualCfg is the deterministic test configuration: both loops disabled,
// expiry after k unadvanced observed polls.
func manualCfg(store storageunit.ConditionalStore, k int) cas.Config {
	return cas.Config{Store: store, PollInterval: -1, RenewalInterval: -1, ExpiryPolls: k}
}

// mustStart starts a coordinator with the given identity and registers its
// Close.
func mustStart(t *testing.T, co *cas.Coordinator, id string, roles coord.Role, count uint32) coord.Bootstrap {
	t.Helper()
	b, err := co.Start(coord.Params{
		Self:              coord.Node{ID: storageunit.NodeID(id), Addr: id + ":1"},
		InitialRoles:      roles,
		DeclaredUnitCount: count,
	})
	if err != nil {
		t.Fatalf("Start(%s): %v", id, err)
	}
	t.Cleanup(func() { _ = co.Close() })
	return b
}

// wireRow / wireDoc pin the membership document's JSON WIRE FORMAT from the
// outside: these field names are what every node in a (possibly
// mixed-version) cluster reads, so a rename in the adapter must break here.
type wireRow struct {
	ID                string `json:"id"`
	Addr              string `json:"addr"`
	Roles             uint8  `json:"roles"`
	DeclaredUnitCount uint32 `json:"declaredUnitCount"`
	LeaseGen          uint64 `json:"leaseGen"`
	Inc               string `json:"inc"`
}

type wireDoc struct {
	Members []wireRow `json:"members"`
}

func readDoc(t *testing.T, store storageunit.ConditionalStore) wireDoc {
	t.Helper()
	data, _, err := store.Get(cas.DefaultKey)
	if err != nil {
		t.Fatalf("reading membership document: %v", err)
	}
	var d wireDoc
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatalf("decoding membership document %q: %v", data, err)
	}
	return d
}

func docIDs(d wireDoc) []string {
	ids := make([]string, 0, len(d.Members))
	for _, r := range d.Members {
		ids = append(ids, r.ID)
	}
	return ids
}

func viewIDs(v coord.View) []string {
	ids := make([]string, 0, len(v.Members))
	for _, m := range v.Members {
		ids = append(ids, string(m.ID))
	}
	return ids
}

func sameIDs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// docVersion returns the document's current CAS version token.
func docVersion(t *testing.T, store storageunit.ConditionalStore) string {
	t.Helper()
	_, ver, err := store.Get(cas.DefaultKey)
	if err != nil {
		t.Fatalf("reading membership document version: %v", err)
	}
	return ver
}

// TestStart_FoundsOnEmptyStoreAndJoinsIncumbent pins the bootstrap answer
// the conditional write makes exact: the PutIfAbsent winner FOUNDED, a
// later starter finds the incumbent document and JOINED - and sees the
// incumbent in its view the moment Start returns (the synchronous first
// observation).
func TestStart_FoundsOnEmptyStoreAndJoinsIncumbent(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(store, 5))
	if got := mustStart(t, a, "a", 0, 0); got != coord.BootstrapFounded {
		t.Fatalf("first starter reported %v, want BootstrapFounded", got)
	}
	if !a.Populated() || !sameIDs(viewIDs(a.View()), []string{"a"}) {
		t.Fatalf("founder's view = %v, want [a]", viewIDs(a.View()))
	}

	b := cas.New(manualCfg(store, 5))
	if got := mustStart(t, b, "b", 0, 0); got != coord.BootstrapJoined {
		t.Fatalf("second starter reported %v, want BootstrapJoined", got)
	}
	if got := viewIDs(b.View()); !sameIDs(got, []string{"a", "b"}) {
		t.Fatalf("joiner's view = %v, want [a b] the moment Start returns", got)
	}

	// The founder learns of the joiner on its own poll cadence.
	a.TestingPollOnce()
	if got := viewIDs(a.View()); !sameIDs(got, []string{"a", "b"}) {
		t.Fatalf("founder's view after poll = %v, want [a b]", got)
	}
}

// TestStart_FounderDropsJoiningRole_JoinerKeepsIt pins the port's founder
// guard on the CAS paths: a founder advertising Joining would exclude
// itself from its own current set, so the founding row drops it; a joiner's
// row keeps it, atomic with the row's first appearance.
func TestStart_FounderDropsJoiningRole_JoinerKeepsIt(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(store, 5))
	if got := mustStart(t, a, "a", coord.RoleJoining, 0); got != coord.BootstrapFounded {
		t.Fatalf("founder reported %v", got)
	}
	if m, _ := a.View().Member("a"); m.Roles != 0 {
		t.Fatalf("founder advertises roles %v, want none (Joining dropped)", m.Roles)
	}

	b := cas.New(manualCfg(store, 5))
	if got := mustStart(t, b, "b", coord.RoleJoining|coord.RoleDraining, 0); got != coord.BootstrapJoined {
		t.Fatalf("joiner reported %v", got)
	}
	if m, _ := b.View().Member("b"); m.Roles != coord.RoleJoining|coord.RoleDraining {
		t.Fatalf("joiner advertises roles %v, want Joining|Draining kept", m.Roles)
	}
	// The stance rode the row's FIRST appearance: the document already
	// carries it with no separate role write.
	d := readDoc(t, store)
	for _, r := range d.Members {
		if r.ID == "b" && coord.Role(r.Roles) != coord.RoleJoining|coord.RoleDraining {
			t.Fatalf("joiner's document row carries roles %d, want Joining|Draining", r.Roles)
		}
	}
}

// TestStart_ConcurrentRacersExactlyOneFounds pins the race the conditional
// write decides: any number of nodes starting against one empty store agree
// that exactly one founded, and nobody's row is lost.
func TestStart_ConcurrentRacersExactlyOneFounds(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	ids := []string{"a", "b", "c"}
	boots := make([]coord.Bootstrap, len(ids))
	cos := make([]*cas.Coordinator, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		co := cas.New(manualCfg(store, 5))
		cos[i] = co
		t.Cleanup(func() { _ = co.Close() })
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := co.Start(coord.Params{Self: coord.Node{ID: storageunit.NodeID(id), Addr: id + ":1"}})
			if err != nil {
				t.Errorf("Start(%s): %v", id, err)
				return
			}
			boots[i] = b
		}()
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	founded := 0
	for _, b := range boots {
		if b == coord.BootstrapFounded {
			founded++
		}
	}
	if founded != 1 {
		t.Fatalf("%d racers reported BootstrapFounded, want exactly 1 (%v)", founded, boots)
	}
	if got := docIDs(readDoc(t, store)); !sameIDs(got, ids) {
		t.Fatalf("document rows = %v, want %v: a CAS loser lost a row", got, ids)
	}
}

// TestStart_EmptyMembershipDocumentFounds pins the present-but-empty edge:
// a document whose members all left gracefully holds no incumbent to learn
// anything from, so starting into it is a FOUNDING (with the founder's
// Joining guard applied), not a join that would wait forever on a departed
// incumbent.
func TestStart_EmptyMembershipDocumentFounds(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	if _, err := store.PutIfAbsent(cas.DefaultKey, []byte(`{"members":[]}`)); err != nil {
		t.Fatalf("seeding empty document: %v", err)
	}
	a := cas.New(manualCfg(store, 5))
	if got := mustStart(t, a, "a", coord.RoleJoining, 0); got != coord.BootstrapFounded {
		t.Fatalf("starter into an empty membership reported %v, want BootstrapFounded", got)
	}
	if m, _ := a.View().Member("a"); m.Roles != 0 {
		t.Fatalf("founder into an empty membership advertises %v, want no roles", m.Roles)
	}
}

// TestBootstrap_AllZombieDocumentExpiresInExactlyKPolls pins the bounded
// zombie window after a full-cluster crash. The store outlives the cluster:
// SIGKILL/power-loss of every node leaves all rows in the document, so a
// restarting node finds only dead incumbents and reports BootstrapJoined
// (the documented degradation - the marker path, cluster.Config
// ConditionalStore wired to the SAME store, is what makes recovery bounded;
// see the package doc). What THIS adapter guarantees is that the window is
// structurally tight: bootstrap seeds the expiry baseline for every
// incumbent row at Start itself, so the zombies drop from the view after
// exactly ExpiryPolls observed polls - not ExpiryPolls polls after the
// first poll - the view collapses to self, and the node GCs the zombie
// rows out of the document.
func TestBootstrap_AllZombieDocumentExpiresInExactlyKPolls(t *testing.T) {
	const k = 3
	store := storageunit.NewMemConditionalStore()

	// Two nodes crash hard: their rows linger, nobody left to renew or reap.
	for _, id := range []string{"z1", "z2"} {
		z := cas.New(manualCfg(store, k))
		mustStart(t, z, id, 0, 0)
		if err := z.TestingShutdownNoLeave(); err != nil {
			t.Fatalf("hard-killing %s: %v", id, err)
		}
	}
	if got := docIDs(readDoc(t, store)); !sameIDs(got, []string{"z1", "z2"}) {
		t.Fatalf("crash left document rows %v, want [z1 z2]", got)
	}

	// The restarting node joins the all-zombie document (the degradation the
	// package doc documents: rows exist, so bootstrap cannot vouch nobody is
	// alive) and sees the zombies in its first view.
	n := cas.New(manualCfg(store, k))
	if got := mustStart(t, n, "n", 0, 0); got != coord.BootstrapJoined {
		t.Fatalf("starter into the zombie document reported %v, want BootstrapJoined", got)
	}
	if got := viewIDs(n.View()); !sameIDs(got, []string{"n", "z1", "z2"}) {
		t.Fatalf("first view = %v, want [n z1 z2]", got)
	}

	// The baselines were seeded AT START: k-1 polls are not enough...
	for i := 0; i < k-1; i++ {
		n.TestingPollOnce()
	}
	if got := viewIDs(n.View()); !sameIDs(got, []string{"n", "z1", "z2"}) {
		t.Fatalf("view after %d polls = %v, want zombies still present (expiry needs %d)", k-1, got, k)
	}

	// ...and the k-th poll drops both zombies: the view collapses to self and
	// the GC reaps the zombie rows from the document.
	n.TestingPollOnce()
	if got := viewIDs(n.View()); !sameIDs(got, []string{"n"}) {
		t.Fatalf("view after %d polls = %v, want [n] (zombie window must be exactly ExpiryPolls polls from Start)", k, got)
	}
	if got := docIDs(readDoc(t, store)); !sameIDs(got, []string{"n"}) {
		t.Fatalf("document rows after the zombie GC = %v, want [n]", got)
	}
}

// TestStart_Validation pins the fail-fast contract: a coordinator that
// cannot establish itself must error rather than come up partial.
func TestStart_Validation(t *testing.T) {
	if _, err := cas.New(cas.Config{}).Start(coord.Params{Self: coord.Node{ID: "a", Addr: "a:1"}}); err == nil || !strings.Contains(err.Error(), "Store") {
		t.Fatalf("Start without a store: err = %v, want a Config.Store error", err)
	}
	store := storageunit.NewMemConditionalStore()
	if _, err := cas.New(manualCfg(store, 5)).Start(coord.Params{Self: coord.Node{Addr: "a:1"}}); err == nil {
		t.Fatal("Start without an ID did not error")
	}
	if _, err := cas.New(manualCfg(store, 5)).Start(coord.Params{Self: coord.Node{ID: "a"}}); err == nil {
		t.Fatal("Start without an Addr did not error")
	}
	co := cas.New(manualCfg(store, 5))
	mustStart(t, co, "a", 0, 0)
	if _, err := co.Start(coord.Params{Self: coord.Node{ID: "a", Addr: "a:1"}}); err == nil {
		t.Fatal("second Start did not error")
	}
}

// TestDocument_WireFormat pins the document's JSON field names and that
// DeclaredUnitCount rides the member row (the reshard unanimity gate treats
// an unknown count as a veto, so silently dropping it would break
// declarative reshard under this adapter).
func TestDocument_WireFormat(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(store, 5))
	mustStart(t, a, "a", 0, 8)
	b := cas.New(manualCfg(store, 5))
	mustStart(t, b, "b", coord.RoleJoining, 16)

	d := readDoc(t, store)
	if !sameIDs(docIDs(d), []string{"a", "b"}) {
		t.Fatalf("document rows = %v, want [a b] (ID-sorted)", docIDs(d))
	}
	ra, rb := d.Members[0], d.Members[1]
	if ra.Addr != "a:1" || ra.Roles != 0 || ra.DeclaredUnitCount != 8 || ra.LeaseGen != 1 {
		t.Fatalf("founder row = %+v, want addr a:1, no roles, count 8, leaseGen 1", ra)
	}
	if rb.Addr != "b:1" || coord.Role(rb.Roles) != coord.RoleJoining || rb.DeclaredUnitCount != 16 || rb.LeaseGen != 1 {
		t.Fatalf("joiner row = %+v, want addr b:1, Joining, count 16, leaseGen 1", rb)
	}
	if ra.Inc == "" || rb.Inc == "" || ra.Inc == rb.Inc {
		t.Fatalf("incarnation nonces = %q / %q, want distinct non-empty values", ra.Inc, rb.Inc)
	}

	// And the facts surface through the port on the observer's next poll.
	a.TestingPollOnce()
	m, ok := a.View().Member("b")
	if !ok || m.DeclaredUnitCount != 16 || !m.Joining() {
		t.Fatalf("observed member b = %+v ok=%v, want declared count 16 and Joining", m, ok)
	}
}

// TestLeaseExpiry_DropsSilentMemberAndGCsItsRow is the crash path end to
// end, driven poll by poll with zero timing dependence: a member that stops
// renewing survives exactly ExpiryPolls unadvanced observed polls, then
// drops from the observer's view (self stays: a node never expires itself),
// the observer GCs its row, and a later renewal RE-ADDS the member (the
// rejoin self-heal).
func TestLeaseExpiry_DropsSilentMemberAndGCsItsRow(t *testing.T) {
	const k = 3
	store := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(store, k))
	mustStart(t, a, "a", 0, 0)
	b := cas.New(manualCfg(store, k))
	mustStart(t, b, "b", 0, 0)

	// Baseline: a's first observation of b's leaseGen.
	a.TestingPollOnce()
	if got := viewIDs(a.View()); !sameIDs(got, []string{"a", "b"}) {
		t.Fatalf("baseline view = %v, want [a b]", got)
	}

	// k-1 more silent polls: b is stale but not yet expired.
	for i := 0; i < k-1; i++ {
		a.TestingPollOnce()
	}
	if got := viewIDs(a.View()); !sameIDs(got, []string{"a", "b"}) {
		t.Fatalf("view after %d silent polls = %v, want [a b] still (expiry needs %d)", k-1, got, k)
	}

	// The k-th silent poll expires b. Note a itself never renewed either:
	// self is exempt from its own expiry judgment.
	a.TestingPollOnce()
	if got := viewIDs(a.View()); !sameIDs(got, []string{"a"}) {
		t.Fatalf("view after %d silent polls = %v, want [a]", k, got)
	}
	if !a.Populated() {
		t.Fatal("observer must stay populated after expiring a peer")
	}
	// One snapshot, one basis: the placement members drop b in the same swap.
	pm := a.PlacementMembers()
	if len(pm) != 1 || pm[0].ID != "a" {
		t.Fatalf("PlacementMembers after expiry = %+v, want [a]", pm)
	}
	// The expired row was GC'd out of the document.
	if got := docIDs(readDoc(t, store)); !sameIDs(got, []string{"a"}) {
		t.Fatalf("document rows after GC = %v, want [a]", got)
	}

	// Rejoin: b's next renewal re-adds its row, BETWEEN two of a's polls -
	// so a's tracker still holds the dead incarnation's counter, and the
	// fresh row restarts leaseGen at the very value a last tracked. The
	// incarnation nonce is what makes a re-admit b instead of reaping the
	// rejoiner forever (the ABA this test originally caught).
	if err := b.TestingRenewOnce(); err != nil {
		t.Fatalf("renewal after GC: %v", err)
	}
	a.TestingPollOnce()
	if got := viewIDs(a.View()); !sameIDs(got, []string{"a", "b"}) {
		t.Fatalf("view after b's rejoin renewal = %v, want [a b]", got)
	}
}

// TestObserve_PeerGCdSelfRowKeepsSelfInView pins the port's "View always
// contains Self once started" shape against the one document state that
// used to break it: a peer expired this node (an over-eager expiry after a
// stall) and GC'd its row, so this node's own poll reads a document WITHOUT
// its row. The published view must still contain Self - with the locally
// desired roles - because consumers compute their own position from it (a
// self-less view silently drops a Draining stance and releases every
// mounted unit). The renewal loop then re-inserts the row (self-heal).
func TestObserve_PeerGCdSelfRowKeepsSelfInView(t *testing.T) {
	const k = 2
	store := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(store, k))
	mustStart(t, a, "a", 0, 0)
	b := cas.New(manualCfg(store, k))
	mustStart(t, b, "b", 0, 0)

	// b is mid-drain: the stance a self-less view would silently lose.
	if err := b.SetRole(coord.RoleDraining); err != nil {
		t.Fatalf("SetRole: %v", err)
	}

	// a expires b (baseline + k silent polls; the role write does not move
	// b's leaseGen, so it does not reset a's count) and GCs b's row.
	a.TestingPollOnce()
	for i := 0; i < k; i++ {
		a.TestingPollOnce()
	}
	if got := docIDs(readDoc(t, store)); !sameIDs(got, []string{"a"}) {
		t.Fatalf("document rows after a's GC = %v, want [a] (b's row reaped)", got)
	}

	// b polls the document that lacks its own row: Self must stay in the
	// view, carrying the locally-known Draining stance, and the placement
	// basis must keep self too (one snapshot, one basis).
	b.TestingPollOnce()
	if got := viewIDs(b.View()); !sameIDs(got, []string{"a", "b"}) {
		t.Fatalf("b's view after its row was GC'd = %v, want [a b]: View must always contain Self", got)
	}
	m, ok := b.View().Member("b")
	if !ok || !m.Draining() {
		t.Fatalf("synthesized self = %+v ok=%v, want present with the locally-desired Draining stance", m, ok)
	}
	if _, d := b.TransitionSets(); len(d) != 1 {
		t.Fatalf("b's draining set with a GC'd self row = %v, want {b}: the drain stance must not silently vanish", d)
	}
	foundSelf := false
	for _, n := range b.PlacementMembers() {
		if n.ID == "b" {
			foundSelf = true
		}
	}
	if !foundSelf {
		t.Fatalf("placement members %v lost self", b.PlacementMembers())
	}

	// The renewal re-inserts the document row, roles included: the self-heal
	// path back into the membership.
	if err := b.TestingRenewOnce(); err != nil {
		t.Fatalf("renewal after the GC: %v", err)
	}
	d := readDoc(t, store)
	if got := docIDs(d); !sameIDs(got, []string{"a", "b"}) {
		t.Fatalf("document rows after b's renewal = %v, want [a b]: the renewal must re-insert self", got)
	}
	for _, r := range d.Members {
		if r.ID == "b" && coord.Role(r.Roles) != coord.RoleDraining {
			t.Fatalf("re-inserted row roles = %d, want Draining carried from the local record", r.Roles)
		}
	}
}

// TestLeaseExpiry_RenewalResetsTheCounter pins the observer-side judgment:
// expiry counts CONSECUTIVE unadvanced polls, so a renewal observed at any
// point restarts the count and a member renewing slower than the poll (but
// inside the expiry budget) is never dropped.
func TestLeaseExpiry_RenewalResetsTheCounter(t *testing.T) {
	const k = 2
	store := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(store, k))
	mustStart(t, a, "a", 0, 0)
	b := cas.New(manualCfg(store, k))
	mustStart(t, b, "b", 0, 0)

	a.TestingPollOnce() // baseline
	for cycle := 0; cycle < 5; cycle++ {
		a.TestingPollOnce() // one unadvanced poll: stale but under k
		if err := b.TestingRenewOnce(); err != nil {
			t.Fatalf("cycle %d renew: %v", cycle, err)
		}
		a.TestingPollOnce() // observes the advance: count restarts
		if got := viewIDs(a.View()); !sameIDs(got, []string{"a", "b"}) {
			t.Fatalf("cycle %d: view = %v, want [a b] (renewal must reset the count)", cycle, got)
		}
	}
}

// hookStore wraps a ConditionalStore and runs an armed hook ONCE, just
// before the next CompareAndSet passes through: the seam for injecting a
// genuine conflicting write into another writer's read-modify-write window.
type hookStore struct {
	storageunit.ConditionalStore
	mu   sync.Mutex
	hook func()
}

func (h *hookStore) arm(f func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hook = f
}

func (h *hookStore) CompareAndSet(key string, data []byte, expectedVersion string) (string, error) {
	h.mu.Lock()
	hook := h.hook
	h.hook = nil
	h.mu.Unlock()
	if hook != nil {
		hook()
	}
	return h.ConditionalStore.CompareAndSet(key, data, expectedVersion)
}

// TestRenew_RetriesAfterCASConflict pins the read-modify-write loop's whole
// point: a renewal that loses the CAS race re-reads the winner's document
// and re-applies its edit, so BOTH edits land - the conflicting role write
// is not clobbered and the renewal still advances the lease.
func TestRenew_RetriesAfterCASConflict(t *testing.T) {
	mem := storageunit.NewMemConditionalStore()
	hooked := &hookStore{ConditionalStore: mem}

	a := cas.New(manualCfg(hooked, 5)) // a's writes go through the hook
	mustStart(t, a, "a", 0, 0)
	b := cas.New(manualCfg(mem, 5)) // b's writes bypass it
	mustStart(t, b, "b", 0, 0)

	// Inside a's renewal window (after its read, before its CAS), b lands a
	// real conflicting write: a's first CAS must fail on the moved version.
	hooked.arm(func() {
		if err := b.SetRole(coord.RoleDraining); err != nil {
			t.Errorf("conflicting SetRole: %v", err)
		}
	})
	if err := a.TestingRenewOnce(); err != nil {
		t.Fatalf("renewal with an injected conflict must retry and succeed, got %v", err)
	}

	d := readDoc(t, mem)
	for _, r := range d.Members {
		switch r.ID {
		case "a":
			if r.LeaseGen != 2 {
				t.Fatalf("a's leaseGen = %d, want 2 (the retried renewal landed)", r.LeaseGen)
			}
		case "b":
			if coord.Role(r.Roles) != coord.RoleDraining {
				t.Fatalf("b's roles = %d, want Draining: the retry lost the winner's edit", r.Roles)
			}
		}
	}
}

// TestGC_ConcurrentObserversResolveByCAS pins the racing-GC claim: two live
// observers judge the same silent member expired on the same tick and both
// attempt the removal concurrently; CAS lets exactly one win, the loser
// re-reads a document that is already correct, and nobody's live row is
// harmed.
func TestGC_ConcurrentObserversResolveByCAS(t *testing.T) {
	const k = 2
	st := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(st, k))
	mustStart(t, a, "a", 0, 0)
	b := cas.New(manualCfg(st, k))
	mustStart(t, b, "b", 0, 0)
	c := cas.New(manualCfg(st, k))
	mustStart(t, c, "c", 0, 0)

	// Baselines, then one live round: a and b keep renewing, c is silent.
	a.TestingPollOnce()
	b.TestingPollOnce()
	for round := 0; round < k-1; round++ {
		if err := a.TestingRenewOnce(); err != nil {
			t.Fatal(err)
		}
		if err := b.TestingRenewOnce(); err != nil {
			t.Fatal(err)
		}
		a.TestingPollOnce()
		b.TestingPollOnce()
	}
	if err := a.TestingRenewOnce(); err != nil {
		t.Fatal(err)
	}
	if err := b.TestingRenewOnce(); err != nil {
		t.Fatal(err)
	}

	// The expiring polls, concurrently: both observers cross c's k-th
	// unadvanced observation on this tick and race the GC write.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a.TestingPollOnce() }()
	go func() { defer wg.Done(); b.TestingPollOnce() }()
	wg.Wait()

	// One settling round for whichever observer lost the race.
	if err := a.TestingRenewOnce(); err != nil {
		t.Fatal(err)
	}
	if err := b.TestingRenewOnce(); err != nil {
		t.Fatal(err)
	}
	a.TestingPollOnce()
	b.TestingPollOnce()

	if got := docIDs(readDoc(t, st)); !sameIDs(got, []string{"a", "b"}) {
		t.Fatalf("document rows after racing GCs = %v, want [a b]", got)
	}
	if got := viewIDs(a.View()); !sameIDs(got, []string{"a", "b"}) {
		t.Fatalf("a's view = %v, want [a b]", got)
	}
	if got := viewIDs(b.View()); !sameIDs(got, []string{"a", "b"}) {
		t.Fatalf("b's view = %v, want [a b]", got)
	}
}

// TestClose_RemovesSelfFromDocument pins the graceful fast path: Close
// CAS-removes the member's own row, so peers drop it on their next poll
// with no expiry wait.
func TestClose_RemovesSelfFromDocument(t *testing.T) {
	st := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(st, 5))
	mustStart(t, a, "a", 0, 0)
	b := cas.New(manualCfg(st, 5))
	mustStart(t, b, "b", 0, 0)

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := docIDs(readDoc(t, st)); !sameIDs(got, []string{"a"}) {
		t.Fatalf("document rows after b's graceful Close = %v, want [a]", got)
	}
	a.TestingPollOnce()
	if got := viewIDs(a.View()); !sameIDs(got, []string{"a"}) {
		t.Fatalf("a's view after b's graceful Close = %v, want [a]", got)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := docIDs(readDoc(t, st)); len(got) != 0 {
		t.Fatalf("document rows after the last member's Close = %v, want none", got)
	}
}

// TestClose_IsIdempotentAndSafeUnstarted pins the harness-adjacent
// lifecycle bar: Close never panics or errors, started or not, once or
// twice.
func TestClose_IsIdempotentAndSafeUnstarted(t *testing.T) {
	un := cas.New(cas.Config{})
	if err := un.Close(); err != nil {
		t.Fatalf("unstarted Close: %v", err)
	}
	if err := un.Close(); err != nil {
		t.Fatalf("second unstarted Close: %v", err)
	}

	st := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(st, 5))
	mustStart(t, a, "a", 0, 0)
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestHardShutdown_LeavesRowForExpiry pins the SIGKILL model: a hard
// shutdown stops the loops but leaves the row in the document (even through
// a subsequent Close), so peers must reap it via lease expiry - the exact
// path TestLeaseExpiry_DropsSilentMemberAndGCsItsRow exercises.
func TestHardShutdown_LeavesRowForExpiry(t *testing.T) {
	st := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(st, 5))
	mustStart(t, a, "a", 0, 0)
	b := cas.New(manualCfg(st, 5))
	mustStart(t, b, "b", 0, 0)

	if err := b.TestingShutdownNoLeave(); err != nil {
		t.Fatalf("TestingShutdownNoLeave: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close after hard shutdown: %v", err)
	}
	if got := docIDs(readDoc(t, st)); !sameIDs(got, []string{"a", "b"}) {
		t.Fatalf("document rows after hard kill = %v, want [a b]: the row must linger for expiry", got)
	}
}

// TestSetRole_VisibleAfterPoll_NotBefore pins role delivery to the poll
// cadence, both directions: the CAS write lands in the document
// immediately, but no query method - the writer's own included - serves it
// until a poll refreshes that node's snapshot. Roles ride the poll, never
// the hint.
func TestSetRole_VisibleAfterPoll_NotBefore(t *testing.T) {
	st := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(st, 5))
	mustStart(t, a, "a", 0, 0)
	b := cas.New(manualCfg(st, 5))
	mustStart(t, b, "b", 0, 0)
	a.TestingPollOnce() // a absorbs b

	if err := b.SetRole(coord.RoleDraining); err != nil {
		t.Fatalf("SetRole: %v", err)
	}
	// In the document at once...
	for _, r := range readDoc(t, st).Members {
		if r.ID == "b" && coord.Role(r.Roles) != coord.RoleDraining {
			t.Fatalf("document row b roles = %d, want Draining immediately after SetRole", r.Roles)
		}
	}
	// ...but visible nowhere until a poll: not to the observer, not to the
	// writer itself.
	if j, d := a.TransitionSets(); j != nil || d != nil {
		t.Fatalf("observer served the flip before its poll: joining=%v draining=%v", j, d)
	}
	if j, d := b.TransitionSets(); j != nil || d != nil {
		t.Fatalf("writer served its own flip before its poll: joining=%v draining=%v", j, d)
	}

	a.TestingPollOnce()
	b.TestingPollOnce()
	if _, d := a.TransitionSets(); len(d) != 1 {
		t.Fatalf("observer's draining set after poll = %v, want {b}", d)
	}
	if m, _ := b.View().Member("b"); !m.Draining() {
		t.Fatalf("writer's own view after poll = %+v, want Draining", m)
	}

	// Retraction takes the same path back to the nil steady state.
	if err := b.SetRole(0); err != nil {
		t.Fatalf("SetRole(0): %v", err)
	}
	a.TestingPollOnce()
	b.TestingPollOnce()
	if j, d := a.TransitionSets(); j != nil || d != nil {
		t.Fatalf("retraction did not restore nil sets: joining=%v draining=%v", j, d)
	}
}

// TestSetRole_DedupsNoOpWrite pins the declarative-idempotent clause: a
// SetRole re-stating the advertised set must not churn the document (every
// churned write is a CAS conflict some peer must retry through).
func TestSetRole_DedupsNoOpWrite(t *testing.T) {
	st := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(st, 5))
	mustStart(t, a, "a", 0, 0)

	if err := a.SetRole(coord.RoleDraining); err != nil {
		t.Fatalf("SetRole: %v", err)
	}
	ver := docVersion(t, st)
	if err := a.SetRole(coord.RoleDraining); err != nil {
		t.Fatalf("repeat SetRole: %v", err)
	}
	if got := docVersion(t, st); got != ver {
		t.Fatalf("a no-op SetRole moved the document version %s -> %s", ver, got)
	}
	if err := a.SetRole(0); err != nil {
		t.Fatalf("SetRole(0): %v", err)
	}
	if got := docVersion(t, st); got == ver {
		t.Fatal("a genuine role change did not write the document")
	}
}

// TestSetRole_ConcurrentSettersConvergeOnLastDesired pins the single-source
// -of-truth contract for roles: the LOCAL DESIRED set is authoritative, and
// the document converges to it no matter how concurrent SetRoles interleave.
//
// The deterministic interleaving (via hookStore): inside SetRole(Joining)'s
// CAS window - after its edit closure ran, before its write lands - a LATER
// SetRole(Draining) completes fully. The first writer's CAS fails on the
// moved version and retries; the retried closure must re-read the CURRENT
// desired set (Draining), not replay its captured argument. Replaying the
// argument ends with doc=Joining while the local record says Draining, and
// because dedup consults the local record, a later SetRole(Draining) is
// swallowed as a no-op: the wrong advertised role sticks until an unrelated
// role change.
func TestSetRole_ConcurrentSettersConvergeOnLastDesired(t *testing.T) {
	mem := storageunit.NewMemConditionalStore()
	hooked := &hookStore{ConditionalStore: mem}
	a := cas.New(manualCfg(hooked, 5))
	mustStart(t, a, "a", 0, 0)

	hooked.arm(func() {
		if err := a.SetRole(coord.RoleDraining); err != nil {
			t.Errorf("inner SetRole(Draining): %v", err)
		}
	})
	if err := a.SetRole(coord.RoleJoining); err != nil {
		t.Fatalf("outer SetRole(Joining): %v", err)
	}

	// The document must hold the LAST DESIRED state (Draining), not
	// whichever mutate happened to land last.
	for _, r := range readDoc(t, mem).Members {
		if r.ID == "a" && coord.Role(r.Roles) != coord.RoleDraining {
			t.Fatalf("document roles after the interleaved setters = %d, want Draining: the doc diverged from the desired record", r.Roles)
		}
	}

	// And re-stating the desired set stays a genuine no-op (dedup compares
	// desired-vs-desired, and doc == desired, so nothing needs to move).
	ver := docVersion(t, mem)
	if err := a.SetRole(coord.RoleDraining); err != nil {
		t.Fatalf("re-stating SetRole(Draining): %v", err)
	}
	if got := docVersion(t, mem); got != ver {
		t.Fatalf("re-stating the converged role moved the document %s -> %s", ver, got)
	}

	// The writer's own view serves the desired stance after its poll.
	a.TestingPollOnce()
	if m, _ := a.View().Member("a"); !m.Draining() || m.Joining() {
		t.Fatalf("view member = %+v, want Draining only", m)
	}
}

// TestRenew_HealsDocRoleDrift pins the renewal loop's drift heal: when
// self's document row advertises roles that differ from the locally desired
// set (any lost interleaving this adapter has not imagined yet), the next
// renewal repairs the row instead of blindly incrementing the counter next
// to wrong roles.
func TestRenew_HealsDocRoleDrift(t *testing.T) {
	store := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(store, 5))
	mustStart(t, a, "a", 0, 0)
	if err := a.SetRole(coord.RoleDraining); err != nil {
		t.Fatalf("SetRole: %v", err)
	}

	// Corrupt the row behind the adapter's back: flip roles to steady state.
	data, ver, err := store.Get(cas.DefaultKey)
	if err != nil {
		t.Fatalf("reading document: %v", err)
	}
	var d wireDoc
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatalf("decoding document: %v", err)
	}
	for i := range d.Members {
		if d.Members[i].ID == "a" {
			d.Members[i].Roles = 0
		}
	}
	out, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("encoding corrupted document: %v", err)
	}
	if _, err := store.CompareAndSet(cas.DefaultKey, out, ver); err != nil {
		t.Fatalf("planting the drift: %v", err)
	}

	// One renewal heals the drift back to the desired record.
	if err := a.TestingRenewOnce(); err != nil {
		t.Fatalf("renewal: %v", err)
	}
	for _, r := range readDoc(t, store).Members {
		if r.ID == "a" && coord.Role(r.Roles) != coord.RoleDraining {
			t.Fatalf("row roles after the renewal = %d, want Draining: the renewal must repair drift", r.Roles)
		}
	}
}

// TestChanged_FiresOnObservedChangeAndCoalesces pins the hint semantics: a
// poll that observes nothing new fires nothing, a poll that observes ONLY a
// lease renewal fires nothing, polls that observe genuine VIEW changes fire,
// and undelivered hints coalesce to depth one - lossy by contract.
//
// The renewal clause is a regression pin, not a nicety. A renewal moves the
// document version on every renewal interval while leaving the view
// unchanged, so a hint keyed on version movement fires on virtually every
// poll. The storage layer debounces view changes with extend-on-burst
// semantics (the settle timer runs one settle delay after the LAST hint), so
// a hint stream at poll cadence re-arms that debounce forever: the reconcile
// never runs and rebalance-idle is never reached - on the production knobs
// (poll 1s, renewal 2s, settle 5s) as surely as on test knobs. The
// integration tree's CAS lifecycle test is where that starvation actually
// bit; this pins it at the adapter boundary.
func TestChanged_FiresOnObservedChangeAndCoalesces(t *testing.T) {
	st := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(st, 5))
	mustStart(t, a, "a", 0, 0)
	b := cas.New(manualCfg(st, 5))
	mustStart(t, b, "b", 0, 0)

	a.TestingPollOnce() // sync a with b's join
	drain(a.Changed())

	// Quiet store: polls observe nothing, no hints.
	a.TestingPollOnce()
	a.TestingPollOnce()
	select {
	case <-a.Changed():
		t.Fatal("a poll that observed no change fired a hint")
	default:
	}

	// A renewal is the mechanism's heartbeat, not a view change: the version
	// moved, the snapshot did not, so the poll must fire NOTHING.
	if err := b.TestingRenewOnce(); err != nil {
		t.Fatal(err)
	}
	a.TestingPollOnce()
	select {
	case <-a.Changed():
		t.Fatal("a poll that observed only a lease renewal fired a hint; a renewal-cadence hint stream starves the consumer's change debounce")
	default:
	}

	// Two observed VIEW changes, no consumer in between: exactly one pending.
	if err := b.SetRole(coord.RoleDraining); err != nil {
		t.Fatal(err)
	}
	a.TestingPollOnce()
	if err := b.SetRole(0); err != nil {
		t.Fatal(err)
	}
	a.TestingPollOnce()
	select {
	case <-a.Changed():
	default:
		t.Fatal("observed view changes fired no hint")
	}
	select {
	case <-a.Changed():
		t.Fatal("hints did not coalesce: two pending after two observed changes")
	default:
	}
}

func drain(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// TestGeneration_IsUnsupported pins the split the spec keeps: generation
// agreement stays with the cluster-side arbiter, so this adapter holds no
// opinion and refuses proposals.
func TestGeneration_IsUnsupported(t *testing.T) {
	st := storageunit.NewMemConditionalStore()
	a := cas.New(manualCfg(st, 5))
	mustStart(t, a, "a", 0, 0)
	if g := a.Generation(); g != 0 {
		t.Fatalf("Generation() = %d, want 0", g)
	}
	if err := a.ProposeGeneration(3); !errors.Is(err, coord.ErrGenerationUnsupported) {
		t.Fatalf("ProposeGeneration = %v, want ErrGenerationUnsupported", err)
	}
}
