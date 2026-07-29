// Package gossip implements shale's coordination port (pkg/coord) over SWIM
// gossip plus a bounded-load consistent hash ring.
//
// It is the adapter that owns EVERYTHING the port deliberately refuses to
// name: the memberlist transport, seed bootstrap, the Meta payload the roles
// and declared unit count ride in, the ring, and the two loops that keep the
// ring in step with gossip (an event-driven one and a periodic reconcile that
// heals a dropped event). None of that is visible through coord.Coordinator,
// which is what makes a second adapter - a lease/CAS coordinator over the same
// conditional store shale already uses for reshard agreement - a drop-in.
//
// TWO BASES, SPLIT BY QUESTION. Placement (Locate) runs off the RING; the
// View and every stance query (roles, TransitionSets, declared counts) run
// off the LIVE membership snapshot, presence and roles from the same rows.
// The ring trails the snapshot by one event-loop hop and heals a dropped
// event only on the reconcile cadence - tolerable for placement (a node the
// ring does not place cannot be in any replica set), but never for stance:
// serving membership questions from the ring is how an unknown-stance
// phantom once read as an established peer. See View for the full argument.
//
// ADVISORY, NOT AUTHORITATIVE. Nothing here fences anything. A stale or
// split-brained ring costs availability (ops route to a node that refuses them
// with a retryable error) and never correctness; the durable store's own
// higher-epoch-open fencing is what makes two nodes disagreeing about an owner
// safe.
package gossip

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/membership"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// DefaultReconcileInterval is how often the adapter re-syncs the ring against
// the authoritative membership snapshot, healing any join/leave the event
// channel dropped under backpressure. Small enough that a dropped event
// recovers within seconds, large enough to be free in steady state.
const DefaultReconcileInterval = 5 * time.Second

// Config holds the GOSSIP-ONLY knobs: how this node joins the mesh and how
// loudly it talks. Everything the storage layer knows about itself (identity,
// dial address, declared unit count, initial roles) arrives at Start via
// coord.Params instead, so this struct never duplicates cluster config.
type Config struct {
	// BindAddr is the host:port memberlist listens on (UDP + TCP). Required.
	// The host may be empty for "all interfaces".
	BindAddr string

	// Seeds are the BindAddrs of already-running peers to contact at startup.
	// Empty means this node is the seed (a founder).
	Seeds []string

	// LogOutput is where memberlist's internal logger writes. nil means
	// memberlist's default (stderr); tests pass io.Discard.
	LogOutput io.Writer

	// ReconcileInterval overrides DefaultReconcileInterval. Tests park it far
	// in the future to stop a background tick from healing a divergence they
	// induced on purpose.
	ReconcileInterval time.Duration

	// RejoinInterval and MetaRefreshInterval override the membership layer's
	// production defaults. Zero keeps the default (which is what production
	// wants); a test that must not have a background re-Join or Meta
	// re-broadcast sets them negative to disable.
	RejoinInterval      time.Duration
	MetaRefreshInterval time.Duration
}

// Coordinator is the gossip + consistent-hash implementation of
// coord.Coordinator.
type Coordinator struct {
	cfg Config

	// self is the identity published at Start. Written once, before any
	// goroutine exists, then read-only.
	self coord.Node

	// mem is the gossip transport. Nil until Start; the field is written once
	// under startMu before any loop spawns, then read-only.
	mem *membership.Membership

	// r is the placement ring. Created in Start before any goroutine spawns
	// and never reassigned - every mutation goes through the ring's own
	// internal lock, so a concurrent reader of this field is race-free.
	r *ring.Ring

	// roles is this node's currently advertised role set, so SetRole can dedup
	// a no-op write without a round trip through the Meta encoder.
	rolesMu sync.Mutex
	roles   coord.Role

	// epoch is the local monotone view counter. Bumped on every observed
	// change (an event, or a reconcile that moved the ring).
	epoch atomic.Uint64

	// changed is the coalescing change hint. Capacity 1: a send that would
	// block is dropped, because the contract is a hint, not a queue.
	changed chan struct{}

	closeCh   chan struct{}
	closeOnce sync.Once
	loopWG    sync.WaitGroup

	startMu sync.Mutex
	started bool
	// static marks a Coordinator built by NewStatic: a fixed member set, no
	// transport, no loops. Written before the value escapes, then read-only.
	static bool

	// reduced memoizes the last hypothetical-placement ring (see Locate). A
	// reconcile pass asks for the SAME exclusion across every unit it owns, and
	// rebuilding the ring per unit is the expensive part (partitions x virtual
	// nodes), so a one-entry memo collapses that to one build per pass. Keyed by
	// the exclusion set AND the view epoch, so any membership change discards it.
	reducedMu    sync.Mutex
	reducedKey   string
	reducedEpoch uint64
	reduced      *ring.Ring

	// facts carries the per-member roles + declared unit counts a STATIC
	// coordinator reports, since it has no gossip cache to read them from.
	// Nil (and unused) on a real gossip coordinator. Guarded by factsMu.
	factsMu sync.Mutex
	facts   map[storageunit.NodeID]coord.Member
	closed  atomic.Bool
}

// New returns an unstarted Coordinator. Nothing binds, joins or allocates a
// ring until Start; the operator constructs the value, the storage layer
// starts and closes it.
func New(cfg Config) *Coordinator {
	return &Coordinator{
		cfg:     cfg,
		changed: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
}

// Compile-time proof the adapter satisfies the port and its optional test
// seams. A second coordinator has exactly this bar to clear.
var (
	_ coord.Coordinator    = (*Coordinator)(nil)
	_ coord.HardShutdowner = (*Coordinator)(nil)
	_ coord.MemberSetter   = (*Coordinator)(nil)
)

// Start brings up memberlist, joins the seeds, seeds the ring with everything
// gossip already knows, and starts the event + reconcile loops. It is called
// once, by the storage layer.
func (c *Coordinator) Start(p coord.Params) (coord.Bootstrap, error) {
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.static {
		// A static coordinator has nothing to bootstrap: its membership was
		// fixed at construction. Adopt the identity and report a founding.
		c.self = p.Self
		c.roles = p.InitialRoles &^ coord.RoleJoining
		return coord.BootstrapFounded, nil
	}
	if c.started {
		return 0, errors.New("gossip: Start called twice")
	}
	if c.cfg.BindAddr == "" {
		return 0, errors.New("gossip: BindAddr is required")
	}
	if p.Self.ID == "" {
		return 0, errors.New("gossip: Params.Self.ID is required")
	}
	if p.Self.Addr == "" {
		return 0, errors.New("gossip: Params.Self.Addr is required (peers dial it)")
	}

	// A node that FOUNDS the cluster must not advertise the Joining role:
	// there is no incumbent serving the positions it is about to mount, so
	// "warming against a live holder" describes nothing, and a founder that
	// advertised it would exclude itself from its own CURRENT set with nobody
	// left to be current.
	//
	// Whether this node founds is a DISCOVERY question, not a config one, and
	// it is only answered by the startup Join below (a node with seeds
	// configured but nobody up yet - the first pod of a homogeneous solo-start
	// boot - FOUNDS despite its config). The Meta payload has to be chosen
	// before that answer exists, so seeds>0 tentatively advertises Joining
	// and the solo-found case corrects itself right after Open: safe, because
	// a node that contacted nobody has no peer that could have observed the
	// tentative stance.
	bootstrap := coord.BootstrapFounded
	roles := p.InitialRoles &^ coord.RoleJoining
	if len(c.cfg.Seeds) > 0 {
		bootstrap = coord.BootstrapJoined
		roles = p.InitialRoles
	}

	mem, err := membership.Open(membership.Config{
		NodeID:   string(p.Self.ID),
		BindAddr: c.cfg.BindAddr,
		GRPCAddr: p.Self.Addr,
		Seeds:    c.cfg.Seeds,
		// The role set must ride in the VERY FIRST Meta payload, atomically
		// with this node's presence: a peer that learns "member" before
		// "warming" clean-cuts the position the newcomer displaces instead of
		// holding and draining it.
		Joining:             roles.Has(coord.RoleJoining),
		DeclaredUnitCount:   p.DeclaredUnitCount,
		LogOutput:           c.cfg.LogOutput,
		RejoinInterval:      intervalOr(c.cfg.RejoinInterval, membership.DefaultRejoinInterval),
		MetaRefreshInterval: intervalOr(c.cfg.MetaRefreshInterval, membership.DefaultMetaRefreshInterval),
		AllowSoloStart:      p.SoloStart,
	})
	if err != nil {
		return 0, fmt.Errorf("gossip: membership: %w", err)
	}

	// The discovery correction: seeds were configured but the startup Join
	// reached NOBODY (the solo-start first pod). This node FOUNDED the
	// cluster, whatever its config says, so report that - and retract the
	// tentatively-advertised Joining role, which described warming against an
	// incumbent that does not exist. No peer can have observed the tentative
	// stance (there were no peers), so the retraction is unobservable.
	if bootstrap == coord.BootstrapJoined && mem.ContactedAtOpen() == 0 {
		bootstrap = coord.BootstrapFounded
		roles = roles &^ coord.RoleJoining
		if err := mem.SetJoining(false); err != nil {
			_ = mem.Close()
			return 0, fmt.Errorf("gossip: retracting solo-found Joining role: %w", err)
		}
	}

	c.self = p.Self
	c.mem = mem
	c.r = ring.New()
	c.roles = roles

	// Seed the ring with whatever gossip knows right now: always at least this
	// node, plus any peer the seed Join already returned.
	for _, m := range mem.Members() {
		c.r.Add(ring.Member{ID: m.ID, Addr: m.Addr})
	}

	c.started = true
	c.loopWG.Add(2)
	go c.runEventsLoop()
	go c.runReconcileLoop()
	return bootstrap, nil
}

// NewStatic returns a STARTED Coordinator with a fixed member set and NO
// transport: the same placement math, nothing to gossip, nothing to reconcile.
//
// It exists for in-process callers that need deterministic placement without
// standing up a mesh - white-box tests being the main one. Roles are whatever
// SetRole records locally (no peer ever sees them), Changed never fires, and
// Start is a no-op that only adopts the identity.
func NewStatic(self coord.Node, members []coord.Node) *Coordinator {
	c := New(Config{})
	c.self = self
	c.static = true
	c.started = true
	c.r = ring.New()
	for _, m := range members {
		c.r.Add(ring.Member{ID: string(m.ID), Addr: m.Addr})
	}
	return c
}

// intervalOr returns def when v is zero, and disables (returns 0) when v is
// negative. A caller that genuinely wants a loop off passes a negative value;
// zero means "I have no opinion, use the production default".
func intervalOr(v, def time.Duration) time.Duration {
	switch {
	case v == 0:
		return def
	case v < 0:
		return 0
	default:
		return v
	}
}

// runEventsLoop mirrors gossip join/leave into the ring. It signals a change
// hint on EVERY event, including one that moves nothing (a Meta re-broadcast
// with an unchanged address): the storage layer's response to a hint is an
// idempotent reconcile, and a no-op event is exactly the signal that some
// member's ROLE may have flipped without its address moving.
func (c *Coordinator) runEventsLoop() {
	defer c.loopWG.Done()
	events := c.mem.Events()
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return
			}
			switch ev.Type {
			case membership.EventJoin:
				// A DRAINING member STAYS in the ring: it is still a current
				// owner and still serves. The current/pending split is
				// computed per-op by the storage layer against Placement, not
				// baked into membership here.
				c.r.Add(ring.Member{ID: ev.Member.ID, Addr: ev.Member.Addr})
			case membership.EventLeave:
				c.r.Remove(ev.Member.ID)
			}
			c.epoch.Add(1)
			c.signal()
		case <-c.closeCh:
			return
		}
	}
}

// runReconcileLoop re-syncs the ring against the authoritative membership
// snapshot on a fixed cadence. The event channel drops on backpressure by
// design (it must never block memberlist's callback goroutine), so without
// this a single dropped join would leave the ring permanently short a member.
func (c *Coordinator) runReconcileLoop() {
	defer c.loopWG.Done()
	t := time.NewTicker(intervalOr(c.cfg.ReconcileInterval, DefaultReconcileInterval))
	defer t.Stop()
	for {
		select {
		case <-c.closeCh:
			return
		case <-t.C:
			if c.reconcileRing() {
				c.epoch.Add(1)
				c.signal()
			}
		}
	}
}

// reconcileRing applies any add/remove the event channel missed and reports
// whether the ring actually moved. Idempotent: re-adding a present member with
// the same address is skipped, and an absent non-member is already correct.
//
// A member whose ADDRESS changed counts as a change, so the storage layer gets
// a hint and can evict the cached client pointed at the stale endpoint.
func (c *Coordinator) reconcileRing() bool {
	if c.mem == nil || c.r == nil {
		return false
	}
	want := make(map[string]ring.Member)
	for _, m := range c.mem.Snapshot() {
		want[m.ID] = ring.Member{ID: m.ID, Addr: m.Addr}
	}
	changed := false
	have := make(map[string]string, len(want))
	for _, m := range c.r.Members() {
		have[m.ID] = m.Addr
	}
	for id, m := range want {
		if addr, ok := have[id]; !ok || addr != m.Addr {
			c.r.Add(m)
			changed = true
		}
	}
	for id := range have {
		if _, ok := want[id]; !ok {
			c.r.Remove(id)
			changed = true
		}
	}
	return changed
}

// signal delivers a coalescing change hint. A full channel means an
// undelivered hint is already pending, so dropping this one loses nothing: the
// receiver re-reads the whole view either way.
func (c *Coordinator) signal() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

// Changed returns the coalescing change-hint channel. See coord.Coordinator.
func (c *Coordinator) Changed() <-chan struct{} { return c.changed }

// View returns the current membership snapshot: every member gossip currently
// knows, presence and roles read from the SAME snapshot rows.
//
// THE BASIS IS MEMBERSHIP, NOT THE RING. The ring (Locate's basis) trails the
// snapshot by one event-loop hop and heals a backpressure-dropped event only
// on the reconcile cadence - tolerable for placement (a node the ring does not
// place cannot be in any replica set), but wrong for every stance question the
// storage layer asks of View: a ring member with no snapshot row would surface
// with ZERO roles, so an unknown-stance phantom would read as an ESTABLISHED
// peer to the boot-defer gate, and a snapshot member the ring has not absorbed
// would be invisible to the reshard unanimity guard it should be vetoing.
// Reading presence and roles from one snapshot makes both divergences
// structurally impossible: a member is either in the view with its true
// gossiped stance, or not in it at all.
func (c *Coordinator) View() coord.View {
	v := coord.View{Self: c.self, Epoch: c.epoch.Load()}
	if c.mem == nil {
		// Static mode: the fixed ring IS the member truth; roles come from
		// the facts override.
		if c.r == nil {
			return v
		}
		placed := c.r.Members()
		if len(placed) == 0 {
			return v
		}
		facts := c.memberFacts()
		v.Members = make([]coord.Member, 0, len(placed))
		for _, p := range placed {
			id := storageunit.NodeID(p.ID)
			m := coord.Member{Node: coord.Node{ID: id, Addr: p.Addr}}
			if f, ok := facts[id]; ok {
				m.Roles = f.Roles
				m.DeclaredUnitCount = f.DeclaredUnitCount
			}
			v.Members = append(v.Members, m)
		}
		sort.Slice(v.Members, func(i, j int) bool { return v.Members[i].ID < v.Members[j].ID })
		return v
	}
	snap := c.mem.Snapshot()
	if len(snap) == 0 {
		return v
	}
	v.Members = make([]coord.Member, 0, len(snap))
	for _, m := range snap {
		var roles coord.Role
		if m.Joining {
			roles |= coord.RoleJoining
		}
		if m.Draining {
			roles |= coord.RoleDraining
		}
		v.Members = append(v.Members, coord.Member{
			Node:              coord.Node{ID: storageunit.NodeID(m.ID), Addr: m.Addr},
			Roles:             roles,
			DeclaredUnitCount: m.DeclaredUnitCount,
		})
	}
	sort.Slice(v.Members, func(i, j int) bool { return v.Members[i].ID < v.Members[j].ID })
	return v
}

// Populated reports whether the view has at least one member. See
// coord.Coordinator: this is the per-operation routing predicate, so it reads
// the ring directly instead of paying View's snapshot construction. The ring
// is a different BASIS than the (snapshot-based) View, but the boolean is
// equivalent at all times: before Start both are empty, and after Start both
// always contain at least this node (Start seeds the ring with self; the
// snapshot upserts self at Open). The ring is internally synchronized.
func (c *Coordinator) Populated() bool {
	return c.r != nil && !c.r.Empty()
}

// TransitionSets returns the joining / draining member-ID sets. See
// coord.Coordinator: per-operation, so it does ONE membership scan and builds
// only the (steady-state nil) result maps - none of View's snapshot
// construction. Liveness matches View exactly: both read the same live
// membership snapshot, so a role flip is visible here the moment gossip
// delivers it, hint or no hint.
func (c *Coordinator) TransitionSets() (joining, draining map[storageunit.NodeID]struct{}) {
	if c.mem == nil {
		// Static / test mode: roles live in the facts override.
		c.factsMu.Lock()
		defer c.factsMu.Unlock()
		for id, m := range c.facts {
			if m.Joining() {
				if joining == nil {
					joining = make(map[storageunit.NodeID]struct{}, 2)
				}
				joining[id] = struct{}{}
			}
			if m.Draining() {
				if draining == nil {
					draining = make(map[storageunit.NodeID]struct{}, 2)
				}
				draining[id] = struct{}{}
			}
		}
		return joining, draining
	}
	for _, m := range c.mem.Snapshot() {
		if m.Joining {
			if joining == nil {
				joining = make(map[storageunit.NodeID]struct{}, 2)
			}
			joining[storageunit.NodeID(m.ID)] = struct{}{}
		}
		if m.Draining {
			if draining == nil {
				draining = make(map[storageunit.NodeID]struct{}, 2)
			}
			draining[storageunit.NodeID(m.ID)] = struct{}{}
		}
	}
	return joining, draining
}

// memberFacts collects the per-member roles + declared counts from the STATIC
// facts override. Gossip mode never calls it: the snapshot-based View reads
// presence and roles from the same membership rows directly.
func (c *Coordinator) memberFacts() map[storageunit.NodeID]coord.Member {
	// Copy: the caller reads the result outside the lock, and a test may be
	// writing the static override concurrently.
	c.factsMu.Lock()
	defer c.factsMu.Unlock()
	out := make(map[storageunit.NodeID]coord.Member, len(c.facts))
	for id, m := range c.facts {
		out[id] = m
	}
	return out
}

// Locate returns up to n nodes for u, primary first. See coord.Coordinator.
//
// An exclusion is honored by locating over a ring GENUINELY REBUILT from the
// non-excluded members, never by locating over everyone and filtering. Bounded
// -load consistent hashing is not removal-invariant, so the filtered answer is
// a different (wrong) placement: a unit whose predicted post-transition owners
// are computed that way can be disjoint from its real ones, which strands the
// handoff on nodes that will never own it.
func (c *Coordinator) Locate(u storageunit.GenUnit, n int, p coord.Placement) []coord.Node {
	if n <= 0 || c.r == nil {
		return nil
	}
	r := c.r
	if len(p.Exclude) > 0 {
		r = c.reducedRing(p.Exclude)
		if r.Empty() {
			// Every member is excluded: there is no hypothetical placement to
			// answer with. nil lets the caller fall back to the real one.
			return nil
		}
	}
	ms := r.LocateKeyN(GenUnitBytes(u), n)
	if len(ms) == 0 {
		return nil
	}
	out := make([]coord.Node, 0, len(ms))
	for _, m := range ms {
		out = append(out, coord.Node{ID: storageunit.NodeID(m.ID), Addr: m.Addr})
	}
	return out
}

// reducedRing returns a ring holding every current member except the excluded
// ids, memoized on (exclusion set, view epoch). See Locate for why the ring has
// to be genuinely rebuilt rather than filtered.
//
// The memo is what keeps the exact construction affordable. A single reconcile
// pass asks the same exclusion question once per unit it owns, and a cluster
// can own dozens; without the memo each of those rebuilds the whole ring. The
// epoch in the key is the correctness half: any membership change bumps it, so
// a stale reduced ring can never outlive the membership it was built from.
func (c *Coordinator) reducedRing(exclude map[storageunit.NodeID]struct{}) *ring.Ring {
	key := excludeKey(exclude)
	epoch := c.epoch.Load()

	c.reducedMu.Lock()
	defer c.reducedMu.Unlock()
	if c.reduced != nil && c.reducedKey == key && c.reducedEpoch == epoch {
		return c.reduced
	}
	r := ring.New()
	for _, m := range c.r.Members() {
		if _, ex := exclude[storageunit.NodeID(m.ID)]; ex {
			continue
		}
		r.Add(m)
	}
	c.reduced, c.reducedKey, c.reducedEpoch = r, key, epoch
	return r
}

// excludeKey renders an exclusion set as an order-independent string, so two
// callers passing the same members in different map-iteration orders hit the
// same memo entry.
func excludeKey(exclude map[storageunit.NodeID]struct{}) string {
	ids := make([]string, 0, len(exclude))
	for id := range exclude {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	return strings.Join(ids, "\x00")
}

// SetRole publishes this node's complete role set, deduping a no-op write.
func (c *Coordinator) SetRole(want coord.Role) error {
	c.rolesMu.Lock()
	defer c.rolesMu.Unlock()
	if c.mem == nil || c.roles == want {
		c.roles = want
		return nil
	}
	prev := c.roles
	c.roles = want
	var firstErr error
	if prev.Has(coord.RoleJoining) != want.Has(coord.RoleJoining) {
		if err := c.mem.SetJoining(want.Has(coord.RoleJoining)); err != nil {
			firstErr = err
		}
	}
	if prev.Has(coord.RoleDraining) != want.Has(coord.RoleDraining) {
		if err := c.mem.SetDraining(want.Has(coord.RoleDraining)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Generation reports no opinion: this coordinator gossips membership, it does
// not agree storage generations. The storage layer drives those through its
// own agreement (a conditional-store arbiter, or the coordinated freeze
// barrier). A lease/CAS coordinator is where these two methods get real
// bodies; they are on the port so that adapter needs no port change.
func (c *Coordinator) Generation() storageunit.Generation { return 0 }

// ProposeGeneration always refuses. See Generation.
func (c *Coordinator) ProposeGeneration(storageunit.Generation) error {
	return coord.ErrGenerationUnsupported
}

// Close stops the loops and leaves the mesh. Idempotent.
func (c *Coordinator) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		c.loopWG.Wait()
		return nil
	}
	c.closeOnce.Do(func() { close(c.closeCh) })
	c.loopWG.Wait()
	if c.mem != nil {
		return c.mem.Close()
	}
	return nil
}

// GenUnitBytes is the STABLE ring-input encoding of a generation-qualified
// unit: 8 big-endian bytes of Generation followed by 4 of UnitID.
//
// It must be identical on every node, so it lives in exactly one place. The
// generation is part of the hash input, so a unit's gen-g and gen-(g+1) ids
// hash to different ring positions - which is what lets a doubling reshard
// land the new generation's units wherever the ring places them. The id
// carries no "{...}" hash tag, so ShardKey passes it through whole.
func GenUnitBytes(gu storageunit.GenUnit) []byte {
	var b [12]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(gu.Gen))
	binary.BigEndian.PutUint32(b[8:12], uint32(gu.ID))
	return b[:]
}
