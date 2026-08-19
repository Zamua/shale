// Package coordstatic is the transport-free STATIC implementation of shale's
// coordination port (pkg/coord): a fixed member set, the same bounded-load
// consistent-hash placement math every adapter uses, and nothing to gossip,
// poll or reconcile.
//
// It exists for in-process callers that need deterministic placement without
// standing up a coordination mechanism - white-box tests being the main one.
// Roles and declared unit counts are whatever the facts override records (no
// peer ever sees them), Changed fires only when a Testing* seam mutates the
// member set or facts, and Start is a no-op that only adopts the identity.
package coordstatic

import (
	"encoding/binary"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// Coordinator is the static implementation of coord.Coordinator.
type Coordinator struct {
	// self is the identity reported as View.Self. Set at construction;
	// Start re-adopts the identity it is passed.
	self coord.Node

	// r is the placement ring, fixed at construction. Nil on an unstarted
	// coordinator, where every query answers empty. Never reassigned after
	// construction: the ring's own internal lock covers Testing* mutations.
	r *ring.Ring

	// roles is this node's locally recorded role set: SetRole records it,
	// nothing publishes it (there are no peers).
	rolesMu sync.Mutex
	roles   coord.Role

	// epoch is the local monotone view counter, bumped on every Testing*
	// mutation. The reduced-ring memo keys on it, so every ring or facts
	// mutation must bump it.
	epoch atomic.Uint64

	// changed is the coalescing change hint. Capacity 1: a send that would
	// block is dropped, because the contract is a hint, not a queue.
	changed chan struct{}

	// facts carries the per-member roles + declared unit counts this
	// coordinator reports; there is no transport to learn them from.
	// Guarded by factsMu.
	factsMu sync.Mutex
	facts   map[storageunit.NodeID]coord.Member

	// reduced memoizes the last hypothetical-placement ring (see Locate),
	// keyed by the exclusion set and the epoch it was built from.
	reducedMu    sync.Mutex
	reducedKey   string
	reducedEpoch uint64
	reduced      *ring.Ring
}

// Compile-time proof the adapter satisfies the port and the seams the static
// mode offers.
var (
	_ coord.Coordinator    = (*Coordinator)(nil)
	_ coord.MemberSetter   = (*Coordinator)(nil)
	_ coord.HardShutdowner = (*Coordinator)(nil)
)

// New returns a STARTED Coordinator with the fixed member set. Start is still
// safe to call (the storage layer calls it through the port); it only adopts
// the identity.
func New(self coord.Node, members []coord.Node) *Coordinator {
	c := NewUnstarted()
	c.self = self
	c.r = ring.New()
	for _, m := range members {
		c.r.Add(ring.Member{ID: string(m.ID), Addr: m.Addr})
	}
	return c
}

// NewUnstarted returns a Coordinator with no members at all: every query
// answers empty and Close is a safe no-op, per the port's pre-Start contract.
func NewUnstarted() *Coordinator {
	return &Coordinator{changed: make(chan struct{}, 1)}
}

// Start adopts the identity and reports a founding: a static coordinator's
// membership was fixed at construction, so there is nothing to bootstrap. A
// founder never advertises Joining (there is no incumbent to warm against),
// so the requested role is dropped per the port.
func (c *Coordinator) Start(p coord.Params) (coord.Bootstrap, error) {
	c.self = p.Self
	c.rolesMu.Lock()
	c.roles = p.InitialRoles &^ coord.RoleJoining
	c.rolesMu.Unlock()
	return coord.BootstrapFounded, nil
}

// signal delivers a coalescing change hint. A full channel means an
// undelivered hint is already pending, so dropping this one loses nothing.
func (c *Coordinator) signal() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

// Changed returns the coalescing change-hint channel. Only the Testing*
// mutation seams fire it; a static membership otherwise never changes.
func (c *Coordinator) Changed() <-chan struct{} { return c.changed }

// View returns the fixed membership: the ring IS the member truth, roles and
// declared counts come from the facts override.
func (c *Coordinator) View() coord.View {
	v := coord.View{Self: c.self, Epoch: c.epoch.Load()}
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

// Populated reports whether the member set is non-empty, straight off the
// ring per the port's per-op cost clause.
func (c *Coordinator) Populated() bool {
	return c.r != nil && !c.r.Empty()
}

// PlacementMembers returns the ring's member set - which, single-basis, is
// always exactly View's member set.
func (c *Coordinator) PlacementMembers() []coord.Node {
	if c.r == nil {
		return nil
	}
	ms := c.r.Members()
	out := make([]coord.Node, 0, len(ms))
	for _, m := range ms {
		out = append(out, coord.Node{ID: storageunit.NodeID(m.ID), Addr: m.Addr})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// TransitionSets returns the joining / draining member-ID sets from the facts
// override: nil maps in the steady state, fresh maps every call.
func (c *Coordinator) TransitionSets() (joining, draining map[storageunit.NodeID]struct{}) {
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

// memberFacts copies the facts override: the caller reads the result outside
// the lock, and a test may be writing the override concurrently.
func (c *Coordinator) memberFacts() map[storageunit.NodeID]coord.Member {
	c.factsMu.Lock()
	defer c.factsMu.Unlock()
	out := make(map[storageunit.NodeID]coord.Member, len(c.facts))
	for id, m := range c.facts {
		out[id] = m
	}
	return out
}

// Locate returns up to n nodes for u, primary first - the same pkg/ring math
// and the same genUnitBytes input as every other adapter, so this coordinator
// places identically over the same member set.
//
// An exclusion is honored by locating over a ring GENUINELY REBUILT from the
// non-excluded members, never by filtering: bounded-load consistent hashing is
// not removal-invariant, so the filtered answer would be a different (wrong)
// placement - the port's exactness contract.
func (c *Coordinator) Locate(u storageunit.GenUnit, n int, p coord.Placement) []coord.Node {
	if n <= 0 || c.r == nil {
		return nil
	}
	r := c.r
	if len(p.Exclude) > 0 {
		r = c.reducedRing(p.Exclude)
		if r.Empty() {
			// Every member is excluded: no hypothetical placement to answer
			// with. nil lets the caller fall back to the real one.
			return nil
		}
	}
	ms := r.LocateKeyN(genUnitBytes(u), n)
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
// ids, memoized on (exclusion set, epoch). The epoch key is the correctness
// half: every membership mutation bumps it, so a stale reduced ring can never
// outlive the membership it was built from.
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

// genUnitBytes is the STABLE ring-input encoding of a generation-qualified
// unit: 8 big-endian bytes of Generation followed by 4 of UnitID. It must be
// byte-identical to pkg/coord/internal/placement.GenUnitBytes (unreachable
// from here by the internal rule) or this coordinator places units on
// different nodes than the real adapters over the same member set.
func genUnitBytes(gu storageunit.GenUnit) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b[:8], uint64(gu.Gen))
	binary.BigEndian.PutUint32(b[8:], uint32(gu.ID))
	return b
}

// SetRole records this node's complete role set locally; there are no peers
// to publish it to.
func (c *Coordinator) SetRole(want coord.Role) error {
	c.rolesMu.Lock()
	c.roles = want
	c.rolesMu.Unlock()
	return nil
}

// Generation reports no opinion: a static coordinator does not agree storage
// generations.
func (c *Coordinator) Generation() storageunit.Generation { return 0 }

// ProposeGeneration always refuses. See Generation.
func (c *Coordinator) ProposeGeneration(storageunit.Generation) error {
	return coord.ErrGenerationUnsupported
}

// Close is a no-op: there are no loops and no transport. Idempotent.
func (c *Coordinator) Close() error { return nil }

// TestingShutdownNoLeave is a no-op: there is no transport to hard-stop. It
// exists so a cluster-level hard-kill seam works uniformly across adapters.
func (c *Coordinator) TestingShutdownNoLeave() error { return nil }

// TestingSetMembers replaces the placement basis with exactly nodes, modeling
// a node whose topology view has DIVERGED from its peers'.
//
// It mutates the existing ring in place - Remove every member not wanted,
// then Add the wanted set - rather than swapping the ring pointer, which a
// concurrent query reads without a lock. Re-adding a present member with a
// changed Addr updates the address.
func (c *Coordinator) TestingSetMembers(nodes []coord.Node) {
	if c.r == nil {
		return
	}
	want := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		want[string(n.ID)] = struct{}{}
	}
	for _, cur := range c.r.Members() {
		if _, keep := want[cur.ID]; !keep {
			c.r.Remove(cur.ID)
		}
	}
	for _, n := range nodes {
		c.r.Add(ring.Member{ID: string(n.ID), Addr: n.Addr})
	}
	c.epoch.Add(1)
	c.signal()
}

// TestingRingMembers returns the raw placement basis, for a test asserting on
// the ring directly rather than through the enriched View.
func (c *Coordinator) TestingRingMembers() []coord.Node {
	if c.r == nil {
		return nil
	}
	ms := c.r.Members()
	out := make([]coord.Node, 0, len(ms))
	for _, m := range ms {
		out = append(out, coord.Node{ID: storageunit.NodeID(m.ID), Addr: m.Addr})
	}
	return out
}

// TestingRemoveFromRing drops id from the placement basis without a hint,
// modeling a member silently missing from placement.
func (c *Coordinator) TestingRemoveFromRing(id string) {
	if c.r == nil {
		return
	}
	c.r.Remove(id)
	// Bump the epoch even though nothing observed this: every ring mutation
	// must, or a memoized hypothetical placement could outlive the membership
	// it was built from.
	c.epoch.Add(1)
}

// TestingSetFacts records the roles + declared unit count this coordinator
// reports for m.ID. A test that needs a member to advertise a declared count
// (the declarative-reshard unanimity gate) or a role sets it here.
func (c *Coordinator) TestingSetFacts(m coord.Member) {
	c.factsMu.Lock()
	if c.facts == nil {
		c.facts = make(map[storageunit.NodeID]coord.Member, 1)
	}
	c.facts[m.ID] = m
	c.factsMu.Unlock()
	c.epoch.Add(1)
	c.signal()
}
