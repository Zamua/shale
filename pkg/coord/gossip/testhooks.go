package gossip

import (
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/membership"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// Test-only seams. Nothing here is called by production code; they exist so
// tests in other packages can model failure modes the normal lifecycle API
// cannot reach. They follow the established Testing* convention and change no
// production behavior.

// TestingSetMembers replaces the placement basis with exactly nodes, modeling
// a node whose topology view has DIVERGED from its peers' (a stale ring after a
// dropped event, or a count-preserving membership swap mid-reshard).
//
// It mutates the existing ring in place - Remove every member not wanted, then
// Add the wanted set - rather than swapping the ring pointer. The pointer is
// assigned once in Start before any goroutine spawns; thereafter only the
// ring's own internally-locked Add/Remove mutate it, and the events loop reads
// the field concurrently. Reassigning it here would race that unguarded read.
//
// The divergence is NOT pinned: the periodic reconcile heals it back toward
// gossip on its next tick, exactly as it did before the port existed. A test
// that needs the divergence to persist parks Config.ReconcileInterval.
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

// TestingShutdownNoLeave hard-shuts the gossip transport WITHOUT broadcasting
// a graceful leave, modeling a SIGKILLed process: peers get no clean leave
// and must reap this node through SWIM failure detection.
func (c *Coordinator) TestingShutdownNoLeave() error {
	if c.mem == nil {
		return nil
	}
	return c.mem.TestingShutdownNoLeave()
}

// TestingInjectUpdate forces a Meta-rebroadcast for an existing node id at a
// new gRPC address, driving the same path memberlist's NotifyUpdate takes.
// memberlist exposes no public way to do this, and it is the only way to
// exercise "a peer moved to a new endpoint" without restarting a process.
func (c *Coordinator) TestingInjectUpdate(id, newAddr string) {
	if c.mem == nil {
		return
	}
	membership.TestingInjectUpdate(c.mem, id, newAddr)
}

// TestingReconcileRing runs one ring-vs-gossip reconciliation pass
// synchronously and reports whether the ring moved. It lets a test drive the
// heal explicitly instead of waiting out a background tick. A heal that moved
// the ring bumps the epoch exactly as the background loop does - every ring
// mutation must, or a memoized hypothetical placement (reducedRing's memo is
// keyed on the epoch) could outlive the membership it was built from.
func (c *Coordinator) TestingReconcileRing() bool {
	changed := c.reconcileRing()
	if changed {
		c.epoch.Add(1)
	}
	return changed
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

// TestingRemoveFromRing drops id from the placement basis WITHOUT touching
// gossip, simulating the event-channel drop the periodic reconcile exists to
// heal.
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

// TestingSetFacts records the roles + declared unit count a STATIC coordinator
// reports for id. A static coordinator has no gossip cache to read them from,
// so a test that needs a member to advertise a declared count (the
// declarative-reshard unanimity gate) or a role sets it here.
func (c *Coordinator) TestingSetFacts(m coord.Member) {
	c.factsMu.Lock()
	defer c.factsMu.Unlock()
	if c.facts == nil {
		c.facts = make(map[storageunit.NodeID]coord.Member, 1)
	}
	c.facts[m.ID] = m
	c.epoch.Add(1)
	c.signal()
}
