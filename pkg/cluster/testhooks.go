package cluster

import (
	"fmt"

	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// Test-only seams for the cluster layer. Nothing here is called by production
// code; the exported functions exist so tests in other packages can simulate
// failure modes the normal lifecycle API does not expose. They follow the
// established Testing* convention (TestingClearMount / TestingSetRingMembers /
// TestingDropAllPeerClients). Adding them changes NO production behavior.

// TestingRunReconcile runs ONE reconcile pass (the same runReconcile the
// periodic self-heal loop ticks), synchronously. Test-only white-box hook: it
// lets a test drive the reconcile - including the arbiter-driven reshard step
// (observeReshard) - on demand instead of waiting out the production reconcile
// interval. Follows the Testing* convention.
func (c *Cluster) TestingRunReconcile() {
	c.runReconcile()
}

// TestingSetRingMembers replaces the local node's placement basis with the
// given members, modelling a node whose topology view has DIVERGED from its
// peers'. Test-only white-box hook; multi-node mode only. Follows the Testing*
// convention.
//
// It PANICS on a coordinator without the MemberSetter seam rather than
// no-opping: a divergence a test believes it induced but did not is a test
// that pins nothing while reporting success, and only the caller's own
// arming guard would catch it. Fail loud at the seam instead.
//
// It keeps taking ring.Member so the existing white-box call sites are
// unchanged; the value is just an (id, addr) pair.
func (c *Cluster) TestingSetRingMembers(members []ring.Member) {
	setter, ok := c.coord.(coord.MemberSetter)
	if !ok {
		panic(fmt.Sprintf("cluster: TestingSetRingMembers needs a coord.MemberSetter, got %T; use a coordinator that offers the seam (internal/coordstatic)", c.coord))
	}
	nodes := make([]coord.Node, 0, len(members))
	for _, m := range members {
		nodes = append(nodes, coord.Node{ID: storageunit.NodeID(m.ID), Addr: m.Addr})
	}
	setter.TestingSetMembers(nodes)
}

// TestingHardKill tears this node down like Close BUT WITHOUT the graceful
// membership Leave, modeling a SIGKILLed process / a k8s pod delete: peers do
// not get a clean leave and must reap the node via SWIM failure detection. It
// first hard-shuts the coordinator's transport (no departure announcement) via
// its TestingShutdownNoLeave seam, then runs the normal Close teardown (which,
// seeing the transport already down, skips its own leave/shutdown) so every
// cluster goroutine - the coordination + reconcile loops, the read-repair pool,
// the rebalance coordinator, the settle timer - stops cleanly and no goroutine
// leaks. A coordinator that does not offer the seam just gets a normal Close.
//
// NB it deliberately does NOT run the graceful-leave DRAIN that Close performs
// when GracefulLeaveDrainTimeout > 0: a SIGKILL drains nothing. The in-process
// R=2 harness leaves that timeout at 0, so Close skips the drain anyway; a caller
// that sets a nonzero drain timeout should not use this seam (the transport is
// already dead when Close's drain would run).
func (c *Cluster) TestingHardKill() error {
	if hs, ok := c.coord.(coord.HardShutdowner); ok {
		// Hard-shut the transport with no departure announcement FIRST, so the
		// subsequent Close does not send a graceful leave (the coordinator's own
		// Close sees the transport already down and is a no-op) - the whole
		// point of modeling a SIGKILL.
		_ = hs.TestingShutdownNoLeave()
	}
	return c.Close()
}
