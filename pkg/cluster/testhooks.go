package cluster

import "github.com/Zamua/shale/pkg/coord"

// Test-only seams for the cluster layer. Nothing here is called by production
// code; the exported functions exist so tests in other packages can simulate
// failure modes the normal lifecycle API does not expose. They follow the
// established Testing* convention (TestingClearMount / TestingSetRingMembers /
// TestingDropAllPeerClients). Adding them changes NO production behavior.

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
