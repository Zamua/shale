package membership

// Test-only seams for the membership layer. Nothing in this file is called by
// production code paths; the exported functions exist solely so tests in OTHER
// packages (which cannot reach the unexported memberlist handle) can simulate
// failure modes that the normal lifecycle API does not expose. They follow the
// established Testing* naming convention (see the cluster package's
// TestingSetRingMembers / TestingDropAllPeerClients). Adding these changes NO
// production behavior: no non-test caller invokes them.

// TestingShutdownNoLeave shuts this node's memberlist transport down WITHOUT the
// graceful Leave broadcast that Close performs, modeling an UNGRACEFUL departure
// (a SIGKILLed process / a k8s pod delete). Peers therefore do NOT receive a
// clean leave notification; they must detect the departure via SWIM failure
// detection (a StateDead transition after the probe/suspicion timeout), which is
// exactly the regime a rolling hard-restart exercises and the regime the
// graceful-leave path bypasses.
//
// It is otherwise identical to Close: it stops the periodic rejoin + meta-refresh
// goroutines (so nothing calls ml.Join / ml.UpdateNode on a dead transport), then
// Shutdown()s the transport and shuts the event delegate down, so the node's own
// goroutines all stop cleanly (no leak). The ONLY difference from Close is the
// absence of the ml.Leave call.
//
// Idempotent: a no-op returning nil once the Membership is already closed.
func (m *Membership) TestingShutdownNoLeave() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	rejoinDone := m.rejoinDone
	metaRefreshDone := m.metaRefreshDone
	m.mu.Unlock()

	// Stop the periodic background goroutines and wait for them to exit BEFORE
	// tearing the transport down (identical to Close), so neither can call
	// ml.Join / ml.UpdateNode on a shut-down memberlist.
	if rejoinDone != nil {
		close(rejoinDone)
		m.rejoinWG.Wait()
	}
	if metaRefreshDone != nil {
		close(metaRefreshDone)
		m.metaRefreshWG.Wait()
	}

	// The ONLY divergence from Close: NO m.ml.Leave. A SIGKILL sends no leave
	// broadcast; peers reap the node via failure detection.
	err := m.ml.Shutdown()
	m.events.shutdown()
	return err
}
