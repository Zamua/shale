package cas

// Test-only seams. Nothing here is called by production code; they exist so
// tests can drive the adapter's two I/O cadences DETERMINISTICALLY (no
// timing luck: construct with negative intervals to disable the loops, then
// tick these by hand) and model failure modes the normal lifecycle API
// cannot reach. They follow the established Testing* convention and change
// no production behavior.

// TestingPollOnce runs one observation pass synchronously - the same pass
// the background poll loop ticks: read the document, judge lease expiry,
// publish the snapshot, GC expired rows, fire the hint. Safe alongside a
// running loop (passes serialize on pollMu); a no-op before Start.
func (c *Coordinator) TestingPollOnce() {
	if c.cfg.Store == nil || !c.started.Load() {
		return
	}
	c.pollOnce()
}

// TestingRenewOnce CAS-advances this node's leaseGen once, synchronously -
// the same cycle the background renewal loop ticks (including the re-add of
// a GC'd self row). A no-op before Start.
func (c *Coordinator) TestingRenewOnce() error {
	if c.cfg.Store == nil || !c.started.Load() {
		return nil
	}
	return c.renewOnce()
}

// TestingShutdownNoLeave stops both loops WITHOUT the graceful CAS-removal,
// modeling a SIGKILLed process: the row lingers in the document and peers
// must reap it through lease expiry. A later Close skips the removal too - a
// killed process runs nothing.
func (c *Coordinator) TestingShutdownNoLeave() error {
	c.hardStopped.Store(true)
	c.closeOnce.Do(func() { close(c.closeCh) })
	c.loopWG.Wait()
	return nil
}
