package cluster

// Consumer notification for the unit-mount lifecycle (Config.OnUnitMounted).
//
// It hangs off the same seam as the serving-marker publish and the tombstone
// purge: the one place a position becomes locally serving. That is what makes
// it complete without any mount path having to remember it.
//
// shale carries the EVENT and nothing else. It does not know what recovery
// work a consumer runs, cannot judge whether a pass succeeded, and therefore
// owns no retry, no backoff and no poison policy - deciding those would mean
// owning bookkeeping that belongs to the app.

import (
	"runtime/debug"

	"github.com/Zamua/shale/pkg/storageunit"
)

// notifyUnitMounted spawns the consumer's OnUnitMounted callback for a
// position that just became serving. Spawn-only: the mount path must not
// wait on app code. The goroutine is tracked on loopWG so Close drains it
// rather than leaking it past shutdown.
func (c *Cluster) notifyUnitMounted(ru storageunit.ReplicaUnit) {
	fn := c.cfg.OnUnitMounted
	if fn == nil {
		return
	}
	token := unitToken(ru.Unit)
	c.loopWG.Add(1)
	go func() {
		defer c.loopWG.Done()
		defer func() {
			// Recover rather than let it reach the runtime: shale spawned this
			// goroutine, so the app had no opportunity to wrap it, and an
			// unrecovered panic here would take down a node that is otherwise
			// serving correctly. Loud, and the pass is abandoned - the next
			// mount of this position fires the hook again.
			if r := recover(); r != nil {
				c.logf("shale: OnUnitMounted PANICKED for unit %s: %v\n%s", token, r, debug.Stack())
			}
		}()
		fn(token)
	}()
}
