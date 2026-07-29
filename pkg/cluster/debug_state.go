package cluster

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Zamua/shale/pkg/storageunit"
)

// DebugState returns a human-readable dump of this node's per-position handoff
// state, for LIVE diagnosis on the SHALE_DEBUG_ADDR /debug/shale/state endpoint.
// For every ReplicaUnit this node desires (current owner), pending-owns, has
// mounted, or holds a handoff phase for, it reports desired / pending / mounted
// + the current handoff phase.
//
// THERE ARE TWO WEDGE SIGNATURES, and they are mirror images. Both are a
// position stuck in a loser phase (Draining) whose drainCheck waits on a
// successor serving marker that never comes; they differ in which half of
// desired/mounted is the surprising one, and they starve different paths:
//
//   - DESIRED but NOT mounted - starves the WRITE path: a write to the position
//     cannot get a local ack, and the acquire half skips it because it is parked
//     in a loser phase.
//   - MOUNTED but NOT desired - starves the SCAN path: the ring says the
//     position is not this node's, but the mount is still in the mount map, and
//     mountedUnits() is pure mount-map presence, so EVERY cross-shard scan walks
//     it. If the handle is fenced, every such scan is refused. Point reads and
//     writes are unaffected (they route to a specific position and never touch
//     this one), so the cluster looks healthy while all fan-out work dies.
//
// The second shape went undocumented and was found in production, where it had
// killed every background cross-shard job for many hours while reads and writes
// stayed correct throughout. If you are diagnosing "everything works except the
// fan-out", look for mounted-but-not-desired.
//
// Read-only; takes the read locks briefly.
func (c *Cluster) DebugState() string {
	var b strings.Builder
	gs := c.genSnapshot()
	fmt.Fprintf(&b, "node=%s gen=%d unitCount=%d multiReplicated=%v\n",
		c.cfg.NodeID, gs.gen, gs.count, c.multiReplicated())

	draining := c.drainingIDs()
	dids := make([]string, 0, len(draining))
	for id := range draining {
		dids = append(dids, string(id))
	}
	sort.Strings(dids)
	fmt.Fprintf(&b, "draining-members=%v\n", dids)

	joining := c.joiningIDs()
	jids := make([]string, 0, len(joining))
	for id := range joining {
		jids = append(jids, string(id))
	}
	sort.Strings(jids)
	fmt.Fprintf(&b, "joining-members=%v self-joining=%v\n", jids, c.selfJoining.Load())

	// `desired` is this node's REAL ownership set (the full ring), so a warming
	// joiner's owned-but-unmounted positions correctly show as desired-but-unmounted
	// (the wedge indicator). The CURRENT-view (joining-excluded) set is a routing
	// concern, not an ownership one, so it is not what the wedge flag keys on.
	desired := ruBoolSet(c.desiredReplicaUnits())
	pending := ruBoolSet(c.desiredPendingReplicaUnits(draining))

	snap := c.mounts.snapshot()
	mounted, phases := snap.mounted, snap.phases
	acquiring := make([]string, 0, len(snap.inFlight))
	for _, ru := range snap.inFlight {
		acquiring = append(acquiring, ru.String())
	}
	sort.Strings(acquiring)
	fmt.Fprintf(&b, "acquire-in-flight=%v\n", acquiring)

	all := make(map[storageunit.ReplicaUnit]struct{})
	for ru := range desired {
		all[ru] = struct{}{}
	}
	for ru := range pending {
		all[ru] = struct{}{}
	}
	for ru := range mounted {
		all[ru] = struct{}{}
	}
	for ru := range phases {
		all[ru] = struct{}{}
	}
	list := make([]storageunit.ReplicaUnit, 0, len(all))
	for ru := range all {
		list = append(list, ru)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].String() < list[j].String() })

	wedged := 0
	for _, ru := range list {
		ph := "none"
		if st, ok := phases[ru]; ok {
			ph = fmt.Sprintf("%s@%d", st.Phase, st.OpenEpoch)
		}
		flag := ""
		if desired[ru] && !mounted[ru] {
			flag = "  <<< DESIRED-BUT-UNMOUNTED (wedge)"
			wedged++
		}
		acqErr := ""
		if v, ok := c.mounts.acquireErrOf(ru); ok {
			acqErr = fmt.Sprintf(" lastAcquireErr=%q", v)
		}
		fmt.Fprintf(&b, "  %s desired=%v pending=%v mounted=%v phase=%s%s%s\n",
			ru, desired[ru], pending[ru], mounted[ru], ph, acqErr, flag)
	}
	fmt.Fprintf(&b, "summary: %d positions, %d desired-but-unmounted\n", len(list), wedged)
	return b.String()
}

func ruBoolSet(rus []storageunit.ReplicaUnit) map[storageunit.ReplicaUnit]bool {
	m := make(map[storageunit.ReplicaUnit]bool, len(rus))
	for _, ru := range rus {
		m[ru] = true
	}
	return m
}
