package cluster

// Shared white-box coordination fixtures. The tests in this package build
// Cluster values by hand (no Open, no transport) and need deterministic
// placement over a member set they control, so they stand up a TRANSPORT-FREE
// static coordinator instead of a gossip mesh: same placement math, nothing to
// join, nothing to reconcile.

import (
	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/storageunit"
)

// nodesFor builds the coord.Node set for ids, using the tests' "<id>:0"
// synthetic address convention.
func nodesFor(ids ...string) []coord.Node {
	out := make([]coord.Node, 0, len(ids))
	for _, id := range ids {
		out = append(out, coord.Node{ID: storageunit.NodeID(id), Addr: id + ":0"})
	}
	return out
}

// staticCoord returns a transport-free coordinator for self over members.
func staticCoord(self string, members []coord.Node) *gossip.Coordinator {
	return gossip.NewStatic(coord.Node{ID: storageunit.NodeID(self), Addr: self + ":0"}, members)
}

// setCoordMembers replaces c's coordination view with exactly members.
func setCoordMembers(c *Cluster, members ...coord.Node) {
	c.coord = staticCoord(c.cfg.NodeID, members)
}

// coordMembers returns the placement basis of c's (static) coordinator.
func coordMembers(c *Cluster) []coord.Node {
	return c.coord.(*gossip.Coordinator).TestingRingMembers()
}

// addCoordMember folds n into c's existing coordination view, replacing any
// entry with the same id.
func addCoordMember(c *Cluster, n coord.Node) {
	cur := coordMembers(c)
	out := make([]coord.Node, 0, len(cur)+1)
	for _, m := range cur {
		if m.ID != n.ID {
			out = append(out, m)
		}
	}
	out = append(out, n)
	c.coord.(*gossip.Coordinator).TestingSetMembers(out)
}

// removeCoordMembers drops ids from c's coordination view.
func removeCoordMembers(c *Cluster, ids ...string) {
	drop := make(map[storageunit.NodeID]struct{}, len(ids))
	for _, id := range ids {
		drop[storageunit.NodeID(id)] = struct{}{}
	}
	cur := coordMembers(c)
	out := make([]coord.Node, 0, len(cur))
	for _, m := range cur {
		if _, gone := drop[m.ID]; !gone {
			out = append(out, m)
		}
	}
	c.coord.(*gossip.Coordinator).TestingSetMembers(out)
}
