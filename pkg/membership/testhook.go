package membership

import "github.com/hashicorp/memberlist"

// TestingInjectUpdate synthesizes a memberlist NotifyUpdate callback
// as though gossip had just delivered new Meta bytes (a fresh GRPCAddr)
// for the named node. It is a TEST-ONLY hook for downstream packages
// (notably pkg/cluster's internal tests) that need to exercise the
// addr-change eviction path.
//
// memberlist's public API has no way to force a Meta-rebroadcast with
// a different addr for an existing node ID, so without this hook the
// NotifyUpdate codepath cannot be driven from a test. The function
// lives in a regular .go file (not export_test.go) because pkg/cluster
// tests live in a different package and would not otherwise see the
// helper.
//
// Do NOT call from production code: it synthesizes events that did not
// originate from gossip, which will desynchronize the ring from the
// real membership view until the periodic reconciler corrects it.
func TestingInjectUpdate(m *Membership, nodeID, newAddr string) {
	m.events.NotifyUpdate(&memberlist.Node{
		Name: nodeID,
		Meta: []byte(newAddr),
	})
}
