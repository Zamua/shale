// Package rebalance owns the v0.3 shard-handoff protocol: planning
// which partitions move on a ring change (plan.go), driving each
// move through its source / destination state machine (state.go,
// execute.go), and cleaning up the post-handoff data on the old
// owner once a grace window has elapsed (sweep.go).
//
// See docs/SPEC.md "Plan" and "Cutover" for the protocol; each file
// header expands on its specific role.
package rebalance
