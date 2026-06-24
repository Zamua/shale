//go:build !race

package cluster_test

// convScale is 1 without the race detector: the convergence ceilings stay
// tight so a real stall fails fast. See the -race build for the rationale.
const convScale = 1
