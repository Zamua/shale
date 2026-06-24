//go:build race

package cluster_test

// convScale multiplies the test cluster's convergence CEILINGS (ring
// settle, rebalance-idle, write-readiness) - the MAX a setup helper waits
// before declaring failure. It is NOT a per-test or suite timeout: a fast
// runner still returns the moment the cluster is ready (a few seconds), so
// this never slows a passing test. The race detector's CPU+memory overhead
// on a 2-vCPU CI runner can make a 3-node R3 cluster take meaningfully
// longer to MOUNT all replicas; under -race the ceilings get headroom so a
// slow runner is not spuriously failed mid-mount. Non-race builds use 1.
const convScale = 3
