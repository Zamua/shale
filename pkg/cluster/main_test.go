package cluster_test

// Package-level TestMain for the cluster test binary. The whole
// binary (both the cluster_test "external" file set + the cluster
// "internal" file set, since the latter still lives in package
// cluster and compiles into the same binary) runs through here.
//
// goleak.VerifyTestMain catches goroutine leaks that survive the
// last test's t.Cleanup. The cluster surface spawns many background
// goroutines (events loop, reconcile loop, sweep loop, read-repair
// workers, fan-out drainers, memberlist gossip, gRPC client streams)
// so a leak that escapes Close shows up here as a per-test or
// per-binary residue.
//
// The set of known third-party leakers we IGNORE lives in ONE shared
// place - internal/goleakignore.Options() - so this binary and the
// integration binary can never drift (they ignore exactly the same
// set; see that package's doc for the rationale + the push-pull
// teardown story). The Cluster's OWN goroutines are deliberately NOT
// ignored, so a real leak still fails here.

import (
	"testing"

	"github.com/Zamua/shale/internal/goleakignore"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleakignore.Options()...)
}
