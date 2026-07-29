package gossip_test

// Package-level TestMain for the gossip adapter's test binary.
//
// goleak.VerifyTestMain catches goroutine leaks that survive the last test's
// t.Cleanup. This adapter owns background goroutines (the event loop, the ring
// reconcile loop, and memberlist's own gossip machinery), so a Close that fails
// to join them shows up here as binary residue rather than as a mystery hang in
// a downstream package.
//
// The known third-party leakers we ignore live in ONE shared place -
// internal/goleakignore.Options() - so this binary can never drift from the
// cluster and integration binaries. The adapter's OWN goroutines are
// deliberately NOT ignored, so a real leak still fails here.

import (
	"testing"

	"github.com/Zamua/shale/internal/goleakignore"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleakignore.Options()...)
}
