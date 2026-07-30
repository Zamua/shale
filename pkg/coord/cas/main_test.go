package cas_test

// Package-level TestMain for the CAS adapter's test binary.
//
// goleak.VerifyTestMain catches goroutine leaks that survive the last
// test's t.Cleanup. This adapter owns two background goroutines (the
// renewal loop and the poll loop), so a Close that fails to join them shows
// up here as binary residue. NO ignore options: unlike the gossip binary
// there is no third-party machinery (no memberlist, no gRPC) to excuse, so
// any leak is the adapter's own.

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
