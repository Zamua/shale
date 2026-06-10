package cluster

import "time"

// Test-only hooks. This file is compiled only under `go test`, so it can
// expose unexported tunables to the external cluster_test package without
// widening the public API surface.

// SetCASBaseBackoffZero zeroes the inter-attempt CAS backoff (so retry
// tests run fast) and returns the prior value for restoration.
func SetCASBaseBackoffZero() time.Duration {
	prev := casBaseBackoff
	casBaseBackoff = 0
	return prev
}

// RestoreCASBaseBackoff resets the CAS backoff to d.
func RestoreCASBaseBackoff(d time.Duration) { casBaseBackoff = d }
