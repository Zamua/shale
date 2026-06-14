package integration

import "github.com/Zamua/shale/pkg/membership"

// Use memberlist's tight loopback preset for this package's in-process
// test clusters so 3-node fixtures converge in milliseconds instead of
// the seconds the LAN preset costs. Runs once before any test in the
// integration test binary; production is unaffected (it never calls this).
func init() { membership.UseLocalGossipForTests() }
