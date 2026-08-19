package testport_test

import (
	"net"
	"testing"

	"github.com/Zamua/shale/internal/testport"
)

// TestTCPOnlyProbeHandsOutAPortItCannotFullyBind demonstrates the defect
// deterministically rather than waiting for it under load: hold the UDP half
// of a port, and a TCP-only probe will still certify that number as free. A
// caller that binds both halves then fails on a port a helper promised it.
//
// The demonstration is direct rather than statistical because the flake it
// causes is not: it surfaces only under the port churn of a concurrent
// whole-repo run, as an unrelated test failing for no stated reason.
func TestTCPOnlyProbeHandsOutAPortItCannotFullyBind(t *testing.T) {
	// Find a port, then occupy ONLY its UDP half and release TCP - the exact
	// split state the kernel can hand a caller.
	port := testport.Free(t)
	udp, err := net.ListenPacket("udp", testport.Addr(port))
	if err != nil {
		t.Skipf("could not occupy the udp half of %d: %v", port, err)
	}
	defer func() { _ = udp.Close() }()

	// The TCP half is genuinely free: a TCP-only probe on THIS port would
	// certify it. Prove that directly.
	l, err := net.Listen("tcp", testport.Addr(port))
	if err != nil {
		t.Skipf("tcp half of %d not free, cannot demonstrate the split: %v", port, err)
	}
	_ = l.Close()

	// So a helper that certifies on TCP alone declares this port usable...
	// while the bind a caller actually performs fails:
	if _, err := net.ListenPacket("udp", testport.Addr(port)); err == nil {
		t.Fatal("expected the udp half to be occupied; the demonstration is not set up")
	}

	// ...and testport.Free never returns it, because it claims both halves
	// before trusting the number.
	for i := 0; i < 50; i++ {
		if got := testport.Free(t); got == port {
			t.Fatalf("Free returned %d, whose udp half is occupied: it certified only the tcp half", got)
		}
	}
}

// TestFreeIsBindableOnBothHalves pins the guarantee the callers rely on: the
// number comes back bindable on TCP and UDP together.
func TestFreeIsBindableOnBothHalves(t *testing.T) {
	for i := 0; i < 200; i++ {
		port := testport.Free(t)
		l, terr := net.Listen("tcp", testport.Addr(port))
		u, uerr := net.ListenPacket("udp", testport.Addr(port))
		if terr != nil || uerr != nil {
			t.Fatalf("Free returned %d but it does not bind on both halves: tcp=%v udp=%v", port, terr, uerr)
		}
		_ = l.Close()
		_ = u.Close()
	}
}
