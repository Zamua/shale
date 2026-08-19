// Package testport hands out loopback ports for tests that bind real sockets.
//
// It exists because the obvious implementation is wrong in a way that only
// shows under load. A test that needs a port typically opens a TCP listener on
// :0, reads the number the kernel assigned, closes it, and hands the number to
// whatever is about to bind. That certifies the TCP half. Anything binding
// BOTH halves on the same number can still fail, because the kernel allocates
// the two independently and the UDP half may be in use.
//
// Under a single package that is rare. Under `go test ./...`, which runs
// package binaries concurrently and churns hundreds of ports, it is frequent
// enough to fail a run - and it presents as an unrelated flaky test rather
// than as a port problem, which is what makes it expensive.
//
// The residual race is not removable here: the sockets must be closed before
// the caller can bind, and the kernel may reissue the number in between. A
// caller that opens a real transport should retry a bind conflict on a fresh
// port rather than treating this as a guarantee.
package testport

import (
	"fmt"
	"net"
	"testing"
)

// Free returns a loopback port free on BOTH TCP and UDP, retrying until it
// finds one. It fails the test rather than returning a port it could not
// certify, because handing back an unusable number turns a port problem into a
// mystery in whatever binds it next.
func Free(t testing.TB) int {
	t.Helper()
	for range 16 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			continue
		}
		port := l.Addr().(*net.TCPAddr).Port
		udp, uerr := net.ListenPacket("udp", Addr(port))
		_ = l.Close()
		if uerr != nil {
			continue
		}
		_ = udp.Close()
		return port
	}
	t.Fatal("testport.Free: exhausted 16 attempts to find a port free on both TCP and UDP")
	return 0
}

// Addr renders a loopback bind address for port.
func Addr(port int) string { return fmt.Sprintf("127.0.0.1:%d", port) }
