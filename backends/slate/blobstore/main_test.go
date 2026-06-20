package blobstore

// Package-level TestMain for the blobstore test binary.
//
// goleak.VerifyTestMain catches goroutine leaks that survive the last
// test's t.Cleanup. The MinIO adapter's GetStream returns a reader
// backed by a minio-go body bound to the request context; Close must
// release it. A reader left unclosed (or a ctx that outlives the
// stream) shows up here as a residue.
//
// The two ignores below cover the ONE well-behaved third-party leaker:
// minio-go shares a single *http.Transport whose keep-alive connection
// pool parks idle persistConn read/write loops for reuse. minio-go
// exposes no Client.Close, so those pooled idle connections are still
// alive when goleak samples once at binary exit. They are NOT a real
// leak (the stdlib reaps idle keep-alives on its own timer) and they
// are keyed to the idle-pool functions specifically, so a genuinely
// unclosed GetStream body - which blocks DEEPER in the GET read path,
// not in the idle pool - would still fail goleak here.

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// minio-go's shared http.Transport keep-alive connection pool:
		// idle persistConn loops parked for reuse, never closed (no
		// minio Client.Close). Reaped by the stdlib's idle timer, not a
		// leak of this adapter's own goroutines. AnyFunction (not
		// TopFunction): an idle readLoop blocked in a network read has
		// internal/poll.runtime_pollWait as its TOP frame and persistConn
		// .readLoop only DEEPER in the stack, so the top-frame form would
		// miss it (the goleakignore-package lesson). writeLoop parks in a
		// select with itself on top, but AnyFunction matches that too.
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}
