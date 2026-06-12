//go:build chaos && slatedb

package chaos

import "go.uber.org/goleak"

// extraGoleakOptions extends the canonical ignore list with the background
// goroutines a REAL slatedb-over-MinIO run legitimately leaves at process exit.
// These are NOT Cluster leaks - they are the object-store HTTP client's
// keep-alive connection pool and slatedb's own async rust runtime, neither of
// which the chaos teardown can deterministically join (idle keep-alive conns are
// closed on a transport timer, not synchronously; an OpenUnit in flight on a node
// being torn down leaves its uniffi async call parked until the rust side
// returns). The in-memory soak never sees these (no HTTP, no cgo), so they are
// scoped to the slatedb build only - the in-memory leak check stays tight.
//
// Specified by function-name STRING (goleak takes strings, not the funcs) so this
// core-module test file does not have to import minio-go or slatedb-go.
var extraGoleakOptions = []goleak.Option{
	// minio-go's HTTP transport keep-alive connection pool: a persistent
	// connection's read/write loops linger after the last request until the idle
	// timer fires. The PARKED top frame is internal/poll.runtime_pollWait (a
	// blocking socket read), with readLoop/writeLoop only DEEPER in the stack, so
	// these need the AnyFunction form (IgnoreTopFunction would miss the parked
	// goroutine just as the integration list once missed pushPullTrigger).
	goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
	goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	// slatedb-go's uniffi binding runs DB operations on an async rust runtime; an
	// OpenUnit (DbBuilder.Build) or a flush in flight on a node that is being torn
	// down parks here until the rust side completes. Matched as ANY frame because
	// the parked top frame is the channel receive inside uniffiRustCallAsync.
	goleak.IgnoreAnyFunction("slatedb.io/slatedb-go/uniffi.uniffiRustCallAsync[...]"),
	goleak.IgnoreAnyFunction("slatedb.io/slatedb-go/uniffi.rustCallAsync"),
	goleak.IgnoreAnyFunction("slatedb.io/slatedb-go/uniffi.uniffiRustCallAsyncPoll"),
}
