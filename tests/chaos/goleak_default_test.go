//go:build chaos && !slatedb

package chaos

import "go.uber.org/goleak"

// extraGoleakOptions is EMPTY for the default (in-memory shared-backing) soak:
// the in-memory stack spins up no HTTP client and no cgo runtime, so the shared
// goleakignore.Options() canonical list is exhaustive and the leak check stays
// tight (a real leaked Cluster goroutine still fails). The slatedb-tagged build
// replaces this with the minio-go + slatedb-go background-goroutine ignores (see
// goleak_slate_test.go).
var extraGoleakOptions []goleak.Option
