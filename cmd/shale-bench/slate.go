// Slate backend wiring for the comparative bench harness.
//
// This file is gated behind the `slatedb` build tag. The default
// `go build ./cmd/shale-bench/` (no tags, CGO disabled) does NOT
// include this file, and the harness rejects --backend=slate with a
// clear "requires -tags slatedb build" error. To build a slate-aware
// harness binary:
//
//	CGO_ENABLED=1 \
//	CGO_LDFLAGS="-L/path/to/slatedb/target/release" \
//	DYLD_LIBRARY_PATH=/path/to/slatedb/target/release \
//	go build -tags slatedb -o shale-bench ./cmd/shale-bench/
//
// # Configuration via env vars
//
// All connection parameters come from process env so the per-node
// constructor stays parameter-free (and so the same bench scripts can
// drive raw/pebble/memory without ever touching slate config):
//
//	SHALE_BENCH_S3_ENDPOINT     e.g. "http://127.0.0.1:9000". Required.
//	SHALE_BENCH_S3_BUCKET       e.g. "shale-bench". Must already exist.
//	SHALE_BENCH_S3_REGION       default "us-east-1".
//	SHALE_BENCH_S3_ACCESS_KEY   required for non-anonymous stores.
//	SHALE_BENCH_S3_SECRET_KEY   required for non-anonymous stores.
//	SHALE_BENCH_S3_USE_SSL      "true" enforces TLS; default false
//	                            (MinIO over plaintext http).
//
// # Multi-node store layout
//
// SlateDB enforces a single writer per (bucket, DbName) via the
// writer-epoch protocol: a second open of the same store fences the
// first. For multi-node bench scenarios that share one process, each
// node MUST get its own DbName so the nodes don't fence each other
// mid-bench. We use the node id (already unique per cluster: n1, n2,
// n3) as a DbName suffix: e.g. "shale-bench-n1", "shale-bench-n2".
// All nodes share one bucket; the per-DbName key prefix isolates them.
//
// # Env-var collision caveat
//
// backends/slate.New writes AWS_* env vars before resolving the
// object store. That means all slate stores in this process must
// point at the SAME bucket + endpoint + credentials (per the package
// doc on slate.go). Picking different DbNames inside one bucket is
// safe; pointing two stores at different buckets in one process is
// not, and would be a misuse of the harness regardless.

//go:build slatedb

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/Zamua/shale/backends/slate"
	"github.com/Zamua/shale/pkg/backend"
	slatedb "slatedb.io/slatedb-go/uniffi"
)

// slateEnvPrefix is the namespace for slate-bench env vars. Kept in
// one place so the docstring above and the lookups below cannot drift.
const slateEnvPrefix = "SHALE_BENCH_S3_"

func init() {
	// Replace the no-tag stub with the real slate constructor. Runs
	// at package init so the dispatch from openBackend in main.go is
	// in place before any --backend=slate scenario fires.
	openSlateBackend = newSlateBackend
}

// buildSlateWriteOptions returns a *slatedb.WriteOptions for the given
// awaitDurable flag, or nil for the slatedb-default path (which is
// itself AwaitDurable=true). Surfaced as a package-level helper so
// main.go's --slate-await-durable flag can plumb the choice through
// slateWriteOpts without main.go having to import slatedb directly
// (that import is gated behind the slatedb build tag).
func buildSlateWriteOptions(awaitDurable bool) any {
	return &slatedb.WriteOptions{AwaitDurable: awaitDurable}
}

func init() {
	// Bind the typed-nil helper to the untyped factory in main.go so the
	// no-tag build doesn't need to know slatedb exists.
	makeSlateWriteOptions = buildSlateWriteOptions
}

// newSlateBackend opens one SlateDB instance against the configured
// S3-compatible endpoint, using `id` as the DbName so each cluster
// node gets its own store + writer epoch inside the shared bucket.
func newSlateBackend(id string) (backend.Backend, error) {
	endpoint := os.Getenv(slateEnvPrefix + "ENDPOINT")
	if endpoint == "" {
		return nil, fmt.Errorf("slate: %sENDPOINT env var required (e.g. http://127.0.0.1:9000)", slateEnvPrefix)
	}
	bucket := os.Getenv(slateEnvPrefix + "BUCKET")
	if bucket == "" {
		return nil, fmt.Errorf("slate: %sBUCKET env var required", slateEnvPrefix)
	}
	region := os.Getenv(slateEnvPrefix + "REGION")
	if region == "" {
		region = "us-east-1"
	}
	accessKey := os.Getenv(slateEnvPrefix + "ACCESS_KEY")
	secretKey := os.Getenv(slateEnvPrefix + "SECRET_KEY")
	useSSL := strings.EqualFold(os.Getenv(slateEnvPrefix+"USE_SSL"), "true")

	// DbName carries the node id so multi-node scenarios don't fence
	// each other. The "shale-bench-" prefix keeps the keys easy to
	// recognize when poking at the bucket directly.
	dbName := "shale-bench-" + id

	cfg := slate.Config{
		Bucket:    bucket,
		DbName:    dbName,
		Endpoint:  endpoint,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    useSSL,
	}
	// slateWriteOpts is the cross-build-tag handoff from main.go's
	// --slate-await-durable flag. Nil means "stick with slatedb's
	// default (AwaitDurable=true)"; non-nil is a *slatedb.WriteOptions
	// constructed via buildSlateWriteOptions above.
	if slateWriteOpts != nil {
		if wo, ok := slateWriteOpts.(*slatedb.WriteOptions); ok {
			cfg.WriteOptions = wo
		} else {
			return nil, fmt.Errorf("slate: internal bench wiring error, slateWriteOpts has wrong type %T", slateWriteOpts)
		}
	}
	return slate.New(cfg)
}
