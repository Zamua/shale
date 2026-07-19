//go:build slatedb

package slate

import (
	"errors"
	"os"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

// Config captures the connection parameters for the SlateDB backend.
// New() writes the relevant fields to AWS_* process env vars before
// resolving the object store: SlateDB's underlying OpenDAL/object_store
// crate reads them from the environment and ignores URL query params.
//
// For tests + local dev that don't need S3, use NewWithStore directly
// with a "memory:///" ObjectStore; the env-var dance is then skipped.
type Config struct {
	// Bucket is the S3 bucket name. Required.
	Bucket string

	// DbName is the logical SlateDB instance name, and becomes the key
	// prefix inside Bucket. Required.
	//
	// IT IDENTIFIES A CLUSTER, NOT A NODE. Every node in one cluster
	// MUST pass the SAME DbName: under the unit model the per-unit
	// databases hang beneath this prefix ("<DbName>u/g<gen>/u<id>", see
	// dbname.go), so two nodes given different DbNames address disjoint
	// prefix trees. They will not error - each simply opens its own
	// empty units and cannot see the other's data.
	//
	// Distinct DbNames therefore mean distinct logical CLUSTERS sharing
	// one bucket, which is a supported way to isolate deployments. What
	// it is NOT is a way to distinguish peers within a cluster.
	DbName string

	// Endpoint is the S3 endpoint URL (e.g. "http://minio:9000").
	// Empty for AWS S3 (the SDK picks the standard endpoint).
	Endpoint string

	// Region is the AWS region. Defaults to "us-east-1" when empty.
	Region string

	// AccessKey + SecretKey are S3 credentials. Required when the
	// target store doesn't accept anonymous access (i.e. always for
	// real deployments).
	AccessKey string
	SecretKey string

	// UseSSL toggles AWS_ALLOW_HTTP. When false (the default), the
	// underlying object_store crate is told plaintext HTTP is OK
	// (suitable for MinIO at http://...). When true, the crate
	// enforces TLS, matching real AWS S3.
	UseSSL bool

	// Settings is forwarded verbatim to slatedb. Nil = slatedb
	// defaults (the same DbBuilder.Build() path slate.New used
	// pre-v0.5).
	//
	// The shale layer never reads, mutates, copies, or validates this
	// value. Operator owns the lifecycle: build the *slatedb.Settings
	// (slatedb.SettingsDefault, SettingsFromFile, SettingsFromEnv,
	// SettingsFromJsonString, ...), mutate via Settings.Set(path,
	// jsonValue), pass it here. slate.New hands it to the DbBuilder
	// before opening.
	//
	// CAVEAT (slatedb-go v0.13.1 binding): the Settings handle is an
	// opaque uniffi object whose only mutation API is `Set(key
	// string, valueJson string) error` using dotted JSON paths
	// (e.g. `Set("flush_interval", "\"250ms\"")`). The Rust crate's
	// per-field accessors are NOT yet exposed in Go. AwaitDurable in
	// particular is a per-write knob on slatedb.WriteOptions and is
	// NOT settable through Settings at all in this binding; reach it
	// via WriteOptions below.
	Settings *slatedb.Settings

	// WriteOptions, if non-nil, is applied to every Put/Delete and to
	// every transaction Commit via slatedb's *WithOptions APIs. Nil =
	// slatedb defaults (AwaitDurable=true): Put/Delete route through
	// plain db.Put/db.Delete and tx.Commit calls plain tx.Commit, the
	// same path slate used pre-WriteOptions pass-through.
	//
	// Set AwaitDurable=false to opt into relaxed durability mode (ack
	// at memtable insert, eventually durable via the background WAL
	// flush). Pair with cluster.Config.ReplicationFactor >= 2; R=1
	// relaxed is unsafe. See README "Relaxed durability mode" and the
	// shale spec section "Backend durability is a backend concern".
	//
	// Shale never reads, mutates, copies, or validates this value. It
	// is passed by-value to the slatedb-go *WithOptions APIs at each
	// call site (slatedb's WriteOptions is a small struct, not a
	// handle), so the operator-supplied pointer is dereferenced once
	// per write; the operator may keep ownership and mutate it across
	// the backend's lifetime if they want runtime-tunable durability,
	// though doing so races with concurrent writes and is not
	// recommended.
	WriteOptions *slatedb.WriteOptions

	// Cache, if non-nil, is the slatedb SST block + metadata cache the
	// DbBuilder is given via WithDbCache before Build. Nil = slatedb's
	// default (NO block cache: every read re-fetches SST blocks from the
	// object store). On an object-store backend that is pathological -
	// repeated point reads of the same hot SSTs each pay an object GET -
	// so any latency-sensitive deployment should pass a cache here.
	//
	// Build one with slatedb.DbCacheNewMokaCache (in-memory) or
	// DbCacheNewFoyerCache (in-memory + local-disk spill); a Moka cache
	// of a few hundred MiB holds the hot SST working set in RAM and
	// eliminates the steady re-read storm against the object store.
	//
	// Shale never reads, mutates, copies, or validates this handle: the
	// operator owns its lifecycle (same as Settings / WriteOptions). The
	// binding's WithDbCache clones the underlying Arc, so ONE cache may
	// be shared across multiple slate backends / Db handles on a node;
	// the operator keeps the original and Destroys it once on shutdown.
	Cache *slatedb.DbCache
}

func (c Config) validate() error {
	if c.Bucket == "" {
		return errors.New("slate: Config.Bucket required")
	}
	if c.DbName == "" {
		return errors.New("slate: Config.DbName required")
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.Region == "" {
		c.Region = "us-east-1"
	}
}

// applyEnv writes the AWS_* env vars the underlying object_store crate
// reads. Always uses path-style addressing (bucket.host virtual-hosted
// style doesn't work against most non-AWS S3-compatibles without
// custom DNS, and is harmless on AWS proper).
//
// The env vars are PROCESS-GLOBAL, so before writing anything it
// registers the config with the per-process guard (envguard.go): a
// construction whose object-store config CONFLICTS with one this process
// already applied fails fast here with a config-conflict error instead of
// silently clobbering the earlier env (and authenticating requests with
// the wrong credentials). An identical repeat registers cleanly.
func (c Config) applyEnv() error {
	if err := registerEnvConfig(envConfigTuple{
		endpoint:  c.Endpoint,
		region:    c.Region,
		accessKey: c.AccessKey,
		secretKey: c.SecretKey,
		useSSL:    c.UseSSL,
	}); err != nil {
		return err
	}
	if c.Endpoint != "" {
		os.Setenv("AWS_ENDPOINT_URL", c.Endpoint)
	}
	os.Setenv("AWS_REGION", c.Region)
	if c.AccessKey != "" {
		os.Setenv("AWS_ACCESS_KEY_ID", c.AccessKey)
	}
	if c.SecretKey != "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", c.SecretKey)
	}
	if !c.UseSSL {
		os.Setenv("AWS_ALLOW_HTTP", "true")
	}
	os.Setenv("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false")
	return nil
}
