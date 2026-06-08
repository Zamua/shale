//go:build slatedb

package slate

import (
	"errors"
	"os"
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

	// DbName is the logical SlateDB instance name. Becomes the key
	// prefix inside Bucket; lets multiple shale nodes share one
	// bucket. Required.
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
func (c Config) applyEnv() {
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
}
