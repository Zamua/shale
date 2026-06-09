// Command shaled-slate runs a single shale node with the SlateDB
// (object-storage-backed) backend. See pkg/shaled for the shared
// run-loop helper; this main is the slate-specific thin shell: it
// registers the slate-* flags, opens a *slate.Slate, and hands the
// resulting backend.Backend to shaled.Run.
//
// Build/runtime requirements: the slate backend depends on the
// cgo-backed slatedb.io/slatedb-go binding. shaled-slate is gated
// behind the `slatedb` build tag for that reason; building requires
// CGO + the slatedb cdylib on the loader path:
//
//	CGO_ENABLED=1 \
//	CGO_LDFLAGS="-L/path/to/slatedb/target/release" \
//	DYLD_LIBRARY_PATH=/path/to/slatedb/target/release \
//	go build -tags slatedb -o shaled-slate ./cmd/shaled-slate
//
// (run from the backends/slate module root)
//
// Without the build tag, the binary falls back to backend_default.go
// which prints an instructive rebuild error and exits non-zero.
//
// Flags (in addition to the standard cluster/node flags exposed by
// pkg/shaled.BindStdFlags):
//
//	--slate-bucket       SHALE_SLATE_BUCKET      S3 bucket name (required)
//	--slate-db-name      SHALE_SLATE_DB_NAME     logical DB name (required)
//	--slate-endpoint     SHALE_SLATE_ENDPOINT    S3-compatible endpoint URL
//	--slate-region       SHALE_SLATE_REGION      AWS region (default us-east-1)
//	--slate-access-key   SHALE_SLATE_ACCESS_KEY  access key ID (required)
//	--slate-secret-key   SHALE_SLATE_SECRET_KEY  secret access key (required)
//	--slate-use-ssl      SHALE_SLATE_USE_SSL     TLS to endpoint (default true)
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Zamua/shale/pkg/shaled"
)

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "shaled-slate: %v\n", err)
		os.Exit(1)
	}
}

// slateConfig mirrors the fields the slate backend needs. Held in
// its own struct so the slate build-tag stub and real impl can share
// the shape without main.go importing backends/slate directly (the
// stub path must compile WITHOUT cgo / WITHOUT the slatedb runtime).
type slateConfig struct {
	Bucket    string
	DbName    string
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
	UseSSL    bool
}

func run(argv []string, stderr *os.File) error {
	logger := log.New(stderr, "", log.LstdFlags|log.Lmicroseconds)

	fs := flag.NewFlagSet("shaled-slate", flag.ContinueOnError)
	std := shaled.BindStdFlags(fs)

	slateBucket := fs.String("slate-bucket", shaled.EnvOr("SHALE_SLATE_BUCKET", ""),
		"slate: object-storage bucket (required)")
	slateDbName := fs.String("slate-db-name", shaled.EnvOr("SHALE_SLATE_DB_NAME", ""),
		"slate: logical database name (required)")
	slateEndpoint := fs.String("slate-endpoint", shaled.EnvOr("SHALE_SLATE_ENDPOINT", ""),
		"slate: S3-compatible endpoint URL (empty for AWS S3)")
	slateRegion := fs.String("slate-region", shaled.EnvOr("SHALE_SLATE_REGION", "us-east-1"),
		"slate: object-storage region")
	slateAccessKey := fs.String("slate-access-key", shaled.EnvOr("SHALE_SLATE_ACCESS_KEY", ""),
		"slate: access key ID (required)")
	slateSecretKey := fs.String("slate-secret-key", shaled.EnvOr("SHALE_SLATE_SECRET_KEY", ""),
		"slate: secret access key (required)")
	slateUseSSL := fs.String("slate-use-ssl", shaled.EnvOr("SHALE_SLATE_USE_SSL", "true"),
		"slate: use TLS to reach the endpoint (true|false)")

	if err := fs.Parse(argv); err != nil {
		return err
	}
	if err := std.Validate(); err != nil {
		return err
	}

	cfg := slateConfig{
		Bucket:    *slateBucket,
		DbName:    *slateDbName,
		Endpoint:  *slateEndpoint,
		Region:    *slateRegion,
		AccessKey: *slateAccessKey,
		SecretKey: *slateSecretKey,
		UseSSL:    strings.EqualFold(*slateUseSSL, "true"),
	}
	if err := cfg.validate(); err != nil {
		return err
	}

	be, closeBackend, err := openSlateBackend(cfg, logger)
	if err != nil {
		return err
	}

	return shaled.Run(shaled.RunConfig{
		Std:          *std,
		BackendLabel: "slate",
		Backend:      be,
		CloseBackend: closeBackend,
		Logger:       logger,
	})
}

func (c slateConfig) validate() error {
	if c.Bucket == "" {
		return errors.New("--slate-bucket is required (or set SHALE_SLATE_BUCKET)")
	}
	if c.DbName == "" {
		return errors.New("--slate-db-name is required (or set SHALE_SLATE_DB_NAME)")
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return errors.New("--slate-access-key and --slate-secret-key are required (or set SHALE_SLATE_{ACCESS,SECRET}_KEY)")
	}
	return nil
}

// openSlateBackend is supplied by one of the build-tag-gated
// sibling files in this directory:
//
//   - backend_slatedb.go (//go:build slatedb): opens a real
//     *slate.Slate against the configured object store.
//   - backend_default.go (//go:build !slatedb): fails fast with a
//     "rebuild with -tags slatedb" error so a tag-less build of
//     this directory still compiles and produces a binary that
//     refuses to run rather than silently misbehaving.
//
// Only one is in any given build. main.go references the symbol
// without knowing which file defined it.
