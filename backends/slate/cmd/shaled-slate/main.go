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
//	--slate-db-name      SHALE_SLATE_DB_NAME     logical DB name (required in legacy mode)
//	--slate-endpoint     SHALE_SLATE_ENDPOINT    S3-compatible endpoint URL
//	--slate-region       SHALE_SLATE_REGION      AWS region (default us-east-1)
//	--slate-access-key   SHALE_SLATE_ACCESS_KEY  access key ID (required)
//	--slate-secret-key   SHALE_SLATE_SECRET_KEY  secret access key (required)
//	--slate-use-ssl      SHALE_SLATE_USE_SSL     TLS to endpoint (default true)
//	--slate-key-prefix   SHALE_SLATE_KEY_PREFIX  bucket key prefix (multi-backend; default empty)
//	--unit-count         SHALE_UNIT_COUNT        storage units N (power of two; default 1)
//
// Two backend modes selected by --unit-count:
//
//   - --unit-count=1 (default): LEGACY single-backend mode. Opens ONE
//     slatedb at --slate-db-name and serves it as the node's single
//     Backend. Byte-for-byte the established behavior; existing deploys
//     are unaffected.
//   - --unit-count>1: MULTI-BACKEND mode (the v0.8 per-shard
//     lease-handoff model). Constructs a slate Backing over the shared
//     bucket and mounts one slatedb PER owned storage unit, keyed by
//     GenUnit. --slate-db-name is IGNORED (per-unit DbNames are derived
//     from the GenUnit); --slate-key-prefix namespaces the cluster's
//     units inside the shared bucket. This is the deployable form of the
//     model that supports cluster-wide doubling reshards.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/Zamua/shale/pkg/shaled"
	"github.com/Zamua/shale/pkg/storageunit"
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

	// KeyPrefix namespaces the cluster's per-unit databases inside the
	// shared bucket (multi-backend mode only). Empty in legacy mode and
	// for single-cluster buckets. When set it should end in "/".
	KeyPrefix string
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
	slateKeyPrefix := fs.String("slate-key-prefix", shaled.EnvOr("SHALE_SLATE_KEY_PREFIX", ""),
		"slate: key prefix namespacing this cluster's units inside the shared bucket (multi-backend mode; default empty)")
	unitCountRaw := fs.String("unit-count", shaled.EnvOr("SHALE_UNIT_COUNT", "1"),
		"storage units N (power of two): 1 = legacy single-backend, >1 = multi-backend lease-handoff mode")

	if err := fs.Parse(argv); err != nil {
		return err
	}
	if err := std.Validate(); err != nil {
		return err
	}

	// Parse + validate the unit count up front (a power of two in range,
	// enforced by storageunit.NewUnitCount). This decides which backend
	// mode the node runs.
	n, err := strconv.Atoi(strings.TrimSpace(*unitCountRaw))
	if err != nil {
		return fmt.Errorf("--unit-count %q is not an integer: %w", *unitCountRaw, err)
	}
	unitCount, err := storageunit.NewUnitCount(n)
	if err != nil {
		return fmt.Errorf("--unit-count invalid: %w", err)
	}

	cfg := slateConfig{
		Bucket:    *slateBucket,
		DbName:    *slateDbName,
		Endpoint:  *slateEndpoint,
		Region:    *slateRegion,
		AccessKey: *slateAccessKey,
		SecretKey: *slateSecretKey,
		UseSSL:    strings.EqualFold(*slateUseSSL, "true"),
		KeyPrefix: *slateKeyPrefix,
	}
	if err := cfg.validate(unitCount); err != nil {
		return err
	}

	// MULTI-BACKEND mode: --unit-count > 1. Construct the slate factory
	// (one slatedb per owned unit over a SHARED bucket) and hand the
	// factory + unit count to shaled.Run, which selects multi-backend
	// mode in cluster.Open. --slate-db-name is ignored here (per-unit
	// DbNames are factory-derived from the GenUnit).
	if unitCount.N() > 1 {
		fac, closeFactory, err := openSlateFactory(cfg, logger)
		if err != nil {
			return err
		}
		return shaled.Run(shaled.RunConfig{
			Std:            *std,
			BackendLabel:   "slate",
			BackendFactory: fac,
			UnitCount:      unitCount,
			CloseBackend:   closeFactory,
			Logger:         logger,
		})
	}

	// LEGACY single-backend mode: --unit-count == 1 (the default).
	// Unchanged from the established behavior - one slatedb at
	// --slate-db-name served as the node's single Backend.
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

// validate enforces the required config for the selected backend mode.
// --slate-db-name is required ONLY in legacy mode (unitCount == 1); in
// multi-backend mode per-unit DbNames are derived from the GenUnit, so a
// --slate-db-name is ignored and not required. Bucket + credentials are
// required in both modes.
func (c slateConfig) validate(unitCount storageunit.UnitCount) error {
	if c.Bucket == "" {
		return errors.New("--slate-bucket is required (or set SHALE_SLATE_BUCKET)")
	}
	if unitCount.N() == 1 && c.DbName == "" {
		return errors.New("--slate-db-name is required in legacy mode (or set SHALE_SLATE_DB_NAME; not needed when --unit-count > 1)")
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return errors.New("--slate-access-key and --slate-secret-key are required (or set SHALE_SLATE_{ACCESS,SECRET}_KEY)")
	}
	return nil
}

// openSlateBackend (legacy single-backend mode) and openSlateFactory
// (multi-backend mode) are each supplied by one of the build-tag-gated
// sibling files in this directory:
//
//   - backend_slatedb.go (//go:build slatedb): opens a real
//     *slate.Slate (legacy) / a real slate factory Handle over a shared
//     Backing (multi-backend) against the configured object store.
//   - backend_default.go (//go:build !slatedb): both fail fast with a
//     "rebuild with -tags slatedb" error so a tag-less build of this
//     directory still compiles and produces a binary that refuses to run
//     rather than silently misbehaving.
//
// Only one set is in any given build. main.go references the symbols
// without knowing which file defined them.
