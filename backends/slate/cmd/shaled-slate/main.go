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
//	--slate-db-name      SHALE_SLATE_DB_NAME     logical DB name (single-backend; required there)
//	--slate-endpoint     SHALE_SLATE_ENDPOINT    S3-compatible endpoint URL
//	--slate-region       SHALE_SLATE_REGION      AWS region (default us-east-1)
//	--slate-access-key   SHALE_SLATE_ACCESS_KEY  access key ID (required)
//	--slate-secret-key   SHALE_SLATE_SECRET_KEY  secret access key (required)
//	--slate-use-ssl      SHALE_SLATE_USE_SSL     TLS to endpoint (default true)
//	--slate-key-prefix   SHALE_SLATE_KEY_PREFIX  shared-bucket prefix (multi-backend only)
//	--slate-relaxed-durability SHALE_SLATE_RELAXED_DURABILITY  R>=2 replica relaxed durability (default false)
//	--multi-backend      SHALE_MULTI_BACKEND     one slatedb per unit (default false)
//	--coord-key          SHALE_COORD_KEY         membership-document key (multi-backend only; default cas.DefaultKey)
//
// In multi-backend mode (--multi-backend=true, paired with the std
// --unit-count > 1), the binary builds a slate Backing over the shared
// bucket and routes each key to its storage unit's owner; --slate-db-name
// is ignored (per-unit DbNames are derived from each GenUnit). The
// multi-backend constructor is also slatedb-tagged (real slatedb), so a
// tag-less build fails fast on it the same way the single backend does.
//
// Multi-backend mode is also MULTI-NODE: membership rides the SAME bucket
// through the CAS coordinator (pkg/coord/cas) over the conditional store,
// so nodes need no extra port, no seed list - every node points at the
// bucket and coordinates through the membership document at --coord-key.
// Single-backend mode is single-node only.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Zamua/shale/pkg/coord/cas"
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

	// MultiBackend selects the v0.8 multi-backend run path: instead of
	// ONE slate.Slate, the binary builds a slate Backing over the shared
	// bucket and hands a per-node Handle (a storageunit.BackendFactory) to
	// shaled.Run. DbName is IGNORED in this mode (per-unit DbNames are
	// derived from each GenUnit by the factory); KeyPrefix + UnitCount apply.
	MultiBackend bool

	// KeyPrefix is the shared-bucket key prefix the multi-backend factory
	// namespaces its per-unit databases under (so one bucket can host
	// multiple unrelated clusters). Read only when MultiBackend is true;
	// the single-backend slate.Config uses DbName directly and has no
	// KeyPrefix.
	KeyPrefix string

	// UnitCount is the validated power-of-two unit count N for multi-backend
	// mode (carried over from the std --unit-count flag). Consumed only when
	// MultiBackend is true.
	UnitCount storageunit.UnitCount

	// RelaxedDurability opts the multi-backend R>=2 replica path into relaxed
	// durability (WriteOptions{AwaitDurable:false}): a write acks at memtable
	// insert and is carried to the bucket by the background WAL flush, so
	// durability comes from REPLICATION rather than a per-write object-store
	// round-trip. Safe only at R>=2 (the peer replica's memtable holds the
	// write if one replica crashes pre-flush); ignored on the R=1 path, which
	// stays strict. Consumed only when MultiBackend is true. Default false.
	RelaxedDurability bool
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
		"slate: shared-bucket key prefix for per-unit databases (multi-backend mode only)")
	multiBackend := fs.String("multi-backend", shaled.EnvOr("SHALE_MULTI_BACKEND", "false"),
		"run in multi-backend mode: one slatedb per storage unit over a shared bucket (true|false)")
	coordKey := fs.String("coord-key", shaled.EnvOr("SHALE_COORD_KEY", ""),
		"membership-document key for the CAS coordinator (multi-backend mode only; empty = the well-known default)")
	slateRelaxedDurability := fs.String("slate-relaxed-durability", shaled.EnvOr("SHALE_SLATE_RELAXED_DURABILITY", "false"),
		"slate: relaxed durability on the R>=2 replica path - ack at memtable insert, durable via background WAL flush (true|false). Safe only at R>=2")

	if err := fs.Parse(argv); err != nil {
		return err
	}
	if err := std.Validate(); err != nil {
		return err
	}

	cfg := slateConfig{
		Bucket:            *slateBucket,
		DbName:            *slateDbName,
		Endpoint:          *slateEndpoint,
		Region:            *slateRegion,
		AccessKey:         *slateAccessKey,
		SecretKey:         *slateSecretKey,
		UseSSL:            strings.EqualFold(*slateUseSSL, "true"),
		MultiBackend:      strings.EqualFold(*multiBackend, "true"),
		KeyPrefix:         *slateKeyPrefix,
		UnitCount:         std.UnitCount,
		RelaxedDurability: strings.EqualFold(*slateRelaxedDurability, "true"),
	}
	if err := cfg.validate(); err != nil {
		return err
	}

	if cfg.MultiBackend {
		// Multi-backend mode: build a slate Backing over the shared bucket
		// and hand a per-node Handle (storageunit.BackendFactory) to
		// shaled.Run. The unit count comes from the std --unit-count flag.
		factory, closeFactory, err := openSlateFactory(cfg, logger)
		if err != nil {
			return err
		}
		// The shared CAS arbiter over the same bucket drives the homogeneous
		// try-join-else-form bootstrap (the __cluster/init form-lock + durable
		// generation) so every pod runs one identical config in a single
		// StatefulSet, no seed wart. nil would fall back to the legacy
		// seed-RPC bootstrap.
		condStore, err := openSlateCondStore(cfg)
		if err != nil {
			return err
		}
		logger.Printf("shaled-slate: homogeneous bootstrap armed (conditional store over bucket=%s key-prefix=%q)",
			cfg.Bucket, cfg.KeyPrefix)
		// Membership rides the same conditional store: the CAS coordinator's
		// document is this cluster's one truth channel, so a node needs only
		// the bucket credentials to find its peers. An empty Key selects
		// cas.DefaultKey.
		coordinator := cas.New(cas.Config{
			Store: condStore,
			Key:   *coordKey,
		})
		return shaled.Run(shaled.RunConfig{
			Std:              *std,
			BackendLabel:     "slate-multi",
			BackendFactory:   factory,
			CloseFactory:     closeFactory,
			ConditionalStore: condStore,
			Coordinator:      coordinator,
			Logger:           logger,
		})
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
	// --slate-db-name is required ONLY in single-backend mode. In
	// multi-backend mode per-unit DbNames are derived from each GenUnit by
	// the factory, so DbName is ignored and need not be set.
	if !c.MultiBackend && c.DbName == "" {
		return errors.New("--slate-db-name is required (or set SHALE_SLATE_DB_NAME)")
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return errors.New("--slate-access-key and --slate-secret-key are required (or set SHALE_SLATE_{ACCESS,SECRET}_KEY)")
	}
	return nil
}

// openSlateBackend (single-backend) + openSlateFactory (multi-backend)
// are supplied by one of the build-tag-gated sibling files in this
// directory:
//
//   - backend_slatedb.go (//go:build slatedb): opens a real *slate.Slate
//     (single) / a real slate.Backing + per-node Handle (multi) against
//     the configured object store.
//   - backend_default.go (//go:build !slatedb): both fail fast with a
//     "rebuild with -tags slatedb" error so a tag-less build of this
//     directory still compiles and produces a binary that refuses to run
//     rather than silently misbehaving.
//
// Only one is in any given build. main.go references the symbols without
// knowing which file defined them. openSlateFactory returns a
// storageunit.BackendFactory + a CloseFactory func.
