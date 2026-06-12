//go:build chaos && slatedb

package chaos

// The REAL slatedb factory provider for the chaos soak. It is the production
// realization of the factoryProvider seam: instead of the in-memory
// shared-backing double, every node's Handle is a real slatedb BackendFactory
// over ONE shared MinIO bucket. A node "kill" stops the goroutine, but the dead
// node's units are real slatedb databases in MinIO, so a survivor's re-OpenUnit
// (the handoff) reads the dead node's acked bytes back FROM OBJECT STORAGE -
// which is what proves DURABLE-handoff losslessness rather than merely the
// in-memory-shared-map losslessness the double can show.
//
// Gated chaos && slatedb: needs cgo + the native slatedb lib + a running MinIO,
// so it never builds under plain -tags chaos and never under go test ./....
//
// Per-seed isolation: the durable state is the bucket. Reset rotates the
// Backing's KeyPrefix to a fresh value so one seed's leftover unit databases
// never leak into the next seed's run (the in-memory double gets this for free
// by constructing a new Backing). The bucket itself is created once for the
// whole test and torn down on Close.

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Zamua/shale/backends/slate"
	"github.com/Zamua/shale/pkg/storageunit"
)

// slateProviderConfig captures the MinIO connection parameters the slate
// provider needs. The chaos entrypoint fills it from the same env the direct
// factory integration tests use.
type slateProviderConfig struct {
	Endpoint  string // "http://host:port"
	AccessKey string
	SecretKey string
	Bucket    string // shared bucket (created by the provider; torn down on Close)
}

// slateFactoryProvider is the slatedb-backed factoryProvider. It holds the
// MinIO connection params + a minio client (for bucket lifecycle), and an
// atomically-rotated KeyPrefix so each seed runs against a clean sub-namespace
// of the one bucket. The live *slate.Backing is rebuilt on each Reset against the
// current prefix; NewHandle returns a per-node Handle off it.
type slateFactoryProvider struct {
	cfg     slateProviderConfig
	host    string // "host:port" for the minio client
	mc      *minio.Client
	seedGen atomic.Int64 // rotated per Reset to give each seed a fresh KeyPrefix

	backing *slate.Backing // current backing (rebuilt each Reset)
}

// newSlateFactoryProvider connects to MinIO and returns a provider over an
// ALREADY-CREATED shared bucket (the caller owns the bucket lifecycle - create
// before, tear down after - so it stays in one place; see the test's
// makeSlateBucket + t.Cleanup). The provider is ready for the first Reset, which
// rotates the per-seed KeyPrefix and builds the live *slate.Backing.
func newSlateFactoryProvider(cfg slateProviderConfig) (*slateFactoryProvider, error) {
	host := cfg.Endpoint
	for _, p := range []string{"http://", "https://"} {
		host = strings.TrimPrefix(host, p)
	}
	mc, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, fmt.Errorf("slate provider: minio client: %w", err)
	}
	return &slateFactoryProvider{cfg: cfg, host: host, mc: mc}, nil
}

// Reset rotates the KeyPrefix to a fresh sub-namespace and rebuilds the
// *slate.Backing against it, so the upcoming seed's units land at a clean prefix
// with no leftover databases (and no stale durable writer-epoch) from a prior
// seed. The bucket is reused; only the prefix changes.
func (p *slateFactoryProvider) Reset() error {
	gen := p.seedGen.Add(1)
	prefix := fmt.Sprintf("seed%d/", gen)
	b, err := slate.NewBacking(slate.BackingConfig{
		Bucket:    p.cfg.Bucket,
		Endpoint:  p.cfg.Endpoint,
		Region:    "us-east-1",
		AccessKey: p.cfg.AccessKey,
		SecretKey: p.cfg.SecretKey,
		UseSSL:    false,
		KeyPrefix: prefix,
	})
	if err != nil {
		return fmt.Errorf("slate provider: new backing: %w", err)
	}
	p.backing = b
	return nil
}

// NewHandle returns a fresh per-node slatedb factory handle over the current
// backing. Every node in one seed's cluster shares that backing (same bucket,
// same prefix), so a unit's bytes live at a fixed object-store prefix that
// whichever node currently owns the lease opens - the copy-free handoff.
func (p *slateFactoryProvider) NewHandle() storageunit.BackendFactory {
	return p.backing.Handle()
}

// Close is a no-op: the bucket lifecycle is owned by the test (makeSlateBucket +
// t.Cleanup remove every object + the bucket), so the provider does not delete it
// here. Kept to satisfy the factoryProvider interface. The held minio client
// needs no explicit close.
func (p *slateFactoryProvider) Close() error { return nil }

// compile-time assertion the slate provider satisfies the seam.
var _ factoryProvider = (*slateFactoryProvider)(nil)
