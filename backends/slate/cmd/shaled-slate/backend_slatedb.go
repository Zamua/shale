// backend_slatedb.go: the real openSlateBackend, built only with
// -tags slatedb. Wires shaled-slate's parsed flags into
// slate.Config (including Settings, which stays at nil here so
// slatedb's own defaults apply; operators who want non-default
// Settings can fork this main and pass cfg.Settings explicitly).

//go:build slatedb

package main

import (
	"fmt"
	"log"

	"github.com/Zamua/shale/backends/slate"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

func openSlateBackend(cfg slateConfig, logger *log.Logger) (backend.Backend, func() error, error) {
	be, err := slate.New(slate.Config{
		Bucket:    cfg.Bucket,
		DbName:    cfg.DbName,
		Endpoint:  cfg.Endpoint,
		Region:    cfg.Region,
		AccessKey: cfg.AccessKey,
		SecretKey: cfg.SecretKey,
		UseSSL:    cfg.UseSSL,
		// Settings is intentionally nil: shaled-slate exposes the
		// "operator runs slate with defaults" surface. Operators who
		// need non-default slatedb Settings build their own shaled-*
		// from this directory as a template.
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open slate: %w", err)
	}
	logger.Printf("shaled-slate: slate backend bucket=%s db=%s endpoint=%s region=%s",
		cfg.Bucket, cfg.DbName, cfg.Endpoint, cfg.Region)
	// Close is a no-op: backend.Backend.Close (invoked by
	// shaled.Run on shutdown) is enough for the slate backend.
	return be, func() error { return nil }, nil
}

// openSlateFactory (multi-backend mode) builds a slate Backing over the
// SHARED bucket and returns a per-node Handle as the cluster's
// storageunit.BackendFactory. Every node in the cluster constructs its
// own Handle off an identical Backing (same bucket + credentials), which
// is what makes a unit's bytes reachable by whichever node currently
// leases it (copy-free handoff). The returned close function releases any
// units this Handle still has mounted on shutdown.
//
// --slate-db-name is intentionally NOT forwarded: per-unit DbNames are
// derived from the GenUnit inside the factory, so a unit's database lives
// at a deterministic prefix under cfg.KeyPrefix in the shared bucket.
func openSlateFactory(cfg slateConfig, logger *log.Logger) (storageunit.BackendFactory, func() error, error) {
	backing, err := slate.NewBacking(slate.BackingConfig{
		Bucket:    cfg.Bucket,
		Endpoint:  cfg.Endpoint,
		Region:    cfg.Region,
		AccessKey: cfg.AccessKey,
		SecretKey: cfg.SecretKey,
		UseSSL:    cfg.UseSSL,
		KeyPrefix: cfg.KeyPrefix,
		// Settings stays nil: shaled-slate exposes the defaults surface
		// (same as the legacy path); operators who need non-default
		// slatedb Settings build their own shaled-* from this template.
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open slate backing: %w", err)
	}
	handle := backing.Handle()
	logger.Printf("shaled-slate: slate factory (multi-backend) bucket=%s keyPrefix=%q endpoint=%s region=%s",
		cfg.Bucket, cfg.KeyPrefix, cfg.Endpoint, cfg.Region)
	// Close releases units this Handle still has mounted. Cluster.Close
	// (invoked by shaled.Run first) already releases units via CloseUnit;
	// this is the belt-and-suspenders shutdown for anything still held.
	return handle, handle.Close, nil
}
