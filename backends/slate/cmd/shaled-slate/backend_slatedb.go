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

// openSlateFactory builds the multi-backend slate Backing over the shared
// bucket and returns a per-node Handle (a storageunit.BackendFactory) plus a
// CloseFactory that releases any unit still mounted on the Handle at
// shutdown. The Handle implements OpenUnit/CloseUnit so cluster.Open mounts
// the node's owned units and a lease handoff is copy-free (every node's
// Handle points at the SAME bucket). --slate-db-name is ignored here: the
// per-unit DbName is derived from each GenUnit by the factory.
func openSlateFactory(cfg slateConfig, logger *log.Logger) (storageunit.BackendFactory, func() error, error) {
	backing, err := slate.NewBacking(slate.BackingConfig{
		Bucket:    cfg.Bucket,
		Endpoint:  cfg.Endpoint,
		Region:    cfg.Region,
		AccessKey: cfg.AccessKey,
		SecretKey: cfg.SecretKey,
		UseSSL:    cfg.UseSSL,
		KeyPrefix: cfg.KeyPrefix,
		// Settings + Cache intentionally nil: shaled-slate exposes the
		// "operator runs slate with defaults" surface, same as the single
		// backend. Operators who need a shared block cache or non-default
		// slatedb Settings build their own shaled-* from this directory.
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open slate backing: %w", err)
	}
	handle := backing.Handle()
	logger.Printf("shaled-slate: multi-backend slate bucket=%s key-prefix=%q endpoint=%s region=%s units=%d",
		cfg.Bucket, cfg.KeyPrefix, cfg.Endpoint, cfg.Region, cfg.UnitCount.N())
	// CloseFactory releases any unit the Handle still has mounted after
	// Cluster.Close. Cluster.Close already closes the mounted units it
	// tracks; Handle.Close is the backing-level safety net (best-effort
	// flush + shutdown of anything left).
	return handle, handle.Close, nil
}
