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
