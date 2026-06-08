// backend_slatedb.go - slate-backed Backend constructor. Compiled only
// when the binary is built with -tags slatedb. The default-tag stub
// (backend_default.go) takes over when the tag is absent and returns a
// clear "rebuild with -tags slatedb" error.
//
// The actual pkg/backend/slate package lands later in the v0.1 cycle;
// this file is the integration point. When slate.New ships, swap the
// placeholder body for the real constructor: there's no other change
// needed on the shaled side.

//go:build slatedb

package main

import (
	"fmt"
	"log"

	"github.com/Zamua/shale/pkg/backend"
)

func openSlateBackend(cfg slateConfig, logger *log.Logger) (backend.Backend, func() error, error) {
	// Intentionally not yet wired: pkg/backend/slate has not been
	// committed. Once it ships, this body becomes:
	//
	//   be, err := slate.New(slate.Config{
	//       Endpoint:  cfg.Endpoint,
	//       Region:    cfg.Region,
	//       Bucket:    cfg.Bucket,
	//       AccessKey: cfg.AccessKey,
	//       SecretKey: cfg.SecretKey,
	//       UseSSL:    cfg.UseSSL,
	//       DbName:    cfg.DbName,
	//   })
	//   if err != nil { return nil, nil, fmt.Errorf("open slate: %w", err) }
	//   logger.Printf("shaled: slate backend bucket=%s db=%s endpoint=%s",
	//       cfg.Bucket, cfg.DbName, cfg.Endpoint)
	//   return be, be.Close, nil
	_ = cfg
	_ = logger
	return nil, nil, fmt.Errorf(
		"slate backend not yet implemented (pkg/backend/slate is pending); " +
			"use --backend=memory for now")
}
