// backend_slatedb.go - slate-backed Backend constructor. Compiled only
// when the binary is built with -tags slatedb. The default-tag stub
// (backend_default.go) takes over when the tag is absent and returns a
// clear "rebuild with -tags slatedb" error.
//
// As of v0.5, the slate backend lives in its own Go module at
// backends/slate. The core cmd/shaled stays memory-only and does NOT
// import backends/slate (that would re-introduce the slatedb cgo
// dep into the core module the multi-module split was designed to
// avoid). Per-backend shaled binaries live alongside each backend
// module: backends/slate/cmd/shaled-slate/ for slate. This file is
// kept as a build-tag stub solely so that an inadvertent
// `-tags slatedb` build of the core shaled errors clearly instead of
// silently succeeding with the no-tag stub.

//go:build slatedb

package main

import (
	"fmt"
	"log"

	"github.com/Zamua/shale/pkg/backend"
)

func openSlateBackend(cfg slateConfig, logger *log.Logger) (backend.Backend, func() error, error) {
	// As of v0.5, the core shaled does NOT bundle the slate backend:
	// that would pull slatedb's cgo dep back into the core module.
	// Operators who want a slate-aware shaled build the per-backend
	// binary from backends/slate/cmd/shaled-slate/ instead. This stub
	// makes that explicit when someone accidentally runs the core
	// shaled with -tags slatedb.
	_ = cfg
	_ = logger
	return nil, nil, fmt.Errorf(
		"slate backend not available in the core shaled (per the v0.5 multi-module split); " +
			"build the per-backend binary from backends/slate/cmd/shaled-slate, " +
			"or use --backend=memory")
}
