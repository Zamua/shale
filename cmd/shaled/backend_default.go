// backend_default.go - stub for the slate backend when the binary is
// NOT built with -tags slatedb. The real impl lives in
// backend_slatedb.go (matching build tag). This split mirrors the
// pattern hostthis uses for its slatedb metadata backend.

//go:build !slatedb

package main

import (
	"fmt"
	"log"

	"github.com/Zamua/shale/pkg/backend"
)

func openSlateBackend(_ slateConfig, _ *log.Logger) (backend.Backend, func() error, error) {
	return nil, nil, fmt.Errorf(
		"--backend=slate requires a binary built with -tags slatedb; " +
			"rebuild via `go build -tags slatedb ./cmd/shaled` " +
			"(and ensure the slatedb runtime is available on the loader path)")
}
