// backend_default.go: tag-less stub for openSlateBackend. The real
// impl lives in backend_slatedb.go (//go:build slatedb). A build of
// shaled-slate WITHOUT the slatedb tag still compiles, but running
// it fails fast with an instructive rebuild error so operators don't
// silently end up with a "shaled-slate" that has no slate.

//go:build !slatedb

package main

import (
	"fmt"
	"log"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

const rebuildHint = "shaled-slate: built without -tags slatedb; rebuild via " +
	"`CGO_ENABLED=1 CGO_LDFLAGS=-L/path/to/slatedb/target/release " +
	"go build -tags slatedb ./cmd/shaled-slate` " +
	"(and ensure the slatedb runtime is on the loader path)"

func openSlateBackend(_ slateConfig, _ *log.Logger) (backend.Backend, func() error, error) {
	return nil, nil, fmt.Errorf("%s", rebuildHint)
}

func openSlateFactory(_ slateConfig, _ *log.Logger) (storageunit.BackendFactory, func() error, error) {
	return nil, nil, fmt.Errorf("%s", rebuildHint)
}

func openSlateCondStore(_ slateConfig) (storageunit.ConditionalStore, error) {
	return nil, fmt.Errorf("%s", rebuildHint)
}
