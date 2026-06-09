// backend_pebble.go - constructor for the Pebble (local LSM) backend.
// Always compiled; Pebble is pure Go and has no build-tag gating,
// unlike the slate backend which depends on a cgo runtime.

package main

import (
	"fmt"
	"log"

	"github.com/Zamua/shale/pkg/backend"
	pebblebe "github.com/Zamua/shale/pkg/backend/pebble"
)

// openPebbleBackend opens a Pebble database at cfg.Dir. The returned
// close func is a no-op because backend.Backend.Close (invoked by main
// on shutdown) is enough to flush the WAL and release the directory
// lock.
func openPebbleBackend(cfg pebbleConfig, logger *log.Logger) (backend.Backend, func() error, error) {
	be, err := pebblebe.New(pebblebe.Config{Dir: cfg.Dir})
	if err != nil {
		return nil, nil, fmt.Errorf("open pebble: %w", err)
	}
	logger.Printf("shaled: pebble backend dir=%s", cfg.Dir)
	return be, func() error { return nil }, nil
}
