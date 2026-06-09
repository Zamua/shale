// Command shaled-pebble runs a single shale node with the Pebble
// (local-disk, pure-Go LSM) backend. See pkg/shaled for the shared
// run-loop helper; this main is the pebble-specific thin shell: it
// registers --pebble-dir, opens a *pebble.Pebble, and hands the
// resulting backend.Backend to shaled.Run.
//
// Pebble is pure Go, no cgo, no build-tag gating: `go build
// ./cmd/shaled-pebble` (run from backends/pebble) always works.
//
// Flags (in addition to the standard cluster/node flags exposed by
// pkg/shaled.BindStdFlags):
//
//	--pebble-dir   SHALE_PEBBLE_DIR    on-disk data directory (required)
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	pebblebe "github.com/Zamua/shale/backends/pebble"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/shaled"
)

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "shaled-pebble: %v\n", err)
		os.Exit(1)
	}
}

func run(argv []string, stderr *os.File) error {
	logger := log.New(stderr, "", log.LstdFlags|log.Lmicroseconds)

	fs := flag.NewFlagSet("shaled-pebble", flag.ContinueOnError)
	std := shaled.BindStdFlags(fs)
	pebbleDir := fs.String("pebble-dir", shaled.EnvOr("SHALE_PEBBLE_DIR", ""),
		"pebble: on-disk data directory (required)")

	if err := fs.Parse(argv); err != nil {
		return err
	}
	if err := std.Validate(); err != nil {
		return err
	}
	if *pebbleDir == "" {
		return errors.New("--pebble-dir is required (or set SHALE_PEBBLE_DIR)")
	}

	be, closeBackend, err := openPebbleBackend(*pebbleDir, logger)
	if err != nil {
		return err
	}

	return shaled.Run(shaled.RunConfig{
		Std:          *std,
		BackendLabel: "pebble",
		Backend:      be,
		CloseBackend: closeBackend,
		Logger:       logger,
	})
}

// openPebbleBackend opens a Pebble database at dir. The returned close
// func is a no-op because backend.Backend.Close (invoked by shaled.Run
// on shutdown) is enough to flush the WAL and release the directory
// lock.
func openPebbleBackend(dir string, logger *log.Logger) (backend.Backend, func() error, error) {
	be, err := pebblebe.New(pebblebe.Config{Dir: dir})
	if err != nil {
		return nil, nil, fmt.Errorf("open pebble: %w", err)
	}
	logger.Printf("shaled-pebble: pebble backend dir=%s", dir)
	return be, func() error { return nil }, nil
}
