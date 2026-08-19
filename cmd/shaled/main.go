// Command shaled runs a single shale node as its own process with
// the in-process memory backend. It opens a memory.Memory, wraps it
// in a cluster.Cluster, and serves the gRPC ShaleNode service
// (pkg/rpc) on --grpc-addr. The CLI (cmd/shale) talks to this
// service.
//
// SINGLE-NODE ONLY. Multi-node needs a coordination adapter
// (shaled.RunConfig.Coordinator) plus a BackendFactory, and this
// binary wires neither: the memory backend is a single backend.Backend
// with no shared store for peers to coordinate or hand units over.
// The deployable multi-node binary is backends/slate/cmd/shaled-slate
// in --multi-backend mode.
//
// Why memory-only:
//
// Pre-v0.5, this binary multiplexed memory + pebble + slate behind a
// --backend flag, with the slate path gated behind a `slatedb` build
// tag. That shape didn't compose with the v0.5 multi-module split:
// the core module would have to import every backend module, dragging
// each backend's dep weight (slatedb's cgo + native lib, pebble's LSM
// transitives) back into core. The split was designed to prevent
// exactly that.
//
// So the core shaled is now the minimal reference: it serves the
// cluster + gRPC stack against the in-process memory store. This is
// enough for:
//
//   - Smoke tests + demos (start a node, hit it from the CLI, see
//     it work).
//   - Operators who explicitly don't need durability.
//
// Per-backend shaleds (`shaled-pebble`, `shaled-slate`, third-party
// `shaled-foodb`) live in their own modules under backends/<name>/
// cmd/shaled-<name>/ and share the run-loop helper at pkg/shaled.
// They are what operators ship to production.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/shaled"
)

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "shaled: %v\n", err)
		os.Exit(1)
	}
}

func run(argv []string, stderr *os.File) error {
	logger := log.New(stderr, "", log.LstdFlags|log.Lmicroseconds)

	fs := flag.NewFlagSet("shaled", flag.ContinueOnError)
	std := shaled.BindStdFlags(fs)
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if err := std.Validate(); err != nil {
		return err
	}

	be, closeBackend, err := openMemoryBackend(logger)
	if err != nil {
		return err
	}

	return shaled.Run(shaled.RunConfig{
		Std:          *std,
		BackendLabel: "memory",
		Backend:      be,
		CloseBackend: closeBackend,
		Logger:       logger,
	})
}

// openMemoryBackend returns a fresh in-process Memory backend. The
// returned close func is a no-op: Backend.Close (called by shaled.Run
// on shutdown) is enough to release memory backend resources.
func openMemoryBackend(logger *log.Logger) (backend.Backend, func() error, error) {
	logger.Printf("shaled: memory backend (non-durable; data lost on restart)")
	return memory.New(), func() error { return nil }, nil
}
