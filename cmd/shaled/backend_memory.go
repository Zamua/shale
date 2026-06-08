// backend_memory.go - constructor for the in-process memory backend.
// Always compiled; the memory backend has no external dependencies and
// no build-tag gating.

package main

import (
	"log"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
)

// openMemoryBackend returns a fresh in-process Memory backend. The
// returned close func is a no-op: Backend.Close (called by main on
// shutdown) is enough to release memory backend resources.
func openMemoryBackend(logger *log.Logger) (backend.Backend, func() error, error) {
	logger.Printf("shaled: memory backend (non-durable; data lost on restart)")
	return memory.New(), func() error { return nil }, nil
}
