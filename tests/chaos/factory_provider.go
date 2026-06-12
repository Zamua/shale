//go:build chaos

package chaos

// The factory seam the in-process chaos cluster mounts its per-unit storage
// through. The adapter (adapter_inproc.go) used to hold a concrete
// *sharedfactory.Backing and call backing.Handle() per node; this interface
// abstracts that ONE dependency so the SAME harness can run against either the
// in-memory shared-backing double (the default) or the REAL slatedb
// BackendFactory over a MinIO bucket (the slate provider, built only under the
// slatedb tag - see factory_slate.go).
//
// The seam is deliberately tiny: every node needs exactly one thing from the
// backing, a per-node Handle that implements storageunit.BackendFactory (the
// mount/lease seam the cluster drives). Both sharedfactory.Backing and
// slate.Backing already expose that as Handle(), so the provider is a one-method
// wrapper. It adds NO method to the shipped storageunit.BackendFactory interface
// and lives entirely in the chaos-tagged test tree.

import (
	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// factoryProvider hands out per-node BackendFactory handles that all share one
// backing, which is what makes a lease handoff copy-free + fence-correct: every
// node's Handle points at the SAME durable bytes (an in-memory map for the
// double, a MinIO bucket for slate). Reset clears the backing's durable state so
// a seed sweep can run each seed against a clean backing (the in-memory double
// constructs a fresh Backing; the slate provider rotates its bucket key-prefix).
type factoryProvider interface {
	// NewHandle returns a fresh per-node factory handle over the shared backing.
	NewHandle() storageunit.BackendFactory
	// Reset clears durable state between seeds so one seed's units never leak
	// into the next. It is called before each seed's cluster is stood up.
	Reset() error
	// Close releases any resources the provider holds (e.g. the slate provider's
	// bucket teardown). Idempotent.
	Close() error
}

// sharedFactoryProvider is the DEFAULT provider: the in-memory shared-backing
// double. It preserves the existing soak's behavior byte-for-byte (the adapter
// used to construct a sharedfactory.Backing directly).
type sharedFactoryProvider struct {
	backing *sharedfactory.Backing
}

func newSharedFactoryProvider() *sharedFactoryProvider {
	return &sharedFactoryProvider{backing: sharedfactory.NewBacking()}
}

func (p *sharedFactoryProvider) NewHandle() storageunit.BackendFactory {
	return p.backing.Handle()
}

// Reset swaps in a fresh in-memory backing so a new seed starts empty. The old
// backing's handles are dropped with their cluster when CloseAll runs.
func (p *sharedFactoryProvider) Reset() error {
	p.backing = sharedfactory.NewBacking()
	return nil
}

func (p *sharedFactoryProvider) Close() error { return nil }
