// Decentralized reshard agreement wiring for multi-backend mode (v0.9).
//
// This file is the cluster-side seam between the agreement layer (pkg/reshard's
// Arbiter, a CAS-guarded epoch object in shared storage) and the per-unit
// routing substrate (genState). It owns ONLY the construction + seeding of the
// Arbiter here; the routing, online copy, and per-unit cut-over machinery the
// Arbiter drives land in sibling files as later phases.
//
// Scope: the decentralized online reshard is R>1 multi-backend ONLY. The R=1
// multi-node path keeps the coordinated freeze barrier
// (multibackend_reshard_barrier.go); the single-node path keeps the inline
// online bisect (multibackend_reshard.go). So the Arbiter is constructed only
// when cfg.ConditionalStore is set AND replicaFactory != nil (R>1).

package cluster

import (
	"fmt"

	"github.com/Zamua/shale/pkg/reshard"
)

// initReshardArbiter constructs and seeds the decentralized reshard Arbiter when
// this cluster opts into the declarative path: a ConditionalStore is configured
// AND this is an R>1 multi-backend cluster (replicaFactory wired). It is a no-op
// (leaves c.arbiter nil) otherwise, so a cluster without a ConditionalStore, or
// at R=1 / legacy, stays byte-for-byte on the existing reshard paths.
//
// Called from initMultiBackend AFTER initReplicatedFactory (which sets
// replicaFactory) and BEFORE the unit mount, so the agreed epoch object exists
// from the moment the node starts serving. Seeding is idempotent across the
// cluster: exactly one node's create-if-absent wins and every other node (and a
// later joiner, even one configured at a different base count) adopts the
// already-seeded State, keeping the single durable object the sole authority.
//
// Fails closed: a ConditionalStore that cannot be reached fails Open rather than
// leaving a node that opted into the decentralized path without its agreement
// object.
func (c *Cluster) initReshardArbiter() error {
	if c.cfg.ConditionalStore == nil || c.replicaFactory == nil {
		return nil
	}
	a := reshard.NewArbiter(c.cfg.ConditionalStore)
	if _, err := a.Seed(c.unitCount); err != nil {
		return fmt.Errorf("cluster: seed reshard arbiter: %w", err)
	}
	c.arbiter = a
	return nil
}
