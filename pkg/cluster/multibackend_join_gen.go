// Generation propagation to a joining node (v0.8 join-after-reshard fix).
//
// The cluster-wide freeze barrier (multibackend_reshard_barrier.go) reshards
// the nodes that are PRESENT when Reshard() runs. A node that JOINS later must
// arrive at the cluster's LIVE generation, or it routes at the wrong one. This
// file is how a multi-backend JOINER (Open WITH seeds) learns the live
// {generation, unit-count} BEFORE it derives, mounts, owns, or serves any
// unit.
//
// THE HAZARD. A node boots its routing state at generation 0 (initGenState
// seeds genState{gen: 0, count: N}); the generation advances ONLY via a
// reshard the node itself runs (the FLIP handler, or the single-node bisect).
// After the cluster has resharded to gen g, a node that joins starts at gen 0.
// Routing is generation-qualified end to end (a key resolves to GenUnit{gen,
// UnitForHash(h, count)} and the ring places a unit by genUnitBytes, hashing
// the generation AHEAD of the unit id), so the gen-0 id and the live gen-g id
// of the same key hash to DIFFERENT ring positions. The gen-0 joiner therefore
// orphans keys: as an originator it forwards to whichever node owns its gen-0
// unit, and that node - routing at gen g - disclaims it ("forwarding loop
// refused: this node does not own the key"). An acked write becomes
// unreachable. Reconcile / settle do NOT self-heal it: the steady-state
// machinery never RAISES a node's generation, only the two reshard paths do,
// neither of which a passive joiner runs.
//
// THE FIX. When a node opens in multi-backend mode WITH seeds, it queries a
// seed's GenState RPC for the live {gen, count} and commitGenState()s it
// BEFORE the mount loop in initMultiBackend runs. The founder (no seeds) keeps
// initGenState's gen-0 default. Because the mount derivation and the first
// routable op both happen strictly AFTER the commit, the joiner never
// resolves, routes, or owns a key at gen 0: there is NO gen-0 serving window.
// Open does not return until this completes, and the caller registers the
// joiner's gRPC server only after Open returns, so no external request can
// reach the joiner before its generation is correct.
//
// FAIL CLOSED. If the seed query fails (seed unreachable, RPC error), Open
// FAILS rather than falling back to gen 0: a joiner that cannot learn the
// generation must not serve at the wrong one. The same retry-on-bind-conflict
// loop that already wraps Open can retry the join.
//
// STABLE MEMBERSHIP. A membership change mid-reshard ABORTS the reshard (the
// coordinator's identity-set re-check), so a join only ever lands at a STABLE
// generation - between reshards, never mid-FLIP. The seed's {gen, count} is
// therefore a single coherent value (nextCount zero, cutOver empty); the
// joiner does not reason about a mid-cutover straddle.

package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/Zamua/shale/pkg/storageunit"
)

// genQueryTimeout bounds the synchronous seed GenState query made during a
// joiner's Open. Generous (the seed answers from an in-memory snapshot, no
// I/O), but bounded so a wedged seed makes Open fail-closed rather than hang.
const genQueryTimeout = 10 * time.Second

// GenStateSnapshot returns the local node's live {generation, unit-count} for
// the cluster-internal GenState RPC. The generation is the uint64 of the
// current genState.gen; the count is N at that generation. Read under the
// genState lock (genSnapshot), so it is a coherent pair. Exported for the rpc
// server adapter; not part of the app-facing KV surface.
func (c *Cluster) GenStateSnapshot() (gen uint64, count uint32) {
	gs := c.genSnapshot()
	return uint64(gs.gen), gs.count.N()
}

// learnGenerationFromSeed is the JOINER's generation-propagation step. It
// queries a live seed's GenState RPC for the cluster's current {gen, count}
// and commitGenState()s it, seeding this node's routing state from the live
// value instead of the gen-0 default. Called from initMultiBackend ONLY when
// this node has seeds (a joiner), and BEFORE any unit is derived or mounted,
// so there is no window in which the joiner routes / owns a key at gen 0.
//
// Fails closed: a seed that is unreachable or rejects the query makes this
// (and therefore Open) fail rather than silently leaving the joiner at gen 0.
func (c *Cluster) learnGenerationFromSeed() error {
	addr, err := c.pickSeedGRPCAddr()
	if err != nil {
		return err
	}
	cli, err := c.clientFor(addr)
	if err != nil {
		return fmt.Errorf("cluster: dial seed %s for generation query: %w", addr, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), genQueryTimeout)
	defer cancel()
	resp, err := cli.GenState(ctx)
	if err != nil {
		return fmt.Errorf("cluster: query seed %s generation: %w", addr, err)
	}

	count, err := storageunit.NewUnitCount(int(resp.GetUnitCount()))
	if err != nil {
		return fmt.Errorf("cluster: seed %s reported invalid unit count %d: %w", addr, resp.GetUnitCount(), err)
	}
	// Commit the live generation BEFORE any unit is derived or mounted. cutOver
	// is empty and nextCount zero: a join only lands at a STABLE generation
	// (mid-reshard membership change aborts the reshard), so there is no
	// in-flight cut-over to carry. From this store on, genSnapshot() returns
	// the live generation, so the mount loop and every later routable op
	// resolve at the right generation.
	c.commitGenState(genState{
		gen:     storageunit.Generation(resp.GetGeneration()),
		count:   count,
		cutOver: make(map[storageunit.UnitID]struct{}),
	})
	return nil
}

// pickSeedGRPCAddr resolves the gRPC address of a live peer to query for the
// generation. The join handshake (membership.Open -> Join) has already
// populated the membership cache with every currently-known node and its gRPC
// Addr (broadcast as the memberlist Meta payload), so Members() here includes
// the seed(s) the joiner contacted. Returns the first peer that is not this
// node. An empty result (no peer visible) is an error: a joiner with seeds
// that sees no peer cannot learn the generation and must fail closed rather
// than serve at gen 0.
func (c *Cluster) pickSeedGRPCAddr() (string, error) {
	self := c.cfg.NodeID
	for _, m := range c.Members() {
		if m.ID == self || m.Addr == "" {
			continue
		}
		return m.Addr, nil
	}
	return "", fmt.Errorf("cluster: no live peer to query for the cluster generation (seeds=%v)", c.cfg.Seeds)
}
