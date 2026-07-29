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
// FAIL CLOSED. The joiner tries EVERY visible non-self peer's GenState in turn;
// only if they ALL fail (each seed unreachable / rejecting) does Open FAIL
// rather than fall back to gen 0, so one momentarily-down peer does not fail a
// join when another healthy seed exists. A joiner that cannot learn the
// generation from anyone must not serve at the wrong one. A failed Open is
// surfaced to the caller for a supervised restart (the bind-conflict retry
// wrapper only retries bind errors, not a gen-query failure).
//
// STABLE MEMBERSHIP. A membership change mid-reshard ABORTS the reshard (the
// coordinator's identity-set re-check), so a join only ever lands at a STABLE
// generation - between reshards, never mid-FLIP. The seed's {gen, count} is
// therefore a single coherent value (nextCount zero, cutOver empty); the
// joiner does not reason about a mid-cutover straddle. (This stable-generation
// guarantee rests on the MULTI-node barrier's abort; the single-node bisect
// path has no membership re-check, so a join racing it is the same out-of-scope
// window, narrowed but not eliminated.)

package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/Zamua/shale/pkg/storageunit"
)

// genQueryTimeout bounds ONE synchronous seed GenState query attempt made
// during a joiner's Open. Generous (the seed answers from an in-memory
// snapshot, no I/O), but bounded so a wedged seed makes a single attempt
// fail rather than hang. The joiner re-sweeps its seeds across many such
// attempts up to GenLearnBudget (see learnGenerationFromSeed).
const genQueryTimeout = 10 * time.Second

// defaultGenLearnBudget is the total wall-clock a joiner spends RE-SWEEPING its
// seeds for the cluster generation before failing Open. It must comfortably
// exceed a worst-case cold-start UNIT MOUNT: a seed binds its gRPC port early
// but does not begin SERVING it until after its own Open returns (i.e. after it
// has mounted every replicated unit, an object-storage-bound step that on a
// loaded backend takes tens of seconds). A single 10s attempt that fails closed
// turns that transient window into a crash-loop under a supervisor; re-sweeping
// for this budget WAITS the seed out instead. Bounded (not infinite) so a
// joiner whose seeds are truly dead still fails Open for a supervised restart.
// Overridable per-cluster via Config.GenLearnBudget (env-plumbed by the caller).
const defaultGenLearnBudget = 180 * time.Second

// genLearnBackoff is the pause between seed re-sweeps in learnGenerationFromSeed.
// Small relative to the budget so the joiner reconnects promptly once the seed
// begins serving, but not a busy-loop. A var (not const) only so tests can
// shrink it; production never reassigns it.
var genLearnBackoff = 2 * time.Second

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
// PATIENT: it re-sweeps the visible seeds with backoff for genLearnBudget()
// before giving up. A seed that is itself cold-starting has bound its gRPC port
// but does not begin SERVING it until after its own Open (its unit mount)
// returns; until then the joiner's GenState dial times out with "connections to
// become ready". A single fail-closed attempt would turn that transient mount
// window into a CRASH-LOOP under a supervisor; re-sweeping waits the seed out.
// This is the gen-query sibling of the scan path's PeerConnectTimeout wait.
//
// Fails closed: only if EVERY visible seed stays unreachable / rejecting for the
// WHOLE budget does this (and therefore Open) fail, rather than silently leaving
// the joiner at gen 0. Bounded so truly-dead seeds still fail Open for a
// supervised restart rather than hanging forever.
func (c *Cluster) learnGenerationFromSeed() error {
	budget := c.genLearnBudget()
	deadline := time.Now().Add(budget)
	var lastErr error
	for attempt := 1; ; attempt++ {
		committed, err := c.trySeedGenSweep()
		if committed {
			return nil
		}
		lastErr = err
		if !time.Now().Before(deadline) {
			return fmt.Errorf("cluster: could not learn the cluster generation within %s (%d attempt(s)): %w", budget, attempt, lastErr)
		}
		time.Sleep(genLearnBackoff)
	}
}

// genLearnBudget is the joiner's total seed-gen-query budget: Config.GenLearnBudget
// when set (>0), else defaultGenLearnBudget.
func (c *Cluster) genLearnBudget() time.Duration {
	if c.cfg.GenLearnBudget > 0 {
		return c.cfg.GenLearnBudget
	}
	return defaultGenLearnBudget
}

// trySeedGenSweep makes ONE pass over the currently-visible seeds, querying each
// for the cluster generation. Returns (true, nil) the instant a seed answers
// (the generation is committed as a side effect). Returns (false, lastErr) if no
// visible seed answered this pass - lastErr says why (no peer visible YET, or
// the last query error), so the patient caller can retry or surface it. The
// addr set is re-fetched each pass so a seed that only just joined gossip (or
// only just began serving gRPC) is picked up on the next sweep.
func (c *Cluster) trySeedGenSweep() (committed bool, lastErr error) {
	addrs := c.seedGRPCAddrs()
	if len(addrs) == 0 {
		return false, fmt.Errorf("cluster: no live peer visible yet to query for the cluster generation")
	}
	// Try each visible peer in turn. A momentarily-unreachable peer must not
	// end the sweep while another healthy seed exists (e.g. a node joins while
	// one of two existing nodes is briefly down). Never fall back to gen 0.
	for _, addr := range addrs {
		cli, err := c.clientFor(addr)
		if err != nil {
			lastErr = fmt.Errorf("cluster: dial seed %s for generation query: %w", addr, err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), genQueryTimeout)
		resp, err := cli.GenState(ctx)
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("cluster: query seed %s generation: %w", addr, err)
			continue
		}
		count, err := storageunit.NewUnitCount(int(resp.GetUnitCount()))
		if err != nil {
			lastErr = fmt.Errorf("cluster: seed %s reported invalid unit count %d: %w", addr, resp.GetUnitCount(), err)
			continue
		}
		// Commit the live generation BEFORE any unit is derived or mounted.
		// cutOver is empty and nextCount zero: a join only lands at a STABLE
		// generation (mid-reshard membership change aborts the reshard), so
		// there is no in-flight cut-over to carry. From this store on,
		// genSnapshot() returns the live generation, so the mount loop and
		// every later routable op resolve at the right generation.
		c.commitGenState(genState{
			gen:     storageunit.Generation(resp.GetGeneration()),
			count:   count,
			cutOver: make(map[storageunit.UnitID]struct{}),
		})
		return true, nil
	}
	return false, lastErr
}

// seedGRPCAddrs returns the gRPC addresses of every visible non-self peer, for
// learnGenerationFromSeed to query in turn. The coordinator has already
// populated its opening view with every currently-known node and its gRPC Addr
// (Open starts the coordinator before this runs), so Members() here includes
// the seed(s) the joiner reached. An empty result (no peer visible) makes the
// join fail closed: a joiner that cannot learn the generation must not serve at
// gen 0.
func (c *Cluster) seedGRPCAddrs() []string {
	self := c.cfg.NodeID
	var addrs []string
	for _, m := range c.Members() {
		if m.ID == self || m.Addr == "" {
			continue
		}
		addrs = append(addrs, m.Addr)
	}
	return addrs
}
