// Founder-restart generation derivation (v0.8 declarative resharding).
//
// THE HAZARD. With declarative resharding, --unit-count is the DESIRED unit
// count, not the initial one: an operator bumps it (2 -> 4) and rolls the
// deploy, and the cluster reshards ITSELF to match. That re-purposing breaks
// the old founder assumption. The founder (no seeds) used to seed its routing
// state at genState{gen: 0, count: --unit-count} unconditionally. If a founder
// now RESTARTS an existing cluster whose durable state is already resharded to
// gen g / count 2N, re-init'ing at the new desired (4) would orphan the live
// data: the gen-g/count-2N units would no longer be routed to, and every acked
// key in them becomes unreachable.
//
// THE FIX. The founder derives the CURRENT {gen, count} from DURABLE STATE at
// startup, exactly as a joiner derives it from a seed (learnGenerationFromSeed).
// The durable state IS the set of units present in the shared backing, which the
// factory's PresentUnits enumerates. The founder commits the derived live state
// BEFORE the mount loop, so it never derives or mounts a unit at the wrong
// generation. --unit-count becomes purely the DESIRED value the reconcile loop
// (a later phase) drives toward; it seeds the actual count ONLY for a FRESH
// cluster with no durable units.
//
// WHY DURABLE-STATE DERIVATION over a persisted {gen,count} object: the units in
// the backing ARE the authoritative durable state already. A separate persisted
// cluster-state object would be a second source of truth to keep in sync, with a
// write-it-on-every-reshard step that could desync from the actual unit set.
// PresentUnits reads the same bytes the mount loop will open.
//
// WHY AN OPTIONAL INTERFACE: enumerating the units PRESENT in the durable
// backing (regardless of mount) is a strictly larger capability than the shipped
// storageunit.BackendFactory contract (which is the locally-mounted-set view:
// OpenUnit / CloseUnit / CurrentEpoch / OpenUnits). Only an object-store-backed
// factory has it. So the cluster reaches it through an OPTIONAL interface it
// type-asserts on c.factory: a factory that satisfies it gets durable-state
// derivation; one that does not keeps the gen-0/desired default (it cannot have
// been the founder of a previously-resharded cluster, since only an enumerable
// durable backing can have produced one). The slate Handle and the in-process
// test double both expose PresentUnits, so both satisfy it.

package cluster

import (
	"context"
	"fmt"
	"math/bits"
	"time"

	"github.com/Zamua/shale/pkg/storageunit"
)

// presentUnitsProvider is the OPTIONAL capability a BackendFactory may expose:
// enumerate the GenUnits PRESENT in the durable backing (regardless of which
// node currently has them mounted). It is intentionally NOT part of the shipped
// storageunit.BackendFactory interface (that is the mounted-set contract); the
// cluster type-asserts this off c.factory and falls back gracefully when the
// factory does not implement it. The slate *Handle and the in-process test
// double's *Handle both satisfy it.
type presentUnitsProvider interface {
	PresentUnits(ctx context.Context) ([]storageunit.GenUnit, error)
}

// presentUnitsTimeout bounds the founder's durable-state enumeration (a single
// bucket list for the slate factory). Generous (one object-store round-trip),
// but bounded so a wedged backing fails Open closed rather than hanging.
const presentUnitsTimeout = 30 * time.Second

// deriveGenerationFromDurableState is the FOUNDER's generation-propagation step
// (the no-seeds analogue of learnGenerationFromSeed). It enumerates the units
// present in the durable backing and, if any exist, commits the live {gen,
// count} derived from them - so a founder restarting a previously-resharded
// cluster routes at the live generation instead of re-init'ing at the desired
// count and orphaning the data. Called from initMultiBackend in the no-seeds
// branch ONLY, BEFORE any unit is derived or mounted.
//
// A FRESH cluster (no durable units) leaves initGenState's gen-0/desired default
// untouched: the desired count seeds the actual count exactly here, the only
// place it does so directly. A factory that does NOT expose PresentUnits keeps
// the default too (it cannot have been the founder of a resharded cluster). Fail
// closed: an enumeration error fails Open rather than guessing the generation.
func (c *Cluster) deriveGenerationFromDurableState() error {
	prov, ok := c.factory.(presentUnitsProvider)
	if !ok {
		// Non-enumerable factory: keep the gen-0/desired default. Such a factory
		// has no durable cross-restart unit set to recover, so it can only ever be
		// a fresh founder.
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), presentUnitsTimeout)
	defer cancel()
	present, err := prov.PresentUnits(ctx)
	if err != nil {
		return fmt.Errorf("cluster: enumerate durable units for founder generation: %w", err)
	}
	if len(present) == 0 {
		// Fresh cluster: nothing durable to recover, establish at the desired
		// count. This is the ONLY place --unit-count (DESIRED) seeds the actual
		// live count directly.
		return nil
	}

	gen, count, ok := liveGenFromPresentUnits(present)
	if !ok {
		// Units exist but no generation forms a complete power-of-two unit set.
		// This should not happen for a cleanly-shut cluster (every committed
		// generation has its full 0..N-1 unit set durable). Fail closed rather
		// than route at a guessed generation: the operator must investigate the
		// bucket state.
		return fmt.Errorf("cluster: durable units present but no complete generation found (units=%v)", present)
	}

	// Commit the derived live generation BEFORE the mount loop. cutOver is empty
	// and nextCount zero: a restart recovers a STABLE generation (a reshard either
	// completed or aborted; an in-flight reshard at restart is out of scope, the
	// same window the join path leaves open). From this commit on, genSnapshot()
	// returns the live generation, so desiredGenUnits and every later routable op
	// resolve at the right generation.
	c.commitGenState(genState{
		gen:     gen,
		count:   count,
		cutOver: make(map[storageunit.UnitID]struct{}),
	})
	return nil
}

// liveGenFromPresentUnits derives the cluster's live {generation, unit-count}
// from the set of units present in the durable backing. The live generation is
// the HIGHEST generation whose units form a COMPLETE power-of-two set
// ({0, 1, ..., N-1} for some power-of-two N). Selecting the highest COMPLETE
// generation is what makes this robust to a backing that still holds RETIRED
// lower-generation units (CloseUnit frees a unit's in-process database but does
// NOT delete its durable bytes, so after a completed g -> g+1 reshard the bucket
// holds BOTH the gen-g and gen-(g+1) unit sets) AND to an ABORTED reshard that
// left a PARTIAL higher generation (some gen-(g+1) children half-built before
// the abort): a partial generation is not complete, so the derivation falls back
// to the highest complete one, which is the live one. ok=false if NO generation
// forms a complete power-of-two set (a corrupt / impossible backing state).
func liveGenFromPresentUnits(present []storageunit.GenUnit) (gen storageunit.Generation, count storageunit.UnitCount, ok bool) {
	// Group the present unit IDs by generation.
	byGen := make(map[storageunit.Generation]map[storageunit.UnitID]struct{})
	for _, gu := range present {
		ids, exists := byGen[gu.Gen]
		if !exists {
			ids = make(map[storageunit.UnitID]struct{})
			byGen[gu.Gen] = ids
		}
		ids[gu.ID] = struct{}{}
	}

	var (
		best     storageunit.Generation
		bestN    int
		bestSeen bool
	)
	for g, ids := range byGen {
		n, complete := completeUnitCount(ids)
		if !complete {
			continue
		}
		if !bestSeen || g > best {
			best, bestN, bestSeen = g, n, true
		}
	}
	if !bestSeen {
		return 0, storageunit.UnitCount{}, false
	}
	uc, err := storageunit.NewUnitCount(bestN)
	if err != nil {
		// completeUnitCount already guarantees a power of two in range, so this
		// is unreachable; treat a surprise as "no complete generation".
		return 0, storageunit.UnitCount{}, false
	}
	return best, uc, true
}

// completeUnitCount reports whether ids is exactly {0, 1, ..., N-1} for some
// power-of-two N, and returns that N. A generation's unit set is complete iff
// its size is a power of two AND every id 0..N-1 is present (no gap, no out-of-
// range id). Used to pick the live generation: only a complete generation was
// ever committed (FLIP advances to g+1 only after every node has bisected all N
// units, so a committed g+1 has its full 0..2N-1 set).
func completeUnitCount(ids map[storageunit.UnitID]struct{}) (n int, complete bool) {
	size := len(ids)
	if size == 0 || bits.OnesCount(uint(size)) != 1 {
		return 0, false
	}
	for i := 0; i < size; i++ {
		if _, present := ids[storageunit.UnitID(i)]; !present {
			return 0, false
		}
	}
	return size, true
}
