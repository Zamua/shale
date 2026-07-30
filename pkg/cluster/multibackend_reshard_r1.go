// The R=1 multi-node online split driver: the parent-anchored, raw-bytes leg
// of the decentralized (arbiter-driven) reshard.
//
// The R>1 driver (multibackend_reshard_driver.go) cannot run on R=1 data: its
// copy + dual-write machinery is built on LWW envelopes (applyEnvelopeIfNewer*),
// and R=1 units store RAW bytes ("At R=1 there are no envelopes", replicate.go).
// R=1 also has no dual-write router, so nothing orders a write across two
// generations. This file supplies the R=1 leg by REUSING the proven single-node
// bisect mechanics per owned parent, plus ONE new invariant:
//
// PARENT-ANCHORED PLACEMENT. While a split is in flight on this node
// (genState.nextCount != 0 at R=1), OWNERSHIP/forwarding for a key resolves the
// PARENT GenUnit{g, K} - placement IGNORES the cutOver set - while only the
// owner's LOCAL db resolution (localWriteBackendForKey / localBackendForKey,
// via resolveGenUnit) honors cutOver. Every write for K's key-space therefore
// funnels to ONE node (K's gen-g owner) for the whole in-flight window, and
// that owner's pause-held clear+copy+flip is exactly the single-node bisect's
// proven cut-over boundary:
//
//   - a write that resolves the parent BEFORE the flip holds the pause READ
//     side across its apply, so the cut-over's WRITE-side hold drains it, and
//     the clear+copy that follows captures it;
//   - a write that arrives AFTER the flip resolves the gen-(g+1) child
//     (mounted co-located on this same owner) and lands there directly.
//
// The durable cut-over markers are needed only to gate cross-node FINALIZE
// (each node retires its parents + advances its generation once it observes
// EVERY unit's marker), not routing. Post-finalize staggered forwards (one node
// at g+1, a peer still in flight at g) hit the existing acquiring / loop-guard
// retryables - never a wrong ack - the same "retryable across the flip window"
// model the SPEC documents for every generation change.
//
// THE INVARIANT THIS FILE IS BUILT TO: NO ACKED WRITE IS LOST.
//
// Scope: SPLIT only (N -> 2N). An R=1 merge would need a cross-node raw-bytes
// forward with no envelope ordering; observeReshard refuses to enter or advance
// toward a merge target at R=1. Membership is assumed stable for the duration
// of a reshard (same standing scope assumption as every reshard path).

package cluster

import (
	"errors"
	"fmt"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// ErrReshardNeedsConditionalStore is returned by Reshard on a multi-node
// cluster with no Config.ConditionalStore: the multi-node reshard is
// arbiter-driven, and the arbiter's agreement object lives in the shared
// conditional store. (A deployable multi-node cluster already requires
// CAS-capable storage for slatedb manifest fencing, so this refuses only
// configurations that could not deploy anyway.)
var ErrReshardNeedsConditionalStore = errors.New(
	"cluster: multi-node Reshard requires Config.ConditionalStore (the arbiter-driven reshard's agreement object)")

// ErrReshardInProgress is returned by Reshard when the retarget was accepted
// and the cluster is converging, but this node's generation did not advance
// within the synchronous wait budget. The reshard KEEPS RUNNING in the
// background (every node's reconcile tick drives it to completion); the
// operator re-checks progress via GenStateSnapshot / a later Reshard call
// (idempotent: retargeting an already-agreed target is a no-op).
var ErrReshardInProgress = errors.New(
	"cluster: reshard accepted and converging in the background; local generation has not advanced yet")

// reshardConvergeTimeout bounds how long a delegated Reshard call waits
// synchronously for the local generation to advance before returning
// ErrReshardInProgress. Peers converge on their own reconcile cadence
// (reconcileInterval + membership-event ticks), so the budget spans several
// such ticks. var so tests can shrink it.
var reshardConvergeTimeout = 45 * time.Second

// reshardConvergePoll is the delay between the delegated Reshard's local
// reconcile pumps while it waits for convergence. var so tests can tune it.
var reshardConvergePoll = 25 * time.Millisecond

// reshardDelegated is the MULTI-NODE Reshard entry: it delegates the doubling
// to the decentralized arbiter (Retarget the agreed count to 2N) and then
// synchronously pumps this node's reconcile - which drives its share of the
// split AND observes peers' durable cut-over markers - until the LOCAL
// generation advances or the bounded budget expires. It preserves the
// synchronous operator surface (Reshard returns nil once this node serves the
// doubled generation) over the eventually-convergent driver.
//
// It deliberately does NOT hold reshardMu: runReconcile takes reshardMu +
// reconcileMu itself, and the arbiter target is the serialization point (a
// concurrent Reshard retargets to the same value and both wait on the same
// convergence).
func (c *Cluster) reshardDelegated() error {
	if c.arbiter == nil {
		return ErrReshardNeedsConditionalStore
	}
	start := c.genSnapshot()
	next, err := start.count.Double()
	if err != nil {
		return fmt.Errorf("cluster: Reshard cannot double %s: %w", start.count, err)
	}
	if _, err := c.arbiter.Retarget(next); err != nil {
		return fmt.Errorf("cluster: Reshard retarget: %w", err)
	}
	deadline := time.Now().Add(reshardConvergeTimeout)
	for {
		if c.closed.Load() {
			return backend.ErrClosed
		}
		c.runReconcile()
		gs := c.genSnapshot()
		if gs.gen > start.gen && gs.nextCount.IsZero() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster: Reshard to %s: %w", next, ErrReshardInProgress)
		}
		time.Sleep(reshardConvergePoll)
	}
}

// driveR1SplitCopies drives this node's share of an in-flight R=1 split: for
// each mounted, not-yet-cut-over gen-g parent, run the online bisect into its
// co-located gen-(g+1) children and flip that unit (local cutOver + the durable
// cut-over marker). A unit that fails this tick is simply retried next tick
// (the copy is idempotent; the marker create is idempotent). Parents are NOT
// retired here - finalizeReshard does that once every unit cluster-wide has cut
// over. Caller holds reconcileMu + reshardMu (observeReshard).
func (c *Cluster) driveR1SplitCopies(gs genState) {
	for _, ru := range c.ownedParentSlots(gs) {
		k := ru.Unit.ID
		if gs.hasCutOver(k) {
			// Already flipped locally. Re-publish the durable marker (create-only,
			// idempotent): a publish that failed after the local flip would
			// otherwise never be retried, and peers need the marker to converge.
			_ = c.publishCutoverMarker(gs.gen, k)
			continue
		}
		_ = c.bisectUnitOnlineR1(gs, k) // a failed unit retries next tick
	}
}

// bisectUnitOnlineR1 bisects one gen-g parent K into its two gen-(g+1) children
// and flips K's routing, reusing the single-node bisectUnit mechanics verbatim
// (background copy -> pause WRITE side -> clear+copy -> cut-over) with two
// differences: the parent is NOT retired here (finalize does that once every
// unit's durable marker is observed cluster-wide), and the flip additionally
// publishes K's durable cut-over marker so peers can observe it.
//
// CLEAR-BEFORE-COPY (no deleted-key resurrection): the children may hold stale
// bytes from an earlier interrupted run (a crashed node's re-driven split
// re-opens the same durable children). The pause-held clear+copy makes each
// child an EXACT image of the drained parent, deletes included.
//
// BREAK-DEMO (Config.TestingForceUncleanReshard): both copy passes are skipped,
// so the flip publishes children that never received the parent's data - the
// lossless gate's oracle must catch the resulting acked-write loss.
func (c *Cluster) bisectUnitOnlineR1(gs genState, k storageunit.UnitID) error {
	oldGU := storageunit.NewGenUnit(gs.gen, k)
	low, high, err := storageunit.ChildUnits(k, gs.count)
	if err != nil {
		return fmt.Errorf("child units of %d: %w", k, err)
	}
	lowBE, err := c.mountChildR1(storageunit.NewGenUnit(gs.gen+1, low))
	if err != nil {
		return err
	}
	highBE, err := c.mountChildR1(storageunit.NewGenUnit(gs.gen+1, high))
	if err != nil {
		return err
	}
	src, ok := c.mounts.backendFor(replica0(oldGU))
	if !ok {
		return fmt.Errorf("cluster: reshard: parent %s not mounted", oldGU)
	}

	// BACKGROUND COPY (outside the pause): the parent keeps serving writes;
	// whatever lands after this pass is captured by the drained clear+copy
	// below. Bounds the pause-held work to the write delta.
	if !c.cfg.TestingForceUncleanReshard {
		if err := c.copyUnitInto(src, lowBE, highBE, gs.count, gs.nextCount); err != nil {
			return fmt.Errorf("copy %s: %w", oldGU, err)
		}
	}

	// THE CUT-OVER BOUNDARY: take K's write-pause WRITE side. Every in-flight
	// writer (local or forwarded - parent-anchored placement funnels them all
	// here) holds the READ side across its apply, so this drains them; no new
	// writer can resolve the parent once the flip below commits.
	pause := c.pauseLockFor(k)
	pause.Lock()
	defer pause.Unlock()
	if !c.cfg.TestingForceUncleanReshard {
		if err := clearBackend(lowBE); err != nil {
			return fmt.Errorf("catch-up clear child of %s: %w", oldGU, err)
		}
		if err := clearBackend(highBE); err != nil {
			return fmt.Errorf("catch-up clear child of %s: %w", oldGU, err)
		}
		if err := c.copyUnitInto(src, lowBE, highBE, gs.count, gs.nextCount); err != nil {
			return fmt.Errorf("catch-up %s: %w", oldGU, err)
		}
	}
	// ATOMIC CUT-OVER: flip K's routing while still holding the pause. Writers
	// blocked on the pause wake after this and resolve the gen-(g+1) child.
	cur := c.genSnapshot().clone()
	cur.cutOver[k] = struct{}{}
	c.commitGenState(cur)
	// Publish the durable marker (create-only; exactly one node's create wins).
	// On failure the local flip stands and driveR1SplitCopies re-publishes next
	// tick, so a transient store error cannot strand peers.
	return c.publishCutoverMarker(gs.gen, k)
}

// mountChildR1 returns the mounted backend for the gen-(g+1) child gu, opening
// + mounting it if this node does not hold it yet. The reconcile may have
// acquired it already (the in-flight desired set co-locates children with their
// parents, desiredGenUnits); either way the same mounted handle serves the copy
// and, post-flip, the routed writes.
func (c *Cluster) mountChildR1(gu storageunit.GenUnit) (backend.Backend, error) {
	if b, ok := c.mounts.backendFor(replica0(gu)); ok {
		return b, nil
	}
	b, _, err := c.factory.OpenUnit(storageunit.SoleMount(gu), epochAtOpen)
	if err != nil {
		return nil, fmt.Errorf("open child %s: %w", gu, err)
	}
	if !c.mounts.mountUnlessClosed(replica0(gu), b) {
		// Close raced us: do not leak a freshly-opened backend past shutdown.
		_ = c.factory.CloseUnit(storageunit.SoleMount(gu))
		return nil, backend.ErrClosed
	}
	return b, nil
}

// placementGenUnitForKey resolves the generation-qualified unit OWNERSHIP uses
// for key. It differs from genUnitForKey (the local-db resolve) in exactly one
// window: an R=1 in-flight split, where placement is PARENT-ANCHORED - it
// resolves GenUnit{g, K} ignoring cutOver, so every node (whatever subset of
// cut-over markers it has observed) forwards K's key-space to the SAME node,
// K's gen-g owner, for the whole window. Only that owner's local resolution
// honors cutOver, which is what makes its pause-held flip the single write-path
// boundary (see this file's header). Steady state and R>1 resolve identically
// to genUnitForKey.
func (c *Cluster) placementGenUnitForKey(key []byte) storageunit.GenUnit {
	h := storageunit.HashShardKey(c.shardKey(key))
	gs := c.genSnapshot()
	if !c.replicaLayout() && !gs.nextCount.IsZero() {
		return storageunit.NewGenUnit(gs.gen, storageunit.UnitForHash(h, gs.count))
	}
	return gs.resolveGenUnit(h)
}
