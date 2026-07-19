// Replicated multi-backend node (v0.8 Phase 2b): R>1 per-UNIT replication
// under a STATIC topology.
//
// Phase 2 (multibackend.go) is single-replica (R=1): each unit has ONE
// durable database and ONE owner. Phase 2b extends it to R>1: each unit has R
// INDEPENDENT durable databases (one per replica position), mounted on R
// different nodes (the unit's replica set = LocateKeyN over the unit id). A
// write fans out to the unit's R replica nodes and a read applies the
// configured consistency across them, reusing the LEGACY R>1 envelope /
// apply-if-newer / quorum machinery (cas.go, apply_if_newer.go,
// apply_batch.go, replicate.go) re-keyed from per-NODE to per-UNIT.
//
// THE ONLY NEW CODE is the per-unit routing + the multi-backend dispatchers
// that apply against the key's MOUNTED unit backend instead of the single
// c.backend. The legacy per-node R>1 path (keyed to ring.LocateKeyN over the
// raw shard key) is UNTOUCHED; the R=1 multi-backend paths (Phase 2/3/4) are
// untouched outside their own branches.
//
// THE INVARIANT, PER UNIT (identical structure to the legacy R>1 proof):
//
//	NO ACKED WRITE MAY BE LOST.
//
//   - FAN-OUT ACK ACCOUNTING: a write is acked only after W of the unit's R
//     replicas durably applied it (requiredWriteAcks); a replica mid-acquire
//     returns the transient errUnitAcquiring, counting toward neither budget.
//   - NEVER-CLOBBER APPLY: every replica applies APPLY-IF-NEWER (txApplyIfNewer),
//     so a reordered older write or a stale read-repair self-resolves to a no-op.
//   - INDEPENDENCE: the R replicas are independent durable databases, so the
//     loss of one node leaves W-1+ complete copies; an acked write reached W
//     replicas, so a quorum read still finds it.
//
// SCOPE: STATIC unit -> replica-set topology fixed at Open. Membership-change
// lease handoff at R>1 and resharding at R>1 are OUT of scope (later phases),
// exactly as Phase 2 bounded its static topology before Phase 3 added handoff.

package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// multiReplicated reports whether this cluster runs the R>1 (replicated)
// multi-backend paths: multi-backend mode, R>1, and a populated ring. It is
// the multi analogue of casReplicated(): when true, Put / Get / Delete /
// CommitCASApply route to the per-unit replicated paths below; when false
// (multi + R=1) the Phase 2/3/4 single-mount paths run unchanged. A nil /
// empty ring (single-node) collapses to the single-mount R=1 path, which is
// correct: with one node there is one replica.
func (c *Cluster) multiReplicated() bool {
	return c.multi && c.replicationFactor() > 1 && c.ring != nil && !c.ring.Empty()
}

// unitReplicas returns the ordered replica set (primary + R-1 ring
// successors) for the generation-qualified unit gu: ring.LocateKeyN over
// genUnitBytes(gu), the SAME successor-chain machinery the legacy per-node
// R>1 path uses, but hashing the unit id instead of the raw key's shard key.
// Co-location holds by construction: every key in a {tag} set hashes to one
// unit, so the set's whole replica placement is identical. With a nil / empty
// ring it returns the local node (single-node: this node is the one replica).
func (c *Cluster) unitReplicas(gu storageunit.GenUnit) []ring.Member {
	if c.ring == nil || c.ring.Empty() {
		return []ring.Member{{ID: c.cfg.NodeID, Addr: c.cfg.GRPCAddr}}
	}
	return c.ring.LocateKeyN(genUnitBytes(gu), c.replicationFactor())
}

// replicaLayout reports whether this cluster addresses storage BY REPLICA
// POSITION, i.e. whether every mount it opens is a storageunit.ReplicaMount
// rather than a storageunit.SoleMount. It is THE LAYOUT SELECTOR, and it is the
// one thing that decides which of the two on-disk encodings this cluster's
// bytes live at.
//
// IT IS NOT multiReplicated(), and substituting one for the other CORRUPTS
// ADDRESSING. multiReplicated() additionally requires a populated ring, so on a
// SINGLE-NODE R>1 cluster (nil ring) the two disagree: such a cluster mounts by
// replica position (this predicate, true) while serving reads and writes
// through the single-owner path (multiReplicated(), false). That divergence is
// deliberate and pre-existing; see the package doc. Addressing storage through
// multiReplicated() would make a single-node R>1 cluster resolve its mounts to
// the SOLE encoding while its own earlier boots wrote them to the REPLICA
// encoding, which reads as total data loss.
//
// It replaces the old "does this factory implement ReplicaBackendFactory"
// question. That question was the leak: it asked the adapter what it could do.
// This asks only what THIS CLUSTER is, which is fixed at Open by config.
func (c *Cluster) replicaLayout() bool {
	return c.multi && c.replicationFactor() > 1
}

// mountRefFor is the ONE place a ReplicaUnit the cluster tracks becomes the
// MountRef the storage port is keyed by. Every mount this cluster holds is
// tracked as a ReplicaUnit (mountMap is ReplicaUnit-keyed, with the R=1 paths
// using position 0), but the PORT needs the layout selector too, so the
// conversion has to go through the cluster's own layout.
//
// Route every ReplicaUnit-keyed factory call through this rather than calling
// storageunit.ReplicaMount or storageunit.SoleMount at the call site: a call
// site that picks the wrong one addresses the wrong bytes, and the failure mode
// is a silently-empty mount rather than an error.
func (c *Cluster) mountRefFor(ru storageunit.ReplicaUnit) storageunit.MountRef {
	if c.replicaLayout() {
		return storageunit.ReplicaMount(ru)
	}
	return storageunit.SoleMount(ru.Unit)
}

// desiredReplicaUnits returns the units this node should have mounted at R>1,
// each paired with the replica POSITION this node holds it at. A unit gu is
// desired iff self appears anywhere in unitReplicas(gu); the position is
// self's index in that set. It is the R>1 analogue of desiredGenUnits,
// generation-aware via genSnapshot, and (v0.9) reshard-aware: while a split is
// in flight it ALSO desires the gen-(g+1) children (desiredReplicaUnitsVia).
func (c *Cluster) desiredReplicaUnits() []storageunit.ReplicaUnit {
	return c.desiredReplicaUnitsVia(c.unitReplicas)
}

// desiredReplicaUnitsVia is the shared core of desiredReplicaUnits and its
// draining-excluded sibling desiredPendingReplicaUnits: the two differ ONLY in
// how a unit's replica set is resolved (the live ring vs the ring with draining
// members excluded), so both pass that resolver in as replicaAt and share
// everything else. This keeps the two desired-set producers in lockstep - a
// requirement for the overlap reconcile, which derives both its current and
// pending sets from them and would treat any divergence as a spurious
// transition.
//
// The gen-g pass uses ownedReplicaUnitsAt to enumerate the units this node
// replicates at the live generation. When a reshard is in flight
// (genState.nextCount != 0) it then appends this node's gen-(g+1) targets: a
// SPLIT's children co-located at their parent's slot (splitChildrenVia), or a
// MERGE's survivors at their own gen-(g+1) ring home (ownedReplicaUnitsAt at
// gen+1). Both augmentations are no-ops in steady state, so the common path is
// unchanged.
func (c *Cluster) desiredReplicaUnitsVia(replicaAt func(gu storageunit.GenUnit) []ring.Member) []storageunit.ReplicaUnit {
	gs := c.genSnapshot()
	self := storageunit.NodeID(c.cfg.NodeID)

	out := c.ownedReplicaUnitsAt(gs.gen, gs.count, self, replicaAt)
	if !gs.nextCount.IsZero() {
		if gs.nextCount.N() > gs.count.N() { // SPLIT: co-located children
			out = append(out, c.splitChildrenVia(gs, self, replicaAt)...)
		} else { // MERGE: survivors at their gen-(g+1) ring home
			out = append(out, c.ownedReplicaUnitsAt(gs.gen+1, gs.nextCount, self, replicaAt)...)
		}
	}
	return out
}

// ownedReplicaUnitsAt returns the ReplicaUnits this node replicates among the
// `count` units at generation `gen`, resolving each unit's replica set via
// replicaAt (the live ring, or the draining-excluded ring for the pending set).
// It is the shared core for both the gen-g pass and a merge's gen-(g+1) survivor
// pass.
func (c *Cluster) ownedReplicaUnitsAt(gen storageunit.Generation, count storageunit.UnitCount, self storageunit.NodeID, replicaAt func(gu storageunit.GenUnit) []ring.Member) []storageunit.ReplicaUnit {
	// Adapt the ring-backed replica lookup to the pure ReplicaLookup contract:
	// map a UnitID to its replica nodes at `gen`, projecting each ring member id
	// into a storageunit.NodeID.
	replicas := storageunit.ReplicaLookupFunc(func(u storageunit.UnitID) []storageunit.NodeID {
		return memberNodeIDs(replicaAt(storageunit.NewGenUnit(gen, u)))
	})
	owned := storageunit.OwnedReplicaUnits(self, count, replicas)
	out := make([]storageunit.ReplicaUnit, 0, len(owned))
	for _, o := range owned {
		out = append(out, storageunit.NewReplicaUnit(storageunit.NewGenUnit(gen, o.Unit), o.Replica))
	}
	return out
}

// memberNodeIDs projects a ring member set into the pure-domain NodeID slice the
// ReplicaLookup contract speaks.
func memberNodeIDs(set []ring.Member) []storageunit.NodeID {
	nodes := make([]storageunit.NodeID, len(set))
	for i, m := range set {
		nodes[i] = storageunit.NodeID(m.ID)
	}
	return nodes
}

// mountReplicaUnits opens every unit this node replicates at its replica
// position into the mount map, via the per-replica factory (independent
// durable databases). It is the R>1 analogue of initMultiBackend's mount
// loop. On any open error it rolls back what it already mounted so Open fails
// cleanly. Caller is initMultiBackend, on the replicaLayout() branch.
func (c *Cluster) mountReplicaUnits() error {
	units := c.desiredReplicaUnits()

	// BOUNDED-CONCURRENCY MOUNT (Phase 2g). Open owned positions through a worker
	// pool, not strictly one at a time. Each (unit, replica) is an INDEPENDENT
	// durable database (its own dbName) and the single-writer fence is per-dbName,
	// so concurrent opens of DISTINCT positions never fence one another; the bound
	// keeps the cold-start open burst from overwhelming the shared object store.
	// Time-to-Ready drops from sum(per-open latency) to ~ceil(n/conc) opens.
	// conc==1 reproduces the strictly-sequential mount exactly. The per-position
	// rules below (defer-on-marker, skip-on-error, mount-on-success) are
	// UNCHANGED; only the iteration is parallel, and the set of positions that end
	// up mounted/skipped/deferred is identical to the serial mount.
	conc := c.openConcurrency()
	if conc > len(units) {
		conc = len(units)
	}

	var mounted, skipped, deferred atomic.Int64
	var g errgroup.Group
	if conc > 0 {
		g.SetLimit(conc)
	}

	for _, ru := range units {
		ru := ru // capture per iteration (worker closure)
		g.Go(func() error {
			// FENCING SAFETY (Phase 2f): never OPEN a position a live peer is already
			// serving. The open ITSELF fences the peer - slatedb bumps the durable
			// writer-epoch inside DbBuilder.Build(), so even an open that later ERRORS
			// has ALREADY fenced any peer holding the position (the peer then fails
			// every op with "detected newer DB client"). A re-bootstrapping seed pod
			// (empty/unreachable seeds -> a 1-node ring -> it desires EVERY position)
			// that opened everything would fence every serving peer, turning a one-node
			// restart into a cluster-wide write outage. So read the FENCE-FREE serving
			// marker FIRST (a sibling object, not a slatedb open): if a peer reached
			// Ready serving this position, SKIP it at boot (desired-but-unmounted),
			// never fencing the live peer. BOOT-ONLY: the reconcile-time acquire does
			// NOT consult the marker (it is the legitimate ownership-change path that
			// SHOULD take over), so a genuine cold start (no markers) still mounts
			// everything, and a true sole-survivor re-acquires a stale-marked position
			// once the ring converges.
			if epoch, ok, merr := c.factory.ReadServingMarker(storageunit.ReplicaMount(ru)); merr == nil && ok && epoch > 0 {
				c.lastAcquireErr.Store(ru, fmt.Sprintf("boot-deferred: a peer is serving this position (serving marker epoch %d); not fenced - reconcile hands off after convergence", epoch))
				c.logf("shale: boot defer - NOT opening replica %s: a peer is serving it (marker epoch %d); "+
					"avoiding a fence; reconcile will acquire it after ring convergence", ru, epoch)
				deferred.Add(1)
				return nil
			}
			b, openedEpoch, err := c.factory.OpenUnit(storageunit.ReplicaMount(ru), epochAtOpen)
			if err != nil {
				// DEGRADED BOOT (Phase 2f): a replica position whose backing store
				// cannot be opened - a corrupt/truncated durable database, or an open
				// that stalls past SLATE_DB_OPEN_TIMEOUT (buildDb's runWithTimeout
				// returns an ERROR, not a hang) - is SKIPPED, not fatal. Aborting Open
				// here (the old behavior) bricked the WHOLE node on one bad replica,
				// even though every other position it holds is healthy and the damaged
				// unit is still served by its surviving replica(s). Record the error
				// for /debug/shale/state (EXACTLY as the reconcile-time
				// acquireReplicaUnit does for a desired-but-unmountable position) and
				// log it loudly so a degraded boot is never silent, then CONTINUE. The
				// position stays DESIRED-BUT-UNMOUNTED; the periodic reconcile retries
				// the open, so a transient failure self-heals and a permanent one stays
				// observable until repaired. Reads to the unit are served by the peer;
				// writes to it block under W (bounded, never lost) until it is restored.
				c.lastAcquireErr.Store(ru, err.Error())
				c.logf("shale: DEGRADED BOOT - skipping replica %s: open failed: %v "+
					"(unit served by peer; desired-but-unmounted, reconcile will retry)", ru, err)
				skipped.Add(1)
				return nil
			}
			c.lastAcquireErr.Delete(ru)
			c.myOpenEpoch.Store(ru, openedEpoch) // this node's exact open epoch (drain gate)
			c.mountMu.Lock()
			c.storeMount(ru, b)
			c.mountMu.Unlock()
			mounted.Add(1)
			return nil
		})
	}
	// Workers never return an error: degraded boot is non-fatal PER POSITION (skip
	// + record + reconcile-retry), so a bad position must NOT cancel its siblings.
	// g.Wait therefore always returns nil here; the bound is the only thing Wait
	// enforces (it blocks until every queued open has run).
	_ = g.Wait()

	if skipped.Load() > 0 || deferred.Load() > 0 {
		c.logf("shale: degraded boot complete - %d mounted, %d SKIPPED (un-openable), %d DEFERRED "+
			"(a peer is already serving - NOT fenced; reconcile hands off after convergence); see /debug/shale/state",
			mounted.Load(), skipped.Load(), deferred.Load())
	}
	// JOIN write-transparency (v0.8 Phase 2e, entry side): if this node BOOT-DEFERRED
	// one or more owned positions (a peer is serving them, so we did not fence it),
	// it is WARMING into the ring - advertise the Joining bit so every node excludes
	// it from the CURRENT set (quorum-floored) until it has mounted every position
	// it owns. That keeps the still-mounted displaced peer the stable holder and
	// routes this node's positions through the pending-owner acquire path (which
	// serving-marks them, gating the displaced peer's drain), the exact mirror of a
	// graceful leave. The reconcile clears the bit once every owned position is
	// mounted (maintainJoiningState). A cold start defers nothing, so the bit is
	// never set and first-cluster convergence is unchanged.
	if deferred.Load() > 0 {
		c.setSelfJoining(true)
	}
	return nil
}

// logf writes a one-line cluster-level event to cfg.LogOutput, FALLING BACK to
// os.Stderr when no sink is wired. The fallback is deliberate: degraded boot is
// a rare, operator-critical event (a node came up missing a replica), so it must
// NOT be silent just because the deployment did not wire a log sink - otherwise
// the crash-loop this change removes is merely replaced by a silently half-
// functional node. We do NOT route this through the (nil-in-prod) LogOutput-only
// path that membership's verbose internal logger uses, precisely so this stays
// visible without re-enabling memberlist's DEBUG spam. It is only ever called
// for a degraded boot (once, at Open), never on a hot path.
func (c *Cluster) logf(format string, args ...any) {
	w := c.cfg.LogOutput
	if w == nil {
		w = os.Stderr
	}
	_, _ = fmt.Fprintf(w, format+"\n", args...)
}

// applyEnvelopeIfNewerToUnit is the multi-backend analogue of
// applyEnvelopeIfNewer: it applies an incoming LWW envelope APPLY-IF-NEWER
// against the key's MOUNTED unit backend (resolved under the write-pause via
// localWriteBackendForKey) instead of the single c.backend. The never-clobber
// compare runs in one transaction on that unit, under c.applyMu (the shared
// applyEnvelopesTx body), so two concurrent applies on the same key cannot
// race. ok=false from the resolve (owner-but-unmounted, the static-topology
// window is only the Open mount, so in practice always mounted here) yields
// the retryable acquiring-window error the fan-out tolerates.
func (c *Cluster) applyEnvelopeIfNewerToUnit(key, incomingEnvBytes []byte) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	if _, _, err := decodeStamp(incomingEnvBytes); err != nil {
		return err
	}

	b, ru, unlock, ok := c.localWriteBackendForKey(key)
	defer unlock()
	if !ok {
		return errUnitAcquiring("Put")
	}

	mapBegin, mapTx := c.unitApplyErrMaps(ru, b, "Put")
	_, err := c.applyEnvelopesTx(b, []EnvelopeWrite{{Key: key, Envelope: incomingEnvBytes}}, mapBegin, mapTx)
	return err
}

// fenceToTransient recodes a FENCED apply error (backend.ErrFenced, surfaced when
// a higher-epoch owner of this position fenced this now-stale writer) to the
// TRANSIENT acquiring-window error, evicting the stale mount, so the fan-out
// RETRIES the write onto the re-resolved union instead of HARD-failing it (a hard
// error fast-fails the write once too many legs miss the stable-R quorum). The
// fenced op did NOT durably apply, so not counting it as an ack is correct; the
// successor that fenced this leg serves the write once it finishes mounting (the
// cost is added latency in the mount window, not a lost or rejected write). The
// fence can surface at ANY apply op on real slatedb - the Get inside
// txApplyIfNewer, the buffered Put, or the Commit flush - so EVERY apply error
// site routes through here, not just Commit. evictStaleMount is a no-op for a
// Draining mount (the fence is the expected handoff signal there) and re-acquires
// a stale non-draining one. A non-fence error passes through unchanged (stays a
// hard failure). The in-memory test double fences at Begin (already recoded by
// the Begin path); only real slatedb / the fence-at-commit double reach here.
func (c *Cluster) fenceToTransient(ru storageunit.ReplicaUnit, b backend.Backend, op string, err error) error {
	if errors.Is(err, backend.ErrFenced) {
		c.evictStaleMount(ru, b)
		return errUnitAcquiring(op)
	}
	return err
}

// unitApplyErrMaps returns the two applyEnvelopesTx error mappers every
// mounted-unit apply site shares, closing over the resolved mount (ru, b)
// and the op name carried by the transient refusal: a Begin failure
// UNCONDITIONALLY evicts the (stale) mount and reports the retryable
// acquiring-window error, and an in-transaction failure recodes a fence
// to that same transient via fenceToTransient (a non-fence error passes
// through unchanged, staying a hard failure).
func (c *Cluster) unitApplyErrMaps(ru storageunit.ReplicaUnit, b backend.Backend, op string) (mapBeginErr, mapTxErr func(error) error) {
	return func(error) error {
			c.evictStaleMount(ru, b)
			return errUnitAcquiring(op)
		}, func(err error) error {
			return c.fenceToTransient(ru, b, op, err)
		}
}

// acquireReplicaUnit mounts replica position ru.Replica of unit ru.Unit via
// the per-replica factory, opening at a strictly higher epoch than this node's
// current durable lease for that replica position (the fence). On error the
// replica is left unmounted (a routed op gets the retryable acquiring-window
// error and the next reconcile retries). Caller MUST hold reconcileMu.
//
// It writes the durable SERVING MARKER after mounting, EXACTLY as the overlap
// acquire does. This is REQUIRED for the graceful-leave (scale-down) drain to
// complete: a position moving OFF a leaving node can land on its successor via
// THIS clean-cut path (initial-convergence / pure-new-mount), not only the
// pending-owner overlap path. The leaving node is DRAINING that exact position
// and releases ONLY on a serving marker strictly above its open epoch; if this
// path mounted silently (no marker, the old behavior) the draining leaver would
// wait out the full grace timeout. The clean-cut gainer opens at durable+1
// (strictly above the leaver's open epoch), so the marker it writes here releases
// the draining leaver. The marker is monotonic + idempotent, so writing it on a
// pure new mount (no draining leaver) is a harmless no-op observer-wise.
func (c *Cluster) acquireReplicaUnit(ru storageunit.ReplicaUnit) {
	epoch := acquireBaseEpoch
	b, openedEpoch, err := c.factory.OpenUnit(storageunit.ReplicaMount(ru), epoch)
	if err != nil {
		// SELF-HEAL a factory/cluster desync (the mass-restart auto-recovery wedge,
		// #408): this path only runs for a DESIRED-but-UNMOUNTED position, so the
		// cluster has no live mount for ru. If the factory nonetheless refuses the
		// open because it still holds ru open on this handle ("already open on this
		// handle"), its openReplica state is STALE relative to mountMap and every
		// retry fails identically forever. Close the stale handle to re-sync, then
		// reopen ONCE. The close is safe: there is no mountMap entry pointing at it,
		// so no routed op can be reading it. If the reopen still fails (a genuine
		// open failure, e.g. a peer holds the durable db), record it + retry next tick.
		_ = c.factory.CloseUnit(storageunit.ReplicaMount(ru))
		b, openedEpoch, err = c.factory.OpenUnit(storageunit.ReplicaMount(ru), epoch)
		if err != nil {
			c.lastAcquireErr.Store(ru, err.Error())
			return
		}
	}
	c.lastAcquireErr.Delete(ru)
	// Record THIS node's exact open epoch (from the factory return) BEFORE the
	// mount, so a beginDrain that sees mountMap[ru] also sees the epoch.
	c.myOpenEpoch.Store(ru, openedEpoch)
	c.mountMu.Lock()
	if c.closed.Load() {
		c.mountMu.Unlock()
		c.myOpenEpoch.Delete(ru)
		_ = c.factory.CloseUnit(storageunit.ReplicaMount(ru))
		return
	}
	c.storeMount(ru, b)
	c.mountMu.Unlock()

	// Write the durable serving marker AFTER the mount (outside the lock: shared
	// storage I/O), at THIS node's EXACT open epoch (the factory return value, NOT
	// a re-read of the climbing durable). This is the poll-observable release
	// signal a DRAINING predecessor reads (drainCheck); without it a clean-cut
	// successor of a leaving node never releases that node's drain.
	_ = c.factory.WriteServingMarker(storageunit.ReplicaMount(ru), openedEpoch)
}

// releaseReplicaUnit unmounts the ReplicaUnit ru via the per-replica factory.
// The mount-map entry is removed BEFORE the close so a routed op stops resolving
// the local backend immediately. Caller MUST hold reconcileMu.
func (c *Cluster) releaseReplicaUnit(ru storageunit.ReplicaUnit) {
	c.mountMu.Lock()
	delete(c.mountMap, ru)
	c.mountMu.Unlock()
	c.myOpenEpoch.Delete(ru) // a re-acquire records a fresh open epoch.
	_ = c.factory.CloseUnit(storageunit.ReplicaMount(ru))
}

// applyBatchToUnit is the multi-backend analogue of ApplyBatchLocal's apply
// loop: it applies a CAS write-set APPLY-IF-NEWER against the batch's MOUNTED
// unit in ONE transaction, under c.applyMu. Every key in a CAS commit
// co-shards with the pin key (the cross-shard guard guarantees it), so one
// unit covers the whole batch; it is resolved from the first key. An empty
// batch is a no-op. An unmounted unit (static-topology window) returns the
// retryable acquiring-window error the owner's fan-out tolerates.
func (c *Cluster) applyBatchToUnit(writes []EnvelopeWrite) error {
	if len(writes) == 0 {
		return nil
	}
	b, ru, unlock, ok := c.localWriteBackendForKey(writes[0].Key)
	defer unlock()
	if !ok {
		return errUnitAcquiring("ApplyBatch")
	}
	return c.applyBatchToBackend(b, ru, writes)
}

// applyBatchToReplicaUnit applies a CAS write-set APPLY-IF-NEWER against the
// EXPLICIT mounted position ru (the position-addressed sibling of
// applyBatchToUnit). It is the owner-side extra-position apply of the CAS
// write-set fan-out (workstream B): a designated owner that holds TWO positions
// of the pin unit mid-transition (the index-shuffling survivor: its draining old
// slot AND its acquired new slot) commits owner-locally against one and applies
// the batch to the other through this, so both durable copies carry the write
// exactly as the put path's dual-position routing achieves. Mirrors
// resolveAndApplyReplicaPut's reshard pause discipline. An unmounted ru returns
// the retryable acquiring-window error (counts as no ack, never a failure).
func (c *Cluster) applyBatchToReplicaUnit(ru storageunit.ReplicaUnit, writes []EnvelopeWrite) error {
	if len(writes) == 0 {
		return nil
	}
	if c.parentSlotMidSplit(ru) {
		pause := c.pauseLockFor(ru.Unit.ID)
		pause.RLock()
		defer pause.RUnlock()
	}
	b, ok := c.localBackendForReplicaUnit(ru)
	if !ok {
		return errUnitAcquiring("ApplyBatch")
	}
	return c.applyBatchToBackend(b, ru, writes)
}

// applyBatchToBackend is the shared apply loop of applyBatchToUnit and
// applyBatchToReplicaUnit: ONE transaction on the resolved mounted backend,
// APPLY-IF-NEWER per key, under c.applyMu (the shared applyEnvelopesTx body),
// with every backend error site fence-recoded to the transient acquiring-
// window error (the fan-out classifies it as neither ack nor failure). A
// corrupt entry envelope stays a raw hard error.
func (c *Cluster) applyBatchToBackend(b backend.Backend, ru storageunit.ReplicaUnit, writes []EnvelopeWrite) error {
	mapBegin, mapTx := c.unitApplyErrMaps(ru, b, "ApplyBatch")
	_, err := c.applyEnvelopesTx(b, writes, mapBegin, mapTx)
	return err
}

// putReplicatedUnit stamps + envelopes + fans a write out to the R replica
// nodes of key's UNIT, waiting for W acks per WriteConsistency. value == nil
// is the Delete / tombstone shape.
//
// v0.8 Phase 2d (write-availability through a membership change): the write
// is wrapped in a WriteTimeout-bounded RETRY (retryWriteThroughHandoff). The
// stamp is computed ONCE and the same envelope bytes are reused across every
// attempt, so a retry is idempotent: apply-if-newer makes a re-applied
// identical envelope a no-op (its stamp does not strictly beat the stored
// equal stamp), and the LWW ordering of this write relative to concurrent
// writes is stable regardless of how many attempts it takes. The replica set
// is re-resolved from the LIVE ring on every attempt, so a write started mid-
// reassignment lands on the post-reassignment replica set once it settles.
func (c *Cluster) putReplicatedUnit(key, value []byte) error {
	stamp := Stamp{
		TimestampNanos: c.stamps.Next(),
		NodeID:         c.cfg.NodeID,
	}
	envBytes := Encode(Envelope{Stamp: stamp, Payload: value})
	return c.retryWriteThroughHandoff(func(attemptCtx context.Context) writeAttempt {
		return c.putReplicatedUnitAttempt(attemptCtx, key, envBytes)
	})
}

// putReplicatedUnitAttempt is ONE fan-out pass of a replicated unit write. It
// re-resolves the replica set from the live ring, fans the pre-stamped
// envelope out to the unit's R replica nodes, and reports a structured
// writeAttempt the retry wrapper classifies (acks-met vs acquiring-shortfall
// vs hard-failure). It carries no retry policy of its own.
func (c *Cluster) putReplicatedUnitAttempt(ctx context.Context, key, envBytes []byte) writeAttempt {
	// v0.9 (decentralized online split): while a split is in flight, DUAL-WRITE
	// to the key's parent + child generations, acking over the AUTHORITATIVE legs
	// only (parent pre-cut-over, child after). Takes precedence over the
	// single-generation path below; inactive (ok=false) when no split is running.
	if legs, ok := c.routedReplicasForReshard(key); ok {
		return c.putReshardDualWrite(ctx, legs, key, envBytes)
	}
	// v0.8 Phase 2e (pending ranges): route to the UNION of current + pending
	// owners during a membership transition (DUAL-WRITE), but hold the ack bar W
	// at the STABLE R quorum (over stableR = len(current)), NOT raised to cover
	// the transient extra union member. The pending replica is a bonus write
	// target. In steady state routed == the stable set and stableR == len(routed),
	// so this is the unchanged static path.
	routed, stableR := c.routedReplicasWithUnit(key)
	if len(routed) == 0 {
		return writeAttempt{err: status.Error(codes.Unavailable, "shale: no replicas available for key")}
	}
	replicas := make([]ring.Member, len(routed))
	for i, rr := range routed {
		replicas[i] = rr.member
	}
	w := c.writeAckBar(stableR)

	// Position-address by the fan-out INDEX, not the member ID: under a leave that
	// shuffles replica indices, a survivor is routed to TWO positions (the slot it
	// is draining AND the slot it acquired), so the same member appears twice in
	// routed at different ru. A member-keyed lookup would collapse those onto one
	// position, leaving the acquired slot unwritten - the during-leave
	// write-availability gap. idx maps each fan-out goroutine to its exact
	// routedReplica.
	acks, errs, transient, resultsCh := fanout(ctx, replicas, w,
		func(opCtx context.Context, idx int, replica ring.Member) ([]byte, error) {
			return nil, c.dispatchReplicaPutUnit(opCtx, replica, routed[idx].ru, key, envBytes)
		})

	go func() {
		//nolint:revive // empty-block: idiomatic channel drain.
		for range resultsCh {
		}
	}()

	return classifyWriteAttempt(acks, w, errs, transient)
}

// dispatchReplicaPutUnit is the multi-backend analogue of dispatchReplicaPut:
// the local-self branch applies the envelope APPLY-IF-NEWER into the key's
// MOUNTED unit (applyEnvelopeIfNewerToUnit) instead of c.applyEnvelopeIfNewer;
// the remote branch dispatches PutForwarded to the replica node, whose RPC
// handler lands in LocalReplicaPut (multi R>1 branch). A frozen / acquiring
// replica returns the transient code the fan-out tolerates.
func (c *Cluster) dispatchReplicaPutUnit(ctx context.Context, replica ring.Member, ru storageunit.ReplicaUnit, key, envBytes []byte) error {
	if replica.ID == c.cfg.NodeID {
		// v0.8 Phase 2e (pending ranges): apply the union dual-write to the EXPLICIT
		// position ru this member holds. For a current owner ru is its current
		// index; for a pending owner ru is the slot it acquired (which its own
		// current-set ring index would NOT resolve, since a pending owner is absent
		// from the current set). applyEnvelopeIfNewerToBackend resolves mountMap[ru]
		// directly. A pending owner still mid-mount has no mounted entry for ru and
		// returns errUnitAcquiring; the union covers the key via the still-mounted
		// current owner and the fan-out tolerates the transient. There is NO
		// per-position forward (the union routes directly to both current and
		// pending owners).
		if c.isFrozen() {
			// Reshard write-freeze (Phase 4 / multi-node reshard): refuse with the
			// retryable error, same as the remote leg, so no write is acked during
			// the static bisect.
			return errWriteFrozen("Put")
		}
		// resolveAndApplyReplicaPut quiesces a parent-slot write around the v0.9
		// finalize retire (pause read side) so no acked write lands on a retiring
		// parent; outside a split it is the plain resolve + apply.
		return c.resolveAndApplyReplicaPut(ru, key, envBytes)
	}
	cli, err := c.clientFor(replica.Addr)
	if err != nil {
		return err
	}
	// Position-addressed forward: carry the explicit ru so the remote replica
	// resolves mountMap[ru] directly (a pending owner is not at this position in
	// its own current-set ring index).
	return cli.PutAtReplica(ctx, ru, key, envBytes)
}

// getReplicatedUnit fetches the LWW winner across N of the unit's R replica
// nodes per ReadConsistency, read-repairing laggards. It mirrors getReplicated
// verbatim except the replica set is the unit's (routedReplicasForKey) and the
// local-self read serves from the mounted unit backend (via the multi
// dispatcher). Read-repair rides dispatchReplicaPutUnit, so a stale repair is
// a never-clobber no-op against the mounted unit.
func (c *Cluster) getReplicatedUnit(key []byte) ([]byte, error) {
	// v0.8 Phase 2e (pending ranges): read across the ROUTED union (current +
	// pending owners) during a transition so any union member that physically
	// holds the position can answer (a mid-mount pending owner returns the
	// transient acquiring error the fan-out skips). In steady state routed == the
	// stable set, so this is the unchanged static path. The read N (quorum / all)
	// is computed over the STABLE replica count, NOT widened by the union, matching
	// the write ack bar.
	// POSITION-ADDRESSED union read (workstream B): resolve the routed union WITH
	// each member's ReplicaUnit and dispatch by explicit position, exactly as the
	// write fan-out does. The member-keyed forward (GetForwarded without a ru) is
	// guard-gated on the receiver's ring view, which REFUSES a displaced current
	// owner during a join for a key it does not physically hold (the fresh-key
	// If-None-Match read) - the position-addressed wire (GetAtReplica ->
	// LocalReplicaGetAt) resolves mountMap[ru] directly with no guard, so every
	// union leg answers value / not-found / acquiring on its own mount state. In
	// steady state routed == the stable set at its ring indices, so the resolved
	// backends are identical to the old member-keyed path.
	// ALL-LEGS-TRANSIENT RETRY (docs/SPEC.md "Union reads" guard 2): one sweep
	// that finds every union leg transiently unable to serve (mid-acquire, or
	// the fence-at-open-start blip recodes) is RE-POLLED within the ReadTimeout
	// budget - the read mirror of the write path's retry-through-handoff - so a
	// sub-second transient window is bounded latency, not a client error. Each
	// attempt re-resolves the routed union from the live ring; any non-transient
	// outcome returns immediately.
	return retryReadThroughHandoff(c, func(deadline time.Time) ([]byte, error) {
		return c.getReplicatedUnitOnce(deadline, key)
	})
}

// getReplicatedUnitOnce is ONE union read sweep (the pre-retry getReplicatedUnit
// body): resolve the routed union, fan out position-addressed reads, gather per
// ReadConsistency, LWW-select, read-repair. It returns the acquiring-tagged
// error ONLY for the all-legs-transient outcome, which is the retry wrapper's
// re-poll signal.
func (c *Cluster) getReplicatedUnitOnce(deadline time.Time, key []byte) ([]byte, error) {
	routed, stableR := c.routedReplicasWithUnit(key)
	if len(routed) == 0 {
		return nil, status.Error(codes.Unavailable, "shale: no replicas available for key")
	}
	rc := c.cfg.ReadConsistency
	// N is the stable read quorum, NOT widened by a transient union member; clamp
	// to the routed size so a single-replica routed set is still answerable.
	n := min(requiredReadReplicas(rc, stableR), len(routed))
	queried := make([]ring.Member, len(routed))
	for i, rr := range routed {
		queried[i] = rr.member
	}

	// The fan-out shares the retry wrapper's overall wall-clock deadline, so
	// attempts never stack budgets past ReadTimeout.
	fanoutCtx, cancelFanout := context.WithDeadline(context.Background(), deadline)
	_, _, _, resultsCh := fanout(fanoutCtx, queried, n,
		func(ctx context.Context, idx int, replica ring.Member) ([]byte, error) {
			return c.dispatchReplicaGetUnitAt(ctx, replica, routed[idx].ru, key)
		})

	gathered := make([]collected, 0, len(queried))
	var nonTransientErr error
	var firstUnreachable error
	sawHandoffTransient := false
	usable := 0

	for res := range resultsCh {
		if res.Err != nil {
			// READ-leg classification (docs/SPEC.md "Union reads" guard 2):
			// handoff-class transients (acquiring / fenced-recode / closed-mid-
			// release) earn the full-budget re-poll; a bare unreachable leg is
			// skipped too but tracked separately - unreachable-only sweeps get
			// the capped re-poll, then surface the dial error verbatim.
			if isTransientReadLegErr(res.Err) {
				sawHandoffTransient = true
				continue
			}
			if isUnreachableLegErr(res.Err) {
				if firstUnreachable == nil {
					firstUnreachable = res.Err
				}
				continue
			}
			if errors.Is(res.Err, backend.ErrNotFound) {
				if usable < n {
					gathered = append(gathered, collected{member: res.Member, env: Envelope{}, hadValue: false})
					usable++
				}
				continue
			}
			if nonTransientErr == nil {
				nonTransientErr = res.Err
			}
			continue
		}
		env, err := Decode(res.Value)
		if err != nil {
			if nonTransientErr == nil {
				nonTransientErr = err
			}
			continue
		}
		if usable < n {
			gathered = append(gathered, collected{member: res.Member, env: env, hadValue: true})
			usable++
		} else if rc != ReadNearest {
			gathered = append(gathered, collected{member: res.Member, env: env, hadValue: true})
		}
	}
	cancelFanout()

	if len(gathered) == 0 {
		if nonTransientErr != nil {
			return nil, nonTransientErr
		}
		if sawHandoffTransient {
			// Every union leg reported the transient acquiring window and none
			// answered (all routed positions mid-handoff at once). Surface the
			// RETRYABLE acquiring error, never ErrNotFound: a key that exists
			// must not be reported absent because its unit is mid-transition
			// everywhere (docs/SPEC.md "Union reads", guard 2).
			return nil, errUnitAcquiring("Get")
		}
		if firstUnreachable != nil {
			// Only unreachable legs: the capped re-poll's signal (the wrapper
			// unwraps the marker, so callers only ever see the dial error).
			return nil, &unreachableOnlyError{inner: firstUnreachable}
		}
		return nil, backend.ErrNotFound
	}

	winner := gathered[0]
	for _, g := range gathered[1:] {
		if g.hadValue && (!winner.hadValue || g.env.Stamp.Greater(winner.env.Stamp)) {
			winner = g
		}
	}

	// Ratchet the node's stamp clock with the winning stamp (a no-op
	// zero for a no-value winner); see stampclock.go and the single-
	// backend getReplicated mirror.
	c.stamps.Observe(winner.env.Stamp.TimestampNanos)

	if rc != ReadNearest && winner.hadValue {
		c.scheduleReadRepairUnit(key, winner.env, gathered)
	}

	if !winner.hadValue {
		return nil, backend.ErrNotFound
	}
	if len(winner.env.Payload) == 0 {
		return nil, backend.ErrNotFound
	}
	return winner.env.Payload, nil
}

// dispatchReplicaGetUnitAt is the POSITION-ADDRESSED read leg of the routed
// union fan-out (the read mirror of dispatchReplicaPutUnit): the local-self
// branch resolves mountMap[ru] directly (a mid-mount position returns the
// transient errUnitAcquiring the fan-out skips); the remote branch carries the
// explicit ru on the wire (GetAtReplica -> LocalReplicaGetAt) so the receiver
// resolves the exact mounted copy it holds, with no ring-view ownership guard -
// a displaced current owner during a join answers from its still-mounted
// position, and a fresh key it holds no value for flows back as a plain
// not-found (usable by the read quorum) instead of a loop-guard refusal.
func (c *Cluster) dispatchReplicaGetUnitAt(ctx context.Context, replica ring.Member, ru storageunit.ReplicaUnit, key []byte) ([]byte, error) {
	if replica.ID == c.cfg.NodeID {
		// Read-side mounted-position fallback (docs/SPEC.md "Union reads",
		// guard 1): a ring change can shuffle this member's index within the
		// unit's replica set, leaving the bytes mounted at the OLD position
		// while the routed set addresses the NEW one; serve the read from the
		// mounted copy of the same unit rather than refusing.
		b, _, ok := c.localReadBackendForReplicaUnit(ru)
		if !ok {
			return nil, errUnitAcquiring("Get")
		}
		// b is the fence-self-healing mount (storeMount): a fenced read recodes to
		// the transient acquiring-window error + evicts the stale mount here, so a
		// fenced GET self-heals instead of returning the raw fence forever (#433).
		return b.Get(key)
	}
	cli, err := c.clientFor(replica.Addr)
	if err != nil {
		return nil, err
	}
	v, found, err := cli.GetAtReplica(ctx, ru, key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, backend.ErrNotFound
	}
	return v, nil
}

// scheduleReadRepairUnit is the multi-backend analogue of scheduleReadRepair:
// it pushes the winning envelope to laggers via the unit dispatcher (so the
// repair applies apply-if-newer into the mounted unit). Same lifecycle
// (repairCtx / repairWG) so Close drains in-flight repairs.
func (c *Cluster) scheduleReadRepairUnit(key []byte, winnerEnv Envelope, gathered []collected) {
	if c.repairCtx == nil || c.repairCtx.Err() != nil {
		return
	}
	winnerBytes := Encode(winnerEnv)
	// Map each routed union member to the ReplicaUnit it holds so a repair to a
	// pending owner is position-addressed (same as the primary write path).
	routed, _ := c.routedReplicasWithUnit(key)
	ruByMember := make(map[string]storageunit.ReplicaUnit, len(routed))
	for _, rr := range routed {
		ruByMember[rr.member.ID] = rr.ru
	}
	laggers := make([]ring.Member, 0, len(gathered))
	for _, g := range gathered {
		if !g.hadValue || winnerEnv.Stamp.Greater(g.env.Stamp) {
			laggers = append(laggers, g.member)
		}
	}
	if len(laggers) == 0 {
		return
	}
	for _, m := range laggers {
		ru, ok := ruByMember[m.ID]
		if !ok {
			// A lagger no longer in the routed set (the union shifted between the
			// read and the repair); skip - the next read re-resolves.
			continue
		}
		c.repairWG.Go(func() {
			_ = c.dispatchReplicaPutUnit(c.repairCtx, m, ru, key, winnerBytes)
		})
	}
}
