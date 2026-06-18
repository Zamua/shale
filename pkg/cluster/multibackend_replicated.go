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
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
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

// replicasForKey resolves a key to its unit's replica set. The unit is
// generation-aware (genUnitForKey), so every key in a co-located set shares
// one unit and therefore one replica set.
func (c *Cluster) replicasForKey(key []byte) []ring.Member {
	return c.unitReplicas(c.genUnitForKey(key))
}

// initReplicatedFactory wires the R>1 capability view of the factory. Called
// from initMultiBackend. At R=1 (or legacy mode) it is a no-op: replicaFactory
// stays nil and the single-mount paths run. validateBackendMode already
// guaranteed the factory implements ReplicaBackendFactory when R>1, so the
// assertion here cannot fail in a validated cluster.
func (c *Cluster) initReplicatedFactory() {
	if !c.multi || c.replicationFactor() <= 1 {
		return
	}
	if rf, ok := c.factory.(storageunit.ReplicaBackendFactory); ok {
		c.replicaFactory = rf
	}
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
// cleanly. Caller (initMultiBackend) has already wired replicaFactory.
func (c *Cluster) mountReplicaUnits() error {
	var mounted, skipped int
	for _, ru := range c.desiredReplicaUnits() {
		b, openedEpoch, err := c.replicaFactory.OpenReplicaUnit(ru, epochAtOpen)
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
			skipped++
			continue
		}
		c.lastAcquireErr.Delete(ru)
		c.myOpenEpoch.Store(ru, openedEpoch) // this node's exact open epoch (drain gate)
		c.mountMu.Lock()
		c.mountMap[ru] = b
		c.mountMu.Unlock()
		mounted++
	}
	if skipped > 0 {
		c.logf("shale: degraded boot complete - %d replica position(s) mounted, %d SKIPPED "+
			"(un-openable backing store); skipped units are read-only until repaired; see /debug/shale/state",
			mounted, skipped)
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
// compare (txApplyIfNewer) runs in one transaction on that unit, under
// c.applyMu, so two concurrent applies on the same key cannot race. ok=false
// from the resolve (owner-but-unmounted, the static-topology window is only
// the Open mount, so in practice always mounted here) yields the retryable
// acquiring-window error the fan-out tolerates.
func (c *Cluster) applyEnvelopeIfNewerToUnit(key, incomingEnvBytes []byte) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	incoming, err := Decode(incomingEnvBytes)
	if err != nil {
		return err
	}

	b, ru, unlock, ok := c.localWriteBackendForKey(key)
	defer unlock()
	if !ok {
		return errUnitAcquiring("Put")
	}

	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	tx, err := b.Begin(backend.SnapshotIsolation)
	if err != nil {
		c.evictStaleMount(ru, b)
		return errUnitAcquiring("Put")
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	apply, aerr := txApplyIfNewer(tx, key, incoming.Stamp)
	if aerr != nil {
		return c.fenceToTransient(ru, b, "Put", aerr)
	}
	if apply {
		if err := tx.Put(key, incomingEnvBytes); err != nil {
			return c.fenceToTransient(ru, b, "Put", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return c.fenceToTransient(ru, b, "Put", err)
	}
	committed = true
	return nil
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
	b, openedEpoch, err := c.replicaFactory.OpenReplicaUnit(ru, epoch)
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
		_ = c.replicaFactory.CloseReplicaUnit(ru)
		b, openedEpoch, err = c.replicaFactory.OpenReplicaUnit(ru, epoch)
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
		_ = c.replicaFactory.CloseReplicaUnit(ru)
		return
	}
	c.mountMap[ru] = b
	c.mountMu.Unlock()

	// Write the durable serving marker AFTER the mount (outside the lock: shared
	// storage I/O), at THIS node's EXACT open epoch (the factory return value, NOT
	// a re-read of the climbing durable). This is the poll-observable release
	// signal a DRAINING predecessor reads (drainCheck); without it a clean-cut
	// successor of a leaving node never releases that node's drain.
	_ = c.replicaFactory.WriteServingMarker(ru, openedEpoch)
}

// releaseReplicaUnit unmounts the ReplicaUnit ru via the per-replica factory.
// The mount-map entry is removed BEFORE the close so a routed op stops resolving
// the local backend immediately. Caller MUST hold reconcileMu.
func (c *Cluster) releaseReplicaUnit(ru storageunit.ReplicaUnit) {
	c.mountMu.Lock()
	delete(c.mountMap, ru)
	c.mountMu.Unlock()
	c.myOpenEpoch.Delete(ru) // a re-acquire records a fresh open epoch.
	_ = c.replicaFactory.CloseReplicaUnit(ru)
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

	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	tx, err := b.Begin(backend.SnapshotIsolation)
	if err != nil {
		c.evictStaleMount(ru, b)
		return errUnitAcquiring("ApplyBatch")
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	for _, w := range writes {
		incoming, derr := Decode(w.Envelope)
		if derr != nil {
			return derr
		}
		apply, aerr := txApplyIfNewer(tx, w.Key, incoming.Stamp)
		if aerr != nil {
			return c.fenceToTransient(ru, b, "ApplyBatch", aerr)
		}
		if !apply {
			continue
		}
		if err := tx.Put(w.Key, w.Envelope); err != nil {
			return c.fenceToTransient(ru, b, "ApplyBatch", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return c.fenceToTransient(ru, b, "ApplyBatch", err)
	}
	committed = true
	return nil
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
		TimestampNanos: uint64(time.Now().UnixNano()),
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
	acks, errs, resultsCh := fanout(ctx, replicas, w,
		func(opCtx context.Context, idx int, replica ring.Member) ([]byte, error) {
			return nil, c.dispatchReplicaPutUnit(opCtx, replica, routed[idx].ru, key, envBytes)
		})

	go func() {
		//nolint:revive // empty-block: idiomatic channel drain.
		for range resultsCh {
		}
	}()

	return classifyWriteAttempt(acks, w, errs)
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
// verbatim except the replica set is the unit's (replicasForKey) and the
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
	allReplicas, stableR := c.routedReplicasForKey(key)
	if len(allReplicas) == 0 {
		return nil, status.Error(codes.Unavailable, "shale: no replicas available for key")
	}
	rc := c.cfg.ReadConsistency
	// N is the stable read quorum, NOT widened by a transient union member; clamp
	// to the routed size so a single-replica routed set is still answerable.
	n := min(requiredReadReplicas(rc, stableR), len(allReplicas))
	queried := allReplicas

	fanoutCtx, cancelFanout := context.WithTimeout(context.Background(), c.cfg.ReadTimeout)
	_, _, resultsCh := fanout(fanoutCtx, queried, n,
		func(ctx context.Context, _ int, replica ring.Member) ([]byte, error) {
			return c.dispatchReplicaGetUnit(ctx, replica, key)
		})

	gathered := make([]collected, 0, len(queried))
	var nonTransientErr error
	usable := 0

	for res := range resultsCh {
		if res.Err != nil {
			if isTransientReplicaErr(res.Err) {
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
		return nil, backend.ErrNotFound
	}

	winner := gathered[0]
	for _, g := range gathered[1:] {
		if g.hadValue && (!winner.hadValue || g.env.Stamp.Greater(winner.env.Stamp)) {
			winner = g
		}
	}

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

// dispatchReplicaGetUnit is the multi-backend analogue of dispatchReplicaGet:
// the local-self branch serves from the key's MOUNTED unit (localBackendForKey)
// instead of c.backend; the remote branch forwards to the replica node, whose
// LocalGet serves from its mounted unit. An owner-but-unmounted replica
// returns the transient acquiring-window error the read fan-out skips.
func (c *Cluster) dispatchReplicaGetUnit(ctx context.Context, replica ring.Member, key []byte) ([]byte, error) {
	if replica.ID == c.cfg.NodeID {
		// v0.8 Phase 2e (pending ranges): serve the union read locally. The
		// resolver finds the CURRENT position (current owner / leaver) or the
		// PENDING position (a mounted pending owner). A pending owner still
		// mid-mount has no mounted position and returns the transient
		// errUnitAcquiring, which the read fan-out skips while another union member
		// (the still-mounted current owner) answers.
		b, ok := c.localBackendForKey(key)
		if !ok {
			return nil, errUnitAcquiring("Get")
		}
		return b.Get(key)
	}
	cli, err := c.clientFor(replica.Addr)
	if err != nil {
		return nil, err
	}
	v, found, err := cli.GetForwarded(ctx, key)
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
