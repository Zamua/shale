package cluster

import (
	"context"
	"errors"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/ring"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EnvelopeWrite is one already-encoded entry in a CAS write-set fan-out:
// a Key plus its Encode()d Envelope (shared stamp + payload; an empty
// payload is a tombstone). The owner builds these once at commit time;
// every replica writes the envelope bytes verbatim, never re-stamping.
// Exported so the rpc layer can construct a batch from the ApplyBatch
// wire request without pkg/cluster importing pkg/rpc.
type EnvelopeWrite struct {
	Key      []byte
	Envelope []byte
}

// ApplyBatchLocal applies a CAS write-set fan-out to the LOCAL backend in
// ONE transaction, APPLY-IF-NEWER per key: for each (key, envelope) it
// decodes the stored value's stamp and writes the incoming envelope only
// if the incoming stamp strictly beats it (or there is no stored value).
// An older-or-equal entry is skipped, leaving the newer stored value
// intact. The whole batch commits together (rolling back on any error)
// so a mid-handoff key can't leave a partial batch. It is the replica
// side of the CAS write-set replication: the owner has already validated
// + committed these envelopes locally, so the replica trusts the fan-out
// the same way LocalReplicaPut trusts OwnsReplica (no ownership
// re-check), but it does NOT trust the ARRIVAL ORDER of competing
// commits: a reordered older batch self-resolves to a no-op via the
// apply-if-newer check. This is what lets CommitCASApply release
// casCommitMu before fanning out (the fan-out is now order-independent).
//
// Atomicity: the whole get-compare-put sequence runs under c.applyMu (the
// same node-wide lock the single-key replica paths take) AND inside one
// transaction, so two concurrent batches touching the same key cannot
// both read the old stamp and race. The memory backend's tx has
// snapshot-isolation reads but no write-write conflict detection, so the
// lock - not the tx - is what makes this correct.
//
// The migration guard applies per key the same way dispatchReplicaPut
// does: if any key in the batch is migrating out of or being received
// into this node's partition, the whole batch is refused with the
// transient migration-guard error (codes.ResourceExhausted) so the
// owner's fanout classifies it as transient (not a failure) and another
// replica can still satisfy the W target. We check ALL keys before
// opening the tx so a mid-handoff key can't leave a partial batch.
//
// Caller (rpc.Server.ApplyBatch) is responsible for any wire decoding;
// ApplyBatchLocal trusts the bytes it is handed (an incoming envelope
// that fails to Decode is a hard error, not v0.3 compat data).
func (c *Cluster) ApplyBatchLocal(writes []EnvelopeWrite) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	if c.multiReplicated() {
		// R>1 (replicated multi-backend, v0.8 Phase 2b): apply the CAS write-set
		// APPLY-IF-NEWER against the batch's MOUNTED unit, in ONE transaction.
		// The whole batch co-shards (the cross-shard guard guarantees it), so
		// one unit covers it; resolve it from the first key.
		return c.applyBatchToUnit(writes)
	}
	if c.multi {
		// ApplyBatch is the R>1 CAS write-set fan-out protocol. An R=1 multi-
		// backend cluster never sends it (no fan-out), so refuse cleanly rather
		// than dereference the nil c.backend below. A registered RPC must fail
		// closed, never panic.
		return errors.New("cluster: ApplyBatch unsupported in multi-backend mode (single-replica)")
	}
	if rb := c.rebalance.Load(); rb != nil {
		for _, w := range writes {
			if rb.IsMigrating(w.Key) || rb.IsReceiving(w.Key) {
				return migrationGuardError(c.retryAfterMs())
			}
		}
	}

	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	tx, err := c.backend.Begin(backend.SnapshotIsolation)
	if err != nil {
		return err
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
			return aerr
		}
		if !apply {
			continue
		}
		if err := tx.Put(w.Key, w.Envelope); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// txApplyIfNewer reports whether an incoming envelope carrying
// incomingStamp should be written for key, given what tx currently holds:
// true if there is no stored value OR incomingStamp strictly beats the
// stored value's stamp; false otherwise (older-or-equal => no-op). A
// stored value that fails to Decode is treated as a zero-Stamp legacy
// value (any real incoming stamp wins). Callers hold c.applyMu so the
// read here and the subsequent Put are atomic per key.
func txApplyIfNewer(tx backend.Transaction, key []byte, incomingStamp Stamp) (bool, error) {
	stored, gerr := tx.Get(key)
	if gerr != nil {
		if errors.Is(gerr, backend.ErrNotFound) {
			return true, nil
		}
		return false, gerr
	}
	storedEnv, derr := Decode(stored)
	if derr != nil {
		storedEnv = Envelope{}
	}
	return incomingStamp.Greater(storedEnv.Stamp), nil
}

// replicateCASBatch fans the already-encoded write-set envelopes out to
// the shard's R replicas and waits for W total acks, COUNTING the owner's
// already-committed local copy as one ack. So the fan-out only needs W-1
// more replica acks; under WriteOne (W=1) the local commit alone
// satisfies W and no remote ack is required to return nil (the fan-out
// still runs, best-effort, for durability).
//
// The owner is excluded from the dispatched set (it already wrote via the
// local tx). The remaining R-1 replicas are dispatched through ApplyBatch
// (in-process for any other local replica, gRPC otherwise). Returns
// codes.Unavailable when fewer than W acks land (the write is ALREADY
// durable on the owner + any replica that did ack; this is the same
// best-effort-to-W shape putReplicated has). Migration-guard rejections
// from a mid-handoff replica are transient and count toward neither acks
// nor the failure budget.
//
// shardKey is the pin key's shard key (LocateKeyN's hashing input); every
// write in a CAS commit co-shards with the pin key, so one replica set
// covers the whole batch. Each dispatch inherits a WriteTimeout deadline
// so a hung peer cancels at the deadline instead of leaking goroutines,
// the same shape putReplicated uses.
func (c *Cluster) replicateCASBatch(pinKey []byte, writes []EnvelopeWrite) error {
	// Re-key the replica set per UNIT in multi-backend mode (v0.8 Phase 2b):
	// the batch fans out to the pin key's UNIT's R replica nodes (every key in
	// a CAS commit co-shards with the pin key, so one unit covers the batch),
	// not the per-node LocateKeyN over the raw shard key the legacy path uses.
	var allReplicas []ring.Member
	if c.multiReplicated() {
		allReplicas = c.replicasForKey(pinKey)
	} else {
		allReplicas = c.ring.LocateKeyN(c.shardKey(pinKey), c.replicationFactor())
	}
	if len(allReplicas) == 0 {
		return status.Error(codes.Unavailable, "shale: no replicas available for CAS write-set")
	}

	// W is computed over the full replica set (the owner is one of them
	// and its local commit is already one ack).
	w := requiredWriteAcks(c.cfg.WriteConsistency, len(allReplicas))

	// The replicas the fan-out actually dispatches to: everyone except
	// the owner (this node), which already holds the write.
	others := make([]ring.Member, 0, len(allReplicas))
	for _, r := range allReplicas {
		if r.ID == c.cfg.NodeID {
			continue
		}
		others = append(others, r)
	}
	if len(others) == 0 {
		// Owner is the only replica (R clamped to 1 by a small live ring).
		// The local commit is the entire durability; nothing to fan out.
		if w > 1 {
			return status.Errorf(codes.Unavailable,
				"shale: CAS write needed %d acks, only the owner is a replica", w)
		}
		return nil
	}

	// Owner's local commit is the first ack; the fan-out must collect
	// (w - 1) more from the other replicas. remoteNeeded <= 0 means W is
	// already satisfied by the local commit alone (WriteOne): dispatch the
	// fan-out best-effort (durability is still desirable) but do NOT gate
	// the return on it, mirroring putReplicated's surplus-in-background
	// behavior at W=1.
	remoteNeeded := w - 1

	fanoutCtx, cancelFanout := context.WithTimeout(context.Background(), c.cfg.WriteTimeout)
	acks, errs, resultsCh := fanout(fanoutCtx, others, remoteNeeded,
		func(ctx context.Context, replica ring.Member) ([]byte, error) {
			return nil, c.dispatchApplyBatch(ctx, replica, writes)
		})

	// Drain surplus replicas in the background so the WaitGroup finalizes
	// and no goroutine leaks; cancel the fan-out context once every
	// replica has reported so any in-flight gRPC call gets torn down.
	go func() {
		defer cancelFanout()
		//nolint:revive // empty-block: idiomatic channel drain.
		for range resultsCh {
		}
	}()

	if remoteNeeded <= 0 {
		// W satisfied by the owner's local commit; remote acks are bonus
		// durability collected in the background by the drainer above.
		return nil
	}

	// Total acks = owner's local commit (1) + remote acks collected.
	totalAcks := acks + 1
	if totalAcks < w {
		return status.Errorf(codes.Unavailable,
			"shale: CAS write needed %d acks, got %d (%d failures: %v)",
			w, totalAcks, len(errs), firstErr(errs))
	}
	return nil
}

// dispatchApplyBatch routes one replica's batch apply to either the local
// backend (in-process ApplyBatchLocal, with its migration guard) or a
// peer's gRPC ApplyBatch. Returns the raw replica outcome; the migration-
// guard ResourceExhausted is surfaced verbatim so fanout's
// isTransientReplicaErr can classify it transient. Mirrors
// dispatchReplicaPut's local-or-remote shape.
func (c *Cluster) dispatchApplyBatch(ctx context.Context, replica ring.Member, writes []EnvelopeWrite) error {
	if replica.ID == c.cfg.NodeID {
		// The owner already wrote via the CAS local commit; this branch
		// only fires for a NON-owner local replica that happens to be the
		// dispatch target. In practice the owner is excluded from `others`
		// before dispatch, so this is defensive: re-applying the same
		// envelopes is idempotent (verbatim Put of identical bytes).
		return c.ApplyBatchLocal(writes)
	}
	cli, err := c.clientFor(replica.Addr)
	if err != nil {
		return err
	}
	return cli.ApplyBatch(ctx, writes)
}
