package cluster

// castx.go holds the CLUSTER-LEVEL OCC TRANSACTION: clusterTx (the
// single-shard-pinned read-set / write-set buffer that Transact hands to the
// caller's closure), the commitCAS dispatch that ships the validated buffer to
// the pinned shard owner, and casResultToError, the mapping from a
// backend.CASResult into the error the caller sees. It is the cluster-side half
// of cas.go (which owns the OWNER-side validate-and-apply). See doc.go.

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Zamua/shale/pkg/backend"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
)

// txRoutedGet performs the normal single-key routed Get the CAS read-set
// records against. It reuses Cluster.Get so a read inside a transaction
// sees exactly what a standalone Get would (same local/remote routing,
// same replication read path). Returns backend.ErrNotFound on absence.
func (c *Cluster) txRoutedGet(key []byte) ([]byte, error) {
	return c.Get(key)
}

// commitCAS dispatches a CAS commit to the pinned shard owner. When the
// owner is this node it is an in-process fast-path (CommitCASApply, no
// RPC); otherwise it serializes the read-set + write-set onto the wire
// and calls the peer's CommitCAS RPC. Either way a reported conflict maps
// to backend.ErrCASConflict; a backend / ownership failure (including the
// owner reporting it no longer owns the pin key) surfaces as that error.
func (c *Cluster) commitCAS(pinKey []byte, level backend.IsolationLevel, reads []backend.ReadCheck, writes []backend.WriteOp) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	// Re-resolve the DESIGNATED owner from the LIVE view in case it moved
	// between pin and commit. casDesignatedOwner is ownerOf in steady state; only
	// during a join transition whose full-ring head is a warming (Joining)
	// newcomer does it re-designate to the displaced still-mounted current head,
	// so the commit lands on a node that can actually serve the owner-local
	// transaction (see docs/SPEC.md "CAS during a transition"). When the
	// designated owner is us, take the in-process fast-path (CommitCASApply, no
	// RPC); otherwise serialize onto the wire and call the peer's CommitCAS RPC.
	curOwner, isLocal := c.casDesignatedOwner(pinKey)
	if isLocal {
		res := c.CommitCASApply(context.Background(), level, pinKey, reads, writes)
		if res.Committed && res.UnderReplicated {
			// Committed durably on this (owner) node but the fan-out missed W.
			// Log the degraded replication ONCE (res.Err is the under-W detail)
			// before casResultToError maps Committed==true to nil - a success, so
			// Transact does NOT re-run fn.
			detail := "replica fan-out missed W"
			if res.Err != nil {
				detail = res.Err.Error()
			}
			c.warnUnderReplicated(pinKey, detail)
		}
		return casResultToError(res)
	}
	cli, err := c.clientFor(curOwner.Addr)
	if err != nil {
		return err
	}
	req := &pb.CommitCASRequest{
		PinKey:         pinKey,
		IsolationLevel: int32(level),
		Reads:          make([]*pb.ReadCheck, len(reads)),
		Writes:         make([]*pb.WriteOp, len(writes)),
	}
	for i, r := range reads {
		req.Reads[i] = &pb.ReadCheck{Key: r.Key, ExpectedValue: r.ExpectedVal, ExpectAbsent: r.ExpectAbsent}
	}
	for i, w := range writes {
		req.Writes[i] = &pb.WriteOp{Key: w.Key, Value: w.Value, Delete: w.Del}
	}
	// Bound the forwarded commit so an unresponsive owner times out rather
	// than wedging the caller (same rationale as cluster.go's forwarded RPCs).
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.WriteTimeout)
	defer cancel()
	resp, err := cli.CommitCAS(ctx, req)
	if err != nil {
		return err
	}
	if resp.GetConflict() {
		return backend.ErrCASConflict
	}
	// Committed==true wins over any error string, mirroring casResultToError's
	// ordering: the remote owner's owner-local commit durably applied, so the
	// commit SUCCEEDED even when the fan-out missed W (under_replicated). The
	// forwarding node must return nil so Transact does NOT re-run fn and re-
	// commit an already-durable write. The owner already logged the under-W
	// detail; note the degraded replication on the forwarding node too.
	if resp.GetCommitted() {
		if resp.GetUnderReplicated() {
			c.warnUnderReplicated(pinKey, "owner reported W shortfall on the replica fan-out")
		}
		return nil
	}
	if e := resp.GetError(); e != "" {
		return errors.New("cluster: CommitCAS owner error: " + e)
	}
	return nil
}

// warnUnderReplicated writes a single WARN to the configured log sink when a
// CAS commit committed durably on the owner but replicated to fewer than W
// replicas (outcome (c), docs/SPEC.md "The four commit outcomes"). Best-effort
// observability, gated on cfg.LogOutput (nil sink => silent; the test default
// is io.Discard). Rate-reasonable by construction: a committed-under-
// replication result is now a SUCCESS and is NOT retried, so this fires at most
// once per under-replicated commit, never once per re-run. detail is the under-
// W fan-out error (owner path) or a short owner-reported note (forwarded path).
func (c *Cluster) warnUnderReplicated(pinKey []byte, detail string) {
	if c.cfg.LogOutput == nil {
		return
	}
	if c.multi {
		_, _ = fmt.Fprintf(c.cfg.LogOutput,
			"WARN cluster: CAS commit under-replicated (durable on owner, missed W): key=%x unit=%s: %s\n",
			pinKey, c.genUnitForKey(pinKey), detail)
		return
	}
	_, _ = fmt.Fprintf(c.cfg.LogOutput,
		"WARN cluster: CAS commit under-replicated (durable on owner, missed W): key=%x: %s\n",
		pinKey, detail)
}

// casResultToError maps a backend.CASResult into the error the cluster
// surface (clusterTx.Commit -> Transact) sees. It is the tx-commit-to-error
// conversion the retry loop keys off, so the ordering of the checks is
// load-bearing:
//
//   - Committed==true FIRST: the owner-local commit durably applied, so the
//     transaction SUCCEEDED even when the replica fan-out missed W
//     (UnderReplicated, with Err carrying the under-W detail for logging).
//     Returns nil so Transact does NOT re-run fn - re-running would re-commit
//     an already-durable write (amplification) and could make a retried
//     insert observe its own committed write as a false conflict. The caller
//     (commitCAS) logs the degraded replication once before this maps to nil.
//   - Conflict: the OCC retry signal, backend.ErrCASConflict.
//   - Err (with Committed==false): a genuine not-committed failure - a
//     backend / ownership error, or a pre-commit retryable cut-over / fence /
//     reshard-cutover refusal that applied nothing and IS re-run by Transact.
//
// See docs/SPEC.md "The four commit outcomes".
func casResultToError(r backend.CASResult) error {
	if r.Committed {
		// Success, possibly under-replicated. r.Err (if any) is the under-W
		// detail, kept for the caller's WARN, and is deliberately NOT returned.
		return nil
	}
	if r.Conflict {
		return backend.ErrCASConflict
	}
	if r.Err != nil {
		return r.Err
	}
	return nil
}

// clusterTx is the CAS-buffered transaction returned by Cluster.Begin
// (and driven by Cluster.Transact). It is a BUFFER, not a live backend
// session: it never holds an open backend.Transaction across a network
// round-trip. See docs/SPEC.md "Single-shard transactions (CAS / OCC)".
//
// Lazy pinning: the shard is pinned on the first key touched (the first
// Get / Put / Delete), to whichever node the ring says owns that key's
// shard. Every subsequent key MUST shard to that same owner; a key that
// shards elsewhere returns backend.ErrCrossShard at the offending
// operation (the cross-shard guard, the load-bearing correctness
// property). This holds whether the pinned shard is local or remote: a
// genuinely cross-shard transaction STILL fails with ErrCrossShard.
//
// Buffering:
//   - Get does a real routed Get (the normal single-key read path,
//     local-or-remote) and records (key, value-seen) in the read-set; a
//     not-found records an expect_absent entry. A Get of a key the tx
//     itself already wrote is served from the write buffer (read-your-
//     writes) with no round-trip and adds NO read-check.
//   - Put / Delete buffer into the write-set; they do not hit the owner.
//   - Commit assembles the read-set + write-set and sends ONE CommitCAS
//     to the pinned owner (an in-process fast-path when the owner is this
//     node, no RPC). A reported conflict maps to backend.ErrCASConflict.
//   - Rollback / abandoning the tx is purely local: nothing was sent to
//     the owner, so there is nothing to undo.
type clusterTx struct {
	c     *Cluster
	level backend.IsolationLevel

	mu      sync.Mutex
	pinned  bool
	pinKey  []byte
	ownerID string // legacy mode: ring owner NODE of the pinned shard; the cross-shard guard compares against this
	// pinUnit is the generation-qualified storage UNIT of the pinned key in
	// multi-backend mode. The guard compares units (not owner nodes) there: a
	// node owns many units, so a single owner-node check would wrongly admit
	// two keys on different units of the same node into one transaction, which
	// commits against only the pin unit (a co-location split / silent loss).
	// Qualified by generation so a key resolved before a reshard cut-over and
	// one resolved after are correctly seen as different units.
	pinUnit storageunit.GenUnit
	done    bool

	// reads is the read-set: keys the tx READ from the cluster (and did
	// not itself write), in first-seen order, deduped by readSeen.
	reads    []backend.ReadCheck
	readSeen map[string]int // key -> index into reads (for dedupe)

	// writeBuf is the buffered write-set in order. writeIdx maps a key to
	// its latest entry so read-your-writes can serve the buffered value
	// and a re-write of the same key overwrites in place rather than
	// appending a duplicate.
	writeBuf []backend.WriteOp
	writeIdx map[string]int
}

// newCASTx constructs an empty CAS-buffered transaction.
func (c *Cluster) newCASTx(level backend.IsolationLevel) *clusterTx {
	return &clusterTx{
		c:        c,
		level:    level,
		readSeen: make(map[string]int),
		writeIdx: make(map[string]int),
	}
}

// pin records the pin key without performing an operation. Transact calls
// it so the shard is fixed to the caller-supplied pinKey even if fn's
// first touched key differs (they must still co-shard, enforced on that
// first op). A no-op if already pinned.
func (t *clusterTx) pin(key []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pinLocked(key)
}

// pinLocked pins the shard on key if not already pinned. Caller holds mu.
// Pinning is a pure ring lookup that cannot fail; the owner Addr is
// resolved fresh from the live ring at Commit time, so the tx records
// only the owner ID (for the cross-shard equality check) and the pin key.
func (t *clusterTx) pinLocked(key []byte) {
	if t.pinned {
		return
	}
	owner, _ := t.c.ownerOf(key)
	t.pinned = true
	t.pinKey = append([]byte(nil), key...)
	t.ownerID = string(owner.ID)
	if t.c.multi {
		t.pinUnit = t.c.genUnitForKey(key)
	}
}

// guardShard pins on first touch + enforces the cross-shard guard on
// every subsequent key. Caller holds mu. Returns backend.ErrCrossShard
// if key shards to a different owner than the pinned one.
func (t *clusterTx) guardShard(key []byte) error {
	if t.done {
		return errors.New("cluster: transaction already finalized")
	}
	if !t.pinned {
		t.pinLocked(key)
		return nil
	}
	// Multi-backend: the transactable boundary is the storage UNIT (one
	// slatedb engine), not the owner node. A node owns many units, so
	// comparing owner nodes would admit a cross-unit transaction that then
	// commits against only the pin unit. Compare units.
	//
	// TODO(reshard-tx): pinUnit is captured at pin time and genUnitForKey here
	// reads the CURRENT genState. If a reshard cut-over for the pin key's old
	// unit commits between the pin and a later same-shard op, the same shard
	// key resolves to a different GenUnit and this returns a SPURIOUS
	// ErrCrossShard, failing a legitimate single-node transaction mid-reshard.
	// Transactions during a reshard are rare (reshard is an explicit op) and
	// this is not data-loss, but the fix is to resolve the pin unit and all
	// guardShard comparisons against the generation captured AT pin time (a
	// stable snapshot for the tx lifetime) so a mid-tx cut-over does not flip
	// the comparison. The reshard cut-over should also be made tx-aware.
	if t.c.multi {
		if t.c.genUnitForKey(key) != t.pinUnit {
			return backend.ErrCrossShard
		}
		return nil
	}
	owner, _ := t.c.ownerOf(key)
	if string(owner.ID) != t.ownerID {
		return backend.ErrCrossShard
	}
	return nil
}

func (t *clusterTx) Get(key []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.guardShard(key); err != nil {
		return nil, err
	}
	// Read-your-writes: a key the tx already wrote is served from the
	// buffer and adds no read-check (validating it against pre-write
	// state would be wrong: the tx itself changed it).
	if idx, ok := t.writeIdx[string(key)]; ok {
		w := t.writeBuf[idx]
		if w.Del {
			return nil, backend.ErrNotFound
		}
		return append([]byte(nil), w.Value...), nil
	}
	// Routed Get against the cluster. Released the lock would let a
	// concurrent op race the read-set; the routed Get is a single network
	// op and clusterTx is not advertised as goroutine-safe across
	// concurrent ops on the SAME tx, so holding mu here is fine.
	val, err := t.c.txRoutedGet(key)
	if err != nil && !errors.Is(err, backend.ErrNotFound) {
		return nil, err
	}
	absent := errors.Is(err, backend.ErrNotFound)
	t.recordRead(key, val, absent)
	if absent {
		return nil, backend.ErrNotFound
	}
	return val, nil
}

// recordRead adds (or refreshes) a read-check for key. Caller holds mu.
// The FIRST observation of a key is the snapshot the OCC validate checks
// against; a later Get of the same key (without an intervening write)
// keeps the first-seen expectation rather than overwriting it, so the
// read-set stays a faithful record of what the client computed against.
func (t *clusterTx) recordRead(key, val []byte, absent bool) {
	if _, ok := t.readSeen[string(key)]; ok {
		return
	}
	rc := backend.ReadCheck{Key: append([]byte(nil), key...), ExpectAbsent: absent}
	if !absent {
		rc.ExpectedVal = append([]byte(nil), val...)
	}
	t.readSeen[string(key)] = len(t.reads)
	t.reads = append(t.reads, rc)
}

func (t *clusterTx) Put(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.guardShard(key); err != nil {
		return err
	}
	t.bufferWrite(backend.WriteOp{Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
	return nil
}

func (t *clusterTx) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.guardShard(key); err != nil {
		return err
	}
	t.bufferWrite(backend.WriteOp{Key: append([]byte(nil), key...), Del: true})
	return nil
}

// bufferWrite appends w to the write-set, or overwrites the prior entry
// for the same key in place (last-write-wins within the tx). Caller holds
// mu. The key is now "owned" by the tx, so subsequent Gets read-your-
// writes from here.
func (t *clusterTx) bufferWrite(w backend.WriteOp) {
	if idx, ok := t.writeIdx[string(w.Key)]; ok {
		t.writeBuf[idx] = w
		return
	}
	t.writeIdx[string(w.Key)] = len(t.writeBuf)
	t.writeBuf = append(t.writeBuf, w)
}

// ScanPrefix inside a CAS-buffered transaction is not supported in v0.6:
// a scanned range cannot be cheaply turned into a read-set of discrete
// key checks, and validating a range against concurrent inserts needs
// phantom protection the value-based read-set does not provide. Callers
// scan outside the transaction. See docs/SPEC.md "Begin vs Transact".
func (t *clusterTx) ScanPrefix(_ []byte) (backend.Iterator, error) {
	return nil, errors.New("cluster: ScanPrefix is not supported inside a CAS transaction; scan outside the transaction (see docs/SPEC.md)")
}

// Commit assembles the buffered read-set + write-set and ships ONE
// CommitCAS to the pinned shard owner. A nil-buffer tx (nothing touched,
// or a read-only tx with nothing to validate against) commits trivially.
// A reported conflict returns backend.ErrCASConflict; a backend /
// ownership failure returns that error.
func (t *clusterTx) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return errors.New("cluster: transaction already finalized")
	}
	t.done = true

	// Nothing to commit: no shard was pinned (the tx touched no key) or
	// the tx had no writes and no reads. Either way there is no state to
	// validate-and-apply; succeed trivially.
	if !t.pinned || (len(t.writeBuf) == 0 && len(t.reads) == 0) {
		return nil
	}

	return t.c.commitCAS(t.pinKey, t.level, t.reads, t.writeBuf)
}

// Rollback abandons the buffer. Purely local: nothing was sent to the
// owner (Commit is the only thing that ships), so there is nothing to
// undo. Idempotent with Commit via the done flag.
func (t *clusterTx) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done = true
	return nil
}
