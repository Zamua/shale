package cluster

// White-box tests for the mount-time tombstone purge (docs/SPEC.md "Tombstone
// purge"). Properties pinned:
//   - an expired tombstone envelope is purged to a NATIVE backend delete; a
//     younger-than-grace tombstone and a live value survive untouched;
//   - a key that changes between the collect scan and its delete is KEPT (the
//     per-key transaction re-check refuses, the guard and subject sharing one
//     read);
//   - an enabled-but-ineligible configuration (write ack bar below R) REFUSES
//     loudly and purges nothing;
//   - mountServing invokes the purge hook exactly once per serving mount (the
//     unforgettability property, same seam as the marker publish).

import (
	"bytes"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// plantEnvelope writes an envelope for key directly into b, stamped at
// ageAgo before now. Empty payload = a shale tombstone.
func plantEnvelope(t *testing.T, b backend.Backend, key string, payload []byte, ageAgo time.Duration) {
	t.Helper()
	stamp := uint64(time.Now().Add(-ageAgo).UnixNano())
	enc := Encode(Envelope{Stamp: Stamp{TimestampNanos: stamp, NodeID: "planter"}, Payload: payload})
	if err := b.Put([]byte(key), enc); err != nil {
		t.Fatalf("plant %s: %v", key, err)
	}
}

// purgeFixture builds an R=2 write-all cluster with one mounted position and
// returns it with that position's backend.
func purgeFixture(t *testing.T, grace time.Duration) (*Cluster, storageunit.ReplicaUnit, backend.Backend) {
	t.Helper()
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2")
	// grace deliberately NOT set on cfg: the mount-time hook must not spawn
	// background passes that race the test's plants; the tests drive
	// runTombstonePurge / purgeOneTombstone with grace explicitly.
	c.cfg.WriteConsistency = WriteQuorum // R=2: quorum == all.
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}
	mounted := c.mounts.mountedList()
	if len(mounted) == 0 {
		t.Fatal("fixture mounted nothing")
	}
	ru := mounted[0]
	b, ok := c.mounts.backendFor(ru)
	if !ok {
		t.Fatalf("no backend for %s", ru)
	}
	return c, ru, b
}

func TestTombstonePurge_ExpiredPurgedYoungAndLiveSurvive(t *testing.T) {
	const grace = time.Hour
	c, ru, b := purgeFixture(t, grace)

	plantEnvelope(t, b, "dead-old", nil, 2*time.Hour)                  // expired tombstone: purge.
	plantEnvelope(t, b, "dead-young", nil, time.Minute)                // young tombstone: keep.
	plantEnvelope(t, b, "alive", []byte("v"), 2*time.Hour)             // old LIVE value: keep.
	if err := b.Put([]byte("raw"), []byte("not-an-env")); err != nil { // undecodable: keep.
		t.Fatalf("plant raw: %v", err)
	}

	c.runTombstonePurge(ru, grace)

	if _, err := b.Get([]byte("dead-old")); err == nil {
		t.Fatal("expired tombstone survived the purge")
	}
	for _, k := range []string{"dead-young", "alive", "raw"} {
		if _, err := b.Get([]byte(k)); err != nil {
			t.Fatalf("%s was purged; only expired tombstones may be: %v", k, err)
		}
	}
}

// The collect->delete window: a key rewritten after the scan selected it must
// be kept. Pinned deterministically by mutating between the two phases.
func TestTombstonePurge_RewrittenKeyIsKeptByTheReCheck(t *testing.T) {
	const grace = time.Hour
	c, ru, b := purgeFixture(t, grace)

	plantEnvelope(t, b, "contested", nil, 2*time.Hour)
	keys, _, err := collectExpiredTombstones(b, grace, uint64(time.Now().UnixNano()), nil)
	if err != nil || len(keys) != 1 {
		t.Fatalf("collect: keys=%d err=%v, want the one expired tombstone", len(keys), err)
	}

	// The key is re-created between the scan and the delete.
	plantEnvelope(t, b, "contested", []byte("recreated"), 0)

	if err := c.purgeOneTombstone(ru, b, keys[0], grace); !errors.Is(err, backend.ErrCASConflict) {
		t.Fatalf("purge of a rewritten key: err = %v, want the CAS-conflict skip", err)
	}
	v, gerr := b.Get([]byte("contested"))
	if gerr != nil {
		t.Fatalf("recreated value was deleted: %v", gerr)
	}
	env, derr := Decode(v)
	if derr != nil || !bytes.Equal(env.Payload, []byte("recreated")) {
		t.Fatalf("recreated value corrupted: %q %v", env.Payload, derr)
	}
}

// An enabled purge with a write ack bar below R must refuse loudly and touch
// nothing - the resurrection precondition expressed as a refusal.
func TestTombstonePurge_RefusesBelowWriteAll(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 3, backing, "n1", "n2", "n3")
	var log strings.Builder
	c.cfg.LogOutput = &log
	c.cfg.TombstoneGracePeriod = time.Hour
	c.cfg.WriteConsistency = WriteQuorum // R=3: quorum 2 < 3.
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}
	ru := c.mounts.mountedList()[0]
	b, _ := c.mounts.backendFor(ru)
	plantEnvelope(t, b, "dead-old", nil, 2*time.Hour)

	c.startTombstonePurge(ru)
	c.loopWG.Wait() // no goroutine should have been spawned; Wait returns immediately.

	if _, err := b.Get([]byte("dead-old")); err != nil {
		t.Fatalf("tombstone purged under an ineligible config: %v", err)
	}
	if !strings.Contains(log.String(), "REFUSED") {
		t.Fatalf("ineligible purge did not refuse loudly; log: %q", log.String())
	}
}

// The purge's guarded-delete window must be covered by the SAME exclusion
// every local envelope writer honors (applyMu + casCommitMu): the memory
// backend's transaction commit performs no conflict detection, so the locks
// ARE the atomicity. Probed from inside the window: TryLock must fail for
// both while the transaction is open.
func TestTombstonePurge_GuardWindowHoldsTheWriterLocks(t *testing.T) {
	const grace = time.Hour
	c, ru, b := purgeFixture(t, grace)
	plantEnvelope(t, b, "probed", nil, 2*time.Hour)

	var applyFree, casFree bool
	probe := &probeBackend{Backend: b, onTxGet: func() {
		if c.applyMu.TryLock() {
			applyFree = true
			c.applyMu.Unlock()
		}
		if c.casCommitMu.TryLock() {
			casFree = true
			c.casCommitMu.Unlock()
		}
	}}
	if err := c.purgeOneTombstone(ru, probe, []byte("probed"), grace); err != nil {
		t.Fatalf("purgeOneTombstone: %v", err)
	}
	if applyFree {
		t.Fatal("applyMu was free inside the guarded-delete window; a racing apply could be clobbered")
	}
	if casFree {
		t.Fatal("casCommitMu was free inside the guarded-delete window; a racing CAS commit could be clobbered")
	}
	if _, err := b.Get([]byte("probed")); err == nil {
		t.Fatal("expired tombstone survived the probed purge")
	}
}

// probeBackend delegates to a real backend but fires onTxGet inside every
// transaction Get - i.e., inside purgeOneTombstone's guarded window.
type probeBackend struct {
	backend.Backend
	onTxGet func()
}

func (p *probeBackend) Begin(level backend.IsolationLevel) (backend.Transaction, error) {
	tx, err := p.Backend.Begin(level)
	if err != nil {
		return nil, err
	}
	return &probeTx{Transaction: tx, onGet: p.onTxGet}, nil
}

type probeTx struct {
	backend.Transaction
	onGet func()
}

func (p *probeTx) Get(key []byte) ([]byte, error) {
	v, err := p.Transaction.Get(key)
	p.onGet()
	return v, err
}

// A scan error aborts the collect fail-closed: no keys are returned for a
// partial read, so nothing downstream can delete on incomplete evidence.
func TestCollectExpiredTombstones_ScanErrorKeepsEverything(t *testing.T) {
	keys, _, err := collectExpiredTombstones(&scanErrBackend{}, time.Hour, uint64(time.Now().UnixNano()), nil)
	if err == nil {
		t.Fatal("collect swallowed the scan error")
	}
	if len(keys) != 0 {
		t.Fatalf("collect returned %d keys from a failed scan; a partial result must not be actionable", len(keys))
	}
}

type scanErrBackend struct{ backend.Backend }

func (s *scanErrBackend) ScanPrefix(_ []byte) (backend.Iterator, error) {
	return &erringIter{}, nil
}

type erringIter struct{ n int }

func (e *erringIter) Next() ([]byte, []byte, error) {
	if e.n == 0 { // one healthy expired tombstone, then the failure
		e.n++
		return []byte("k"), Encode(Envelope{Stamp: Stamp{TimestampNanos: 1}, Payload: nil}), nil
	}
	return nil, nil, errors.New("iterator torn mid-scan")
}
func (e *erringIter) Close() error { return nil }

// mountServing invokes the purge hook once per serving mount, beside the
// marker publish - the same seam, the same unforgettability argument.
func TestMountServing_InvokesThePurgeHook(t *testing.T) {
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "n1", 4, 2, backing, "n1", "n2")
	var calls atomic.Int32
	c.mounts.purge = func(storageunit.ReplicaUnit) { calls.Add(1) }
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mountReplicaUnits: %v", err)
	}
	mounted := len(c.mounts.mountedList())
	if mounted == 0 {
		t.Fatal("fixture mounted nothing")
	}
	if got := int(calls.Load()); got != mounted {
		t.Fatalf("purge hook invoked %d times for %d serving mounts", got, mounted)
	}
}
