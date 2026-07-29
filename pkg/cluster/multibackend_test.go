package cluster

// White-box tests for multi-backend mode (v0.8 Phase 2). These live in
// package cluster (not cluster_test) so they can reach the unexported
// mode wiring: validateBackendMode, the mount table, and the
// per-unit routing. The cross-node forwarding path is covered separately
// by the in-process integration tests in tests/integration.

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/storageunit"
)

// TestValidateBackendMode pins the legacy-vs-multi-backend XOR: exactly
// one mode must be selected, and a multi-backend config is single-replica
// in Phase 2.
func TestValidateBackendMode(t *testing.T) {
	uc := storageunit.MustUnitCount(8)
	fac := memfactory.New()

	cases := []struct {
		name     string
		cfg      Config
		wantErr  bool
		wantMode bool // expected multi value when wantErr is false
	}{
		{
			name:     "legacy backend only",
			cfg:      Config{Backend: memory.New()},
			wantMode: false,
		},
		{
			name:     "multi factory + unitcount",
			cfg:      Config{BackendFactory: fac, UnitCount: uc},
			wantMode: true,
		},
		{
			name:    "both modes set is an error",
			cfg:     Config{Backend: memory.New(), BackendFactory: fac, UnitCount: uc},
			wantErr: true,
		},
		{
			name:    "backend + factory only is an error",
			cfg:     Config{Backend: memory.New(), BackendFactory: fac},
			wantErr: true,
		},
		{
			name:    "neither mode set is an error",
			cfg:     Config{},
			wantErr: true,
		},
		{
			name:    "factory without unitcount is an error",
			cfg:     Config{BackendFactory: fac},
			wantErr: true,
		},
		{
			name:    "unitcount without factory is an error",
			cfg:     Config{UnitCount: uc},
			wantErr: true,
		},
		{
			// R>1 is NOT a capability question any more. shale declares ONE
			// storage port; a factory that compiles against it has stated it
			// meets the contract, so there is nothing left to assert at Open
			// and nothing to refuse. The predecessor of this case pinned the
			// opposite (R>1 rejected unless the factory also implemented a
			// second ReplicaBackendFactory interface), which was exactly the
			// "ask the adapter what it can do" branch the port collapse
			// removed.
			name:     "multi-backend with R>1 is allowed: no capability check",
			cfg:      Config{BackendFactory: fac, UnitCount: uc, ReplicationFactor: 3},
			wantMode: true,
		},
		{
			name:     "multi-backend with R=1 is allowed",
			cfg:      Config{BackendFactory: fac, UnitCount: uc, ReplicationFactor: 1},
			wantMode: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			multi, err := validateBackendMode(&cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (multi=%v)", multi)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if multi != tc.wantMode {
				t.Fatalf("multi = %v, want %v", multi, tc.wantMode)
			}
		})
	}
}

// openSingleNodeMulti opens a single-node multi-backend cluster (no
// membership / ring), which owns the whole unit space locally.
func openSingleNodeMulti(t *testing.T, n int) (*Cluster, *memfactory.Factory) {
	t.Helper()
	fac := memfactory.New()
	c, err := Open(Config{
		NodeID:         "solo",
		BackendFactory: fac,
		UnitCount:      storageunit.MustUnitCount(n),
	})
	if err != nil {
		t.Fatalf("Open multi-backend: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, fac
}

// TestMultiBackendMountsAllUnitsSingleNode verifies a single-node multi-
// backend cluster mounts every unit (it owns them all on an empty ring).
func TestMultiBackendMountsAllUnitsSingleNode(t *testing.T) {
	const n = 8
	c, fac := openSingleNodeMulti(t, n)

	if !c.multi {
		t.Fatal("expected multi-backend mode")
	}
	if c.backend != nil {
		t.Fatal("c.backend must be nil in multi-backend mode")
	}
	open := fac.OpenUnits()
	if len(open) != n {
		t.Fatalf("factory has %d units open, want %d (single node owns all): %v", len(open), n, open)
	}
	if got := c.mounts.mountedCount(); got != n {
		t.Fatalf("the mount table has %d units, want %d", got, n)
	}
}

// TestMultiBackendRoundTripAndPlacement writes keys through the cluster
// surface and asserts each lands in the mounted backend for ITS unit (the
// per-unit routing actually selects the right engine), and reads back.
func TestMultiBackendRoundTripAndPlacement(t *testing.T) {
	const n = 8
	c, _ := openSingleNodeMulti(t, n)

	keys := [][]byte{
		[]byte("alpha"), []byte("bravo"), []byte("charlie"),
		[]byte("delta"), []byte("echo"), []byte("foxtrot"),
	}
	for _, k := range keys {
		if err := c.Put(k, []byte("v-"+string(k))); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	for _, k := range keys {
		// Read back through the cluster surface.
		got, err := c.Get(k)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if want := []byte("v-" + string(k)); !bytes.Equal(got, want) {
			t.Fatalf("Get %s = %q, want %q", k, got, want)
		}
		// And confirm physical placement: the value lives in the backend
		// for THIS key's generation-qualified unit, and nowhere else.
		gu := c.genUnitForKey(k)
		b, _ := c.mounts.backendFor(replica0(gu))
		if v, err := b.Get(k); err != nil || !bytes.Equal(v, []byte("v-"+string(k))) {
			t.Fatalf("key %s not in unit %s's backend: v=%q err=%v", k, gu, v, err)
		}
	}

	// Delete removes from the right unit.
	if err := c.Delete(keys[0]); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Get(keys[0]); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Get after Delete: err = %v, want ErrNotFound", err)
	}
}

// TestMultiBackendScanPrefixSpansUnits checks that the cluster routes a
// prefix scan to the prefix's unit (per-shard scan) AND that the admin
// LocalScanPrefix spans every mounted unit (so keysHeld-style counts see
// the whole node).
func TestMultiBackendScanPrefixSpansUnits(t *testing.T) {
	const n = 8
	c, _ := openSingleNodeMulti(t, n)

	// Co-located keys via a shared hash tag land on one unit, so a
	// ScanPrefix on the tag returns all of them.
	for i := range 5 {
		k := []byte("{grp}-" + string(rune('a'+i)))
		if err := c.Put(k, []byte("x")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// A few keys outside the group, on (likely) other units.
	for i := range 5 {
		if err := c.Put([]byte("other-"+string(rune('a'+i))), []byte("y")); err != nil {
			t.Fatalf("Put other: %v", err)
		}
	}

	// Routed scan on the group prefix returns exactly the 5 grouped keys.
	it, err := c.ScanPrefix([]byte("{grp}"))
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}
	got := 0
	for {
		k, _, err := it.Next()
		if err != nil {
			t.Fatalf("scan next: %v", err)
		}
		if k == nil {
			break
		}
		got++
	}
	_ = it.Close()
	if got != 5 {
		t.Fatalf("routed ScanPrefix returned %d keys, want 5", got)
	}

	// LocalScanPrefix(nil) spans every mounted unit: all 10 keys.
	lit, err := c.LocalScanPrefix(nil)
	if err != nil {
		t.Fatalf("LocalScanPrefix: %v", err)
	}
	total := 0
	for {
		k, _, err := lit.Next()
		if err != nil {
			t.Fatalf("local scan next: %v", err)
		}
		if k == nil {
			break
		}
		total++
	}
	_ = lit.Close()
	if total != 10 {
		t.Fatalf("LocalScanPrefix spanned %d keys, want 10 (all units)", total)
	}
}

// TestMultiBackendTransact exercises the CAS / OCC path in multi-backend
// mode: a Transact closure on a single shard commits against the pin key's
// mounted unit (localBeginForKey), not a single backend.
func TestMultiBackendTransact(t *testing.T) {
	c, _ := openSingleNodeMulti(t, 8)

	pin := []byte("{ctr}")
	// Seed the counter.
	if err := c.Put(pin, []byte("0")); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Increment via Transact a few times; each commit validates + applies
	// on the pin key's unit.
	for i := range 3 {
		err := c.Transact(pin, func(tx backend.Transaction) error {
			v, err := tx.Get(pin)
			if err != nil {
				return err
			}
			next := append([]byte(nil), v...)
			next = append(next, '+')
			return tx.Put(pin, next)
		})
		if err != nil {
			t.Fatalf("Transact %d: %v", i, err)
		}
	}
	got, err := c.Get(pin)
	if err != nil {
		t.Fatalf("Get after Transact: %v", err)
	}
	if want := []byte("0+++"); !bytes.Equal(got, want) {
		t.Fatalf("counter = %q, want %q", got, want)
	}
}

// TestMultiBackendCloseClosesUnits verifies Close releases every mounted
// unit via the factory (CloseUnit each), leaving none open.
func TestMultiBackendCloseClosesUnits(t *testing.T) {
	fac := memfactory.New()
	c, err := Open(Config{
		NodeID:         "solo",
		BackendFactory: fac,
		UnitCount:      storageunit.MustUnitCount(4),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if len(fac.OpenUnits()) != 4 {
		t.Fatalf("expected 4 units open before Close, got %d", len(fac.OpenUnits()))
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if open := fac.OpenUnits(); len(open) != 0 {
		t.Fatalf("expected 0 units open after Close, got %d: %v", len(open), open)
	}
	// Ops after Close are refused.
	if err := c.Put([]byte("k"), []byte("v")); !errors.Is(err, backend.ErrClosed) {
		t.Fatalf("Put after Close: err = %v, want ErrClosed", err)
	}
}

// TestMultiBackendOpenUnitErrorRollsBack feeds a factory that fails to open
// one unit and asserts Open fails cleanly with nothing left mounted (the
// rollback closes whatever it had already opened).
func TestMultiBackendOpenUnitErrorRollsBack(t *testing.T) {
	fac := &failingFactory{Factory: memfactory.New(), failOn: storageunit.UnitID(2)}
	c, err := Open(Config{
		NodeID:         "solo",
		BackendFactory: fac,
		UnitCount:      storageunit.MustUnitCount(8),
	})
	if err == nil {
		_ = c.Close()
		t.Fatal("expected Open to fail when a unit cannot be opened")
	}
	if open := fac.OpenUnits(); len(open) != 0 {
		t.Fatalf("expected rollback to leave 0 units open, got %d: %v", len(open), open)
	}
}

// failingFactory wraps a memfactory.Factory and fails OpenUnit for one
// specific unit id, to exercise the Open rollback path.
type failingFactory struct {
	*memfactory.Factory
	failOn storageunit.UnitID
}

func (f *failingFactory) OpenUnit(m storageunit.MountRef, epoch storageunit.Epoch) (backend.Backend, storageunit.Epoch, error) {
	if m.Unit().ID == f.failOn {
		return nil, 0, errors.New("failingFactory: injected open failure")
	}
	return f.Factory.OpenUnit(m, epoch)
}

// TestMultiBackendTransact_CrossUnitSameNodeRejected pins the multi-backend
// cross-shard guard against a co-location split. A node owns many units, so
// two keys on DIFFERENT units of the SAME node share an owner NODE. The
// transactable boundary is the storage UNIT (one engine), not the node: a
// transaction spanning two units must be REJECTED with ErrCrossShard, not
// silently committed against only the pin key's unit (which would write the
// second key to the wrong unit and lose it on read). A node-based guard
// (comparing owner.ID) admits this split; the unit-based guard rejects it.
// This test fails on the node-based guard and passes on the unit-based one.
func TestMultiBackendTransact_CrossUnitSameNodeRejected(t *testing.T) {
	c, fac := openSingleNodeMulti(t, 8) // single node owns all 8 units

	// Find two no-tag keys that map to DIFFERENT units (same node).
	k1 := []byte("nokey-0")
	var k2 []byte
	for i := 1; i < 10000; i++ {
		cand := fmt.Appendf(nil, "nokey-%d", i)
		if c.genUnitForKey(cand) != c.genUnitForKey(k1) {
			k2 = cand
			break
		}
	}
	if k2 == nil {
		t.Fatal("could not find two keys mapping to different units")
	}

	err := c.Transact(k1, func(tx backend.Transaction) error {
		if err := tx.Put(k1, []byte("v1")); err != nil {
			return err
		}
		return tx.Put(k2, []byte("v2")) // different unit -> cross-shard
	})
	if !errors.Is(err, backend.ErrCrossShard) {
		t.Fatalf("cross-unit Transact: got %v, want ErrCrossShard (a node-based guard would silently admit + split this)", err)
	}

	// The rejected write must not have landed anywhere: k2 absent from its
	// own (generation-qualified) unit's backend.
	gu2 := c.genUnitForKey(k2)
	be2, ok := fac.UnitBackend(gu2)
	if !ok {
		t.Fatalf("unit %s not mounted", gu2)
	}
	if _, err := be2.Get(k2); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("k2 in unit %s after rejected cross-unit txn: got %v, want ErrNotFound", gu2, err)
	}
}
