//go:build slatedb && integration

// Real-MinIO end-to-end coverage for the slatedb-backed
// storageunit.BackendFactory (Backing + Handle). These tests prove the
// load-bearing properties the v0.8 multi-backend model depends on, against
// REAL slatedb instances in a REAL shared MinIO bucket:
//
//   - copy-free fence handoff: a unit handed off A -> B lands B against the
//     SAME bytes (no copy) and FENCES a stale A writer (single writer per
//     unit across a handoff);
//   - independent generations: gen-g unit K and gen-(g+1) unit K are
//     distinct databases that do not fence or see each other;
//   - durability across release/re-acquire: acked writes survive
//     CloseUnit + a fresh OpenUnit (bytes durable in the bucket);
//   - PresentUnits enumeration: the backing reports exactly the units whose
//     databases exist in the bucket.
//
// Gating: both `slatedb` (cgo binding) and `integration` (needs a running
// MinIO). The default `go test ./...` skips both. The fixture points at a
// MinIO reachable at SLATE_MINIO_ENDPOINT (default http://localhost:9000)
// with SLATE_MINIO_ACCESS / SLATE_MINIO_SECRET creds, and creates a FRESH
// bucket per test, removed on cleanup.

package slate_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Zamua/shale/backends/slate"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// factoryFixture holds a freshly-created shared bucket + the connection
// details a slate.BackingConfig needs.
type factoryFixture struct {
	Endpoint  string // "http://host:port"
	Host      string // "host:port" (for the minio client)
	Bucket    string
	AccessKey string
	SecretKey string
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// startFactoryMinIO connects to an already-running MinIO, creates a unique
// bucket, and registers bucket teardown on t.Cleanup. The bucket name is
// unique per test invocation so parallel runs do not collide and a fresh
// bucket gives each test an empty backing.
func startFactoryMinIO(t *testing.T) *factoryFixture {
	t.Helper()

	// The tagged suite mixes fixtures with DIFFERENT per-process env
	// tuples in one test binary (this fixture's fixed SLATE_MINIO_ENDPOINT
	// vs startMinIO's per-test ephemeral testcontainers port). Clear the
	// per-process env-config guard (envguard.go) so this test's config
	// registers afresh instead of conflicting with a previous test's.
	slate.ResetEnvGuardForTest()

	endpoint := env("SLATE_MINIO_ENDPOINT", "http://localhost:9000")
	access := env("SLATE_MINIO_ACCESS", "admin")
	secret := env("SLATE_MINIO_SECRET", "supersecret")

	host := endpoint
	for _, p := range []string{"http://", "https://"} {
		if len(host) >= len(p) && host[:len(p)] == p {
			host = host[len(p):]
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mc, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}

	bucket := fmt.Sprintf("shale-factory-itest-%d", time.Now().UnixNano())
	if err := mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("create bucket %q (is MinIO running at %s?): %v", bucket, endpoint, err)
	}
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanCancel()
		// Remove every object, then the bucket itself.
		objCh := mc.ListObjects(cleanCtx, bucket, minio.ListObjectsOptions{Recursive: true})
		for rErr := range mc.RemoveObjects(cleanCtx, bucket, listToKeys(objCh), minio.RemoveObjectsOptions{}) {
			if rErr.Err != nil {
				t.Logf("remove object %s: %v", rErr.ObjectName, rErr.Err)
			}
		}
		if err := mc.RemoveBucket(cleanCtx, bucket); err != nil {
			t.Logf("remove bucket %q: %v", bucket, err)
		}
	})

	return &factoryFixture{
		Endpoint:  endpoint,
		Host:      host,
		Bucket:    bucket,
		AccessKey: access,
		SecretKey: secret,
	}
}

// listToKeys adapts a ListObjects channel to the ObjectInfo channel
// RemoveObjects wants.
func listToKeys(in <-chan minio.ObjectInfo) <-chan minio.ObjectInfo {
	out := make(chan minio.ObjectInfo)
	go func() {
		defer close(out)
		for o := range in {
			out <- o
		}
	}()
	return out
}

// newBacking builds a Backing against the fixture's shared bucket.
func newBacking(t *testing.T, fx *factoryFixture) *slate.Backing {
	t.Helper()
	b, err := slate.NewBacking(slate.BackingConfig{
		Bucket:    fx.Bucket,
		Endpoint:  fx.Endpoint,
		Region:    "us-east-1",
		AccessKey: fx.AccessKey,
		SecretKey: fx.SecretKey,
		UseSSL:    false,
	})
	if err != nil {
		t.Fatalf("new backing: %v", err)
	}
	return b
}

func gu(gen storageunit.Generation, id storageunit.UnitID) storageunit.GenUnit {
	return storageunit.NewGenUnit(gen, id)
}

// TestFactory_CopyFreeFenceHandoff is the production analogue of the chaos
// harness's lossless-handoff gate, against real slatedb fencing. A unit is
// written + acked on Handle A, released, then acquired on Handle B at a
// higher epoch off the SAME backing/bucket. Asserts B reads every key A
// acked (copy-free), and a stale A writer is FENCED (the two never both
// write).
func TestFactory_CopyFreeFenceHandoff(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBacking(t, fx)

	nodeA := b.Handle()
	nodeB := b.Handle()

	unit := gu(0, 3)

	// A acquires at epoch 1, writes a recorded dataset, acks each.
	beA, err := nodeA.OpenUnit(unit, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("A open: %v", err)
	}
	const n = 200
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k:%04d", i))
		v := []byte(fmt.Sprintf("v-%d", i))
		if err := beA.Put(k, v); err != nil {
			t.Fatalf("A put %d: %v", i, err)
		}
	}

	// A releases (flush + shutdown; bytes stay in the bucket).
	if err := nodeA.CloseUnit(unit); err != nil {
		t.Fatalf("A close: %v", err)
	}

	// Simulate a STALE A that came back not knowing about the impending
	// handoff: open A's database DIRECTLY (a raw slate.New, the legacy
	// single-instance path) at the current manifest epoch, BEFORE B fences.
	// This is the "old owner still holds a live writer" we must fence. We
	// open it here so that B's subsequent higher-epoch open is what locks it
	// out - the natural, single-direction handoff fence (no in-process
	// reopen-after-fence, which real slatedb does not support: a db prefix
	// fenced FORWARD by another in-process writer cannot be safely re-opened
	// in the SAME process - it trips slatedb's local-epoch assertion).
	staleA, err := slate.New(slate.Config{
		Bucket:    fx.Bucket,
		DbName:    backingDbName(unit),
		Endpoint:  fx.Endpoint,
		Region:    "us-east-1",
		AccessKey: fx.AccessKey,
		SecretKey: fx.SecretKey,
		UseSSL:    false,
	})
	if err != nil {
		t.Fatalf("open stale-A direct: %v", err)
	}
	t.Cleanup(func() { _ = staleA.Close() })

	// B acquires at a higher epoch off the SAME backing. The intended floor
	// is 2; the factory fences authoritatively above the durable manifest
	// epoch. Opening B FENCES the staleA writer that opened just above.
	beB, err := nodeB.OpenUnit(unit, storageunit.Epoch(2))
	if err != nil {
		t.Fatalf("B open: %v", err)
	}

	// Copy-free: B sees every key A acked, NO copy happened (same bytes
	// straight from object storage).
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k:%04d", i))
		want := []byte(fmt.Sprintf("v-%d", i))
		got, err := beB.Get(k)
		if err != nil {
			t.Fatalf("B get %d (copy-free read of A's acked write): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("B get %d: got %q want %q", i, got, want)
		}
	}

	// Fence: staleA is now below B's epoch; its writes must fail. slatedb's
	// fence propagates through background tasks, so poll a bounded window.
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = staleA.Put([]byte("intruder"), []byte("should-fail"))
		if lastErr != nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if lastErr == nil {
		t.Fatalf("stale A writer was NOT fenced after B acquired at a higher epoch: two writers could both write the unit")
	}
	if !slate.IsFenced(lastErr) {
		t.Fatalf("stale A write failed but not with a writer-epoch fence (IsFenced=false): %v", lastErr)
	}
	t.Logf("stale A fenced as expected: %v", lastErr)

	if err := nodeB.CloseUnit(unit); err != nil {
		t.Fatalf("B close: %v", err)
	}
}

// TestFactory_IndependentGenerations proves gen-g unit K and gen-(g+1)
// unit K are DISTINCT databases: a write to one is invisible to the other
// and neither fences the other (the doubling-bisect coexistence property).
func TestFactory_IndependentGenerations(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBacking(t, fx)
	h := b.Handle()

	old := gu(0, 1) // gen-0 unit 1
	nu := gu(1, 1)  // gen-1 unit 1 (same UnitID, next generation)

	beOld, err := h.OpenUnit(old, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("open old: %v", err)
	}
	beNew, err := h.OpenUnit(nu, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("open new: %v", err)
	}

	if err := beOld.Put([]byte("key"), []byte("old-value")); err != nil {
		t.Fatalf("put old: %v", err)
	}
	if err := beNew.Put([]byte("key"), []byte("new-value")); err != nil {
		t.Fatalf("put new: %v", err)
	}

	gotOld, err := beOld.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get old: %v", err)
	}
	if !bytes.Equal(gotOld, []byte("old-value")) {
		t.Fatalf("old unit: got %q want old-value (new unit leaked in)", gotOld)
	}
	gotNew, err := beNew.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get new: %v", err)
	}
	if !bytes.Equal(gotNew, []byte("new-value")) {
		t.Fatalf("new unit: got %q want new-value (old unit leaked in)", gotNew)
	}

	// Both still writable: opening one did not fence the other.
	if err := beOld.Put([]byte("after"), []byte("still-live")); err != nil {
		t.Fatalf("old unit fenced by opening the new gen unit: %v", err)
	}

	_ = h.CloseUnit(old)
	_ = h.CloseUnit(nu)
}

// TestFactory_DurabilityAcrossReacquire asserts acked writes survive
// CloseUnit + a fresh OpenUnit on the same handle (bytes durable in the
// bucket, AwaitDurable=true).
func TestFactory_DurabilityAcrossReacquire(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBacking(t, fx)
	h := b.Handle()
	unit := gu(0, 7)

	be, err := h.OpenUnit(unit, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := be.Put([]byte("durable"), []byte("yes")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := h.CloseUnit(unit); err != nil {
		t.Fatalf("close: %v", err)
	}

	be2, err := h.OpenUnit(unit, storageunit.Epoch(2))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := be2.Get([]byte("durable"))
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !bytes.Equal(got, []byte("yes")) {
		t.Fatalf("durability: got %q want yes", got)
	}
	_ = h.CloseUnit(unit)
}

// TestFactory_CurrentEpochAndOpenUnits pins the local-view queries:
// CurrentEpoch reports the opened epoch (ok=false when unmounted) and
// OpenUnits returns the mounted set ascending.
func TestFactory_CurrentEpochAndOpenUnits(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBacking(t, fx)
	h := b.Handle()

	if _, ok := h.CurrentEpoch(gu(0, 0)); ok {
		t.Fatalf("CurrentEpoch ok=true for an unmounted unit")
	}

	units := []storageunit.GenUnit{gu(0, 2), gu(0, 0), gu(1, 0)}
	for _, u := range units {
		if _, err := h.OpenUnit(u, storageunit.Epoch(3)); err != nil {
			t.Fatalf("open %s: %v", u, err)
		}
	}

	if e, ok := h.CurrentEpoch(gu(0, 0)); !ok || e < storageunit.Epoch(3) {
		t.Fatalf("CurrentEpoch(g0/u0) = (%d,%v), want (>=3, true)", e, ok)
	}

	got := h.OpenUnits()
	want := []storageunit.GenUnit{gu(0, 0), gu(0, 2), gu(1, 0)}
	if len(got) != len(want) {
		t.Fatalf("OpenUnits len = %d want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("OpenUnits[%d] = %s want %s (not ascending?)", i, got[i], want[i])
		}
	}
	for _, u := range units {
		_ = h.CloseUnit(u)
	}
}

// TestFactory_DoubleOpenRejected pins the one-live-writer-per-handle
// guard: re-opening a unit this handle ALREADY holds is an error at ANY
// epoch (equal, lower, OR higher). The caller must CloseUnit first. The
// strictly-higher same-node re-open the sharedfactory double permits is NOT
// supported here: closing + reopening the same slatedb db in-process panics
// a slatedb async task ("stored epoch is lower than local epoch"), and the
// SUT never needs it (the reconcile always releases before re-acquiring).
func TestFactory_DoubleOpenRejected(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBacking(t, fx)
	h := b.Handle()
	unit := gu(0, 4)

	if _, err := h.OpenUnit(unit, storageunit.Epoch(5)); err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, err := h.OpenUnit(unit, storageunit.Epoch(5)); err == nil {
		t.Fatalf("re-open at the same epoch should be rejected (double-open), got nil")
	}
	if _, err := h.OpenUnit(unit, storageunit.Epoch(4)); err == nil {
		t.Fatalf("re-open at a lower epoch should be rejected, got nil")
	}
	// A strictly-higher re-open WITHOUT a prior close is ALSO rejected now.
	if _, err := h.OpenUnit(unit, storageunit.Epoch(6)); err == nil {
		t.Fatalf("re-open at a higher epoch (without CloseUnit) should be rejected, got nil")
	}
	// After CloseUnit the held-state is cleared, so the held-check no longer
	// rejects: a fresh ON-A-DIFFERENT-PROCESS open (the deployable handoff)
	// would succeed. We do NOT re-open the same prefix in THIS process here -
	// real slatedb does not support re-opening a db prefix in the same
	// process after it has been opened (process-local epoch state), so the
	// re-open-after-close path is exercised cross-process by the chaosreal
	// adapter, not here. Closing simply must succeed and clear the slot.
	if err := h.CloseUnit(unit); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, ok := h.CurrentEpoch(unit); ok {
		t.Fatalf("CurrentEpoch ok=true after CloseUnit; the slot was not cleared")
	}
}

// TestFactory_FenceEpochArithmetic pins the FACTORY's own epoch arithmetic
// (fenceEpoch's max(intended, durableEpoch+1)) directly, so a regression in
// the factory's math FAILS a test rather than hiding behind slatedb's own
// monotonic manifest. The plain fence-handoff test always passes a generous
// intended floor (already above the durable epoch), so it would still pass
// even if fenceEpoch were neutered to return `intended` verbatim - slatedb
// would advance the manifest regardless. This test passes an INTENTIONALLY
// STALE intended floor (at or below the durable epoch) and asserts, via
// Backing.DurableEpoch before + after, that the open landed STRICTLY above
// the durable epoch: only correct factory arithmetic can satisfy that, since
// a neutered fenceEpoch would try to open at the stale floor.
func TestFactory_FenceEpochArithmetic(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBacking(t, fx)
	unit := gu(0, 8)

	// Seed the unit so its durable manifest exists and has advanced an epoch
	// or two: open + write + close a couple of times on a first handle.
	h1 := b.Handle()
	for i := 0; i < 2; i++ {
		be, err := h1.OpenUnit(unit, storageunit.Epoch(1))
		if err != nil {
			t.Fatalf("seed open %d: %v", i, err)
		}
		if err := be.Put([]byte("seed"), []byte("x")); err != nil {
			t.Fatalf("seed put %d: %v", i, err)
		}
		if err := h1.CloseUnit(unit); err != nil {
			t.Fatalf("seed close %d: %v", i, err)
		}
	}

	durableBefore, err := b.DurableEpoch(unit)
	if err != nil {
		t.Fatalf("durable before: %v", err)
	}
	if durableBefore == 0 {
		t.Fatalf("expected a non-zero durable epoch after seeding, got 0")
	}

	// Acquire on a FRESH handle (cold acquire: it never held the unit, so it
	// cannot consult a local epoch) with an INTENTIONALLY STALE intended
	// floor: epoch 1, which is <= durableBefore. A correct factory ignores
	// this stale floor and opens at durableBefore+1; a neutered fenceEpoch
	// that returned `intended` would try to open at 1 and the durable epoch
	// would NOT advance past durableBefore.
	stale := storageunit.Epoch(1)
	if stale > durableBefore {
		t.Fatalf("test bug: intended floor %d is not stale relative to durable %d", stale, durableBefore)
	}
	h2 := b.Handle()
	be, err := h2.OpenUnit(unit, stale)
	if err != nil {
		t.Fatalf("cold acquire with stale floor: %v", err)
	}
	// A write forces the open's higher writer-epoch to be durably committed
	// to the manifest, so DurableEpoch observes the advance.
	if err := be.Put([]byte("after"), []byte("y")); err != nil {
		t.Fatalf("post-acquire put: %v", err)
	}

	durableAfter, err := b.DurableEpoch(unit)
	if err != nil {
		t.Fatalf("durable after: %v", err)
	}
	if durableAfter <= durableBefore {
		t.Fatalf("factory under-fenced: durable epoch %d did not advance above %d (intended floor was a stale %d). A neutered fenceEpoch would open at the stale floor instead of max(intended, durable+1).", durableAfter, durableBefore, stale)
	}
	// And the opened epoch the handle recorded is strictly above durableBefore.
	if e, ok := h2.CurrentEpoch(unit); !ok || e <= durableBefore {
		t.Fatalf("CurrentEpoch = (%d, %v), want > durableBefore=%d", e, ok, durableBefore)
	}
	_ = h2.CloseUnit(unit)
}

// TestFactory_PresentUnits enumerates units present in the bucket after
// opening a few across generations.
func TestFactory_PresentUnits(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBacking(t, fx)
	h := b.Handle()

	want := []storageunit.GenUnit{gu(0, 0), gu(0, 5), gu(1, 2)}
	for _, u := range want {
		be, err := h.OpenUnit(u, storageunit.Epoch(1))
		if err != nil {
			t.Fatalf("open %s: %v", u, err)
		}
		// Write so the db materializes objects in the bucket.
		if err := be.Put([]byte("seed"), []byte("x")); err != nil {
			t.Fatalf("seed put %s: %v", u, err)
		}
		if err := h.CloseUnit(u); err != nil {
			t.Fatalf("close %s: %v", u, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, err := b.PresentUnits(ctx)
	if err != nil {
		t.Fatalf("PresentUnits: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("PresentUnits = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("PresentUnits[%d] = %s want %s", i, got[i], want[i])
		}
	}
}

// TestFactory_OpenReplicaUnit_IndependentPositions is the R>1 analogue of
// TestFactory_IndependentGenerations, against the ReplicaBackendFactory
// surface. It opens replica 0 and replica 1 of the SAME unit, writes a
// DISTINCT value to each, and asserts:
//
//   - the two positions do NOT alias: replica 0's bytes are never visible via
//     replica 1 (and vice versa) - each is a complete independent database at
//     its own disjoint prefix (dbNameReplica), which is the replica-
//     independence durability guarantee;
//   - opening one position does NOT fence the other (both stay writable after
//     both are open - per-position epoch fencing);
//   - each position's acked write survives an independent CloseReplicaUnit +
//     reopen (AwaitDurable=true, bytes durable at the position's own prefix).
//
// committed-but-RUN-on-the-cluster: needs `slatedb` (cgo) + `integration`
// (real MinIO); the box constraint forbids the slatedb Rust compile here, so
// this runs on the staging cluster / CI, not in the authoring workflow.
func TestFactory_OpenReplicaUnit_IndependentPositions(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBacking(t, fx)
	h := b.Handle()

	unit := gu(1, 5)
	r0 := storageunit.NewReplicaUnit(unit, 0)
	r1 := storageunit.NewReplicaUnit(unit, 1)

	be0, _, err := h.OpenReplicaUnit(r0, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("open replica 0: %v", err)
	}
	be1, _, err := h.OpenReplicaUnit(r1, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("open replica 1: %v", err)
	}

	if err := be0.Put([]byte("key"), []byte("r0-value")); err != nil {
		t.Fatalf("put r0: %v", err)
	}
	if err := be1.Put([]byte("key"), []byte("r1-value")); err != nil {
		t.Fatalf("put r1: %v", err)
	}

	// Each position holds ONLY its own value: no aliasing.
	got0, err := be0.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get r0: %v", err)
	}
	if !bytes.Equal(got0, []byte("r0-value")) {
		t.Fatalf("replica 0: got %q want r0-value (replica 1 leaked across the position boundary)", got0)
	}
	got1, err := be1.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get r1: %v", err)
	}
	if !bytes.Equal(got1, []byte("r1-value")) {
		t.Fatalf("replica 1: got %q want r1-value (replica 0 leaked across the position boundary)", got1)
	}

	// A key written ONLY to r0 is absent from r1 (and vice versa): the
	// positions share no key-space at all.
	if err := be0.Put([]byte("only-r0"), []byte("x")); err != nil {
		t.Fatalf("put only-r0: %v", err)
	}
	if _, err := be1.Get([]byte("only-r0")); err == nil {
		t.Fatalf("replica 1 saw key 'only-r0' written only to replica 0: positions are aliasing")
	}

	// Opening r1 did not fence r0: r0 stays writable.
	if err := be0.Put([]byte("after"), []byte("still-live")); err != nil {
		t.Fatalf("replica 0 fenced by opening replica 1: %v", err)
	}

	// Independent close + reopen: each position's writes are durable at its
	// own prefix and survive a release/re-acquire that does not touch the
	// other position.
	if err := h.CloseReplicaUnit(r0); err != nil {
		t.Fatalf("close r0: %v", err)
	}
	if err := h.CloseReplicaUnit(r1); err != nil {
		t.Fatalf("close r1: %v", err)
	}

	re0, _, err := h.OpenReplicaUnit(r0, storageunit.Epoch(2))
	if err != nil {
		t.Fatalf("reopen r0: %v", err)
	}
	re1, _, err := h.OpenReplicaUnit(r1, storageunit.Epoch(2))
	if err != nil {
		t.Fatalf("reopen r1: %v", err)
	}
	rg0, err := re0.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get r0 after reopen: %v", err)
	}
	if !bytes.Equal(rg0, []byte("r0-value")) {
		t.Fatalf("replica 0 durability: got %q want r0-value", rg0)
	}
	rg1, err := re1.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get r1 after reopen: %v", err)
	}
	if !bytes.Equal(rg1, []byte("r1-value")) {
		t.Fatalf("replica 1 durability: got %q want r1-value", rg1)
	}

	_ = h.CloseReplicaUnit(r0)
	_ = h.CloseReplicaUnit(r1)
}

// TestFactory_ReplicaDoubleOpenRejected pins the one-live-writer-per-position
// guard: re-opening a replica position this handle ALREADY holds is rejected
// at ANY epoch, mirroring TestFactory_DoubleOpenRejected for the R>1 path.
// CloseReplicaUnit must clear the slot first.
func TestFactory_ReplicaDoubleOpenRejected(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBacking(t, fx)
	h := b.Handle()
	r0 := storageunit.NewReplicaUnit(gu(0, 4), 0)

	if _, _, err := h.OpenReplicaUnit(r0, storageunit.Epoch(5)); err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, _, err := h.OpenReplicaUnit(r0, storageunit.Epoch(5)); err == nil {
		t.Fatalf("re-open at the same epoch should be rejected (double-open), got nil")
	}
	if _, _, err := h.OpenReplicaUnit(r0, storageunit.Epoch(6)); err == nil {
		t.Fatalf("re-open at a higher epoch (without CloseReplicaUnit) should be rejected, got nil")
	}
	// Opening a DIFFERENT position of the same unit is NOT a double-open: the
	// positions are independent.
	r1 := storageunit.NewReplicaUnit(gu(0, 4), 1)
	if _, _, err := h.OpenReplicaUnit(r1, storageunit.Epoch(5)); err != nil {
		t.Fatalf("opening a different position should succeed: %v", err)
	}
	// CloseReplicaUnit clears the slot; closing a position not held is a no-op.
	if err := h.CloseReplicaUnit(r0); err != nil {
		t.Fatalf("close r0: %v", err)
	}
	if err := h.CloseReplicaUnit(r0); err != nil {
		t.Fatalf("idempotent close of an unheld position should be nil: %v", err)
	}
	_ = h.CloseReplicaUnit(r1)
}

// backingDbName mirrors the factory's GenUnit -> DbName mapping for the
// direct-open fence test (which needs the raw db name to reopen a stale
// writer against the same database the factory used). Kept in sync with
// BackingConfig.dbName (no KeyPrefix in these tests).
func backingDbName(u storageunit.GenUnit) string {
	return fmt.Sprintf("u/g%d/u%d", u.Gen, u.ID)
}

// guard: keep backend imported even if a future edit drops its only use.
var _ = backend.ErrNotFound
