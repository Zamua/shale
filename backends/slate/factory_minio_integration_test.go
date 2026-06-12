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

	// B acquires at a higher epoch off the SAME backing. The intended
	// floor is 2; the factory fences authoritatively above the durable
	// manifest epoch.
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

	// Fence: a stale A writer (re-opened at the now-stale epoch, simulating
	// an A that did not observe the handoff) must be locked out. We re-open
	// A's instance directly against the same db at the stale epoch and
	// confirm its write fails once B holds the unit at a higher epoch.
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
		t.Fatalf("reopen stale-A direct: %v", err)
	}
	t.Cleanup(func() { _ = staleA.Close() })

	// Opening staleA bumps the epoch above B; to test the fence in the
	// handoff direction, re-acquire on B at a still-higher epoch so B is
	// the live writer and staleA is the one fenced.
	if _, err := nodeB.OpenUnit(unit, storageunit.Epoch(5)); err != nil {
		t.Fatalf("B re-acquire higher: %v", err)
	}
	// staleA is now below B's epoch; its writes must fail. slatedb's fence
	// propagates through background tasks, so poll a bounded window.
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
		t.Fatalf("stale A writer was NOT fenced after B re-acquired at a higher epoch: two writers could both write the unit")
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
// guard: re-opening a unit this handle already holds at an equal-or-lower
// epoch is an error.
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
	// A strictly-higher re-open is allowed (same node bumping its epoch).
	if _, err := h.OpenUnit(unit, storageunit.Epoch(6)); err != nil {
		t.Fatalf("strictly-higher re-open should be allowed: %v", err)
	}
	_ = h.CloseUnit(unit)
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

// backingDbName mirrors the factory's GenUnit -> DbName mapping for the
// direct-open fence test (which needs the raw db name to reopen a stale
// writer against the same database the factory used). Kept in sync with
// BackingConfig.dbName (no KeyPrefix in these tests).
func backingDbName(u storageunit.GenUnit) string {
	return fmt.Sprintf("u/g%d/u%d", u.Gen, u.ID)
}

// guard: keep backend imported even if a future edit drops its only use.
var _ = backend.ErrNotFound
