//go:build slatedb && integration

// factory_relaxed_durability_slatedb_test.go: pins the relaxed-durability
// wiring on the multi-backend R>=2 replica path (BackingConfig.
// RelaxedReplicaDurability). With relaxed durability the per-write ack no
// longer blocks on the object-store flush (durability comes from REPLICATION
// plus the background WAL flush), so the correctness property we must guard is
// NOT latency (that is a deployment benchmark) but that a relaxed-acked write
// is (a) immediately readable on the writer and (b) still durable across a
// GRACEFUL release: CloseReplicaUnit forces a flush, so a fresh higher-epoch
// open off the same bucket must read every relaxed-acked key back. That is the
// "graceful perturbation loses nothing" invariant for relaxed mode.
//
// Runs under the slatedb + integration build tags against a real MinIO (same
// fixture as factory_minio_integration_test.go). Operator entry point:
// make test-slate-minio.
package slate_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Zamua/shale/backends/slate"
	"github.com/Zamua/shale/pkg/storageunit"
)

// newBackingRelaxed mirrors newBacking but flips RelaxedReplicaDurability so the
// R>=2 replica path opens its units with WriteOptions{AwaitDurable:false}.
func newBackingRelaxed(t *testing.T, fx *factoryFixture) *slate.Backing {
	t.Helper()
	b, err := slate.NewBacking(slate.BackingConfig{
		Bucket:                   fx.Bucket,
		Endpoint:                 fx.Endpoint,
		Region:                   "us-east-1",
		AccessKey:                fx.AccessKey,
		SecretKey:                fx.SecretKey,
		UseSSL:                   false,
		RelaxedReplicaDurability: true,
	})
	if err != nil {
		t.Fatalf("new relaxed backing: %v", err)
	}
	return b
}

// TestFactory_RelaxedDurability_ReadYourWritesAndSurvivesGracefulClose pins the
// two correctness guarantees of relaxed mode on the replica path: a relaxed
// write is read-your-writes visible immediately, and it survives the graceful
// CloseReplicaUnit flush (a fresh higher-epoch open reads it back). If the
// relaxed flag ever silently stopped flushing on close, the reopened replica
// would miss the un-flushed tail and this fails.
func TestFactory_RelaxedDurability_ReadYourWritesAndSurvivesGracefulClose(t *testing.T) {
	fx := startFactoryMinIO(t)
	b := newBackingRelaxed(t, fx)

	owner := b.Handle()
	p := ruSM(7, 1) // unit 7, replica position 1 (an R>=2 replica unit)

	// Acquire the replica at epoch 1 and write a recorded dataset. Each Put
	// acks at memtable insert (relaxed) rather than after the object-store
	// flush; correctness is unchanged.
	be, _, err := owner.OpenReplicaUnit(p, storageunit.Epoch(1))
	if err != nil {
		t.Fatalf("OpenReplicaUnit: %v", err)
	}
	const n = 200
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k:%04d", i))
		v := []byte(fmt.Sprintf("v-%d", i))
		if err := be.Put(k, v); err != nil {
			t.Fatalf("relaxed put %d: %v", i, err)
		}
	}

	// Read-your-writes: every relaxed-acked key is visible on the writer.
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k:%04d", i))
		want := []byte(fmt.Sprintf("v-%d", i))
		got, err := be.Get(k)
		if err != nil {
			t.Fatalf("relaxed read-your-writes get %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("relaxed get %d: got %q want %q", i, got, want)
		}
	}

	// Graceful release: CloseReplicaUnit forces a flush, so the relaxed tail
	// becomes durable in the bucket before the unit lets go. This is the
	// graceful-perturbation path (scale-down, reshard cut-over, rolling
	// restart) - it must lose nothing.
	if err := owner.CloseReplicaUnit(p); err != nil {
		t.Fatalf("CloseReplicaUnit (graceful flush): %v", err)
	}

	// Fresh handle, higher epoch, same bucket: the successor must read every
	// relaxed-acked key back. If relaxed mode had skipped the close flush, the
	// un-flushed tail would be gone and these gets would miss.
	successor := b.Handle()
	beNext, _, err := successor.OpenReplicaUnit(p, storageunit.Epoch(2))
	if err != nil {
		t.Fatalf("successor OpenReplicaUnit: %v", err)
	}
	t.Cleanup(func() { _ = successor.CloseReplicaUnit(p) })
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k:%04d", i))
		want := []byte(fmt.Sprintf("v-%d", i))
		got, err := beNext.Get(k)
		if err != nil {
			t.Fatalf("successor get %d (relaxed write durable across graceful close): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("successor get %d: got %q want %q", i, got, want)
		}
	}
}
