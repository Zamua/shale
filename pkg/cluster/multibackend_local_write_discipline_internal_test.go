package cluster

// The evict-on-stale-mount write discipline, pinned across EVERY entry point
// that runs it. Four distinct paths reach a single-copy local write on a
// mounted unit: the two client-facing ones (Cluster.Put, Cluster.Delete) and
// the two forwarded-replica ones (LocalReplicaPut, LocalReplicaDelete). All
// four must treat a failed backend write identically:
//
//	evict the stale mount, then return the RETRYABLE acquiring-window error.
//
// Neither half is optional. Skipping the evict strands a fenced handle that
// fails every subsequent write until the slow self-heal tick notices; skipping
// the retryable refusal (returning nil, or a hard error the originator will not
// retry) ACKS a write that never landed, which is the acked-then-lost failure
// the whole reshard barrier exists to prevent.
//
// This test exists because the discipline was copy-pasted per entry point, so
// nothing structurally stopped one copy from drifting: a guard added to Put and
// missed on LocalReplicaDelete would leave the forwarded delete path silently
// acking a write that did not land, and no per-path test would catch it. The
// four sites now share one implementation (withLocalWriteBackend); this pins
// the CONTRACT so a future change cannot quietly re-diverge them, whether or
// not they still share code.

import (
	"errors"
	"strings"
	"testing"

	"github.com/Zamua/shale/internal/memfactory"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errDiskGone stands in for any backend write failure: a fenced handle whose
// lease moved to a higher epoch, or genuine storage trouble. The discipline
// does not distinguish them today (both mean "this mount did not take the
// write"), which is exactly why it must be identical at all four sites.
var errDiskGone = errors.New("test: backend write failed")

// failWriteBackend is a mounted unit whose writes always fail. Reads and every
// other method delegate to the real memory backend, so the mount is otherwise
// healthy and only the write path is exercised.
type failWriteBackend struct {
	backend.Backend
}

func (failWriteBackend) Put(_, _ []byte) error { return errDiskGone }
func (failWriteBackend) Delete(_ []byte) error { return errDiskGone }

// openSoloMultiCluster opens a single-node multi-backend (R=1) cluster, which
// owns and mounts every unit. All four entry points under test route through
// the same local-write path on this shape.
func openSoloMultiCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	c, err := Open(Config{
		NodeID:         "solo",
		BackendFactory: memfactory.New(),
		UnitCount:      storageunit.MustUnitCount(n),
	})
	if err != nil {
		t.Fatalf("Open solo multi cluster: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// swapInFailingMount resolves the position key routes to and replaces its
// mounted backend with one whose writes fail, returning the position and the
// exact backend pointer installed (eviction is compare-by-pointer).
func swapInFailingMount(t *testing.T, c *Cluster, key []byte) (storageunit.ReplicaUnit, backend.Backend) {
	t.Helper()
	mounted, ru, unlock, ok := c.localWriteBackendForKey(key)
	unlock()
	if !ok {
		t.Fatalf("fixture: solo cluster should mount the unit for key %q", key)
	}
	failing := failWriteBackend{Backend: mounted}
	c.mounts.mountUndecorated(ru, failing)
	return ru, failing
}

func TestLocalWriteDiscipline_EveryEntryPointEvictsStaleMountAndRefuses(t *testing.T) {
	key := []byte("discipline-key")

	// Each case drives one entry point against a mount whose write fails. The
	// op name is what the acquiring-window error carries back to the caller, so
	// it is asserted per site: it is the per-site detail the shared helper is
	// parameterised on, and the one thing that legitimately differs.
	cases := []struct {
		name string
		op   string
		call func(c *Cluster) error
	}{
		{"Cluster.Put", "Put", func(c *Cluster) error {
			return c.Put(key, []byte("v"))
		}},
		{"Cluster.Delete", "Delete", func(c *Cluster) error {
			return c.Delete(key)
		}},
		{"LocalReplicaPut", "Put", func(c *Cluster) error {
			return c.LocalReplicaPut(key, []byte("v"))
		}},
		{"LocalReplicaDelete", "Delete", func(c *Cluster) error {
			return c.LocalReplicaDelete(key)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := openSoloMultiCluster(t, 4)
			ru, failing := swapInFailingMount(t, c, key)

			err := tc.call(c)

			// 1. NEVER ACK A WRITE THAT DID NOT LAND. A nil return here is the
			// acked-then-lost bug: the caller believes the write is durable.
			if err == nil {
				t.Fatalf("%s acked a write that failed on the backend; it must refuse so the originator retries (never ack a write that did not land)", tc.name)
			}
			// 2. The refusal must be the RETRYABLE acquiring-window error, not
			// the raw backend error. A raw error is a HARD failure the
			// originator will not retry, so the write is dropped rather than
			// re-sent onto the freshly re-acquired mount.
			if errors.Is(err, errDiskGone) {
				t.Fatalf("%s surfaced the raw backend error %v; it must recode to the retryable acquiring-window error so the write is retried, not hard-failed", tc.name, err)
			}
			if !isAcquiringErr(err) {
				t.Fatalf("%s returned %v; want the acquiring-window sentinel", tc.name, err)
			}
			if got := status.Code(err); got != codes.Unavailable {
				t.Fatalf("%s returned code %v, want %v", tc.name, got, codes.Unavailable)
			}
			if got := status.Convert(err).Message(); !strings.Contains(got, tc.op) {
				t.Fatalf("%s error message %q should name the op %q", tc.name, got, tc.op)
			}

			// 3. The stale mount must be EVICTED so the next reconcile
			// re-acquires it fresh. Asserted by pointer: a reconcile may have
			// already swapped a healthy backend in, which is a pass; what must
			// never remain is the failed handle itself.
			cur, stillMounted := c.mounts.backendFor(ru)
			if stillMounted && cur == failing {
				t.Fatalf("%s left the failed mount for %v in the mount map; it must be evicted so the next reconcile re-acquires at the durable-max epoch", tc.name, ru)
			}
		})
	}
}
