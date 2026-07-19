//go:build slatedb

package integration

// The third Backend implementation in the single-node safety net.
//
// backends/slate lives behind the slatedb build tag because it needs cgo
// plus the native slatedb library at link time, so it cannot join the
// untagged table in singlenode_backends_test.go. The point it makes is the
// same: single-node mode asks nothing of the adapter beyond
// pkg/backend.Backend, so the backend that CAN satisfy the distributed
// fencing contract is accepted here on exactly the same terms as the two
// that cannot.
//
// The object store is "memory:///", so this needs no MinIO.

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Zamua/shale/backends/slate"
	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	slatedb "slatedb.io/slatedb-go/uniffi"
)

func TestSingleNode_SlateBackend_RoundTrip(t *testing.T) {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	if err != nil {
		t.Fatalf("resolve memory object store: %v", err)
	}
	be, err := slate.NewWithStore("singlenode-safety-net", store)
	if err != nil {
		t.Fatalf("open slate backend: %v", err)
	}

	c, err := cluster.Open(cluster.Config{NodeID: "solo", Backend: be})
	if err != nil {
		t.Fatalf("Open single-node with slate backend: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := c.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("one")) {
		t.Fatalf("Get returned %q, want %q", got, "one")
	}

	if err := c.Delete([]byte("alpha")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Get([]byte("alpha")); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("Get after Delete returned %v, want ErrNotFound", err)
	}

	members := c.Members()
	if len(members) != 1 || members[0].ID != "solo" {
		t.Fatalf("single-node reported members %v, want exactly the local node", members)
	}
}
