package cluster_test

// Regression test for the Put-with-empty-value rejection.

import (
	"errors"
	"testing"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
)

// TestPut_RejectsEmptyValue pins that Put(key, nil) +
// Put(key, []byte{}) MUST return ErrEmptyValue at both R=1 and R>1.
// The empty-payload envelope shape is reserved for tombstones; a
// silent split-by-replication-factor semantic (R>1 stores a
// tombstone surfacing as NotFound, R=1 stores empty bytes surfacing
// as empty success) would be a foot-gun. See docs/SPEC.md "Value
// envelope".
func TestPut_RejectsEmptyValue(t *testing.T) {
	t.Run("single-node-R1", func(t *testing.T) {
		c, err := cluster.Open(cluster.Config{NodeID: "n1", Backend: memory.New()})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { _ = c.Close() })
		if err := c.Put([]byte("k"), nil); !errors.Is(err, cluster.ErrEmptyValue) {
			t.Errorf("Put nil at R=1: got %v want ErrEmptyValue", err)
		}
		if err := c.Put([]byte("k"), []byte{}); !errors.Is(err, cluster.ErrEmptyValue) {
			t.Errorf("Put []byte{} at R=1: got %v want ErrEmptyValue", err)
		}
	})
}
