package cluster_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
)

func TestOpen_RequiresNodeID(t *testing.T) {
	if _, err := cluster.Open(cluster.Config{Backend: memory.New()}); err == nil {
		t.Fatalf("Open with empty NodeID should error")
	}
}

func TestOpen_RequiresBackend(t *testing.T) {
	if _, err := cluster.Open(cluster.Config{NodeID: "n1"}); err == nil {
		t.Fatalf("Open with nil Backend should error")
	}
}

func TestSingleNode_RoundTrip(t *testing.T) {
	c, err := cluster.Open(cluster.Config{NodeID: "n1", Backend: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("got %q want v", got)
	}
}

func TestClosed(t *testing.T) {
	c, _ := cluster.Open(cluster.Config{NodeID: "n1", Backend: memory.New()})
	_ = c.Close()
	if err := c.Put([]byte("k"), []byte("v")); !errors.Is(err, backend.ErrClosed) {
		t.Fatalf("Put on closed should be ErrClosed, got %v", err)
	}
}

func TestAggregate_SingleNode(t *testing.T) {
	c, _ := cluster.Open(cluster.Config{NodeID: "n1", Backend: memory.New()})
	t.Cleanup(func() { _ = c.Close() })
	_ = c.Put([]byte("a"), []byte("1"))
	_ = c.Put([]byte("b"), []byte("2"))

	results := c.Aggregate(func(b backend.Backend) any {
		it, _ := b.ScanPrefix(nil)
		defer it.Close()
		count := 0
		for {
			k, _, err := it.Next()
			if err != nil || k == nil {
				break
			}
			count++
		}
		return count
	})
	if len(results) != 1 || results[0].(int) != 2 {
		t.Fatalf("expected single-node aggregate count=2, got %v", results)
	}
}
