package cluster_test

import (
	"testing"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
)

// TestOpen_RejectsOversizedNodeID pins that Open refuses to construct
// a Cluster whose NodeID would silently truncate when encoded into
// the envelope's 2-byte length prefix. MaxNodeIDLen is 256; anything
// larger is rejected at startup so the originator never ships a
// truncated Stamp.NodeID through the LWW comparator.
func TestOpen_RejectsOversizedNodeID(t *testing.T) {
	oversize := make([]byte, cluster.MaxNodeIDLen+1)
	for i := range oversize {
		oversize[i] = 'a'
	}
	_, err := cluster.Open(cluster.Config{
		NodeID:  string(oversize),
		Backend: memory.New(),
	})
	if err == nil {
		t.Fatalf("Open with NodeID of length %d should error (MaxNodeIDLen=%d)", len(oversize), cluster.MaxNodeIDLen)
	}
}
