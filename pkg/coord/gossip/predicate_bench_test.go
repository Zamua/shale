package gossip_test

import (
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/coord"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/storageunit"
)

// These benchmarks pin the cost of the two PER-OPERATION port queries against
// the View-construction shape they replaced. The storage layer asks both
// questions on every Put/Get/Delete, so their cost is data-path cost:
// the original port draft answered them via View() - member-list copy, facts
// map, slice allocation, sort - and measured ~two orders of magnitude slower
// than the pre-port ring predicate, with allocation on every op. If
// BenchmarkPopulated ever grows an allocation or approaches
// BenchmarkViewEmpty, the port has regressed into that shape again.
//
// The static variants are hermetic (no transport); the Gossip variants run
// the PRODUCTION code path on a real solo memberlist so the guard covers the
// branch production actually executes, not only the static one.

func staticN(n int) *gossip.Coordinator {
	members := make([]coord.Node, 0, n)
	for i := 0; i < n; i++ {
		members = append(members, coord.Node{
			ID:   storageunit.NodeID(fmt.Sprintf("bench-node-%02d", i)),
			Addr: fmt.Sprintf("127.0.0.1:%d", 9000+i),
		})
	}
	return gossip.NewStatic(members[0], members)
}

func benchSizes(b *testing.B, f func(b *testing.B, c *gossip.Coordinator)) {
	for _, n := range []int{3, 16} {
		c := staticN(n)
		b.Run(fmt.Sprintf("members=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			f(b, c)
		})
	}
}

// BenchmarkPopulated is the routing predicate as shipped: multiReplicated /
// casReplicated call this once per operation.
func BenchmarkPopulated(b *testing.B) {
	benchSizes(b, func(b *testing.B, c *gossip.Coordinator) {
		for i := 0; i < b.N; i++ {
			if !c.Populated() {
				b.Fatal("static coordinator must be populated")
			}
		}
	})
}

// BenchmarkViewEmpty is the shape Populated replaced - the full snapshot
// built to answer one boolean. Kept as the comparison baseline.
func BenchmarkViewEmpty(b *testing.B) {
	benchSizes(b, func(b *testing.B, c *gossip.Coordinator) {
		for i := 0; i < b.N; i++ {
			if c.View().Empty() {
				b.Fatal("static coordinator must be populated")
			}
		}
	})
}

// BenchmarkTransitionSets is the per-op joining/draining split as shipped.
// Steady state (no transitional roles) must stay allocation-free.
func BenchmarkTransitionSets(b *testing.B) {
	benchSizes(b, func(b *testing.B, c *gossip.Coordinator) {
		for i := 0; i < b.N; i++ {
			j, d := c.TransitionSets()
			if j != nil || d != nil {
				b.Fatal("steady-state static coordinator must have nil transition sets")
			}
		}
	})
}

// BenchmarkViewTransitionScan is the shape TransitionSets replaced - a full
// snapshot scanned for role bits. Kept as the comparison baseline.
func BenchmarkViewTransitionScan(b *testing.B) {
	benchSizes(b, func(b *testing.B, c *gossip.Coordinator) {
		for i := 0; i < b.N; i++ {
			var j, d map[storageunit.NodeID]struct{}
			for _, m := range c.View().Members {
				if m.Joining() {
					if j == nil {
						j = make(map[storageunit.NodeID]struct{}, 2)
					}
					j[m.ID] = struct{}{}
				}
				if m.Draining() {
					if d == nil {
						d = make(map[storageunit.NodeID]struct{}, 2)
					}
					d[m.ID] = struct{}{}
				}
			}
			if j != nil || d != nil {
				b.Fatal("steady-state static coordinator must have nil transition sets")
			}
		}
	})
}

// benchSolo stands up a real solo gossip coordinator (production code path:
// live membership snapshot, not the static facts override).
func benchSolo(b *testing.B) *gossip.Coordinator {
	b.Helper()
	co := gossip.New(gossip.Config{
		BindAddr:            "127.0.0.1:" + strconv.Itoa(freePort(b)),
		LogOutput:           io.Discard,
		ReconcileInterval:   time.Hour,
		RejoinInterval:      -1,
		MetaRefreshInterval: -1,
	})
	if _, err := co.Start(coord.Params{Self: coord.Node{ID: "bench-solo", Addr: "127.0.0.1:1"}}); err != nil {
		b.Fatalf("Start: %v", err)
	}
	b.Cleanup(func() { _ = co.Close() })
	return co
}

func BenchmarkPopulatedGossip(b *testing.B) {
	co := benchSolo(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !co.Populated() {
			b.Fatal("solo coordinator must be populated")
		}
	}
}

func BenchmarkViewEmptyGossip(b *testing.B) {
	co := benchSolo(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if co.View().Empty() {
			b.Fatal("solo coordinator must be populated")
		}
	}
}

func BenchmarkTransitionSetsGossip(b *testing.B) {
	co := benchSolo(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j, d := co.TransitionSets()
		if j != nil || d != nil {
			b.Fatal("steady-state solo coordinator must have nil transition sets")
		}
	}
}
