package cluster_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend/memory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/rebalance"
	"github.com/Zamua/shale/pkg/ring"
)

// slugShardKey co-locates every key belonging to one logical subject on
// a single shard, mirroring the production app's ShardKeyFn (hostthis's
// shaleShardKey). It maps:
//
//	pastes/<slug>          -> <slug>
//	versions/<slug>/<NNNN>  -> <slug>
//
// so that a paste and ALL its version keys hash to the same shard and
// therefore the same ring owner. Any other key routes by its whole self.
//
// This is the shape the founder-grows memory test
// (TestFounderGrows_RebalanceReachesEveryKey) does NOT exercise: that
// test uses single keys with no ShardKeyFn, so a key's raw-key partition
// and its routed partition coincide. Production keys do not: one logical
// subject spans many raw keys that the default (raw-key) partition
// function scatters across different partitions, while the cluster routes
// reads on the shard key. If the rebalance/reconcile partition function
// ignores ShardKeyFn, the partition a subject's keys physically live in
// disagrees with the partition the ring routes its reads to, and the
// founder-grows handoff drops the multi-key subjects.
func slugShardKey(key []byte) []byte {
	if rest, ok := bytes.CutPrefix(key, []byte("pastes/")); ok {
		return firstSeg(rest)
	}
	if rest, ok := bytes.CutPrefix(key, []byte("versions/")); ok {
		return firstSeg(rest)
	}
	return key
}

func firstSeg(s []byte) []byte {
	if before, _, ok := bytes.Cut(s, []byte{'/'}); ok {
		return before
	}
	return s
}

// openClusterNodeWithShardKey is openClusterNodeAt plus a ShardKeyFn, so
// the cluster routes (and must rebalance) on the shard key rather than
// the raw key.
func openClusterNodeWithShardKey(t *testing.T, id, bindAddr, seedBindAddr string, mem *memory.Memory, skf func([]byte) []byte) (*cluster.Cluster, func()) {
	t.Helper()
	grpcHarness, stop := startGRPC(t)
	cfg := cluster.Config{
		NodeID:                 id,
		Backend:                mem,
		BindAddr:               bindAddr,
		GRPCAddr:               grpcHarness.addr,
		ShardKeyFn:             skf,
		LogOutput:              io.Discard,
		RebalanceSettleDelay:   500 * time.Millisecond,
		RebalanceGraceDuration: 1500 * time.Millisecond,
	}
	if seedBindAddr != "" {
		cfg.Seeds = []string{seedBindAddr}
	}
	c, err := cluster.Open(cfg)
	if err != nil {
		stop()
		t.Fatalf("openClusterNodeWithShardKey %s: %v", id, err)
	}
	grpcHarness.register(c)
	return c, func() {
		_ = c.Close()
		stop()
	}
}

// TestFounderGrows_MultiKeyShard_ReachesEveryKey is the prod-shape
// regression for the founder-grows rebalance loss: the founder writes
// MULTI-KEY subjects (a paste plus several version keys) that a custom
// ShardKeyFn co-locates onto one shard, THEN a second node joins. After
// the ring redistributes and rebalance settles, every raw key must be
// readable from the CLUSTER (routed Get via either node), and every
// subject's keys must physically live together on the single node the
// ring routes that subject to.
//
// This is what the hostthis slate gate exposed and what the pure-memory
// founder-grows test missed: with a ShardKeyFn, the rebalance partition
// function must be computed on the SHARD KEY (same as routing), not the
// raw key. If it is computed on the raw key, a subject's `pastes/<slug>`
// key and its `versions/<slug>/<n>` keys scatter across partitions; the
// reconcile pass on the joiner only repairs whichever raw-key partition
// happens to land on it, leaving the rest stranded on the founder while
// the ring routes the subject's reads to the joiner. Routed Get then
// finds nothing: data lost from the cluster.
func TestFounderGrows_MultiKeyShard_ReachesEveryKey(t *testing.T) {
	rebalance.SetSweepInterval(50 * time.Millisecond)

	founderMem := memory.New()
	joinerMem := memory.New()

	founderBind := hostPort(freePort(t))
	joinerBind := hostPort(freePort(t))

	founder, founderStop := openClusterNodeWithShardKey(t, "fgmk-founder", founderBind, "", founderMem, slugShardKey)
	defer founderStop()

	if err := waitForRingSize(founder, 1, 5*time.Second); err != nil {
		t.Fatalf("founder solo ring: %v", err)
	}

	// 40 subjects, each spanning a `pastes/<slug>` key plus 3 version
	// keys = 4 raw keys per subject, all co-located on the slug shard.
	// The multi-key-per-shard shape is the whole point: it is what the
	// scatter bug strands.
	const subjects = 40
	type subject struct {
		slug string
		keys [][]byte
	}
	all := make([]subject, 0, subjects)
	for i := range subjects {
		slug := fmt.Sprintf("slug%04d", i)
		keys := [][]byte{
			[]byte("pastes/" + slug),
			fmt.Appendf(nil, "versions/%s/0001", slug),
			fmt.Appendf(nil, "versions/%s/0002", slug),
			fmt.Appendf(nil, "versions/%s/0003", slug),
		}
		for _, k := range keys {
			if err := putWithMigrationRetry(founder, k, []byte("v-"+slug)); err != nil {
				t.Fatalf("Put %s: %v", k, err)
			}
		}
		all = append(all, subject{slug: slug, keys: keys})
	}

	wantKeys := subjects * 4
	if got := countBackend(t, founderMem); got != wantKeys {
		t.Fatalf("founder pre-growth key count = %d, want %d", got, wantKeys)
	}

	// Grow: 2nd node joins.
	joiner, joinerStop := openClusterNodeWithShardKey(t, "fgmk-joiner", joinerBind, founderBind, joinerMem, slugShardKey)
	defer joinerStop()

	for _, c := range []*cluster.Cluster{founder, joiner} {
		if err := waitForRingSize(c, 2, 5*time.Second); err != nil {
			t.Fatalf("2-node ring on %s: %v", c.NodeID(), err)
		}
	}

	for _, c := range []*cluster.Cluster{founder, joiner} {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := c.WaitForRebalanceIdle(ctx); err != nil {
			cancel()
			t.Fatalf("%s did not idle: %v", c.NodeID(), err)
		}
		cancel()
	}
	// One extra settle window so a late reconcile tick lands + the
	// source sweep drops stale copies.
	time.Sleep(1500 * time.Millisecond)

	// --- Assertion A: routed Get for every raw key via BOTH nodes. The
	// non-owning node forwards over gRPC; both must return the value.
	for _, s := range all {
		for _, k := range s.keys {
			if _, err := founder.Get(k); err != nil {
				t.Fatalf("DATA LOSS: Get(%q) via founder after rebalance: %v", k, err)
			}
			if _, err := joiner.Get(k); err != nil {
				t.Fatalf("DATA LOSS: Get(%q) via joiner (forwarded) after rebalance: %v", k, err)
			}
		}
	}

	// --- Assertion B: physical co-location matches the ring. Every key
	// of a subject must live on the single node the ring routes that
	// subject's shard to (a subject's keys must not be split across
	// nodes, and must not be stranded on the non-owner).
	r := ring.New()
	for _, m := range founder.Members() {
		r.Add(m)
	}
	backends := map[string]*memory.Memory{
		"fgmk-founder": founderMem,
		"fgmk-joiner":  joinerMem,
	}
	splitOrStranded := 0
	var firstBad string
	for _, s := range all {
		owner := r.LocateKey(slugShardKey([]byte("pastes/" + s.slug))).ID
		for _, k := range s.keys {
			if _, err := backends[owner].Get(k); err != nil {
				if firstBad == "" {
					firstBad = fmt.Sprintf("%q (subject %s -> ring-owner %s) missing on owner backend", k, s.slug, owner)
				}
				splitOrStranded++
			}
		}
	}
	if splitOrStranded > 0 {
		t.Fatalf("%d/%d keys not on their subject's ring-owner backend (first: %s); "+
			"founder-grows multi-key handoff did not honor ShardKeyFn",
			splitOrStranded, wantKeys, firstBad)
	}
}
