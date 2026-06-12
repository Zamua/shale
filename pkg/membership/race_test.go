package membership

// race_test.go pins the v0.4 fix for the membership Members/Snapshot
// data race: nodeToMember used to be invoked from Members() while
// memberlist's internal aliveNode goroutine concurrently wrote
// *memberlist.Node.Name + .Meta, producing a race-detector positive
// any time a node was probed mid-snapshot. The fix is to read from a
// cache populated inside the event callbacks (which memberlist
// serializes against its alive/dead transitions) instead of
// dereferencing Node pointers on the read path.
//
// The regression test is a load test for the alive-broadcast write
// path. The original race fires when memberlist's internal aliveNode
// routine writes a Node's Name + Meta concurrent with our read path
// dereferencing the same fields. To make that collision dense enough
// for -race to catch it within a short test budget we explicitly
// drive memberlist.UpdateNode() on every peer at a high cadence;
// UpdateNode re-broadcasts the local node's alive state, which routes
// through aliveNode() on every receiving peer + on the local node
// itself and writes Name/Meta during the handler. With five peers each
// pumping UpdateNode at 50ms intervals, the seed sees a steady stream
// of aliveNode writes against the Node pointers Members()/Snapshot()
// would dereference under the pre-fix code.

import (
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMembers_NoRaceWithMemberlistInternal pins the no-race property
// of Members/Snapshot under heavy alive-broadcast pressure. Run with
// `go test -race` for the assertion to mean anything; without -race
// it still exercises the code path but won't detect the regression.
//
// Layout:
//
//   - One seed Membership we hammer Members/Snapshot on.
//   - Four long-lived companion peers, each driving UpdateNode() at a
//     50ms cadence so memberlist's aliveNode write path is firing
//     constantly against every Node pointer the seed holds.
//   - Many reader goroutines so the snapshot path is constantly inside
//     the read window while alive writes pour in.
//
// To verify this test actually pins the fix: revert Members() to
// dereference *memberlist.Node directly (i.e. iterate m.ml.Members()
// and call nodeToMember on each) + rerun under -race. The test should
// reliably fail with a `DATA RACE` report against
// memberlist.(*Memberlist).aliveNode vs membership.nodeToMember.
func TestMembers_NoRaceWithMemberlistInternal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race exercise in short mode")
	}
	const peerCount = 4

	seedPort := ephemeralPort(t)
	seed := openTestMembership(t, Config{
		NodeID:    "seed",
		BindAddr:  bindAddr(seedPort),
		GRPCAddr:  "127.0.0.1:30001",
		LogOutput: io.Discard,
	})

	// Open long-lived companion peers so gossip + probing runs
	// continuously. We retain handles so we can drive UpdateNode on
	// each one to force alive-broadcast pressure into the seed.
	peers := make([]*Membership, 0, peerCount)
	for i, name := range []string{"alpha", "beta", "gamma", "delta"}[:peerCount] {
		port := ephemeralPort(t)
		p := openTestMembership(t, Config{
			NodeID:    name,
			BindAddr:  bindAddr(port),
			GRPCAddr:  "127.0.0.1:" + strconv.Itoa(31001+i),
			Seeds:     []string{bindAddr(seedPort)},
			LogOutput: io.Discard,
		})
		peers = append(peers, p)
	}

	// Wait briefly for the cluster to converge so all peers know each
	// other and UpdateNode actually has somewhere to broadcast. If we
	// race UpdateNode in before convergence the alive-handler writes
	// still happen, but the read path has fewer Node pointers to fight
	// over and the test takes longer to flake out a regression.
	if err := waitForMembers(seed, map[string]string{
		"seed":  "127.0.0.1:30001",
		"alpha": "127.0.0.1:31001",
		"beta":  "127.0.0.1:31002",
		"gamma": "127.0.0.1:31003",
		"delta": "127.0.0.1:31004",
	}, 10*time.Second); err != nil {
		t.Fatalf("cluster convergence: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var snapshots atomic.Uint64
	var updates atomic.Uint64

	// Many reader goroutines hammering Members + Snapshot. With more
	// readers than peers we keep the cache read path saturated while
	// the gossip-driven write path fires.
	const readers = 8
	for range readers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					_ = seed.Members()
					_ = seed.Snapshot()
					snapshots.Add(1)
				}
			}
		})
	}

	// Drive UpdateNode() on each peer at a 50ms cadence. UpdateNode
	// re-advertises the local node, which fires memberlist's
	// alive-handler on every receiving peer (including the seed) and
	// writes Name/Meta into the Node entries the seed's Members()
	// would dereference under the pre-fix code. Hammering from every
	// peer simultaneously turns up the write pressure enough that the
	// race detector reliably catches a regression within a few
	// seconds.
	for _, p := range peers {
		wg.Add(1)
		go func(p *Membership) {
			defer wg.Done()
			tick := time.NewTicker(50 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					// 500ms broadcast budget is well above the
					// per-tick interval; on a loopback this
					// returns in ms. Errors here are not
					// interesting -- the test asserts on the
					// absence of a race detector hit, not on
					// broadcast success.
					_ = p.ml.UpdateNode(500 * time.Millisecond)
					updates.Add(1)
				}
			}
		}(p)
	}

	// 10s is enough to drive a couple hundred UpdateNode broadcasts
	// across the four peers + tens of thousands of snapshot reads on
	// the seed. The original race fires inside the first second on a
	// loaded machine once UpdateNode is driving aliveNode at this
	// cadence.
	time.Sleep(10 * time.Second)
	close(stop)
	wg.Wait()

	if snapshots.Load() == 0 {
		t.Fatalf("reader goroutines never ran a snapshot; test misconfigured")
	}
	if updates.Load() == 0 {
		t.Fatalf("UpdateNode broadcasts never fired; test misconfigured")
	}
}
