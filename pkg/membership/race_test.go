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
// The test stands up a 2-node cluster, then aggressively hammers
// Members() / Snapshot() from one goroutine while another goroutine
// forces churn (open + close a peer repeatedly). Run under -race the
// test must complete cleanly; a regression that reads Node fields on
// the snapshot path will be caught by the race detector and fail the
// test.

import (
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMembers_NoRaceWithMemberlistInternal pins the no-race property
// of Members/Snapshot under concurrent memberlist alive-node activity.
// Run with `go test -race` for the assertion to mean anything; without
// -race it still exercises the code path but won't detect the
// regression.
//
// Layout:
//
//   - One seed Membership we hammer Members/Snapshot on.
//   - Three long-lived peers gossiping with the seed continuously; their
//     periodic alive broadcasts drive memberlist's aliveNode write path
//     against the Node pointers the seed's Members() observes.
//   - A churn goroutine that joins + leaves short-lived peers, which
//     pushes NotifyJoin / NotifyLeave through the seed's event delegate
//     and exercises the cache write path under the read load.
//   - Many reader goroutines so the snapshot path is constantly inside
//     the cache read while writes pour in.
func TestMembers_NoRaceWithMemberlistInternal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race exercise in short mode")
	}
	seedPort := ephemeralPort(t)
	seed := openTestMembership(t, Config{
		NodeID:    "seed",
		BindAddr:  bindAddr(seedPort),
		GRPCAddr:  "127.0.0.1:30001",
		LogOutput: io.Discard,
	})

	// Open three long-lived companion peers so gossip + probing
	// runs continuously for the whole duration; their periodic alive
	// broadcasts produce a steady stream of internal aliveNode writes
	// against the same Node pointers Members() observes.
	for i, name := range []string{"alpha", "beta", "gamma"} {
		port := ephemeralPort(t)
		_ = openTestMembership(t, Config{
			NodeID:    name,
			BindAddr:  bindAddr(port),
			GRPCAddr:  "127.0.0.1:" + strconv.Itoa(31001+i),
			Seeds:     []string{bindAddr(seedPort)},
			LogOutput: io.Discard,
		})
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var snapshots atomic.Uint64

	// Many reader goroutines hammering Members + Snapshot. With
	// runtime.GOMAXPROCS readers we keep the cache read path hot
	// while the gossip-driven write path is firing.
	const readers = 8
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
		}()
	}

	// Churner: open + close short-lived peers in a tight loop so the
	// seed sees fresh NotifyJoin / NotifyLeave events on top of the
	// steady gossip from the long-lived peers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			port := ephemeralPort(t)
			peer, err := Open(Config{
				NodeID:    churnID(i),
				BindAddr:  bindAddr(port),
				GRPCAddr:  "127.0.0.1:1",
				Seeds:     []string{bindAddr(seedPort)},
				LogOutput: io.Discard,
			})
			if err != nil {
				// A transient seed-unreachable on a busy box is
				// acceptable; just spin and retry. We are not
				// asserting on join success, only on the absence
				// of a race.
				continue
			}
			_ = peer.Close()
			i++
		}
	}()

	// Five seconds is enough to drive thousands of alive-node writes
	// alongside thousands of Members snapshots; the original race
	// fires inside the first second on a loaded machine.
	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()

	if snapshots.Load() == 0 {
		t.Fatalf("reader goroutines never ran a snapshot; test misconfigured")
	}
}

// churnID produces a unique node ID for the i-th churn iteration.
// Pulled out so the loop body stays focused.
func churnID(i int) string {
	const hex = "0123456789abcdef"
	// 4 hex chars is enough headroom for any sensible iteration count
	// inside the 5s test window (max ~few hundred).
	b := []byte{'c', 'h', 'u', 'r', 'n', '-', hex[(i>>12)&0xf], hex[(i>>8)&0xf], hex[(i>>4)&0xf], hex[i&0xf]}
	return string(b)
}
