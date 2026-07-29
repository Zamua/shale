//go:build chaos

package chaos

// In-process adapter: the live cluster the chaos soak drives. This is the
// INFRASTRUCTURE layer behind the small ClusterClient + Topology interfaces the
// workload driver and chaos scheduler depend on. It stands up N shale nodes via
// goroutines + ephemeral ports over ONE sharedfactory backing - the same shape
// tests/integration uses (startSharedNode + the shared backing), so a lease
// handoff is copy-free and a reshard exercises the real cluster-wide freeze.
//
// It is gated behind `//go:build chaos` because it pulls in the whole multi-node
// stack (memberlist gossip, gRPC servers) and is only built for the soak. The
// oracle + driver + scheduler logic that depends on these interfaces is reusable
// against a REAL N-node deploy by swapping this file for a gRPC-client +
// orchestrator implementation (see README "Extending to a real N-node deploy").

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/coord/gossip"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/rpc"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc"
)

// node is one in-process shale node: its cluster handle, the shared-backing
// factory handle (the mount/lease seam; an in-memory double or the real slatedb
// factory, behind the factoryProvider), its memberlist bind address (so a new
// joiner can seed off it), its gRPC server (so it can be killed hard or shut
// gracefully).
type node struct {
	id         string
	cl         *cluster.Cluster
	handle     storageunit.BackendFactory
	bindAddr   string
	grpcAddr   string
	grpcServer *grpc.Server
	serveDone  chan struct{}
	down       bool // true after a hard kill, until restarted
}

// inProcCluster is the Topology + ClusterClient implementation over a set of
// in-process nodes sharing one backing. All structural mutations (add/remove/
// kill/restart node, reshard) and member reads go through it; it serializes
// structural changes behind structMu so the harness's own events never overlap
// (the interesting concurrency - a membership change during a reshard barrier -
// is produced deliberately by the scheduler's combination events, which run the
// reshard in a goroutine).
type inProcCluster struct {
	provider  factoryProvider
	unitCount int

	mu       sync.Mutex // guards nodes + nextID
	nodes    []*node
	nextID   int
	structMu sync.Mutex // serializes structural changes across the harness

	settleDelay time.Duration

	// Convergence + settle budgets, scaled by the config's settleScale so a
	// slow real backend (slatedb over MinIO) is given proportionally longer to
	// mount every unit before a wait gives up. At 1x these are the original
	// in-memory values. memberConverge bounds the per-add/remove member-count
	// convergence wait; reconcileIdle bounds each WaitSettled per-node
	// reconcile-idle wait. The WaitSettled timeouts themselves are passed by the
	// caller (scaled via scaled()).
	memberConverge time.Duration
	reconcileIdle  time.Duration
	scale          float64
}

// scaled returns d multiplied by the cluster's settle scale (>= 1x). Callers use
// it to stretch the WaitSettled / sweep budgets for a slow backend.
func (c *inProcCluster) scaled(d time.Duration) time.Duration {
	if c.scale <= 1 {
		return d
	}
	return time.Duration(float64(d) * c.scale)
}

// newInProcCluster brings up `count` nodes over the provider's shared backing at
// unitCount units, waits for ring convergence + reconcile quiescence, and
// returns the live cluster. It mirrors startReplicatedCluster's settle discipline
// so the soak starts against a fully-settled cluster. The provider supplies every
// node's per-node factory handle (the in-memory double by default, or the real
// slatedb factory over a MinIO bucket under -tags slatedb); all handles share one
// backing, which is what makes a lease handoff copy-free + fence-correct.
func newInProcCluster(provider factoryProvider, count, unitCount int, settleDelay time.Duration, settleScale float64) (*inProcCluster, error) {
	scale := settleScale
	if scale < 1 {
		scale = 1
	}
	c := &inProcCluster{
		provider:    provider,
		unitCount:   unitCount,
		settleDelay: settleDelay,
		scale:       scale,
		// Base in-memory budgets, scaled for a slow backend.
		memberConverge: time.Duration(float64(8*time.Second) * scale),
		reconcileIdle:  time.Duration(float64(3*time.Second) * scale),
	}
	seed := ""
	for i := 0; i < count; i++ {
		n, err := c.startNode(seed)
		if err != nil {
			c.CloseAll()
			return nil, err
		}
		if i == 0 {
			seed = n.bindAddr
		}
	}
	if err := c.waitConverged(count, time.Duration(float64(20*time.Second)*scale)); err != nil {
		c.CloseAll()
		return nil, err
	}
	// Let the join-driven reconcile settle so ownership is stable before the
	// workload starts (matches the integration fixture's pre-write settle).
	time.Sleep(900 * time.Millisecond)
	return c, nil
}

// startNode brings up one node seeded at seedAddr (empty = founder), retrying a
// memberlist bind conflict with a fresh port (the release-rebind race the
// integration helpers also self-heal). It appends the node and returns it.
func (c *inProcCluster) startNode(seedAddr string) (*node, error) {
	c.mu.Lock()
	c.nextID++
	id := "chaos-n" + strconv.Itoa(c.nextID)
	c.mu.Unlock()

	h := c.provider.NewHandle()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("startNode %s: listen gRPC: %w", id, err)
	}
	grpcAddr := lis.Addr().String()
	grpcSrv := grpc.NewServer()
	serveDone := make(chan struct{})

	const maxAttempts = 8
	var cl *cluster.Cluster
	var bindAddr string
	for attempt := 0; attempt < maxAttempts; attempt++ {
		port, perr := freeTCPPort()
		if perr != nil {
			_ = lis.Close()
			return nil, fmt.Errorf("startNode %s: free port: %w", id, perr)
		}
		bindAddr = "127.0.0.1:" + strconv.Itoa(port)
		cfg := cluster.Config{
			NodeID:               id,
			BackendFactory:       h,
			UnitCount:            storageunit.MustUnitCount(c.unitCount),
			GRPCAddr:             grpcAddr,
			LogOutput:            io.Discard,
			RebalanceSettleDelay: c.settleDelay,
		}
		gcfg := gossip.Config{BindAddr: bindAddr, LogOutput: io.Discard}
		if seedAddr != "" {
			gcfg.Seeds = []string{seedAddr}
		}
		cfg.Coordinator = gossip.New(gcfg)
		var oerr error
		cl, oerr = cluster.Open(cfg)
		if oerr == nil {
			break
		}
		if !isBindConflict(oerr) {
			_ = lis.Close()
			return nil, fmt.Errorf("startNode %s: cluster.Open: %w", id, oerr)
		}
		if attempt == maxAttempts-1 {
			_ = lis.Close()
			return nil, fmt.Errorf("startNode %s: %d bind attempts all conflicted: %w", id, maxAttempts, oerr)
		}
	}

	rpc.NewServer(cl).Register(grpcSrv)
	go func() {
		defer close(serveDone)
		_ = grpcSrv.Serve(lis)
	}()

	n := &node{
		id:         id,
		cl:         cl,
		handle:     h,
		bindAddr:   bindAddr,
		grpcAddr:   grpcAddr,
		grpcServer: grpcSrv,
		serveDone:  serveDone,
	}
	c.mu.Lock()
	c.nodes = append(c.nodes, n)
	c.mu.Unlock()
	return n, nil
}

// liveNodes returns a snapshot of the currently-up (not killed) nodes.
func (c *inProcCluster) liveNodes() []*node {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*node, 0, len(c.nodes))
	for _, n := range c.nodes {
		if !n.down {
			out = append(out, n)
		}
	}
	return out
}

// EntryNode returns a live cluster handle to issue an op through, chosen by idx
// modulo the live set, so the workload routes both locally and forwarded.
// Returns nil if (transiently) no node is live.
func (c *inProcCluster) EntryNode(idx int) *cluster.Cluster {
	live := c.liveNodes()
	if len(live) == 0 {
		return nil
	}
	return live[idx%len(live)].cl
}

// AllLiveClusters returns the cluster handle of every live node (for the final
// read-from-every-node sweep).
func (c *inProcCluster) AllLiveClusters() []*cluster.Cluster {
	live := c.liveNodes()
	out := make([]*cluster.Cluster, len(live))
	for i, n := range live {
		out[i] = n.cl
	}
	return out
}

// MemberCount returns the founder's view of the live member count. Used by the
// final accounting + the converge waits.
func (c *inProcCluster) MemberCount() int {
	live := c.liveNodes()
	if len(live) == 0 {
		return 0
	}
	return len(live[0].cl.Members())
}

// waitConverged waits until every live node reports `want` members.
func (c *inProcCluster) waitConverged(want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		live := c.liveNodes()
		all := len(live) > 0
		for _, n := range live {
			if len(n.cl.Members()) != want {
				all = false
				break
			}
		}
		if all {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	sizes := []int{}
	for _, n := range c.liveNodes() {
		sizes = append(sizes, len(n.cl.Members()))
	}
	return fmt.Errorf("not all live nodes converged to %d members: sizes=%v", want, sizes)
}

// --- Topology mutations (chaos events) ----------------------------------

// AddNode joins a fresh node seeded off an existing live member and waits for
// the new member count to converge. The Phase 3 lease handoff redistributes
// units to it.
func (c *inProcCluster) AddNode() (string, error) {
	c.structMu.Lock()
	defer c.structMu.Unlock()
	live := c.liveNodes()
	if len(live) == 0 {
		return "", fmt.Errorf("AddNode: no live node to seed off")
	}
	seed := live[0].bindAddr
	n, err := c.startNode(seed)
	if err != nil {
		return "", err
	}
	want := len(live) + 1
	_ = c.waitConverged(want, c.memberConverge)
	return n.id, nil
}

// RemoveNode gracefully closes a live node (broadcasts the memberlist Leave) and
// waits for survivors to converge. Survivors re-acquire its units via reconcile.
// Never removes the last node.
func (c *inProcCluster) RemoveNode(id string) error {
	c.structMu.Lock()
	defer c.structMu.Unlock()
	return c.removeLocked(id, true)
}

// KillNode hard-stops a node: grpc.Server.Stop (no graceful drain) + cluster
// Close, then marks it down. Models a process death mid-workload. Never kills
// the last node.
func (c *inProcCluster) KillNode(id string) error {
	c.structMu.Lock()
	defer c.structMu.Unlock()
	return c.removeLocked(id, false)
}

// removeLocked tears a node down. graceful=true does a GracefulStop + cluster
// Close (a clean Leave); graceful=false does a hard Stop (a crash). Either way
// the node is removed from the live set. Refuses to drop the last live node.
func (c *inProcCluster) removeLocked(id string, graceful bool) error {
	c.mu.Lock()
	liveCount := 0
	var target *node
	for _, n := range c.nodes {
		if !n.down {
			liveCount++
			if n.id == id {
				target = n
			}
		}
	}
	c.mu.Unlock()
	if target == nil {
		return fmt.Errorf("removeLocked: node %q not live", id)
	}
	if liveCount <= 1 {
		return fmt.Errorf("removeLocked: refusing to remove the last live node %q", id)
	}

	if graceful {
		// Clean Leave: close the cluster (which gracefully leaves memberlist +
		// closes the backend handle), then stop the gRPC server.
		_ = target.cl.Close()
		target.grpcServer.GracefulStop()
		<-target.serveDone
	} else {
		// Crash: hard-stop the gRPC server first (interrupts in-flight forwards),
		// then close the cluster so memberlist eventually fails-detects it.
		target.grpcServer.Stop()
		<-target.serveDone
		_ = target.cl.Close()
	}

	c.mu.Lock()
	target.down = true
	// Drop it from the slice so a new node can reuse the id space cleanly.
	kept := c.nodes[:0]
	for _, n := range c.nodes {
		if n != target {
			kept = append(kept, n)
		}
	}
	c.nodes = kept
	want := liveCount - 1
	c.mu.Unlock()

	_ = c.waitConverged(want, c.memberConverge)
	return nil
}

// Reshard triggers a doubling reshard on a live coordinator node via the
// cluster-wide freeze barrier. Returns the coordinator id used.
func (c *inProcCluster) Reshard() error {
	c.structMu.Lock()
	defer c.structMu.Unlock()
	live := c.liveNodes()
	if len(live) == 0 {
		return fmt.Errorf("Reshard: no live node")
	}
	return live[0].cl.Reshard()
}

// ReshardAsync kicks the reshard off in a goroutine and returns immediately with
// a channel that delivers the result. Used by the combination events so the
// scheduler can fire a membership change WHILE the barrier is in flight (to drive
// the clean-abort path). The structMu is intentionally NOT held across the async
// barrier: the membership change must be able to proceed concurrently.
func (c *inProcCluster) ReshardAsync() <-chan error {
	done := make(chan error, 1)
	live := c.liveNodes()
	if len(live) == 0 {
		done <- fmt.Errorf("ReshardAsync: no live node")
		return done
	}
	coord := live[0].cl
	go func() { done <- coord.Reshard() }()
	return done
}

// CloseAll tears down every node (used at the end / on setup failure). Idempotent
// per node via the down flag.
func (c *inProcCluster) CloseAll() {
	c.mu.Lock()
	nodes := append([]*node(nil), c.nodes...)
	c.mu.Unlock()
	for _, n := range nodes {
		if n.down {
			continue
		}
		_ = n.cl.Close()
		n.grpcServer.GracefulStop()
		<-n.serveDone
		n.down = true
	}
}

// WaitSettled drives the cluster to a fully-settled state before the final
// sweep: every live node agrees on the member set, every node's reconcile/reshard
// machinery is idle, AND the union of mounted units across all live nodes covers
// the WHOLE expected unit space exactly once (no owner-but-unmounted unit left
// over from the last chaos event). Without this, the final sweep would read a
// half-converged cluster and retry every owner-but-unmounted key for the full
// per-key budget, which dominates the run's wall clock. We force a synchronous
// reconcile on every node each iteration (the debounced async reconcile may not
// have fired yet) and loop until the mount set is complete + stable or the budget
// runs out. expectedUnits is base<<generation (the scheduler's authoritative live
// count); a value <= 0 skips the mount-completeness gate (members + idle only).
func (c *inProcCluster) WaitSettled(expectedUnits int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		live := c.liveNodes()
		if len(live) == 0 {
			return
		}
		// Force a synchronous reconcile + wait idle on every node.
		ctx, cancel := context.WithTimeout(context.Background(), c.reconcileIdle)
		for _, n := range live {
			n.cl.TestingRunReconcile()
			_ = n.cl.WaitForRebalanceIdle(ctx)
		}
		cancel()

		// Member convergence: every node sees the same count.
		want := len(live)
		converged := true
		for _, n := range live {
			if len(n.cl.Members()) != want {
				converged = false
				break
			}
		}

		// Routing completeness, self-validating (does NOT trust an external count).
		// The property the final sweep needs is that EVERY unit is mounted on the
		// SAME node the ring assigns it to, at ONE uniform generation. If a unit is
		// mounted on a non-owner (or its ring owner has not yet acquired it), a Get
		// for that unit's key resolves to an owner that does not have it and every
		// node disclaims it ("forwarding loop refused"). We therefore require:
		//   (a) every live node's mounted units are all at one generation g (no node
		//       lagging at a prior gen after a reshard, and no node that restarted
		//       still empty / behind);
		//   (b) the union of mounted units is exactly [0, M) for some M, each mounted
		//       once;
		//   (c) each unit is mounted on the node the ring (built from the live
		//       members at gen g) assigns it to - so routing and placement agree.
		mountOK := true
		if converged {
			gens := make(map[storageunit.Generation]struct{})
			mountOwner := make(map[storageunit.UnitID]string)
			dup := false
			for _, n := range live {
				open := n.handle.OpenUnits()
				if len(open) == 0 {
					// A live node that mounts nothing is mid-acquire (a fresh joiner /
					// restarted node that has not taken its units yet): not settled.
					mountOK = false
				}
				for _, m := range open {
					gu := m.Unit()
					gens[gu.Gen] = struct{}{}
					if _, seen := mountOwner[gu.ID]; seen {
						dup = true
					}
					mountOwner[gu.ID] = n.id
				}
			}
			if dup || len(mountOwner) == 0 || len(gens) != 1 {
				mountOK = false
			} else if mountOK {
				var g storageunit.Generation
				for gg := range gens {
					g = gg
				}
				m := len(mountOwner)
				// Contiguous [0, M).
				for u := storageunit.UnitID(0); int(u) < m && mountOK; u++ {
					if _, ok := mountOwner[u]; !ok {
						mountOK = false
					}
				}
				// Each unit mounted on its ring-assigned owner.
				if mountOK {
					members := live[0].cl.Members()
					r := ring.New()
					for _, mem := range members {
						r.Add(mem)
					}
					for u := storageunit.UnitID(0); int(u) < m; u++ {
						owner := r.LocateKey(genUnitBytes(storageunit.NewGenUnit(g, u))).ID
						if mountOwner[u] != owner {
							mountOK = false
							break
						}
					}
				}
				if mountOK && expectedUnits > 0 && m != expectedUnits {
					// The cluster settled to a different count than the scheduler's
					// bookkeeping expected; the cluster's own state is authoritative.
					mountOK = true
				}
			}
		}

		if converged && mountOK {
			time.Sleep(150 * time.Millisecond) // brief drain for async side effects
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// --- small helpers --------------------------------------------------------

// freeTCPPort grabs an OS-assigned ephemeral port + releases it (the caller binds
// it for real, accepting the release-rebind race that the retry loop heals).
func freeTCPPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port, nil
}

// isBindConflict reports whether err is a memberlist bind conflict (the
// release-then-rebind race), matched on the stable wrapped substring.
func isBindConflict(err error) bool {
	return err != nil && contains(err.Error(), "address already in use")
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// genUnitBytes mirrors the cluster's internal unit-id encoding (8-byte big-endian
// generation, then 4-byte big-endian unit id) so a test-side ring placement
// agrees with the cluster's routing. Used by WaitSettled to check that each unit
// is mounted on the node the ring assigns it to.
func genUnitBytes(gu storageunit.GenUnit) []byte {
	var b [12]byte
	g := uint64(gu.Gen)
	for i := 0; i < 8; i++ {
		b[7-i] = byte(g >> (8 * i))
	}
	u := uint32(gu.ID)
	b[8] = byte(u >> 24)
	b[9] = byte(u >> 16)
	b[10] = byte(u >> 8)
	b[11] = byte(u)
	return b[:]
}

// errNotFound is the sentinel a Get returns when a key is absent; the driver maps
// it to Observed{NotFound:true}. Re-exported here so the driver does not import
// backend directly for the comparison.
var errNotFound = backend.ErrNotFound
