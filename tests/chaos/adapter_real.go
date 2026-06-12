//go:build chaosreal

package chaos

// REAL-CLUSTER adapter: the live cluster the chaos orchestrator drives is a set
// of SEPARATE OS PROCESSES (shaled-slate child processes) in MULTI-BACKEND mode,
// talking real gRPC + real memberlist, over a SHARED object-storage bucket (MinIO)
// as the durable backing. This is the infrastructure layer behind the same
// Topology + ClusterClient seams the in-process adapter (adapter_inproc.go)
// implements - but every structural action is a real operator action over a
// process boundary:
//
//	AddNode     = launch a shaled-slate process (os/exec), fresh gRPC + memberlist
//	              port pair, seeded at an existing live node; wait until ready.
//	KillNode    = SIGKILL the process. A HARD death: no graceful drain, no flush.
//	              This is the DURABLE-HANDOFF test - the acked writes the dead node
//	              owned live as real slatedb databases in the shared bucket, so a
//	              survivor re-OpenUnits them (copy-free lease handoff) and serves
//	              them. The killed owner's acked keys SURVIVE on a survivor.
//	RemoveNode  = graceful stop (SIGTERM -> shaled.Run's signal handler does a
//	              clean memberlist Leave + backend Close).
//	Reshard     = the DECLARATIVE trigger (v0.8): bump the desired --unit-count
//	              (N -> 2N) and re-roll every node, the rolling-deploy a
//	              `kubectl set env` + `kubectl rollout restart` performs. The
//	              cluster's deterministic coordinator then reshards itself to match
//	              the new desired count. There is NO operator Reshard RPC anymore.
//
// Every node runs `--unit-count > 1` (multi-backend mode) against the SAME bucket
// + the SAME `--slate-key-prefix`, so the cluster shares ONE durable backing and a
// unit's bytes are reachable by whichever node currently holds its lease. This is
// the deployable v0.8 model end to end: no per-node `--slate-db-name`; per-unit
// DbNames are factory-derived from the GenUnit.
//
// The KV ops (Put/Get/Delete) go through the REAL gRPC client (pkg/rpc.Client),
// exactly the surface an application or the shale CLI uses. The oracle (oracle.go,
// build-tag-free) is UNCHANGED and consumes acks from this real client identically
// to the in-process one.
//
// Gated behind //go:build chaosreal so the normal suite + the in-process chaos
// soak (//go:build chaos) are entirely unaffected: this file is excluded from
// both. It reuses ONLY the pure oracle from the chaos package; it shares no code
// with the //go:build chaos files (which are not compiled under chaosreal).

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Zamua/shale/pkg/rpc"
)

// realNode is one shaled-slate child process: its node id, the OS process, the
// ports it bound, a pooled gRPC client to its own gRPC address, and a down flag
// (true after a hard kill / graceful stop, until a fresh process is launched). In
// multi-backend mode there is no per-node DbName: every node shares the one bucket
// + key-prefix and owns whichever GenUnits the ring leases to it.
type realNode struct {
	id        string
	grpcAddr  string
	bindAddr  string
	cmd       *exec.Cmd
	logPath   string
	unitCount int // the --unit-count (desired) this process was launched with

	mu     sync.Mutex
	client *rpc.Client
	down   bool
}

// clientLocked returns (lazily dialing) the gRPC client for this node. Held under
// the node mu so a concurrent killer that closes the client races cleanly.
func (n *realNode) clientLocked() (*rpc.Client, error) {
	if n.down {
		return nil, fmt.Errorf("realNode %s: down", n.id)
	}
	if n.client == nil {
		c, err := rpc.NewClient(n.grpcAddr)
		if err != nil {
			return nil, err
		}
		n.client = c
	}
	return n.client, nil
}

func (n *realNode) closeClient() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.client != nil {
		_ = n.client.Close()
		n.client = nil
	}
}

// realClusterConfig is the operator-supplied environment the orchestrator needs
// to launch + reach the cluster. All of it is resolved from env in
// chaos_real_test.go so the run is a single command.
type realClusterConfig struct {
	binaryPath string // absolute path to the prebuilt shaled-slate binary
	libDir     string // dir holding libslatedb_uniffi.dylib (DYLD_LIBRARY_PATH)
	bucket     string // shared object-storage bucket (the durable backing)
	endpoint   string // S3-compatible endpoint URL (MinIO)
	accessKey  string
	secretKey  string
	region     string
	useSSL     bool
	logDir     string // dir to write per-node stdout/stderr logs
	unitCount  int    // starting unit count N (power of two > 1; multi-backend mode)
	keyPrefix  string // shared key-prefix every node mounts under (one cluster, one backing)
}

// realTopology stands up + manages the shaled-slate child processes. It is the
// REAL analogue of adapter_inproc.go's inProcCluster: same structural-mutation
// surface (AddNode / KillNode / RemoveNode / Reshard / member reads), but every
// action crosses a process boundary. Structural changes serialize behind structMu
// so the orchestrator's own events never overlap.
type realTopology struct {
	cfg realClusterConfig

	mu     sync.Mutex
	nodes  []*realNode
	nextID int
	// desiredCount is the DECLARATIVE desired unit count every node is launched
	// with (the --unit-count deploy config). It starts at cfg.unitCount and the
	// declarative-reshard seam BUMPS it (N -> 2N) before re-rolling the nodes, so
	// a freshly launched / re-rolled node advertises the new desired count and the
	// cluster's coordinator reshards itself to match. Read in launchNode under mu.
	desiredCount int
	founder      *realNode // first-launched node; the stable seed + entry anchor
	structMu     sync.Mutex
}

// newRealTopology launches `count` shaled-slate processes in MULTI-BACKEND mode
// against the shared bucket + shared key-prefix and waits for the cluster to
// converge to `count` members (observed via a Topology RPC to the founder). Every
// node mounts the SAME (bucket, key-prefix) namespace and owns whichever GenUnits
// the ring leases to it; per-unit single-writer fencing is what keeps two nodes
// off the same unit, NOT a per-node DbName. Returns a live topology or an error
// (after cleaning up).
func newRealTopology(cfg realClusterConfig, count int) (*realTopology, error) {
	t := &realTopology{cfg: cfg, desiredCount: cfg.unitCount}
	var seed string
	for i := 0; i < count; i++ {
		n, err := t.launchNode(seed)
		if err != nil {
			t.CloseAll()
			return nil, fmt.Errorf("newRealTopology: launch node %d: %w", i, err)
		}
		if i == 0 {
			t.founder = n
			seed = n.bindAddr
		}
	}
	if err := t.waitConverged(count, 30*time.Second); err != nil {
		t.CloseAll()
		return nil, err
	}
	// Brief settle so the join-driven reconcile quiesces before the workload.
	time.Sleep(1500 * time.Millisecond)
	return t, nil
}

// launchNode starts one shaled-slate process seeded at seedAddr ("" = founder),
// retrying a port-bind conflict with a fresh pair, registers it, and waits until
// its gRPC is reachable (Ping). The process inherits the env it needs (slate
// connection + DYLD_LIBRARY_PATH for the cgo cdylib).
func (t *realTopology) launchNode(seedAddr string) (*realNode, error) {
	t.mu.Lock()
	t.nextID++
	id := "real-n" + strconv.Itoa(t.nextID)
	desired := t.desiredCount
	t.mu.Unlock()

	const maxAttempts = 6
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		grpcPort, perr := freeTCPPortReal()
		if perr != nil {
			return nil, fmt.Errorf("launchNode %s: free grpc port: %w", id, perr)
		}
		bindPort, perr := freeTCPPortReal()
		if perr != nil {
			return nil, fmt.Errorf("launchNode %s: free bind port: %w", id, perr)
		}
		grpcAddr := "127.0.0.1:" + strconv.Itoa(grpcPort)
		bindAddr := "127.0.0.1:" + strconv.Itoa(bindPort)

		// Multi-backend mode: every node mounts the SAME (bucket, key-prefix) at the
		// SAME unit count and owns whichever GenUnits the ring leases it. NO per-node
		// --slate-db-name (factory-derived per-unit DbNames replace it); the shared
		// key-prefix is what makes the handoff copy-free across the process boundary.
		args := []string{
			"--node-id", id,
			"--unit-count", strconv.Itoa(desired),
			"--slate-key-prefix", t.cfg.keyPrefix,
			"--grpc-addr", grpcAddr,
			"--bind-addr", bindAddr,
		}
		if seedAddr != "" {
			args = append(args, "--seeds", seedAddr)
		}

		logPath := t.cfg.logDir + "/" + id + ".log"
		logf, err := os.Create(logPath)
		if err != nil {
			return nil, fmt.Errorf("launchNode %s: create log: %w", id, err)
		}

		cmd := exec.Command(t.cfg.binaryPath, args...)
		cmd.Stdout = logf
		cmd.Stderr = logf
		cmd.Env = append(os.Environ(),
			"DYLD_LIBRARY_PATH="+t.cfg.libDir,
			"SHALE_SLATE_BUCKET="+t.cfg.bucket,
			"SHALE_SLATE_ENDPOINT="+t.cfg.endpoint,
			"SHALE_SLATE_ACCESS_KEY="+t.cfg.accessKey,
			"SHALE_SLATE_SECRET_KEY="+t.cfg.secretKey,
			"SHALE_SLATE_REGION="+t.cfg.region,
			"SHALE_SLATE_USE_SSL="+strconv.FormatBool(t.cfg.useSSL),
		)
		// Put the child in its own process group so a SIGKILL/Wait reaps it
		// cleanly and a panic in the parent never strands an orphan.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		if err := cmd.Start(); err != nil {
			_ = logf.Close()
			return nil, fmt.Errorf("launchNode %s: start: %w", id, err)
		}

		n := &realNode{
			id:        id,
			grpcAddr:  grpcAddr,
			bindAddr:  bindAddr,
			cmd:       cmd,
			logPath:   logPath,
			unitCount: desired,
		}

		// Wait until the node's gRPC answers a Ping (process up + listener bound).
		// If the process exited early (e.g. a port we picked got taken between the
		// probe-release and the child's bind), retry with a fresh pair.
		if err := waitNodeReady(n, 20*time.Second); err != nil {
			_ = killProcess(cmd)
			_ = logf.Close()
			lastErr = err
			if exited(cmd) {
				continue // likely a bind race; retry
			}
			return nil, fmt.Errorf("launchNode %s: not ready: %w", id, err)
		}

		t.mu.Lock()
		t.nodes = append(t.nodes, n)
		t.mu.Unlock()
		return n, nil
	}
	return nil, fmt.Errorf("launchNode: %d attempts all failed: %w", maxAttempts, lastErr)
}

// waitNodeReady pings the node's gRPC until it answers or the deadline passes.
func waitNodeReady(n *realNode, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if exited(n.cmd) {
			return fmt.Errorf("process exited before becoming ready")
		}
		n.mu.Lock()
		c, err := n.clientLocked()
		n.mu.Unlock()
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			perr := c.Ping(ctx)
			cancel()
			if perr == nil {
				return nil
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	return fmt.Errorf("ping never succeeded within %s", timeout)
}

// liveNodes returns a snapshot of the currently-up nodes.
func (t *realTopology) liveNodes() []*realNode {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*realNode, 0, len(t.nodes))
	for _, n := range t.nodes {
		n.mu.Lock()
		down := n.down
		n.mu.Unlock()
		if !down {
			out = append(out, n)
		}
	}
	return out
}

// liveNodeIDs returns the ids of the live nodes (for victim selection / report).
func (t *realTopology) liveNodeIDs() []string {
	live := t.liveNodes()
	out := make([]string, len(live))
	for i, n := range live {
		out[i] = n.id
	}
	return out
}

// EntryNode returns a live node to issue an op through, chosen by idx modulo the
// live set so the workload routes both locally and forwarded. nil if none live.
func (t *realTopology) EntryNode(idx int) *realNode {
	live := t.liveNodes()
	if len(live) == 0 {
		return nil
	}
	return live[idx%len(live)]
}

// FounderClient returns a client to the (always-alive) founder, the stable entry
// the orchestrator uses for member counts + the read-after-kill durability check.
func (t *realTopology) FounderClient() (*rpc.Client, error) {
	t.founder.mu.Lock()
	defer t.founder.mu.Unlock()
	return t.founder.clientLocked()
}

// MemberCount returns the founder's view of the live member count via a Topology
// RPC. A single-node response (SingleNode=true) counts as 1.
func (t *realTopology) MemberCount() int {
	c, err := t.FounderClient()
	if err != nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.Topology(ctx)
	if err != nil {
		return 0
	}
	if resp.GetSingleNode() {
		return 1
	}
	if nodes := resp.GetNodes(); len(nodes) > 0 {
		return len(nodes)
	}
	if resp.GetNodeId() != "" {
		return 1
	}
	return 0
}

// waitConverged waits until the founder reports `want` members.
func (t *realTopology) waitConverged(want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last int
	for time.Now().Before(deadline) {
		last = t.MemberCount()
		if last == want {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("cluster did not converge to %d members (founder sees %d)", want, last)
}

// --- Topology mutations (chaos events) ----------------------------------

// AddNode launches a fresh shaled-slate process seeded at the founder and waits
// for the member count to converge. Returns the new node id.
func (t *realTopology) AddNode() (string, error) {
	t.structMu.Lock()
	defer t.structMu.Unlock()
	live := t.liveNodes()
	if len(live) == 0 {
		return "", fmt.Errorf("AddNode: no live node to seed off")
	}
	n, err := t.launchNode(t.founder.bindAddr)
	if err != nil {
		return "", err
	}
	want := len(live) + 1
	if err := t.waitConverged(want, 20*time.Second); err != nil {
		// Joined-but-not-yet-converged is not fatal to the orchestrator (the
		// scheduler re-reads the member count); surface it for the log.
		return n.id, fmt.Errorf("AddNode %s launched but convergence wait: %w", n.id, err)
	}
	return n.id, nil
}

// KillNode HARD-kills a node: SIGKILL the process group, reap it, mark it down.
// No graceful drain, no flush - this is the durability test. Refuses the founder
// (the stable anchor) and the last live node.
func (t *realTopology) KillNode(id string) error {
	t.structMu.Lock()
	defer t.structMu.Unlock()
	return t.teardown(id, false)
}

// RemoveNode gracefully stops a node: SIGTERM, which shaled.Run handles by a
// clean memberlist Leave + backend Close. Refuses the founder + the last node.
func (t *realTopology) RemoveNode(id string) error {
	t.structMu.Lock()
	defer t.structMu.Unlock()
	return t.teardown(id, true)
}

// teardown stops node id. graceful=true sends SIGTERM (clean Leave); graceful=
// false sends SIGKILL (a crash). Either way it is reaped + dropped from the live
// set + the survivors are awaited to converge. Refuses the founder and the last
// live node.
func (t *realTopology) teardown(id string, graceful bool) error {
	t.mu.Lock()
	var target *realNode
	liveCount := 0
	for _, n := range t.nodes {
		n.mu.Lock()
		down := n.down
		n.mu.Unlock()
		if !down {
			liveCount++
			if n.id == id {
				target = n
			}
		}
	}
	t.mu.Unlock()

	if target == nil {
		return fmt.Errorf("teardown: node %q not live", id)
	}
	if target == t.founder {
		return fmt.Errorf("teardown: refusing to stop the founder %q (the stable anchor)", id)
	}
	if liveCount <= 1 {
		return fmt.Errorf("teardown: refusing to stop the last live node %q", id)
	}

	target.closeClient()
	if graceful {
		_ = target.cmd.Process.Signal(syscall.SIGTERM)
		// Give the graceful drain a moment, then reap (hard-kill if it ignores).
		if !waitExit(target.cmd, 10*time.Second) {
			_ = killProcess(target.cmd)
		}
	} else {
		// HARD death.
		_ = killProcess(target.cmd)
	}
	_ = target.cmd.Wait()

	target.mu.Lock()
	target.down = true
	target.mu.Unlock()

	// Drop it from the slice (a future AddNode reuses the id space cleanly).
	t.mu.Lock()
	kept := t.nodes[:0]
	for _, n := range t.nodes {
		if n != target {
			kept = append(kept, n)
		}
	}
	t.nodes = kept
	t.mu.Unlock()

	want := liveCount - 1
	// Memberlist fail-detection of a hard-killed peer takes a few gossip
	// intervals; allow generously.
	_ = t.waitConverged(want, 20*time.Second)
	return nil
}

// Reshard is the real-cluster reshard seam, RE-POINTED to the DECLARATIVE
// trigger. The operator Reshard RPC it used to dial was DELETED: resharding is
// now driven from the --unit-count desired-state config rather than an imperative
// RPC (see docs/SPEC.md "Declarative resharding"). This seam reproduces the real
// redeploy flow that scales a running cluster:
//
//  1. Read the cluster's CURRENT count off GenState (the founder's live state).
//     That is `from`. The declarative target is the doubling `to = from*2`.
//  2. BUMP the desired --unit-count (the deploy config) to `to`, then RE-ROLL every
//     live node: restart each shaled-slate process with --unit-count = to, exactly
//     the rolling-deploy a `kubectl set env` + `kubectl rollout restart` performs.
//     Each re-rolled node rejoins via a live seed and advertises the new desired
//     count via its membership Meta; durable units stay in the shared bucket so a
//     restarted founder DERIVES the still-live count from durable state (it does
//     NOT re-init at the new desired) and joiners learn it from a seed - so no node
//     orphans the count-`from` data on restart.
//  3. POLL GenState until the coordinator's reconcile loop observes a STABLE
//     membership where ALL live members report the same desired count `to` > the
//     current count, and drives the freeze barrier to commit `from -> to`. The live
//     GenState count flipping to `to` is the commit signal.
//
// There is NO network reshard RPC: the only trigger is the deploy config, gated by
// the deploy pipeline (k8s RBAC), which is the whole security point of v0.8.
// Serialized behind structMu so a concurrent chaos event never re-rolls mid-reshard.
func (t *realTopology) Reshard() (from, to uint32, err error) {
	t.structMu.Lock()
	defer t.structMu.Unlock()

	cur, err := t.liveGenCount()
	if err != nil {
		return 0, 0, fmt.Errorf("declarative reshard: read current count: %w", err)
	}
	if cur < 2 || (cur&(cur-1)) != 0 {
		return 0, 0, fmt.Errorf("declarative reshard: current count %d is not a power of two > 1", cur)
	}
	target := cur * 2
	from, to = uint32(cur), uint32(target)

	// IDEMPOTENT RE-ROLL: if a prior call already bumped the desired count past the
	// live count (an earlier attempt re-rolled but its commit-poll timed out), do
	// NOT re-roll again - the nodes already carry the new --unit-count. Re-rolling a
	// second time only widens the disruption window. Just (re-)poll for the commit.
	t.mu.Lock()
	alreadyRolled := t.desiredCount == int(target)
	t.desiredCount = int(target)
	t.mu.Unlock()

	if !alreadyRolled {
		// Bump the deploy config + re-roll every live node to the new desired count
		// (the rolling redeploy). Roll the non-founder nodes first (each seeded at the
		// still-live founder), then the founder last, seeded at an already-re-rolled
		// peer, so a live seed always exists for the rejoin and the cluster never
		// drops below one member.
		if err := t.rollDeploy(int(target)); err != nil {
			return from, to, fmt.Errorf("declarative reshard: re-roll to --unit-count %d: %w", target, err)
		}
	}

	// The coordinator's reconcile fires only once membership is STABLE (debounced)
	// AND every live member reports the new desired count. Back-to-back re-rolls
	// keep re-arming that debounce, so let membership QUIESCE after the last roll
	// before polling for the commit - otherwise the very first poll deadline can
	// straddle the still-settling window. This mirrors a real deploy: the rollout
	// completes, then the cluster settles, then the controller reshards.
	t.quiesceMembership(30 * time.Second)
	// Final lease-settle: the founder's re-roll (the last one) triggered one more
	// unit redistribution. Give it a beat to land so the reshard BISECT scans
	// settled units, not a lease mid-move (the "detected newer DB client" fence).
	time.Sleep(8 * time.Second)

	// Poll GenState until the coordinator commits the doubling. The reconcile loop
	// flips the live count to `target` on its own once stable+unanimous. Generous
	// deadline: a real bisect copies each unit's keys through object storage.
	if err := t.waitGenCount(int(target), 120*time.Second); err != nil {
		return from, to, fmt.Errorf("declarative reshard: coordinator did not commit %d -> %d: %w", cur, target, err)
	}
	return from, to, nil
}

// quiesceMembership waits until the founder's live member count is steady across a
// short window (no joins/leaves), so the coordinator's membership-stable debounce
// can elapse and its declarative reconcile can fire. Bounded by timeout.
func (t *realTopology) quiesceMembership(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	prev, stableSince := -1, time.Now()
	for time.Now().Before(deadline) {
		cur := t.MemberCount()
		if cur != prev {
			prev = cur
			stableSince = time.Now()
		} else if cur > 0 && time.Since(stableSince) >= 4*time.Second {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// liveGenCount returns the cluster's current unit count via the founder's GenState
// RPC (the live {gen,count} every node converges to). The founder is the stable
// anchor, so this is the authoritative current count for the declarative target.
func (t *realTopology) liveGenCount() (int, error) {
	c, err := t.FounderClient()
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, count, err := c.GenState(ctx)
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

// waitGenCount polls every live node's GenState until ALL of them report the given
// unit count (the reshard has committed and propagated cluster-wide), or the
// deadline passes. Requiring agreement across all live nodes - not just the founder
// - confirms the barrier's FLIP reached every participant, not a straddle.
func (t *realTopology) waitGenCount(want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		live := t.liveNodes()
		all := len(live) > 0
		for _, n := range live {
			n.mu.Lock()
			c, cerr := n.clientLocked()
			n.mu.Unlock()
			if cerr != nil {
				all = false
				lastErr = cerr
				break
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, count, gerr := c.GenState(ctx)
			cancel()
			if gerr != nil {
				all = false
				lastErr = gerr
				break
			}
			if int(count) != want {
				all = false
				lastErr = fmt.Errorf("node %s at count %d, want %d", n.id, count, want)
				break
			}
		}
		if all {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no live nodes")
	}
	return fmt.Errorf("not all live nodes reached count %d within %s (last: %v)", want, timeout, lastErr)
}

// rollDeploy re-rolls every live node onto the new desired unit count, one at a
// time, the way a rolling deploy restarts pods. Non-founder nodes roll first (each
// re-launched seeded at the founder); the founder rolls LAST, seeded at an
// already-re-rolled peer so a live, already-bumped seed exists for its rejoin. Each
// re-rolled node carries the new --unit-count (read from t.desiredCount in
// launchNode), so after the roll every live member advertises desired == target and
// the coordinator's reconcile loop can fire. Caller holds structMu.
func (t *realTopology) rollDeploy(target int) error {
	t.mu.Lock()
	order := append([]*realNode(nil), t.nodes...)
	founder := t.founder
	t.mu.Unlock()

	// Roll non-founder nodes first, founder last.
	rollOne := func(n *realNode, seed string) error {
		newNode, err := t.relaunchNode(n, seed)
		if err != nil {
			return err
		}
		if n == founder {
			t.mu.Lock()
			t.founder = newNode
			t.mu.Unlock()
		}
		// Let the rejoin + reconcile settle before rolling the next node, so the
		// membership-stable debounce the coordinator waits on is not perpetually
		// reset by back-to-back restarts.
		if err := t.waitConverged(len(order), 25*time.Second); err != nil {
			return fmt.Errorf("post-reroll convergence: %w", err)
		}
		// LEASE-SETTLE pause. Each restart triggers a unit-lease redistribution
		// (the Phase 3 copy-free handoff): the rejoined node re-acquires its units
		// at a higher epoch, fencing the prior holder. Until that quiesces, a unit's
		// lease is MOVING, and a reshard BISECT that scans a unit mid-move is fenced
		// ("detected newer DB client"). Rolling the next node immediately would pile
		// a fresh redistribution on top, so the leases never settle and the eventual
		// reshard keeps getting fenced. Pause so each node's redistribution lands
		// before the next restart - the real deploy's per-pod readiness gate.
		time.Sleep(6 * time.Second)
		return nil
	}

	for _, n := range order {
		if n == founder {
			continue
		}
		if err := rollOne(n, founder.bindAddr); err != nil {
			return err
		}
	}
	// Founder last: seed it at any other live node (already re-rolled).
	if len(order) > 1 {
		live := t.liveNodes()
		var seed string
		for _, ln := range live {
			if ln != founder {
				seed = ln.bindAddr
				break
			}
		}
		if seed == "" {
			return fmt.Errorf("rollDeploy: no live peer to seed the founder's re-roll")
		}
		if err := rollOne(founder, seed); err != nil {
			return err
		}
	}
	return nil
}

// relaunchNode stops node n (graceful: a clean Leave + slatedb flush, the way a
// deploy drains a pod before replacing it) and launches a replacement seeded at
// seedAddr, carrying the current t.desiredCount. The durable units stay in the
// shared bucket, so the replacement re-OpenUnits the same data (copy-free); it does
// NOT start fresh. Returns the replacement node. The old node is dropped from the
// slice and the new one appended. Caller holds structMu.
func (t *realTopology) relaunchNode(n *realNode, seedAddr string) (*realNode, error) {
	// Graceful stop (SIGTERM): clean Leave + backend Close so slatedb flushes.
	n.closeClient()
	_ = n.cmd.Process.Signal(syscall.SIGTERM)
	if !waitExit(n.cmd, 10*time.Second) {
		_ = killProcess(n.cmd)
	}
	_ = n.cmd.Wait()
	n.mu.Lock()
	n.down = true
	n.mu.Unlock()

	// Drop the stopped node from the live set.
	t.mu.Lock()
	kept := t.nodes[:0]
	for _, x := range t.nodes {
		if x != n {
			kept = append(kept, x)
		}
	}
	t.nodes = kept
	t.mu.Unlock()

	// Launch the replacement (it reads the bumped t.desiredCount in launchNode).
	return t.launchNode(seedAddr)
}

// CloseAll tears down every node (end of run / setup failure). Idempotent.
func (t *realTopology) CloseAll() {
	t.mu.Lock()
	nodes := append([]*realNode(nil), t.nodes...)
	t.mu.Unlock()
	for _, n := range nodes {
		n.mu.Lock()
		down := n.down
		n.mu.Unlock()
		if down {
			continue
		}
		n.closeClient()
		// Graceful first, then a hard reap so a clean shutdown flushes slatedb.
		_ = n.cmd.Process.Signal(syscall.SIGTERM)
		if !waitExit(n.cmd, 8*time.Second) {
			_ = killProcess(n.cmd)
		}
		_ = n.cmd.Wait()
		n.mu.Lock()
		n.down = true
		n.mu.Unlock()
	}
}

// --- small helpers --------------------------------------------------------

// freeTCPPortReal grabs an OS-assigned ephemeral port and releases it (the child
// binds it for real; the retry loop in launchNode heals the release-rebind race).
func freeTCPPortReal() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port, nil
}

// exited reports whether the process has already terminated (non-blocking).
func exited(cmd *exec.Cmd) bool {
	if cmd.ProcessState != nil {
		return true
	}
	// Probe with signal 0: an ESRCH means it is gone.
	if cmd.Process == nil {
		return true
	}
	err := cmd.Process.Signal(syscall.Signal(0))
	return err != nil
}

// killProcess SIGKILLs the whole process group (negative pid) so no child of the
// shaled process is stranded, falling back to the bare pid.
func killProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	pid := cmd.Process.Pid
	if err := syscall.Kill(-pid, syscall.SIGKILL); err == nil {
		return nil
	}
	return cmd.Process.Kill()
}

// waitExit waits up to timeout for the process to exit, returning true if it did.
// Non-destructive (does not Wait/reap; the caller does that).
func waitExit(cmd *exec.Cmd, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if exited(cmd) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
