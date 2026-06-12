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
//	Reshard     = dial the founder + issue the operator Reshard RPC, driving a real
//	              cluster-coordinated doubling (N -> 2N) over the process boundary.
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

// reshardRPCTimeout bounds the operator Reshard RPC. A real cluster-coordinated
// doubling copies every unit's keys through object storage (each new gen-(g+1)
// unit is a fresh slatedb LSM filled by a scan of the old unit), so it is far
// slower than the in-process bisect - the deadline is generous accordingly.
const reshardRPCTimeout = 180 * time.Second

// realNode is one shaled-slate child process: its node id, the OS process, the
// ports it bound, a pooled gRPC client to its own gRPC address, and a down flag
// (true after a hard kill / graceful stop, until a fresh process is launched). In
// multi-backend mode there is no per-node DbName: every node shares the one bucket
// + key-prefix and owns whichever GenUnits the ring leases to it.
type realNode struct {
	id       string
	grpcAddr string
	bindAddr string
	cmd      *exec.Cmd
	logPath  string

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

	mu       sync.Mutex
	nodes    []*realNode
	nextID   int
	founder  *realNode // first-launched node; the stable seed + entry anchor
	structMu sync.Mutex
}

// newRealTopology launches `count` shaled-slate processes in MULTI-BACKEND mode
// against the shared bucket + shared key-prefix and waits for the cluster to
// converge to `count` members (observed via a Topology RPC to the founder). Every
// node mounts the SAME (bucket, key-prefix) namespace and owns whichever GenUnits
// the ring leases to it; per-unit single-writer fencing is what keeps two nodes
// off the same unit, NOT a per-node DbName. Returns a live topology or an error
// (after cleaning up).
func newRealTopology(cfg realClusterConfig, count int) (*realTopology, error) {
	t := &realTopology{cfg: cfg}
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
			"--unit-count", strconv.Itoa(t.cfg.unitCount),
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
			id:       id,
			grpcAddr: grpcAddr,
			bindAddr: bindAddr,
			cmd:      cmd,
			logPath:  logPath,
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

// Reshard drives a real cluster-coordinated doubling (N -> 2N) by dialing the
// founder and issuing the OPERATOR Reshard RPC - the exact surface `shale reshard`
// uses. It returns the {from, to} unit counts on a committed doubling, or an error
// if the RPC failed (transport) or the cluster refused it (a non-empty response
// error: in-flight, ceiling, or membership-unstable). A refused reshard leaves the
// generation unchanged, so the caller may retry. This is the deployable v0.8
// reshard end to end: the bisect copies each unit's keys through object storage,
// the cluster-wide freeze barrier coordinates the doubling, and acked writes
// survive (the oracle asserts it).
func (t *realTopology) Reshard() (from, to uint32, err error) {
	c, err := t.FounderClient()
	if err != nil {
		return 0, 0, fmt.Errorf("real-cluster Reshard: founder client: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), reshardRPCTimeout)
	defer cancel()
	resp, err := c.Reshard(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("real-cluster Reshard: RPC: %w", err)
	}
	if e := resp.GetError(); e != "" {
		return resp.GetFromUnitCount(), resp.GetToUnitCount(), fmt.Errorf("real-cluster Reshard: refused: %s", e)
	}
	return resp.GetFromUnitCount(), resp.GetToUnitCount(), nil
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
