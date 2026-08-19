// Package clustertest holds the ONE canonical set of multi-node test
// harness helpers shared by every test binary that stands up real
// Cluster instances over loopback (the cluster package's external tests
// and the integration tree).
//
// Why a shared package: both trees need the same primitives - build a
// coordinator at the shared test cadence, wait for the ring to
// converge, wait until a freshly-joined cluster is genuinely
// write-ready, and classify the transient gRPC codes a warming cluster
// emits. Kept as hand-copied pairs those definitions drift; routing
// both trees through this package makes that drift impossible, exactly
// as internal/goleakignore does for the goleak ignore lists.
//
// The helpers depend only on the cluster package's PUBLIC surface, the
// same surface the external tests use, so there is no import cycle.
package clustertest

import (
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/testport"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/coord/cas"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FreePort returns an OS-assigned ephemeral port free on both TCP and UDP.
// The implementation lives in internal/testport so the test packages that
// need it cannot drift. Probing cannot RESERVE the port: a caller that binds
// the result accepts the release-then-rebind race.
func FreePort(t *testing.T) int {
	t.Helper()
	return testport.Free(t)
}

// HostPort renders a loopback bind address for port.
func HostPort(port int) string {
	return "127.0.0.1:" + strconv.Itoa(port)
}

// Shared CAS-coordinator test cadence: the same ratios as the production
// defaults (renewal 2s / poll 1s / 5 expiry polls), shrunk so a silent death
// is detected in ExpiryPolls*poll = 250ms of observed silence against a
// 100ms renewal - the same 2.5x margin.
const (
	CASTestRenewal     = 100 * time.Millisecond
	CASTestPoll        = 50 * time.Millisecond
	CASTestExpiryPolls = 5
)

// CASCoordinator returns an unstarted CAS coordinator over store at the
// shared test cadence, with quiet logs. Every node of one in-process test
// cluster passes the SAME store instance: the store IS the discovery
// mechanism, so the first node to Open founds the cluster and every later
// one joins by CAS-appending its row - no bind address, no seed list.
func CASCoordinator(store storageunit.ConditionalStore) *cas.Coordinator {
	return cas.New(cas.Config{
		Store:           store,
		RenewalInterval: CASTestRenewal,
		PollInterval:    CASTestPoll,
		ExpiryPolls:     CASTestExpiryPolls,
		LogOutput:       io.Discard,
	})
}

// OpenClusterCAS wires a CASCoordinator over store into cfg and opens the
// Cluster, failing the test on any Open error.
func OpenClusterCAS(t *testing.T, cfg cluster.Config, store storageunit.ConditionalStore) *cluster.Cluster {
	t.Helper()
	cfg.Coordinator = CASCoordinator(store)
	c, err := cluster.Open(cfg)
	if err != nil {
		t.Fatalf("OpenClusterCAS %s: cluster.Open: %v", cfg.NodeID, err)
	}
	return c
}

// WaitForRingSize polls c.Members() until it has want entries or the
// timeout expires.
func WaitForRingSize(c *cluster.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if converged(c, want) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("convergence: members=%v placement=%v, want %d of each",
		c.Members(), c.PlacementMembers(), want)
}

// converged reports whether BOTH member bases hold exactly want members: the
// membership VIEW (where a join/leave lands first) and the PLACEMENT basis
// (the ring routing is computed over; an adapter may let it trail the view).
// Readiness must gate on both - the view alone is satisfied before the ring
// has absorbed the change, so a test that proceeds on it races the takeover
// its "converged" cluster has not started (post-leave, WaitForRebalanceIdle
// reads vacuously idle in that window because the un-evicted ring never
// scheduled a reconcile).
func converged(c *cluster.Cluster, want int) bool {
	return len(c.Members()) == want && len(c.PlacementMembers()) == want
}

// WaitForMembersAll polls every cluster in cs until they each report
// want members. Convergence rides each node's own poll cadence - a
// fresh join can reach the seed before reaching the other peers - so
// any multi-node assertion must wait for every node to agree on the
// ring before issuing routed ops.
func WaitForMembersAll(cs []*cluster.Cluster, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allOK := true
		for _, c := range cs {
			if !converged(c, want) {
				allOK = false
				break
			}
		}
		if allOK {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	sizes := make([]int, len(cs))
	for i, c := range cs {
		sizes[i] = len(c.Members())
	}
	return fmt.Errorf("not all nodes converged to %d members: sizes=%v", want, sizes)
}

// WaitForWriteReady gates a freshly-stood-up multi-node cluster on the
// one condition the membership + rebalance-idle waits do NOT cover: that
// the routed + replicated write path actually round-trips at the
// cluster's configured WriteConsistency, from EVERY node, for keys that
// land on EVERY owner. Ring convergence proves every node is a MEMBER; it
// does NOT prove (a) every peer's gRPC HTTP/2 server goroutine is
// servicing connections yet, nor (b) that every node has closed the
// StateReceiving window its bootstrap rebalance opened. Either gap makes
// a fan-out leg come back transient (a peer's server not accepting yet,
// or a local-self leg hitting the migration guard), and a transient leg
// counts as NEITHER ack NOR failure - so a WriteAll R3 lands 2 acks, 0
// failures and the test's seed Put fails "write needed 3 acks, got 2
// (0 failures: <nil>)".
//
// The single-node, single-key probe the prior fix used only warmed ONE
// (origin-node, replica-set) pair, ONCE. Two gaps remained that this gate
// closes:
//
//  1. SPREAD across nodes + keys. The tests fire their seed Put through
//     DIFFERENT nodes (nodes[0]/nodes[1]/nodes[2]) and against many keys,
//     so a node whose receiving window was still open - or whose server
//     lagged - for a DIFFERENT key range was never proven ready. This
//     gate probes a spread of keys (enough that, under consistent
//     hashing, every member is the local-self leg for at least one)
//     THROUGH EVERY node.
//  2. STABILITY across time. The rebalance-idle wait can return idle,
//     then a LATE-arriving join observation (a poll landing under load)
//     fires a fresh debounced reconcile that reopens a StateReceiving
//     window MID-TEST - so a write partway through a test fails even
//     though the first writes succeeded. A single passing probe does not
//     prove the ring will stay stable. This gate therefore requires a
//     full CLEAN SWEEP: every (node, key) probe must succeed within ONE
//     uninterrupted pass. Any transient anywhere in a pass restarts the
//     whole pass. Completing a clean sweep proves the ring held stable
//     (no reopened receiving window, every server accepting) for the full
//     duration of that pass - the deterministic stand-in for "the late
//     membership observations have drained."
//
// It retries ONLY transient warmup errors (Unavailable: refused dial /
// not enough acks yet; ResourceExhausted: a partition still mid-
// migration) until a clean sweep completes or the bounded deadline. A
// non-transient error, or exhausting the deadline, fails the test loudly.
//
// CRUCIAL: this retry is scoped to SETUP. Once it returns the cluster is
// proven write-ready and any Unavailable a test then sees in its body is
// a REAL failure - this helper never wraps those. The probe keys are
// namespaced + tombstoned afterward so they cannot pollute a test that
// reads replica backends directly.
func WaitForWriteReady(t *testing.T, clusters []*cluster.Cluster, deadline time.Duration) {
	t.Helper()
	// 24 distinct keys give consistent hashing ample spread to cover
	// every owner as a local-self leg across a small cluster, without
	// making the warmup expensive.
	const probeKeys = 24
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		sweepClean := true
		for from, c := range clusters {
			for k := range probeKeys {
				probeKey := fmt.Appendf(nil, "__warmup_probe__/%d/%d", from, k)
				err := c.Put(probeKey, []byte("ready"))
				if err == nil {
					// Best-effort cleanup so no stray probe key lands on
					// the replica backends a test might read directly.
					_ = c.Delete(probeKey)
					continue
				}
				if !IsTransientWarmupErr(err) {
					t.Fatalf("WaitForWriteReady: probe write through node %d returned a non-transient error (real failure, not a warmup race): %v", from, err)
				}
				// A transient anywhere taints this sweep: the ring is not
				// stably write-ready yet. Back off and restart the sweep so
				// readiness means "a full clean pass with no reopened
				// receiving window," not "one lucky probe."
				lastErr = err
				sweepClean = false
				time.Sleep(50 * time.Millisecond)
				break
			}
			if !sweepClean {
				break
			}
		}
		if sweepClean {
			return
		}
	}
	t.Fatalf("WaitForWriteReady: cluster never completed a clean write-readiness sweep within %s; last probe error: %v", deadline, lastErr)
}

// IsTransientWarmupErr reports whether err is one of the transient
// signals expected while a freshly-joined cluster's gRPC forwarding
// servers come up: Unavailable (peer's server not accepting yet /
// connection refused / not enough acks landed) or ResourceExhausted (a
// partition is still mid-migration). Only these are retried during the
// setup warmup window; everything else is a real failure.
func IsTransientWarmupErr(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// PutWithTransientRetry wraps Put with a bounded retry on the v0.4
// transient codes: ResourceExhausted from the migration-window write
// rejection (docs/SPEC.md "Cutover") and FailedPrecondition from the
// forwarding loop-guard (docs/SPEC.md "Failure handling"). Per
// docs/SPEC.md the SDK is expected to do this; tests do too so an
// assertion checks "the write eventually succeeds" rather than "no
// transient rejection during bootstrap." Any other error surfaces
// immediately.
//
// Unavailable is intentionally NOT retried: it is reserved for genuine
// peer-down failures in v0.4 (so the fanout's failure budget can
// short-circuit on dead nodes). Migration-guard rejections moved to
// ResourceExhausted so the fanout can distinguish "this replica is
// mid-handoff" (skip) from "this replica is dead" (count as failure).
//
// attempts + backoff are caller-supplied because the two test trees
// budget the retry window differently; the CLASSIFICATION of which
// codes are transient is the part that must never drift, and that
// lives here.
func PutWithTransientRetry(c *cluster.Cluster, key, value []byte, attempts int, backoff time.Duration) error {
	var lastErr error
	for range attempts {
		err := c.Put(key, value)
		if err == nil {
			return nil
		}
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.ResourceExhausted || st.Code() == codes.FailedPrecondition {
				lastErr = err
				time.Sleep(backoff)
				continue
			}
		}
		return err
	}
	return lastErr
}
