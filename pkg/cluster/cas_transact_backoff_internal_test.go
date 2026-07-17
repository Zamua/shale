package cluster

// White-box tests for the retryable-status branch of Transact: the loop that
// rides out a transient codes.Unavailable / codes.FailedPrecondition commit.
// The retryable statuses this loop handles are outcome (d) only - a PRE-commit
// refusal that applied nothing (a cluster-wide freeze, a lease mid-handoff, a
// reshard-cutover re-pin, a recoded graceful-leave fence). A committed-but-
// under-replicated commit (outcome (c)) is classified as success at the source
// and NEVER reaches this loop, so these tests exercise the (d) re-run path.
// The loop must still be polite: exponential backoff with full jitter (base
// casBaseBackoff, x2 per consecutive retryable failure, capped at
// transactRetryableBackoffCap, sleep uniform in (0, current-backoff]) and a
// hard cap of transactRetryableMaxRuns re-runs ending in
// ErrTransactRetriesExhausted (which still carries the retryable gRPC status
// through the wrap). See docs/SPEC.md "The four commit outcomes".
//
// The tests inject the commit outcome via the transactCommit seam and capture
// the requested sleeps via the transactSleep seam, so no multi-node cluster
// (and no real sleeping) is needed. The seam returns a raw error to the loop,
// standing in for a (d) pre-commit refusal.

import (
	"errors"
	"testing"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/backend/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newBackoffTestCluster returns a single-node in-package cluster (memory
// backend) for driving Transact, registered for cleanup.
func newBackoffTestCluster(t *testing.T) *Cluster {
	t.Helper()
	c, err := Open(Config{NodeID: "n1", Backend: memory.New()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// setTransactSeams swaps the commit + sleep seams and the retryable-loop
// tunables for one test, restoring them on cleanup. commit is invoked in
// place of tx.Commit; sleeps are captured into the returned slice pointer.
func setTransactSeams(t *testing.T, commit func() error) *[]time.Duration {
	t.Helper()
	var sleeps []time.Duration
	prevCommit, prevSleep := transactCommit, transactSleep
	prevBase, prevCap := casBaseBackoff, transactRetryableBackoffCap
	prevRuns, prevTO := transactRetryableMaxRuns, transactUnavailableTimeout
	transactCommit = func(_ *clusterTx) error { return commit() }
	transactSleep = func(d time.Duration) { sleeps = append(sleeps, d) }
	casBaseBackoff = 2 * time.Millisecond
	transactRetryableBackoffCap = 500 * time.Millisecond
	// The sleeper is stubbed, so the loop reaches the re-run cap in
	// microseconds of wall clock; the deadline stays a distant outer bound
	// exactly as in production. Kept short enough that a REGRESSION to the
	// old uncapped loop (which spins until the deadline) fails fast instead
	// of hanging the suite for the production 30s.
	transactUnavailableTimeout = 2 * time.Second
	t.Cleanup(func() {
		transactCommit, transactSleep = prevCommit, prevSleep
		casBaseBackoff, transactRetryableBackoffCap = prevBase, prevCap
		transactRetryableMaxRuns, transactUnavailableTimeout = prevRuns, prevTO
	})
	return &sleeps
}

// TestTransact_RetryableBackoff_GrowsExponentiallyToCapWithFullJitter pins
// the backoff shape: the i-th consecutive retryable failure sleeps a uniform
// random duration in (0, min(casBaseBackoff<<i, transactRetryableBackoffCap)],
// i.e. the per-retry envelope doubles from the base up to the cap, and every
// sample is full-jitter within it. A flat 2-4ms backoff (the old behavior)
// violates the growth assertion; a jitterless or over-cap backoff violates
// the envelope assertion.
func TestTransact_RetryableBackoff_GrowsExponentiallyToCapWithFullJitter(t *testing.T) {
	c := newBackoffTestCluster(t)
	commits := 0
	sleeps := setTransactSeams(t, func() error {
		commits++
		return status.Error(codes.Unavailable, "cluster write-frozen for reshard")
	})

	err := c.Transact([]byte("k"), func(_ backend.Transaction) error { return nil })
	if err == nil {
		t.Fatalf("an always-retryable commit must not report success")
	}

	if len(*sleeps) != transactRetryableMaxRuns {
		t.Fatalf("want exactly %d backoff sleeps (one per retryable re-run), got %d (commits %d)",
			transactRetryableMaxRuns, len(*sleeps), commits)
	}
	maxSeen := time.Duration(0)
	for i, d := range *sleeps {
		envelope := casBaseBackoff << i
		if envelope > transactRetryableBackoffCap || envelope <= 0 {
			envelope = transactRetryableBackoffCap
		}
		if d <= 0 {
			t.Fatalf("sleep %d: full jitter must stay in (0, envelope]; got %v", i, d)
		}
		if d > envelope {
			t.Fatalf("sleep %d: %v exceeds the exponential envelope %v (base %v, cap %v)",
				i, d, envelope, casBaseBackoff, transactRetryableBackoffCap)
		}
		if d > maxSeen {
			maxSeen = d
		}
	}
	// Growth actually happened: a flat backoff at the base can never exceed
	// the base envelope. With 16+ samples drawn uniformly from envelopes of
	// 256-500ms, all landing at or under the 2ms base is a ~1e-30 event, so
	// this is deterministic in practice.
	if maxSeen <= casBaseBackoff {
		t.Fatalf("backoff never grew past the base %v (max sample %v); expected exponential growth toward the %v cap",
			casBaseBackoff, maxSeen, transactRetryableBackoffCap)
	}
}

// TestTransact_RetryableRerunCap_TerminalError pins the re-run cap: exactly
// transactRetryableMaxRuns re-runs (so cap+1 commits in total), then Transact
// gives up with ErrTransactRetriesExhausted WRAPPING the last commit error.
// The wrap must keep the retryable gRPC status resolvable (compat with
// callers classifying by code) and must NOT read as a CAS conflict.
func TestTransact_RetryableRerunCap_TerminalError(t *testing.T) {
	c := newBackoffTestCluster(t)
	commits := 0
	setTransactSeams(t, func() error {
		commits++
		return status.Error(codes.Unavailable, "cluster write-frozen for reshard")
	})
	transactRetryableMaxRuns = 5

	err := c.Transact([]byte("k"), func(_ backend.Transaction) error { return nil })

	if wantCommits := transactRetryableMaxRuns + 1; commits != wantCommits {
		t.Fatalf("want exactly %d commits (initial + %d re-runs), got %d",
			wantCommits, transactRetryableMaxRuns, commits)
	}
	if !errors.Is(err, ErrTransactRetriesExhausted) {
		t.Fatalf("want ErrTransactRetriesExhausted after the re-run cap, got %v", err)
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("the terminal error must still carry codes.Unavailable through the wrap; got ok=%v code=%v (%v)",
			ok, st.Code(), err)
	}
	if errors.Is(err, backend.ErrCASConflict) {
		t.Fatalf("retryable exhaustion must not surface as a CAS conflict: %v", err)
	}
}

// TestTransact_RetryableSuccessAfterK_ConsumesNoConflictBudget pins the
// existing semantics the mitigation must preserve: a commit that fails
// retryably K times then succeeds still returns nil, and the retryable
// re-runs consume NO conflict-budget attempts (CASMaxAttempts is shrunk to 1,
// so any budget leakage would abort before the success).
func TestTransact_RetryableSuccessAfterK_ConsumesNoConflictBudget(t *testing.T) {
	c := newBackoffTestCluster(t)
	const k = 3
	commits, fnRuns := 0, 0
	setTransactSeams(t, func() error {
		commits++
		if commits <= k {
			return status.Error(codes.Unavailable, "lease mid-handoff")
		}
		return nil
	})
	prevMax := CASMaxAttempts
	CASMaxAttempts = 1
	t.Cleanup(func() { CASMaxAttempts = prevMax })

	err := c.Transact([]byte("k"), func(_ backend.Transaction) error {
		fnRuns++
		return nil
	})
	if err != nil {
		t.Fatalf("success after %d retryable failures must return nil, got %v", k, err)
	}
	if commits != k+1 || fnRuns != k+1 {
		t.Fatalf("want %d commits and %d fn runs (initial + %d re-runs), got commits=%d fnRuns=%d",
			k+1, k+1, k, commits, fnRuns)
	}
}
