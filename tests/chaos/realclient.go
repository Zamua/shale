//go:build chaosreal

package chaos

// realClient is the ClusterClient over the REAL gRPC surface (pkg/rpc.Client).
// The workload driver in chaos_real_test.go issues every Put/Get/Delete through
// it, retrying the same retryable signals the in-process harness does (the
// acquiring-window / cutover Unavailable + FailedPrecondition, and the
// entry-node-gone case where the chosen entry process was just SIGKILLed). A nil
// return is a genuine ACK that the oracle records; a retryable error is retried,
// never acked. This is the load-bearing rule that makes "no acked write lost" a
// falsifiable claim over a real network.
//
// It is deliberately a thin reimplementation against rpc.Client rather than
// against *cluster.Cluster (the in-process harness's surface): the whole point of
// the real-cluster adapter is that the ops cross a process boundary over gRPC.

import (
	"context"
	"errors"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// realRetryBudget bounds how long a single op retries the retryable signals before
// being reported a hard failure. Generous: a real cross-process failover (memberlist
// fail-detect + ring refresh) is slower than the in-process handoff.
const realRetryBudget = 30 * time.Second

// realRetryDelay is the per-attempt backoff while retrying a retryable error.
const realRetryDelay = 30 * time.Millisecond

// realOpTimeout bounds a single gRPC call so a blackholed peer (a node mid-kill
// whose TCP is half-open) cancels at the deadline instead of blocking the whole op.
const realOpTimeout = 3 * time.Second

// isRetryableReal reports whether err is a transient cutover/handoff/freeze/
// rebalance signal the client retries (NOT an ack-and-lose):
//
//   - codes.Unavailable      the acquiring window + the cluster-wide freeze refusal.
//   - codes.FailedPrecondition the reshard-cutover re-pin / ownership-disagreement.
//   - codes.ResourceExhausted the rebalance migration guard: "key is migrating out;
//     retry after Nms" - a key whose partition is mid-handoff out of this owner.
//     This is the canonical client-retry signal the rebalance path emits (see the
//     RebalanceRetryAfterMs hint); a faithful client MUST retry it, not treat it as
//     a loss. Mirrors the in-process harness, whose in-process Put rides this out.
func isRetryableReal(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.FailedPrecondition, codes.ResourceExhausted:
		return true
	}
	return false
}

// isEntryGoneReal reports whether err is the symptom of issuing an op through a
// process we just SIGKILLed (dial failure / connection refused / transport
// closing). The driver responds by re-selecting a live entry node, never by
// acking-then-losing.
func isEntryGoneReal(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.Canceled, codes.DeadlineExceeded:
		// A dial failure to a dead peer surfaces as Unavailable; a hung half-open
		// peer trips the per-op deadline. Both mean "this entry is gone, retry
		// elsewhere", which is the same retry treatment as a genuine cutover.
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "transport is closing") ||
		strings.Contains(msg, "the cluster is closed") ||
		strings.Contains(msg, "closed")
}

// putAckedReal issues a Put through entrySel (re-evaluated each attempt so a
// SIGKILLed entry node is replaced), retrying the retryable signals up to budget.
// Returns nil on a real ack. Records a retry metric per retry.
func putAckedReal(entrySel func() *realNode, met *realMetrics, key, value string) error {
	deadline := time.Now().Add(realRetryBudget)
	for {
		n := entrySel()
		if n == nil {
			if time.Now().After(deadline) {
				return errors.New("putAckedReal: no live entry node before deadline")
			}
			time.Sleep(realRetryDelay)
			continue
		}
		err := nodePut(n, key, value)
		if err == nil {
			return nil
		}
		if (isRetryableReal(err) || isEntryGoneReal(err)) && time.Now().Before(deadline) {
			met.retryableRetries.Add(1)
			time.Sleep(realRetryDelay)
			continue
		}
		return err
	}
}

// deleteAckedReal is the Delete analogue of putAckedReal.
func deleteAckedReal(entrySel func() *realNode, met *realMetrics, key string) error {
	deadline := time.Now().Add(realRetryBudget)
	for {
		n := entrySel()
		if n == nil {
			if time.Now().After(deadline) {
				return errors.New("deleteAckedReal: no live entry node before deadline")
			}
			time.Sleep(realRetryDelay)
			continue
		}
		err := nodeDelete(n, key)
		if err == nil {
			return nil
		}
		if (isRetryableReal(err) || isEntryGoneReal(err)) && time.Now().Before(deadline) {
			met.retryableRetries.Add(1)
			time.Sleep(realRetryDelay)
			continue
		}
		return err
	}
}

// getObservedReal issues a Get through entrySel, retrying the retryable signals,
// and returns the settled Observed (value or not-found). Uses realRetryBudget.
func getObservedReal(entrySel func() *realNode, met *realMetrics, key string) (Observed, error) {
	return getObservedBudgetReal(entrySel, met, key, realRetryBudget)
}

// getObservedBudgetReal is getObservedReal with an explicit per-read retry budget.
func getObservedBudgetReal(entrySel func() *realNode, met *realMetrics, key string, budget time.Duration) (Observed, error) {
	deadline := time.Now().Add(budget)
	for {
		n := entrySel()
		if n == nil {
			if time.Now().After(deadline) {
				return Observed{}, errors.New("getObservedReal: no live entry node")
			}
			time.Sleep(realRetryDelay)
			continue
		}
		val, found, err := nodeGet(n, key)
		if err == nil {
			if !found {
				return Observed{NotFound: true}, nil
			}
			return Observed{Value: string(val)}, nil
		}
		if (isRetryableReal(err) || isEntryGoneReal(err)) && time.Now().Before(deadline) {
			met.retryableRetries.Add(1)
			time.Sleep(realRetryDelay)
			continue
		}
		return Observed{}, err
	}
}

// nodePut/nodeGet/nodeDelete are the one-shot gRPC calls against a node's client,
// each with a bounded per-op deadline. They lock the node mu only to fetch the
// (lazily-dialed) client, never across the RPC, so a concurrent killer that closes
// the client does not block on an in-flight call.
func nodePut(n *realNode, key, value string) error {
	n.mu.Lock()
	c, err := n.clientLocked()
	n.mu.Unlock()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), realOpTimeout)
	defer cancel()
	return c.Put(ctx, []byte(key), []byte(value))
}

func nodeGet(n *realNode, key string) ([]byte, bool, error) {
	n.mu.Lock()
	c, err := n.clientLocked()
	n.mu.Unlock()
	if err != nil {
		return nil, false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), realOpTimeout)
	defer cancel()
	return c.Get(ctx, []byte(key))
}

func nodeDelete(n *realNode, key string) error {
	n.mu.Lock()
	c, err := n.clientLocked()
	n.mu.Unlock()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), realOpTimeout)
	defer cancel()
	return c.Delete(ctx, []byte(key))
}
