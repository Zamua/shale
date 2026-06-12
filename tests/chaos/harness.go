//go:build chaos

package chaos

// The workload driver + metrics. M writer goroutines hammer acked Put/Delete/
// Transact through the routed cluster surface while N reader goroutines verify
// every read against the oracle, all concurrently with the chaos scheduler. The
// driver is ORCHESTRATION: it talks to the cluster only through *cluster.Cluster
// (the application surface) and to the model only through *Oracle (the domain),
// so the same loops graduate to a real deploy by swapping the adapter.
//
// The single load-bearing rule: a write is recorded in the oracle ONLY after the
// cluster returned success. A retryable error (Unavailable / FailedPrecondition /
// the freeze refusal) is retried, never acked. That is what makes "no acked write
// lost" a meaningful, falsifiable claim.

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// metrics is the accounting reported at the end. All counters are atomic so the
// writer/reader goroutines update them lock-free; the chaos scheduler updates the
// event counters under its own lock and folds them in at report time.
type metrics struct {
	ackedPuts        atomic.Int64
	ackedDeletes     atomic.Int64
	ackedTransacts   atomic.Int64
	readsVerified    atomic.Int64
	retryableRetries atomic.Int64
	// transientReads counts continuous-reader reads that returned a non-PASS value
	// on first read but converged to the correct value on re-verify (an expected
	// mid-cutover artifact, NOT a violation). A high count is informational: it
	// quantifies how much cutover turbulence the readers actually saw.
	transientReads atomic.Int64

	// chaos event counts, set by the scheduler.
	evKill        atomic.Int64
	evRestart     atomic.Int64
	evJoin        atomic.Int64
	evLeave       atomic.Int64
	evReshard     atomic.Int64
	evReshardAbrt atomic.Int64

	// COMBINATION event counts, tracked DISTINCTLY from the base counters above so
	// a passing report can PROVE the hard adversarial cases actually fired (they
	// are the high-value scenarios the single gates cannot express). Without these,
	// a reshard-while-down looks identical to a plain kill+reshard in the report,
	// and a vacuous run that never hit a combination would read as "passed" with no
	// way to tell. Each combination increments BOTH a base counter (for the kill /
	// reshard / leave totals) AND its own combination counter here.
	//
	//   evReshardWhileDown  - a reshard driven over the reduced membership while a
	//                         node is down (then the node restarts at the new gen).
	//   evReshardWhileDownC  - of those, how many actually COMMITTED the reshard.
	//   evLeaveMidReshard   - a membership change fired mid-reshard (the clean-abort
	//                         driver).
	//   evLeaveMidReshardAb  - of those, how many drove the reshard to a clean ABORT
	//                         (the intended outcome); the remainder committed first.
	evReshardWhileDown  atomic.Int64
	evReshardWhileDownC atomic.Int64
	evLeaveMidReshard   atomic.Int64
	evLeaveMidReshardAb atomic.Int64

	finalNodes int
	finalUnits int
}

// violation is one oracle verdict that failed, captured for the final report.
type violation struct {
	verdict Verdict
	detail  string
}

// violationLog accumulates violations across all goroutines (continuous readers,
// writers, and the final sweep). The pass condition is an empty log.
type violationLog struct {
	mu   sync.Mutex
	list []violation
}

func (vl *violationLog) add(v Verdict, detail string) {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	vl.list = append(vl.list, violation{verdict: v, detail: detail})
}

func (vl *violationLog) snapshot() []violation {
	vl.mu.Lock()
	defer vl.mu.Unlock()
	return append([]violation(nil), vl.list...)
}

// config carries the run knobs (seeded from env in chaos_soak_test.go).
type config struct {
	seed        int64
	duration    time.Duration
	writers     int
	readers     int
	nodes       int
	units       int
	chaosEvery  time.Duration
	settleDelay time.Duration
	// settleScale multiplies the harness's convergence + settle budgets (member
	// convergence after add/remove, the inter-event WaitSettled, the final
	// WaitSettled). The in-memory soak leaves it 0 (treated as 1x): handoffs +
	// reshards settle in milliseconds there. A REAL-backend soak (slatedb over
	// MinIO) sets it >1 because each OpenUnit / flush round-trips to object storage
	// (~100ms+), so a multi-unit cluster needs proportionally longer to mount every
	// unit on its ring-assigned owner. Scaling the BUDGET (not the workload) is the
	// difference between "the cluster genuinely lost a write" and "we asked whether
	// it had finished mounting before it could". A budget too tight for a slow
	// backend would surface durable-but-not-yet-mounted units as false LOST
	// verdicts in the final sweep; scaling it lets the cluster actually converge so
	// the sweep judges real durability.
	settleScale float64
	// noReshard, when set, makes the scheduler fire ONLY membership churn
	// (kill/restart, join, leave) and never a reshard or a reshard-combination. It
	// is a DIAGNOSTIC isolation knob (not a normal mode): it lets a real-backend
	// run separate "does a plain lease handoff lose a write" from "does the
	// doubling reshard lose a write" by removing the generation-changing events.
	// A real soak leaves it false (the reshard + join-after-reshard path is the
	// whole point). The vacuous-check tolerates it: a noReshard run is expected to
	// have zero reshards, so its caller must relax the reshard coverage demand.
	noReshard bool
	// reshardOnly, when set, makes the scheduler fire ONLY plain reshards (no
	// membership churn, no combinations). The complementary DIAGNOSTIC to
	// noReshard: it isolates "does the doubling reshard bisect alone lose a write"
	// from "does a reshard RACING a membership change lose one". A real soak leaves
	// it false. Like noReshard it relaxes the combination-coverage vacuous demand.
	reshardOnly bool
	// smoke is set for the fast inner-loop variant (SHALE_CHAOS_SMOKE=1). It runs
	// few events quickly and RELAXES the vacuous-check so it does not demand the
	// full combination coverage a real soak proves. The default (smoke=false) is a
	// genuine soak that MUST exercise each combination, or it fails as vacuous.
	smoke bool
}

// harness wires the cluster, oracle, metrics, and violation log together and runs
// the soak.
type harness struct {
	cfg  config
	cl   *inProcCluster
	or   *Oracle
	met  *metrics
	vlog *violationLog

	// ctrExpected maps a per-writer Transact counter key to the number of acked
	// increments that writer made; the final sweep asserts the stored counter
	// equals it (the no-lost-update RMW invariant). Guarded by ctrMu since each
	// writer records its own entry at shutdown.
	ctrMu       sync.Mutex
	ctrExpected map[string]int64

	// logf is the test's t.Logf, injected so the harness can report without
	// importing testing into the non-test logic.
	logf func(format string, args ...any)
}

// retryBudget bounds how long a single op retries the retryable signals before
// giving up (and being reported as a hard failure). Generous so a reshard freeze
// or a multi-node handoff window is always ridden out.
const retryBudget = 20 * time.Second

// retryDelay is the per-attempt backoff while retrying a retryable error.
const retryDelay = 15 * time.Millisecond

// isRetryable reports whether err is a transient cutover/handoff/freeze signal
// the client is expected to retry (NOT an ack-and-lose). Unavailable is the
// acquiring-window + freeze refusal; FailedPrecondition is the reshard-cutover
// re-pin. Anything else (including a closed-cluster error from a node we are
// mid-killing, which the caller handles by re-selecting an entry node) is not
// retried here.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.FailedPrecondition
}

// putAcked issues a Put through entrySel (a function returning a live entry node,
// re-evaluated each attempt so a killed entry node is replaced), retrying the
// retryable signals up to retryBudget. Returns nil on a real ack. A nil entry
// (no live node, transient) is itself retried. Records a retry metric per retry.
func (h *harness) putAcked(entrySel func() *cluster.Cluster, key, value string) error {
	deadline := time.Now().Add(retryBudget)
	for {
		c := entrySel()
		if c == nil {
			if time.Now().After(deadline) {
				return errors.New("putAcked: no live entry node before deadline")
			}
			time.Sleep(retryDelay)
			continue
		}
		err := c.Put([]byte(key), []byte(value))
		if err == nil {
			return nil
		}
		if isRetryable(err) && time.Now().Before(deadline) {
			h.met.retryableRetries.Add(1)
			time.Sleep(retryDelay)
			continue
		}
		// A closed/unavailable-entry-node error (we caught the node mid-kill), or a
		// just-fenced unit (the owning node lost the lease mid-write, so the write
		// was NOT acked): re-select and retry within budget rather than failing the
		// write, since a different live node owns the key's unit now.
		if (isEntryNodeGone(err) || isFenceTransient(err)) && time.Now().Before(deadline) {
			h.met.retryableRetries.Add(1)
			time.Sleep(retryDelay)
			continue
		}
		return err
	}
}

// deleteAcked is the Delete analogue of putAcked.
func (h *harness) deleteAcked(entrySel func() *cluster.Cluster, key string) error {
	deadline := time.Now().Add(retryBudget)
	for {
		c := entrySel()
		if c == nil {
			if time.Now().After(deadline) {
				return errors.New("deleteAcked: no live entry node before deadline")
			}
			time.Sleep(retryDelay)
			continue
		}
		err := c.Delete([]byte(key))
		if err == nil {
			return nil
		}
		if (isRetryable(err) || isEntryNodeGone(err) || isFenceTransient(err)) && time.Now().Before(deadline) {
			h.met.retryableRetries.Add(1)
			time.Sleep(retryDelay)
			continue
		}
		return err
	}
}

// getObserved issues a Get through entrySel, retrying the retryable signals, and
// returns the settled Observed (value or not-found). A retry budget overrun
// returns the last error (the caller logs it as a read-availability violation).
// Uses the default retryBudget.
func (h *harness) getObserved(entrySel func() *cluster.Cluster, key string) (Observed, error) {
	return h.getObservedBudget(entrySel, key, retryBudget)
}

// getObservedBudget is getObserved with an explicit per-read retry budget. The
// final sweep uses a SHORTER budget than the in-workload reads: the sweep runs
// against a cluster WaitSettled has already driven to a fully-mounted, idle
// state, so a read should resolve almost immediately. A key still failing after
// the short budget is a genuine availability gap worth reporting as a violation,
// not a transient handoff window to ride out - and a short budget keeps the sweep
// from hanging for tens of seconds per key if the cluster did not fully settle.
func (h *harness) getObservedBudget(entrySel func() *cluster.Cluster, key string, budget time.Duration) (Observed, error) {
	deadline := time.Now().Add(budget)
	for {
		c := entrySel()
		if c == nil {
			if time.Now().After(deadline) {
				return Observed{}, errors.New("getObserved: no live entry node")
			}
			time.Sleep(retryDelay)
			continue
		}
		val, err := c.Get([]byte(key))
		if err == nil {
			return Observed{Value: string(val)}, nil
		}
		if errors.Is(err, errNotFound) {
			return Observed{NotFound: true}, nil
		}
		if (isRetryable(err) || isEntryNodeGone(err) || isFenceTransient(err)) && time.Now().Before(deadline) {
			h.met.retryableRetries.Add(1)
			time.Sleep(retryDelay)
			continue
		}
		return Observed{}, err
	}
}

// sweepRead reads key from one specific node for the final sweep. Unlike the
// workload read it does NOT keep retrying the ownership-disagreement error (a
// node refusing to route a key it does not own because its ring is momentarily
// stale): that condition is permanent for THIS read on THIS node, so we return it
// immediately and let the sweep try the next node, rather than burning the whole
// per-read budget on a stale node. We still briefly ride out a genuine acquiring
// window (Unavailable) up to the budget. errNotFound is a settled result.
func (h *harness) sweepRead(c *cluster.Cluster, key string, budget time.Duration) (Observed, error) {
	deadline := time.Now().Add(budget)
	for {
		val, err := c.Get([]byte(key))
		if err == nil {
			return Observed{Value: string(val)}, nil
		}
		if errors.Is(err, errNotFound) {
			return Observed{NotFound: true}, nil
		}
		if isOwnershipDisagreement(err) || isFenceTransient(err) {
			// Stale ring OR a just-fenced unit on THIS node: do not retry here,
			// surface to the sweep so it moves to the next node immediately. A fenced
			// read means this node's lease for the key's unit moved; the NEW owner
			// (another node the sweep also reads) serves it. Treating it as a
			// node-skip (like the ownership disagreement) - not a hard error and not a
			// per-node retry - lets the sweep find the new owner without burning the
			// budget against a node that will keep returning the fence until its
			// reconcile unmounts the unit.
			return Observed{}, err
		}
		if isRetryable(err) && time.Now().Before(deadline) {
			h.met.retryableRetries.Add(1)
			time.Sleep(retryDelay)
			continue
		}
		return Observed{}, err
	}
}

// isOwnershipDisagreement reports whether err is a node refusing to serve/route a
// key because its view of ownership is stale (post-reshard / post-membership ring
// lag): the loop-guard "forwarding loop refused" or the "this node does not own
// the key" precondition. It is NOT a lost write - the owner still serves the key;
// it just means THIS node's ring has not refreshed yet.
func isOwnershipDisagreement(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) != codes.FailedPrecondition {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "does not own the key") ||
		strings.Contains(msg, "forwarding loop refused")
}

// isEntryNodeGone reports whether err is the symptom of issuing an op through a
// node we are mid-killing (closed cluster / dial failure to a dead peer). The
// driver responds by re-selecting a live entry node, NOT by acking-then-losing.
func isEntryNodeGone(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, backend.ErrClosed) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "closed") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "transport is closing") ||
		strings.Contains(msg, "the cluster is closed")
}

// isFenceTransient reports whether err is a slatedb WRITER-EPOCH FENCE surfacing
// on an op against a node that held a unit whose lease just moved: slatedb's
// engine rejects the op with ErrorClosed{Reason: CloseReasonFenced} ("Closed
// error: detected newer DB client", "Reason=2"), which over gRPC arrives as
// codes.Unknown wrapping that string. This is the EXPECTED race in a lease
// handoff: between the new owner's OpenUnit (which fences) and the old owner's
// reconcile unmounting the now-fenced unit, an op can briefly hit the fenced
// instance on the old owner. It is NOT data loss - the data is durable in the
// bucket and the NEW owner serves it; the op must be RETRIED (it resolves once
// the old owner's reconcile unmounts the fenced unit and routing follows the new
// lease). The single-instance backend's TestMinIO_WriterEpochFencing + the
// factory integration tests pin this exact slatedb signature, so matching the
// string here is matching a stable, tested contract - not a fragile guess. A
// fenced READ in particular must be retried (route to the new owner), or the
// reader falsely reports a read-availability loss for a write that is perfectly
// safe in object storage under a different lease holder.
func isFenceTransient(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "detected newer DB client") ||
		strings.Contains(msg, "Reason=2") ||
		(strings.Contains(msg, "Closed error") && strings.Contains(msg, "newer"))
}

// runWriter is one writer goroutine. It owns the disjoint key subspace
// "w<id>-k<n>" plus a per-writer Transact counter "ctr-w<id>". On each tick it
// picks an op by the seeded RNG (this writer's own *rand.Rand, derived from the
// global seed + writer id so the run is reproducible), issues it acked, and
// records the ack in the oracle. It tracks its own per-key version + the set of
// keys it has written (for Delete targeting). Stops when stop closes.
func (h *harness) runWriter(id int, rng *rand.Rand, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	versions := make(map[string]uint64) // per-key monotonic version this writer assigns
	liveKeys := make([]string, 0)       // keys with a current live (non-deleted) value
	ctrKey := fmt.Sprintf("ctr-w%d", id)
	var ctrAcked int64 // count of acked Transact increments (checked at the end)

	entrySel := func() *cluster.Cluster { return h.cl.EntryNode(rng.Intn(1 << 20)) }
	keyCount := 0
	// Bound the per-writer key space so the acked dataset stays finite (a finite
	// dataset keeps the final full sweep tractable) AND so PUTs cycle into
	// OVERWRITES of existing keys - a strictly stronger oracle test than only ever
	// writing fresh keys, because an overwrite that loses surfaces as a stale read.
	const maxKeysPerWriter = 300

	for {
		select {
		case <-stop:
			// Record the writer's final acked-counter expectation into the oracle
			// as a sentinel value so the final sweep can verify the RMW invariant.
			h.recordCounter(ctrKey, ctrAcked)
			return
		default:
		}

		roll := rng.Intn(100)
		switch {
		case roll < 70 || len(liveKeys) == 0:
			// PUT. Below the cap, create a fresh key; at the cap, OVERWRITE a random
			// existing live key with a new version (bounds the dataset + exercises
			// overwrites). An overwrite of a known key is marked in-flight so a reader
			// that samples it mid-write tolerates the benign race.
			var key string
			overwrite := false
			if keyCount < maxKeysPerWriter || len(liveKeys) == 0 {
				key = fmt.Sprintf("w%d-k%d", id, keyCount)
				keyCount++
			} else {
				key = liveKeys[rng.Intn(len(liveKeys))]
				overwrite = true
			}
			versions[key]++
			ver := versions[key]
			val := EncodeValue(key, ver, "payload")
			if overwrite {
				h.or.BeginOp(key)
			}
			err := h.putAcked(entrySel, key, val)
			if err != nil {
				if overwrite {
					h.or.EndOp(key)
				}
				h.vlog.add(VerdictLost, fmt.Sprintf("writer %d Put(%q) failed (not an ack, hard error): %v", id, key, err))
				continue
			}
			h.or.RecordPut(key, val, ver)
			if overwrite {
				h.or.EndOp(key)
			} else {
				liveKeys = append(liveKeys, key)
			}
			h.met.ackedPuts.Add(1)

		case roll < 85:
			// DELETE a random live key. Mark it in-flight so a reader that samples
			// it WHILE the delete is in transit tolerates the (benign) race where the
			// tombstone landed durably before Delete() returned. The key is already
			// in the model (it had an acked Put), so the guard is scoped to exactly
			// this key; losses elsewhere stay strict.
			ki := rng.Intn(len(liveKeys))
			key := liveKeys[ki]
			versions[key]++
			ver := versions[key]
			h.or.BeginOp(key)
			err := h.deleteAcked(entrySel, key)
			if err != nil {
				h.or.EndOp(key)
				h.vlog.add(VerdictLost, fmt.Sprintf("writer %d Delete(%q) failed (hard error): %v", id, key, err))
				continue
			}
			h.or.RecordDelete(key, ver)
			h.or.EndOp(key)
			h.met.ackedDeletes.Add(1)
			// Remove from liveKeys (swap-delete).
			liveKeys[ki] = liveKeys[len(liveKeys)-1]
			liveKeys = liveKeys[:len(liveKeys)-1]

		default:
			// TRANSACT: read-modify-write the per-writer counter. A successful
			// Transact is an acked increment. Retries the retryable freeze/handoff
			// signal internally (Transact already does), so a nil return is a
			// genuine ack.
			if err := h.transactInc(entrySel, ctrKey, ctrAcked+1); err != nil {
				h.vlog.add(VerdictLost, fmt.Sprintf("writer %d Transact(%q) failed (hard error): %v", id, ctrKey, err))
				continue
			}
			ctrAcked++
			h.met.ackedTransacts.Add(1)
		}
	}
}

// transactInc advances the counter key toward target (the caller's acked-count+1)
// under Transact, retrying the entry-node-gone case (Transact handles the freeze/
// handoff retry itself). The update is an IDEMPOTENT monotonic-max, NOT a blind
// +1: a retried Transact whose first commit was durable-but-unacked re-applies the
// same target as a no-op instead of double-counting (see the inline comment + the
// verifyCounters invariant). The counter value is stored as a plain decimal so the
// final sweep can compare it to the acked-increment count exactly.
func (h *harness) transactInc(entrySel func() *cluster.Cluster, ctrKey string, target int64) error {
	deadline := time.Now().Add(retryBudget)
	for {
		c := entrySel()
		if c == nil {
			if time.Now().After(deadline) {
				return errors.New("transactInc: no live entry node")
			}
			time.Sleep(retryDelay)
			continue
		}
		err := c.Transact([]byte(ctrKey), func(tx backend.Transaction) error {
			cur, gerr := tx.Get([]byte(ctrKey))
			if gerr != nil && !errors.Is(gerr, errNotFound) {
				return gerr
			}
			n := int64(0)
			if len(cur) > 0 {
				_, _ = fmt.Sscanf(string(cur), "%d", &n)
			}
			// IDEMPOTENT monotonic-max increment: the writer's target is its own
			// (acked-count + 1). Re-applying the SAME target after a durable-but-
			// unacked commit (entry node died after the commit landed) is a no-op
			// (max(stored, target) == stored), so a retried Transact never DOUBLE-
			// counts. This makes the final stored == acked an exact, falsifiable
			// lossless check robust to Transact's at-least-once contract under a
			// crash, instead of conflating a benign retry with a real lost update.
			next := n
			if target > next {
				next = target
			}
			return tx.Put([]byte(ctrKey), []byte(fmt.Sprintf("%d", next)))
		})
		if err == nil {
			return nil
		}
		if (isRetryable(err) || isEntryNodeGone(err) || isSpuriousCrossShard(err) || isFenceTransient(err)) && time.Now().Before(deadline) {
			h.met.retryableRetries.Add(1)
			time.Sleep(retryDelay)
			continue
		}
		return err
	}
}

// isSpuriousCrossShard reports whether err is the documented SUT mid-reshard
// spurious cross-shard conflict (pkg/cluster guardShard "TODO(reshard-tx)"): a
// single-key Transact whose pin key's unit cuts over to a new generation BETWEEN
// the pin and a later same-shard op resolves the same shard key to a different
// GenUnit, so guardShard returns backend.ErrCrossShard even though the
// transaction touches exactly one shard. The SUT comment is explicit that this is
// NOT data loss - it is a benign transient that clears once the reshard cut-over
// completes and the pin re-resolves consistently. So, like ErrCASConflict, the
// client must RETRY it; treating it as a hard error would falsely flag a lost
// update. (A GENUINELY cross-shard transaction is impossible here: the harness's
// transactInc reads + writes exactly ONE counter key, so any ErrCrossShard it
// sees is necessarily this spurious mid-reshard flavor.)
func isSpuriousCrossShard(err error) bool {
	return errors.Is(err, backend.ErrCrossShard)
}

// recordCounter stashes a writer's final acked-increment count so the final sweep
// can assert the on-disk counter equals it (no lost update under Transact). We
// store it in a side map on the harness rather than the oracle (the oracle models
// last-writer-wins keys; the counter is an RMW key with different semantics).
func (h *harness) recordCounter(ctrKey string, acked int64) {
	h.ctrMu.Lock()
	defer h.ctrMu.Unlock()
	h.ctrExpected[ctrKey] = acked
}
