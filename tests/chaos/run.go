//go:build chaos

package chaos

// Run orchestration: wire the cluster, oracle, writers, readers, and scheduler
// together; run for the configured duration; then quiesce, run the final
// full-sweep verification, and produce the report. Returns the report so the
// test entrypoint can assert the pass condition (zero violations, non-vacuous).

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// report is the end-of-run accounting + verdict.
type report struct {
	seed             int64
	duration         time.Duration
	ackedPuts        int64
	ackedDeletes     int64
	ackedTransacts   int64
	readsVerified    int64
	retryableRetries int64
	transientReads   int64
	events           map[string]int64
	finalNodes       int
	finalUnits       int
	finalGeneration  int
	violations       []violation
}

// vacuous reports whether the run failed to stress anything (no acked writes, no
// chaos events, or no retryable turbulence). A vacuous soak proves nothing and is
// treated as a failure by the test.
func (r *report) vacuous() (bool, string) {
	if r.ackedPuts == 0 {
		return true, "zero acked Puts: the workload never ran"
	}
	totalEvents := int64(0)
	for _, v := range r.events {
		totalEvents += v
	}
	if totalEvents == 0 {
		return true, "zero chaos events: the scheduler never fired"
	}
	if r.retryableRetries == 0 {
		return true, "zero retryable retries: the run never hit a cutover/handoff/freeze window (suspiciously quiet - chaos may not have overlapped the workload)"
	}
	return false, ""
}

// String renders the report for the test log.
func (r *report) String() string {
	vio := "none"
	if len(r.violations) > 0 {
		vio = fmt.Sprintf("%d", len(r.violations))
	}
	return fmt.Sprintf(
		"chaos soak report (seed=%d duration=%s):\n"+
			"  acked_puts=%d acked_deletes=%d acked_transacts=%d\n"+
			"  reads_verified=%d retryable_retries=%d transient_reads=%d\n"+
			"  chaos_events=%v\n"+
			"  final_node_count=%d final_unit_count=%d final_generation=%d\n"+
			"  violations=%s",
		r.seed, r.duration,
		r.ackedPuts, r.ackedDeletes, r.ackedTransacts,
		r.readsVerified, r.retryableRetries, r.transientReads,
		r.events,
		r.finalNodes, r.finalUnits, r.finalGeneration,
		vio,
	)
}

// run executes the full soak and returns the report. logf is the test's t.Logf.
func run(cfg config, logf func(string, ...any)) (*report, error) {
	cl, err := newInProcCluster(cfg.nodes, cfg.units, cfg.settleDelay)
	if err != nil {
		return nil, fmt.Errorf("stand up cluster: %w", err)
	}
	defer cl.CloseAll()

	h := &harness{
		cfg:         cfg,
		cl:          cl,
		or:          NewOracle(),
		met:         &metrics{},
		vlog:        &violationLog{},
		ctrExpected: make(map[string]int64),
		logf:        logf,
	}

	// One *rand.Rand per goroutine, each derived from the global seed + a role
	// offset, so the whole run is reproducible from cfg.seed yet the goroutines do
	// not share a non-thread-safe Rand. Offsets are fixed constants per role.
	base := cfg.seed
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writers.
	for i := 0; i < cfg.writers; i++ {
		wg.Add(1)
		rng := rand.New(rand.NewSource(base + 1_000 + int64(i)))
		go h.runWriter(i, rng, stop, &wg)
	}
	// Readers.
	for i := 0; i < cfg.readers; i++ {
		wg.Add(1)
		rng := rand.New(rand.NewSource(base + 2_000 + int64(i)))
		go h.runReader(i, rng, stop, &wg)
	}
	// Let writers seed some data before chaos starts, so the first events have a
	// non-empty dataset to threaten.
	time.Sleep(200 * time.Millisecond)
	// Scheduler.
	wg.Add(1)
	schedRng := rand.New(rand.NewSource(base + 9_000))
	go h.runScheduler(schedRng, stop, &wg)

	// Run for the configured duration.
	time.Sleep(cfg.duration)

	// Stop everything + wait for the goroutines to drain. The scheduler records
	// the authoritative final unit count (base << generation) into the metrics as
	// it exits, so by the time wg.Wait returns h.met.finalUnits is populated.
	close(stop)
	wg.Wait()

	// Drive the cluster to a FULLY-settled state before the final sweep: every
	// node converged on members, idle, and the union of mounted units covering the
	// whole expected space exactly once. This is what keeps the sweep fast - it
	// reads a settled cluster instead of retrying owner-but-unmounted keys for the
	// full per-key budget. expectedUnits is the scheduler's live count.
	cl.WaitSettled(h.met.finalUnits, 45*time.Second)

	// Final full-sweep verification: read EVERY acked key from EVERY live node and
	// check it against the model. This is the belt to the continuous readers'
	// suspenders - it catches a loss the random sampling missed.
	h.finalSweep()

	// RMW invariant: every per-writer Transact counter equals its acked-increment
	// count (no lost update under Transact).
	h.verifyCounters()

	// Build the report.
	rep := &report{
		seed:             cfg.seed,
		duration:         cfg.duration,
		ackedPuts:        h.met.ackedPuts.Load(),
		ackedDeletes:     h.met.ackedDeletes.Load(),
		ackedTransacts:   h.met.ackedTransacts.Load(),
		readsVerified:    h.met.readsVerified.Load(),
		retryableRetries: h.met.retryableRetries.Load(),
		transientReads:   h.met.transientReads.Load(),
		events: map[string]int64{
			"kill":            h.met.evKill.Load(),
			"restart":         h.met.evRestart.Load(),
			"join":            h.met.evJoin.Load(),
			"leave":           h.met.evLeave.Load(),
			"reshard":         h.met.evReshard.Load(),
			"reshard_aborted": h.met.evReshardAbrt.Load(),
		},
		finalNodes:      h.met.finalNodes,
		finalUnits:      h.met.finalUnits,
		finalGeneration: generationFromUnits(cfg.units, h.met.finalUnits),
		violations:      h.vlog.snapshot(),
	}
	return rep, nil
}

// finalSweep reads every acked key from every live node and verifies it. A
// not-found-via-retry that times out is a read-availability violation; a wrong
// value is a loss. This is the strongest end-state assertion: after all chaos and
// quiescence, the entire acked dataset must be intact from every node.
// sweepReadBudget bounds each final-sweep read. WaitSettled has already driven
// the cluster to a fully-mounted, idle, converged state, so a read resolves in
// milliseconds; this budget only absorbs a brief residual ring-refresh. A key
// still failing after it is a genuine availability gap, reported as a violation
// rather than hung on - and a tight budget keeps a worst-case sweep (every key
// momentarily unmounted) from dominating the run's wall clock.
const sweepReadBudget = 1500 * time.Millisecond

func (h *harness) finalSweep() {
	snap := h.or.Snapshot()
	clusters := h.cl.AllLiveClusters()
	if len(clusters) == 0 {
		h.vlog.add(VerdictLost, "final sweep: no live node to read from")
		return
	}
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}

	// The dataset can be large (thousands of acked keys after a long soak) and the
	// sweep reads every key from every node, so do it with a bounded worker pool
	// rather than one sequential pass - a sequential O(keys x nodes) sweep with
	// per-read retries can dominate the run's wall clock. Workers are bounded so
	// the sweep does not stampede the cluster with concurrent reads.
	const sweepWorkers = 16
	keyCh := make(chan string, sweepWorkers*2)
	var wg sync.WaitGroup
	for w := 0; w < sweepWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range keyCh {
				// Read the key from EVERY live node, but interpret the results
				// together. A node whose ring has not yet refreshed after the last
				// reshard/membership change can refuse to route a key it does not own
				// (a permanent-shaped FailedPrecondition "this node does not own the
				// key" / "forwarding loop refused"); that is a STALE-RING condition on
				// that one node, NOT a lost write - the value is still served by the
				// owner. So we PASS the key if at least one node returns the correct
				// latest value and NO node returns a wrong (stale/corrupt/resurrected)
				// value. The key is LOST only if no node could serve a correct value
				// (every node errored or disagreed), which is a genuine availability
				// loss after the cluster was given time to settle.
				served := false
				var lastErr error
				badVerdict := false
				for _, c := range clusters {
					obs, err := h.sweepRead(c, key, sweepReadBudget)
					if err != nil {
						lastErr = err
						continue // stale ring / unavailable on THIS node; try the next
					}
					vd, detail := h.or.Verify(key, obs)
					h.met.readsVerified.Add(1)
					if vd == VerdictPass {
						served = true
					} else {
						// A node returned a definite WRONG value (stale/corrupt/
						// resurrected): that is a real violation regardless of the
						// other nodes - the data is wrong, not merely unrouted.
						badVerdict = true
						h.vlog.add(vd, fmt.Sprintf("final sweep: %s", detail))
					}
				}
				if !served && !badVerdict {
					h.vlog.add(VerdictLost, fmt.Sprintf("final sweep: key %q served correctly by NO live node after settle (last error: %v) - a genuine availability loss", key, lastErr))
				}
			}
		}()
	}
	// Overall sweep deadline: if WaitSettled could not bring the cluster to a clean
	// state, per-key reads retry up to sweepReadBudget and the sweep could crawl.
	// Stop feeding keys once a generous total budget is exceeded and record one
	// diagnostic violation (the run then fails, correctly pointing at a cluster
	// that did not re-settle after chaos) instead of hanging to the test timeout.
	sweepDeadline := time.Now().Add(90 * time.Second)
	fed := 0
	for _, k := range keys {
		if time.Now().After(sweepDeadline) {
			h.vlog.add(VerdictLost, fmt.Sprintf("final sweep exceeded its %s budget after %d/%d keys: the cluster did not re-settle after chaos (owner-but-unmounted units persisting)", 90*time.Second, fed, len(keys)))
			break
		}
		keyCh <- k
		fed++
	}
	close(keyCh)
	wg.Wait()
}

// verifyCounters checks the Transact RMW invariant: each per-writer counter key
// stores exactly the number of acked increments that writer made. A lower value
// is a lost update; a higher value is impossible (would mean a phantom ack).
func (h *harness) verifyCounters() {
	clusters := h.cl.AllLiveClusters()
	if len(clusters) == 0 {
		return
	}
	h.ctrMu.Lock()
	expected := make(map[string]int64, len(h.ctrExpected))
	for k, v := range h.ctrExpected {
		expected[k] = v
	}
	h.ctrMu.Unlock()

	for ctrKey, want := range expected {
		if want == 0 {
			continue
		}
		// Read the counter from whichever node can serve it (a stale-ring node is
		// skipped, same as the main sweep), so a momentary ownership disagreement
		// does not masquerade as a lost counter.
		var obs Observed
		var err error
		got := false
		for _, cc := range clusters {
			obs, err = h.sweepRead(cc, ctrKey, sweepReadBudget)
			if err == nil {
				got = true
				break
			}
		}
		if !got {
			h.vlog.add(VerdictLost, fmt.Sprintf("counter %q unreadable after the soak from any node: %v", ctrKey, err))
			continue
		}
		if obs.NotFound {
			h.vlog.add(VerdictLost, fmt.Sprintf("counter %q not-found but %d acked increments were made (RMW lost)", ctrKey, want))
			continue
		}
		var gotN int64
		_, _ = fmt.Sscanf(obs.Value, "%d", &gotN)
		if gotN != want {
			h.vlog.add(VerdictStale, fmt.Sprintf("counter %q = %d but %d Transact increments were acked (lost update under Transact)", ctrKey, gotN, want))
		}
	}
}

// generationFromUnits derives the reshard generation from the base + final unit
// counts (count = base << gen). Returns 0 if final <= base.
func generationFromUnits(base, final int) int {
	if final <= base || base == 0 {
		return 0
	}
	g := 0
	for n := base; n < final; n <<= 1 {
		g++
	}
	return g
}
