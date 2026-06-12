//go:build chaosreal

package chaos

// runReal wires the real MULTI-BACKEND cluster, oracle, writers, and the
// chaos+durability+reshard scheduler together; runs for the configured window;
// then quiesces, runs the final full-sweep verification, and produces the report.
// Self-contained: it owns the child-process lifecycle end to end and guarantees
// teardown (no orphaned shaled-slate processes) via a defer.
//
// THE INVARIANT IT GATES ON (the deployable v0.8 claim, end to end): NO ACKED WRITE
// IS LOST. Every acked key, read back through the real gRPC client, must serve its
// CORRECT latest acked value once the cluster settles - never stale, corrupt,
// resurrected, OR permanently unavailable - across real process death, the real
// copy-free lease handoff (a survivor re-OpenUnits a dead node's units from object
// storage), a real operator reshard (N -> 2N), and real joins/leaves. This is the
// full no-acked-write-loss oracle the in-process soak proves, now over a process
// boundary + real object storage. A read that is transiently unavailable/stale
// mid-cutover and then converges is the expected retryable turbulence (counted as a
// transient, never failed); a read that stays wrong/unavailable past the retry +
// settle budget is a hard violation.
//
// Unlike the legacy adapter this replaces, there is NO "tolerated unavailability"
// gap: multi-backend mode DOES re-mount a dead owner's units on a survivor, so a
// killed owner's keys MUST come back. The whole point is to prove that.

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// runReal executes the full real-cluster soak and returns the report. logf is the
// test's t.Logf.
func runReal(cfg realConfig, logf func(string, ...any)) (rep *realReport, err error) {
	top, err := newRealTopology(cfg.cluster, cfg.nodes)
	if err != nil {
		return nil, fmt.Errorf("stand up real cluster: %w", err)
	}
	// GUARANTEE teardown: no orphaned child processes survive this function, even
	// on a panic in the workload.
	defer top.CloseAll()

	rc := &realHarness{
		cfg:  cfg,
		top:  top,
		or:   NewOracle(),
		met:  &realMetrics{},
		vlog: &realViolationLog{},
		logf: logf,
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writers. Each owns a disjoint key subspace so the oracle's single-writer-per-
	// key assumption holds (latest acked version is unambiguous).
	for i := 0; i < cfg.writers; i++ {
		wg.Add(1)
		rng := rand.New(rand.NewSource(cfg.seed + 1_000 + int64(i)))
		go rc.runWriter(i, rng, stop, &wg)
	}
	// Reader.
	wg.Add(1)
	readRng := rand.New(rand.NewSource(cfg.seed + 2_000))
	go rc.runReader(readRng, stop, &wg)

	// Warm up: let the writers ack a dataset spread across the units before any
	// chaos, so the durable-handoff probe and the reshard have real data to move.
	time.Sleep(2 * time.Second)

	// PHASE A - DURABLE CROSS-PROCESS HANDOFF. SIGKILL a non-founder node under the
	// live workload. Its owned units are real slatedb databases in the shared bucket;
	// a survivor re-acquires those leases (the reconcile-driven copy-free handoff) and
	// re-OpenUnits them. Probe EVERY acked key from the surviving cluster after the
	// handoff settles: a killed owner's keys must come back (survived). A key that
	// stays unreadable past the settle re-probe is a hard LOSS. Then restart the
	// killed node (a fresh process rejoining at the live generation).
	rc.runDurabilityKill()
	rc.restartAfterKill()

	// PHASE B - OPERATOR RESHARD (N -> 2N). Issue the operator Reshard RPC mid-run
	// against the live workload. The cluster-wide freeze barrier coordinates the
	// doubling across processes; each unit bisects by copying its keys through object
	// storage. Every acked write must survive. This is the deploy-gap proof: a real
	// reshard on a real running cluster, triggered by the operator RPC.
	//
	// Let the post-restart rebalance settle the unit leases first: the reshard
	// snapshots the member identity set and aborts on any in-flight membership /
	// lease change, and a unit lease still moving from the restart can fence the
	// BISECT scan ("detected newer DB client"). Settling first makes the reshard
	// land on the first attempt (the retry in runReshard still covers a stray race).
	rc.waitMembersStable(15 * time.Second)
	time.Sleep(2 * time.Second)
	rc.runReshard()

	// PHASE C - CHURN. Layer a seeded schedule of real joins / leaves / kill+restarts
	// for the rest of the window, all under the SAME hard oracle: multi-backend mode
	// hands off on every membership change, so no acked write may be lost across any
	// of them.
	wg.Add(1)
	schedRng := rand.New(rand.NewSource(cfg.seed + 9_000))
	go rc.runScheduler(schedRng, cfg.duration, stop, &wg)

	// Run the workload window.
	time.Sleep(cfg.duration)
	close(stop)
	wg.Wait()

	// Final strict sweep: drive the cluster to a stable membership AND wait for the
	// unit leases to settle, then read every acked key. After settle there is no
	// cutover in flight, so any wrong/missing value is unconditionally a violation.
	// The lease-settle gate is load-bearing for the DECLARATIVE reshard: a reshard
	// that committed late (or a phase-C kill+restart at the doubled count) leaves
	// several units mid-redistribution, and the per-key sweep below burns its full
	// retry budget on every key whose unit is still moving. Waiting until a SAMPLE
	// of acked keys reads back cleanly proves the leases have landed, so the full
	// sweep is bounded instead of O(keys x budget). It does NOT weaken the oracle:
	// the sweep still reads + verifies EVERY key strictly; the gate only delays its
	// start until the cluster is actually serving.
	rc.waitMembersStable(20 * time.Second)
	rc.waitClusterReadable(90 * time.Second)
	rc.finalSweep()

	rep = &realReport{
		seed:             cfg.seed,
		duration:         cfg.duration,
		ackedPuts:        rc.met.ackedPuts.Load(),
		ackedDeletes:     rc.met.ackedDeletes.Load(),
		readsVerified:    rc.met.readsVerified.Load(),
		retryableRetries: rc.met.retryableRetries.Load(),
		transientReads:   rc.met.transientReads.Load(),
		events: map[string]int64{
			"kill":    rc.met.evKill.Load(),
			"restart": rc.met.evRestart.Load(),
			"join":    rc.met.evJoin.Load(),
			"leave":   rc.met.evLeave.Load(),
			"reshard": rc.met.evReshard.Load(),
		},
		durSurvived:    rc.met.durSurvived.Load(),
		startUnitCount: cfg.cluster.unitCount,
		reshardFrom:    rc.met.reshardFrom.Load(),
		reshardTo:      rc.met.reshardTo.Load(),
		finalNodes:     top.MemberCount(),
		violations:     rc.vlog.snapshot(),
	}
	return rep, nil
}

// realHarness wires the real cluster, oracle, metrics, and violation log together.
type realHarness struct {
	cfg  realConfig
	top  *realTopology
	or   *Oracle
	met  *realMetrics
	vlog *realViolationLog
	logf func(string, ...any)
}

// recordWrongValue records a PERSISTED wrong-value verdict (one that survived the
// reverify retry budget) as a hard violation. In multi-backend mode the cluster
// hands off on every membership change, so a wrong value that does not self-heal is
// a real acked-write-loss bug - there is no legacy "ring churn excuses it" gap.
func (rc *realHarness) recordWrongValue(vd Verdict, detail string) {
	rc.vlog.add(vd, detail)
}

// recordWriteFailure records a write that never acked even after the full retry
// budget as a hard violation. A faithful client rides out the handoff/reshard
// cutover by re-routing + retrying the retryable signals, so a write that still
// cannot make progress is a real availability bug. (It is NEVER an acked-write loss
// itself - the oracle records only acks - but it signals the real path wedged.)
func (rc *realHarness) recordWriteFailure(detail string) {
	rc.vlog.add(VerdictLost, detail)
}

// runWriter is one writer goroutine over a disjoint key subspace "w<id>-k<n>". On
// each tick it picks Put (overwrite once capped) or Delete by the seeded RNG,
// issues it acked through a ROTATING entry node (real gRPC), and records the ack
// in the oracle ONLY after success. (Transact is omitted: there is no public
// Transact RPC on the real gRPC surface; the in-process soak covers the RMW path.)
func (rc *realHarness) runWriter(id int, rng *rand.Rand, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	versions := make(map[string]uint64)
	liveKeys := make([]string, 0)
	keyCount := 0
	const maxKeysPerWriter = 200

	entrySel := func() *realNode { return rc.top.EntryNode(rng.Intn(1 << 20)) }

	for {
		select {
		case <-stop:
			return
		default:
		}

		roll := rng.Intn(100)
		switch {
		case roll < 80 || len(liveKeys) == 0:
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
				rc.or.BeginOp(key)
			}
			err := putAckedReal(entrySel, rc.met, key, val)
			if err != nil {
				if overwrite {
					rc.or.EndOp(key)
				}
				rc.recordWriteFailure(fmt.Sprintf("writer %d Put(%q) failed (never acked, hard error): %v", id, key, err))
				continue
			}
			rc.or.RecordPut(key, val, ver)
			if overwrite {
				rc.or.EndOp(key)
			} else {
				liveKeys = append(liveKeys, key)
			}
			rc.met.ackedPuts.Add(1)

		default:
			ki := rng.Intn(len(liveKeys))
			key := liveKeys[ki]
			versions[key]++
			ver := versions[key]
			rc.or.BeginOp(key)
			err := deleteAckedReal(entrySel, rc.met, key)
			if err != nil {
				rc.or.EndOp(key)
				rc.recordWriteFailure(fmt.Sprintf("writer %d Delete(%q) failed (never acked, hard error): %v", id, key, err))
				continue
			}
			rc.or.RecordDelete(key, ver)
			rc.or.EndOp(key)
			rc.met.ackedDeletes.Add(1)
			liveKeys[ki] = liveKeys[len(liveKeys)-1]
			liveKeys = liveKeys[:len(liveKeys)-1]
		}
	}
}

// runReader continuously reads known keys through a rotating entry node and feeds
// each result to the oracle. A read that returns the CORRECT value passes; a read
// that returns a WRONG value (stale/corrupt/resurrected) is re-verified, and a
// PERSISTED wrong value is a hard violation. A not-found / unreadable read DURING
// the run is a mid-cutover transient (the handoff/reshard window where this node's
// ring is briefly behind), so the continuous reader counts it as turbulence and
// retries - it does NOT fail on it, because the cluster is mid-change. The strict
// availability check is the FINAL SWEEP, run after the cluster has settled: there a
// not-found for a live acked key IS a loss (multi-backend mode must have handed the
// unit off to a survivor). This is the same transient-vs-persisted discipline the
// in-process soak uses; the difference from the legacy adapter is that "unavailable
// after settle" is now a HARD loss, not a tolerated gap.
func (rc *realHarness) runReader(rng *rand.Rand, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	entrySel := func() *realNode { return rc.top.EntryNode(rng.Intn(1 << 20)) }
	for {
		select {
		case <-stop:
			return
		default:
		}
		keys := rc.or.Keys()
		if len(keys) == 0 {
			time.Sleep(realRetryDelay)
			continue
		}
		key := keys[rng.Intn(len(keys))]
		obs, err := getObservedReal(entrySel, rc.met, key)
		if err != nil {
			// Unreadable even after retry: the owner-killed/departed availability gap.
			// Not a hard violation; counted as turbulence.
			rc.met.transientReads.Add(1)
			time.Sleep(realRetryDelay)
			continue
		}
		vd, _ := rc.or.Verify(key, obs)
		rc.met.readsVerified.Add(1)
		if vd != VerdictPass {
			rc.met.transientReads.Add(1)
			// Re-verify with a bounded retry to decide transient vs persisted. A
			// PERSISTED wrong value is a hard violation even mid-run (no cutover excuses
			// serving a stale/corrupt/resurrected value). A persisted not-found mid-run is
			// the cutover transient (tolerated here; the post-settle final sweep is the
			// strict availability gate).
			if hardVd, hardDetail := rc.reverify(entrySel, key); isWrongValueVerdict(hardVd) {
				rc.recordWrongValue(hardVd, fmt.Sprintf("reader (persisted after re-verify): %s", hardDetail))
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// reverify re-reads key with a bounded retry to decide transient vs. persisted. It
// returns VerdictPass as soon as a read matches the model (a transient cutover
// artifact converged). If reads keep returning a WRONG VALUE, it returns that
// wrong-value verdict (the caller fails on it). A not-found / unreadable read is
// NOT a wrong value: it is the availability gap, so reverify returns VerdictPass for
// it (the caller's isWrongValueVerdict filter then ignores it).
func (rc *realHarness) reverify(entrySel func() *realNode, key string) (Verdict, string) {
	deadline := time.Now().Add(8 * time.Second)
	var lastVd Verdict
	var lastDetail string
	for time.Now().Before(deadline) {
		obs, err := getObservedBudgetReal(entrySel, rc.met, key, 1*time.Second)
		if err != nil {
			// Unreadable: the availability gap, not a wrong value. Treat as pass.
			lastVd, lastDetail = VerdictPass, ""
			time.Sleep(realRetryDelay)
			continue
		}
		vd, detail := rc.or.Verify(key, obs)
		if vd == VerdictPass {
			return VerdictPass, ""
		}
		if !isWrongValueVerdict(vd) {
			// A not-found for a live key (VerdictLost) is the availability gap here, not
			// a wrong value: do not let it count as a hard verdict. Keep retrying in case
			// the key is mid-cutover, but never escalate not-found to a violation.
			lastVd, lastDetail = VerdictPass, ""
			time.Sleep(realRetryDelay)
			continue
		}
		lastVd, lastDetail = vd, detail
		time.Sleep(realRetryDelay)
	}
	return lastVd, lastDetail
}

// isWrongValueVerdict reports whether vd is a "served a WRONG value" verdict.
// VerdictStale (an older acked value re-surfaced), VerdictCorrupt (a never-acked
// payload), and VerdictResurrected (a deleted key served a value) are all real
// durability/correctness bugs the continuous reader hard-gates the moment they
// PERSIST past the reverify budget. VerdictLost (a not-found for a live key) is NOT
// in this set: during the run it is the mid-cutover transient (the reader tolerates
// it + retries). The post-settle FINAL SWEEP and the durability PROBE are where a
// persisted not-found becomes a hard loss - because by then the handoff has had
// time to settle and a survivor MUST be serving the unit.
func isWrongValueVerdict(vd Verdict) bool {
	return vd == VerdictStale || vd == VerdictCorrupt || vd == VerdictResurrected
}

// runDurabilityKill is the load-bearing durable-handoff event. It SIGKILLs a
// non-founder node mid-workload, waits for the cluster to settle the handoff (the
// survivors re-acquire the dead node's unit leases + re-OpenUnit them from object
// storage), then reads EVERY acked key from the SURVIVING cluster. In multi-backend
// mode a killed owner's keys MUST come back: a survivor serves them off the same
// durable slatedb databases the dead node owned. So the probe HARD-gates both
// classes - a WRONG value (stale/corrupt/resurrected) AND a key that stays
// unreadable past the settle budget are violations. durSurvived counts the keys a
// survivor served correctly across the hard process death.
func (rc *realHarness) runDurabilityKill() {
	live := rc.top.liveNodeIDs()
	if len(live) < 2 {
		rc.logf("durability-kill: fewer than 2 live nodes; skipping the kill")
		return
	}
	// Pick a non-founder victim (the founder is the stable entry anchor).
	victim := ""
	for _, id := range live {
		if id != rc.top.founder.id {
			victim = id
			break
		}
	}
	if victim == "" {
		return
	}

	ackedBefore := rc.met.ackedPuts.Load()
	rc.logf("durability-kill: SIGKILL %s mid-workload (acked puts so far: %d)", victim, ackedBefore)
	if err := rc.top.KillNode(victim); err != nil {
		rc.logf("durability-kill: KillNode(%s): %v", victim, err)
		return
	}
	rc.met.evKill.Add(1)

	// Let memberlist fail-detect the dead peer + the survivors re-acquire its unit
	// leases and re-OpenUnit them from object storage (the copy-free handoff). This
	// is a few gossip intervals + a real OpenUnit round-trip per unit, so allow
	// generously before the strict probe.
	time.Sleep(6 * time.Second)

	rc.durabilityProbe()
	rc.logf("durability-kill: probe done: survived=%d of ~%d acked at kill (killed-owner keys re-served by a survivor after handoff)",
		rc.met.durSurvived.Load(), ackedBefore)
}

// restartAfterKill brings the killed node's replacement back (a fresh process,
// reseeded at the founder) and confirms the cluster re-converges.
func (rc *realHarness) restartAfterKill() {
	if _, err := rc.top.AddNode(); err != nil {
		rc.logf("durability-kill: restart AddNode: %v", err)
		return
	}
	rc.met.evRestart.Add(1)
	rc.logf("durability-kill: node restarted; members now %d", rc.top.MemberCount())
}

// durabilityProbe reads every acked key from the SURVIVING cluster (a rotating live
// entry node, so a unit that handed off to any survivor is reachable) after a hard
// SIGKILL + a settle window, and classifies each:
//
//   - SURVIVED:   a survivor serves the CORRECT latest acked value across the hard
//     process death + the copy-free handoff (the durable-handoff success - the
//     write was made durable in object storage before its ack, and a survivor
//     re-OpenUnit'd the unit and reads it back exactly).
//   - VIOLATION:  a survivor serves a WRONG value (stale/corrupt/resurrected) - a
//     real correctness bug. OR no live survivor can serve the key at all even after
//     the settle + a generous per-key retry - a real LOSS, because multi-backend
//     mode must have handed the dead owner's unit to a survivor. (Both fail the run;
//     this is the load-bearing difference from the legacy adapter, which tolerated
//     unavailability.)
func (rc *realHarness) durabilityProbe() {
	snap := rc.or.Snapshot()
	entrySel := rc.liveEntrySelector()
	if entrySel() == nil {
		rc.vlog.add(VerdictLost, "durability probe: no live entry node")
		return
	}
	for key := range snap {
		// A generous per-key budget: the unit may still be mid-handoff when the probe
		// reaches this key, and the retry rides the cutover out. Past the budget, the
		// key is genuinely unreachable - a survivor never re-served the dead owner's
		// unit, which is a real durable-handoff loss.
		obs, rerr := getObservedBudgetReal(entrySel, rc.met, key, 15*time.Second)
		if rerr != nil {
			rc.vlog.add(VerdictLost, fmt.Sprintf("durability probe: key %q unreadable on every survivor past the handoff settle (a killed owner's unit was not handed off): %v", key, rerr))
			continue
		}
		vd, detail := rc.or.Verify(key, obs)
		rc.met.readsVerified.Add(1)
		switch {
		case vd == VerdictPass:
			rc.met.durSurvived.Add(1)
		case isWrongValueVerdict(vd):
			rc.vlog.add(vd, fmt.Sprintf("durability probe: %s", detail))
		default:
			// VerdictLost: a not-found for a live key. Past the settle + the per-key
			// budget this is a genuine loss (the unit should be served by a survivor).
			rc.vlog.add(vd, fmt.Sprintf("durability probe (not-found after settle): %s", detail))
		}
	}
}

// liveEntrySelector returns a selector that picks a live entry node each call
// (re-evaluated so a node killed mid-probe is skipped). Used by the durability
// probe + final sweep to read from whichever survivor currently owns a unit.
func (rc *realHarness) liveEntrySelector() func() *realNode {
	var i int
	return func() *realNode {
		i++
		return rc.top.EntryNode(i)
	}
}

// runReshard drives a real cluster-coordinated doubling (N -> 2N) against the live
// workload via the topology's Reshard seam - the deploy-gap proof. With declarative
// resharding (v0.8) that seam triggers the doubling by bumping the desired
// --unit-count and re-rolling the nodes; the cluster's coordinator drives the freeze
// barrier to completion (each unit bisects by copying its keys through object
// storage, routing flips atomically). The workload keeps running; the writers'
// freeze-window refusals are retried (not acked-then-lost). On a committed doubling
// it records the {from, to} transition + bumps the reshard event count (which the
// vacuous-check requires). A refused / not-yet-committed reshard leaves the
// generation unchanged; it is logged + retried, since the run is vacuous without a
// committed reshard.
//
// The Reshard seam triggers the doubling the DECLARATIVE way (v0.8): it bumps the
// desired --unit-count and re-rolls the nodes (the rolling redeploy), then polls
// GenState until the coordinator commits N -> 2N. There is no operator Reshard RPC.
// See adapter_real.go realTopology.Reshard.
func (rc *realHarness) runReshard() {
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		from, to, err := rc.top.Reshard()
		if err == nil {
			rc.met.reshardFrom.Store(uint64(from))
			rc.met.reshardTo.Store(uint64(to))
			rc.met.evReshard.Add(1)
			rc.logf("reshard: COMMITTED %d -> %d (declarative trigger: --unit-count re-roll, coordinator-driven doubling)", from, to)
			// Let the post-reshard unit redistribution settle before the churn phase
			// piles membership changes on top.
			time.Sleep(3 * time.Second)
			return
		}
		// A refused reshard (in-flight / ceiling / unstable membership / a transient
		// BISECT fence from a lease still settling) leaves the generation unchanged, so
		// it is safe to retry. Settle a beat and try again - the run is vacuous without
		// a committed reshard, so it is worth a few attempts.
		rc.logf("reshard: attempt %d/%d failed: %v (retrying)", attempt+1, maxAttempts, err)
		rc.waitMembersStable(10 * time.Second)
		time.Sleep(2 * time.Second)
	}
	rc.logf("reshard: NO reshard committed after %d attempts (the run will report VACUOUS)", maxAttempts)
}

// runScheduler injects random join/leave/kill+restart churn for the remaining
// window. Every kill is a real SIGKILL; every join a real process launch. The
// oracle checks no acked write the deploy is responsible for is lost across them.
func (rc *realHarness) runScheduler(rng *rand.Rand, window time.Duration, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	deadline := time.Now().Add(window)
	for {
		select {
		case <-stop:
			return
		default:
		}
		if time.Now().After(deadline) {
			return
		}
		// Inter-event sleep, interruptible.
		sleep := time.Duration(float64(2*time.Second) * (0.5 + rng.Float64()))
		if !sleepOrStopReal(stop, sleep) {
			return
		}

		members := rc.top.MemberCount()
		switch {
		case members <= 1:
			rc.doJoin()
		default:
			switch rng.Intn(3) {
			case 0:
				rc.doKillRestart(rng)
			case 1:
				rc.doJoin()
			default:
				rc.doLeave(rng)
			}
		}
	}
}

func (rc *realHarness) doKillRestart(rng *rand.Rand) {
	victim := rc.randomVictim(rng)
	if victim == "" {
		return
	}
	if err := rc.top.KillNode(victim); err != nil {
		return
	}
	rc.met.evKill.Add(1)
	time.Sleep(time.Duration(float64(1*time.Second) * (0.5 + rng.Float64())))
	if _, err := rc.top.AddNode(); err == nil {
		rc.met.evRestart.Add(1)
	}
}

func (rc *realHarness) doJoin() {
	if _, err := rc.top.AddNode(); err == nil {
		rc.met.evJoin.Add(1)
	}
}

func (rc *realHarness) doLeave(rng *rand.Rand) {
	victim := rc.randomVictim(rng)
	if victim == "" {
		return
	}
	if err := rc.top.RemoveNode(victim); err == nil {
		rc.met.evLeave.Add(1)
	}
}

// randomVictim returns a random non-founder live node id, or "" if none.
func (rc *realHarness) randomVictim(rng *rand.Rand) string {
	live := rc.top.liveNodes()
	candidates := make([]string, 0, len(live))
	for _, n := range live {
		if n.id != rc.top.founder.id {
			candidates = append(candidates, n.id)
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	return candidates[rng.Intn(len(candidates))]
}

// finalSweep is the STRICT end-state gate. After the workload + chaos stop and the
// cluster settles to a stable membership (waitMembersStable), it reads every acked
// key from a rotating live entry node (so a unit on any survivor is reachable) with
// a generous per-key budget. Because there is NO cutover in flight after settle,
// the full no-acked-write-loss rule is unconditional here: a key must read back its
// CORRECT latest acked value, and a not-found / unreadable key (past the budget) is
// a real LOSS - multi-backend mode must have every unit mounted on its ring owner.
// This is the end-state belt to the continuous reader's suspenders.
func (rc *realHarness) finalSweep() {
	snap := rc.or.Snapshot()
	entrySel := rc.liveEntrySelector()
	if entrySel() == nil {
		rc.vlog.add(VerdictLost, "final sweep: no live entry node")
		return
	}
	for key := range snap {
		obs, err := getObservedBudgetReal(entrySel, rc.met, key, 15*time.Second)
		if err != nil {
			// Unreadable on every survivor after settle: a genuine loss (the unit holding
			// this key is mounted nowhere). This is the strict availability gate the
			// continuous reader defers to.
			rc.vlog.add(VerdictLost, fmt.Sprintf("final sweep: key %q unreadable on every live node after settle (unit not mounted on its ring owner)", key))
			continue
		}
		vd, detail := rc.or.Verify(key, obs)
		rc.met.readsVerified.Add(1)
		switch {
		case vd == VerdictPass:
			// correct value served; nothing to record.
		case isWrongValueVerdict(vd):
			rc.recordWrongValue(vd, fmt.Sprintf("final sweep: %s", detail))
		default:
			// VerdictLost: not-found for a live acked key after settle - a real loss.
			rc.recordWrongValue(vd, fmt.Sprintf("final sweep (not-found after settle): %s", detail))
		}
	}
}

// waitMembersStable waits until the founder's member count is non-zero + steady
// across two reads (a coarse convergence gate before the strict final sweep).
func (rc *realHarness) waitMembersStable(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	prev := -1
	for time.Now().Before(deadline) {
		cur := rc.top.MemberCount()
		if cur > 0 && cur == prev {
			return
		}
		prev = cur
		time.Sleep(500 * time.Millisecond)
	}
}

// waitClusterReadable blocks until a SAMPLE of acked keys reads back cleanly
// through a rotating live entry node (proving the unit leases have settled after
// the last reshard / churn event), or the timeout passes. It samples up to ~40
// keys from the oracle snapshot and requires the WHOLE sample to be readable in
// one pass with a short per-key budget; any miss re-arms the loop. This gates the
// O(keys x budget) final sweep so it does not start while units are still moving.
// It is purely a settle gate - it records no verdicts and never fails the run; the
// strict per-key correctness check is the finalSweep that follows.
func (rc *realHarness) waitClusterReadable(timeout time.Duration) {
	snap := rc.or.Snapshot()
	if len(snap) == 0 {
		return
	}
	// Take up to 40 sample keys (deterministic order not needed; map iteration is
	// fine for a liveness probe).
	const sampleSize = 40
	sample := make([]string, 0, sampleSize)
	for k := range snap {
		sample = append(sample, k)
		if len(sample) >= sampleSize {
			break
		}
	}
	entrySel := rc.liveEntrySelector()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allReadable := true
		for _, k := range sample {
			obs, err := getObservedBudgetReal(entrySel, rc.met, k, 3*time.Second)
			if err != nil {
				allReadable = false
				break
			}
			// A not-found is acceptable here only if the oracle expects a delete;
			// otherwise treat it as not-yet-settled. We do not verify the value (that
			// is finalSweep's job) - just that the routed unit serves the key.
			vd, _ := rc.or.Verify(k, obs)
			if vd == VerdictLost {
				allReadable = false
				break
			}
		}
		if allReadable {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// sleepOrStopReal sleeps for d but returns false early if stop closes.
func sleepOrStopReal(stop <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stop:
		return false
	case <-t.C:
		return true
	}
}
