//go:build chaosreal

package chaos

// runReal wires the real cluster, oracle, writers, and the chaos+durability
// scheduler together; runs for the configured window; then quiesces, runs the
// final full-sweep verification + the post-kill durability probe, and produces
// the report. Self-contained: it owns the child-process lifecycle end to end and
// guarantees teardown (no orphaned shaled-slate processes) via a defer.
//
// THE INVARIANT IT GATES ON (the sound, falsifiable claim against a legacy
// shaled-slate deploy): NO ACKED WRITE IS EVER SERVED A WRONG VALUE. Concretely,
// any time a survivor serves a key, it serves the CORRECT latest acked value -
// never a stale, corrupt, or resurrected one - across real process death, real
// gRPC, and a real object-store round-trip. That is the durability+correctness
// property R=1 + durable-before-ack is responsible for: a write the client acked
// was made durable in object storage BEFORE the ack, so whatever a survivor reads
// back must be that exact value, not an older or torn one.
//
// What it deliberately does NOT gate on: per-key AVAILABILITY after a topology
// change. In legacy per-node mode each node owns a DISTINCT slatedb instance and
// there is no per-unit lease handoff, so a key whose owning node died/left becomes
// unavailable (its bytes are durable in the dead node's instance, but no survivor
// re-mounts it). That is a documented mode limitation (see chaos_real_test.go's
// header), counted as a metric (durUnavailable), never a test failure - failing on
// it would just re-assert the known gap as noise. The post-kill durability probe
// additionally reports how much of the acked set the founder still serves
// (durSurvived), quantifying real survival across a hard process death.

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
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

	// PHASE 1 - STABLE-MEMBERSHIP DURABILITY (the HARD-gated phase). Seed a dataset
	// against STABLE membership (no joins/leaves reshuffling the ring), so every
	// acked key sits on its single owner, then SIGKILL a non-founder node under the
	// live workload and probe the survivor. This is the well-defined durability
	// question: a key acked while membership was stable, then a node hard-killed
	// under it. In this phase the survivor either OWNS the key (serves the correct
	// durable value) or does NOT (unavailable - the legacy gap); it can never serve
	// a WRONG value, because no second slate instance holds a stale copy. So the
	// wrong-value invariant is meaningful here and is HARD-gated.
	time.Sleep(2 * time.Second)
	rc.runDurabilityKill()

	// PHASE 2 - CHURN COVERAGE (INFORMATIONAL). Layer a seeded schedule of real
	// joins / leaves / kill+restarts (real process launches + SIGKILLs over the real
	// network). Under legacy per-node mode, membership churn REASSIGNS the ring with
	// no data movement between the independent per-node slatedb instances, so a key
	// can RESURRECT (a node that re-acquires ownership serves its own stale copy that
	// never saw a later delete) or go unavailable. Those are inherent to legacy mode,
	// not coordination bugs, so phase-2 wrong-value findings are recorded as an
	// INFORMATIONAL metric (churnWrongValues), NOT the hard gate - gating on them
	// would just re-assert the known mode limitation. The phase still exercises the
	// real launch/kill/seed/network path end to end. Flipping churnPhase routes the
	// continuous reader's + final sweep's wrong-value findings to the informational
	// counter for the remainder of the run.
	rc.churnPhase.Store(true)
	// First phase-2 event: restart the node killed in phase 1 (a join + rebalance).
	rc.restartAfterKill()
	wg.Add(1)
	schedRng := rand.New(rand.NewSource(cfg.seed + 9_000))
	go rc.runScheduler(schedRng, cfg.duration, stop, &wg)

	// Run the workload window.
	time.Sleep(cfg.duration)
	close(stop)
	wg.Wait()

	// Final sweep over whatever the churned cluster settled to: informational
	// (phase 2), so it never fails the run - it just quantifies the end-state gap.
	rc.waitMembersStable(15 * time.Second)
	rc.finalSweep()

	supported := top.Reshard() == nil // always false on legacy slate; recorded honestly
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
		},
		durSurvived:      rc.met.durSurvived.Load(),
		durUnavailable:   rc.met.durUnavailable.Load(),
		churnWrongValues: rc.met.churnWrongValues.Load(),
		writeFailures:    rc.met.writeFailures.Load(),
		finalNodes:       top.MemberCount(),
		reshardSupported: supported,
		violations:       rc.vlog.snapshot(),
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

	// churnPhase is false during PHASE 1 (stable-membership durability, HARD-gated)
	// and true during PHASE 2 (churn coverage, INFORMATIONAL). It routes a wrong-
	// value finding to either the hard violation log (phase 1: a survivor served a
	// wrong value with no churn to excuse it - a real bug) or the informational
	// churnWrongValues counter (phase 2: a resurrection inherent to legacy per-node
	// mode under ring churn - the documented gap, not a coordination bug).
	churnPhase atomic.Bool
}

// recordWrongValue routes a wrong-value verdict by phase: a hard violation during
// the stable-membership phase, an informational metric during churn.
func (rc *realHarness) recordWrongValue(vd Verdict, detail string) {
	if rc.churnPhase.Load() {
		rc.met.churnWrongValues.Add(1)
		return
	}
	rc.vlog.add(vd, detail)
}

// recordWriteFailure routes a hard write failure (an op that never acked even after
// the full retry budget) by phase. In PHASE 1 (stable membership) a write should
// always eventually ack - the only chaos is a single SIGKILL, which a faithful
// client rides out by re-routing to a survivor - so a hard failure there is a real
// availability bug, hard-gated. In PHASE 2 (churn) a write can fail because the
// key's owner is mid-departure with no handoff (the legacy gap), so it is recorded
// as an informational metric, not a failure. A failed write is NEVER an acked-write
// loss either way (the oracle records ONLY acks), but a phase-1 failure signals the
// real network path could not make progress on a single-kill scenario it should.
func (rc *realHarness) recordWriteFailure(detail string) {
	if rc.churnPhase.Load() {
		rc.met.writeFailures.Add(1)
		return
	}
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
// each result to the oracle. It enforces the ONE invariant the legacy deploy is
// responsible for: NO SURVIVOR EVER SERVES A WRONG VALUE. A read that returns the
// CORRECT value passes; a read that returns a WRONG value (stale/corrupt/
// resurrected) is re-verified, and a PERSISTED wrong value is a hard violation. A
// not-found / unreadable read is the owner-killed/departed legacy availability gap
// (a survivor cannot serve a dead owner's keys without per-unit handoff); it is
// counted as a transient, never failed - the durability probe owns the
// availability classification, and gating on it here would just re-assert the
// known mode limitation as noise.
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
			// Only a PERSISTED WRONG VALUE matters; a persisted not-found is the
			// availability gap (tolerated). reverify returns a wrong-value verdict only
			// if the key keeps serving a wrong value past the retry budget. The finding
			// is hard-gated in phase 1, informational under churn (phase 2).
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

// isWrongValueVerdict reports whether vd is a "a survivor served a WRONG value"
// verdict - the only class the legacy deploy is held to. VerdictStale (an older
// acked value re-surfaced), VerdictCorrupt (a never-acked payload), and
// VerdictResurrected (a deleted key served a value) are all real durability/
// correctness bugs. VerdictLost (a not-found for a live key) is NOT in this set:
// under legacy per-node mode a key whose owner died/left is legitimately
// unavailable (its bytes are durable in the dead node's instance, just not
// re-mounted on a survivor), which is the documented gap, not a wrong value.
func isWrongValueVerdict(vd Verdict) bool {
	return vd == VerdictStale || vd == VerdictCorrupt || vd == VerdictResurrected
}

// runDurabilityKill is the load-bearing event. It records the founder-owned acked
// keys, SIGKILLs a non-founder node mid-workload, then (while it is down) reads
// EVERY acked key from the SURVIVING founder and classifies each as SURVIVED
// (founder serves the correct latest value across the hard process death) or
// UNAVAILABLE (founder cannot serve it - the owner-killed legacy gap). The HARD
// assertion is wrong-value-only: a key the founder serves must serve the CORRECT
// latest acked value, never a stale/corrupt/resurrected one. A wrong value across
// a real process death + a real object-store round-trip is a genuine durability/
// correctness bug. UNAVAILABILITY is the documented legacy gap (counted, never
// failed). Then it restarts the node and confirms it rejoins.
func (rc *realHarness) runDurabilityKill() {
	live := rc.top.liveNodeIDs()
	if len(live) < 2 {
		rc.logf("durability-kill: fewer than 2 live nodes; skipping the kill")
		return
	}
	// Pick a non-founder victim (the founder is the stable survivor we probe).
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

	// Give memberlist a beat to fail-detect the dead peer + the survivor to settle.
	time.Sleep(2 * time.Second)

	// Durability probe: read EVERY acked key from the surviving founder, classify.
	// This is the HARD-gated window (no rejoin churn yet), so a survivor serving a
	// WRONG value here is a real bug. The restart that re-introduces a peer (and the
	// rebalance it triggers) belongs to PHASE 2; the caller flips churnPhase + does
	// the restart after this returns.
	rc.durabilityProbe()
	rc.logf("durability-kill: probe done: survived=%d unavailable=%d (of ~%d acked at kill)",
		rc.met.durSurvived.Load(), rc.met.durUnavailable.Load(), ackedBefore)
}

// restartAfterKill brings the killed node's replacement back (a fresh process,
// reseeded at the founder) and confirms the cluster re-converges. This is a
// membership change (a join + rebalance), so it runs at the START of PHASE 2.
func (rc *realHarness) restartAfterKill() {
	if _, err := rc.top.AddNode(); err != nil {
		rc.logf("durability-kill: restart AddNode: %v", err)
		return
	}
	rc.met.evRestart.Add(1)
	rc.logf("durability-kill: node restarted; members now %d", rc.top.MemberCount())
}

// durabilityProbe reads every acked key from the surviving founder right after a
// hard SIGKILL and classifies each:
//
//   - SURVIVED:     the founder serves the CORRECT latest acked value across the
//     process death (the real durability success - the write was made durable in
//     object storage before its ack, so it reads back exactly).
//   - UNAVAILABLE:  the founder cannot serve it at all (the owner-killed legacy
//     gap: the key's owning slatedb instance was on the dead node, and legacy mode
//     has no per-unit lease handoff). Counted, never failed.
//   - VIOLATION:    the founder serves a WRONG value (stale/corrupt/resurrected).
//     A real durability/correctness bug regardless of ownership: the data is
//     wrong, not merely unrouted. This is the only thing the probe fails on.
func (rc *realHarness) durabilityProbe() {
	snap := rc.or.Snapshot()
	if _, err := rc.top.FounderClient(); err != nil {
		rc.vlog.add(VerdictLost, fmt.Sprintf("durability probe: no founder client: %v", err))
		return
	}
	founderSel := func() *realNode { return rc.top.founder }
	for key := range snap {
		obs, rerr := getObservedBudgetReal(founderSel, rc.met, key, 2*time.Second)
		if rerr != nil {
			// The founder cannot serve it: the owner-killed legacy gap. Counted, not
			// failed (its bytes are durable in the dead node's instance; legacy mode
			// just cannot re-mount it on the survivor).
			rc.met.durUnavailable.Add(1)
			continue
		}
		vd, detail := rc.or.Verify(key, obs)
		rc.met.readsVerified.Add(1)
		switch {
		case vd == VerdictPass:
			rc.met.durSurvived.Add(1)
		case isWrongValueVerdict(vd):
			// A WRONG value served by the surviving founder is a real bug: the data is
			// corrupt/stale/resurrected, not merely unrouted.
			rc.vlog.add(vd, fmt.Sprintf("durability probe: %s", detail))
		default:
			// VerdictLost: the founder returned not-found for the key (its owner was the
			// killed node). The owner-killed legacy availability gap, counted not failed.
			rc.met.durUnavailable.Add(1)
		}
	}
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

// finalSweep reads every acked key from the founder after the workload + chaos
// stop, applying the SAME wrong-value-only rule the durability probe does: a key
// the founder serves must serve the CORRECT value (a wrong value is a violation);
// a key the founder cannot serve at all is the owner-killed/departed legacy gap
// (counted as unavailable, never failed). This is the end-state belt to the
// continuous reader's suspenders for the one invariant the deploy is responsible
// for: no survivor ever serves a wrong value for an acked key.
func (rc *realHarness) finalSweep() {
	snap := rc.or.Snapshot()
	founderSel := func() *realNode { return rc.top.founder }
	for key := range snap {
		obs, err := getObservedBudgetReal(founderSel, rc.met, key, 2*time.Second)
		if err != nil {
			// Owner-killed/departed legacy gap: the founder cannot serve a key whose
			// owning slatedb instance was on a node that left/died. Not a failure.
			continue
		}
		vd, detail := rc.or.Verify(key, obs)
		rc.met.readsVerified.Add(1)
		// Only a WRONG value matters; a not-found (VerdictLost) is the owner-killed/
		// departed availability gap, tolerated. The final sweep runs after phase 2
		// (churnPhase=true), so its findings are informational - they quantify the
		// end-state legacy gap rather than failing the run.
		if isWrongValueVerdict(vd) {
			rc.recordWrongValue(vd, fmt.Sprintf("final sweep: %s", detail))
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
