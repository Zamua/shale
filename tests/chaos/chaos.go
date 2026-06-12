//go:build chaos

package chaos

// The reader loop, the chaos scheduler, the run orchestration, and the final
// sweep + report. The scheduler injects node kill+restart, join, leave, and
// reshard - plus the two high-value COMBINATIONS (reshard while a node is down;
// a membership change mid-reshard that must ABORT cleanly) - on a seeded
// schedule. Every decision is drawn from the seeded RNG so a failing run replays
// from its logged seed.

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Zamua/shale/pkg/cluster"
)

// runReader is one reader goroutine. It repeatedly picks a known key (one with an
// acked write) and a rotating entry node, reads it with the standard retry, and
// feeds the result to the oracle. A LOST/STALE/CORRUPT/RESURRECTED verdict is a
// violation recorded immediately, so the oracle is checked CONTINUOUSLY through
// chaos, not only at the end. A read that fails even after the full retry budget
// is itself a read-availability violation (req 6).
func (h *harness) runReader(id int, rng *rand.Rand, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	entrySel := func() *cluster.Cluster { return h.cl.EntryNode(rng.Intn(1 << 20)) }
	for {
		select {
		case <-stop:
			return
		default:
		}
		keys := h.or.Keys()
		if len(keys) == 0 {
			time.Sleep(retryDelay)
			continue
		}
		key := keys[rng.Intn(len(keys))]
		obs, err := h.getObserved(entrySel, key)
		if err != nil {
			h.vlog.add(VerdictLost, fmt.Sprintf("reader %d Get(%q) failed even after retry budget (read-availability violated): %v", id, key, err))
			continue
		}
		vd, _ := h.or.Verify(key, obs)
		h.met.readsVerified.Add(1)
		if vd != VerdictPass {
			// A single non-PASS read mid-chaos can be a TRANSIENT cutover artifact: a
			// reshard/handoff briefly routed to a stale generation/owner that has an
			// older value (or has not yet seen a just-acked delete) while its ring
			// catches up. shale's contract is retryable-availability + eventual
			// correctness, not per-read linearizability across a cutover, so we do not
			// fail on the first wrong read. We RE-VERIFY with a bounded retry: if the
			// key converges to its correct value, it was the expected transient and we
			// count it as such; if it STAYS wrong past the retry budget, that is a real
			// violation (a genuinely lost/stale/resurrected acked write). This mirrors
			// the existing reshard gate's "read through the whole window with retry,
			// never a permanently wrong value" assertion.
			h.met.transientReads.Add(1)
			if hardVd, hardDetail := h.reverify(entrySel, key); hardVd != VerdictPass {
				h.vlog.add(hardVd, fmt.Sprintf("reader %d (persisted after re-verify): %s", id, hardDetail))
			}
		}
		// A tiny pause keeps the readers from starving the writers/scheduler on a
		// single core; it does not gate correctness.
		time.Sleep(time.Millisecond)
	}
}

// reverify re-reads key with a bounded retry to decide whether a non-PASS read
// was a transient cutover artifact (the key converges to its correct value) or a
// persisted violation. It rotates the entry node so it does not keep hitting the
// same stale node, re-reads the model's expectation each attempt (the writer may
// legitimately advance the key concurrently), and returns VerdictPass as soon as
// any read matches the current model. Only if it never converges within the budget
// does it return the last non-PASS verdict + detail, which the caller logs as a
// hard violation. A read error (not a value) is rechecked too: a permanent error
// is itself a read-availability loss.
func (h *harness) reverify(entrySel func() *cluster.Cluster, key string) (Verdict, string) {
	deadline := time.Now().Add(reverifyBudget)
	var lastVd Verdict
	var lastDetail string
	for time.Now().Before(deadline) {
		obs, err := h.getObservedBudget(entrySel, key, 1*time.Second)
		if err != nil {
			lastVd, lastDetail = VerdictLost, fmt.Sprintf("key %q unreadable on re-verify: %v", key, err)
			time.Sleep(retryDelay)
			continue
		}
		vd, detail := h.or.Verify(key, obs)
		if vd == VerdictPass {
			return VerdictPass, ""
		}
		lastVd, lastDetail = vd, detail
		time.Sleep(retryDelay)
	}
	return lastVd, lastDetail
}

// reverifyBudget bounds how long a flagged read is re-checked before it is judged
// a real (persisted) violation rather than a transient cutover artifact. Generous
// enough to ride out a reshard FLIP + ring refresh.
const reverifyBudget = 10 * time.Second

// chaosEventKind enumerates the structural events the scheduler injects.
type chaosEventKind int

const (
	evKill chaosEventKind = iota
	evJoin
	evLeave
	evReshard
	evReshardWhileDown // combination: reshard while a node is down
	evLeaveMidReshard  // combination: membership change mid-reshard -> clean abort
)

// runScheduler injects chaos events until stop closes. Between events it sleeps a
// randomized interval around cfg.chaosEvery. It tracks the live node count + the
// live generation so it can guard events (never drop below one node; serialize a
// reshard against the unit-count ceiling). The scheduler is the ONLY mutator of
// topology + generation, so its bookkeeping is the authoritative live count.
func (h *harness) runScheduler(rng *rand.Rand, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	// liveGen tracks how many times we have successfully resharded (each doubles
	// the units), so we can refuse a reshard that would exceed a sane test ceiling
	// and report the final generation.
	liveGen := 0
	// Cap doublings so a long soak does not explode the unit space (each reshard
	// DOUBLES the units: at base 8 a cap of 5 tops out at 256 units, which keeps
	// the final full-sweep tractable). A reshard monotonically climbs liveGen
	// toward the cap (there is no halving), so the cap is also the total number of
	// reshards a single run can fire; 5 is enough for the reshard + the two reshard
	// COMBINATIONS to each fire a few times in a default soak before the ceiling.
	const maxGen = 5

	// COVERAGE PROLOGUE: deterministically fire each high-value scenario ONCE up
	// front, before the random schedule, so a default soak is GUARANTEED to
	// exercise the hard cases (reshard, reshard-while-down, leave-mid-reshard, join,
	// kill+restart) regardless of how few events the per-event settle cost lets a
	// given DURATION fire. Relying on the weighted RNG alone to eventually hit every
	// combination needs a long run; the prologue makes the default non-vacuous at a
	// reasonable length while the random schedule still piles adversarial churn on
	// top. The smoke variant SKIPS the prologue (it is meant to be fast + shallow).
	// Each prologue step that commits a reshard advances liveGen, same as the random
	// path. A step is best-effort: if a guard refuses it (too few live nodes, at the
	// reshard ceiling) it is simply skipped and the run continues.
	if !h.cfg.smoke {
		// The prologue runs to COMPLETION even if the workload-duration timer fires
		// mid-way (stop is NOT checked here): its whole purpose is to GUARANTEE the
		// hard cases ran, so cutting it short would defeat it. It is naturally
		// bounded (a fixed handful of events), and the writers/readers keep running
		// against it until stop; if stop lands mid-prologue the remaining steps still
		// fire against a quiescing workload, which the oracle still checks. Pick a
		// default DURATION comfortably above the prologue's wall cost so the random
		// schedule below also gets a turn.
		prologue := []chaosEventKind{evReshard, evReshardWhileDown, evLeaveMidReshard, evJoin, evKill}
		if h.cfg.noReshard {
			// DIAGNOSTIC: membership churn only, no generation change.
			prologue = []chaosEventKind{evJoin, evKill, evLeave}
		}
		if h.cfg.reshardOnly {
			// DIAGNOSTIC: plain reshards only, no membership churn.
			prologue = []chaosEventKind{evReshard, evReshard, evReshard}
		}
		for _, kind := range prologue {
			liveGen += h.fireEvent(kind, rng)
			h.cl.WaitSettled(h.cfg.units<<liveGen, h.cl.scaled(15*time.Second))
		}
	}

	for {
		select {
		case <-stop:
			h.met.finalNodes = h.cl.MemberCount()
			h.met.finalUnits = h.cfg.units << liveGen
			return
		default:
		}

		// Randomized inter-event sleep, interruptible by stop.
		sleep := jitterDuration(rng, h.cfg.chaosEvery)
		if !sleepOrStop(stop, sleep) {
			h.met.finalNodes = h.cl.MemberCount()
			h.met.finalUnits = h.cfg.units << liveGen
			return
		}

		nodes := h.cl.MemberCount()
		kind := h.pickEvent(rng, nodes, liveGen, maxGen)
		liveGen += h.fireEvent(kind, rng)
		// Settle the cluster to a clean, fully-mounted state before the next event
		// fires. Without this, a reshard or membership change whose redistribution
		// has not yet completed gets compounded by the next event, and the cluster
		// can fall far enough behind that it never catches up within the final
		// settle budget - leaving owner-but-unmounted units that make the final
		// sweep crawl. Settling between events keeps each event's churn bounded and
		// keeps the workload (which runs concurrently) reading a mostly-settled ring.
		// The workload's retries cover the brief windows this does not close.
		h.cl.WaitSettled(h.cfg.units<<liveGen, h.cl.scaled(15*time.Second))
	}
}

// fireEvent dispatches one chaos event of the given kind and returns the
// liveGen DELTA (1 if it committed a reshard, else 0). It is the single dispatch
// point shared by the deterministic coverage prologue and the random schedule, so
// the two paths stay in lockstep on how an event mutates the generation.
func (h *harness) fireEvent(kind chaosEventKind, rng *rand.Rand) int {
	switch kind {
	case evKill:
		h.doKillRestart(rng)
	case evJoin:
		h.doJoin()
	case evLeave:
		h.doLeave(rng)
	case evReshard:
		if h.doReshard() {
			return 1
		}
	case evReshardWhileDown:
		if h.doReshardWhileDown(rng) {
			return 1
		}
	case evLeaveMidReshard:
		// This intentionally drives the clean-abort path; on the rare timing where
		// the reshard committed before the membership change landed, it counts as a
		// successful reshard instead.
		if h.doLeaveMidReshard(rng) {
			return 1
		}
	}
	return 0
}

// pickEvent chooses the next event kind subject to guards. With <= 1 live node,
// only join is possible. With >= 2, the full menu is available, weighted toward
// the simple events with the combinations sprinkled in.
func (h *harness) pickEvent(rng *rand.Rand, liveNodes, liveGen, maxGen int) chaosEventKind {
	if liveNodes <= 1 {
		return evJoin
	}
	// reshardOnly DIAGNOSTIC: only plain reshards (until the ceiling), no membership
	// churn. Once at the ceiling there is nothing to do, so just reshard-attempt
	// (the guard refuses + it is a no-op delta).
	if h.cfg.reshardOnly {
		return evReshard
	}
	canReshard := liveGen < maxGen && !h.cfg.noReshard
	// Weighted menu. Numbers are relative weights. The per-event SETTLE +
	// convergence wait dominates wall clock, so a default soak fires only a modest
	// number of events; we therefore bias toward the RESHARD + the two COMBINATIONS
	// (the high-value adversarial cases the single gates cannot express, and the
	// ones the join-after-reshard gen path lives in) so they fire a meaningful
	// number of times rather than being crowded out by plain kills/joins. The
	// combinations themselves drive kill / reshard / leave underneath, so biasing
	// toward them still exercises the base operations. When the cluster is at the
	// reshard ceiling (liveGen == maxGen) the menu falls back to membership churn.
	type wk struct {
		kind chaosEventKind
		w    int
	}
	menu := []wk{
		{evKill, 20},
		{evJoin, 18},
		{evLeave, 12},
	}
	if canReshard {
		menu = append(menu,
			wk{evReshard, 20},
			wk{evReshardWhileDown, 15},
			wk{evLeaveMidReshard, 15},
		)
	}
	total := 0
	for _, m := range menu {
		total += m.w
	}
	r := rng.Intn(total)
	for _, m := range menu {
		if r < m.w {
			return m.kind
		}
		r -= m.w
	}
	return evKill
}

// doKillRestart hard-kills a random non-founder live node, waits a randomized
// down-interval (during which survivors re-acquire its units and the workload
// must keep acking on them), then starts a fresh node to model the restart
// rejoining. Counts a kill + a restart event.
func (h *harness) doKillRestart(rng *rand.Rand) {
	victim := h.randomVictim(rng)
	if victim == "" {
		return
	}
	if err := h.cl.KillNode(victim); err != nil {
		// Refused (e.g. last node) - benign, skip.
		return
	}
	h.met.evKill.Add(1)
	// Down interval: long enough that the workload genuinely runs against the
	// reduced membership (survivors must have re-acquired the dead node's units).
	down := jitterDuration(rng, 400*time.Millisecond)
	time.Sleep(down)
	if _, err := h.cl.AddNode(); err == nil {
		h.met.evRestart.Add(1)
	}
}

// doJoin adds a fresh node (the Phase 3 lease handoff redistributes units to it).
func (h *harness) doJoin() {
	if _, err := h.cl.AddNode(); err == nil {
		h.met.evJoin.Add(1)
	}
}

// doLeave gracefully removes a random non-founder live node (survivors converge +
// re-acquire its units).
func (h *harness) doLeave(rng *rand.Rand) {
	victim := h.randomVictim(rng)
	if victim == "" {
		return
	}
	if err := h.cl.RemoveNode(victim); err == nil {
		h.met.evLeave.Add(1)
	}
}

// doReshard runs a plain coordinated reshard (doubling). Returns true if it
// committed (so the scheduler advances its generation bookkeeping).
func (h *harness) doReshard() bool {
	err := h.cl.Reshard()
	if err == nil {
		h.met.evReshard.Add(1)
		return true
	}
	// A reshard can legitimately fail/abort if a membership change is racing it;
	// that is the abort path, counted separately. Not committing is fine - the
	// oracle proves no write was lost either way.
	h.met.evReshardAbrt.Add(1)
	return false
}

// doReshardWhileDown is the FIRST combination: kill a node, then (while it is
// down) reshard over the reduced live membership, then restart the node so it
// rejoins at the new generation and acquires its units. Returns true if the
// reshard committed. The oracle proves no acked write was lost across the whole
// dance.
func (h *harness) doReshardWhileDown(rng *rand.Rand) bool {
	victim := h.randomVictim(rng)
	if victim == "" {
		return false
	}
	if err := h.cl.KillNode(victim); err != nil {
		return false
	}
	h.met.evKill.Add(1)
	// Record that the COMBINATION fired (a node went down with a reshard to follow),
	// distinct from a plain kill, so the report proves this hard case ran.
	h.met.evReshardWhileDown.Add(1)
	// SETTLE the post-kill handoff before resharding: the survivors must have
	// RE-ACQUIRED (re-mounted) the dead node's units before the reshard, or the
	// bisect - which only bisects MOUNTED old units (mountedOldUnits) - would skip
	// the not-yet-reacquired units and their keys would never reach a gen-(g+1)
	// child, losing them at FLIP. The in-memory double re-mounts instantly so this
	// is invisible there; a real slatedb re-acquire is an object-store round-trip,
	// so the harness must wait for the handoff to complete (every unit mounted on
	// its ring owner) before triggering the reshard. The reshard's freeze barrier
	// assumes a settled membership+mount; this makes the harness honor that. Pass 0
	// for expectedUnits: the scheduler's liveGen is not threaded here, but
	// WaitSettled with 0 still enforces the property we need (every unit mounted on
	// its ring owner, contiguous, one generation) without asserting an exact count.
	h.cl.WaitSettled(0, h.cl.scaled(15*time.Second))
	// Reshard over the survivors while the victim is down.
	committed := false
	if err := h.cl.Reshard(); err == nil {
		h.met.evReshard.Add(1)
		h.met.evReshardWhileDownC.Add(1) // the reshard committed WHILE a node was down
		committed = true
	} else {
		h.met.evReshardAbrt.Add(1)
	}
	// Restart the downed node: it rejoins at the (possibly advanced) generation.
	if _, err := h.cl.AddNode(); err == nil {
		h.met.evRestart.Add(1)
	}
	return committed
}

// doLeaveMidReshard is the SECOND combination: kick off a reshard ASYNC, then -
// while the freeze barrier is in flight - fire a membership change (a graceful
// leave). The concurrent membership change must ABORT the reshard cleanly: every
// node unfreezes, discards half-built children, stays at gen g, writes resume.
// The oracle then proves zero loss. On the rare timing where the reshard
// committed before the leave landed, it counts as a committed reshard. Returns
// true iff the reshard committed.
func (h *harness) doLeaveMidReshard(rng *rand.Rand) bool {
	// Record that the COMBINATION fired (a reshard kicked off with a membership
	// change to land mid-barrier), distinct from a plain leave/reshard, so the
	// report proves this hard case ran.
	h.met.evLeaveMidReshard.Add(1)
	done := h.cl.ReshardAsync()
	// Give the barrier a moment to engage (freeze), then fire the membership
	// change so it lands mid-barrier.
	time.Sleep(jitterDuration(rng, 30*time.Millisecond))
	victim := h.randomVictim(rng)
	if victim != "" {
		if err := h.cl.RemoveNode(victim); err == nil {
			h.met.evLeave.Add(1) // the membership change itself landed
		}
	}
	err := <-done
	if err == nil {
		h.met.evReshard.Add(1)
		return true
	}
	// Aborted (the expected outcome of a mid-reshard membership change): count it
	// as both a reshard-abort AND distinctly as a leave-mid-reshard that drove the
	// clean-abort path, so the report proves the abort case actually exercised.
	h.met.evReshardAbrt.Add(1)
	h.met.evLeaveMidReshardAb.Add(1)
	return false
}

// randomVictim returns the id of a random live node that is NOT the founder
// (member index 0), or "" if there is no eligible victim. Keeping the founder
// alive means the harness always has a stable seed/entry node + avoids churning
// the whole cluster at once.
func (h *harness) randomVictim(rng *rand.Rand) string {
	live := h.cl.liveNodes()
	if len(live) <= 1 {
		return ""
	}
	// Exclude index 0 (treat the first-started live node as the founder anchor).
	candidates := live[1:]
	if len(candidates) == 0 {
		return ""
	}
	return candidates[rng.Intn(len(candidates))].id
}

// jitterDuration returns d scaled by a random factor in [0.5, 1.5), so events do
// not fall on a fixed cadence (which could resonate with the reconcile timer).
func jitterDuration(rng *rand.Rand, d time.Duration) time.Duration {
	factor := 0.5 + rng.Float64() // [0.5, 1.5)
	return time.Duration(float64(d) * factor)
}

// sleepOrStop sleeps for d but returns false early if stop closes during the
// sleep. Used so the scheduler's inter-event wait is interruptible at shutdown.
func sleepOrStop(stop <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stop:
		return false
	case <-t.C:
		return true
	}
}
