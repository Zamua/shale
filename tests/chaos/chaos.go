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

func (k chaosEventKind) String() string {
	switch k {
	case evKill:
		return "kill+restart"
	case evJoin:
		return "join"
	case evLeave:
		return "leave"
	case evReshard:
		return "reshard"
	case evReshardWhileDown:
		return "reshard-while-down"
	case evLeaveMidReshard:
		return "leave-mid-reshard"
	default:
		return "unknown"
	}
}

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
	const maxGen = 3 // cap doublings so a long soak does not explode the unit space

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
		switch kind {
		case evKill:
			h.doKillRestart(rng)
		case evJoin:
			h.doJoin()
		case evLeave:
			h.doLeave(rng)
		case evReshard:
			if h.doReshard() {
				liveGen++
			}
		case evReshardWhileDown:
			if h.doReshardWhileDown(rng) {
				liveGen++
			}
		case evLeaveMidReshard:
			// This intentionally drives the clean-abort path; on the rare timing
			// where the reshard committed before the membership change landed, it
			// counts as a successful reshard instead.
			if h.doLeaveMidReshard(rng) {
				liveGen++
			}
		}
		// Settle the cluster to a clean, fully-mounted state before the next event
		// fires. Without this, a reshard or membership change whose redistribution
		// has not yet completed gets compounded by the next event, and the cluster
		// can fall far enough behind that it never catches up within the final
		// settle budget - leaving owner-but-unmounted units that make the final
		// sweep crawl. Settling between events keeps each event's churn bounded and
		// keeps the workload (which runs concurrently) reading a mostly-settled ring.
		// The workload's retries cover the brief windows this does not close.
		h.cl.WaitSettled(h.cfg.units<<liveGen, 15*time.Second)
	}
}

// pickEvent chooses the next event kind subject to guards. With <= 1 live node,
// only join is possible. With >= 2, the full menu is available, weighted toward
// the simple events with the combinations sprinkled in.
func (h *harness) pickEvent(rng *rand.Rand, liveNodes, liveGen, maxGen int) chaosEventKind {
	if liveNodes <= 1 {
		return evJoin
	}
	canReshard := liveGen < maxGen
	// Weighted menu. Numbers are relative weights.
	type wk struct {
		kind chaosEventKind
		w    int
	}
	menu := []wk{
		{evKill, 30},
		{evJoin, 20},
		{evLeave, 15},
	}
	if canReshard {
		menu = append(menu,
			wk{evReshard, 15},
			wk{evReshardWhileDown, 10},
			wk{evLeaveMidReshard, 10},
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
	// Reshard over the survivors while the victim is down.
	committed := false
	if err := h.cl.Reshard(); err == nil {
		h.met.evReshard.Add(1)
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
	done := h.cl.ReshardAsync()
	// Give the barrier a moment to engage (freeze), then fire the membership
	// change so it lands mid-barrier.
	time.Sleep(jitterDuration(rng, 30*time.Millisecond))
	victim := h.randomVictim(rng)
	if victim != "" {
		_ = h.cl.RemoveNode(victim) // may race the abort; either order is valid
	}
	err := <-done
	if err == nil {
		h.met.evReshard.Add(1)
		return true
	}
	// Aborted (the expected outcome of a mid-reshard membership change).
	h.met.evReshardAbrt.Add(1)
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
