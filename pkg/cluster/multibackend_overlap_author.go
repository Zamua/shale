// Author-attributed serving markers (v0.11.2): the ROUTED-SUCCESSOR release
// gate for the Phase 2e overlap handoff.
//
// THE DEFECT THIS CLOSES. A serving marker says "a live owner is serving this
// position at epoch E" and, before this change, nothing about WHO. The draining
// owner's release rule was therefore author-anonymous: marker epoch strictly
// above my own open epoch => surrender my copy. That is correct only while every
// node agrees on who the successors are, and during a membership transition they
// do not. Gossip delivers the JOINER's Joining bit and the LEAVER's Draining bit
// independently, so a node can sit in a STALE view where it has seen the former
// and not the latter. In that view its current set excludes the joiner while its
// pending set still includes the leaver, and for a unit whose ENTIRE replica set
// turns over at once (a FULL MOVE) the resulting routed union can contain NONE
// of the true post-transition owners.
//
// Such a node then: (1) computes its held position as current-but-not-pending
// and arms a drain, (2) observes the true successor's anonymous marker, (3)
// releases its last local copy. Its routed union now names only nodes that never
// held the unit. Every leg of a read answers transiently, the all-legs-transient
// retry loop spins to the ReadTimeout, and the client sees a DeadlineExceeded
// Get (or the "unit for key is handing off to this node; retry shortly"
// ScanPrefix). No ACKED WRITE is lost - the ack bar never drops below R (the
// quorum floor in currentReplicasFromReduced), and a write that cannot reach its
// bar returns a retryable error rather than acking - but reads and scans for that
// unit are unavailable until the view converges.
//
// THE RULE. A draining owner releases only when the marker's author is a node it
// ROUTES the position to. It can then never hand its last copy to a successor
// that is invisible to its own readers: either the author is routed (so a read
// through this node reaches it), or the release waits for the view to converge,
// which is exactly when the true successors enter the union.
//
// TWO ESCAPE HATCHES KEEP IT LIVE, both mandatory:
//
//   - LIVENESS BACKSTOP: the hold is BOUNDED (unroutedAuthorHoldBudget). An
//     unbounded hold would be worse than the hole it closes - it could wedge a
//     graceful leave indefinitely. On expiry the node logs loudly (naming the
//     position and the unrouted author) and releases, degrading to exactly the
//     pre-v0.11.2 behavior.
//   - ROLLING-UPGRADE COMPAT: an UNKNOWN author ("" - a legacy marker, or a
//     factory without the AuthoredMarkerFactory capability) falls back to the
//     epoch-only rule immediately. A mixed-version cluster always makes progress.
//
// See docs/SPEC.md "v0.8 Phase 2e" and docs/design/overlap-handoff.md
// "Author-attributed serving markers".

package cluster

import (
	"time"

	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
)

// unroutedAuthorHoldBudget bounds how long a draining owner refuses to release
// because the serving marker's author is outside its routed union. It only has
// to outlive GOSSIP CONVERGENCE (the observed stale-view window was ~6.3s, one
// ReadTimeout longer than the reads it broke); past that the view is not
// converging and holding buys nothing that releasing does not. It is
// deliberately WELL UNDER the default GracefulLeaveDrainTimeout so the backstop
// fires - and the leave completes - before the leave's own budget runs out.
const unroutedAuthorHoldBudget = 15 * time.Second

// unroutedAuthorHold returns the effective hold budget: the constant above,
// CLAMPED to half of any configured GracefulLeaveDrainTimeout.
//
// The clamp is what keeps the gate from ever being the thing that fails a
// leave. An operator may configure a leave budget SHORTER than the constant; a
// fixed 15s hold would then eat the entire budget and turn a leave that would
// have completed into one that times out - trading an availability bug for a
// shutdown bug. Halving guarantees the backstop fires, the position releases,
// and the completion gate still has budget left to observe it. With no leave
// budget configured (the library default, and the whole join-direction
// displaced-owner path) the constant applies unchanged.
func (c *Cluster) unroutedAuthorHold() time.Duration {
	if lt := c.cfg.GracefulLeaveDrainTimeout; lt > 0 && lt/2 < unroutedAuthorHoldBudget {
		return lt / 2
	}
	return unroutedAuthorHoldBudget
}

// authoredMarkerFactory returns the factory's OPTIONAL author-attribution
// capability, or ok == false when this backend predates it (in which case every
// gate below degrades to the epoch-only rule).
func (c *Cluster) authoredMarkerFactory() (storageunit.AuthoredMarkerFactory, bool) {
	if c.replicaFactory == nil {
		return nil, false
	}
	af, ok := c.replicaFactory.(storageunit.AuthoredMarkerFactory)
	return af, ok
}

// writeServingMarkerAuthored writes ru's serving marker at epoch, ATTRIBUTED to
// this node when the factory supports attribution and author-less when it does
// not. It is the single write path for the mount flip: routing every marker
// write through it is what keeps a node from ever writing an unattributed marker
// that a peer would have to fall back on.
func (c *Cluster) writeServingMarkerAuthored(ru storageunit.ReplicaUnit, epoch storageunit.Epoch) error {
	if af, ok := c.authoredMarkerFactory(); ok {
		return af.WriteServingMarkerFrom(ru, epoch, c.cfg.NodeID)
	}
	return c.replicaFactory.WriteServingMarker(ru, epoch)
}

// readServingMarkerAuthored reads ru's serving marker WITH its author. authorID
// is "" (UNKNOWN) both for a factory without the capability and for a marker
// that carries no attribution; callers must treat the two identically.
func (c *Cluster) readServingMarkerAuthored(ru storageunit.ReplicaUnit) (epoch storageunit.Epoch, authorID string, ok bool, err error) {
	if af, capable := c.authoredMarkerFactory(); capable {
		return af.ReadServingMarkerFrom(ru)
	}
	epoch, ok, err = c.replicaFactory.ReadServingMarker(ru)
	return epoch, "", ok, err
}

// releaseAllowedForAuthor decides whether a position whose EPOCH gate has
// already tripped may actually be released, given WHO wrote the marker. It is
// the routed-successor rule plus its two escape hatches, in one place so
// drainCheck (the displaced/join direction) and allOwnedPositionsHandedOff (the
// graceful-leave direction) can never diverge - the leave gate matters just as
// much, because Close tears the mount down regardless of what drainCheck decided.
//
// It returns true when:
//   - the author is UNKNOWN ("") - legacy marker or a factory without
//     attribution: fall back to the epoch-only rule (rolling-upgrade compat); or
//   - the author is THIS node - holding against yourself is a guaranteed wedge,
//     and a self-authored marker above our own drain epoch means we re-opened
//     the position ourselves; or
//   - the author is in this node's ROUTED union for the position - the normal
//     case, and the whole point: a successor our own readers can reach; or
//   - the hold has exceeded unroutedAuthorHoldBudget - the liveness backstop,
//     which logs loudly and then releases.
//
// It returns false only while actively holding, and arms/keeps the hold clock in
// that case. Any allowed outcome clears the clock, so a position that flaps in
// and out of the hold gets a fresh budget rather than an accumulated one.
func (c *Cluster) releaseAllowedForAuthor(ru storageunit.ReplicaUnit, authorID string) bool {
	if authorID == "" || authorID == c.cfg.NodeID {
		c.unroutedAuthorHoldSince.Delete(ru)
		return true
	}
	if containsMember(c.routedMembersForUnit(ru.Unit), authorID) {
		c.unroutedAuthorHoldSince.Delete(ru)
		return true
	}

	// The author is NOT a node this position's ops reach. Releasing here would
	// destroy the last copy any of our readers can see. Hold - but only for the
	// bounded budget.
	budget := c.unroutedAuthorHold()
	now := time.Now()
	v, loaded := c.unroutedAuthorHoldSince.LoadOrStore(ru, now)
	if !loaded {
		c.logf("shale: holding release of %s: serving marker authored by %q, which is NOT in this node's routed set %v "+
			"(membership view has not converged); will hold up to %s",
			ru, authorID, memberIDs(c.routedMembersForUnit(ru.Unit)), budget)
		return false
	}
	if since, isTime := v.(time.Time); isTime && now.Sub(since) < budget {
		return false
	}

	// LIVENESS BACKSTOP: the view never converged. Releasing restores exactly the
	// pre-v0.11.2 behavior for this position; wedging the drain (and, on the
	// leave path, the node's shutdown) would be strictly worse. Loud on purpose:
	// this line means a membership view stayed stale for longer than gossip
	// convergence should ever take.
	c.logf("shale: WARNING: releasing %s after holding %s for an UNROUTED serving-marker author %q "+
		"(routed set %v never converged to include it); reads for this unit may be briefly unavailable through this node",
		ru, budget, authorID, memberIDs(c.routedMembersForUnit(ru.Unit)))
	c.unroutedAuthorHoldSince.Delete(ru)
	return true
}

// logZeroAnswerSweep emits ONE line when a union read sweep gathers NOTHING:
// the routed union with each leg's ReplicaUnit and its exact error, the stable
// ack count, and this node's LOCAL mount/phase state for the unit.
//
// It exists because the two failure shapes that produce a client-visible read
// error are indistinguishable without it. Shape A is a genuine transient: every
// routed leg really is mid-mount and the next sweep succeeds. Shape B is a
// ROUTING GAP: the union names nodes that never held the unit while the real
// holders sit outside it, so no amount of retrying inside the ReadTimeout can
// help. Both surface as the same aggregate "acquiring" error, so a diagnosis
// from logs alone had to guess between them - and the per-leg detail is exactly
// what settles it (shape B shows legs that are not merely slow but structurally
// unable to answer, with the local mount map empty for the unit).
//
// Silent unless a sweep answers nothing, so it costs nothing in steady state.
func (c *Cluster) logZeroAnswerSweep(routed []routedReplica, stableR int, legErrs []error) {
	if len(routed) == 0 {
		return
	}
	c.mountMu.RLock()
	localMounts := make([]string, 0, 2)
	for ru := range c.mountMap {
		if ru.Unit == routed[0].ru.Unit {
			phase := "Owned"
			if st, inFlight := c.handoffPhase[ru]; inFlight {
				phase = st.Phase.String()
			}
			localMounts = append(localMounts, ru.String()+"="+phase)
		}
	}
	c.mountMu.RUnlock()

	legs := make([]string, 0, len(routed))
	for i, rr := range routed {
		// A nil error on a sweep that gathered nothing means the leg replied but
		// its bytes were unusable (an envelope decode failure), which is a
		// different bug class from a transient and worth naming as such.
		outcome := "replied, but unusable"
		if i < len(legErrs) && legErrs[i] != nil {
			outcome = legErrs[i].Error()
		}
		legs = append(legs, rr.member.ID+"@"+rr.ru.String()+": "+outcome)
	}
	c.logf("shale: union read sweep answered by NO leg for %s (stableR=%d): legs %v; this node's mounts for the unit: %v",
		routed[0].ru.Unit, stableR, legs, localMounts)
}

// memberIDs renders a routed member slice as plain IDs for the hold logs.
func memberIDs(ms []ring.Member) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.ID)
	}
	return out
}
