package decide

// BootState is what a replicated boot's mount pass left behind, plus what the
// coordination view showed of the rest of the cluster at that instant.
type BootState struct {
	// Deferred counts owned positions the mount pass declined to OPEN because
	// a peer's serving marker says a live peer is serving them. Nonzero means
	// this node is WARMING into the ring rather than fully mounted.
	Deferred int
	// Peers counts the OTHER nodes in the coordination view; JoiningPeers, how
	// many of those advertise Joining. Peers > JoiningPeers is the whole
	// evidence for an ESTABLISHED peer.
	Peers        int
	JoiningPeers int
	// SelfJoining is the bit this node ALREADY advertises, taken from the role
	// the coordinator accepted at membership-open - before anything was
	// mounted, so it describes an intent, never an outcome.
	SelfJoining bool
}

// Joining is the bit a boot that ended in this state advertises. It reads only
// the mount pass, never the view, which is what lets the shell PUBLISH the bit
// before it SAMPLES Peers/JoiningPeers: publishing is a coordination round trip
// during which the view can move, and a sample taken after it is never staler.
// BootWarmUp returns exactly this value, so the two orders cannot disagree.
func (s BootState) Joining() bool { return s.Deferred > 0 }

// BootAction is what boot does with that state.
type BootAction struct {
	// Joining is the bit to advertise now the mount pass is DONE. It is not
	// the bit that went in: see BootStaleJoining.
	Joining bool
	// Prompt reports arming the warm-up reconcile immediately (a zero-delay
	// arm) instead of leaving it to the settle debounce.
	Prompt bool
	// Reason names what decided it, so the log line can state WHY.
	Reason BootReason
}

// BootReason names the four boot outcomes.
type BootReason int

const (
	// BootWarm names a boot that deferred no position, so every owned position
	// is mounted.
	BootWarm BootReason = iota
	// BootStaleJoining names the same boot on a node that is advertising
	// Joining. The bit describes an intent formed before the mount pass ran;
	// the outcome contradicts it.
	BootStaleJoining
	// BootEstablishedPeer names a boot that deferred positions with an
	// established peer visible.
	BootEstablishedPeer
	// BootFleetJoining names a boot that deferred positions while every
	// visible peer is itself Joining (or none is visible at all).
	BootFleetJoining
)

func (r BootReason) String() string {
	switch r {
	case BootStaleJoining:
		return "no position was deferred; the Joining bit raised at membership-open is stale and comes down now"
	case BootEstablishedPeer:
		return "an established peer exists, so there is a converged topology to warm into"
	case BootFleetJoining:
		return "no established peer visible: a fleet-wide boot must wait for ring convergence"
	default:
		return "no position was deferred"
	}
}

// BootWarmUp decides, once the mount pass is done, which Joining bit this node
// advertises and whether its deferred positions are warmed NOW or on the
// settle debounce.
//
// THE JOINING BIT IS AN OUTCOME, NOT AN INTENT. It goes up at membership-open,
// before a single position is mounted, because peers must exclude a warming
// node from CURRENT from its first appearance. Once the mount pass has run,
// the boot-defer count says what actually happened, and a node that deferred
// NOTHING is warm by construction - there is nothing left to warm, so nothing
// to re-derive from a later reconcile tick. Leaving the bit up until that tick
// is not merely stale: it is READ BY PEERS as this node not being established,
// which downgrades a peer's own warm-up to the debounce path below and spends a
// settle delay in the write-quorum gap the prompt exists to close. The cost
// lands one node away from the node holding the stale bit, which is why the bit
// is decided HERE, against the outcome that determines it, rather than left to
// a tick that re-derives it from something else.
//
// WHY PROMPT AT ALL (the staggered join): a node joining an established cluster
// knows, at this instant, exactly which owned positions it is missing, and
// until they are warmed the cluster may be unable to assemble a write quorum
// for their units - the displaced peer holds ONE copy and this node's does not
// exist yet. The settle debounce exists to absorb membership CHURN, and a lone
// join into a converged ring is not churn: every debounce tick extends a KNOWN
// quorum gap for no batching benefit, past write-retry budgets sized to ride an
// open rather than an open plus an idle debounce.
//
// WHY ONLY THEN (the mass boot): when every node is booting at once, this
// node's ring is a PARTIAL view and its deferred set was computed against that
// partial view. Acting immediately means acquiring positions it will not own
// once the ring converges, at epochs that fence sibling booting nodes' live
// mounts mid-boot. The debounce is what gives gossip time to converge before
// anyone acts; skipping it in the mass case deterministically lost acked
// writes. An established peer is the discriminator because it is exact rather
// than heuristic: every multi-node replicated node advertises Joining from
// membership-open and clears it only once warmed, so in a mass boot every
// visible peer says Joining, while in a staggered join the established peers
// long ago cleared it. A peer that has not gossiped at all is not visible,
// which also (correctly) reads as not established.
func BootWarmUp(s BootState) BootAction {
	act := BootAction{Joining: s.Joining()}
	switch {
	case s.Deferred == 0 && s.SelfJoining:
		act.Reason = BootStaleJoining
	case s.Deferred == 0:
		act.Reason = BootWarm
	case s.Peers > s.JoiningPeers:
		act.Prompt = true
		act.Reason = BootEstablishedPeer
	default:
		act.Reason = BootFleetJoining
	}
	return act
}
