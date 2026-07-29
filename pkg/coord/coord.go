// Package coord declares shale's COORDINATION PORT: the one narrow question
// the storage layer asks about cluster topology, and the one narrow way it
// publishes its own transitional stance.
//
// The question is always "which nodes SHOULD hold this storage unit", and the
// answer is ADVISORY. A Coordinator never grants a lease, never fences a
// writer, and never gates a write: shale's safety comes from the durable
// store's own fencing (a higher-epoch open displaces a lower one), so a
// Coordinator that is stale, split-brained or plain wrong costs AVAILABILITY,
// never CORRECTNESS. That is what makes the port swappable: a gossip +
// consistent-hash coordinator and a lease/CAS coordinator differ in how fast
// and how consistently they converge, not in what a wrong answer can destroy.
//
// The port is deliberately free of the mechanism that answers it. There is no
// Add, no Remove, no ring, no member table mutation here: membership discovery
// and placement are IMPLEMENTATION, owned entirely by an adapter (today
// pkg/coord/gossip: SWIM gossip + a bounded-load consistent hash ring). The
// cluster layer never tells a Coordinator who the members are; it only asks.
//
// The types are pure: this package imports pkg/storageunit (shale's storage
// domain) and nothing else of shale.
package coord

import (
	"errors"

	"github.com/Zamua/shale/pkg/storageunit"
)

// Node is one addressable participant in the cluster.
//
// ID is the stable, cluster-unique identity placement hashes on; it survives a
// restart on a new port, which is exactly why placement must not hash Addr.
// Addr is the gRPC "host:port" the cluster layer dials to reach the node; an
// empty Addr means "known but not addressable".
type Node struct {
	ID   storageunit.NodeID
	Addr string
}

// Role is a node's TRANSITIONAL stance in the overlap handoff protocol, as a
// bit set. It is a shale-protocol concept, not a gossip concept: any
// coordinator has to be able to express "I am warming into an assignment I do
// not serve yet" and "I am yielding an assignment I still serve", because the
// current/pending routing split is defined in terms of them.
//
// The zero Role is STEADY STATE: neither warming nor yielding.
type Role uint8

const (
	// RoleJoining marks a node that OWNS positions it has not mounted yet. It
	// is excluded from the CURRENT replica set (subject to the quorum floor)
	// so the still-mounted displaced owner stays the stable holder while the
	// newcomer warms.
	RoleJoining Role = 1 << iota
	// RoleDraining marks a node that is YIELDING ownership but is STILL
	// SERVING. It stays in the CURRENT set and is excluded from PENDING, so
	// its successors can warm underneath it before it departs.
	RoleDraining
)

// Has reports whether every bit in want is set in r. Has(0) is true.
func (r Role) Has(want Role) bool { return r&want == want }

// Member is a Node plus the coordination facts the protocol needs about it.
type Member struct {
	Node
	// Roles is the member's complete transitional stance (see Role).
	Roles Role
	// DeclaredUnitCount is the member's STANDING declared unit count - the
	// count its operator configured for it, not the count it currently runs.
	// 0 means UNKNOWN (a member that does not declare one). The declarative
	// reshard gate treats an unknown count as a VETO, never as a vote.
	DeclaredUnitCount uint32
}

// Joining reports whether the member is warming into an assignment.
func (m Member) Joining() bool { return m.Roles.Has(RoleJoining) }

// Draining reports whether the member is yielding an assignment.
func (m Member) Draining() bool { return m.Roles.Has(RoleDraining) }

// View is an IMMUTABLE snapshot of the coordination state. Callers must treat
// Members as read-only; a Coordinator may hand out the same backing array to
// many readers.
//
// Members is sorted by ID (so two nodes log, diff and compare views the same
// way) and always contains Self once the coordinator has started.
type View struct {
	// Self is this node's own identity, as the coordinator advertises it.
	Self Node
	// Members is every member the coordinator currently knows, sorted by ID.
	Members []Member
	// Epoch is a LOCAL monotone counter bumped on every observed change. It
	// orders one node's own successive views; it is NOT comparable across
	// nodes and carries no consensus meaning.
	Epoch uint64
}

// Empty reports whether the view knows no members at all. An empty view means
// "no coordination information yet", which callers collapse to single-node
// behavior (this node owns everything).
func (v View) Empty() bool { return len(v.Members) == 0 }

// Member returns the member with the given id, if the view holds one.
func (v View) Member(id storageunit.NodeID) (Member, bool) {
	for _, m := range v.Members {
		if m.ID == id {
			return m, true
		}
	}
	return Member{}, false
}

// Placement asks Locate for the POST-TRANSITION placement instead of the
// current one.
//
// Exclude names nodes that are on their way OUT of the placement basis -
// draining leavers, or joiners deliberately held out of the ack-bar set.
// Locate with a non-empty Exclude MUST return the placement that WILL HOLD
// once those nodes are no longer members: the same answer this coordinator
// will give after their departure, computed now. The storage layer routes the
// overlap handoff on that promise - current owners keep serving from the
// placement WITH the leavers while successors warm toward the placement
// WITHOUT them - so the predicted set and the eventual real set must be
// IDENTICAL, or the handoff strands on nodes that will never own the unit.
//
// EXACTNESS IS THE CONTRACT: the answer must be what the coordinator's own
// placement mechanism will produce after the departure, NEVER the current
// placement with the excluded nodes filtered out of the result. The two
// differ whenever the mechanism is not removal-invariant - bounded-load
// consistent hashing is the standing example: the assignment a set of nodes
// computes among themselves differs from the assignment they get as a subset
// of a larger set. A coordinator backed by an explicit assignment table meets
// the same contract differently: its post-departure assignment is coordinated
// state, and this call is a read (or deterministic plan) of it.
//
// Determinism carries over from Locate unchanged: two nodes holding the same
// view and the same Exclude set MUST compute the same placement, in the same
// order.
type Placement struct {
	// Exclude is the set of departing nodes the placement must be computed
	// without. Nil or empty asks for the current placement.
	Exclude map[storageunit.NodeID]struct{}
}

// Params are the facts the STORAGE layer knows and the coordinator must
// advertise on this node's behalf. They are passed once, at Start.
type Params struct {
	// Self is this node's identity and dial address.
	Self Node
	// DeclaredUnitCount is this node's standing declared unit count, gossiped
	// so the cluster can detect agreement on a desired count. 0 = do not
	// advertise (unknown).
	DeclaredUnitCount uint32
	// InitialRoles is the role set this node ASKS to advertise in its VERY
	// FIRST announcement, atomically with its own presence. Seeding RoleJoining
	// here rather than calling SetRole after Start closes a real race: a peer
	// that learns "the newcomer is a member" before "the newcomer is warming"
	// will clean-cut release the position the newcomer displaces instead of
	// holding and draining it.
	//
	// It is a REQUEST, not a command. A coordinator that determines this node
	// is FOUNDING the cluster MUST drop RoleJoining: there is no incumbent to
	// warm against, so advertising it would leave a founder permanently
	// excluding itself from its own CURRENT set. Read the result back through
	// View, not from this field.
	InitialRoles Role
	// SoloStart permits the coordinator to come up WITHOUT reaching any peer.
	// The storage layer sets it when it holds an independent durable
	// form-lock, so a homogeneous deployment whose members all carry the same
	// seed list can have its first member up. Without it a coordinator that
	// cannot reach its peers MUST fail Start rather than silently fork.
	SoloStart bool
}

// Bootstrap says how this node entered the cluster, as the coordinator
// determined it at Start. The storage layer needs the distinction because a
// node that FOUNDED a cluster defines its starting generation, while a node
// that JOINED one must learn that generation from an incumbent before it dares
// serve a single read.
type Bootstrap uint8

const (
	// BootstrapFounded means no incumbent was reachable and this node formed
	// the cluster.
	BootstrapFounded Bootstrap = iota + 1
	// BootstrapJoined means an existing cluster was reachable and this node
	// joined it.
	BootstrapJoined
)

// ErrGenerationUnsupported is returned by ProposeGeneration from a Coordinator
// that does not participate in generation agreement. See Coordinator.
var ErrGenerationUnsupported = errors.New("coord: this coordinator does not agree storage generations")

// Coordinator answers "which nodes SHOULD hold this storage unit". Every
// method except Start and Close must be safe to call concurrently.
type Coordinator interface {
	// Start begins participation, publishes p, and reports how this node
	// entered the cluster. It is called exactly once, by the storage layer,
	// before any other method. A Coordinator that cannot establish itself
	// (cannot bind, cannot reach a required peer) MUST return an error rather
	// than come up with a partial view.
	//
	// The returned Bootstrap is the coordinator's own determination and is the
	// ONLY authority on it: "did anyone else already exist" is a question about
	// how the cluster was discovered, which is precisely what a coordination
	// mechanism knows and the storage layer does not.
	Start(p Params) (Bootstrap, error)

	// View returns the current snapshot. MUST NOT block and MUST NOT do I/O.
	View() View

	// Populated reports whether the view currently contains at least one
	// member. It exists because the storage layer asks this question ON EVERY
	// OPERATION (it is the replicated-vs-single-owner routing predicate), so
	// it MUST be answerable without constructing a View: an adapter that
	// implements it as View().Empty() has not implemented it. MUST NOT block
	// and MUST NOT do I/O. Equivalent to !View().Empty() at all times.
	Populated() bool

	// PlacementMembers returns the member set Locate currently computes
	// placement over, sorted by ID. For a single-basis coordinator (an
	// assignment table; the static test coordinator) this IS View's member
	// set. The gossip adapter is dual-basis: its placement ring trails the
	// membership view by an event-loop hop and heals a dropped event only on
	// the reconcile cadence, so this can briefly differ from View - which is
	// precisely why it exists: a guard protecting a decision COMPUTED ON
	// PLACEMENT (the reshard barrier's abort-on-membership-change) must be
	// able to see the placement basis deviate, not only the view. MUST NOT
	// block and MUST NOT do I/O.
	PlacementMembers() []Node

	// TransitionSets returns the IDs of members currently advertising
	// RoleJoining and RoleDraining, as two independent sets. Either or both
	// are nil when no member advertises that role - the common steady state -
	// and callers rely on nil meaning "allocation-free no-op", so an adapter
	// MUST NOT return empty non-nil maps.
	//
	// The storage layer calls this ON EVERY OPERATION to split current from
	// pending ownership, so it MUST cost no more than one membership scan.
	// Role LIVENESS matches View: a role flip MUST be visible here as soon as
	// the coordination mechanism delivers it, NOT merely after the next
	// change hint (hints are lossy; roles are not). MUST NOT block and MUST
	// NOT do I/O. The returned maps are the caller's to keep: an adapter MUST
	// NOT retain or mutate them after returning.
	TransitionSets() (joining, draining map[storageunit.NodeID]struct{})

	// Locate returns up to n nodes for the generation-qualified unit u,
	// PRIMARY FIRST. It returns min(n, |eligible|) nodes and never duplicates
	// one; n <= 0 or an empty eligible set returns nil. MUST NOT block and
	// MUST NOT do I/O.
	//
	// MUST be deterministic: two nodes holding the same View MUST compute the
	// same slice, in the same order. Routing correctness rests on that - a
	// forwarded op that disagrees with its receiver about the primary loops.
	Locate(u storageunit.GenUnit, n int, p Placement) []Node

	// SetRole publishes this node's COMPLETE role set, replacing whatever it
	// advertised before. Declarative and idempotent: passing the roles already
	// advertised is a no-op the adapter must not turn into churn.
	SetRole(r Role) error

	// Changed delivers a coalescing HINT that the view may have changed. It is
	// LOSSY BY CONTRACT: the caller MUST NOT treat one receive as one change,
	// MUST re-read View / Locate to learn what actually moved, and MUST NOT
	// depend on the hint for liveness (a Coordinator is free to coalesce or
	// drop hints, so a caller has to reconcile periodically anyway). The
	// channel is never closed; Close stops delivery.
	Changed() <-chan struct{}

	// Generation returns the storage generation this coordinator believes the
	// cluster has AGREED on, or 0 when it holds no opinion. A Coordinator that
	// does not participate in generation agreement returns 0 always.
	Generation() storageunit.Generation

	// ProposeGeneration asks the coordinator to advance the agreed generation
	// to g. A Coordinator that does not participate in generation agreement
	// returns ErrGenerationUnsupported, and the storage layer keeps driving
	// generations through its own agreement mechanism.
	ProposeGeneration(g storageunit.Generation) error

	// Close stops participation and releases resources. Idempotent. The
	// storage layer owns the coordinator's shutdown even though the operator
	// constructs it.
	Close() error
}

// HardShutdowner is implemented by a Coordinator that can tear its transport
// down WITHOUT the graceful departure announcement, modeling a SIGKILLed
// process. Test seam only; production always goes through Close.
type HardShutdowner interface {
	TestingShutdownNoLeave() error
}

// MemberSetter is implemented by a Coordinator that lets a test replace the
// placement basis directly, to model a node whose view has diverged from its
// peers'. Test seam only.
type MemberSetter interface {
	TestingSetMembers(nodes []Node)
}
