package cluster

// The MIXED-LEG terminal of an R>1 union read.
//
// The R=2 gates in tests/integration drive UNIFORM sweeps: every routed leg
// mid-acquire, or every routed leg down. Real clusters are not uniform. During a
// rolling restart the shape a consumer actually meets is MIXED - one routed
// position is mid-acquire while another replica is genuinely unreachable - and
// the two legs land in DIFFERENT buckets inside a single sweep:
//
//	mid-acquire leg  -> isTransientReadLegErr -> sawHandoffTransient = true
//	dead peer's leg  -> isUnreachableLegErr   -> firstUnreachable    = err
//
// With no leg able to answer, getReplicatedUnitOnce's len(gathered)==0 branch
// chooses the terminal, and it tests sawHandoffTransient BEFORE firstUnreachable.
// That ORDER is the contract: the acquiring reason wins, so the consumer's
// bounded handoff retry fires instead of their outage path.
//
// Nothing pinned the order. Both branches were reachable and both were covered
// in isolation, so swapping them - a plausible refactor ("report the concrete
// dial error, it is more actionable") - left every existing test green while
// turning this read into unreachableOnlyError, i.e. a bare codes.Unavailable
// that does NOT match ErrAcquiring. The consumer's primary retry path silently
// stops firing on the commonest real-world shape.
//
// THE READ PATH AND THE WRITE PATH DECIDE THE MIXED CASE OPPOSITELY, ON PURPOSE.
// Do not "align" them. An R>1 WRITE that misses W with some legs mid-acquire and
// others genuinely down mints a HARD terminal that deliberately does NOT match
// ErrAcquiring: a write's retry has to wait for W to be reachable again, and once
// a real outage is in the mix that wait is bounded by whatever revives the peer,
// not by a mount - so promising a bounded retry there would be a lie. A READ has
// no such requirement. It needs ONE leg to answer, and the mid-acquire leg is the
// one that will come back on its own, so the acquiring reason is the honest and
// actionable one. Same evidence, different obligations, different terminals.
//
// WHY THIS IS A WHITE-BOX TEST rather than another tests/integration gate. The
// ordering governs ONE SWEEP's terminal, and the public Get cannot be held to a
// single sweep. retryReadThroughHandoff re-polls an acquiring sweep for the
// whole ReadTimeout, and its LAST attempt necessarily runs with a spent budget:
// the local leg still answers errUnitAcquiring in-process, but the remote leg's
// RPC sees an already-expired context and returns codes.DeadlineExceeded, which
// classifies as a HARD error and is preferred over BOTH buckets. Measured on a
// real 2-node R=2 cluster with the window held open, Get therefore returns
// DeadlineExceeded after burning the full budget, regardless of the ordering
// under test. That is pre-existing behaviour of the retry wrapper and out of
// scope here; driving the sweep directly is what makes the ordering observable.

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/ring"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deadAddr returns a loopback address with nothing listening on it, by binding
// a port and immediately releasing it. A dial there is REFUSED rather than
// hanging, which is what makes the peer leg a fast, deterministic
// codes.Unavailable (isUnreachableLegErr's exact input) instead of a
// context-deadline artifact.
func deadAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a dead address: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("release the dead address: %v", err)
	}
	return addr
}

// TestAcquiringReason_MixedAcquiringAndUnreachableLegPrefersAcquiring pins the
// terminal-selection ORDER for a sweep whose legs disagree about WHY they could
// not answer.
//
// The fixture makes each leg's bucket unambiguous:
//
//   - The LOCAL leg is mid-acquire. Both replica positions of the key's unit are
//     removed from this node's mount map, the real owner-but-unmounted state, so
//     dispatchReplicaGetUnitAt returns errUnitAcquiring in-process with the
//     sentinel intact.
//   - The PEER leg is genuinely unreachable. Its ring address is a released
//     loopback port, so the dial is refused and the leg returns a bare
//     codes.Unavailable - no shale reason detail attached.
//
// Neither leg can answer, so the sweep must choose, and it must choose ACQUIRING.
//
// The control below proves the two buckets are BOTH populated (not just the
// acquiring one): with the mounts restored and the same dead peer, the sweep is
// SERVED. So the unreachable leg alone never produces a terminal, and every
// terminal this test observes is the mixed one.
func TestAcquiringReason_MixedAcquiringAndUnreachableLegPrefersAcquiring(t *testing.T) {
	const (
		unitCount = 8
		key       = "mixed-leg-key"
	)
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "mx1", unitCount, 2, backing, "mx1", "mx2")
	c.cfg.ReadConsistency = ReadAll
	c.clients = make(map[string]*peerClient)
	t.Cleanup(func() {
		for _, cli := range c.clients {
			_ = cli.Close()
		}
	})

	// Re-point the peer at a refused address (the fixture ring's synthetic
	// "mx2:0" would fail at name resolution, a different and less faithful
	// failure than a dead node). Placement hashes on member ID, so re-adding the
	// same IDs preserves which node holds which replica position.
	peerAddr := deadAddr(t)
	rg := ring.New()
	rg.Add(ring.Member{ID: "mx1", Addr: "127.0.0.1:0"})
	rg.Add(ring.Member{ID: "mx2", Addr: peerAddr})
	c.ring = rg

	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mount this node's replica positions: %v", err)
	}

	// Seed through the mounted local position so a SERVED sweep is possible;
	// without a value the control below could not tell "served" from "empty".
	gu := c.genUnitForKey([]byte(key))
	routed, _ := c.routedReplicasWithUnit([]byte(key))
	if len(routed) != 2 {
		t.Fatalf("routed union = %d legs, want 2 (one local, one dead peer)", len(routed))
	}
	var seeded bool
	for _, rr := range routed {
		if rr.member.ID != c.cfg.NodeID {
			continue
		}
		if b, ok := c.mounts.backendFor(rr.ru); ok {
			if err := b.Put([]byte(key), Encode(Envelope{Payload: []byte("v")})); err != nil {
				t.Fatalf("seed the local replica position: %v", err)
			}
			seeded = true
		}
	}
	if !seeded {
		t.Fatal("this node holds no mounted position for the key's unit; the local leg cannot be the acquiring one")
	}

	// CONTROL: the dead peer alone must NOT produce a terminal. If it did, the
	// assertion below would pass with only ONE bucket populated and would not be
	// testing an ordering at all.
	if _, err := c.getReplicatedUnitOnce(time.Now().Add(3*time.Second), []byte(key)); err != nil {
		t.Fatalf("control: with mounts intact and only the peer dead, the sweep must be SERVED by the "+
			"local position; got %v. The mixed terminal below would not be measuring an ordering.", err)
	}

	// Now open the acquiring window: drop EVERY position of the key's unit from
	// the mount map, leaving this node owner-but-unmounted.
	for _, ru := range c.mounts.mountedList() {
		if ru.Unit == gu {
			c.mounts.unmount(ru)
		}
	}

	_, err := c.getReplicatedUnitOnce(time.Now().Add(3*time.Second), []byte(key))
	if err == nil {
		t.Fatal("a sweep with one mid-acquire leg and one dead peer returned a value; no leg could answer")
	}
	if !errors.Is(err, ErrAcquiring) {
		t.Fatalf("MIXED-LEG SWEEP TERMINAL LOST THE ACQUIRING REASON. One routed leg was mid-acquire and "+
			"the other was a genuinely-unreachable peer; the terminal must prefer the ACQUIRING reason so "+
			"the consumer's bounded handoff retry fires rather than their outage path. Got %v (%T), which "+
			"does not match cluster.ErrAcquiring. Check that getReplicatedUnitOnce's len(gathered)==0 "+
			"branch still tests sawHandoffTransient BEFORE firstUnreachable.", err, err)
	}
	if code := status.Code(err); code != codes.Unavailable {
		t.Fatalf("mixed-leg sweep terminal: code = %v, want Unavailable. err=%v", code, err)
	}

	// The unreachable leg must not have been swallowed either: it is still a
	// routed position, so the sweep genuinely had both kinds of evidence.
	var sawPeer bool
	for _, rr := range routed {
		if rr.member.Addr == peerAddr {
			sawPeer = true
		}
	}
	if !sawPeer {
		t.Fatalf("the dead peer was not in the routed union %v; the sweep never had an unreachable leg", routed)
	}
}

// TestAcquiringReason_UnreachableOnlySweepDoesNotMatchErrAcquiring is the
// negative half, and the reason the preference must be an ORDERING rather than
// a blanket bias toward acquiring. When the ONLY evidence is dead peers, the
// sweep must surface the dial error, so a consumer does not spend a handoff
// retry budget on a real outage.
//
// Same fixture, one difference: this node is not a routed replica at all (the
// ring holds only remote members), so there is no mid-acquire leg to prefer.
func TestAcquiringReason_UnreachableOnlySweepDoesNotMatchErrAcquiring(t *testing.T) {
	const unitCount = 8
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, "mx1", unitCount, 2, backing, "mx2", "mx3")
	c.cfg.ReadConsistency = ReadAll
	c.clients = make(map[string]*peerClient)
	t.Cleanup(func() {
		for _, cli := range c.clients {
			_ = cli.Close()
		}
	})

	rg := ring.New()
	rg.Add(ring.Member{ID: "mx2", Addr: deadAddr(t)})
	rg.Add(ring.Member{ID: "mx3", Addr: deadAddr(t)})
	c.ring = rg

	_, err := c.getReplicatedUnitOnce(time.Now().Add(3*time.Second), []byte("unreachable-only-key"))
	if err == nil {
		t.Fatal("a sweep whose every leg was a refused dial returned a value")
	}
	if errors.Is(err, ErrAcquiring) {
		t.Fatalf("a sweep whose ONLY evidence was unreachable legs matched cluster.ErrAcquiring; the "+
			"acquiring preference has become a blanket bias and a consumer will retry a real outage on "+
			"their handoff budget: %v", err)
	}
	if code := status.Code(err); code != codes.Unavailable {
		t.Fatalf("unreachable-only sweep terminal: code = %v, want Unavailable. err=%v", code, err)
	}
}
