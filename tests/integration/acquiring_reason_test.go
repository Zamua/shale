package integration

// The PATH-INDEPENDENCE gate for cluster.ErrAcquiring.
//
// shale is embedded in-process: routing and peer forwarding happen INSIDE the
// cluster, below the consumer's seam, so an acquiring-window refusal raised by
// a locally-mounted position and one forwarded from a peer return through the
// SAME call site. The contract a consumer codes against is therefore that ONE
// match works for both:
//
//	errors.Is(err, cluster.ErrAcquiring)
//
// These tests pin exactly that, and pin the distinction it exists to draw: a
// genuinely-down peer is ALSO codes.Unavailable and must NOT match, because a
// consumer that retried a real outage as if it were a handoff blip would spin
// through its whole budget on a dead node.

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/cluster"
	"github.com/Zamua/shale/pkg/ring"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// acquiringReasonPair brings up a settled 2-node shared-backing cluster and
// returns (owner, other, key, unit) where key's unit is owned by owner on the
// converged ring, so an op issued on other is REALLY forwarded to owner over
// gRPC while the same op on owner stays in-process.
func acquiringReasonPair(t *testing.T, unitCount int) (owner, other *sharedNode, key string, u storageunit.UnitID) {
	t.Helper()

	backing := sharedfactory.NewBacking()
	n1 := startSharedNode(t, "ar1", "", unitCount, backing)
	n2 := startSharedNode(t, "ar2", n1.BindAddr, unitCount, backing)

	clusters := []*cluster.Cluster{n1.Cluster, n2.Cluster}
	if err := waitForMembersAll(clusters, 2, 15*time.Second); err != nil {
		t.Fatalf("ring convergence: %v", err)
	}
	// Let the post-join reconcile settle so the mount map matches the ring;
	// clearing a mount before the handoff lands would race the reconcile
	// putting it straight back.
	if !waitUntil(10*time.Second, func() bool {
		return len(n2.Handle.OpenUnits()) > 0
	}) {
		t.Fatalf("no unit ever handed off to ar2: n2 open=%v", n2.Handle.OpenUnits())
	}

	byID := map[string]*sharedNode{n1.ID: n1, n2.ID: n2}
	uc := storageunit.MustUnitCount(unitCount)

	// Find a key both nodes agree is owned by the SAME node, so the forwarding
	// direction is unambiguous.
	for i := range 256 {
		k := fmt.Sprintf("ar-key-%03d", i)
		o1 := unitOwnerOnRing(n1.Cluster, k, unitCount)
		o2 := unitOwnerOnRing(n2.Cluster, k, unitCount)
		if o1 == "" || o1 != o2 {
			continue
		}
		own, ok := byID[o1]
		if !ok {
			continue
		}
		oth := n1
		if own == n1 {
			oth = n2
		}
		// The owner must actually hold the unit's mount, else the window we are
		// about to construct is not the one under test.
		unit := storageunit.UnitForShardKey(ring.ShardKey([]byte(k)), uc)
		if err := own.Cluster.Put([]byte(k), []byte("seed")); err != nil {
			continue
		}
		return own, oth, k, unit
	}
	t.Fatal("no key with a stable single owner found across 256 candidates")
	return nil, nil, "", 0
}

// TestAcquiringReason_BothPathsMatchErrAcquiring is the deliverable: the SAME
// errors.Is match identifies the acquiring window whether the refusing position
// is mounted on the node the call entered (in-process, where the private
// sentinel already survived) or on a PEER the op was forwarded to (over gRPC,
// where the identity used to dissolve into a bare codes.Unavailable).
//
// The window is constructed deterministically with TestingClearMount, which
// leaves the ring OWNER of the unit owner-but-unmounted - exactly the state a
// real lease handoff passes through - rather than by hand-building an error.
// It is re-cleared before each leg so a reconcile re-mounting the unit between
// legs cannot make the second leg assert against a served read.
func TestAcquiringReason_BothPathsMatchErrAcquiring(t *testing.T) {
	const unitCount = 8
	owner, other, key, u := acquiringReasonPair(t, unitCount)

	t.Run("forwarded", func(t *testing.T) {
		// REAL forwarded op: `other` is not the owner, so its routed Get goes
		// out over gRPC to `owner`, which is mid-acquire.
		owner.Cluster.TestingClearMount(u)

		_, err := other.Cluster.Get([]byte(key))
		if err == nil {
			t.Fatal("forwarded Get into the acquiring window: got a served read, want a refusal")
		}
		if !errors.Is(err, cluster.ErrAcquiring) {
			t.Fatalf("forwarded acquiring refusal does not match cluster.ErrAcquiring: %v (%T)", err, err)
		}
		// The client-facing code must be unchanged: the existing retry and
		// forwarding shapes key off Unavailable.
		if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
			t.Fatalf("forwarded acquiring refusal: code = %v, want Unavailable. err=%v", st.Code(), err)
		}
	})

	t.Run("local", func(t *testing.T) {
		// Same refusal, raised on the node the call entered.
		owner.Cluster.TestingClearMount(u)

		_, err := owner.Cluster.Get([]byte(key))
		if err == nil {
			t.Fatal("local Get into the acquiring window: got a served read, want a refusal")
		}
		if !errors.Is(err, cluster.ErrAcquiring) {
			t.Fatalf("local acquiring refusal does not match cluster.ErrAcquiring: %v (%T)", err, err)
		}
		if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
			t.Fatalf("local acquiring refusal: code = %v, want Unavailable. err=%v", st.Code(), err)
		}
	})
}

// TestAcquiringReason_PeerDownDoesNotMatchErrAcquiring is the negative half,
// and the reason the whole mechanism exists. A genuinely-down peer also comes
// back codes.Unavailable, so a consumer keying off the CODE (or off the message
// text) would retry a real outage as though it were a bounded handoff blip.
// Only the reason distinguishes them, so a peer-down error must NOT match.
//
// The peer's gRPC server is stopped while its MEMBERSHIP is left intact, so the
// ring still routes the key to it and the forwarded op fails at the dial - a
// real transport failure, not a synthesized one.
func TestAcquiringReason_PeerDownDoesNotMatchErrAcquiring(t *testing.T) {
	const unitCount = 8
	owner, other, key, _ := acquiringReasonPair(t, unitCount)

	// Kill ONLY the owner's gRPC listener. Memberlist keeps running, so the
	// ring is unchanged and `other` still forwards the key to it.
	owner.stop()
	owner.stop = nil

	var err error
	// The forwarded read fails as soon as the dial is refused; poll briefly in
	// case a warm cached connection has to notice the listener is gone.
	ok := waitUntil(5*time.Second, func() bool {
		_, err = other.Cluster.Get([]byte(key))
		return err != nil
	})
	if !ok {
		t.Fatalf("forwarded Get to a downed peer kept succeeding; err=%v", err)
	}
	if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
		t.Fatalf("peer-down forwarded Get: code = %v, want Unavailable (the code acquiring shares). err=%v", st.Code(), err)
	}
	if errors.Is(err, cluster.ErrAcquiring) {
		t.Fatalf("a genuine peer-down error matched cluster.ErrAcquiring; the acquiring/outage distinction is broken: %v", err)
	}
}
