package cluster

// CALL-SITE PINS for fanout's (acks, errs, transient, resultsCh) binding.
//
// fanout returns errs and transient as two SEPARATE []error, and every write
// path immediately forwards both into classifyWriteAttempt(acks, w, errs,
// transient). Because the two are the SAME TYPE, transposing them at a call
// site COMPILES CLEAN. The compiler cannot catch this class of mistake, and
// neither can a unit test of classifyWriteAttempt: the classifier is correct in
// isolation either way, it is the BINDING at the call site that is wrong. So a
// direct test of the classifier passes with the swap in place, which is exactly
// the gap these two tests close.
//
// WHAT THE SWAP WOULD DO. classifyWriteAttempt branches on len(errs)==0. Under
// a mid-acquire shortfall the real errs is EMPTY and transient holds the
// acquiring legs, so the write classifies RETRYABLE and its terminal carries
// ReasonAcquiring (matching the exported ErrAcquiring). Swap the two and the
// classifier reads the acquiring legs as hard failures: the write classifies
// NON-RETRYABLE, the terminal is a plain codes.Unavailable, and the acquiring
// reason is gone. shale's own bounded retry stops firing, and so does any
// downstream consumer gating on errors.Is(err, cluster.ErrAcquiring). Nothing
// looks wrong from outside: the code still compiles, the status code is still
// Unavailable, and the failure is silent.
//
// The two sites pinned here are the ones no other test covered:
//
//	putReplicatedUnitAttempt (multibackend_replicated.go) - the unit-keyed
//	  replicated write path.
//	putReshardDualWrite (multibackend_reshard_route.go)   - the reshard route's
//	  authoritative-leg fan-out.
//
// FIXTURE SHAPE, and why every routed leg is LOCAL. The shortfall has to be
// PURELY transient (acks < W with zero non-transient failures) or the test is
// not exercising the branch the swap corrupts. In this white-box fixture there
// is no peer serving RPC, so any REMOTE leg would fail its dial and land a hard
// codes.Unavailable in errs, which drives the classifier into the hard branch
// with or without the swap and destroys the discrimination. A single-member
// ring keeps the whole routed set local and in-process, where an unmounted
// owner position returns errUnitAcquiring with its sentinel intact - the real
// owner-but-unmounted handoff state, produced by the real dispatch code, not a
// synthetic error handed to the classifier.
//
// Each test opens with a CONTROL: the same write against MOUNTED positions must
// succeed. That is what proves the shortfall below is caused by the acquiring
// window and not by something structural in the fixture (a mis-routed leg, a
// wrong ack bar), which would otherwise let the pin pass for the wrong reason.

import (
	"context"
	"errors"
	"testing"

	"github.com/Zamua/shale/internal/sharedfactory"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// assertAcquiringShortfall is the shared verdict for both pins. It asserts the
// three properties a swapped binding destroys together: the attempt is
// RETRYABLE, its terminal matches the exported ErrAcquiring, and the
// client-facing code is unchanged.
func assertAcquiringShortfall(t *testing.T, site string, got writeAttempt) {
	t.Helper()

	if got.err == nil {
		t.Fatalf("%s: a write whose only routed leg was mid-acquire returned success; "+
			"the fixture did not open an acquiring window", site)
	}
	if !got.retryable {
		t.Fatalf("%s: MID-ACQUIRE SHORTFALL CLASSIFIED AS NON-RETRYABLE. The write missed W with "+
			"ZERO non-transient failures, so classifyWriteAttempt must take its len(errs)==0 branch. "+
			"Getting the hard branch means the classifier saw the acquiring legs in its errs "+
			"parameter: check that this site still passes fanout's THIRD return into errs and its "+
			"FOURTH into transient. err=%v", site, got.err)
	}
	if !errors.Is(got.err, ErrAcquiring) {
		t.Fatalf("%s: TERMINAL LOST cluster.ErrAcquiring. A consumer gating a bounded retry on "+
			"errors.Is(err, cluster.ErrAcquiring) would never fire, and nothing would look wrong "+
			"from outside. This is the observable half of an errs/transient transposition at this "+
			"call site. err=%v (%T)", site, got.err, got.err)
	}
	if code := status.Code(got.err); code != codes.Unavailable {
		t.Fatalf("%s: client-facing code moved: got %v want Unavailable. err=%v", site, code, got.err)
	}
	if r, ok := reasonOf(got.err); !ok || r != ReasonAcquiring {
		t.Fatalf("%s: terminal carries no wire reason detail (reason=%q ok=%v), so the identity dies "+
			"at the node boundary even though the in-process sentinel matched. err=%v",
			site, r, ok, got.err)
	}
}

// SITE: putReplicatedUnitAttempt's fanout -> classifyWriteAttempt binding
// (pkg/cluster/multibackend_replicated.go, the unit-keyed replicated write).
//
// The fixture routes the key to exactly one LOCAL owner position and drops that
// position from the mount map, leaving this node owner-but-unmounted. The
// dispatch then returns errUnitAcquiring in-process, so the fan-out reports
// acks=0 against W=1 with an EMPTY errs and one transient leg: the pure
// mid-acquire shortfall.
func TestFanoutBinding_ReplicatedUnitWriteAcquiringShortfallStaysRetryable(t *testing.T) {
	const (
		unitCount = 8
		self      = "pin1"
	)
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, self, unitCount, 2, backing, self)

	key := []byte("replicated-unit-binding-pin")
	env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1, NodeID: self}, Payload: []byte("v1")})

	// Every routed leg must be local, or a dial failure would land in errs and
	// the shortfall would stop being purely transient (see the file comment).
	routed, stableR := c.routedReplicasWithUnit(key)
	if len(routed) == 0 {
		t.Fatal("the key routed to no replicas; there is no write to classify")
	}
	for _, rr := range routed {
		if rr.member.ID != self {
			t.Fatalf("routed leg on %s is REMOTE; its dial would fail hard and pollute errs, so the "+
				"shortfall would no longer be purely transient. routed=%v", rr.member.ID, routed)
		}
	}
	if w := c.writeAckBar(stableR); w <= 0 {
		t.Fatalf("ack bar W=%d over stableR=%d; the fan-out would not have a bar to miss", w, stableR)
	}

	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mount this node's replica positions: %v", err)
	}

	// CONTROL: with the positions MOUNTED the same write must ack. Without this,
	// a structurally broken fixture (no routed leg, wrong bar) could produce the
	// shortfall below for a reason that has nothing to do with acquiring.
	if att := c.putReplicatedUnitAttempt(context.Background(), key, env); att.err != nil {
		t.Fatalf("control: with every routed position mounted the write must ack, got %v. "+
			"The shortfall asserted below would not be measuring the acquiring window.", att.err)
	}

	// Open the acquiring window: drop EVERY position of the key's unit from the
	// mount map. This is the real handoff state (owner, not yet mounted), not a
	// synthetic error injected at the classifier.
	gu := c.genUnitForKey(key)
	for _, ru := range c.mounts.mountedList() {
		if ru.Unit == gu {
			c.mounts.unmount(ru)
		}
	}

	assertAcquiringShortfall(t, "putReplicatedUnitAttempt", c.putReplicatedUnitAttempt(context.Background(), key, env))
}

// SITE: putReshardDualWrite's fanout -> classifyWriteAttempt binding
// (pkg/cluster/multibackend_reshard_route.go, the reshard route path).
//
// Same shortfall, reached through the v0.9 two-generation dual-write. The legs
// come from the REAL router (routedReplicasForReshard on a cluster with a split
// in flight), so the authoritative set, the slot indices and the ack bar are
// the ones production computes. The SUPPLEMENTARY generation stays mounted
// throughout: it is fired best-effort and never gates, so leaving it healthy
// isolates the shortfall to the authoritative legs this site classifies.
func TestFanoutBinding_ReshardDualWriteAcquiringShortfallStaysRetryable(t *testing.T) {
	const (
		unitCount = 4
		self      = "pin1"
	)
	backing := sharedfactory.NewBacking()
	c := newReplicatedCluster(t, self, unitCount, 2, backing, self)
	enterSplit(t, c) // 4 -> 8, so a key's unit is mid-split and dual-writes

	key := []byte("reshard-route-binding-pin")
	env := Encode(Envelope{Stamp: Stamp{TimestampNanos: 1, NodeID: self}, Payload: []byte("v1")})

	legs, ok := c.routedReplicasForReshard(key)
	if !ok {
		t.Fatal("no reshard in flight; putReshardDualWrite is not the site under test for this key")
	}
	if len(legs.auth) == 0 {
		t.Fatal("the reshard router produced no authoritative legs; there is no ack bar to miss")
	}
	for _, l := range legs.auth {
		if l.member.ID != self {
			t.Fatalf("authoritative leg on %s is REMOTE; its dial would fail hard and pollute errs, "+
				"so the shortfall would no longer be purely transient. auth=%v", l.member.ID, legs.auth)
		}
	}
	if w := c.writeAckBar(legs.stableR); w <= 0 {
		t.Fatalf("ack bar W=%d over stableR=%d; the fan-out would not have a bar to miss", w, legs.stableR)
	}

	// Mount both generations' positions so the CONTROL can ack. mountReplicaUnits
	// covers the split's desired set (parent slots plus the co-located child).
	if err := c.mountReplicaUnits(); err != nil {
		t.Fatalf("mount this node's replica positions: %v", err)
	}
	authMounted := mountedCount(c, legs.auth)
	if authMounted != len(legs.auth) {
		t.Fatalf("only %d of %d authoritative positions mounted; the control below cannot ack",
			authMounted, len(legs.auth))
	}

	// CONTROL: with both generations mounted the dual-write must ack.
	if att := c.putReshardDualWrite(context.Background(), legs, key, env); att.err != nil {
		t.Fatalf("control: with every authoritative position mounted the dual-write must ack, got %v. "+
			"The shortfall asserted below would not be measuring the acquiring window.", att.err)
	}

	// Open the acquiring window on the AUTHORITATIVE generation only, leaving the
	// supplementary legs mounted. The shortfall is therefore attributable to the
	// legs this site's fan-out actually counts.
	authRUs := make(map[storageunit.ReplicaUnit]struct{}, len(legs.auth))
	for _, l := range legs.auth {
		authRUs[l.ru] = struct{}{}
	}
	for ru := range authRUs {
		c.mounts.unmount(ru)
	}

	if suppMounted := mountedCount(c, legs.supp); suppMounted != len(legs.supp) {
		t.Fatalf("clearing the authoritative mounts also unmounted the supplementary generation "+
			"(%d of %d still mounted); the shortfall would not be attributable to the auth legs",
			suppMounted, len(legs.supp))
	}

	assertAcquiringShortfall(t, "putReshardDualWrite", c.putReshardDualWrite(context.Background(), legs, key, env))
}

// mountedCount reports how many of legs' positions currently resolve to a
// mounted backend.
func mountedCount(c *Cluster, legs []routedReplica) int {
	var n int
	for _, l := range legs {
		if _, ok := c.mounts.backendFor(l.ru); ok {
			n++
		}
	}
	return n
}
