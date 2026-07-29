// Union scan (v0.8 Phase 2e union reads, extended to ScanPrefix). A
// single-shard scan is a READ and follows the pending-ranges union contract
// (docs/SPEC.md "Union scans"): during a membership transition the scan must
// be served by whoever in the routed union physically holds the unit, exactly
// as Get is - NOT by the full-ring primary alone, which a JOIN hands to the
// still-warming newcomer and a LEAVE's ordered removal releases from the
// leaver position-by-position while the leaver is still the ring primary.
//
// Placement semantics are read-one (a scan has always been served by a single
// replica): walk the routed union CURRENT-FIRST, one leg per member, and
// serve the whole scan from the FIRST member that physically has the unit
// mounted. In steady state the first leg is the ring primary, so behavior is
// exactly the pre-transition single-owner scan. A mid-acquire leg (transient
// acquiring recode), an unreachable leg (a just-killed leaver), or a fenced
// leg is SKIPPED and the next union member serves; only when NO leg can serve
// does the scan fail (the first hard error, else the retryable acquiring
// error). Remote legs are POSITION-ADDRESSED (ScanPrefixAtReplica ->
// LocalReplicaScanAt) with no ring-ownership guard, and their first message
// is PRIMED at open so a refusing receiver is classified (and skipped) before
// an iterator is returned.

package cluster

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/Zamua/shale/pkg/backend"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"github.com/Zamua/shale/pkg/storageunit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scanReplicatedUnit serves ScanPrefix at R>1 (multiReplicated) from the
// routed union of the prefix's unit. See the file header for the contract.
// A walk in which every reachable leg was TRANSIENT is re-polled within the
// ReadTimeout budget (docs/SPEC.md "Union scans"), the same all-legs-transient
// retry the union read applies, so the fence-window blip is bounded scan
// latency rather than a client error; hard errors return immediately.
func (c *Cluster) scanReplicatedUnit(prefix []byte) (backend.Iterator, error) {
	return retryReadThroughHandoff(c, func(deadline time.Time) (backend.Iterator, error) {
		return c.scanReplicatedUnitOnce(deadline, prefix)
	})
}

// scanReplicatedUnitOnce is ONE union leg walk (the pre-retry scanReplicatedUnit
// body). It returns the acquiring-tagged error ONLY for the all-legs-transient
// outcome, which is the retry wrapper's re-poll signal.
func (c *Cluster) scanReplicatedUnitOnce(deadline time.Time, prefix []byte) (backend.Iterator, error) {
	routed, _ := c.routedReplicasWithUnit(prefix)
	if len(routed) == 0 {
		return nil, status.Error(codes.Unavailable, "shale: no replicas available for key")
	}

	var firstHardErr error
	var firstUnreachable error
	sawHandoffTransient := false
	seen := make(map[storageunit.NodeID]struct{}, len(routed))
	for _, rr := range routed {
		// One leg per MEMBER: a dual-position holder (an index-shuffling
		// survivor routed at both its old and new slots) is tried once - the
		// receiver-side mounted-position fallback covers whichever position it
		// physically holds.
		if _, dup := seen[rr.member.ID]; dup {
			continue
		}
		seen[rr.member.ID] = struct{}{}

		if string(rr.member.ID) == c.cfg.NodeID {
			b, got, ok := c.localReadBackendForReplicaUnit(rr.ru)
			if !ok {
				sawHandoffTransient = true
				continue // mid-acquire here; another union member serves.
			}
			it, err := b.ScanPrefix(prefix)
			if err != nil {
				// A fenced mount recodes to the transient + evicts (targeting
				// the ru actually resolved); a non-fence error is a hard
				// failure candidate, but keep trying the other union legs.
				if err = c.fenceToTransient(got, b, "ScanPrefix", err); isTransientReadLegErr(err) {
					sawHandoffTransient = true
				} else if firstHardErr == nil {
					firstHardErr = err
				}
				continue
			}
			return it, nil
		}

		it, err := c.openRemoteUnionScanLeg(rr, prefix, deadline)
		if err != nil {
			switch {
			case isTransientReadLegErr(err):
				sawHandoffTransient = true
			case isUnreachableLegErr(err):
				if firstUnreachable == nil {
					firstUnreachable = err
				}
			default:
				if firstHardErr == nil {
					firstHardErr = err
				}
			}
			continue
		}
		return it, nil
	}

	if firstHardErr != nil {
		return nil, firstHardErr
	}
	if sawHandoffTransient {
		// Handoff-class evidence: the retryable acquiring window, the
		// wrapper's full-budget re-poll signal.
		return nil, errUnitAcquiring("ScanPrefix")
	}
	if firstUnreachable != nil {
		// Only dead legs: the capped re-poll signal (the wrapper surfaces the
		// dial error verbatim once the cap is hit).
		return nil, &unreachableOnlyError{inner: firstUnreachable}
	}
	return nil, errUnitAcquiring("ScanPrefix")
}

// openRemoteUnionScanLeg opens the position-addressed scan stream on a remote
// union member and PRIMES its first message, so a receiver that refuses
// (mid-acquire recode) or is unreachable is classified here - and the walk
// moves to the next leg - instead of surfacing as a mid-iteration error after
// an iterator was already handed to the caller.
// The leg's OPEN + PRIME are bounded by the walk's shared deadline (a
// wedged-but-connected peer or a black-holed address can stall the walk only
// to the ReadTimeout budget, never hang it); a CHOSEN leg's stream is then
// DETACHED from that deadline for the drain - the budget bounds leg
// selection, not stream consumption (docs/SPEC.md "Union scans").
func (c *Cluster) openRemoteUnionScanLeg(rr routedReplica, prefix []byte, deadline time.Time) (backend.Iterator, error) {
	cli, err := c.clientFor(rr.member.Addr)
	if err != nil {
		return nil, err
	}
	// A cancelable DETACHED context carries the stream; a one-shot timer
	// cancels it if the open+prime outlives the deadline. On a successful
	// prime the timer is disarmed and the stream lives unbounded.
	ctx, cancel := context.WithCancel(context.Background())
	stopPrime := time.AfterFunc(time.Until(deadline), cancel)
	stream, err := cli.ScanPrefixAtReplica(ctx, rr.ru, prefix)
	if err != nil {
		stopPrime.Stop()
		cancel()
		if time.Until(deadline) <= 0 {
			// The OPEN (connect / stream creation) was cut by the deadline: a
			// wedged transport, not a refusal - the retryable window class.
			return nil, errUnitAcquiring("ScanPrefix")
		}
		return nil, err
	}
	inner := &remoteIterator{stream: stream, cancel: cancel}
	first, err := stream.Recv()
	if err != nil {
		stopPrime.Stop()
		if errors.Is(err, io.EOF) {
			// Legitimately empty scan on a serving member.
			inner.done = true
			return inner, nil
		}
		cancel()
		if time.Until(deadline) <= 0 {
			// The prime was cut by the deadline (a wedged leg, not a refusal):
			// classify as the retryable window; the wrapper's budget check
			// surfaces the bounded error.
			return nil, errUnitAcquiring("ScanPrefix")
		}
		return nil, err
	}
	if !stopPrime.Stop() {
		// The deadline fired between the prime succeeding and the disarm: the
		// stream context is (or is about to be) canceled - do not hand out a
		// dying stream; treat the leg as cut by the deadline.
		cancel()
		return nil, errUnitAcquiring("ScanPrefix")
	}
	return &primedRemoteIterator{inner: inner, first: first, hasFirst: true}, nil
}

// primedRemoteIterator replays the primed first stream message, then
// delegates to the underlying remote iterator.
type primedRemoteIterator struct {
	inner    *remoteIterator
	first    *pb.ScanPrefixResponse
	hasFirst bool
}

func (it *primedRemoteIterator) Next() (key, value []byte, err error) {
	if it.hasFirst {
		it.hasFirst = false
		return it.first.GetKey(), it.first.GetValue(), nil
	}
	return it.inner.Next()
}

func (it *primedRemoteIterator) Close() error { return it.inner.Close() }
