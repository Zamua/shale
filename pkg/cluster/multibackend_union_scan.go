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

	"github.com/Zamua/shale/pkg/backend"
	pb "github.com/Zamua/shale/pkg/rpc/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scanReplicatedUnit serves ScanPrefix at R>1 (multiReplicated) from the
// routed union of the prefix's unit. See the file header for the contract.
func (c *Cluster) scanReplicatedUnit(prefix []byte) (backend.Iterator, error) {
	routed, _ := c.routedReplicasWithUnit(prefix)
	if len(routed) == 0 {
		return nil, status.Error(codes.Unavailable, "shale: no replicas available for key")
	}

	var firstHardErr error
	seen := make(map[string]struct{}, len(routed))
	for _, rr := range routed {
		// One leg per MEMBER: a dual-position holder (an index-shuffling
		// survivor routed at both its old and new slots) is tried once - the
		// receiver-side mounted-position fallback covers whichever position it
		// physically holds.
		if _, dup := seen[rr.member.ID]; dup {
			continue
		}
		seen[rr.member.ID] = struct{}{}

		if rr.member.ID == c.cfg.NodeID {
			b, got, ok := c.localReadBackendForReplicaUnit(rr.ru)
			if !ok {
				continue // mid-acquire here; another union member serves.
			}
			it, err := b.ScanPrefix(prefix)
			if err != nil {
				// A fenced mount recodes to the transient + evicts (targeting
				// the ru actually resolved); a non-fence error is a hard
				// failure candidate, but keep trying the other union legs.
				if err = c.fenceToTransient(got, b, "ScanPrefix", err); !isTransientReplicaErr(err) && firstHardErr == nil {
					firstHardErr = err
				}
				continue
			}
			return it, nil
		}

		it, err := c.openRemoteUnionScanLeg(rr, prefix)
		if err != nil {
			if !isTransientUnionScanLegErr(err) && firstHardErr == nil {
				firstHardErr = err
			}
			continue
		}
		return it, nil
	}

	if firstHardErr != nil {
		return nil, firstHardErr
	}
	// Every leg was transiently unable to serve (mid-acquire / unreachable):
	// the retryable acquiring window, same family as the write path.
	return nil, errUnitAcquiring("ScanPrefix")
}

// openRemoteUnionScanLeg opens the position-addressed scan stream on a remote
// union member and PRIMES its first message, so a receiver that refuses
// (mid-acquire recode) or is unreachable is classified here - and the walk
// moves to the next leg - instead of surfacing as a mid-iteration error after
// an iterator was already handed to the caller.
func (c *Cluster) openRemoteUnionScanLeg(rr routedReplica, prefix []byte) (backend.Iterator, error) {
	cli, err := c.clientFor(rr.member.Addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := cli.ScanPrefixAtReplica(ctx, rr.ru, prefix)
	if err != nil {
		cancel()
		return nil, err
	}
	inner := &remoteIterator{stream: stream, cancel: cancel}
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			// Legitimately empty scan on a serving member.
			inner.done = true
			return inner, nil
		}
		cancel()
		return nil, err
	}
	return &primedRemoteIterator{inner: inner, first: first, hasFirst: true}, nil
}

// isTransientUnionScanLegErr classifies a union scan leg failure as
// SKIP-AND-TRY-THE-NEXT-LEG: the mid-acquire recode (ResourceExhausted, per
// isTransientReplicaErr) and an unreachable member (codes.Unavailable - a
// just-killed leaver's refused dial, or a receiver shutting down). Anything
// else (a real server-side failure) is a hard error candidate the walk
// reports if no other leg serves.
func isTransientUnionScanLegErr(err error) bool {
	if isTransientReplicaErr(err) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.Unavailable
	}
	return false
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
