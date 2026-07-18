// execute.go: per-range migration mechanics.
//
// This file owns the "actually move bytes" part of the v0.3 protocol.
// It is split from state.go so the state machine stays small and so
// the source/destination roles each have one focused entry point.
//
// Two interfaces sit at the seam between this package and the rest
// of shale:
//
//	MigrateSource      implemented locally; reads a partition's KVs
//	                   out of a backend.Backend and exposes them as
//	                   a channel. The gRPC server's MigrateRange
//	                   handler iterates that channel + writes to the
//	                   wire. Tests use the same interface with a
//	                   memory-backed Backend; no gRPC needed.
//
//	MigrateDestination implemented by a small gRPC adapter; dials a
//	                   peer, opens MigrateRange, applies KVs to the
//	                   local Backend, validates the checksum. Tests
//	                   substitute a fake that pulls directly from a
//	                   MigrateSource in-process.
//
// Wire-checksum is CRC32 (IEEE polynomial) over the concatenation of
// key|value bytes in the order they were sent. Source computes it as
// it streams; destination recomputes as it receives; mismatch causes
// the destination to roll back its writes for the range and signal
// failure so the next Evaluate pass retries.

package rebalance

import (
	"context"
	"fmt"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/ring"
)

// KeyValue is the in-package representation of a single migrating
// pair. It mirrors the wire's KeyValue but is kept separate so the
// rebalance package does not need to import the generated proto.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// PartitionFn answers "what partition does this key belong to?" for
// a given ring snapshot. The coordinator wires this to ring.Ring's
// PartitionID method; tests inject a deterministic stub.
type PartitionFn func(key []byte) uint64

// ShardKeyFn extracts the routing shard key from a raw key. It mirrors
// cluster.Config.ShardKeyFn: related keys returning the same shard key
// co-locate on one ring owner. The Coordinator applies it before
// PartitionID so rebalance buckets keys into the same partitions the
// cluster routes them to. Nil means identity (hash the whole key).
type ShardKeyFn func(key []byte) []byte

// MigrateSource is the source-side surface: hand it the partitions
// to migrate + the ring generation, get back a channel of pairs +
// an error channel that fires (at most once, then closes) when the
// scan finishes or fails.
//
// The channel is closed when the scan is done. The errCh receives
// nil on clean completion or an error on failure, then is also
// closed. Callers MUST drain both channels.
type MigrateSource interface {
	OpenRange(partitionIDs []uint64, gen uint64) (<-chan KeyValue, <-chan error)
}

// MigrateDestination is the destination-side surface: dial a peer,
// open the MigrateRange stream, apply each KV to local storage, and
// validate the terminal checksum. Returns the total key count copied
// on success.
type MigrateDestination interface {
	FetchRange(ctx context.Context, peer Member, partitionIDs []uint64, gen uint64) (totalKeys int, err error)
}

// -- localSource -----------------------------------------------------

// localSource is the default MigrateSource. It iterates the local
// Backend via ScanPrefix(""), filters keys whose partition id is in
// the requested set, and pushes each match onto the channel. This is
// the "fall back to a full scan with a per-key filter" path called
// out in docs/SPEC.md "Wire": correct for every Backend, efficient
// for memory + slate (which can return only the live working set).
type localSource struct {
	be          backend.Backend
	partitionFn PartitionFn
}

// NewLocalSource builds a MigrateSource that reads from be. The
// partition function should be ring.Ring.PartitionID (or a stable
// stand-in for tests).
func NewLocalSource(be backend.Backend, partitionFn PartitionFn) MigrateSource {
	return &localSource{be: be, partitionFn: partitionFn}
}

func (s *localSource) OpenRange(partitionIDs []uint64, _ uint64) (<-chan KeyValue, <-chan error) {
	out := make(chan KeyValue)
	errCh := make(chan error, 1)

	wanted := make(map[uint64]struct{}, len(partitionIDs))
	for _, pid := range partitionIDs {
		wanted[pid] = struct{}{}
	}

	go func() {
		defer close(out)
		defer close(errCh)

		it, err := s.be.ScanPrefix(nil)
		if err != nil {
			errCh <- fmt.Errorf("rebalance: open scan: %w", err)
			return
		}
		defer func() { _ = it.Close() }()

		for {
			k, v, err := it.Next()
			if err != nil {
				errCh <- fmt.Errorf("rebalance: iterate: %w", err)
				return
			}
			if k == nil {
				errCh <- nil
				return
			}
			if _, ok := wanted[s.partitionFn(k)]; !ok {
				continue
			}
			out <- KeyValue{
				Key:   append([]byte(nil), k...),
				Value: append([]byte(nil), v...),
			}
		}
	}()

	return out, errCh
}

// ringPartitionFn adapts a ring.Ring's PartitionID method to the
// PartitionFn type. Kept here (not in plan.go) because plan.go is
// the pure-domain layer and this adapter only matters when there is
// a real Backend to scan against.
//
// shardKeyFn, when non-nil, is applied to the RAW key first so the
// partition is computed on the same shard key the cluster routes reads
// with (cluster.shardKey -> ring.LocateKey). This MUST match the
// routing extraction: a node owns partition p iff ring.Owner(p) == self,
// and a routed Get for raw key k lands on ring.LocateKey(shardKeyFn(k))
// == ring.Owner(ring.PartitionID(shardKeyFn(k))). If rebalance computed
// the partition on the raw key while routing computed it on the shard
// key, the two disagree for any app with a non-identity ShardKeyFn:
// related keys (one logical subject spanning many raw keys) scatter
// across partitions, the reconcile/handoff scan buckets them by the
// wrong partition, and a subject's keys strand on the wrong node while
// the ring routes its reads elsewhere. That is the founder-grows
// multi-key loss. A nil shardKeyFn is identity (hash whole key), which
// preserves the no-ShardKeyFn path exactly.
func ringPartitionFn(r *ring.Ring, shardKeyFn ShardKeyFn) PartitionFn {
	if shardKeyFn == nil {
		return func(key []byte) uint64 { return r.PartitionID(key) }
	}
	return func(key []byte) uint64 { return r.PartitionID(shardKeyFn(key)) }
}
