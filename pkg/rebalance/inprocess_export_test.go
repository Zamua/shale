// In-process migration scaffolding: a MigrateDestination that pulls from a
// MigrateSource directly (no gRPC) and a checksum helper for asserting the
// destination's view matches the source's.
//
// This lives in a _test.go file, in-package, on purpose. Nothing in the
// PRODUCT build uses it: the shipping destination is the cluster layer's
// gRPC-backed one, which dials the peer and consumes the MigrateRange stream.
// These types exist so this package's tests can drive the Coordinator's
// migration path end-to-end with two backends in one process, no sockets. An
// earlier doc comment claimed single-process integration harnesses as a second
// consumer; no such consumer exists, and keeping the code in the product build
// on the strength of that claim shipped dead weight in the library.
//
// It is `package rebalance` (not rebalance_test) so the external
// rebalance_test files keep calling it as rebalance.NewInProcessDestination
// unchanged, while the compiler keeps it out of any non-test build.

package rebalance

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/Zamua/shale/pkg/backend"
)

// -- in-process destination ------------------------------------------

// inProcessDestination is the test destination: it pulls from a
// MigrateSource directly (no gRPC), validates the checksum, and
// applies each KV to a backend.Backend. Production substitutes the
// cluster layer's gRPC-backed destination, which dials the peer and
// consumes the MigrateRange stream; both satisfy MigrateDestination, so
// the Coordinator does not care which one it has.
//
// The peerLookup callback resolves a Member to a MigrateSource so
// that a multi-node in-process test (two backends, two coordinators)
// can model "destination dials source" without sockets.
type inProcessDestination struct {
	local      backend.Backend
	peerLookup func(peer Member) (MigrateSource, error)
	// applyIfNewer, when set, replaces the raw local.Put as the
	// per-key apply. The cluster layer injects its apply-if-newer
	// (LWW-on-write) applier here so a migrated value at R>1 lands
	// through the same Stamp-compare a live replicated write does:
	// an out-of-order older streamed value loses the compare and is a
	// committed no-op, never a clobber. It returns (wrote, err) so the
	// rollback set records ONLY keys it actually wrote (a no-op apply
	// left a newer concurrent value in place; deleting that key on
	// rollback would clobber the newer write). Nil keeps the raw-Put
	// path for R<=1 / non-envelope callers + the existing tests.
	applyIfNewer func(key, value []byte) (wrote bool, err error)
}

// NewInProcessDestination is the in-package MigrateDestination used
// by tests + by single-process integration harnesses. peerLookup
// resolves "this Member -> a MigrateSource I can pull from."
func NewInProcessDestination(local backend.Backend, peerLookup func(Member) (MigrateSource, error)) MigrateDestination {
	return &inProcessDestination{local: local, peerLookup: peerLookup}
}

// NewInProcessDestinationLWW builds an in-process destination whose
// per-key apply routes through applyIfNewer (apply-if-newer / LWW)
// instead of a raw Put. Used where the migrated values are LWW
// envelopes (R>1) so the in-process path matches the gRPC
// clusterDestination's "never a clobber" apply.
func NewInProcessDestinationLWW(local backend.Backend, peerLookup func(Member) (MigrateSource, error), applyIfNewer func(key, value []byte) (bool, error)) MigrateDestination {
	return &inProcessDestination{local: local, peerLookup: peerLookup, applyIfNewer: applyIfNewer}
}

// applyKV lands one migrated pair, routing through applyIfNewer when
// set (returns whether the key was actually written so the caller can
// keep its rollback set honest).
func (d *inProcessDestination) applyKV(key, value []byte) (wrote bool, err error) {
	if d.applyIfNewer != nil {
		return d.applyIfNewer(key, value)
	}
	if err := d.local.Put(key, value); err != nil {
		return false, err
	}
	return true, nil
}

func (d *inProcessDestination) FetchRange(ctx context.Context, peer Member, partitionIDs []uint64, gen uint64) (int, error) {
	if d.peerLookup == nil {
		return 0, errors.New("rebalance: in-process destination has no peer lookup")
	}
	src, err := d.peerLookup(peer)
	if err != nil {
		return 0, fmt.Errorf("rebalance: resolve peer %s: %w", peer.ID, err)
	}
	kvCh, errCh := src.OpenRange(partitionIDs, gen)

	hasher := crc32.NewIEEE()
	applied := make([][]byte, 0)
	rollback := func() {
		for _, k := range applied {
			_ = d.local.Delete(k)
		}
	}

	count := 0
	for {
		select {
		case <-ctx.Done():
			rollback()
			return 0, ctx.Err()

		case kv, ok := <-kvCh:
			if !ok {
				// Source closed the data channel; wait for the
				// terminal error/nil signal.
				kvCh = nil
				continue
			}
			wrote, err := d.applyKV(kv.Key, kv.Value)
			if err != nil {
				rollback()
				return 0, fmt.Errorf("rebalance: apply key: %w", err)
			}
			if wrote {
				applied = append(applied, append([]byte(nil), kv.Key...))
			}
			hasher.Write(kv.Key)
			hasher.Write(kv.Value)
			count++

		case srcErr, ok := <-errCh:
			if !ok {
				// Both channels closed; treat as clean termination.
				return count, nil
			}
			if srcErr != nil {
				rollback()
				return 0, fmt.Errorf("rebalance: source: %w", srcErr)
			}
			// Clean termination signal; drain any remaining KVs if
			// they were buffered before close, then return.
			if kvCh != nil {
				for kv := range kvCh {
					wrote, err := d.applyKV(kv.Key, kv.Value)
					if err != nil {
						rollback()
						return 0, fmt.Errorf("rebalance: apply key: %w", err)
					}
					if wrote {
						applied = append(applied, append([]byte(nil), kv.Key...))
					}
					hasher.Write(kv.Key)
					hasher.Write(kv.Value)
					count++
				}
			}
			return count, nil
		}
	}
}

// SourceChecksum computes the canonical migration checksum for the
// KVs that belong to a given partition set on a Backend. Exposed so
// tests can sanity-check that the destination's view matches the
// source's view independent of the streaming machinery.
//
// Determinism note: keys are sorted ascending before hashing, which
// matches the order the Backend's ScanPrefix returns them. So the
// source's incremental hash (which hashes in scan order) and this
// function's hash (which sorts) agree as long as the Backend's scan
// is ascending. The memory + pebble backends both satisfy that.
func SourceChecksum(be backend.Backend, partitionIDs []uint64, partitionFn PartitionFn) ([]byte, int, error) {
	it, err := be.ScanPrefix(nil)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = it.Close() }()

	wanted := make(map[uint64]struct{}, len(partitionIDs))
	for _, pid := range partitionIDs {
		wanted[pid] = struct{}{}
	}

	type kv struct{ k, v []byte }
	var pairs []kv
	for {
		k, v, err := it.Next()
		if err != nil {
			return nil, 0, err
		}
		if k == nil {
			break
		}
		if _, ok := wanted[partitionFn(k)]; !ok {
			continue
		}
		pairs = append(pairs, kv{
			k: append([]byte(nil), k...),
			v: append([]byte(nil), v...),
		})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return string(pairs[i].k) < string(pairs[j].k)
	})

	h := crc32.NewIEEE()
	for _, p := range pairs {
		h.Write(p.k)
		h.Write(p.v)
	}
	sum := h.Sum(nil)
	return sum, len(pairs), nil
}
