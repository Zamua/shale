package cluster

import (
	"bytes"

	"github.com/Zamua/shale/pkg/backend"
	"github.com/Zamua/shale/pkg/storageunit"
)

// ScanPrefixAllUnits visits every key with the given prefix across EVERY unit
// mounted on this node, decoding each replicated envelope and skipping
// tombstones.
//
// ScanPrefix cannot answer this question and is not meant to: it routes on the
// prefix, so a prefix that names no shard token - "pastes/" rather than
// "pastes/<slug>" - reaches exactly one unit and returns that unit's share as
// though it were the whole answer. A caller enumerating a keyspace therefore
// gets a SILENT SUBSET, which is the failure this exists to remove.
//
// OFFLINE USE ONLY. It touches every mounted unit, so its cost grows with the
// whole dataset rather than with one shard, and it is complete only for the
// units THIS node holds - a migration or audit runs it on a node that has
// mounted all of them, having confirmed Ready(1.0) first.
//
// Values are handed to fn already decoded, so callers do not repeat the
// envelope handling that the raw ScanPrefix documentation warns about.
func (c *Cluster) ScanPrefixAllUnits(prefix []byte, fn func(key, value []byte) error) error {
	if c.notReady() {
		return backend.ErrClosed
	}
	if !c.multi {
		it, err := c.ScanPrefix(prefix)
		if err != nil {
			return err
		}
		return drainDecoded(it, fn)
	}
	seen := make(map[storageunit.GenUnit]struct{})
	for _, ru := range c.mounts.mountedList() {
		if _, ok := seen[ru.Unit]; ok {
			continue // replica positions of one unit hold the same keys
		}
		seen[ru.Unit] = struct{}{}
		b, ok := c.mounts.anyForUnit(ru.Unit)
		if !ok {
			continue
		}
		it, err := b.ScanPrefix(prefix)
		if err != nil {
			return err
		}
		if err := drainDecoded(it, fn); err != nil {
			return err
		}
	}
	return nil
}

// drainDecoded yields each live payload, dropping tombstones. A replicated
// value is an envelope; an unreplicated one is the payload itself, and Decode
// distinguishes them.
func drainDecoded(it backend.Iterator, fn func(key, value []byte) error) error {
	defer it.Close() //nolint:errcheck
	for {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		if k == nil {
			return nil
		}
		if env, derr := Decode(v); derr == nil {
			v = env.Payload
		}
		if len(v) == 0 {
			continue // delete tombstone
		}
		if err := fn(bytes.Clone(k), bytes.Clone(v)); err != nil {
			return err
		}
	}
}
