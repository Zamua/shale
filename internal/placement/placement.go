// Package placement holds the placement-encoding helpers SHARED by every
// coordinator adapter under pkg/coord.
//
// The ring-input encoding must be identical on every node AND under every
// adapter: two nodes disagreeing about it would route the same unit to
// different owners, and a cluster migrated from one adapter to another (the
// gossip -> CAS switchover shale ran; docs/design/coordinator-migration.md)
// must keep placing every unit exactly where it already lives. That is why
// the encoding lives in exactly one place, internal to
// pkg/coord's adapters, and each adapter's export forwards here.
package placement

import (
	"encoding/binary"

	"github.com/Zamua/shale/pkg/storageunit"
)

// GenUnitBytes is the STABLE ring-input encoding of a generation-qualified
// unit: 8 big-endian bytes of Generation followed by 4 of UnitID.
//
// The generation is part of the hash input, so a unit's gen-g and gen-(g+1)
// ids hash to different ring positions - which is what lets a doubling
// reshard land the new generation's units wherever the ring places them. The
// id carries no "{...}" hash tag, so ShardKey passes it through whole.
func GenUnitBytes(gu storageunit.GenUnit) []byte {
	var b [12]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(gu.Gen))
	binary.BigEndian.PutUint32(b[8:12], uint32(gu.ID))
	return b[:]
}
