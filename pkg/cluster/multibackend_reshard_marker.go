// Durable reshard markers for the decentralized online split (v0.9).
//
// The split's cross-node ordering authority is a small set of CAS-guarded
// objects in the SHARED ConditionalStore (the same If-None-Match / If-Match
// primitive the arbiter epoch uses), NOT node-local state:
//
//   - per-SLOT caught-up marker  __reshard/caughtup/g<gen>/u<K>/r<slot>
//     written (create-only) by the node holding parent slot s of unit K once its
//     local copy of slot s has drained clean (the child slot holds everything
//     the parent slot holds). Proves one replica slot is caught up.
//   - per-UNIT cut-over marker   __reshard/cutover/g<gen>/u<K>
//     written (create-only) once ALL R slot markers are present. This is the
//     single cluster-ordered flip signal every node polls: on observing it a node
//     routes K to its children and moves the ack bar to the child legs. Because it
//     is gated on every slot being caught up, the child set provably holds every
//     write the parent set held when the marker appeared.
//
// All writes are PutIfAbsent (create-only), so they are monotone and idempotent:
// exactly one node wins the create, every other observes it. The marker presence,
// not node-local cutOver, is the authority; cutOver is a per-node cache of "I have
// observed K's durable cut-over marker."

package cluster

import (
	"errors"
	"fmt"

	"github.com/Zamua/shale/pkg/storageunit"
)

const (
	cutoverMarkerPrefix  = "__reshard/cutover/"
	caughtupMarkerPrefix = "__reshard/caughtup/"
)

func cutoverMarkerKey(gen storageunit.Generation, k storageunit.UnitID) string {
	return fmt.Sprintf("%sg%d/u%d", cutoverMarkerPrefix, gen, k)
}

func caughtupMarkerKey(gen storageunit.Generation, k storageunit.UnitID, slot uint8) string {
	return fmt.Sprintf("%sg%d/u%d/r%d", caughtupMarkerPrefix, gen, k, slot)
}

// markerPresent reports whether a marker object exists at key. A nil
// ConditionalStore (no decentralized reshard configured) reports absent.
func (c *Cluster) markerPresent(key string) (bool, error) {
	if c.cfg.ConditionalStore == nil {
		return false, nil
	}
	_, _, err := c.cfg.ConditionalStore.Get(key)
	if errors.Is(err, storageunit.ErrCondNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// publishMarker writes a create-only marker object at key, recording the
// publishing node for debuggability. A marker that already exists
// (ErrPrecondition) is success: the create is idempotent, exactly one writer
// wins and the rest observe it.
func (c *Cluster) publishMarker(key string) error {
	if c.cfg.ConditionalStore == nil {
		return errors.New("cluster: reshard marker requires a ConditionalStore")
	}
	_, err := c.cfg.ConditionalStore.PutIfAbsent(key, []byte(c.cfg.NodeID))
	if errors.Is(err, storageunit.ErrPrecondition) {
		return nil // already published by another node
	}
	return err
}

// publishCaughtupMarker records that this node's parent slot s of unit K has
// caught up (its child copy drained clean).
func (c *Cluster) publishCaughtupMarker(gen storageunit.Generation, k storageunit.UnitID, slot uint8) error {
	return c.publishMarker(caughtupMarkerKey(gen, k, slot))
}

// allSlotsCaughtUp reports whether every one of the r replica slots of unit K has
// published its caught-up marker (the gate for publishing the per-unit cut-over
// marker).
func (c *Cluster) allSlotsCaughtUp(gen storageunit.Generation, k storageunit.UnitID, r int) (bool, error) {
	for slot := 0; slot < r; slot++ {
		present, err := c.markerPresent(caughtupMarkerKey(gen, k, uint8(slot)))
		if err != nil {
			return false, err
		}
		if !present {
			return false, nil
		}
	}
	return true, nil
}

// publishCutoverMarker writes unit K's durable cut-over marker - the cluster-
// observable flip signal. Caller must have confirmed allSlotsCaughtUp first.
func (c *Cluster) publishCutoverMarker(gen storageunit.Generation, k storageunit.UnitID) error {
	return c.publishMarker(cutoverMarkerKey(gen, k))
}

// cutoverMarkerPresent reports whether unit K's durable cut-over marker has been
// published (whether the cluster has agreed K has flipped to its children).
func (c *Cluster) cutoverMarkerPresent(gen storageunit.Generation, k storageunit.UnitID) (bool, error) {
	return c.markerPresent(cutoverMarkerKey(gen, k))
}
