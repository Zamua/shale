// Homogeneous (runtime try-join-else-form) bootstrap.
//
// The __cluster/init marker in the shared ConditionalStore is the linchpin. It
// does double duty:
//
//   - FORM-LOCK. A fresh cluster's nodes all PutIfAbsent it; exactly one wins
//     and becomes the founder, so concurrent first-boot can never split-brain
//     into multiple gen-0 rings. Gossip alone is NOT a sufficient tiebreak (two
//     nodes that have not yet gossip-connected both believe they are alone); the
//     CAS is the authoritative single-winner.
//   - DURABLE {gen, count}. Its value carries the cluster's current generation
//     and unit count. Generation is otherwise in-memory only (a joiner learns it
//     via the GenState RPC from a live seed), so a FULL-cluster restart - no live
//     peer to ask - would reset everyone to gen 0 while the data sits at gen N.
//     Reading the marker on boot lets a restart RESUME the live generation.
//
// This replaces the config-driven "no seeds => founder, seeds => learn-from-RPC"
// split: every node runs the SAME path (identical config, one StatefulSet), and
// the marker's single PutIfAbsent winner is the founder. A restart reads the
// marker and rejoins at the durable generation instead of re-forming gen 0
// (the seed-restart bug). Active only when a ConditionalStore is configured;
// the legacy seed-RPC path is the fallback for callers without one.

package cluster

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Zamua/shale/pkg/storageunit"
)

const clusterInitMarkerKey = "__cluster/init"

// clusterInitRecord is the marker's JSON value: the cluster's durable
// {generation, unit-count}.
type clusterInitRecord struct {
	Gen   uint64 `json:"gen"`
	Count uint32 `json:"count"`
}

func encodeClusterInit(r clusterInitRecord) ([]byte, error) {
	return json.Marshal(r)
}

func decodeClusterInit(data []byte) (clusterInitRecord, error) {
	var r clusterInitRecord
	if err := json.Unmarshal(data, &r); err != nil {
		return clusterInitRecord{}, err
	}
	if r.Count == 0 {
		return clusterInitRecord{}, fmt.Errorf("unit count is zero")
	}
	return r, nil
}

// bootstrapViaMarker performs the runtime form-vs-join decision against the
// __cluster/init marker. Present => an existing cluster: adopt its durable
// {gen, count}, this node JOINS. Absent => contend to FOUND a fresh cluster at
// gen 0 via PutIfAbsent; exactly one node wins, the rest loop and read the
// winner's record. Returns the durable {gen, count} and whether THIS node
// founded.
func (c *Cluster) bootstrapViaMarker() (gen storageunit.Generation, count storageunit.UnitCount, founded bool, err error) {
	cs := c.cfg.ConditionalStore
	for {
		data, _, getErr := cs.Get(clusterInitMarkerKey)
		if getErr == nil {
			rec, decErr := decodeClusterInit(data)
			if decErr != nil {
				return 0, storageunit.UnitCount{}, false, fmt.Errorf("cluster: corrupt %s marker: %w", clusterInitMarkerKey, decErr)
			}
			cnt, cErr := storageunit.NewUnitCount(int(rec.Count))
			if cErr != nil {
				return 0, storageunit.UnitCount{}, false, fmt.Errorf("cluster: bad unit-count in %s: %w", clusterInitMarkerKey, cErr)
			}
			return storageunit.Generation(rec.Gen), cnt, false, nil
		}
		if !errors.Is(getErr, storageunit.ErrCondNotFound) {
			return 0, storageunit.UnitCount{}, false, fmt.Errorf("cluster: read %s: %w", clusterInitMarkerKey, getErr)
		}
		// Absent: contend to found a fresh cluster at gen 0.
		payload, encErr := encodeClusterInit(clusterInitRecord{Gen: 0, Count: c.unitCount.N()})
		if encErr != nil {
			return 0, storageunit.UnitCount{}, false, encErr
		}
		_, putErr := cs.PutIfAbsent(clusterInitMarkerKey, payload)
		if putErr == nil {
			return 0, c.unitCount, true, nil // founded
		}
		if errors.Is(putErr, storageunit.ErrPrecondition) {
			continue // someone founded between our Get and Put: loop, read theirs
		}
		return 0, storageunit.UnitCount{}, false, fmt.Errorf("cluster: found %s: %w", clusterInitMarkerKey, putErr)
	}
}

// NOTE (follow-up, #459): advancing the marker's durable {gen, count} on a
// reshard - so a full-cluster restart of a RESHARDED homogeneous cluster
// resumes the new generation rather than the founding gen 0 - is a separate,
// contained change (a CompareAndSet from the reshard commit path). It is not
// exercised yet (prod is gen 0, never resharded; the existing reshard tests use
// the legacy seed-RPC path), so it is deliberately deferred until after the
// bootstrap lands + validates. A homogeneous cluster MUST NOT reshard until it
// is wired.
