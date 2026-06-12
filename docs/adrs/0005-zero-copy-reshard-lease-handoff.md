# ADR 0005 - Zero-copy resharding via lease-handoff

**Status:** Accepted

## Context

A sharded store has to rebalance as the cluster or the data grows. Local-disk
and in-memory systems MOVE data on rebalance: Cassandra streams SSTables,
Olric copies partitions over the network. That cost is O(data) - slow,
bandwidth-heavy, and the reason rebalancing those systems is a multi-hour
operation.

shale's data lives in object storage (ADR 0001), where every node can already
reach every byte. So a rebalance does not need to move data at all.

## Decision

**Rebalancing and resharding are zero-copy, via lease-handoff.** Ownership of a
storage unit is a LEASE, not a data location. Moving a unit to a new owner is:
close it on the old owner, open it at epoch+1 on the new owner (which fences
the old owner, ADR 0004). The bytes never leave object storage.

Growth is by DOUBLING the unit count, which turns resharding into an online
per-unit BISECT (a cluster-wide FREEZE -> BISECT -> FLIP -> RESUME barrier),
not a global re-partition. Shard count (routing / co-location) is decoupled
from storage-unit count (physical / lease, bounded by per-engine memtable
memory).

## Alternatives considered

- **Copy-on-rebalance** (Olric / Cassandra style). Rejected: O(data) network
  cost, slow, and exactly the pain object storage lets us avoid.
- **Fixed shard count, no resharding.** Rejected: no elasticity; the storage
  tier could not grow with the workload.

## Consequences

- (+) Rebalancing is O(1) in data moved - only an ownership and fence change.
  Resharding a terabyte-scale unit is as cheap as a kilobyte-scale one.
  Validated zero-loss on a real multi-process cluster over MinIO across a
  2->4 reshard with node SIGKILLs.
- (+) Scaling is fast and cheap. Combined with declarative desired-state
  resharding (a separate ADR, decided after this one), scaling the cluster
  becomes "bump the unit-count config and roll the deploy."
- (-) Only DOUBLING is supported today (power-of-two unit counts). Arbitrary N
  and shrink are future work.
- (-) The reshard is a cluster-wide coordinated barrier with a brief freeze
  window during FLIP, so there is short write unavailability. Online /
  zero-freeze reshard is future hardening.
- (-) Zero-copy is fundamentally an object-storage property (ADR 0002): a
  non-shared-storage backend would have to ship the bytes on handoff and lose
  this benefit.
