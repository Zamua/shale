# ADR 0001 - Object storage is the source of truth

**Status:** Accepted

## Context

A distributed key-value store has to decide where the authoritative data
lives. The classic answers are:

- **Local disk on each node** (Cassandra, ScyllaDB, TiKV): each node owns its
  bytes on its own disk.
- **In-memory across the cluster** (Olric, Redis Cluster): the authoritative
  data is RAM on the nodes.

Both make durability and elasticity the cluster's own problem. Local-disk
systems replicate to survive a node loss and STREAM data between nodes on
rebalance. In-memory systems replicate in RAM and lose data on a correlated
failure. In both, the data is co-located with the compute, so scaling means
moving data.

## Decision

**Object storage (S3 / GCS / Azure Blob / MinIO / R2) is the single source of
truth.** Nodes are elastic compute over shared, durable storage; they hold no
authoritative state on local disk. The default `Backend` is SlateDB-on-object-
storage, which writes its LSM (WAL + SSTs) to a bucket.

This is the root bet of the whole system. ADRs 0002-0005 are consequences of
it.

## Alternatives considered

- **Local-disk LSM per node.** Rejected: rebalance copies data node-to-node
  (O(data)), and durability requires node-level replication. The whole point
  of object storage is to delete both problems.
- **In-memory.** Rejected: capacity is bounded by cluster RAM, durability is
  replica-only, and a correlated crash loses un-flushed data. That is a
  distributed cache, not a durable store.

## Consequences

- (+) Durability is free and effectively bottomless - it is the object store's
  job (eleven 9s), not ours.
- (+) Rebalancing can be zero-copy: the bytes never leave object storage, so
  moving a shard is an ownership change, not a data transfer (see ADR 0005).
- (+) Cross-node single-writer fencing comes free from the object store's
  conditional-write / manifest primitive (see ADR 0004).
- (+) Nodes are stateless-ish and elastic; losing a node loses nothing.
- (-) Write latency includes a durable flush to object storage (tens to
  hundreds of ms per commit). The mitigation is relaxed durability (fast-ack)
  at replication factor >= 2, which is future work, not the R=1 baseline.
- (-) Per-request object-storage API cost; mitigated by batching writes
  (memtable -> SST flush) rather than one PUT per key.
- This bet is what makes shale a distinct point in the design space: an
  embeddable, object-storage-native KV. See `docs/SPEC.md` and the landscape
  note in ADR 0003.
