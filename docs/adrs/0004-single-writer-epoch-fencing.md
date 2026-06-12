# ADR 0004 - Single-writer-per-unit via epoch fencing, not consensus

**Status:** Accepted

## Context

A distributed store has to serialize writes safely so two nodes never both
believe they own the same data and commit conflicting writes. The standard
answer is a consensus protocol (Raft / Paxos) per shard - replicate a log,
elect a leader, route writes through it (TiKV, CockroachDB). Consensus is
correct but operationally heavy and adds latency to every write.

Because shale's data lives in object storage (ADR 0001), it already has access
to a durable conditional-write / fencing primitive that the storage engine
exposes - which is a much cheaper way to get single-writer safety.

## Decision

**shale enforces single-writer-per-unit by EPOCH FENCING, not consensus.** Each
storage unit has at most one live writer at a time. Acquiring a unit means
opening it at an epoch strictly above its durable writer epoch, which FENCES
any prior writer: the prior writer's next write/commit fails. The object store
enforces this - SlateDB's manifest writer-epoch IS the fence. The durable epoch
is read WITHOUT opening (so reading it does not fence anyone); opening bumps it.
There is no Raft, no leader election, and no replicated log on the data path.

## Alternatives considered

- **Raft / Paxos per shard.** Rejected: operational and latency overhead, and
  unnecessary when the object store already provides a durable CAS/fencing
  primitive we can lean on.
- **Multi-writer-per-unit with conflict resolution (last-write-wins).**
  Rejected: it loses linearizability per key and reintroduces the Olric AP
  divergence problem (ADR 0003). We want strong semantics per unit.

## Consequences

- (+) Dramatically simpler than consensus: no log replication, no elections.
  Correctness is a single fencing-token check the object store enforces.
- (+) Strong, linearizable semantics per unit (exactly one writer).
- (+) It makes the lease-handoff (ADR 0005) safe and trivial: a handoff is just
  "open at epoch+1 on the new owner", which atomically fences the old owner.
  Proven against real slatedb + MinIO: a stale writer is rejected with the real
  binding's fenced error after a new owner re-acquires.
- (-) One writer per unit, so write throughput PER UNIT is single-node-bounded.
  Horizontal write scale therefore comes from having MANY units (sharding), not
  from multiple writers per unit.
- (-) The fencing primitive is object-storage-conferred (SlateDB's manifest).
  A non-object-storage backend would have to supply its own cross-node fencing
  (see ADR 0002).
