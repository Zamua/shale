# ADR 0002 - Backend-agnostic interfaces; object-storage properties isolated in the slate backend

**Status:** Accepted

## Context

shale bets on object storage (ADR 0001). But coupling the entire codebase to
S3 semantics would make the cluster logic untestable without a bucket and
unportable to anything else. We want the object-storage assumptions confined
to one place.

## Decision

Two clean interfaces keep the cluster layer storage-agnostic:

1. **`Backend`** (`pkg/backend`) is a plain KV: `Put` / `Get` / `Delete` /
   `ScanPrefix` plus a `Transaction`. Nothing object-storage appears in it.
2. **`BackendFactory`** (`pkg/storageunit`) is the v0.8 multi-backend lease
   model, defined in abstract terms: `OpenUnit(GenUnit, Epoch) -> Backend`,
   where opening a unit at an epoch strictly above its current writer epoch
   FENCES the prior writer. Two GenUnits that share a UnitID but differ in
   Generation are independent databases.

The cluster layer (routing, sharding, forwarding, the reshard barrier) depends
ONLY on these two interfaces. Everything object-storage-specific lives entirely
inside the `slate` backend.

The abstraction is proven by more than one implementation of each:

- `Backend` -> `memory`, `pebble` (local disk), `slate` (object storage).
- `BackendFactory` -> an in-memory double (`internal/sharedfactory`, for tests)
  and the slatedb factory (production).

## Alternatives considered

- **Bake object-storage assumptions into the cluster layer.** Rejected: it
  would force a bucket for every test, prevent the in-process chaos harness,
  and lock out other engines.

## Consequences

- (+) The cluster logic is testable in-process with the memory backend, no
  object storage required. The chaos harness runs both in-process and against
  real MinIO with the same code.
- (+) shale could run on `pebble` (local disk) or `memory` today - a sharded
  KV with copy-on-rebalance.
- Important nuance: the two object-storage SUPERPOWERS - zero-copy handoff
  (ADR 0005) and free cross-node fencing (ADR 0004) - are CONFERRED BY object
  storage, not enforced by the interface. A non-shared-storage backend can
  implement the `BackendFactory` contract but loses those properties: it would
  have to ship bytes on handoff and supply its own cross-node fencing. This is
  not an abstraction leak; those properties come FROM object storage. The
  interfaces keep us un-painted-into-a-corner, but the design is optimized for
  shared/object storage and that is the supported path.
