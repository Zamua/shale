# Per-shard lease-handoff storage (design)

Status: PROPOSED (2026-06-11). Targets shale v0.8. Supersedes the current
per-node storage model. This doc is the design; `SPEC.md` continues to
describe the SHIPPED (per-node) behavior until this lands.

## Why

Today shale is one-database-per-node: each node owns a single slatedb (one
LSM under its own object-store prefix) holding every key it owns. That is
simple and proven lossless, but a rebalance must COPY the moved keys from
one node's LSM into another's (the owning engine reads each key's merged
value and streams it over gRPC; the receiver writes it into its own LSM).
The bytes are already durable in object storage, but they live as one
node's LSM files that only that node's engine can interpret, so they can't
just be re-pointed.

The goal is copy-free rebalance and online elastic growth, the cloud-native
shape: data has a permanent home in object storage, and scaling only moves
OWNERSHIP, never bytes.

## The model: three layers

- **Shard** (routing + co-location). `ShardKeyFn(key) -> shard`, a stable
  hash bucket. Co-located keys (a record and its versions) share a shard so
  a transaction stays within one engine. Fine-grained; today's count (271)
  is fine. UNCHANGED from today.
- **Storage unit** (physical + lease). A FIXED, moderate count N of small,
  self-contained slatedb databases, each its own object-store prefix. A
  shard maps to a unit by `shard -> unit` (e.g. a fixed function of the
  shard id). Many shards live in one unit. The unit is the unit of storage
  AND of ownership/lease. N is bounded above by per-engine memory (see
  Constraints) and below by max node count; pick it for the ceiling.
- **Node** (compute). A node MOUNTS (opens) the slatedb instances for the
  units it currently owns. It owns ~N/nodes of them. Adding a node moves
  unit leases to it.

Routing is still a pure function: `key -> ShardKeyFn -> shard -> unit ->
owner`. The owner lookup is the ring over UNIT IDS (N of them), so the ring
operates on units, not the 271 shards. No dynamic range catalog.

## Two maps, opposite natures

- **Data-location map** (`shard -> unit -> object-store prefix`): STATIC. A
  unit's bytes have a permanent home. This permanence is what makes
  rebalance copy-free.
- **Ownership map** (`unit -> node`): DYNAMIC. This is the ring; it changes
  on every membership event. Rebalance = re-run this map + hand leases over.

## Ownership = a lease (the single-writer reality)

slatedb is single-writer per database (writer-epoch fencing: opening at a
higher epoch fences the prior writer). That is WHY there must be more than
one database to get concurrent writers, and it is also the lease primitive.

Lease handoff of unit U from node A to node B:
1. B is assigned U by the new ring.
2. A flushes U's memtable, stops writing U, and signals release.
3. B opens U's slatedb at epoch+1 (fences any stale A writer), and begins
   serving U.
4. The object-store bytes never move. Zero copy.

During the brief A-release -> B-acquire window, U is momentarily
unavailable; route reads to A until B acquires, or fail-fast + retry (reuse
the v0.3 cutover behavior: try_other_owner on reads, FailedPrecondition on
writes, for that one unit). Per-unit, sub-second, the rest of the cluster
undisturbed.

## Rebalance (normal, copy-free)

A membership change re-runs the ownership map. Each unit whose owner changed
is a lease handoff (above). No FetchRange/stream-the-keys path; the bytes
are already where they need to be. The anti-entropy reconcile we built for
the per-node model generalizes: a node that the ring says owns U but does
not have U MOUNTED, mounts it (acquires the lease). Self-healing as before.

## Resharding (grow by DOUBLING, online, no downtime)

When N units across N nodes is maxed (one unit per node) and you must grow,
DOUBLE: N -> 2N. The key property: a key's unit is `hash(key) mod N`; under
doubling, `hash mod 2N = (hash mod N) + N * (one more hash bit)`. So every
unit K splits into EXACTLY TWO new units, K and K+N, decided by a single
additional hash bit. No global re-partition; each unit cleanly bisects.

Per-unit, online procedure (reuses the lease handoff's copy-then-cutover):
1. For live unit K, in the background create units K and K+N (two fresh
   slatedbs) and stream K's keys into them, routed by the new hash bit. K
   keeps serving throughout (pure background copy).
2. Near catch-up: brief write-pause for K's keys only, drain the last
   writes, atomically flip routing for K's key-space from old-unit-K to new
   units K and K+N. Retire old unit K.
3. March through all N units one at a time (or a few in parallel). Only one
   unit's keys ever see a sub-second blink; the cluster serves throughout.
4. After all bisect: 2N self-contained units. Redistribute via copy-free
   lease handoff. New nodes grab units.

The split DOES copy one unit's data once (the keys re-bucket by the new
hash bit), but it is per-unit, parallelizable, bounded (a node rewrites only
its own units once), and online. Routing state added during a reshard is
tiny: a generation number (N -> 2N) plus a per-unit "has this cut over" flag.

This gives online incremental elastic growth (the appeal of range-based
sharding) while keeping hash's even distribution and skipping the dynamic
range catalog / Placement-Driver subsystem entirely. The only things given
up vs range-based: arbitrary-granularity resizing (doubling only) and
per-range hotspot splitting. Neither is needed for hash-uniform keys.

## Migration from the per-node model

One-time: read each node's current single LSM and re-write every key into
its `shard -> unit` destination database. This is the same shape as the
slatedb-direct -> shale migration (a standalone op tool, additive, validated
on a bucket copy first). After it, the per-node dbs are retired and the
units are the source of truth.

## Constraints (why N is bounded)

- **Memtable memory dominates per-engine cost.** slatedb's default
  `MaxMemtableBytes` is 64 MB. N engines per node at default = N * 64 MB,
  which blows a small node fast. `MaxMemtableBytes` is tunable: shrink to a
  few MB and N engines fit (N * ~4 MB). The downside (more frequent SST
  flushes) only bites at high write volume; for low-write workloads a small
  memtable rarely fills so the penalty is moot. Pick N and the memtable
  size together against the node's RAM and the write rate.
- **Single-writer per unit** is preserved end to end (each unit's slatedb
  has one writer = its current owning node). Cross-unit transactions are not
  supported; co-location via ShardKeyFn keeps a transaction inside one
  shard, hence one unit.
- **Block cache** is a second per-engine cost; tune or share it.

## Implementation phases

1. **Storage-unit abstraction + multi-backend node.** Replace
   `cluster.Config.Backend` (one engine) with a backend FACTORY + a
   `unit -> backend` map the node mounts/unmounts. The ring operates on unit
   ids. Routing: `key -> shard -> unit -> owner`.
2. **Lease handoff rebalance.** Replace the copy-based stream with
   close-on-old-owner / open-at-epoch+1-on-new-owner. Preserve the handoff
   window behavior + the anti-entropy reconcile (own-but-not-mounted ->
   mount).
3. **Doubling resharder.** The online per-unit bisect + atomic cutover +
   the generation/per-unit-cutover routing state.
4. **Migration tool.** Per-node LSM -> per-unit LSMs (standalone, additive,
   dry-run on a copy).
5. **Memtable/cache tuning surface** so operators size N against RAM.

Each phase: spec-first, TDD, adversarial review, validated on the slate
backend against live object storage (the per-node redesign's gate test
pattern). Ship behind a config flag; the per-node path stays until cutover.
