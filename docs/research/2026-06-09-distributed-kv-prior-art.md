# Prior art: how distributed K-V systems handle the tensions shale faces

Research note, 2026-06-09. Survey of how production distributed key-value
and database systems handle three design tensions shale is hitting as it
grows from "single SlateDB instance" toward "sharded, replicated, HA K-V
with hostthis as first consumer." Scale we care about: tens of GB, dozens
of writes/sec, hundreds of users. Explicitly NOT Google scale.

This is a research note, not a spec. It informs design decisions; it does
not commit to any of them. The canonical design is still `docs/SPEC.md`.

Sources are primary where possible (papers, vendor design docs, RFCs).
Each claim below was independently verified during the survey; the ones
that survived adversarial review are what's reported here.

---

## 1. Cross-shard atomicity: how the field does multi-key atomic writes

The question: when an atomic write touches keys that live on different
shards, what do real systems do?

### FoundationDB: thin transactional core, features as client "layers"

FoundationDB provides multi-key strictly-serializable transactions across
its *entire* keyspace, via an unbundled architecture: a Sequencer hands
out commit versions (~1M/sec), range-partitioned Resolvers do lock-free
parallel read-write conflict detection, and a commit proceeds only if
*all* Resolvers admit it (otherwise abort + client retry). It combines
optimistic concurrency control with MVCC.

The crucial design choice for us: FDB ships **no built-in secondary
indexes, no query language, no schema**. Everything higher-level (indexes,
referential integrity, the relational Record Layer, even a JanusGraph
backend) is built as a *stateless client-side layer* on top of the
transactional KV core. The core stays small and correct; complexity lives
in layers.

Source: FoundationDB SIGMOD 2021 paper.

### TiKV / Percolator: 2PC with a primary lock as the commit point

TiKV implements cross-shard atomic transactions with Percolator-style
two-phase commit. The mechanism worth internalizing:

- The transaction's writes each take a lock during *prewrite*.
- One lock is designated the **primary lock**; every *secondary* lock
  stores a pointer back to the primary.
- The transaction commits atomically via a **single-shard write** that
  removes the primary lock and writes the commit record at `commit_ts`.
  The instant that one write lands, the whole transaction is durable.
- Committing the secondary locks is *async cleanup*. It can fail without
  affecting durability: a later reader that finds a secondary lock follows
  the pointer to the primary, sees the primary is committed, and rolls the
  secondary forward.

A centralized timestamp oracle (embedded in PD, made HA via Raft) hands
out strictly increasing timestamps for every read and write.

The optimization that matters most for us: **single-region transactions
skip 2PC entirely and use 1PC** - saving one RPC and one write, measured
at ~87% throughput gain on `sysbench oltp_update_non_index`. If a
transaction touches only one shard, the whole 2PC machinery is unnecessary.

Sources: TiKV deep-dive docs (percolator, optimized-percolator), TiDB
dev guide (1pc).

### DynamoDB transactions library: coordinator-free 2PC

The `awslabs/dynamodb-transactions` design (now superseded by the native
`TransactWriteItems` API, but the design is the reference) shows you can
do multi-item 2PC with **no external coordinator** (no Zookeeper, no
dedicated lock service). The trick: the transaction's own state lives as a
**regular row in the data store** (a "TX record"). Multiple processes can
drive the same transaction forward concurrently, so a coordinator crash
doesn't strand the transaction - any process picks it up. Recovery for
stuck transactions is a sweeper process + optimistic concurrency on the TX
record.

Source: awslabs/dynamodb-transactions DESIGN.md.

### etcd: a coordination store, NOT a database

etcd is explicitly scoped as a metadata/coordination store. Its own docs
say: past "a few GB" of data, use a NewSQL database instead. It **cannot
horizontally scale** (no sharding). It gives linearizable reads via Raft
and supports comparison-based multi-key atomic transactions (compare +
read + write, each assigned a global monotonic revision), trading
availability for strong consistency.

The lesson: if shale ever wants an external coordinator for shard
metadata or a timestamp oracle, etcd is the right tool - but ONLY for
the tiny coordination state (membership, shard map), never for data.

Source: etcd v3.5 "why" doc.

### Synthesis for shale

The field offers a clean menu, ordered by complexity:

1. **No cross-shard transactions** (Cassandra-style): accept eventual
   consistency for anything spanning shards; never attempt atomic
   multi-shard writes. Simplest.
2. **1PC for single-shard, 2PC only when keys genuinely cross shards**
   (TiKV): the dominant open-source answer. Single-shard is the fast path.
3. **External coordinator** (etcd/Zookeeper): only for tiny coordination
   state, never the bottleneck data path.

For shale's scale, the strong signal is: make single-shard transactions
the fast path (1PC, which maps directly onto SlateDB's native per-store
transaction), and do NOT build Percolator 2PC until a real cross-shard
atomic requirement appears. The complexity of 2PC is real and our write
rate (dozens/sec) does not demand it yet.

---

## 2. Eventually-consistent secondary indexes: nobody does this synchronously

The question: when a secondary index lives on a different shard than the
primary data, how do real systems keep it consistent?

The answer is unanimous across every production system surveyed: **they
don't keep it synchronously consistent.** They ship eventually-consistent
indexes with documented failure modes plus an out-of-band operator repair
tool.

- **ScyllaDB materialized views**: can get persistently *stuck* - base
  inserts succeed but view rows never materialize. The documented recovery
  is to **drop and recreate the view**. (Source: scylladb/scylladb#3116.)
- **Riak secondary indexes (2i)**: can become stale or missing; recovery
  is `riak-admin repair-2i`, which scans and repairs mismatches between
  the index data and the stored objects. Riak *Search* indexes have **no
  anti-entropy or read-repair at all** - inconsistent results can be
  returned after replica loss, and repair is operator-initiated. (Source:
  Riak KV repair-recovery docs.)
- **DynamoDB Global Secondary Indexes**: explicitly only eventually
  consistent. There is *no* `ConsistentRead` option on a GSI query. There
  is always a propagation delay between a parent-table write and its
  appearance in the index. (Source: AWS DynamoDB GSI docs.)

### Synthesis for shale + hostthis

This directly validates the design sketched for hostthis-on-shale:

- **Split authoritative data from derived data.** Authoritative (the
  paste row, version history, owner pointer, expiry index) is
  slug-keyed, co-located on one shard, written atomically. Derived
  (the per-identity "list my pastes" index, the first-seen aggregate)
  is identity-keyed, on a *different* shard, updated **eventually**.
- **Do not attempt synchronous cross-shard index maintenance.** Every
  system that tried variants of this ships a repair tool because the
  synchronous path is not reliable.
- **Ship an explicit repair command.** `hostthis reindex` (or a shale-level
  `repair-index`) is not a hack or an admission of failure - it is what
  ScyllaDB, Riak, and DynamoDB operators all have. The "repair-on-read +
  background reconciler" pattern is the lightweight version of exactly
  this.

The "repair-on-read feels hacky" instinct is worth examining against this:
the entire industry treats out-of-band index repair as the *normal*
operating model for cross-shard indexes, not an embarrassment.

---

## 3. Compute-storage separation on blob storage: SlateDB is the reference

The question: how do "X-on-object-storage" systems handle writes and reads
on top of eventually-consistent blob storage?

The survey fetched WarpStream (Kafka-on-S3), Turbopuffer (vector search on
object storage), Materialize (persist layer), and Tigris, but the claims
that survived adversarial verification anchor almost entirely on **SlateDB
itself** - which is fitting, since shale uses it. The broader pattern the
others share (monotonic epoch fencing, manifest-as-linearization-point,
object-store CAS where available) matches SlateDB's design.

### SlateDB's core divergence: everything goes to object storage

SlateDB's central architectural choice is writing ALL data - WAL,
MemTables, SSTs, manifest - to object storage instead of local disk. This
is what makes compute and storage separable: any node with credentials can
read the database; durability is the object store's job.

### Writer epochs fence zombie writers

To safely tolerate multiple *potential* writers on the same database,
SlateDB uses a monotonically increasing `writer_epoch` in the manifest:

- A new writer transactionally increments the epoch on startup.
- It then writes `max_parallel_writes` sequential **fencing SSTs**
  starting at the first open WAL slot, guaranteeing any predecessor writer
  still in its own write window will see the higher epoch in its target
  slot and halt.
- Any writer whose epoch is lower than the manifest's current
  `writer_epoch` is a **zombie** and must immediately stop.

This is exactly the single-writer safety shale relies on per-shard. shale
does NOT need to reinvent writer fencing - SlateDB already guarantees it.
shale's job is to route each shard's writes to a single live primary; if
two ever overlap (e.g. during a rolling deploy), SlateDB's epoch fencing
makes the loser halt rather than corrupt.

### CAS-where-available, external coordinator as fallback

When the object store does NOT provide compare-and-swap (vanilla S3 before
Nov 2024), SlateDB falls back to a two-phase write using an external
transactional store (DynamoDB) as the durable coordination point: the
write becomes durable the moment an intent record lands in DynamoDB, and a
destination-uniqueness constraint on that row provides the atomicity S3
alone cannot. When the object store DOES support conditional writes,
SlateDB drops the external coordinator entirely.

This matters directly for the planned **R2 migration**: Cloudflare R2
supports conditional writes, so SlateDB-on-R2 can use the CAS path with no
DynamoDB-style coordinator. One fewer moving part than an S3-classic
deployment.

### Synthesis for shale

- **Lean on SlateDB's epoch fencing as shale's per-shard writer-safety
  primitive.** Don't build a parallel fencing mechanism.
- **Accept the manifest as the single linearization point per shard.**
  Don't chase cross-shard global ordering; it is not needed at our scale.
- **Prefer object-store CAS over an external coordinator.** R2 supports
  it; this simplifies the storage layer relative to S3-classic.

---

## Patterns worth adopting for shale + hostthis

Concrete, in rough priority order.

1. **Make single-shard transactions the fast path (1PC).** Map shale's
   `Begin/Put/Commit` directly onto SlateDB's native per-store
   transaction when all keys hash to one shard. This is the TiKV 1PC
   lesson: single-shard writes should never pay 2PC cost. For hostthis's
   slug-keyed authoritative writes, this is the *only* path needed.

2. **Do NOT build cross-shard 2PC yet.** At dozens of writes/sec and tens
   of GB, Percolator-style 2PC is complexity that does not earn its keep.
   Design the API so it *could* be added later (a transaction that detects
   cross-shard keys can error loudly rather than silently doing the wrong
   thing), but don't implement it until a real consumer needs cross-shard
   atomicity.

3. **Split authoritative from derived; make derived eventually
   consistent.** For hostthis: slug-sharded authoritative data (atomic,
   single-shard tx) + identity-sharded derived indexes (eventually
   consistent). This is what every surveyed system does. It is not a
   compromise; it is the standard model.

4. **Ship an explicit index-repair command.** `hostthis reindex` /
   shale `repair-index`. ScyllaDB, Riak, and DynamoDB all have this. It
   is the operator escape hatch for when the eventual path falls behind or
   a write is lost. Pair it with repair-on-read for the common case.

5. **Lean on SlateDB's writer-epoch fencing.** shale's per-shard
   single-writer guarantee is already provided by SlateDB. Route writes to
   one primary per shard; trust SlateDB to fence any overlap. Don't
   reinvent fencing.

6. **Keep shale a thin transactional KV; push app logic to the consumer.**
   FoundationDB's "core stays small, features are client layers" is the
   right shape. Secondary-index logic for hostthis belongs in hostthis,
   not baked into shale. shale stays a clean KV with hash-tag co-location
   + single-shard transactions.

7. **Store transaction/reconciliation state as regular shale rows, not in
   an external coordinator.** The DynamoDB-transactions pattern: a
   "pending derived update" marker is just another K-V row. Any node can
   drive reconciliation; a sweeper consumes markers. No Zookeeper/etcd on
   the data path.

8. **If shale ever needs external coordination (shard map, membership,
   TSO), use etcd ONLY for that tiny state - never for data.** And note
   shale already uses memberlist gossip, so it may need no external
   coordinator at all.

9. **Prefer object-store CAS over an external coordinator for the storage
   layer.** The R2 migration unlocks SlateDB's conditional-write path,
   removing the need for a DynamoDB-style coordinator that S3-classic would
   require.

10. **Seriously consider whether shale needs sharding at all for current
    consumers.** A genuine open question this survey raises: at our scale,
    a single SlateDB instance with R=N replication (HA, no sharding) may be
    the right answer, with sharding + 2PC deferred until a consumer
    actually outgrows a single instance. Sharding buys horizontal storage +
    write-throughput scale that hostthis (dozens of writes/sec, single-digit
    GB) does not need yet. Don't pay for it before there's a consumer that
    does. (This tension is unresolved and worth a deliberate decision
    before committing to the sharded design.)

---

## Open questions the survey did not resolve

- WarpStream / Turbopuffer / Tigris specifics on producer ordering, write
  idempotency, and read consistency over eventually-consistent object
  storage were fetched but did not survive into the verified claim set.
  Worth a dedicated follow-up if we go deeper on the storage layer.
- For a SlateDB-backed CDC source feeding eventually-consistent indexes:
  does SlateDB expose a durable change feed, or do we fan out writer-side
  before commit? The durable-but-delayed vs fast-but-lossy-on-crash
  trade-off is unresolved.
- If shale adopts a timestamp oracle (for cross-shard ordering), how do we
  keep the TSO round-trip off the write latency path at our scale? TiKV's
  batched-TSO answer assumes a much higher write rate than ours.

---

## Sources

Primary: FoundationDB SIGMOD 2021 paper; TiKV deep-dive (percolator,
optimized-percolator); TiDB dev guide (1pc); awslabs/dynamodb-transactions
DESIGN.md; etcd v3.5 "why"; SlateDB RFC 0001 (manifest) + design overview;
scylladb/scylladb#3116; Riak KV repair-recovery docs; AWS DynamoDB GSI
docs; WarpStream architecture (write-path); Turbopuffer architecture;
Materialize architecture + persist design; Tigris-on-FoundationDB.

25 claims verified under 3-vote adversarial review, 0 refuted.
