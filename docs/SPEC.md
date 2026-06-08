# shale - spec

A Go library that turns N processes each holding a local key-value backend into ONE logical KV cluster, with horizontal write scaling by consistent-hashed sharding.

```go
import "github.com/Zamua/shale/pkg/cluster"

c, _ := cluster.Open(cluster.Config{...})
c.Put([]byte("user:alice"), []byte("data"))
val, _ := c.Get([]byte("user:alice"))
```

App code uses `c` like a local KV store. Behind the interface, shale figures out which node owns the key + forwards the operation.

---

## What it is, what it isn't

- It IS: a clustering layer that gives any KV-shaped storage engine horizontal scale-out.
- It IS: an embedded Go library - apps import it, each instance is a cluster node.
- It IS: backend-agnostic. Default backend ships SlateDB-on-object-storage; any `Backend` impl works.
- It IS NOT: a database service. There's no `shaled` daemon to deploy separately from your app.
- It IS NOT: SQL-aware. No query parsing, no cross-shard JOINs, no transactions across multiple shards.
- It IS NOT: a Postgres / MySQL frontend. Use Vitess / Citus for those.
- It IS NOT: a Redis Cluster competitor. If your data fits in Redis's model, use Redis.

The sweet spot: you want sharded KV semantics on top of object storage (cheap durability), without running Cassandra / Scylla / TiKV as a separate service.

---

## API surface

```go
type Cluster struct{...}

func Open(cfg Config) (*Cluster, error)
func (c *Cluster) Close() error

// Same shape as the Backend interface - keeps app code unchanged whether you
// migrate from a single Backend to a Cluster wrapping multiple.
func (c *Cluster) Put(key, value []byte) error
func (c *Cluster) Get(key []byte) ([]byte, error)
func (c *Cluster) Delete(key []byte) error
func (c *Cluster) ScanPrefix(prefix []byte) Iterator
func (c *Cluster) Begin(IsolationLevel) Transaction

// Cross-shard operations are explicit so you can see the fan-out cost
// at the call site.
func (c *Cluster) Aggregate(fn func(backend.Backend) any) []any
```

Single-key ops are O(1) routing → one local-or-remote backend call. Cross-shard ops fan out to all owners.

---

## Backend interface

```go
package backend

type Backend interface {
    Put(key, value []byte) error
    Get(key []byte) ([]byte, error)
    Delete(key []byte) error
    ScanPrefix(prefix []byte) Iterator
    Begin(isolationLevel IsolationLevel) (Transaction, error)
    Close() error
}

type Iterator interface {
    Next() (key, value []byte, err error)
    Close() error
}

type Transaction interface {
    Get(key []byte) ([]byte, error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Commit() error
    Rollback() error
}
```

Minimum surface that lets the cluster layer do its job. No backend-specific options leak through.

Shipped impls:
- `pkg/backend/memory` - in-process map; tests + dev
- `pkg/backend/slate` - SlateDB-on-object-storage (planned for v0.1)

BYO: implement the interface, register the impl, done.

---

## Cluster model

### Membership

Each node runs a `memberlist` instance (HashiCorp's SWIM gossip protocol). Nodes discover each other via configured seed addresses. Memberlist handles:
- Heartbeats + failure detection
- Broadcast of node-join / node-leave events
- Node metadata (the node's gRPC listen address)

shale subscribes to membership events; when membership changes, the ring is recomputed + shard ownership shifts trigger rebalancing.

### Routing

The hash ring is **ring-based consistent hashing with bounded loads** (Karger 1997 + Mirrokni-Thorup 2017), implemented via `buraksezer/consistent`. Each node has ~64 virtual replicas on a 64-bit ring; a key hashes onto the ring and goes to the next clockwise virtual node. Adding/removing any node moves ~K/N keys.

For a key K:
1. `node := ring.LocateKey(K)` - local in-memory lookup, sub-microsecond
2. If `node.ID == local.ID` → call local Backend directly
3. Else → gRPC call to that node's shale service

Latency: local ops are local-Backend latency. Remote ops are local-Backend latency + one network RTT.

**Why ring-based and not Jump Consistent Hash:** Jump (Lamping-Veach 2014) is zero-state and faster but requires bucket numbers 0..N-1 - removing an arbitrary middle bucket requires re-numbering, which doesn't compose with our gossip-based membership where any node can leave anywhere. Ring-based fits dynamic topology naturally at the cost of a few KB of in-memory state per node + an O(log N) binary search per lookup. Both negligible at our scales.

### Shard keys + hash tags

By default, a key is hashed in its entirety. Apps that want to co-locate related keys on the same shard use the **hash tags convention** (borrowed from Redis Cluster):

```
key                              hashed-on
"pastes/abc12345"                "pastes/abc12345"   (whole key, default)
"{alice}/pastes/abc12345"        "alice"             (just the tag)
"{alice}/versions/abc12345/v2"   "alice"             (just the tag)
```

Anywhere a key contains `{...}`, ONLY the contents between the first matching pair of braces are hashed. Keys with the same tag value land on the same shard. Apps choose the convention based on access patterns:

  - **Hot path = single-key lookup, secondary = grouped list**: shard by the lookup key (no hash tag); accept fan-out for the grouped list.
  - **Hot path = per-group queries, secondary = random-key lookups**: shard by the group (hash tag); accept the extra lookup hop for random-key reads.

The trade-off is the same one DynamoDB forces at table-creation time when you pick the partition key - except shale lets you mix-and-match per key (different prefixes can use different tagging).

For apps that need more control than hash tags, `Config.ShardKeyFn` is an escape hatch:

```go
cluster.Open(cluster.Config{
    ShardKeyFn: func(key []byte) []byte { return extractFirstPathSegment(key) },
    ...
})
```

ShardKeyFn is called for every Put/Get/Delete before ring lookup. Default: identity (hash whole key, honoring hash tags).

### Cross-shard operations

Anything that touches more than one shard goes through `Cluster.Aggregate`:

```go
results := c.Aggregate(func(b backend.Backend) any {
    // runs LOCALLY on each node in parallel; sees only that node's keys
    var slugs []string
    it := b.ScanPrefix([]byte("pastes/"))
    defer it.Close()
    for {
        k, _, err := it.Next()
        if err != nil || k == nil { break }
        slugs = append(slugs, string(k[len("pastes/"):]))
    }
    return slugs
})
// results is []any - one per node. App flattens / merges as it sees fit.
```

Cost: O(K / N) per node, run in parallel. Wall-clock ≈ slowest-node scan + one gRPC round-trip. Bounded + safe for admin/rare operations. NOT recommended for hot-path queries (use shard keys to make those single-shard).

### Replication (v0.4+)

For replication factor R > 1:
- Writes go to the R "closest" nodes on the ring (primary + R-1 successors)
- Reads can be from any of the R (configurable: nearest, quorum, all)
- Failure of < R nodes doesn't lose writes (depending on consistency setting)

v0.1-v0.3 ship with R=1 only (no replication). v0.4 adds it.

### Rebalancing (v0.3+)

When a node joins / leaves:
1. Recompute ring → determines which key ranges change owners
2. For each migrating range: old owner pushes keys to new owner via gRPC
3. Atomic ownership swap: new owner accepts writes; old owner stops accepting
4. Old owner deletes migrated keys locally

Window during which a key range is being migrated: reads may go to either owner (consistent because storage is single-writer per range). Writes briefly block (~ms) at the swap point.

---

## Consistency model

shale is **per-key linearizable**. Each key has exactly one owner at a time; all ops for that key serialize through that owner's local Backend. Cross-key operations are NOT atomic (no distributed transactions).

The Backend's own transaction interface (`Begin/Commit/Rollback`) operates on the LOCAL Backend only - so transactions are scoped to keys owned by ONE node. shale's transaction proxy routes the transaction to that owner.

If a transaction touches keys owned by multiple nodes, shale returns an error. App code is expected to use a single shard-key prefix for related keys (the standard sharded-KV pattern).

---

## Failure handling

- **Single node crashes**: keys it owned are temporarily unavailable. With R > 1, replicas take over reads + writes.
- **Network partition**: nodes on each side see the other as failed. Both sides accept writes. On heal, conflicts resolve via Last-Write-Wins (LWW) using node-local timestamps. (R = 1 has no replication conflicts; R > 1 needs LWW.)
- **Backend failure on one node**: that node reports unhealthy to memberlist; gets removed from ring; data unavailable until restored.

---

## Non-goals

- **SQL queries** - Vitess (MySQL) / Citus (Postgres) do this well; shale stays out of that domain.
- **Cross-shard transactions** - requires consensus (Paxos/Raft); out of scope. Apps that need this use FoundationDB / TiKV.
- **Strong consistency across partitions** - shale chooses AP over CP (eventual consistency on partition heal). Need CP? Use Raft-based stores.
- **Multi-region replication** - out of scope for v1; possibly v2.
- **Backend-specific features** (Redis pub/sub, Postgres extensions, etc.) - the abstraction stays minimal.

---

## Roadmap

| Version | Scope |
| --- | --- |
| v0.1 | Single-node `Cluster` wrapping one Backend. Memory backend impl. SlateDB backend impl. API lockup. |
| v0.2 | Multi-node + memberlist + hash ring + gRPC forwarding. Static topology (no rebalance). |
| v0.3 | Rebalancing on join/leave. Atomic ownership swap. |
| v0.4 | Replication factor R + read consistency settings (nearest / quorum / all). LWW conflict resolution. |
| v0.5 | Prometheus metrics, tracing hooks, real benchmarks vs single-node SlateDB. |
| v0.6 | hostthis migration: swap `slatedb.Db` for `shale.Cluster` with backend=SlateDB. Validate on production-like data. |

Each version ships independently; users can adopt v0.1 today (functionally equivalent to using their Backend directly) and grow into v0.2+ when their workload demands it.

---

## Inspirations

- **Olric** - same memberlist + consistent-hash pattern. In-memory only; we generalize to durable backends.
- **Vasto** - sharded RocksDB with jump consistent hash + single-master topology. Dormant; architecture is sound and we borrow heavily. We diverge by using ring-based hashing + gossip membership instead.
- **Cassandra / ScyllaDB** - shard-per-core + gossip + ring topology. Heavyweight services; we want library shape with the same model.
- **DynamoDB** - the access-pattern discipline. DDB forces you to commit to a partition key at table-creation time, which drives the rest of your data model. shale relaxes this to per-key hash tags, but the underlying principle (think about access patterns before key design) is the same.
- **Redis Cluster** - the hash-tag convention (`{tag}`-based key co-location) is borrowed directly.
- **SlateDB** - the default backend. Provides KV semantics on cheap object storage; we provide horizontal scale-out around it.
