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
func (c *Cluster) Aggregate(fn func(backend.Backend) any) []AggregateResult

// AggregateResult separates "fn ran + here's its return value" from
// "shale couldn't even run fn here" (snapshot fetch failed, peer
// unreachable). Exactly one of Value / Err is meaningful per entry.
type AggregateResult struct {
    Value any
    Err   error
}
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
// results is []AggregateResult - one per node. App checks .Err
// (snapshot-transport failure for that peer) before reading .Value,
// then flattens / merges as it sees fit.
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

## CLI + standalone binary

shale ships two binaries from v0.1, separate from the library import path. Their purpose is dev-cycle ergonomics: poking at a running cluster from the shell, spinning up nodes for integration tests, inspecting topology + state during debugging.

### `shaled` (standalone node)

Runs a shale node as its own process, without an app embedding it. Backend chosen via flag. Useful for:
  - Integration tests (shell scripts spin up N nodes on ephemeral ports)
  - Local multi-node dev clusters
  - Operators who want a managed shale process per host (instead of embedding inside their app)

```
shaled \
  --node-id node-1 \
  --bind-addr :7946 \
  --grpc-addr :7947 \
  --seeds node-2:7946,node-3:7946 \
  --backend memory|slate \
  --slate-bucket my-bucket           # if --backend=slate
```

shaled exposes the same gRPC service the inter-node forwarding layer uses. The CLI talks to it via that gRPC. For v0.1 (single-node), shaled is functional standalone: one node, one Backend, gRPC up so the CLI can hit it.

### `shale` (CLI)

Connects to ONE node's gRPC endpoint (via `--addr` flag or `SHALE_NODE` env, defaults to `127.0.0.1:7947`). All subcommands are operator + developer ergonomics; they're NOT a replacement for the library API (apps embed shale, they don't shell out to it).

Subcommands shipped in v0.1:

  - `shale put <key> <value>` - one-shot write; the node routes per the ring (single-node in v0.1 = always local)
  - `shale get <key>` - one-shot read; prints value to stdout, exits non-zero on not-found
  - `shale delete <key>` - one-shot delete; idempotent
  - `shale scan <prefix>` - prints key=value pairs for everything under prefix
  - `shale topology` - prints the cluster membership + ring: which nodes exist, what range each owns. In single-node v0.1, prints "single-node cluster, node=<id>"
  - `shale stats` - per-node counters: key count, request rate, p50/p99 latency for Put/Get
  - `shale ping` - liveness check; exits 0 if the node responds

Subcommands shipped in later versions:

  - v0.3+: `shale rebalance` - trigger immediate rebalance (normally automatic)
  - v0.3+: `shale migrate-from <backend-spec>` - one-shot migrator from an existing backend (e.g. raw SlateDB) into the shale cluster
  - v0.5+: `shale bench` - load tester; reads + writes at configurable rates

### Output conventions

  - One value per line on stdout, parseable by shell tools
  - Status / errors on stderr
  - Exit codes: 0 success, 1 generic error, 2 not-found, 3 connection error, 4 timeout
  - `--json` flag on every read subcommand for structured output (topology, stats)

### Why ship the CLI in v0.1

The library-only argument is "developers will write Go to test it." That's right for the LIBRARY surface (Put/Get/Delete from app code), but wrong for the OPERATOR surface (is the cluster healthy? who owns this key? what's the per-node load?). Without the CLI:

  - Dev cycle is slow: every quick check requires `go run`-ing a one-off program
  - Integration tests need scaffolding to spin up nodes; shaled lets shell scripts do it
  - Debugging a misbehaving cluster means writing custom inspect-via-Go programs
  - Migration tools (v0.3+) don't have a natural home

The CLI + shaled aren't "nice-to-have"; they're the daily-driver tools for everyone touching shale. Build them with v0.1.

---

## System testing

Local multi-node testing on a single machine is a first-class workflow. It validates the distributed-systems correctness (routing, membership transitions, rebalancing) in a controllable environment without paying for a real cloud cluster.

### Available per version

  - **v0.1**: NOT available. v0.1 ships single-node only. Multiple shaled processes run side-by-side but are independent islands; no cross-node routing.
  - **v0.2+**: full local multi-node testing. N shaled processes join a single logical cluster via memberlist + gRPC forwarding. Throughput tests + topology-change tests become possible.
  - **v0.3+**: rebalancing tests (kill a node, watch keys migrate to survivors; bring the node back, watch keys rebalance home).
  - **v0.4+**: replication tests (with R>1, kill a node mid-test, observe reads + writes continue against the surviving replicas).

### Local 3-node cluster (v0.2+ example)

Each node uses a distinct bucket prefix in shared object storage so their SlateDB stores don't conflict on the single-writer epoch:

```
# shared object storage (any S3-compatible; local MinIO works)
# each node writes under its own prefix:
#   shale-test/node-1/
#   shale-test/node-2/
#   shale-test/node-3/

shaled --node-id n1 --grpc-addr :7947 --bind-addr :7946 \
       --backend slate --slate-bucket shale-test --slate-db-name node-1 \
       --slate-endpoint http://localhost:9000 ...

shaled --node-id n2 --grpc-addr :7949 --bind-addr :7948 --seeds 127.0.0.1:7946 \
       --backend slate --slate-bucket shale-test --slate-db-name node-2 ...

shaled --node-id n3 --grpc-addr :7951 --bind-addr :7950 --seeds 127.0.0.1:7946 \
       --backend slate --slate-bucket shale-test --slate-db-name node-3 ...

# inspect + load test
shale topology --addr 127.0.0.1:7947   # all 3 nodes + ring assignments
shale bench --addr 127.0.0.1:7947 --writes 100k --keys-prefix bench:
```

`shale bench` (v0.5+) reports aggregate throughput, per-node request distribution, p50/p99 latencies.

### Throughput scaling expectations

On a single local machine, throughput should grow as nodes are added until a shared bottleneck is hit:

  - **CPU**: linear up to about (cores - 1) nodes. Each shaled process is small but each one runs its own SlateDB compaction, gRPC handlers, etc.
  - **Object storage**: usually the dominant bottleneck. A single local MinIO process serializes I/O; aggregate write throughput plateaus once MinIO saturates regardless of how many shaled nodes are above it.
  - **Local loopback network**: not a real constraint at our scales.

To exercise more dramatic local scaling, run multiple MinIO instances (each on its own port) and point each shaled node at its own dedicated MinIO. Bottleneck then moves to NVMe + CPU; typical 4-6x linear scaling before hitting limits on a beefy dev machine.

True linear scaling requires distributing nodes across machines (or against a managed object store like R2/S3 with independent capacity per shard). The local single-machine test demonstrates the routing + load-distribution patterns; cloud testing validates the linear-scaling claim under real network + storage isolation.

### Topology-change tests

The more valuable local tests are the failure-injection ones:

  1. **Node kill**: start 3 nodes serving 100k keys; pull the plug on one mid-test.
     - v0.2 with R=1: requests for that node's keys fail. Cluster topology view shrinks.
     - v0.4+ with R>1: replicas take over reads + writes seamlessly.

  2. **Node return**: bring the killed node back.
     - v0.2: it rejoins as an empty node. New keys hashing to it will land; old keys it used to own stay on the temporary takeover node (which v0.2 doesn't have, so they're just gone).
     - v0.3+: rebalancer migrates keys back from successors; ownership returns to the rejoined node.

  3. **Network partition**: simulate one side losing sight of the other (block memberlist UDP between two groups of processes).
     - v0.2 with R=1: both sides accept writes to keys they think they own. On heal, writes diverge; conflicts resolve via LWW or whatever policy is configured.
     - v0.4+ with R>=2: quorum reads/writes prevent split-brain divergence.

  4. **Slow node**: tc/iptables-induce 500ms latency on one node's gRPC.
     - Validates timeouts + retries at the cluster layer.
     - Validates that the bounded-loads consistent hashing doesn't pile work onto a struggling node.

  5. **Backend failure**: kill the MinIO instance one node depends on.
     - Validates that the node detects the failure, marks itself unhealthy, propagates that to memberlist, peers route around it.

### Test framework

The `tests/integration/` directory (per CLAUDE.md layout) holds in-process tests that spin up N nodes via goroutines + ephemeral ports. No external services required for the basic correctness tests. The MinIO-backed scaling tests are separate (`tests/scaling/`) and require an operator to bring up MinIO first.

The CI matrix runs the integration tests on every PR; the scaling tests run on demand.

---

## Roadmap

- [ ] **v0.1** - single-node Cluster wrapping one Backend; memory backend impl; SlateDB backend impl; gRPC service (used by CLI + ready for v0.2 inter-node); `shaled` standalone binary; `shale` CLI with put/get/delete/scan/topology/stats/ping. API lockup.
- [ ] **v0.2** - multi-node + memberlist + hash ring + gRPC forwarding. Static topology (no rebalance). `shale topology` now shows real membership + ring.
- [ ] **v0.3** - rebalancing on join/leave. Atomic ownership swap. `shale rebalance` + `shale migrate-from` subcommands.
- [ ] **v0.4** - replication factor R + read consistency settings (nearest / quorum / all). LWW conflict resolution.
- [ ] **v0.5** - Prometheus metrics, tracing hooks, real benchmarks vs single-node SlateDB. `shale bench` subcommand.
- [ ] **v0.6** - hostthis migration: swap raw SlateDB for shale-with-SlateDB-backend. Validate on production-like data.

Each version ships independently; users can adopt v0.1 today (functionally equivalent to using their Backend directly, plus the CLI for daily ergonomics) and grow into v0.2+ when their workload demands it.

---

## Inspirations

- **Olric** - same memberlist + consistent-hash pattern. In-memory only; we generalize to durable backends.
- **Vasto** - sharded RocksDB with jump consistent hash + single-master topology. Dormant; architecture is sound and we borrow heavily. We diverge by using ring-based hashing + gossip membership instead.
- **Cassandra / ScyllaDB** - shard-per-core + gossip + ring topology. Heavyweight services; we want library shape with the same model.
- **DynamoDB** - the access-pattern discipline. DDB forces you to commit to a partition key at table-creation time, which drives the rest of your data model. shale relaxes this to per-key hash tags, but the underlying principle (think about access patterns before key design) is the same.
- **Redis Cluster** - the hash-tag convention (`{tag}`-based key co-location) is borrowed directly.
- **SlateDB** - the default backend. Provides KV semantics on cheap object storage; we provide horizontal scale-out around it.
