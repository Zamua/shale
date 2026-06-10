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
- `pkg/backend/memory` - in-process map; tests + dev. Lives in the core shale module; tests + dev workflows depend on it.
- `backends/slate` - SlateDB-on-object-storage (separate Go module; see "Repo layout" below).
- `backends/pebble` - Pebble local-disk LSM (separate Go module; see "Repo layout" below).

BYO: implement the interface in your own Go module (the interface is small + stable), import shale's core, register the impl, done. The `backends/` subdirectories in this repo are reference impls; nothing about them is privileged.

### Backend durability is a backend concern

Shale's contract is "Put returns when Backend.Put returns." What that ack MEANS (in-memory only? local fsync? round-trip to object storage?) is up to the backend. Shale doesn't translate, doesn't enforce, doesn't expose a Durability enum at the cluster level.

This means a backend can ship a "fast-ack, eventually-durable" mode if it wants to. Slate, for instance, supports `await_durable=false` which acks at memtable insert (microseconds) rather than waiting for the WAL flush to S3 (~100ms). The slate backend exposes this as a `WriteOptions` pass-through on `slate.Config` (see "Backend-specific Settings pass-through" below); operator picks it at backend construction, shale neither knows nor cares.

CAVEAT FOR FAST-ACK BACKENDS WITH SHALE REPLICATION

If your Backend's Put can return BEFORE durable persistence, you should pair it with `ReplicationFactor >= 2`. The reason: a single replica crash inside the backend's loss window would otherwise destroy un-flushed writes. With R = 2 or more, loss requires every ack'd replica to crash inside the same window, which is much rarer for independent failures.

This is a NOTE, not enforcement. Shale doesn't refuse R = 1 with a fast-ack backend; the operator's responsibility.

For correlated failures (whole-DC outage, same-software-bug crash cascade), even R >= 2 doesn't fully protect a fast-ack backend. Spreading replicas across failure domains helps; documenting that the loss window exists is essential.

See `backends/slate/README.md` for slate's specific durability options.

### Backend-specific Settings pass-through

Each backend's `Config` struct optionally carries a `Settings` (or `Options`) field whose type is owned by the underlying engine, not by shale. The constructor forwards it untouched. Shale never reads, mutates, or wraps it; it never knows what's in it.

Concrete shapes:

```go
// backends/slate
type Config struct {
    Bucket    string
    DbName    string
    Endpoint  string
    Region    string
    AccessKey string
    SecretKey string
    UseSSL    bool

    // Settings is forwarded verbatim to slatedb. Nil = slatedb defaults.
    // Operator owns the lifecycle: build the *slatedb.Settings, pass it
    // here, slate.New hands it to NewDbBuilder. Shale neither inspects
    // nor copies it.
    Settings *slatedb.Settings

    // WriteOptions, if non-nil, is applied to every Put/Delete and to
    // every transaction Commit via slatedb's *WithOptions APIs. Nil =
    // slatedb defaults (AwaitDurable=true). Set AwaitDurable=false to
    // opt into fast-ack mode (see durability note above; pair with
    // ReplicationFactor >= 2). slatedb-go separates database-level
    // configuration (Settings, applied at Build) from per-write
    // configuration (WriteOptions, applied per call); slate's
    // pass-through mirrors that separation.
    WriteOptions *slatedb.WriteOptions
}

// backends/pebble
type Config struct {
    Dir string

    // Options is forwarded verbatim to pebble.Open. Nil = pebble defaults.
    Options *pebble.Options
}

// pkg/backend/memory
type Config struct{} // nothing to pass through; no underlying engine.
```

Example: an operator who wants slate's `await_durable=false` mode (acks at memtable insert, ~microseconds, instead of waiting for the WAL flush to object storage, ~100ms) hands a `WriteOptions` value to the backend. In the slatedb-go v0.13.1 binding `AwaitDurable` lives on `WriteOptions` (per-write knob), not on `Settings` (database-level config); the slate backend forwards both:

```go
wopts := &slatedb.WriteOptions{AwaitDurable: false} // fast-ack, backend-specific

be, _ := slate.New(slate.Config{
    Bucket:       "my-bucket",
    DbName:       "my-db",
    WriteOptions: wopts,
})

c, _ := cluster.Open(cluster.Config{
    NodeID:            "n1",
    Backend:           be,
    ReplicationFactor: 2,  // recommended pairing for fast-ack; see "durability" above
    ...
})
```

Equivalent for pebble: build a `*pebble.Options{DisableWAL: true}` (or whatever pebble exposes), drop it into `pebble.Config.Options`, hand the backend to shale.

Design choices:

  - **Pass-through, not enum.** A `cluster.Config.Durability` enum would force shale to translate "what the operator wanted" into "what the backend supports," fragmenting on every backend's quirks. The pass-through keeps the per-backend control surface intact + keeps shale ignorant.
  - **Backend-specific type, not `interface{}`.** Each backend exposes the upstream engine's actual Settings type. Operators get the engine's full knob surface + documentation, plus type-checked field access. The alternative (an opaque `any` blob) loses both.
  - **No shale-level validation.** If the operator wires a Settings combo the engine rejects, the engine's constructor surfaces the error at backend Open time, before shale ever sees the backend. Shale would have nothing useful to add.
  - **No shipping defaults via shale.** Shale's only opinion is "if you pass nil, the backend picks its own default." The backend ships its own defaults independently of shale's release cycle.

The CAVEAT FOR FAST-ACK BACKENDS WITH SHALE REPLICATION above still applies: a backend in a fast-ack mode (whether reached via Settings or otherwise) should pair with `ReplicationFactor >= 2`. Shale notes the recommendation; it doesn't enforce.

---

## Repo layout (multi-module)

shale's repo is a Go **multi-module monorepo** as of v0.5. The core module lives at the repo root; each non-trivial backend is its own module inside `backends/<name>/`. Operators import only the modules they need; CGO + heavy dep weight stay with the backends that actually use them.

### Module boundaries

```
github.com/Zamua/shale                       core module (go.mod at repo root)
  pkg/backend/                               the Backend interface + sentinels (memory)
  pkg/backend/memory/                        in-process map; stays in core for tests + dev
  pkg/cluster/                               public Cluster surface
  pkg/ring/                                  consistent hash ring
  pkg/membership/                            memberlist wrapper
  pkg/rpc/                                   gRPC server/client for inter-node ops
  pkg/rebalance/                             v0.3 handoff protocol
  cmd/shale/                                 the CLI
  cmd/shaled/                                the standalone-node binary (thin shell; see below)

github.com/Zamua/shale/backends/slate        separate module (go.mod inside backends/slate/)
  config.go, slate.go, ...                   pulls in slatedb.io/slatedb-go (cgo)

github.com/Zamua/shale/backends/pebble       separate module (go.mod inside backends/pebble/)
  pebble.go, ...                             pulls in github.com/cockroachdb/pebble
```

### Why split

  - **Dep weight + CGO isolation.** The slate backend transitively pulls slatedb-go (cgo, native shared library, OpenDAL transitive deps). Operators who only want pebble (or memory) shouldn't pay that cost. A user importing `github.com/Zamua/shale/pkg/cluster` + `backends/pebble` resolves zero slate transitives.
  - **Independent versioning.** Backend impls can release on their own cadence. A bug fix in `backends/slate` ships as `backends/slate/v0.5.3` without bumping the core module. Conversely, an unrelated cluster bug fix bumps only the core.
  - **Lower friction for third-party backends.** Anyone can publish `github.com/foo/shale-rocksdb` (or similar) as a single-module repo that imports `github.com/Zamua/shale/pkg/backend` for the interface. The in-repo `backends/*` are reference impls; nothing in the layout privileges them.
  - **Memory stays in core.** The memory backend has zero external deps + is what every cluster-layer test uses; pulling it out would require every internal test target to take a module dependency on itself. The cost / benefit doesn't warrant it.

### `shaled` as a thin shell

Pre-v0.5, `cmd/shaled` hardcoded the backend choice with a `--backend=memory|pebble|slate` flag and switched into one of three constructor files (with `slate` gated behind a `slatedb` build tag). That shape doesn't compose with the multi-module split: shaled-in-core can't import `backends/slate` without dragging slate's deps back into the core module.

The v0.5 shape:

  - **`cmd/shaled` in the core module ships only the memory backend.** It serves the cluster + gRPC stack against an in-process memory store. This is enough for integration tests, smoke tests, and demos.
  - **Backend-specific builds use build tags + a per-backend constructor file**, where the constructor lives in the backend module's own `cmd/shaled-<backend>/` directory (or operators clone shaled + add their own constructor). Building a slate-aware shaled is:

    ```
    cd backends/slate
    CGO_ENABLED=1 go build -tags slatedb -o shaled-slate ./cmd/shaled-slate
    ```

    Equivalent for pebble (no cgo, no tag needed). Each backend module owns its own thin shaled main that wires its constructor into the core's `shaled` run loop (which exports the necessary helpers).
  - **Operators with a custom mix** (e.g. a private backend, or multiple backends in one binary) clone the relevant `cmd/shaled-*` as a template + customize. This is explicit + small (the main is ~30 lines once the wiring helper is factored out).

The core `cmd/shaled` thus becomes the minimal, dependency-free reference; per-backend shaleds are the artifacts operators actually ship. Distributing a single "fat" binary that supports every backend would re-create the dep-weight problem the split was designed to solve.

### `go.work` for development

A repo-root `go.work` file unifies the core module + every `backends/*` module:

```
go 1.25.4

use (
    .
    ./backends/slate
    ./backends/pebble
)
```

With workspace mode active (Go 1.18+), `go test ./...` from the repo root traverses every module; cross-module changes (e.g. a Backend interface tweak) test against in-tree backend impls without a publish + bump cycle. CI runs in workspace mode for the full matrix; release builds disable workspace mode (`GOWORK=off`) so each module's `go.mod` is the source of truth for its own deps.

### Migration path for existing imports

Today: `import "github.com/Zamua/shale/pkg/backend/slate"` and `import "github.com/Zamua/shale/pkg/backend/pebble"`.

v0.5 introduces: `import "github.com/Zamua/shale/backends/slate"` + a separate `require github.com/Zamua/shale/backends/slate v0.5.0` line in the consumer's `go.mod`.

The recommended path is **break + bump**:

  - v0.4.x is the last release with `pkg/backend/{slate,pebble}` import paths.
  - v0.5.0 ships the new layout. The rewrite is mechanical: a single `gofmt -r 'github.com/Zamua/shale/pkg/backend/slate -> github.com/Zamua/shale/backends/slate'` over the consumer's tree, followed by the same for pebble (`gofmt -r 'github.com/Zamua/shale/pkg/backend/pebble -> github.com/Zamua/shale/backends/pebble'`), then `go mod tidy` to pick up the new `require` lines. There is no CHANGELOG file in this repo; the conventional-commit history is the changelog.
  - The old paths are deleted, not deprecated-with-forwarders. The pre-v1 contract says API breaks are allowed at minor versions; forwarders would either (a) pull slate's deps back into the core module (defeating the split) or (b) live as build-tag-gated empty stubs that confuse more than they help.

Operators who can't rewrite immediately stay on the v0.4 line until they're ready. The v0.4 line gets bug fixes for the v0.4.x window only; new development moves to v0.5.

The split happens NOW (the v0.5.x window) rather than later because v0.6's hostthis migration locks in the API; landing the module restructure before v0.6 means consumers do one rewrite, not two.

---

## External backends and the FooDB pattern

Shale's contract assumes one Backend instance per shaled. For **embedded** backends (slate, pebble, memory) this is automatic: each shaled embeds its own `*Slate` / `*Pebble` / memory map; no two shaleds share storage. For **external** backends (Redis, a standalone KV server, a HTTP KV service; call this "FooDB" generically), it requires explicit per-shaled provisioning. Get this wrong and shale's correctness model collapses.

### The pattern (code sketch)

A FooDB-backed Backend is a thin client wrapper. Each shaled holds a client to **its own** FooDB instance, not a shared one:

```
shaled-1  -- backend.Backend --> FooDB instance A   (only shaled-1 reads/writes this)
shaled-2  -- backend.Backend --> FooDB instance B   (only shaled-2 reads/writes this)
shaled-3  -- backend.Backend --> FooDB instance C   (only shaled-3 reads/writes this)
```

Shale's hash ring routes each key to one shaled; that shaled does the Put/Get against ITS FooDB. Replication (R>1) and rebalance (v0.3+) work unchanged: replicated writes fan out to R shaleds, each of which writes to its own FooDB; rebalance streams keys from one shaled-and-its-FooDB pair to another.

The Backend impl:

```go
// backends/foodb (hypothetical third-party module)
package foodb

import (
    "github.com/Zamua/shale/pkg/backend"
    "github.com/example/foodb-client-go"
)

type Config struct {
    Addr     string             // address of THIS shaled's dedicated FooDB
    Settings *foodb.ClientOpts  // pass-through, same pattern as slate/pebble
}

type FooDB struct{ c *foodb.Client }

func New(cfg Config) (*FooDB, error) {
    c, err := foodb.Dial(cfg.Addr, cfg.Settings)
    if err != nil { return nil, err }
    return &FooDB{c: c}, nil
}

func (f *FooDB) Put(k, v []byte) error           { return f.c.Set(k, v) }
func (f *FooDB) Get(k []byte) ([]byte, error)    { /* translate not-found */ }
func (f *FooDB) Delete(k []byte) error           { return f.c.Del(k) }
func (f *FooDB) ScanPrefix(p []byte) (backend.Iterator, error) { /* ... */ }
func (f *FooDB) Begin(level backend.IsolationLevel) (backend.Transaction, error) { /* ... */ }
func (f *FooDB) Close() error                    { return f.c.Close() }
```

Reads + writes pay an extra hop (shaled → FooDB) on top of shale's normal routing. Transactions need a FooDB that exposes the equivalent shape; if the engine doesn't have transactions, the Backend impl can return a "not supported" error on Begin and shale's transaction proxy surfaces that to the caller.

### When this helps, when it doesn't

**Wrong tool: shale in front of a single shared FooDB.** If every shaled connects to the same FooDB instance, shale adds a network hop on every operation with zero shard or replication benefit (the underlying FooDB is the only durability + only point of failure). Just use FooDB directly.

**Wrong tool, dangerously: many shaleds writing to one shared FooDB.** This breaks shale's per-shaled isolation model. Two scenarios fail:

  - **Routing assumes ownership.** Shale's ring says "shaled-2 owns this key." Shaled-2 writes it to the shared FooDB. Shaled-1 (for the same key, after a rebalance) reads it from the same shared FooDB and gets shaled-2's write. So far so good. Now membership changes; shaled-3 becomes the owner. Shale's rebalancer streams the key from shaled-2 to shaled-3 (a copy through both of their Backends), but in the shared-FooDB world both reads + writes land at the same place: the key gets written twice with potentially different envelopes (different LWW stamps from the two shaleds), and the rebalance's per-range write-rejection window doesn't apply because both shaleds are talking to the same store.
  - **Replication writes collide.** Under R>=2, shaled-2 and shaled-3 both write the same key (one as primary, one as replica). If they share a FooDB, the second write overwrites the first; LWW reconciliation cannot run because there's only ONE physical copy.

The rule: **one FooDB instance per shaled. Period.** If you can't provision one FooDB per shaled (whether physical instances, separate logical databases, or keyspace-scoped client credentials that hard-prevent cross-shaled writes), shale isn't the right tool for your FooDB.

**Right tool: future-proofing at N=1.** A common use is "I'm on one FooDB today + I want to scale by adding more later without changing app code." Run shale at N=1 in front of FooDB instance A:

  - Cost: every op pays an extra hop (shaled → FooDB). At N=1 there's no sharding payoff to amortize.
  - Win: when you scale to N=3, you provision FooDB instances B + C, stand up shaled-2 + shaled-3 each pointing at their own FooDB, and the ring rebalances automatically. App code didn't change; the cutover is the same join + rebalance flow shale uses for embedded backends.

Whether the flexibility win is worth the per-op hop depends on the workload. For apps that genuinely expect to scale horizontally and value not rewriting later, it's the right trade. For apps that will never outgrow one FooDB, skip shale and use FooDB directly.

### Reference

A hypothetical `github.com/foo/shale-redis` would be a single-module repo:
  - `go.mod` requires `github.com/Zamua/shale/pkg/backend` for the interface + the redis-go client.
  - One `redis.go` implementing `backend.Backend` against a single Redis instance.
  - A `cmd/shaled-redis/` thin wrapper using the same pattern as `backends/slate/cmd/shaled-slate/`.

The shale repo doesn't ship this; the external-backend story is "publish your own module + import shale's interface." Nothing about a third-party backend is second-class.

---

## Cluster model

### Membership

Each node runs a `memberlist` instance (HashiCorp's SWIM gossip protocol). Nodes discover each other via configured seed addresses. Memberlist handles:
- Heartbeats + failure detection
- Broadcast of node-join / node-leave events
- Node metadata (the node's gRPC listen address)

shale subscribes to membership events; when membership changes, the ring is recomputed + shard ownership shifts trigger rebalancing.

The membership layer's event delegate uses non-blocking sends to its event channel so a slow subscriber can't deadlock memberlist's gossip goroutines. When the channel is full, the event is dropped + `Membership.DropCount()` is incremented. To keep the ring consistent with reality despite drops, the cluster runs a periodic reconciler (~5s) that calls `Membership.Snapshot()` and applies any missed adds / removes.

`Members()` and `Snapshot()` return from an internal cache that the event delegate maintains: every `NotifyJoin` / `NotifyLeave` / `NotifyUpdate` callback updates the cache, and reads consult the cache under a `sync.RWMutex`. Reading directly from `memberlist.Members()` instead would race with memberlist's internal `aliveNode` goroutine, which mutates the `*Node` fields exposed by that call without exposing a per-node lock. The event callbacks are serialized against those internal transitions, so cache writes from inside the callbacks are race-free, and the cache stays consistent with the authoritative state memberlist itself publishes via those same events. Even when the channel drops, the cache update happens before the send attempt, so a dropped notification still leaves the cache (and therefore `Snapshot`) authoritative for the reconciler.

### Routing

The hash ring is **ring-based consistent hashing with bounded loads** (Karger 1997 + Mirrokni-Thorup 2017), implemented via `buraksezer/consistent`. Each node has ~64 virtual replicas on a 64-bit ring; a key hashes onto the ring and goes to the next clockwise virtual node. Adding/removing any node moves ~K/N keys.

For a key K:
1. `node := ring.LocateKey(K)` - local in-memory lookup, sub-microsecond
2. If `node.ID == local.ID` → call local Backend directly
3. Else → gRPC call to that node's shale service

Latency: local ops are local-Backend latency. Remote ops are local-Backend latency + one network RTT.

**Forwarding loop-guard.** Every inter-node Put / Get / Delete / ScanPrefix RPC carries a `forwarded` bool. When a node receives a request whose key it thinks belongs to a different shard, it normally dials the owning node + re-issues the request with `forwarded=true`. If the receiving node ALSO thinks the key belongs somewhere else AND `forwarded` is already true, it returns `FailedPrecondition` instead of re-forwarding. This prevents ping-pong loops during the brief window when two nodes disagree about ownership (e.g. just after a member join / leave, when one node's ring has updated and the other's hasn't yet). Clients see a transient error on that key; the next attempt after both rings converge succeeds.

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

Under the hood, `Aggregate` snapshots each peer's local keyspace via an internal `LocalScan` RPC that bypasses ring routing and reads the receiving node's Backend directly. `LocalScan` is also used by the `stats` handler's `keys_held` counter. It is admin-only + intentionally undocumented on the public surface: apps should use `ScanPrefix` (single-shard, routed) or `Aggregate` (cross-shard, fan-out) instead. It's described here for transparency, not as part of the stable API.

### Replication (v0.4+)

v0.1-v0.3 ship with R=1: each key has exactly one owner, no copies. v0.4 introduces optional replication. The defaults preserve v0.3 behavior (R=1, single-owner) so existing deployments are not silently re-shaped; HA is opt-in via `Config.ReplicationFactor`.

#### Configuration knobs

Three knobs on `Config`, all per-cluster (not per-key):

  - **`ReplicationFactor int`** (default `1`). The number of nodes that hold a copy of each key. With R=1 the cluster behaves exactly as in v0.3: one owner per key, no replicas, no LWW envelope cost paid on the read path. With R>1 each write fans out to R nodes (primary + R-1 successors on the ring).
  - **`WriteConsistency`** (`One | Quorum | All`, default `Quorum`). The number of replica acks required before `Put` / `Delete` returns success. `Quorum = floor(R_live/2) + 1` where `R_live` is the number of replicas actually returned by `ring.LocateKeyN(key, R)` after the live-membership clamp (when the ring has fewer than R distinct members, `LocateKeyN` returns the available subset). The quorum tracks live R, not configured R: a configured R=5 cluster running on 2 live members yields W=2 for Quorum (a majority of live), not W=3 (unreachable, would block forever). `All` requires every live replica to ack; any down replica fails the write. `One` returns as soon as the primary acks and is the loosest setting.
  - **`ReadConsistency`** (`Nearest | Quorum | All`, default `Nearest`). `Nearest` reads with consistency target 1 (the call returns on the first replica's reply); `Quorum` waits for floor(R_live/2)+1 successful responses and returns the LWW winner; `All` waits for every live replica. In every case the dispatch goes to all R replicas so a down primary falls back to the next ring successor (`Quorum` / `All` tolerate up to `R - N` such failures; `Nearest` tolerates `R - 1`). Surplus successful responses past the consistency target seed read-repair feedback on `Quorum` / `All`.
  - **`WriteTimeout duration`** (default `5s`). Per-Put / per-Delete fan-out budget. The originator wraps the fanout context with this timeout so a blackholed peer (hung gRPC, half-open TCP) cancels at deadline instead of leaking goroutines indefinitely. A write that exhausts the timeout without reaching W acks returns `Unavailable`.
  - **`ReadTimeout duration`** (default `5s`). Per-Get fan-out budget. Same shape as `WriteTimeout`: an unreachable peer doesn't block the read indefinitely.

The default pairing (`Quorum` writes, `Nearest` reads) is the v0.4 baseline: writes are durable across a quorum loss, reads are cheap. Operators who want stronger read freshness flip `ReadConsistency` to `Quorum`. Operators willing to tolerate write loss for lower write latency flip `WriteConsistency` to `One`. The two knobs are independent.

#### Value envelope

When R>1, every Backend value is wrapped in an LWW envelope before storage:

```
Envelope {
  Stamp { TimestampNanos uint64; NodeID string }
  Payload []byte
}
```

`Stamp.NodeID` is bounded by `MaxNodeIDLen` (256 bytes) so the envelope's 2-byte length prefix can never silently truncate. `Open` rejects any `Config.NodeID` longer than the bound.

The envelope is opaque to `Backend`: the cluster layer encodes on `Put` and decodes on `Get`, so backend implementations are unchanged. A value written in v0.3 (no envelope) decodes as `Stamp{0, ""}`: it loses every comparison against a stamped write and is re-stamped on the next `Put`. This is the migration path; no offline conversion step is required.

`Delete` writes a tombstone: an empty-payload envelope carrying the current `Stamp`. The tombstone participates in LWW like any other write, so a delete that races with a concurrent write resolves by timestamp rather than by op order. `Get` treats an empty payload as `NotFound`. Tombstone GC is deferred (see "Out of scope" below).

Because the empty-payload shape is reserved for tombstones, `Put(key, nil)` and `Put(key, []byte(""))` are rejected with `ErrEmptyValue` at the cluster surface. Otherwise the same call would silently store a tombstone at R>1 (surfacing as `NotFound` on subsequent reads) while at R=1 it would store an empty payload (surfacing as an empty successful read): the asymmetry is a foot-gun. Apps remove a key by calling `Delete` explicitly.

#### LWW comparator

For two envelopes A and B, A wins iff:

  1. `A.Stamp.TimestampNanos > B.Stamp.TimestampNanos`, OR
  2. timestamps equal AND `A.Stamp.NodeID > B.Stamp.NodeID` (lexicographic).

`TimestampNanos` is `time.Now().UnixNano()` taken by the **originating node** at the moment `Put` is called, before fan-out. Every replica stores the same Stamp for that write; replicas do NOT re-stamp on receipt. This keeps the clock single-sourced per write and avoids skew between the primary's stamp and a successor's stamp for the same `Put`. The NodeID tiebreak resolves the rare case where two originators race with identical nanosecond clocks.

Clock skew between nodes is the standard LWW caveat: a node with a forward-skewed clock can shadow honestly-timestamped writes from healthier peers. Operators run NTP. shale does not synthesize a hybrid logical clock in v0.4; that lands later if real workloads hit the skew limitation.

#### Fan-out + ack accounting

With R>1, `ring.LocateKeyN(key, R)` returns the primary plus R-1 successors on the ring. Put dials all R in parallel:

  - The local node, if it is one of the R, writes via its local Backend.
  - Remote replicas receive a forwarded Put RPC carrying the envelope as-is.

`Put` returns success once W acks have arrived (W per `WriteConsistency`). Acks above W are not waited on but are NOT cancelled: the surplus writes continue in the background so the eventually-consistent state matches the consistency setting's intent. Failures above (R - W) cause `Put` to return error.

#### Read path

`Get` dispatches read RPCs to all R live replicas (so a hung / down primary falls back to a successor without losing the call) and waits for N successful responses per `ReadConsistency` (1 / quorum / R). Each replica returns its local envelope (or `NotFound`). The cluster layer:

  1. Collects responses up to the consistency target.
  2. Picks the LWW winner across the collected envelopes.
  3. Returns the winner's payload (or `NotFound` if the winner is a tombstone or no replica had the key).

Surplus responses past the consistency target keep flowing onto the result channel until every dispatched op reports (or `ReadTimeout` fires), so read-repair below can classify replicas that disagreed with the winner.

On `Quorum` / `All`, if the collected envelopes disagree (any replica returned an older Stamp than the winner, or `NotFound` while a peer has a value), the cluster issues **read-repair**: an async forwarded Put of the winner envelope to every lagging replica. Read-repair is best-effort: errors are swallowed, no retry. Read-repair is **skipped on `Nearest`** because a single-replica read has nothing to compare against; lagging replicas catch up via the next quorum/all read or via future anti-entropy (v0.4.1+).

#### Failure modes

  - **Replica down at Put time**: tolerated up to `R - W` simultaneous failures. With the default R=3 W=2, one replica down still completes writes. With `WriteConsistency=All`, any replica down fails the write. A down replica returns `codes.Unavailable` (real peer-down), which counts against the failure budget so the fanout fails fast once `R - W + 1` such replies land instead of waiting on every remaining peer.
  - **Network partition**: both sides accept writes for keys they can reach a sufficient quorum on. Writes on either side carry the originator's Stamp. On heal, the LWW comparator reconciles deterministically: the higher-timestamp write wins, lower-timestamp writes are lost. This is the AP choice (see "Consistency model"); workloads that cannot tolerate write loss should use a CP system.
  - **All R replicas down**: `Put` and `Get` return `Unavailable`. No way to make progress without at least one replica.
  - **Replica down at Read time**: tolerated up to (R - N) where N is the consistency-target count. `Nearest` with primary down falls back to the next ring successor; `Quorum` / `All` collect from surviving replicas and fail if too few respond.
  - **Replica mid-handoff (rebalance)**: the receiving node returns `codes.ResourceExhausted` (the migration-guard sentinel), distinct from `Unavailable`. The fanout classifies it as transient (skip this replica, wait for another) and does NOT count it against the failure budget. Without that distinction, a single mid-handoff replica during normal v0.3 rebalance would force every otherwise-quorum write to wait for the migration to complete.

#### Out of scope for v0.4

  - **Hinted handoff** (deferred to v0.4.1): when a replica is down at Put time, the coordinator should durably hint the missed write and replay it when the replica returns. v0.4 ships without hints, so a write that lands on (R - W) acks succeeds but the down replicas miss it permanently until a Quorum / All read repairs them or anti-entropy reconciles. The hint protocol is a v0.4.1 follow-up.
  - **Anti-entropy / Merkle-tree repair** (v0.4.1+): a background process that walks ranges + reconciles cold replicas without waiting for a read. Not in v0.4.
  - **Per-key replication factor**: every key in the cluster uses the same R. Per-key overrides are out of scope; operators who need per-prefix replication policy should run separate clusters.
  - **Rebalance-with-replication**: a separate workflow (planned post-v0.4) that integrates the v0.3 rebalance protocol with the v0.4 replication overlay so live writes flow via replication during the bulk copy and the per-range write-rejection window collapses to the ownership swap.

### Rebalancing (v0.3+)

v0.2 ships the gossip ring but does not migrate data: when membership changes, the ring redirects lookups to a node that does not hold the key, so reads against the new owner return not-found and writes land on the wrong shard. v0.3 closes that gap by physically migrating keys when membership changes, so the data state catches up to the ring state.

The design optimizes for an implementation small enough to reason about end to end, on the assumption that shale is embedded in apps run by individual developers and small teams (not Google-scale operators). It accepts brief per-range unavailability (tens of seconds for the streaming copy, milliseconds at the ownership swap) in exchange for: no coordinator election, no per-Backend WAL-tailing requirement, no five-state-machine recovery protocol. v0.4 adds replication (R>1), which is the natural place to recover read availability during migration by serving reads off a replica that already holds the range; trying to engineer that into v0.3 at R=1 buys little and complicates a lot.

R=1 throughout this section. Notes on what changes for R>1 are flagged at the end.

#### Trigger

Migration is driven by membership change, with a settling delay.

When `Membership` reports a join or leave, the node records the event and schedules a `rebalance.Evaluate()` for `T_settle` seconds in the future (default 5s, configurable via `Config.RebalanceSettleDelay`). Any further membership event inside the window resets the timer. When the timer fires, the node snapshots the current ring and proceeds.

The settling delay collapses bursts (rolling restart, several nodes joining within a second, a flapping peer) into one rebalance pass instead of thrashing the cluster through intermediate ring shapes. It also gives the existing membership reconciler (~5s) time to absorb missed events, so by the time `Evaluate` runs, every node tends to have the same view of who is in the cluster.

There is no consensus and no coordinator election. Each node independently decides what to send and what to receive based on its local view of membership. Because memberlist converges in bounded time and each node uses the same ring algorithm against the same member list, peers reach the same migration decisions without negotiation. Disagreement during convergence is handled by the existing forwarding loop-guard plus the per-range cutover below.

An operator can also trigger evaluation explicitly:

  - `shale rebalance --dry-run` prints the plan the local node would execute against the current ring without doing the migration.
  - `shale rebalance --apply` runs `Evaluate` immediately, bypassing the settling delay. Useful for planned topology changes (node decommission) where the operator wants to watch the plan before pulling the trigger.

#### Plan

Each node computes its plan locally from two inputs:

  - the previous ring snapshot (cached from the last evaluation, or empty on first run)
  - the new ring snapshot

The unit of migration is the **key range**, defined as the contiguous arc of the hash ring between two adjacent virtual nodes. `buraksezer/consistent` exposes enough state to enumerate ranges. For each range, the node compares old owner and new owner:

  - old=self, new=peer: schedule **send** to peer
  - old=peer, new=self: schedule **receive** from peer (passive; the source is the one who initiates the stream)
  - old=self, new=self: no-op
  - old=peer1, new=peer2: no-op for this node; the two peers handle it between themselves

Both sides of any (send, receive) pair derive the plan from the same ring inputs, so they agree on who sends what without explicit negotiation. The plan is held in memory; there is no persisted plan object and no plan ID. A crash mid-migration is recovered by the next `Evaluate` run on restart: the node observes the current ring versus what it actually holds and re-issues whatever migrations are still needed. This recovery path is the same code path as the steady-state one.

#### Wire

A single server-streaming gRPC method:

```
rpc MigrateRange(RangeSpec) returns (stream MigrateChunk)
```

`RangeSpec` identifies the arc of the ring being transferred (start + end hash values, plus a `ring_generation` freshness field). `MigrateChunk` carries either a `(key, value)` pair or a terminal marker with the count + checksum of all keys sent.

`ring_generation` is the destination's per-node monotonic ring-change counter at the moment it opened the stream. v0.3 carries this on the wire for future use, but the counter is NOT cluster-wide (each node bumps its own on every NotifyJoin/NotifyLeave + a node that joined later naturally has a lower count than a longer-lived peer), so the source CANNOT meaningfully compare the destination's value against its own. A strict less-than rejection spuriously cancels legitimate streams during normal join/leave races. v0.3 therefore accepts every stream regardless of the destination's reported generation; wrong-owner protection comes from the forwarding loop-guard + the per-key Put/Get migration guards instead. A cluster-wide ring generation (and the strict freshness check the field was originally designed for) lands when the gossip layer carries one in v0.4 or later.

The destination initiates the call (it is the node whose ring says "I am the new owner"). The source iterates its local Backend over keys whose hashed shard key falls in the range, streams `(key, value)` pairs, then closes with the terminal marker. The destination writes each pair to its local Backend as it arrives. Backpressure is gRPC flow control; no per-key acknowledgement is needed.

One range per RPC. Multiple ranges between the same source and destination run sequentially to keep the resource footprint predictable and the per-peer protocol single-threaded.

If the source's Backend does not support a range-bounded scan, it falls back to a full `ScanPrefix("")` with a range filter applied per key. This is wasteful but correct; backend authors are encouraged to implement a faster path.

#### Cutover

Each range has a lifecycle:

1. **Pre-migration**: source owns the range. Source serves reads and writes. Destination's ring already lists destination as the new owner, but `MigrateRange` has not started or is queued behind earlier ranges.
2. **Migrating**: destination opens `MigrateRange`. Source marks the range as migrating-out, destination marks it as migrating-in. While in this state:
   - **Reads** for keys in the range are served by the **source**. The forwarding loop-guard ensures that a read landing on the destination via stale ring is forwarded back to the source.
   - **Writes** for keys in the range are **rejected by the source** with a transient error (`ResourceExhausted` with retry hint). The destination, if it receives a write via the new ring, also rejects with `ResourceExhausted`. Clients retry with backoff; the SDK's client wrapper handles this transparently with a bounded retry budget. `ResourceExhausted` (not `Unavailable`) is the chosen code so the v0.4 replication fanout's failure budget can distinguish "this replica is mid-handoff, try another" from "this replica is dead, count it as failure"; conflating the two would force the fanout to wait on every peer instead of failing fast on real peer-down.
   - This is the chosen cutover semantics. Rejecting writes during the streaming copy is what avoids the "did this write make it across the cutover" problem without a WAL-tailing catch-up phase. The write-unavailability window per range is bounded by streaming time for that range's data.
3. **Swap**: source completes the stream by sending the terminal `MigrationDone{total_keys, checksum}` chunk. The destination validates the checksum against its own running CRC32 and ack's implicitly by reading + draining the stream to a clean EOF (the gRPC stream return value IS the ack; there is no separate `MigrateAck` message). The source's `MigrateRange` handler returns successfully only after the destination has consumed `MigrationDone`. The coordinator observes that successful return and flips ownership state for the range from "owner" to "former owner, forwards reads". This flip is atomic per range.
4. **Post-migration**: destination is authoritative for reads and writes. Source forwards any straggler read it receives for the range to the destination via the standard forwarding path. Once the source observes that all peers' rings agree the destination owns the range (or after a grace period of `T_drain`, default 30s), source deletes its local copy of the range's keys.

The write-rejection window is the price paid for avoiding WAL-tailing. At hostthis-scale workloads (tens of thousands of keys per range, small values), the streaming copy completes in well under a minute on a local network; clients retrying with exponential backoff over that window do not see user-visible errors.

#### Failure handling

  - **Source crashes mid-stream**: destination's `MigrateRange` call returns an error. Destination discards what it received (the partial range is not authoritative). On the next `Evaluate` pass, if the source comes back, migration retries. If the source does not come back, the range is owned by no one at R=1; reads return not-found, writes are rejected. This is the unavoidable R=1 failure mode and is the central motivation for v0.4 replication.
  - **Destination crashes mid-stream**: source's stream returns an error. Source remains the owner. On next `Evaluate`, migration retries to whichever node the new ring assigns.
  - **Checksum mismatch on `MigrationDone`**: destination errors its stream (does not drain it cleanly), the source's handler returns that error rather than success, and the coordinator does NOT flip state. Destination rolls back its writes for the range; source remains owner; migration retries on the next `Evaluate`. Checksum is computed over the sorted `(key, value)` byte stream.
  - **Ring divergence during cutover**: handled by the existing loop-guard. A read or write landing on the wrong node gets one forward; if the forwarded-to node also disagrees, the request returns `FailedPrecondition` and the client retries after the rings converge.
  - **Operator cancellation**: `shale rebalance --cancel` aborts in-progress streams. Source remains owner of any range not yet swapped; destination discards any partial state.

#### Observability

  - `shale topology` shows each range's state (`stable | migrating-out | migrating-in | draining`) and, for in-flight migrations, the source, destination, bytes streamed so far, and elapsed time.
  - Per-node counters: `rebalance_ranges_migrated_total`, `rebalance_bytes_streamed_total`, `rebalance_writes_rejected_total`, `rebalance_failures_total`.
  - Structured log line per range transition with `range_id`, `from`, `to`, `key_count`, `bytes`, `duration_ms`.

#### What v0.4 changes

With R>1, the read-unavailability problem largely disappears: reads can be served from any replica, including ones not currently sending or receiving. The migration unit becomes a (range, replica-position) pair rather than just a range. The write-rejection window can shrink to the duration of the ownership swap rather than the entire streaming copy, because the destination can receive writes via the replication path while the bulk copy is in flight and the conflict resolver (LWW, v0.4) reconciles. The wire protocol stays the same shape; the cutover protocol gains a "live writes also flow via replication" overlay.

The v0.3 design does not preclude any of this. It deliberately leaves the per-range cutover as the only place where v0.4 needs to intervene.

#### Known limitations in v0.3

  - **Write-unavailability window per migrating range** is proportional to the size of that range. A range with millions of small keys can stall writes for tens of seconds. Operators with hot ranges should plan topology changes during low-traffic windows or wait for v0.4.
  - **No live progress streaming**: the source streams to one destination; if the operator wants finer-grained progress than the per-range chunk counter, they can watch the gRPC stream metrics directly, but there is no built-in real-time per-key progress feed.
  - **Backend scan efficiency varies**: backends without range-bounded scans pay an O(total keys) cost per migrating range. The memory backend is fine; SlateDB's range scan is efficient; future backend authors should implement bounded range scans for production workloads.
  - **No throttling of concurrent migrations**: if many ranges change owners at once (e.g. a 3-node cluster grows to 6 nodes), every node opens its `MigrateRange` calls in parallel against its peers. Object-store IO is the practical limiter today; an explicit per-node concurrency cap is a v0.3.x follow-up if hot-spotting shows up in practice.

---

## Consistency model

shale is **per-key linearizable**. Each key has exactly one owner at a time; all ops for that key serialize through that owner's local Backend. Cross-key operations are NOT atomic (no distributed transactions).

Multi-key atomicity within ONE shard is provided by **optimistic concurrency control (OCC)**: a transaction reads through normal routed Gets, computes locally, and commits a read-set + write-set in a single owner-local validate-and-apply step. The owner re-checks the read-set against current state inside one short `backend.Transaction` and applies the write-set only if nothing it read changed; otherwise it reports a conflict and the client retries. This is the same compare-and-set / conditional-write model DynamoDB, etcd, and FoundationDB use, and it fits the object-store grain of the default backend.

If a transaction touches keys owned by multiple nodes, shale returns `backend.ErrCrossShard`. App code is expected to use a single shard-key prefix (or hash tag) for related keys so they co-locate on one shard (the standard sharded-KV pattern).

### Single-shard transactions (CAS / OCC)

A `Cluster.Begin` transaction is **single-shard by construction** and runs **optimistically**: it never holds an open `backend.Transaction` across a network round-trip. The transaction is lazy: `Begin` returns a `clusterTx` that has not yet touched a backend. The shard is **pinned** on the first key the caller operates on (the first `Get` / `Put` / `Delete`), to whichever node the ring says owns that key's shard. Every subsequent key in the same transaction MUST shard to that same owner; a key that shards elsewhere returns `backend.ErrCrossShard` at the offending operation (not deferred to Commit), so the caller sees the limitation at the call site.

The cross-shard guard is the load-bearing correctness property and it stays **client-side and unchanged**: a genuinely cross-shard transaction (two keys whose shards have different owners) STILL fails with `ErrCrossShard`, and that check fires on the client *before* anything goes on the wire. shale does NOT gain cross-shard transactions; it gains atomic multi-key transactions against an already-single-shard key-set, and runs them whether that shard is local or remote with ONE uniform model.

#### Why CAS, not an interactive proxy

An interactive remote transaction (open a `backend.Transaction` on the owner and round-trip each op to it over a long-lived stream) holds a backend writer OPEN on the owner across every client think-time gap. On a backend like slate that is an open object-store writer pinned for the whole client-side transaction; it is the throughput anti-pattern scalable systems avoid. CAS keeps the owner's transaction tiny and owner-local: the client reads via normal Gets, computes locally with no writer held anywhere, then ships a read-set + write-set in ONE unary call. The owner validates and applies inside a single short local transaction that opens and commits without any network round-trip inside it. There is no transaction open across the network, so the disconnect / leak-guard complexity of an interactive proxy mostly evaporates: a cancelled `CommitCAS` context just rolls back the owner's local transaction via `defer`, and a client that thinks for an hour holds nothing on the owner.

#### The CAS-buffered transaction (client side)

`clusterTx` is a buffer, not a live session:

  - **`Get(key)`** does a real routed `Get` (local-or-remote, the normal single-key read path) and records `(key, value-seen)` in the **read-set**. A not-found records an `expect_absent` entry for that key. A Get whose key shards to a different owner than the pinned key returns `backend.ErrCrossShard` immediately, before recording anything.
  - **`Put(key, value)` / `Delete(key)`** BUFFER into the **write-set** (an ordered list of write ops); they do NOT hit the owner yet. They enforce the same cross-shard guard before buffering.
  - **Read-your-writes within the transaction** is served from the local write buffer: a `Get` after a `Put`/`Delete` of the same key in the same transaction returns the buffered value (or buffered-absence) without a round-trip, and does NOT add a read-check for that key (the client wrote it; validating it against pre-write state would be wrong). Only keys the transaction READ from the cluster (and did not itself write) become read-checks.
  - **`Commit()`** sends ONE `CommitCAS` to the pinned shard owner. If the owner is this node, it is a **local fast-path**: the owner-side handler runs in-process with no RPC. On a reported conflict, `Commit` returns the sentinel `backend.ErrCASConflict`. On a backend / ownership failure, `Commit` returns that error. On success, `Commit` returns nil.
  - **`Rollback()` / abandoning the tx** is purely local: nothing was sent to the owner, so there is nothing to undo. It just marks the buffer finalized.

Because the buffer captures the exact value each read observed, the read-set is a precise record of the snapshot the client computed against. `IsolationLevel` is carried to the owner so its local validate-and-apply transaction opens at the requested level; the OCC read-set check is what provides the cross-key atomicity regardless.

#### CommitCAS wire protocol

A new unary RPC on `ShaleNode`:

```
rpc CommitCAS(CommitCASRequest) returns (CommitCASResponse);
```

```
message CommitCASRequest {
  bytes pin_key         = 1;  // the key that pinned the shard; owner verifies it owns this
  int32 isolation_level = 2;  // mirrors backend.IsolationLevel (0=SnapshotIsolation, 1=SerializableSnapshot)
  repeated ReadCheck reads  = 3;
  repeated WriteOp   writes = 4;
}

message ReadCheck {
  bytes key            = 1;
  bytes expected_value = 2;  // the value the client observed for key
  bool  expect_absent  = 3;  // true => key must NOT exist; expected_value ignored
}

message WriteOp {
  bytes key   = 1;
  bytes value = 2;  // ignored when delete=true
  bool  delete = 3; // true => Delete(key); false => Put(key, value)
}

message CommitCASResponse {
  bool   committed = 1;  // true => the write-set was applied + committed
  bool   conflict  = 2;  // true => a read-check failed; client should retry
  string error     = 3;  // non-empty => a backend / ownership failure (NOT a conflict)
}
```

`committed`, `conflict`, and `error` are mutually exclusive outcomes: exactly one of "committed=true", "conflict=true", or "error set" is meaningful per response. A `conflict` is NOT an `error`: it is the expected OCC retry signal, so it travels as a typed boolean rather than a gRPC error code (the same not-found-is-not-an-error convention `GetResponse` already uses).

#### Owner-side validate-and-apply (the heart of it)

The `CommitCAS` handler is fully synchronous and owner-local. There is no open transaction across the network at any point:

  1. **Ownership check.** Verify the local node owns `pin_key` via `Cluster.OwnsKey`. If not (the ring moved and this node is no longer the owner), return `error` (a NotOwner condition) WITHOUT opening a backend transaction, so the client re-pins against the new ring and retries. It does NOT apply anything to the wrong backend.
  2. **Open ONE local transaction** via `Cluster.LocalBegin(isolation_level)` (the owner-local `backend.Begin`). A `defer tx.Rollback()` is armed immediately, guarded by a `committed` flag so a successful commit does not double-finalize.
  3. **Validate the read-set.** For each `ReadCheck`, `Get` the key inside the transaction:
       - `expect_absent=true`: a found value is a CONFLICT.
       - otherwise: a not-found OR a value that does not match `expected_value` byte-for-byte is a CONFLICT.
     On the first conflict, the handler stops, the deferred `Rollback` runs, and it returns `{conflict:true}`.
  4. **Apply the write-set.** For each `WriteOp` in order: `delete=true` -> `tx.Delete(key)`; else `tx.Put(key, value)`. A backend error here returns `{error}` (the deferred rollback runs).
  5. **Commit.** `tx.Commit()`. On a commit error, the handler does NOT set `committed` (the deferred `Rollback` runs) and returns `{error}`: same leak-guard discipline a failed commit demands, because a backend's failed `Commit` is not guaranteed to have finalized the transaction. On success, it sets `committed` (suppressing the deferred rollback) and returns `{committed:true}`.

A cancelled RPC context (client disconnect, deadline) propagates to the local transaction; the deferred `Rollback` runs and the transaction did not happen (all-or-nothing holds). Within a single commit the whole sequence is one in-handler goroutine against one `backend.Transaction` (which is not goroutine-safe), so there is nothing to coordinate inside it.

**Serializing concurrent commits.** Across commits the owner serializes the entire validate-and-apply (read-set check through `Commit`) under a per-node lock. This is required for OCC correctness on a backend whose `Commit` does not itself detect write-write conflicts: two commits that both read `counter == 5` and both write `6` would otherwise both succeed and lose an update. The lock is held only for the owner-local critical section (no network inside it), so it is cheap; a future refinement can stripe it per shard rather than per node. Backends that DO provide write-write conflict detection (slate's snapshot isolation) make the lock belt-and-suspenders rather than load-bearing, but holding it keeps the guarantee uniform across backends.

**OCC serializes only against other CAS commits.** The read-set check protects a transaction against concurrent *conditional* writes (other `CommitCAS` calls), not against an unconditional `Cluster.Put` / `Cluster.Delete` on the same key. A plain `Put` carries no read-set and does not take the commit lock, so a plain write racing a CAS commit on the same key is last-writer-wins and is outside OCC's guarantee. This matches every OCC system (DynamoDB conditional writes, etcd txn): mixing unconditional and conditional writes on the same key is the caller's responsibility. If a key is governed by OCC invariants (e.g. a quota counter), every writer of that key should go through `Transact` / a CAS commit, never a bare `Put`.

**Ownership is checked once, on the pin key, not per read-check key.** The cross-shard guard already ran on the client, so every read-check and write-op key shards to the same owner as `pin_key`; re-checking each would only reintroduce torn-state failure modes during a ring move with no benefit.

#### The retry closure (primary consumer API)

The ergonomic surface is a retry closure that hides the conflict loop:

```go
func (c *Cluster) Transact(pinKey []byte, fn func(tx backend.Transaction) error) error
```

`Transact` opens a CAS-buffered transaction pinned to `pinKey`, runs `fn` (which issues Gets + buffered Puts/Deletes against `tx`), commits via `CommitCAS`, and on `backend.ErrCASConflict` RE-RUNS `fn` from scratch (fresh reads, fresh buffer) up to a bounded number of attempts (`CASMaxAttempts`, default 10) with a small randomized backoff between attempts. If it never converges within the budget it returns `backend.ErrCASConflict` (the exhausted-retries case reuses the same sentinel; the caller cannot make progress either way). A NON-conflict error from `fn` or from `Commit` aborts immediately with that error and is NOT retried.

`fn` MUST be re-runnable and side-effect-free outside `tx`: `Transact` may invoke it multiple times. Mutating external state (incrementing a process-local counter, sending a notification) inside `fn` is a bug, because a conflict re-runs the whole closure. The canonical pattern is "read current value(s) via `tx`, compute the new value(s) purely, buffer the writes, return nil"; the retry then re-reads the now-changed value and recomputes.

`pinKey` should be a key in (or sharding to the same shard as) the transaction's key-set; it fixes the shard the OCC commit validates against. The first `tx.Get`/`tx.Put` would pin the same shard anyway, so passing the natural anchor key (e.g. the counter the transaction updates) is the convention.

#### Begin vs Transact

`Cluster.Begin(level) (backend.Transaction, error)` SURVIVES, re-shaped to CAS-buffered OCC semantics. It is the lower-level surface for callers who want explicit control of the commit point or who are migrating existing `Begin`/`Commit` code; `Transact` is the recommended primary API because almost every OCC caller wants the conflict-retry loop and writing it by hand is error-prone. There is now ONE transaction model, not an interactive-local / CAS-remote split: a single-node / local-pin commit goes through the SAME `CommitCAS` validate-and-apply path (just in-process, no RPC), so local and remote transactions have identical semantics. This is a behavior change to `Begin`'s pre-v0.6 contract (it used to run ops against a live local `backend.Transaction` and return `ErrCrossShard` on a remote pin). Changing it pre-1.0 is acceptable and is documented here; the payoff is one uniform OCC model everywhere.

`tx.Commit()` returning `backend.ErrCASConflict` is the signal a hand-rolled `Begin` caller checks to decide whether to retry. `tx.ScanPrefix` inside a CAS-buffered transaction is NOT supported in v0.6 (a scanned range cannot be cheaply turned into a read-set of discrete key checks, and validating a range against concurrent inserts needs phantom protection the value-based read-set does not provide); it returns an error directing the caller to scan outside the transaction. Single-key reads + writes are the supported transactional surface.

#### ABA caveat (value-based read-checks)

Read-checks compare **value bytes**, not a version number or sequence number. This is vulnerable to ABA: if a key goes X -> Y -> X between the client's read and the owner's validate, the value still matches `expected_value` and validation passes even though the key changed and changed back. shale accepts this deliberately:

  - **It is benign for shale's target usage** because the values fully capture state. A reservation counter that returns to `5` means a net-zero change, so "5 + my delta" is still the correct result. A slug that was created then deleted is absent again, so a "create if absent" is still valid against the observed-absent read-check. The transaction's correctness depends on the *current value*, which ABA preserves, not on *whether the value was ever touched*.
  - **Value-based is simpler**: no per-key version counter to store, increment, and replicate; the read-set is just the bytes the client already read.

If a future consumer needs strict ABA-safety (a use case where "was this touched at all" matters, independent of the final value), the upgrade path is a version/seqno-based `ReadCheck` (compare a monotonic per-key version instead of, or alongside, the value bytes). That is a forward option, not a v0.6 deliverable; v0.6 ships value-based.

#### Replication scope (v0.6 vs v0.6.x)

Single-key `Put` already replicates to R replicas when `ReplicationFactor > 1` (primary + R-1 successors, ack-counted per `WriteConsistency`). The OCC commit does NOT yet replicate: a committed write-set lands only on the **owner's local Backend**.

**Decision for v0.6: the OCC commit executes at R=1 on the owner.** The validate-and-apply path is the foundational primitive; it is cleanly testable on its own (validation, conflict detection, read-your-writes, cross-shard guard, retry loop) without dragging in fan-out / ack accounting / LWW reconciliation. Write-set replication composes cleanly ON TOP and does not change the `CommitCAS` protocol: it is a separate additive step after the owner's `tx.Commit()` succeeds.

**Known gap and its closure in v0.6.x.** With a fast-ack Backend (slate `AwaitDurable=false`, the hostthis default) and the R=1 commit path, a committed write-set sits in the owner's loss window with no replica copy until v0.6.x. Operators running transactions against a fast-ack backend should know that, unlike single-key Puts, transactional writes are not yet replicated in v0.6. The closure:

  - **`ApplyBatch` unary RPC.** After the owner's `tx.Commit()` returns nil, the owner replicates the **committed write-set** (the exact ordered puts + delete tombstones from that transaction, nothing re-validated) to the shard's R-1 other replicas via `ApplyBatch`. Each replica applies the whole batch atomically through its OWN local `backend.Transaction` (begin, apply all entries, commit), so a replica holds either the entire write-set or none of it.
  - **No re-validation on replicas.** The owner already validated the read-set; replicas just apply. This is what makes the write-set "exactly what gets replicated" and composes more cleanly than an interactive proxy would.
  - **Ack accounting reuses `WriteConsistency`.** `ApplyBatch` fan-out waits for W acks per `One | Quorum | All`, mirroring single-key Put accounting, with the same failure budget and migration-guard handling (a mid-handoff replica is transient, not a failure).
  - **Stamping.** Each entry carries an LWW envelope stamped by the owner at commit time (single-sourced; replicas do not re-stamp), so the batch reconciles against single-key writes under the existing LWW comparator.

This is **specified now, implemented in v0.6.x.** v0.6 ships the OCC commit with full tests; v0.6.x adds `ApplyBatch` write-set replication so transactional writes get the same R-replica durability single-key writes already have.

---

## Failure handling

- **Single node crashes**: keys it owned are temporarily unavailable at R=1. With R>1 (v0.4+), replicas take over reads + writes up to (R - W) / (R - N) tolerated failures per the configured consistency.
- **Network partition**: nodes on each side see the other as failed. Both sides accept writes. On heal, conflicts resolve via Last-Write-Wins (LWW) using the originator's wall-clock timestamp + nodeID tiebreak (see "Replication (v0.4+)" for the full envelope + comparator). R=1 has no replication conflicts; R>1 relies on LWW.
- **Backend failure on one node**: that node reports unhealthy to memberlist; gets removed from ring; data unavailable until restored (or served from replicas under R>1).

---

## Non-goals

- **SQL queries** - Vitess (MySQL) / Citus (Postgres) do this well; shale stays out of that domain.
- **Cross-shard transactions** - requires consensus (Paxos/Raft); out of scope. Apps that need this use FoundationDB / TiKV. (Single-shard transactions DO work, including when the shard lives on a remote node, via the CAS / OCC commit: see "Single-shard transactions (CAS / OCC)". What stays out of scope is a transaction spanning more than one shard, plus any 2PC, timestamp oracle, or external coordinator.)
- **Interactive open-across-the-network transactions** - shale does NOT hold a backend writer open on the owner across client think-time. Multi-key atomicity is OCC (buffer locally, validate-and-apply in one owner-local commit), not a long-lived remote transaction session.
- **Strong consistency across partitions** - shale chooses AP over CP (eventual consistency on partition heal). Need CP? Use Raft-based stores.
- **Multi-region replication** - out of scope for v1; possibly v2.
- **Backend-specific features** (Redis pub/sub, Postgres extensions, etc.) - the abstraction stays minimal.

---

## CLI + standalone binary

shale ships two binaries from v0.1, separate from the library import path. Their purpose is dev-cycle ergonomics: poking at a running cluster from the shell, spinning up nodes for integration tests, inspecting topology + state during debugging.

### `shaled` (standalone node)

Runs a shale node as its own process, without an app embedding it. Useful for:
  - Integration tests (shell scripts spin up N nodes on ephemeral ports)
  - Local multi-node dev clusters
  - Operators who want a managed shale process per host (instead of embedding inside their app)

As of v0.5, shaled is a thin shell + the backend choice is the binary you build, not a flag (see "Repo layout / `shaled` as a thin shell" above). The core `cmd/shaled` ships memory-only. Per-backend builds live in each backend module:

```
shaled                      # core build: memory backend, no external deps
shaled-pebble               # built from backends/pebble/cmd/shaled-pebble/
shaled-slate                # built from backends/slate/cmd/shaled-slate/ with -tags slatedb

# Memory shaled (the default; used by integration tests):
shaled --node-id node-1 --bind-addr :7946 --grpc-addr :7947 --seeds node-2:7946

# Pebble shaled (durable, pure Go):
shaled-pebble --node-id node-1 --bind-addr :7946 --grpc-addr :7947 \
  --seeds node-2:7946 --pebble-dir /var/lib/shale/node-1

# Slate shaled (S3-compatible object storage; cgo build):
shaled-slate --node-id node-1 --bind-addr :7946 --grpc-addr :7947 \
  --seeds node-2:7946 \
  --slate-bucket my-bucket --slate-db-name node-1 \
  --slate-endpoint http://minio:9000 \
  --slate-access-key X --slate-secret-key Y
```

shaled exposes the same gRPC service the inter-node forwarding layer uses. The CLI talks to it via that gRPC.

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

shaled-slate --node-id n1 --grpc-addr :7947 --bind-addr :7946 \
       --slate-bucket shale-test --slate-db-name node-1 \
       --slate-endpoint http://localhost:9000 ...

shaled-slate --node-id n2 --grpc-addr :7949 --bind-addr :7948 --seeds 127.0.0.1:7946 \
       --slate-bucket shale-test --slate-db-name node-2 ...

shaled-slate --node-id n3 --grpc-addr :7951 --bind-addr :7950 --seeds 127.0.0.1:7946 \
       --slate-bucket shale-test --slate-db-name node-3 ...

# inspect + load test
shale topology --addr 127.0.0.1:7947   # all 3 nodes + ring assignments
shale bench --addr 127.0.0.1:7947 --writes 100k --keys-prefix bench:
```

`shale bench` (v0.5+) reports aggregate throughput, per-node request distribution, p50/p99 latencies.

### Comparative benchmark harness (v0.5+)

`shale bench` measures one running cluster at the operator's chosen R + W/R consistency. The separate `cmd/shale-bench` harness (driven by `make bench-v0.5` -> `scripts/run-bench.sh`) answers the comparison question:

> "What is shale's overhead vs the raw backend, and what does R=3 cost vs R=1?"

It spins up every scenario in one process via the same in-process pattern as `tests/integration/` (loopback memberlist + ephemeral-port gRPC), drives an identical workload through `putGetter` adapters that wrap either a bare `backend.Backend` or a `*cluster.Cluster`, and emits one markdown table. Scenarios:

  - `raw-pebble` / `raw-memory` - baseline; no shale layer
  - `cluster-*-n1-r1` - shale overhead at 1 node, R=1 (cluster code path, no gRPC hop)
  - `cluster-*-n3-r1` - sharding cost (3 nodes, one fan-out per Put)
  - `cluster-*-n3-r3` - replication cost (3 nodes, R=3, WriteQuorum + ReadQuorum)

Output lives in `docs/BENCH-v0.5.md`. Numbers are machine-specific by design; the canonical use is "operator runs the suite on their target hardware before vs after a change to spot regressions."

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

### SlateDB backend end-to-end coverage

`backends/slate` ships two test layers (run from inside that module, or via `go test` in workspace mode from the repo root):

  - **Default** (`slatedb` build tag): drives the binding against an in-process `memory:///` object store. Fast, no Docker, no S3.
  - **End-to-end** (`slatedb integration` build tags): spins up a real MinIO container via testcontainers-go, creates a fresh bucket, and runs the Slate type against it. Covers 1k small keys, 10x 1MB blobs, durability across writer reopen, and the writer-epoch fencing guarantee (two writers against the same DB → second fences the first). Operator entry point: `make test-slate-minio`. v0.6 (the hostthis migration) gates on this passing against the deployment's chosen object store.

Default `go test ./...` skips both layers (no cgo, no Docker), so the regular dev loop stays fast.

---

## Roadmap

- [ ] **v0.1** - single-node Cluster wrapping one Backend; memory backend impl; SlateDB backend impl; gRPC service (used by CLI + ready for v0.2 inter-node); `shaled` standalone binary; `shale` CLI with put/get/delete/scan/topology/stats/ping. API lockup.
- [ ] **v0.2** - multi-node + memberlist + hash ring + gRPC forwarding. Static topology (no rebalance). `shale topology` now shows real membership + ring.
- [ ] **v0.3** - rebalancing on join/leave. Atomic ownership swap. `shale rebalance` + `shale migrate-from` subcommands.
- [~] **v0.4** (in progress) - replication factor R + tunable consistency. LWW conflict resolution. Sub-tasks:
  - [ ] LWW value envelope (Stamp + Payload), encoded on Put / decoded on Get, transparent to Backend
  - [ ] v0.3-value compatibility: bare values decode as `Stamp{0, ""}`, re-stamped on next Put
  - [ ] `Config.ReplicationFactor` (default 1) + `ring.LocateKeyN(key, R)`
  - [ ] `Config.WriteConsistency` (One / Quorum / All, default Quorum) with parallel fan-out + ack accounting
  - [ ] `Config.ReadConsistency` (Nearest / Quorum / All, default Nearest)
  - [ ] LWW comparator (higher TimestampNanos wins, NodeID lex tiebreak; stamped by originator pre-fan-out)
  - [ ] Async read-repair on Quorum / All when replicas disagree (skipped on Nearest; best-effort, errors swallowed)
  - [ ] Delete as empty-payload tombstone envelope; Get treats empty payload as NotFound
  - [ ] Failure handling: tolerate down replicas up to (R - W) / (R - N); Unavailable when all R down
- [ ] **v0.4.1** - hinted handoff for replicas down at Put time; foundations for anti-entropy.
- [ ] **v0.5** - Prometheus metrics, tracing hooks, real benchmarks vs single-node SlateDB. `shale bench` subcommand.
- [ ] **v0.5.x** - multi-module monorepo split: `backends/slate` + `backends/pebble` move out of `pkg/backend/` into their own Go modules; backend-specific `Settings` pass-through (`Config.Settings *engine.Settings` on each backend); `cmd/shaled` becomes a thin core shell with per-backend builds living alongside each backend module. Import-path break-and-bump (no deprecation forwarders). Done before v0.6 stabilizes the API.
- [~] **v0.6** (in progress) - single-shard transactions via CAS / optimistic concurrency, then the hostthis migration. Sub-tasks:
  - [ ] `CommitCAS` unary RPC (ReadCheck with expect_absent + value-byte compare; WriteOp with delete flag) on `ShaleNode`
  - [ ] Owner-side validate-and-apply handler: ownership check on pin_key, ONE `LocalBegin` transaction, read-set validation -> conflict, write-set apply, commit with failed-commit-rolls-back leak guard
  - [ ] CAS-buffered `clusterTx`: recorded routed Gets (not-found -> expect_absent), buffered Puts/Deletes, read-your-writes from the buffer, cross-shard guard before buffering, local fast-path commit when owner is this node
  - [ ] `backend.ErrCASConflict` sentinel; `Commit` returns it on a reported conflict
  - [ ] `Cluster.Transact(pinKey, fn)` retry closure: re-run fn on conflict up to `CASMaxAttempts` (default 10) with backoff; non-conflict errors abort immediately
  - [ ] `Begin` re-shaped to uniform OCC (one transaction model local + remote; pre-1.0 contract change documented)
  - [ ] ABA caveat documented (value-based read-checks; benign for shale usage); version/seqno read-checks noted as the future option
  - [ ] hostthis migration: swap raw SlateDB for shale-with-SlateDB-backend. Default config: `slate.Config{WriteOptions: &slatedb.WriteOptions{AwaitDurable: false}}` paired with `cluster.Config{ReplicationFactor: 3}`. The v0.5 bench measured this at 11,661 puts/s on the cluster-n3-r3 production shape, ~150x the strict baseline of 77 puts/s, with the durability budget covered by 3-way replication during the ~100ms per-replica WAL-flush window. Validate on production-like data.
- [ ] **v0.6.x** - `ApplyBatch` unary RPC: replicate the committed OCC write-set to R-1 replicas (apply-only, no re-validation), ack-counted per `WriteConsistency`, owner-stamped LWW envelopes. Closes the R=1 transactional-write durability gap left by v0.6.

Each version ships independently; users can adopt v0.1 today (functionally equivalent to using their Backend directly, plus the CLI for daily ergonomics) and grow into v0.2+ when their workload demands it.

---

## Inspirations

- **Olric** - same memberlist + consistent-hash pattern. In-memory only; we generalize to durable backends.
- **Vasto** - sharded RocksDB with jump consistent hash + single-master topology. Dormant; architecture is sound and we borrow heavily. We diverge by using ring-based hashing + gossip membership instead.
- **Cassandra / ScyllaDB** - shard-per-core + gossip + ring topology. Heavyweight services; we want library shape with the same model.
- **DynamoDB** - the access-pattern discipline. DDB forces you to commit to a partition key at table-creation time, which drives the rest of your data model. shale relaxes this to per-key hash tags, but the underlying principle (think about access patterns before key design) is the same.
- **Redis Cluster** - the hash-tag convention (`{tag}`-based key co-location) is borrowed directly.
- **SlateDB** - the default backend. Provides KV semantics on cheap object storage; we provide horizontal scale-out around it.
