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

    // Cache, if non-nil, is the slatedb SST block + metadata cache handed
    // to the DbBuilder via WithDbCache. Nil = slatedb default: NO block
    // cache, so every read re-fetches SST blocks from the object store. On
    // an object-store backend that is a steady self-inflicted read storm
    // (the same hot SSTs fetched repeatedly), so any latency-sensitive
    // deployment should pass a cache. Build one with
    // slatedb.DbCacheNewMokaCache (in-memory) or DbCacheNewFoyerCache
    // (memory + local-disk); WithDbCache clones the Arc so ONE cache may be
    // shared across a node's backends. Operator owns the handle's lifecycle,
    // same as Settings / WriteOptions.
    Cache *slatedb.DbCache
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
- Node metadata (the node's gRPC listen address AND a `Draining` flag - see below)

shale subscribes to membership events; when membership changes, the ring is recomputed + shard ownership shifts trigger rebalancing.

**Node metadata: address + a `Draining` bit.** Each node publishes a small `Meta` payload over gossip. It carries the node's gRPC dial address (so peers can route writes/reads to it) AND a boolean `Draining` flag used by the graceful-leave (scale-down) path (see "v0.8 Phase 2e: Graceful leave"). The `Member` value object the membership layer returns from `Members()` / `Snapshot()` exposes both fields (`Addr` and `Draining`), decoded from the peer's `Meta` in the `NotifyJoin` / `NotifyUpdate` callbacks. A `Membership.SetDraining(bool)` method updates the LOCAL node's `Meta` flag and calls `memberlist.UpdateNode` so the change gossips out; the node STAYS a full, alive member - `SetDraining(true)` is NOT `Leave()`. A `Draining` member is REACHABLE (still in the snapshot, address known, serving gRPC) AND, under the pending-ranges model (Phase 2e), it STAYS A CURRENT OWNER in the consistent-hash ring: it keeps the positions it already serves and is NOT dropped from ownership when it sets `Draining`. The `Draining` bit is consumed by the ROUTING layer (`replicasForKey`), which computes a position's CURRENT replica set over the ring INCLUDING draining members and its PENDING replica set over the ring EXCLUDING draining members, and during a transition ROUTES to the UNION (so the draining node keeps receiving writes + serving reads until its successor is provably serving, at which point ordered removal collapses the union onto the pending set). The ring-from-membership reconcile (`reconcileRingFromMembership`) therefore does NOT exclude a draining member - the exclusion is computed per-op inside `replicasForKey`, not baked into the ring. This is the foundation of the draining node-state described in Phase 2e. (NB this REVERSES the earlier draining-exclusion design, where `reconcileRingFromMembership` dropped a draining member from the ring and survivors forwarded back to it; that per-position forwarding model is SUPERSEDED - see Phase 2e.)

The membership layer's event delegate uses non-blocking sends to its event channel so a slow subscriber can't deadlock memberlist's gossip goroutines. When the channel is full, the event is dropped + `Membership.DropCount()` is incremented. To keep the ring consistent with reality despite drops, the cluster runs a periodic reconciler (~5s) that calls `Membership.Snapshot()` and applies any missed adds / removes.

`Members()` and `Snapshot()` return from an internal cache that the event delegate maintains: every `NotifyJoin` / `NotifyLeave` / `NotifyUpdate` callback updates the cache, and reads consult the cache under a `sync.RWMutex`. Reading directly from `memberlist.Members()` instead would race with memberlist's internal `aliveNode` goroutine, which mutates the `*Node` fields exposed by that call without exposing a per-node lock. The event callbacks are serialized against those internal transitions, so cache writes from inside the callbacks are race-free, and the cache stays consistent with the authoritative state memberlist itself publishes via those same events. Even when the channel drops, the cache update happens before the send attempt, so a dropped notification still leaves the cache (and therefore `Snapshot`) authoritative for the reconciler.

`Membership.Close()` performs a best-effort graceful leave (broadcast "I am leaving" to peers so they record a clean `EventLeave` rather than waiting for failure detection) followed by `Shutdown()` of the local memberlist. The leave broadcast is given a **bounded timeout**: if the broadcast does not complete within it, `Close` proceeds to `Shutdown` anyway. The wait is bounded because memberlist only completes a leave broadcast once the departing node's message is actually gossiped out, which requires at least one live peer to receive it; an unbounded wait would block `Close` forever when peers are slow, unreachable, or the broadcast never finishes (the failure-detection path on the remaining peers still observes the departure regardless). Close is idempotent and never blocks indefinitely.

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

**Retry-on-transient (single-backend, symmetric with the multi-backend handoff retry).** A single fan-out pass can fall short of W for a reason that is NOT a real failure: one or more replica legs come back TRANSIENT (the v0.3 migration-guard `codes.ResourceExhausted` from a replica whose partition is mid-handoff, i.e. a still-open `StateReceiving` window). The fan-out counts a transient leg toward NEITHER the ack budget NOR the failure budget, so a write whose only shortfall is transient legs lands `acks < W` with ZERO accumulated failures. Rather than return `Unavailable` immediately on this, the single-backend replicated write path RETRIES the fan-out, bounded by `WriteTimeout`, exactly as the multi-backend handoff path does (see "v0.8 Phase 2d"). It uses the SAME retry machinery: re-run the fan-out after a jittered exponential backoff (base `RebalanceRetryAfterMs`, x2, cap 500ms), bounded by the `WriteTimeout` wall clock, surfacing the last retryable error only once the budget is exhausted. The retry fires ONLY when the ack-shortfall is PURELY transient (`acks < W` AND zero non-transient failures); a real failure (a genuinely-down peer that exhausted the `(R - W)` failure budget) is returned IMMEDIATELY, unretried, so the retry never papers over a real outage by spinning until `WriteTimeout`. This closes a flake where a SWIM gossip join reopening a `StateReceiving` window mid-write made an otherwise-satisfiable W momentarily unreachable and failed the write with no retry; the symmetric retry waits the window out instead.

**Why retrying a partially-acked write is safe (LWW idempotency).** A retry re-dispatches the SAME pre-stamped envelope to ALL R replicas. The stamp is computed ONCE (before the first attempt) and reused across every attempt, so a leg that already acked re-applies an envelope whose stamp is equal-not-greater than what it already stored; apply-if-newer (above) makes that a silent NO-OP. So retrying a write that partially acked cannot double-apply, cannot move any replica backward, and cannot lose data: it only gives the fan-out more wall-clock to collect W acks across the transient window. This is the same safety basis the multi-backend Phase 2d retry relies on.

#### Read path

`Get` dispatches read RPCs to all R live replicas (so a hung / down primary falls back to a successor without losing the call) and waits for N successful responses per `ReadConsistency` (1 / quorum / R). Each replica returns its local envelope (or `NotFound`). The cluster layer:

  1. Collects responses up to the consistency target.
  2. Picks the LWW winner across the collected envelopes.
  3. Returns the winner's payload (or `NotFound` if the winner is a tombstone or no replica had the key).

Surplus responses past the consistency target keep flowing onto the result channel until every dispatched op reports (or `ReadTimeout` fires), so read-repair below can classify replicas that disagreed with the winner.

On `Quorum` / `All`, if the collected envelopes disagree (any replica returned an older Stamp than the winner, or `NotFound` while a peer has a value), the cluster issues **read-repair**: an async forwarded Put of the winner envelope to every lagging replica. Read-repair is best-effort: errors are swallowed, no retry. Read-repair is **skipped on `Nearest`** because a single-replica read has nothing to compare against; lagging replicas catch up via the next quorum/all read or via future anti-entropy (v0.4.1+). Read-repair rides the ordinary replica-receiving write path, so it inherits the apply-if-newer rule below: a repair built from a stale read can no longer clobber a replica that has since taken a newer write (its older stamp simply loses the apply-if-newer compare and the repair is a silent no-op).

#### LWW on write (apply-if-newer, v0.7+)

The LWW comparator (above) resolves conflicts on the READ side. v0.7 makes the comparator also resolve on the WRITE side: every replica-receiving write at R>1 is **apply-if-newer**. Before persisting an incoming envelope, the receiving node decodes the incoming Stamp, decodes the Stamp of the value already stored under that key, and writes the incoming bytes **only if** `incoming.Stamp.Greater(stored.Stamp)` is true OR there is no stored value. An incoming write whose stamp is not strictly greater than the stored stamp is a silent no-op: the replica already holds an equal-or-newer value, so overwriting would move it backward.

This complements LWW-on-read: where read-repair pulls laggards forward, apply-if-newer stops any replica-receiving write (including a late read-repair) from pushing a replica backward.

  - **Absent stored value:** apply (there is nothing to lose to). A v0.3 bare value (decodes as `Stamp{0, ""}`) loses to any real stamped incoming write and is replaced, the same migration behavior the read path already has.
  - **Tombstones:** the rule is purely stamp-vs-stamp; payload is irrelevant. A tombstone envelope (empty payload, real stamp) applies over an older value, and a newer value applies over an older tombstone. Equal stamps do not apply (the comparator is strict), so a re-delivered identical envelope is correctly a no-op.
  - **Equal stamps:** `Greater` is strict (`A.Greater(A) == false`), so an envelope with a stamp equal to the stored one does not apply. Idempotent re-delivery of the same write is therefore a no-op, not an overwrite.

**Why a per-node serialization lock.** The read-stored / compare / put-if-newer step must be **atomic per key**: two concurrent applies to the same key must not both read the old stamp, both decide they are newer, and race their puts (the older one could land last and win). It runs inside one backend transaction (`Begin` -> `Get` -> compare -> `Put` -> `Commit`), but the memory backend's transaction provides snapshot-isolation reads with **no write-write conflict detection** (a known property), so two get-compare-put transactions on the same key can both commit and one update is lost. v0.7 therefore serializes the apply-if-newer critical section under a dedicated per-node mutex (`Cluster.applyMu`), distinct from `casCommitMu`. The window is a single local backend op (no network inside it), so the lock is cheap. A node-wide lock is sufficient for v0.x; per-key striping is a possible refinement, not a v0.7 requirement. On a backend that DOES detect write-write conflicts the lock is belt-and-suspenders, the same reasoning `casCommitMu` already uses.

**Which write paths become apply-if-newer (all R>1 only):**

  - **Single-key replicated Put**, both receiving branches: the local-self branch (the originator writing its own replica copy) and the remote branch (a peer's forwarded Put landing on the receiving node's local Backend). Wherever a replicated envelope is written to a Backend on the RECEIVING node, the apply-if-newer compare runs first.
  - **CAS write-set fan-out** (`ApplyBatch` on a replica): each `(key, envelope)` in the batch is applied apply-if-newer inside the batch's one transaction.
  - **Read-repair**, for free: it dispatches through the single-key replica-receiving path, so the apply-if-newer compare already guards it (see the read-path note above).

**Which write paths stay verbatim:**

  - **R == 1**: raw values, no envelopes, no stamps. There is nothing to compare, so R=1 keeps the unconditional `backend.Put` / `backend.Delete` it has always had. No lock is taken on the R=1 path.
  - **The owner's OWN validated CAS local commit** (the `LocalBegin` validate-and-apply inside `CommitCASApply`): authoritative-by-construction. It is validated under `casCommitMu` and writes the freshest stamp directly; an apply-if-newer check there would be redundant (the owner just established this is the newest write for the shard). Only the REPLICA-RECEIVING paths get apply-if-newer. The owner-local commit is the single source of write ordering for its shard; the fan-out is the thing that must self-resolve when it arrives out of order, and apply-if-newer on each replica is what makes it do so.

**The bug this fixes.** Before v0.7, replica-receiving writes applied verbatim and LWW resolved only on read. A read-repair scheduled from an OLD quorum read could fire AFTER a newer write committed and overwrite the newer value with the stale one. On the owner's own local copy this then made a later CAS validate-and-apply read a stale value and MISS a conflict, losing an update (observed as concurrent increments landing short of their count under `Quorum` / `All` reads; it did not occur under `Nearest`, which issues no read-repair). Apply-if-newer closes it at the source: a stale repair (older stamp) can never overwrite a newer value, on a replica or on the owner.

#### Failure modes

  - **Replica down at Put time**: tolerated up to `R - W` simultaneous failures. With the default R=3 W=2, one replica down still completes writes. With `WriteConsistency=All`, any replica down fails the write. A down replica returns `codes.Unavailable` (real peer-down), which counts against the failure budget so the fanout fails fast once `R - W + 1` such replies land instead of waiting on every remaining peer.
  - **Network partition**: both sides accept writes for keys they can reach a sufficient quorum on. Writes on either side carry the originator's Stamp. On heal, the LWW comparator reconciles deterministically: the higher-timestamp write wins, lower-timestamp writes are lost. This is the AP choice (see "Consistency model"); workloads that cannot tolerate write loss should use a CP system.
  - **All R replicas down**: `Put` and `Get` return `Unavailable`. No way to make progress without at least one replica.
  - **Replica down at Read time**: tolerated up to (R - N) where N is the consistency-target count. `Nearest` with primary down falls back to the next ring successor; `Quorum` / `All` collect from surviving replicas and fail if too few respond.
  - **Replica mid-handoff (rebalance)**: the receiving node returns `codes.ResourceExhausted` (the migration-guard sentinel), distinct from `Unavailable`. The fanout classifies it as transient (skip this replica, wait for another) and does NOT count it against the failure budget. Without that distinction, a single mid-handoff replica during normal v0.3 rebalance would force every otherwise-quorum write to wait for the migration to complete. If ENOUGH replica legs are simultaneously transient that W is momentarily unreachable in one fan-out pass (e.g. a late SWIM gossip join reopens a `StateReceiving` window mid-write), the single-backend write path RETRIES the fan-out, bounded by `WriteTimeout`, rather than failing the write (see "Retry-on-transient" under "Fan-out + ack accounting"). The retry is idempotent under LWW (same pre-stamped envelope, apply-if-newer no-ops a leg that already acked) and fast-fails on any real failure. (The v0.8 multi-backend acquiring-window refusal `errUnitAcquiring` is the SAME class: it is sentinel-tagged so `isTransientReplicaErr` skips it too even though it travels client-facing as `codes.Unavailable`, and the cross-node replica leg re-codes it to `codes.ResourceExhausted`; see "v0.8 Phase 2d".)

#### Out of scope for v0.4

  - **Hinted handoff** (deferred to v0.4.1): when a replica is down at Put time, the coordinator should durably hint the missed write and replay it when the replica returns. v0.4 ships without hints, so a write that lands on (R - W) acks succeeds but the down replicas miss it permanently until a Quorum / All read repairs them or anti-entropy reconciles. The hint protocol is a v0.4.1 follow-up.
  - **Anti-entropy / Merkle-tree repair** (v0.4.1+): a background process that walks ranges + reconciles cold replicas without waiting for a read. Not in v0.4.
  - **Per-key replication factor**: every key in the cluster uses the same R. Per-key overrides are out of scope; operators who need per-prefix replication policy should run separate clusters.
  - **Rebalance-with-replication**: a separate workflow (planned post-v0.4) that integrates the v0.3 rebalance protocol with the v0.4 replication overlay so live writes flow via replication during the bulk copy and the per-range write-rejection window collapses to the ownership swap.

### Rebalancing (v0.3+)

v0.2 ships the gossip ring but does not migrate data: when membership changes, the ring redirects lookups to a node that does not hold the key, so reads against the new owner return not-found and writes land on the wrong shard. v0.3 closes that gap by physically migrating keys when membership changes, so the data state catches up to the ring state.

The design optimizes for an implementation small enough to reason about end to end, on the assumption that shale is embedded in apps run by individual developers and small teams (not Google-scale operators). It accepts brief per-range unavailability (tens of seconds for the streaming copy, milliseconds at the ownership swap) in exchange for: no coordinator election, no per-Backend WAL-tailing requirement, no five-state-machine recovery protocol. v0.4 adds replication (R>1), which is the natural place to recover read availability during migration by serving reads off a replica that already holds the range; trying to engineer that into v0.3 at R=1 buys little and complicates a lot.

This section reads most simply at R=1 (one owner per key), and the wire protocol, cutover, and reconcile machinery are first described in those terms. The generalization to R>1 is not deferred work: the rebalance is **replica-set aware** at every R. The single rule that makes both cases one mechanism is stated up front in "Replica-set placement (the rule for all R)" below, and the R>1 behavior of each pass is flagged inline and gathered at the end ("What v0.4 changes"). The mental model: at R=1 a key's desired holder set is the single owner `ring.LocateKey(shardKeyFn(k))`; at R>1 it is the R-node replica set `ring.LocateKeyN(shardKeyFn(k), R)`. R=1 is the degenerate one-element case of the R>1 rule, not a separate code path.

#### Replica-set placement (the rule for all R)

The single invariant the whole rebalance enforces, at every ReplicationFactor:

> On the current ring, every key is physically resident on **exactly** the nodes in `ring.LocateKeyN(shardKeyFn(k), R)` (its **replica set**): the R nodes that must hold a copy. No more, no fewer, once the cluster has settled.

Every placement decision below derives from that one rule:

  - **A node DELETES a key in the post-handoff sweep ONLY when it is no longer in that key's replica set.** A node that handed off the *primary* role for a key but is still one of the R replica-owners RETAINS its copy. Deleting a key the node still replica-owns is the bug this design fixes (see "The replica-aware invariant" below); the sweep is gated on replica-set membership, not on whether the primary moved.
  - **A node that is newly in a key's replica set, but does not yet hold the key, RECEIVES it** from any current holder (any peer already in the replica set). "Newly a replica-owner" is the deliver-side trigger, exactly mirroring the delete-side gate.

At R=1 these collapse to today's behavior with no change: `LocateKeyN(k, 1)` is the single-element set `{LocateKey(k)}`, so after a primary moves the old owner is NOT in the new replica set and DELETES (identical to the v0.3 single-owner sweep), and the new owner is the only newly-added replica and RECEIVES. R=1 stays byte-for-byte the v0.3 path. At R=2 on a 2-node cluster every key's replica set is BOTH nodes, so when a node joins the founder RETAINS every key (it is still a replica) and the joiner RECEIVES every key (it is newly a replica of all of them): both nodes end up holding all keys, which is what R=2 requires.

The Coordinator is therefore parameterized by `ReplicationFactor` and a replica-set predicate (the current ring plus R), not just a single-owner ring view. The cluster wires `Config.ReplicationFactor` and the ring into it the same way it wires `Config.ShardKeyFn`. At R=1 the predicate degenerates to the single-owner comparison the v0.3 Coordinator already made.

#### Trigger

Migration is driven by membership change, with a settling delay.

When `Membership` reports a join or leave, the node records the event and schedules a `rebalance.Evaluate()` for `T_settle` seconds in the future (default 5s, configurable via `Config.RebalanceSettleDelay`). Any further membership event inside the window resets the timer. When the timer fires, the node snapshots the current ring and proceeds.

The settling delay collapses bursts (rolling restart, several nodes joining within a second, a flapping peer) into one rebalance pass instead of thrashing the cluster through intermediate ring shapes. It also gives the existing membership reconciler (~5s) time to absorb missed events, so by the time `Evaluate` runs, every node tends to have the same view of who is in the cluster.

A node is considered *rebalance-idle* only when no settle-timer evaluation is pending (scheduled but not yet fired) and every tracked migration has reached a terminal state. `WaitForRebalanceIdle` blocks until both hold, so a caller that observes idle immediately after a membership change is guaranteed the debounced evaluation has already run and drained, not merely that nothing has been scheduled yet. A pending debounce timer therefore counts as not-idle even while the Coordinator's range table is still empty.

There is no consensus and no coordinator election. Each node independently decides what to send and what to receive based on its local view of membership. Because memberlist converges in bounded time and each node uses the same ring algorithm against the same member list, peers reach the same migration decisions without negotiation. Disagreement during convergence is handled by the existing forwarding loop-guard plus the per-range cutover below.

An operator can also trigger evaluation explicitly:

  - `shale rebalance --dry-run` prints the plan the local node would execute against the current ring without doing the migration.
  - `shale rebalance --apply` runs `Evaluate` immediately, bypassing the settling delay. Useful for planned topology changes (node decommission) where the operator wants to watch the plan before pulling the trigger.

#### Plan

Each node computes its plan locally from two inputs:

  - the previous ring snapshot (cached from the last evaluation, or empty on first run)
  - the new ring snapshot

The unit of migration is the **key range**, defined as the contiguous arc of the hash ring between two adjacent virtual nodes. `buraksezer/consistent` exposes enough state to enumerate ranges. For each range, the node compares its **old replica-set membership** against its **new replica-set membership** (at R=1 the replica set is the single owner, so this is exactly the old-owner-vs-new-owner comparison):

  - was-replica, no-longer-replica: schedule **send** to a peer that newly needs the range (the new copy must exist somewhere before the local copy is dropped); the local copy is deleted only by the post-handoff sweep, and only after confirming the node is no longer in the range's replica set.
  - not-replica, now-replica: schedule **receive** from a current holder (passive; the source is the one who initiates the stream).
  - replica in both: **retain**. The node keeps its copy. At R=1 this is the old=self/new=self no-op; at R>1 it is the case the v0.3 single-owner diff got wrong (it would delete a key whose primary moved even though the node is still a replica) and is now an explicit RETAIN.
  - replica in neither: no-op for this node; the peers that do hold or need the range handle it between themselves.

The delete-side and deliver-side decisions are therefore symmetric and both keyed on replica-set membership: a node sheds a range only when it leaves the range's replica set, and pulls a range only when it enters it. Both sides of any (send, receive) pair derive the plan from the same ring inputs (current ring plus R), so they agree on who sends what without explicit negotiation. The plan is held in memory; there is no persisted plan object and no plan ID. A crash mid-migration is recovered by the next `Evaluate` run on restart: the node observes the current ring versus what it actually holds and re-issues whatever migrations are still needed. This recovery path is the same code path as the steady-state one.

#### Reconcile (owned-but-missing repair)

The ring-vs-ring `ComputePlan` above is correct but incomplete on its own: it can only see ownership transitions that show up as a *difference between two ring snapshots it actually observed*. It is blind to a partition this node owns but never physically received, because that partition looks stable (old owner = new owner = self) in both snapshots the node holds. That blind spot is the **founder-grows (1 -> 2) data-loss path** and it arises from a ring-convergence race at the joiner's bootstrap:

  - A joiner's `Membership.Join` returns once it has *contacted* a seed, but memberlist's push/pull state transfer and the `NotifyJoin` callbacks that populate the local member list may not have completed. Under slow gossip (real gRPC + an object-store backend), the at-startup ring snapshot the joiner pins as its `lastEvalRing` baseline can be self-only: `{joiner}`.
  - When the founder later becomes visible, the joiner runs `ComputePlan(joiner, old={joiner}, new={founder, joiner})`. For every partition the consistent-hash ring assigns to the joiner in *both* rings (old owner self, new owner self), the diff is a no-op. No `Receive` is scheduled. The keys for those partitions physically live on the founder and are never streamed across, so a routed `Get` to the new owner (the joiner) finds nothing. The bytes survive on the founder but are unreachable from the cluster: data loss by un-reachability, not destruction.

Ring-vs-ring diffing cannot close this because the joiner's two snapshots agree; the only authority that disagrees is **physical placement**. So `Evaluate` runs a second, independent pass keyed on what this node actually holds, not on ring history:

  - For each partition `p` whose **replica set on the current ring includes self** (at R=1: self is `p`'s owner; at R>1: self is one of the R nodes `LocateKeyN` assigns `p`), the node asks: do I physically hold any of `p`'s keys? It answers by consulting the same local scan + partition function the source/sweep already use (`Coordinator.partFn`). A partition the ring assigns self a replica slot for, but for which the local backend holds **zero** keys, is an *owned-but-missing* partition.
  - For each owned-but-missing partition, the node schedules a **Receive from a peer that already holds `p`** (at R=1: the partition's previous owner, the node the *current* ring would have placed `p` on before self joined; at R>1: any current replica that holds a copy, not necessarily the prior primary). That `From` is a peer `ComputePlan` would have named had the joiner seen the converged ring at bootstrap. The Receive is registered through the *identical* `tryRegister` -> `runReceive` -> `FetchRange` path the ring-vs-ring plan uses; there is no second migration mechanism.

This pass is folded into the existing settle-timer `Evaluate` (it runs on every tick, after the ring-vs-ring plan), and is also reachable from the ~5s membership reconciler that already re-arms the settle timer. So convergence is self-healing on a bounded schedule: even if the bootstrap snapshot was self-only, the first settle-timer `Evaluate` after the founder becomes visible repairs every owned-but-missing partition. No new wire message, no persisted state, no operator action.

Two guards keep the reconcile pass from over-firing:

  - **Hold-detection, not count-matching.** A partition is owned-but-missing only when the node holds *zero* keys for it. A partition the node already received (or always held) is skipped, so a settled cluster issues no reconcile Receives and the pass is idle in steady state. (A partition that legitimately holds zero keys because no key has ever hashed into it is indistinguishable from missing, but pulling an empty range from its prior owner is a harmless zero-key `FetchRange` that flips straight to `Done`.)
  - **No double-register.** `tryRegister` already refuses to register a partition that is in a non-terminal state, so a reconcile Receive scheduled while the ring-vs-ring plan already has that partition in `Receiving` is dropped. The two passes can name the same partition without racing.

**The convergence invariant.** Once the ring has settled (membership stable, every in-flight migration terminal), every node physically holds every partition the current ring assigns it a replica slot for (at R=1: every partition it owns; at R>1: every partition in whose replica set it appears), and no committed key is under-replicated or unreachable: a routed `Get` to any key reaches a replica that holds it, and every key is physically resident on exactly the R nodes `LocateKeyN(key, R)` returns. The ring-vs-ring plan moves data across replica-set *transitions* the node witnessed; the reconcile pass repairs replica slots the node holds but never witnessed a transition for. Together they make physical placement match the ring's replica-set assignment, which is the property the founder-grows gate test asserts at R=1 and the membership-change-at-R2 gate test asserts at R>1.

**The replica-aware invariant (the bug this fixes).** Two properties the replica-set rule guarantees that the v0.3 single-owner diff violated at R>1:

  - **A node must never delete a key it still replica-owns.** The v0.3 sweep deleted from a node every key whose ring-*primary* moved to a joiner, with no check for whether the node was still one of the R replica-owners. At N=2,R=2 that deleted from the founder the roughly-half of keys whose primary moved to the joiner even though the founder is still a replica of all of them. The corrected sweep deletes a key only when the node is no longer in `LocateKeyN(key, R)`, so a node that remains a replica RETAINS its copy.
  - **The cluster must not lose an acked key across a membership change at R>1.** The v0.3 plan also never delivered to the joiner the keys whose primary STAYED on the founder, even though the joiner is a replica of those too at R=2: the joiner settled holding only the keys whose primary moved, never reaching full replication. The corrected deliver side schedules a Receive for every key the node is newly a replica of, so at N=2,R=2 the joiner ends up holding ALL keys and the cluster preserves R=2 (every key on both nodes) with no acked write lost. The combination - delete only on leaving the replica set, receive on entering it - is copy-before-delete at the granularity of the replica set rather than the single owner.

**The delete side is symmetric to reconcile: evict replica slots the node holds but no longer owns.** The post-handoff sweep (the "Swap"/"Post-migration" cutover steps below) deletes a node's local copy of a partition it *handed off as primary*, gated on the node having left the partition's replica set. At R>1 that sweep alone is incomplete in the same way `ComputePlan` is incomplete on the deliver side: a node can hold a partition it was never the *primary* of (it was a successor replica), and a membership change can drop it OUT of that partition's replica set without producing any handoff event for it to sweep. Example: grow 2 -> 3 at R=2. Both original nodes hold every key. After the third node joins, each key's replica set is only 2 of the 3 nodes, so for the keys whose replica set no longer includes it, a node that was a *non-primary* replica must shed its copy - but it never sent those keys, so there is no `StateHandedOff` range for the sweep to act on. Left unaddressed, the cluster over-retains: it converges to N x nodes physical copies instead of N x R, an unbounded leak as the cluster grows. So `Evaluate` runs an **evict pass** symmetric to reconcile, keyed on physical placement: it scans local keys, buckets them by partition, and for every partition the node physically holds but is NOT in `ReplicasForPartition(p, R)` of the current ring, it registers the partition straight into the handed-off state so the existing grace sweep deletes it after `T_drain`. The grace delay is load-bearing: it gives any node newly in the replica set time to pull its copy (the reconcile Receive is a pull) before the evicting node drops it, and at R>1 the data is in any case still resident on the other R-1 replicas, so an eviction can never drop a key below its replica count. At R<=1 the evict pass is a no-op: the single-owner `ComputePlan` already emits a Send (and thus a sweep delete) for every partition the sole owner sheds, so there is no held-but-never-primary class to evict. Reconcile (deliver) and evict (delete) are the two physical-placement passes that, together with the ring-vs-ring plan, make a node's local holdings exactly its replica-set assignment.

**Both physical-placement passes run on a bounded periodic schedule, not just the one post-event `Evaluate`.** A membership change bumps the node's ring generation once and fires one settle-timer `Evaluate`. That single pass is not always enough to converge at R>1: a reconcile Receive can come back *empty* when its chosen source was itself still settling (a source-not-ready race - on a shrink, two survivors can each newly enter a partition's replica set and briefly race over who holds the data first). A zero-key pull is ambiguous (genuinely-empty partition vs source-not-ready), so the node retries a zero-key pull a small bounded number of times before accepting it as empty, and re-runs both the reconcile and evict passes on the existing grace-sweep ticker (against the ring captured by the last `Evaluate`). This makes convergence self-healing on a bounded schedule with no extra membership event required: a transient race resolves within a tick or two, while a settled cluster (every owned partition held, no stale-replica partition held) runs both passes as cheap no-ops.

The empty-pull retry is **bounded**: a zero-key pull is retried a small fixed number of times (the empty-retry cap) before the partition is accepted as clean-empty and skipped, so a genuinely-empty partition does not retry forever. Correctness of placement does NOT depend on this repair path. Live replicated writes always reach every replica directly via the `putReplicated` fan-out (the originator writes its own replica copy and forwards to the other R-1 replicas), so a node that is a replica of a partition receives that partition's live writes whether or not its reconcile pull ever found a non-empty source. The reconcile pull is purely the catch-up / repair path for data written before the node entered the replica set; the bounded empty-retry only governs how aggressively that one-time catch-up probes a possibly-still-settling source. Full anti-entropy (periodic Merkle-tree reconciliation that re-checks every replica assignment indefinitely, including re-pulling a partition the node was earlier told is empty) is explicitly v0.4.1+ future work; the v0.4 reconcile pass is bounded catch-up, not continuous anti-entropy.

**The partition function MUST match routing (honor `ShardKeyFn`).** The reconcile pass, the source-side handoff scan, and the grace sweep all bucket a backend's keys into partitions via `Coordinator.partFn`. For the convergence invariant to hold, that partition function MUST compute the partition on the **same shard key the cluster routes reads with**: `partition(k) = ring.PartitionID(shardKeyFn(k))`, where `shardKeyFn` is the app's `Config.ShardKeyFn` (identity when unset). Routing places a key on `ring.LocateKey(shardKeyFn(k))`, which is the owner of `ring.PartitionID(shardKeyFn(k))`; if rebalance instead bucketed on the raw key, the two would disagree for any app with a non-identity `ShardKeyFn`. The concrete failure: an app that co-locates one logical subject across many raw keys (e.g. `pastes/<slug>` plus `versions/<slug>/<n>` all shard-keyed to `<slug>`) has those keys hash to ONE partition under routing but SCATTER across many partitions under a raw-key bucketing. The reconcile pass on a joiner would then repair only whichever raw-key partition happened to land on it and strand the subject's remaining keys on the founder, while the ring routes the subject's reads to the joiner: the multi-key founder-grows loss. So `Config.ShardKeyFn` governs not just Put/Get/Delete routing but also where the rebalance machinery places a key. The Coordinator takes the `ShardKeyFn` as an option and the cluster wires `Config.ShardKeyFn` into it; the source-side `MigrateRange` handler applies the same extraction so the keys streamed for a requested partition are exactly the keys routing assigns to it.

#### Interaction with the existing 2 -> 3 growth path

The reconcile pass is additive and changes nothing about a node that *did* see the ring transition. In the established 2 -> 3 path, the third node joins a ring whose two members are already gossiped + converged; its bootstrap snapshot lists all three, the synthesized `old = current - self` baseline is `{n1, n2}`, and `ComputePlan` emits the correct Receives directly. Hold-detection then finds those partitions already in flight (or, after they land, physically held) and the reconcile pass issues nothing further. The 2 -> 3 case never depended on the blind spot, so repairing the blind spot leaves it untouched. The danger the founder-grows case exposes is specifically the *founder's* self-only bootstrap snapshot under slow gossip; the reconcile pass closes that without perturbing the snapshot-was-converged case.

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
4. **Post-migration**: destination is authoritative for reads and writes. Source forwards any straggler read it receives for the range to the destination via the standard forwarding path. Once the source observes that all peers' rings agree the destination owns the range (or after a grace period of `T_drain`, default 30s), the source's grace sweep deletes its local copy of the range's keys **only if the source is no longer in the range's replica set** on the current ring. At R=1 the source is never in the new replica set after handing off the primary, so it always deletes (identical to v0.3). At R>1 a source that handed off the primary role but remains one of the R replica-owners RETAINS its copy: the sweep skips any key for which the node is still in `LocateKeyN(key, R)`. This is the per-key gate that keeps a node from deleting data it still replica-owns.

The write-rejection window is the price paid for avoiding WAL-tailing. At hostthis-scale workloads (tens of thousands of keys per range, small values), the streaming copy completes in well under a minute on a local network; clients retrying with exponential backoff over that window do not see user-visible errors.

#### Why reconcile cannot itself lose data

The reconcile pass (above) only ever schedules **Receives**: it *pulls* a partition's keys onto the node that owns it but is missing them. It never schedules a Send and never deletes anything. The copy-before-delete safety that protects the existing handoff is therefore preserved verbatim:

  - **A Receive is a non-destructive copy.** `FetchRange` writes the pulled keys into the destination's local backend and never touches the source. The only code path that deletes a source's local copy is the grace sweep (`StateHandedOff -> StateDone` after `T_drain`), and the sweep fires only after the source's `runSendWired` has observed a successful destination ack via `MarkSendComplete` (gated by `AwaitHandoffSignal`, which the cluster always sets). The reconcile pass does not produce Sends, so it never arms the sweep on the prior owner. The prior owner keeps its copy until (and only until) it has independently received its own ack-gated Send for that partition. There is no path by which scheduling a reconcile Receive causes any node to delete the only copy of a key.
  - **The destination's atomicity is unchanged.** A reconcile Receive uses the same `FetchRange` that validates the terminal CRC32 + count and rolls back its partial writes on any mid-stream failure (source EOF before `MigrationDone`, checksum mismatch, transport error). A failed reconcile pull leaves the destination exactly as it was (no partial range made authoritative) and the prior owner still holds the data, so the next settle-timer `Evaluate` re-detects the partition as owned-but-missing and retries. The failure mode is "retry," never "loss."
  - **Idempotent and convergent.** Re-pulling a partition that is actually already held is prevented by hold-detection (the node skips partitions for which it holds any key) and by `tryRegister` (no double-register of an in-flight partition). In the worst case a benign empty-range pull flips straight to `Done`. The pass quiesces once every owned partition is physically held, which is exactly the invariant below.

#### Failure handling

  - **Source crashes mid-stream**: destination's `MigrateRange` call returns an error. Destination discards what it received (the partial range is not authoritative). On the next `Evaluate` pass, if the source comes back, migration retries. If the source does not come back, the range is owned by no one at R=1; reads return not-found, writes are rejected. This is the unavoidable R=1 failure mode and is the central motivation for v0.4 replication.
  - **Destination crashes mid-stream**: source's stream returns an error. Source remains the owner. On next `Evaluate`, migration retries to whichever node the new ring assigns.
  - **Checksum mismatch on `MigrationDone`**: destination errors its stream (does not drain it cleanly), the source's handler returns that error rather than success, and the coordinator does NOT flip state. Destination rolls back its writes for the range; source remains owner; migration retries on the next `Evaluate`. Checksum is computed over the sorted `(key, value)` byte stream.
  - **Ring divergence during cutover**: handled by the existing loop-guard. A read or write landing on the wrong node gets one forward; if the forwarded-to node also disagrees, the request returns `FailedPrecondition` and the client retries after the rings converge.
  - **Operator cancellation**: `shale rebalance --cancel` aborts in-progress streams. Source remains owner of any range not yet swapped; destination discards any partial state.
  - **Owned-but-missing after a bootstrap race**: a node that pinned a self-only ring snapshot at join (slow gossip) owns partitions it never received. The reconcile pass detects each such partition (owned by self, zero keys held) and pulls it from its prior owner. The pull is a non-destructive Receive; on failure the prior owner still holds the data and the next `Evaluate` retries. This is the founder-grows path and it self-heals within one settle interval; no data is destroyed because no Send (and therefore no sweep delete) is ever scheduled by the reconcile pass.

#### Observability

  - `shale topology` shows each range's state (`stable | migrating-out | migrating-in | draining`) and, for in-flight migrations, the source, destination, bytes streamed so far, and elapsed time.
  - Per-node counters: `rebalance_ranges_migrated_total`, `rebalance_bytes_streamed_total`, `rebalance_writes_rejected_total`, `rebalance_failures_total`.
  - Structured log line per range transition with `range_id`, `from`, `to`, `key_count`, `bytes`, `duration_ms`.

#### What v0.4 changes

With R>1, the read-unavailability problem largely disappears: reads can be served from any replica, including ones not currently sending or receiving. The migration unit becomes a (range, replica-position) pair rather than just a range. The write-rejection window can shrink to the duration of the ownership swap rather than the entire streaming copy, because the destination can receive writes via the replication path while the bulk copy is in flight and the conflict resolver (LWW, v0.4) reconciles. The wire protocol stays the same shape; the cutover protocol gains a "live writes also flow via replication" overlay.

What is NOT deferred is correct placement at R>1: the rebalance is replica-set aware in every pass, per "Replica-set placement (the rule for all R)" above. The delete side is gated on leaving `LocateKeyN(key, R)` (a node still in the replica set RETAINS its copy through the sweep), and the deliver side fires for every key the node is newly a replica of. That is what makes a membership change at R=2 settle with every key resident on both nodes rather than split single-owner. The read-availability and shrunken-write-window optimizations layered on top are the parts the cutover overlay still adds; placement correctness is already in place.

The v0.3 design does not preclude any of the optimization work. It deliberately leaves the per-range cutover as the only place where the v0.4 availability overlay needs to intervene.

The reconcile pass generalizes cleanly to R>1: the unit becomes a (partition, replica-position) pair, and "owned-but-missing" means this node holds a replica slot the ring assigns to it but has not yet received. The pull is the same non-destructive `FetchRange` from a peer that already holds a copy (any current replica, not necessarily the prior primary). At R>1 a reconcile pull is itself a replica-receiving write, so it applies through the SAME apply-if-newer path as a live replicated Put rather than a raw `Put`: each streamed (key, value) pair carries an LWW Envelope (the identical bytes a replicated write fans out), and the receiving node lands it via `applyEnvelopeIfNewer` (see "LWW on write (apply-if-newer)"), which decodes the incoming Stamp, compares it against the Stamp already stored under that key, and writes only if the incoming Stamp strictly beats the stored one. This makes "never a clobber" a literal property of the migration apply, not just an aspiration: at R>1 the joiner reconcile-pulls partitions that have LIVE writers on the other replicas (the pull reads from a writable replica with no write-rejection lock), so an in-flight migration stream can carry an OLDER value for some key K while a concurrent `putReplicated` of K lands a NEWER value on the receiving node. The older streamed value loses the per-key Stamp compare and is a committed no-op; the newer concurrent write survives. A reordered or stale reconcile pull is therefore a silent no-op, never an overwrite of a newer concurrent write, exactly as the read-repair and CAS fan-out paths self-resolve under the same rule. At R<=1 there are no envelopes (raw values), no live cross-replica writers for the same partition, and no apply-if-newer: the migration apply is the unchanged raw `Put`. Because the pass only copies and never deletes, it is also the natural seed for the v0.4.1+ anti-entropy / Merkle-tree repair: founder-grows is the degenerate, zero-key-held case of the same "make my physical holdings match my ring assignment" reconciliation. The `FetchRange` stream still validates the terminal CRC32 + key count and rolls back its partial writes on a bad stream; routing each apply through apply-if-newer does not change that failure/rollback contract (a rolled-back stream leaves no apply-if-newer side effect, since each apply runs in its own committed-or-rolled-back local transaction).

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

**Transparent retry through a transient commit failure (freeze + reshard cutover).** Two distinct transient signals on the COMMIT can fire while the cluster is mid-reshard, and `Transact` rides BOTH out transparently rather than surfacing them as hard failures. (1) `codes.Unavailable` - the cluster-wide write-freeze window, or the pin unit's lease mid-handoff: the owner refuses the commit with the retryable code, and `Transact` re-runs `fn` after a randomized backoff WITHOUT consuming a conflict attempt, bounded by `transactUnavailableTimeout` (default 30s, generous enough to ride out a freeze). (2) `codes.FailedPrecondition` - the reshard cutover re-pin signal: when a forwarded `CommitCAS` lands on the node that JUST lost ownership of `pinKey` across the FLIP/redistribution, the owner refuses WITHOUT applying ("re-pin against the current ring", the same ring-refresh loop-guard reads already retry across the staggered-generation window). Because the commit re-resolves the owner from the LIVE ring on every attempt, a re-run lands on the NEW owner and commits. `Transact` therefore treats `codes.FailedPrecondition` from a commit identically to `codes.Unavailable`: re-run `fn` after backoff, no conflict attempt spent, bounded by the same `transactUnavailableTimeout`. Past that deadline either code surfaces as-is (still a retryable status the caller may handle). The net effect: a `Transact` spanning a reshard barrier commits exactly once at the new generation, transparently, never returning the bare re-pin error to the caller. (A NON-transient `codes.FailedPrecondition` - a genuine cross-shard guard violation - never reaches this path: the cross-shard guard fires inside `fn` as `backend.ErrCrossShard` and aborts immediately, before any commit.)

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

Single-key `Put` replicates to R replicas when `ReplicationFactor > 1` (primary + R-1 successors, ack-counted per `WriteConsistency`). The OCC commit followed the same R-replica model one minor version later: v0.6 shipped the commit at R=1 (write-set on the owner's local Backend only), and v0.6.x replicates the committed write-set to R replicas (this change).

**Decision for v0.6: the OCC commit executed at R=1 on the owner.** The validate-and-apply path is the foundational primitive; it is cleanly testable on its own (validation, conflict detection, read-your-writes, cross-shard guard, retry loop) without dragging in fan-out / ack accounting / LWW reconciliation. Write-set replication composes cleanly ON TOP and does not change the `CommitCAS` protocol: it is a separate additive step after the owner's `tx.Commit()` succeeds. v0.6.x adds exactly that step.

**Known gap.** With a fast-ack Backend (slate `AwaitDurable=false`, the hostthis default) and the R=1 commit path, a committed write-set sits in the owner's loss window with no replica copy. Operators running transactions against a fast-ack backend should know that, unlike single-key Puts, transactional writes were not replicated in v0.6 proper. v0.6.x closes this.

#### Write-set replication (v0.6.x)

v0.6.x makes a CAS commit replicate its write-set to the shard's R replicas so transactional writes survive a single-replica loss, the "R for durability" property that pairs with a fast-ack backend. It does NOT change the `CommitCAS` protocol: it changes how `CommitCASApply` runs on the owner, and adds one new RPC (`ApplyBatch`) for the owner-to-replica fan-out.

The crux is the **envelope split** already in place for single-key writes (see "Replication (v0.4+) -> Value envelope"): at `ReplicationFactor > 1` the Backend stores ENCODED `Envelope` bytes (stamp + payload), not raw values; at `R == 1` it stores raw values. The v0.6 CAS path did raw `tx.Get` / `tx.Put`, which is correct only at R=1. At R>1 it was wrong two ways: (a) `tx.Get` returns the stored envelope bytes, but a `ReadCheck`'s `expected_value` is the DECODED payload the client saw (the client read path, `getReplicated`, already returns decoded payloads), so a byte-for-byte compare always mismatches; (b) `tx.Put` of a raw value corrupts reads, since `getReplicated` expects an envelope. v0.6.x makes `CommitCASApply` envelope-aware at R>1 while leaving R=1 byte-for-byte unchanged.

**R == 1 (no ring, or `ReplicationFactor` 1): UNCHANGED.** Raw values, no envelopes, no fan-out, exactly the v0.6 path. This is the contract for every existing single-node deploy: a regression here would corrupt or fail every non-replicated transaction. The decode-on-validate / encode-on-apply / fan-out logic is gated entirely behind R>1.

**R > 1: envelope-aware validate-and-apply, then fan out.** Under the same owner-local `backend.Transaction` as v0.6:

  1. **Validate (decode-on-read).** For each `ReadCheck`, `tx.Get` returns the stored envelope; `Decode` it before comparing. A winning **tombstone** envelope (empty payload) counts as not-found: it satisfies `expect_absent`, and conflicts a value-match check (the key the client saw is gone). Otherwise compare the decoded payload to `expected_value` byte-for-byte. Conflict semantics are identical to R=1; only the decode step is inserted.
  2. **Apply (shared-stamp encode-on-write).** Compute ONE shared `Stamp{now, owner NodeID}` for the whole commit. For each `WriteOp`, build an `Envelope` (Put -> payload = value; Delete -> empty payload tombstone) with that shared stamp, `Encode` it, and `tx.Put(key, envBytes)` into the local tx. Delete is written as a tombstone-envelope Put, NOT `tx.Delete`, so `getReplicated`'s LWW comparator sees a stamped tombstone (a bare key-removal would lose to a stale stamped value on another replica). Commit the local tx: atomic on the owner.
  3. **Replicate (fan out the SAME envelopes).** After the local commit returns nil, fan out the identical encoded envelopes to the R-1 OTHER replicas via `ApplyBatch` (one call per replica carrying the whole write-set). Reuse `fanout` + `requiredWriteAcks`; wait for W total acks. **The owner's own local commit counts as 1 ack.** So `ApplyBatch` only needs W-1 more replica acks; under `WriteOne` (W=1) the local commit alone satisfies W and no replica ack is required to return success (the fan-out still runs for durability, best-effort). Migration-guard rejections from a mid-handoff replica are transient (don't count toward acks or the failure budget), same as single-key Put. Each replica applies the batch **apply-if-newer** (see "LWW on write"), so a fan-out that arrives out of order relative to a later commit's fan-out self-resolves: the older shared stamp loses the per-key compare and is a no-op.

The owner's local commit plus the replica fan-out mirror `putReplicated`, except the owner side is a transactional validate-and-apply rather than a single Put, and the owner's commit is pre-counted as one of the W acks.

**Lock scope: `casCommitMu` covers validate + local commit only (v0.7+).** The serialization lock is held across steps 1-2 (read-set validation through the owner's local `tx.Commit`) and is **RELEASED before the step-3 fan-out.** Establishing OCC order is the local commit's job: two commits cannot both pass validation against the same observed value and both apply, because the second to acquire the lock re-validates against the first's already-committed write. Once the local commit fixes the order, the fan-out no longer needs the lock: reordered fan-outs arriving at a replica self-resolve via apply-if-newer (the older shared stamp loses on every key). This restores the property CAS was designed for, **no lock held across a network round-trip**: v0.6.x had to hold `casCommitMu` across the whole fan-out precisely because replicas applied verbatim, so an older commit's fan-out could clobber a newer one on a replica; apply-if-newer removes that hazard and lets the lock release at the local commit boundary. The fan-out remains best-effort-to-W and runs after release; an under-W result is still surfaced (the write is already durable on the owner + acked replicas).

**The shared commit stamp.** Every write in one CAS commit carries the SAME `Stamp`, so the whole write-set replicates and LWW-resolves as a unit: a later single-key Put or a later CAS commit with a greater stamp wins uniformly across all the keys, and a concurrent write with a lesser stamp loses uniformly. A per-op stamp would let LWW split a single transaction's keys across two winners on a laggy replica, breaking the atomicity the OCC commit just established.

**Atomicity boundary (same model as single-key Put).** The CAS commit is atomic ON THE OWNER: the owner-local tx either commits the whole write-set or none of it. Replication is best-effort-to-W AFTER that local commit, exactly like `putReplicated`. If fewer than W acks land, `CommitCASApply` returns an error (`codes.Unavailable`), but the write is ALREADY durable on the owner and on however many replicas did ack. This is the same success-but-under-W shape a single-key Put already has: the operator's `WriteConsistency` choice governs the durability guarantee, and an under-W result means the write landed on fewer than W replicas, not that it was rolled back. Each replica applies the whole batch atomically through its OWN local `backend.Transaction` (begin, apply all entries, commit), so a replica holds either the entire write-set or none of it. There is no 2PC across replicas: replicas are apply-only.

**Validation soundness at R>1 (owner-local validation).** The read-set is validated against the OWNER's local copy, not a full `getReplicated` quorum read. This is sound because the owner is always one of the R replicas AND is the write-routing target for this shard: every committed write to this shard (single-key Put fan-out and CAS commit alike) lands on the owner's local copy, so the owner's local copy reflects this shard's committed state. The assumption is exactly that the owner is a replica and a write target, which the ring guarantees for the pin key's owner. The heavier alternative, validating via a quorum read across replicas, would tolerate a stale-owner window during a ring move at the cost of R-fan-out reads inside the commit critical section; it is deferred. v0.6.x ships owner-local validation; quorum-read validation is a forward option, not a later deliverable.

Owner-local validation is sound under `ReadConsistency=Quorum` / `All` as well, but **only because of LWW-on-write (v0.7+)**. The hazard it closes: under `Quorum` / `All` an async read-repair scheduled from an OLD read could, in v0.6.x, fire after a newer commit and overwrite the owner's own local copy with the stale envelope (replica-receiving writes applied verbatim, and the owner's local copy is a replica). A later CAS validate-and-apply would then read that stale value and miss a real conflict, losing an update. With apply-if-newer the repair's older stamp loses the compare and the owner's newer value survives, so the owner's local copy always reflects the shard's newest committed write regardless of read consistency. Before v0.7 this soundness held cleanly only under `Nearest` (which issues no read-repair).

#### `ApplyBatch` wire protocol (v0.6.x)

A new unary RPC on `ShaleNode`, used only owner-to-replica for CAS write-set fan-out:

```
rpc ApplyBatch(ApplyBatchRequest) returns (ApplyBatchResponse);
```

```
message ApplyBatchRequest {
  repeated EnvelopeWrite writes = 1;
}

message EnvelopeWrite {
  bytes key      = 1;
  bytes envelope = 2;  // already Encode()d by the owner (shared stamp + payload);
                       // an empty-payload envelope is a tombstone. Written verbatim.
}

message ApplyBatchResponse {
  string error = 1;  // non-empty => the replica failed to apply (rolled back)
}
```

The envelope is opaque to the replica: the owner `Encode`d it (shared stamp + payload, tombstones included), so the replica writes the bytes verbatim and never re-stamps. The handler opens ONE local `backend.Transaction`, `tx.Put(key, envelope)` for each `EnvelopeWrite` (apply-only, no re-validation), and commits; any error rolls the whole batch back. It respects the migration guard the same way `dispatchReplicaPut` does: if a key in the batch is migrating or being received on this replica, it returns the migration-guard error (`codes.ResourceExhausted`) so the owner's `fanout` classifies it transient rather than a failure. There is no ownership re-check beyond the migration guard (the owner already validated; the replica trusts the fan-out target the same way `LocalReplicaPut` trusts `OwnsReplica`).

This is **implemented in v0.6.x.** v0.6 shipped the OCC commit at R=1 with full tests; v0.6.x adds envelope-aware CAS validate-and-apply plus `ApplyBatch` write-set replication so transactional writes get the same R-replica durability single-key writes already have.

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

## Per-shard lease-handoff storage (v0.8, PROPOSED)

Status: PROPOSED, not yet shipped. Everything above describes the current per-node model; this section is the v0.8 design. Domain types have landed in `pkg/storageunit` (pure, no I/O); the wiring phases are listed in the Roadmap.

### Why

The per-node model (one slatedb per node, all the node's keys in one LSM) makes a rebalance COPY the moved keys: the owning engine reads each key's merged value and streams it over gRPC to the receiver, which writes it into its own LSM. The bytes are already durable in object storage, but they live as one node's LSM files that only that node's engine can interpret, so they cannot be re-pointed. v0.8 makes rebalance copy-free: data has a permanent home in object storage; scaling moves OWNERSHIP, not bytes.

### Model

The keyspace is partitioned into a FIXED, power-of-two count N of STORAGE UNITS. A unit is `hash(ShardKey(key)) & (N-1)` (the low log2(N) bits of the same xxhash the ring uses). Each unit is a self-contained slatedb database under its own object-store prefix.

- **Co-location** is by ShardKey: co-located keys (a record and its versions) share a ShardKey tag, hence the same hash, hence the same unit, so a transaction stays inside one engine and CAS works. There is NO separate coarse-partition layer: the unit, derived directly from the shard-key hash, is the single routing + storage + lease granularity. This supersedes the per-node ring's 271-partition routing (in v0.8 the ring is re-keyed onto unit ids); co-location is achieved by hash tags, not by a shard->unit grouping.
- **Ownership** is a LEASE. The ring assigns units to nodes (N units; N must comfortably exceed the max node count). A node MOUNTS the units it owns, ~N/nodes of them.
- N is bounded ABOVE by per-engine memory (slatedb's memtable, default 64 MB, tunable to a few MB so many units fit a small node) and BELOW by the max node count. Pick N for the ceiling.

### Two maps, opposite natures

- **Data-location** (`unit -> object-store prefix`): STATIC. A unit's bytes have a permanent home; that permanence is what makes rebalance copy-free.
- **Ownership** (`unit -> node`): DYNAMIC. The ring; changes on every membership event. Rebalance re-runs it and hands leases over.

### Lease handoff (the single-writer reality)

slatedb is single-writer per database (writer-epoch fencing: opening at a higher epoch fences the prior writer). That is why concurrent writers need separate databases, and it is also the lease primitive. Handoff of unit U from node A to B: B is assigned U; A flushes U's memtable, stops writing U, releases; B opens U at epoch+1 (fencing any stale A writer) and serves U. Object-store bytes never move. During the brief release-to-acquire window U is momentarily unavailable (route reads to A until B acquires, or fail-fast and retry, for that one unit). Rebalance is a lease handoff per unit whose owner changed; the anti-entropy reconcile generalizes (a node the ring says owns U but has not MOUNTED, mounts it). The cross-node epoch source of truth is the durable lease state (the slatedb manifest writer-epoch), not in-process state.

### Resharding: grow by doubling, online, no downtime

When N units across N nodes is maxed and you must grow, DOUBLE: N -> 2N. Because `hash mod 2N = (hash mod N) + N*(one more hash bit)`, every unit K bisects into EXACTLY units K and K+N by a single additional hash bit. No global re-partition. Online, per-unit: for a live unit K, background-create K and K+N and stream K's keys into them by the new bit (K keeps serving); near catch-up, a brief write-pause for K's keys only, atomically flip routing for K's key-space, retire K. March through all N units one at a time. The split copies one unit's data once (per-unit, parallelizable, bounded, online); the routing state added is tiny (a cluster generation that takes N -> 2N plus a per-old-unit cut-over flag). A unit's STORAGE IDENTITY is qualified by that generation - `(generation, UnitID)`, not bare `UnitID` - so the gen-g unit K (count N) and the gen-(g+1) unit K (count 2N) are DISTINCT databases that can coexist during the bisect; see "v0.8 Phase 4" for the full identity + routing + safety design. This buys online elastic growth without the dynamic range-catalog of range-based sharding, keeping hash's even distribution. Tradeoff vs range-based: doubling-only granularity and no per-range hotspot splitting (neither needed for hash-uniform keys).

### Migration from per-node

One-time, standalone op tool (additive, dry-run on a bucket copy first): read each node's single LSM and re-write every key into its destination unit's database. After it, the per-node dbs retire and units are the source of truth.

### Constraints

- Memtable memory dominates per-engine cost (slatedb default 64 MB, tunable). N engines per node = N x memtable, so shrink the memtable for many units. The small-memtable downside (more frequent SST flushes) only bites at high write volume.
- Single-writer per unit is preserved end to end. Cross-unit transactions are unsupported; ShardKey co-location keeps a transaction inside one unit.

### v0.8 Phase 2: multi-backend node (static routing)

Phase 2 wires the `pkg/storageunit` domain into the cluster as a MULTI-BACKEND node with a STATIC topology. It mirrors how v0.2 did static routing before v0.3 added rebalance: the lease handoff on membership change is OUT of scope (Phase 3).

- **Two config modes, mutually exclusive.** The legacy single `Config.Backend` is the per-node mode and stays the default, unchanged byte-for-byte. Phase 2 adds `Config.BackendFactory` (`storageunit.BackendFactory`) + `Config.UnitCount` (`storageunit.UnitCount`); when BOTH are set the cluster runs in multi-backend mode. `Open` validates the XOR: factory+unitcount OR backend, never both and never neither, erroring clearly otherwise.
- **Unit ownership via the SAME ring.** Unit U is owned by `ring.LocateKey(unitID-bytes(U)).ID`. A bare unit id carries no `{...}` tag, so it hashes whole; units are fed through the one existing ring (no second ring). The cluster builds a `storageunit.OwnerLookup` from its ring and uses it for every unit owner question.
- **Mount owned units on Open.** Derive the node's owned units with `storageunit.OwnedUnits(self, UnitCount, ownerLookup)`, then `factory.OpenUnit(u, epoch)` each into a `unit -> backend.Backend` mount map the cluster holds. Real epoch fencing is a Phase-3 concern: Phase 2 opens at a fixed/zero epoch (clear TODO, no durable epoch logic invented here).
- **Routing: key -> unit -> owner -> mount-or-forward.** For every op (Put/Get/Delete/ScanPrefix and the CAS/commit path): `shardKey = ring.ShardKey(key)` (the SAME extraction the ring uses, so co-location holds), `unit = storageunit.UnitForShardKey(shardKey, UnitCount)`, `owner = ownerLookup(unit)`. If owner == self, apply against `mountMap[unit]` (NOT a single `c.backend`); else forward to the owner over the existing gRPC path. The forwarded op carries the key; the receiver re-derives the unit and applies against its own mount map.
- **Unit-based owner guard.** The forwarding loop-guard (`OwnsKey` / the forwarded-but-not-mine refusal) becomes unit-based: a forwarded op whose unit this node has not mounted is REFUSED, not re-forwarded. On `Close`, close every mounted unit (`factory.CloseUnit` each) before the node shuts down.

**IN SCOPE: static topology.** The unit set a node owns is fixed at Open. If membership changes mid-run, Phase 2 may serve stale ownership for the moved units; that is acceptable and documented, exactly as v0.2 served stale routing before v0.3. **OUT OF SCOPE (later phases):** rebalance / lease handoff and mount-unmount on membership change (Phase 3); epoch fencing / writer-epoch handoff (Phase 3, opened at a fixed epoch here); doubling / resharding and the migration tool (Phase 4+). The legacy per-node path is untouched.

### v0.8 Phase 2b: R>1 (replicated) multi-backend, static topology

Phase 2 (and Phases 3, 4) are single-replica (R=1): each unit has exactly ONE durable database and ONE owner; `ApplyBatch` is refused in multi-backend mode with "unsupported in multi-backend mode (single-replica)". Phase 2b extends Phase 2 to R>1 (replicated): each unit has R INDEPENDENT replicas, each a separate database in shared storage, mounted on R different nodes. A node crash is fully recoverable (a surviving replica is a complete independent copy of the unit), and a node leaving cannot orphan a unit's only copy. This mirrors exactly how v0.2 did static routing before v0.3 added rebalance: the unit -> replica-set assignment is FIXED at Open; membership-change handoff at R>1 is OUT of scope (a later phase). It reuses the legacy R>1 write/read/apply machinery verbatim, re-keyed from per-NODE ownership to per-UNIT ownership.

- **The ring yields R replica nodes per unit.** Phase 2 resolved a unit's single owner with `ring.LocateKey(genUnitBytes(gu))` (`genUnitOwner` / `unitOwnerOf`). Phase 2b resolves a unit's REPLICA SET with `ring.LocateKeyN(genUnitBytes(gu), R)`: the primary plus its (R-1) ring successors, the SAME successor-chain machinery the legacy per-node R>1 path uses, but hashing the generation-qualified unit id instead of the raw key's shard key. A new `unitReplicas(gu) []ring.Member` in the cluster layer wraps this lookup; `genUnitOwner` / `unitOwnerOf` stay R=1-exact (Phase 2b never narrows them) and the new replica-set lookup is additive. Co-location is preserved by construction: every key in a `{tag}` set hashes to one unit, and that unit has one replica set, so the set's whole replica placement is identical.

- **`OwnedUnits` generalizes to (unit, replica-position).** Phase 2 derived "which units do I mount" with `storageunit.OwnedUnits(self, c, owners)`, which keeps a unit iff `owners.OwnerOf(u) == self` (the SINGLE owner). Phase 2b generalizes this to keep a unit iff self appears ANYWHERE in the unit's R-member replica set, PAIRED with the position self holds. The pure domain stays pure: `pkg/storageunit` gains a `ReplicaLookup` interface (`ReplicasOf(u UnitID) []NodeID`, the ordered replica set) alongside the existing `OwnerLookup`, and `OwnedReplicaUnits(self, c, replicas) []OwnedReplica` (the R>1 analogue of `OwnedUnits`: enumerate 0..N-1, keep every unit whose replica set contains self, recording self's index as the `OwnedReplica{Unit, Replica}` position). `OwnerLookup` / `OwnedUnits` are UNCHANGED (R=1 still uses them). The cluster has a SEPARATE generation-aware derivation `desiredReplicaUnits() []ReplicaUnit` for the R>1 mount (it qualifies each `OwnedReplica` with the live generation and resolves the replica set via `unitReplicas(gu)`, the ring `LocateKeyN` over `genUnitBytes(gu)`); the R=1 `desiredGenUnits` is left byte-for-byte the Phase 2/3 single-owner derivation.

- **Each replica is an INDEPENDENT durable database.** This is the load-bearing factory contract. At R=1 a unit `gu` has ONE durable backing store, keyed by `GenUnit` alone (the lease moves between nodes, the bytes stay put: `sharedfactory.Backing.stores map[GenUnit]*memory.Memory`, and the deployable slate factory's one slatedb prefix per `GenUnit`). At R>1 the R replicas of `gu` are R SEPARATE databases that must each persist independently, so the durable identity is `(GenUnit, replica-position)`, NOT `GenUnit`. The domain models this as a `storageunit.ReplicaUnit{Unit GenUnit, Replica uint8}` value object, and the factory mounts a unit at a replica position via a CAPABILITY interface `storageunit.ReplicaBackendFactory` (`OpenReplicaUnit(ru, epoch)` / `CloseReplicaUnit(ru)`) that EXTENDS `BackendFactory`. R>1 support is opt-in: a factory advertises it by implementing the extension; the R=1 factory contract (one durable store per `GenUnit`, copy-free handoff) is UNCHANGED, so single-replica factories (and the deployable slate factory, until it adopts the extension) are not forced to grow a replica argument they do not use. The cluster validates at Open that the factory implements `ReplicaBackendFactory` when R>1 (a single-replica factory at R>1 is a config error, not a runtime nil-deref). Concretely the test `sharedfactory.Backing` keys a SECOND pair of maps (`replicaStores` / `replicaEpochs`) by `ReplicaUnit` (so replica 0 and replica 1 of one unit are distinct stores with distinct fences that survive each other), and the deployable slate factory would derive R distinct DbName prefixes per `GenUnit`. The node-side mount map stays `mountMap[GenUnit]backend.Backend` (a node holds AT MOST ONE position per unit, because a replica set is distinct members, so the per-node mount is unambiguous keyed by `GenUnit`); the cluster carries a parallel `replicaPos map[GenUnit]uint8` recording the position this node holds each unit at, so Close releases the right per-replica database. The durable-per-replica identity lives in the factory; the node mount stays `GenUnit`-keyed.

- **Write path: key -> unit -> fan out to the unit's R replicas -> apply-if-newer into each replica's mount.** The originator resolves `replicas = unitReplicas(genUnitForKey(key))` (the unit's R replica nodes) and fans the write out exactly as the legacy `putReplicated` does, but over the UNIT's replica set rather than `ring.LocateKeyN(shardKey(key), R)`. Reuse verbatim: `fanout` (the ack/failure-budget engine), `requiredWriteAcks` (W per `WriteConsistency`), the `Stamp` + `Encode(Envelope{...})` stamping, the `WriteTimeout`-bounded dispatch + surplus drainer. The ONLY new code is a multi-backend dispatcher: where legacy `dispatchReplicaPut` applies locally via `c.applyEnvelopeIfNewer(key, env)` (which writes the single `c.backend`) and remotely via `cli.PutForwarded`, the multi-backend dispatcher applies locally via a new `applyEnvelopeIfNewerToUnit(key, env)` that resolves the key's mounted unit backend (`localWriteBackendForKey`, already used by the R=1 multi write path, which holds the per-unit write-pause read-lock and returns the mounted `backend.Backend`) and runs the SAME never-clobber apply-if-newer compare against THAT backend's transaction (`txApplyIfNewer` + `tx.Put`), and remotely dispatches `cli.PutForwarded` to the replica node, whose RPC handler lands in `LocalReplicaPut`. `LocalReplicaPut`'s multi-backend branch already resolves `localWriteBackendForKey` and writes the mounted unit; Phase 2b changes its multi branch to apply the incoming envelope APPLY-IF-NEWER against that unit (the bytes are an LWW envelope at R>1) instead of the raw `b.Put` it does at R=1. Owner-but-unmounted (handoff window) and stale-mount eviction stay exactly as Phase 3 built them: the retryable `errUnitAcquiring` and `evictStaleMount` are unchanged, so a not-yet-mounted replica returns the transient code the fan-out tolerates. Delete is a tombstone (empty-payload stamped envelope) through the same path, identical to legacy.

- **CAS write-set fan-out re-keyed per unit.** `CommitCASApply` at R>1 already encodes one shared-stamp envelope per write-op and, after the owner-local commit, fans the batch out via `replicateCASBatch`. In multi-backend mode the owner-local commit goes through `localBeginForKey(pinKey, level)` (already the multi path: it opens a tx on the pin key's mounted unit). `replicateCASBatch` is re-keyed: the replica set is `unitReplicas(genUnitForKey(pinKey))` instead of `ring.LocateKeyN(shardKey(pinKey), R)`; the per-replica dispatch (`dispatchApplyBatch`) sends the batch to each replica node's `ApplyBatch` RPC -> `ApplyBatchLocal`. `ApplyBatchLocal`'s multi-backend guard (the current hard refusal) is REPLACED by the real per-unit apply: open one transaction on the batch's mounted unit backend (`localWriteBackendForKey` on the pin key, or per-key the unit each `EnvelopeWrite.Key` resolves to - the cross-shard guard guarantees they co-shard, so one unit covers the batch) and run the existing apply-if-newer loop (`txApplyIfNewer` + `tx.Put`) inside it, under `c.applyMu`, committing the whole batch together. The R=1 multi guard (refuse) is removed only on the R>1 path; an R=1 multi cluster never sends `ApplyBatch` and the refusal can stay for that case, or fold into the `casReplicated()`-style predicate.

- **Read path: ReadConsistency across the unit's replica nodes.** `getReplicated` already reads from N-of-R replicas per `ReadConsistency` (1 / quorum / R), picks the LWW max-stamp winner, and read-repairs laggards. In multi-backend mode the replica set is `unitReplicas(genUnitForKey(key))` instead of `ring.LocateKeyN(shardKey(key), R)`; the per-replica read dispatch (`dispatchReplicaGet`) reads locally via the key's mounted unit backend (`localBackendForKey`, already used by the R=1 multi `Get`) instead of `c.backend.Get`, and remotely via `cli.GetForwarded` (whose handler serves from the replica's mounted unit via `LocalGet`, already multi-aware). The LWW winner selection, the `ReadTimeout`-bounded dispatch, read-repair (which rides the same multi-backend `dispatchReplicaPut`, so a stale repair is a never-clobber no-op against the mounted unit), and tombstone -> `ErrNotFound` are all reused verbatim. A replica node that has not mounted the unit returns the transient acquiring-window error, which the read fan-out skips while another replica satisfies the consistency target.

- **Top-level dispatch.** `Put` / `Get` / `Delete` / `CommitCASApply` gain a multi-backend R>1 branch BEFORE the current `c.multi` R=1 branch: a new predicate `c.multiReplicated()` (the multi analogue of `casReplicated()`: `c.multi && replicationFactor() > 1 && ring populated`) routes to the per-unit replicated paths above. When `c.multi` but R=1, the existing Phase 2/3/4 single-mount paths run unchanged. When not `c.multi`, the legacy per-node paths run unchanged. The three modes are mutually exclusive and each is byte-for-byte preserved outside its own branch.

- **NO ACKED WRITE LOST (how the invariant holds).** Identical structure to the legacy R>1 proof, now per unit. (1) FAN-OUT ACK ACCOUNTING: a write is acked to the client only after W of the unit's R replicas have durably applied it (W per `WriteConsistency`, the owner's local commit counted as one ack, the same `requiredWriteAcks` math). Fewer than W acks returns `codes.Unavailable` and the write is NOT acked (the client retries; Phase 2d makes that retry internal + bounded). A replica mid-acquire returns the sentinel-tagged transient `errUnitAcquiring`, which (per the Phase 2d budget fix) counts toward neither acks nor the failure budget, so a single not-yet-mounted replica cannot fail an otherwise-satisfiable W. (2) NEVER-CLOBBER APPLY: every replica applies APPLY-IF-NEWER (`txApplyIfNewer`: write only if the incoming stamp strictly beats the stored stamp), so a reordered older write or a stale read-repair self-resolves to a no-op and an acked write is never overwritten by a staler one. (3) INDEPENDENCE: the R replicas are independent durable databases, so the loss of one node (hence one replica) leaves W-1 or more complete copies; any acked write reached W replicas, so at least W-1 surviving replicas still hold it and a quorum read finds it. The static-topology boundary (next) means the replica SET never changes mid-run, so no acked write can be stranded on a replica the ring later stops counting.

- **DDD shape.** `pkg/storageunit` stays PURE (no I/O): it gains `ReplicaLookup` (the ordered replica-set analogue of `OwnerLookup`), `OwnedReplicaUnits` / `OwnedReplica` (the R>1 analogue of `OwnedUnits`), the `ReplicaUnit{Unit GenUnit, Replica uint8}` value object for the durable per-replica identity, and the `ReplicaBackendFactory` capability interface (the R>1 factory seam) - all plain data + pure interfaces, trivially testable without a ring or a factory. The R>1-per-unit WIRING lives entirely in the cluster layer (`multibackend_replicated.go`): `unitReplicas` (ring `LocateKeyN` over the unit id), the per-unit write/read paths (`putReplicatedUnit` / `getReplicatedUnit`, structural mirrors of `putReplicated` / `getReplicated`), their multi-backend dispatchers (`dispatchReplicaPutUnit` / `dispatchReplicaGetUnit` / `applyEnvelopeIfNewerToUnit` / `applyBatchToUnit` / `scheduleReadRepairUnit`), the re-keyed `replicateCASBatch` replica-set resolution, and the `multiReplicated()` predicate. The envelope / apply-if-newer / quorum / fan-out PRIMITIVES are REUSED, not forked: `fanout`, `txApplyIfNewer`, `Encode`/`Decode`, `requiredWriteAcks` / `requiredReadReplicas`, the `collected` LWW-winner shape, the `repairWG` / `repairCtx` lifecycle. (The per-unit write/read paths are thin structural mirrors of the legacy node-keyed ones - the same fan-out engine, the only change being the replica set is the UNIT's and the local apply lands in the mounted unit backend - kept as separate functions so the legacy per-node path stays byte-for-byte untouched.) The factory contract (per-replica durable identity) is an infrastructure-adapter change in `internal/sharedfactory` (and a future deployable slate factory), depending on the domain's `ReplicaUnit` / `ReplicaBackendFactory`, never the reverse.

**THE R>1 MULTI-BACKEND LOSSLESS GATE (the data-loss oracle).** A new in-process integration gate (`tests/integration/lossless_multibackend_r2_gate_test.go`) stands up a 3-node multi-backend cluster at R=2 on a per-replica shared-backing factory, writes a recorded BASELINE dataset spanning every unit (including co-located `{tag}` sets), then runs a CONCURRENT PROBE that keeps acking new keys THROUGH THE FULL ROUTED SURFACE (writers rotate the entry node so writes route both locally and forwarded; a key is recorded ONLY once its Put returns nil), folding those acked keys into the recorded set. It then asserts: (1) THE ORACLE - every baseline key AND every acked probe key is readable with its EXACT value from EVERY node (forwarding to a replica), zero loss; (2) each unit is mounted on EXACTLY R=2 distinct nodes (the replica set), and a write landed on both; (3) co-located sets share one unit hence one replica set; (4) SURVIVE-ONE-LOSS - wiping ONE replica copy of each unit still reads every value (the surviving replica is a complete copy). It is kept honest by a BREAK DEMONSTRATION (`TestR2GateCatchesLostWrite`) that deliberately drops the fan-out to one replica (or wipes one replica's store) AND removes the other replica's copy, then shows the oracle FAILS (catches the lost write) - proving the gate is not a rubber stamp, exactly like the Phase 3 / Phase 4 gates. (Surviving a single replica loss is asserted positively; the break removes BOTH copies so the loss is genuine.) Recording ONLY acked keys is what keeps the oracle honest: a write the cluster never acknowledged is not owed durability, so an unacked-then-lost key cannot mask a genuine loss, and a key the probe DID get an ack for that then fails the readback is a real violation the oracle must catch.

- **Initial-convergence mount (NOT a handoff).** A node's replica set is derived from the FULL ring, which is not known until membership converges (the founder boots alone, peers join over the next moment). So a node mounts its replica copies as the ring fills: the existing reconcile machinery, with an R>1 branch (`reconcileReplicaUnits`) whose desired set is `OwnedReplicaUnits` against the live ring, opening each newly-desired unit at its replica POSITION (an independent durable database) and releasing ones it no longer replicates. This is NOT a lease handoff and NOT re-replication: the R>1 replica copies are independent durable databases, so a node simply OPENS the copies the static ring assigns it (no bytes move between nodes, no copy). Once the cluster has converged the topology is static; a later phase adds true membership-change re-replication. Because the gate writes only AFTER 3-node convergence, every acked write lands on a fully-resolved replica set.

**IN SCOPE: R>1 replicated multi-backend under a STATIC unit -> replica-set topology** fixed once membership converges at Open, reusing the legacy R>1 write/read/apply machinery re-keyed per unit. **OUT OF SCOPE (later phases):** membership-change lease handoff / re-replication at R>1 AFTER convergence (the replica set is static post-convergence; a node join/leave once the cluster is formed re-mounts a reassigned replica's EXISTING shared-storage copy, and Phase 2d below keeps writes AVAILABLE across that re-mount, but standing up a brand-new R-th copy by re-replicating bytes onto a member that has none is still a distinct phase); resharding / doubling at R>1 (Phase 4 is R=1 only); anti-entropy re-replication to restore R copies after a permanent node loss. The legacy per-node R>1 path (keyed to node ownership) is UNCHANGED byte-for-byte; the R=1 multi-backend paths (Phases 2/3/4) are UNCHANGED outside their own branches.

### v0.8 Phase 2d: write-availability through an R>1 membership change (retry-on-acquiring)

Phase 2b mounts a node's replica copies on the INITIAL convergence and then assumes the unit -> replica-set topology is STATIC. Phase 2d removes that assumption for the WRITE-AVAILABILITY axis: a unit whose replica assignment moves while the cluster is live (scale 3 -> 12, a node leaving) is RE-MOUNTED by `reconcileReplicaUnits`, and a write that lands on a replica mid-mount no longer ERRORS - it WAITS (bounded latency) for the mount to finish, then succeeds. The durability and single-writer invariants are unchanged; only the availability cost of the handoff window is removed.

**The mechanism it fixes (measured, not theoretical).** During an R>1 membership change `reconcileReplicaUnits` (Phase 2b) reassigns units: for each unit whose desired replica POSITION on this node changed (or that this node newly replicates), it RELEASES the old mount and ACQUIRES the new one (`OpenReplicaUnit`, which mounts the per-(unit, replica) database from shared storage). Until the acquire finishes, a routed op for that replica returns `errUnitAcquiring`. The Phase 2b comment claims this transient "counts toward NEITHER the ack nor the failure budget" - but it does not, and that gap IS the bug:

  - `errUnitAcquiring` carries `codes.Unavailable` (multibackend_rebalance.go).
  - The fan-out's `isTransientReplicaErr` (replicate.go) classifies ONLY `codes.ResourceExhausted` as transient. `codes.Unavailable` is deliberately NOT transient: it is the canonical "peer is down" code, and the failure-budget short-circuit MUST count a down peer as a failure so a real outage fails fast instead of hanging on every replica.
  - So a mid-acquire replica's `Unavailable` counts toward the FAILURE budget. At R=2, `WriteQuorum` gives `W = requiredWriteAcks = 2`, so the failure budget is `R - W = 0`: ONE acquiring replica is a budget-exhausting failure, and the write returns `codes.Unavailable` to the client immediately. During a big membership change MANY units are simultaneously mid-acquire, so a large fraction of writes error. Empirically (staging chaos, scale 3 -> 12 at R=2): only ~52-64% of writes acked, the rest erroring. ZERO acked writes were ever lost (durability already holds: the new owner opens the SAME shared-storage replica database, so it sees every durable-before-ack write); the errors are pure AVAILABILITY cost. This is the same class of refusal at R=1 multi-backend (`Put` returning `errUnitAcquiring` directly to the caller, or `putForwarded` propagating it), so the fix benefits R=1 AND R>1.

**The choice: Option A (retry-on-acquiring), NOT Option B (overlap / lease-handoff).** Two designs were considered. (A) RETRY-ON-ACQUIRING: when an op would fail because a replica is mid-acquire, the cluster RETRIES internally with backoff, bounded by `WriteTimeout`, so the handoff blip becomes bounded LATENCY instead of an error. (B) OVERLAP / LEASE-HANDOFF: the new owner pre-mounts and catches up while the old owner keeps serving, ownership flips atomically only once the new owner is ready, so there is never a window with fewer than W ready replicas (eliminating even the latency). **Phase 2d picks A.** Why:

  - **The acceptance bar is an ack-rate jump, and A clears it.** The staging chaos judge wants the scale-3->12 ack rate to go from ~50% to ~100% with zero acked loss. A acquire completes in the time to open a slatedb manifest from object storage (sub-second typically); riding that out under the 5s `WriteTimeout` budget converts essentially every handoff-window refusal into a successful (slightly slower) write. The residual error case - an acquire so slow it exhausts `WriteTimeout` - is an acceptable degraded outcome that A shares with any timeout-bounded write, and it is rare.
  - **A is far smaller and touches no fencing / ordering.** A is a retry wrapper around the EXISTING refusal signal; it does not reorder acquire-vs-release, does not change owner selection, does not touch the epoch fence. B rewrites the reconcile ordering (acquire-before-release), the routing's owner preference, AND the cutover fencing - a much larger, data-loss-sensitive change. The project rule is "the staging test is the judge, and the smaller change that passes it wins." A is that change.
  - **B is recorded as a future optimization.** If, after A ships, the staging chaos still shows a latency tail an operator cares about (writes that succeed but slowly during the churn), B is the follow-up that removes the latency. A and B compose: A is correct on its own, and B later just shrinks the window A waits on.

**Where the retry slots in (two complementary layers, both bounded by `WriteTimeout`).** The acquiring-window refusal can surface at two points in the write path, and Phase 2d handles both:

  1. **Inside the fan-out: make `errUnitAcquiring` actually transient (the budget fix).** The Phase 2b invariant that a mid-acquire replica "counts toward neither budget" is made TRUE by giving the fan-out a way to recognize the acquiring-window code. `errUnitAcquiring` keeps `codes.Unavailable` ON THE WIRE (it is the retryable code the client/forwarding shape already understands, and Phase 3 / the freeze barrier depend on that), but it is tagged so `isTransientReplicaErr` can distinguish "this replica is mid-acquire, skip it and wait for the budget to refill" from "this replica is down, count it." The chosen tag is a SENTINEL wrapped error: `errUnitAcquiring` wraps a package-private `errAcquiringSentinel`, and `isTransientReplicaErr` returns true when `errors.Is(err, errAcquiringSentinel)` OR the gRPC status is `codes.ResourceExhausted`. The sentinel survives the local-self dispatch path verbatim (it is a Go error, not re-wrapped). For the REMOTE dispatch path the sentinel does not cross gRPC (only the status code does), so the cross-node acquiring refusal is ALSO emitted by the replica's RPC handler as `codes.ResourceExhausted` for forwarded replica writes (`LocalReplicaPut` / `ApplyBatchLocal`'s mid-acquire branch), the SAME transient code the migration guard already uses, which `isTransientReplicaErr` already skips. (The CLIENT-facing top-level refusal stays `codes.Unavailable` - only the INTERNAL replica-to-replica fan-out leg re-codes to the transient `ResourceExhausted`, exactly as the v0.3 rebalance migration guard does for the same reason.) With this, a single mid-acquire replica no longer consumes the R=2 failure budget: the fan-out waits for the OTHER replicas to satisfy W, and if the acquiring replica finishes its mount before `WriteTimeout` it lands as an ack.
  2. **Around the op: a bounded retry when the WHOLE fan-out cannot reach W.** Layer 1 fixes the common case (one of R replicas acquiring, the rest ack). But during a big churn it is possible for ENOUGH of a unit's replicas to be simultaneously mid-acquire that W acks are not reachable in one fan-out pass (e.g. R=2 with BOTH replicas re-mounting). Layer 2 catches this: when a replicated write returns the retryable acquiring/quorum-unavailable outcome, the Put / Delete / CommitCASApply surface RE-RUNS the fan-out after a short backoff, bounded by the SAME `WriteTimeout` wall-clock budget, so the write waits for the acquires to settle rather than erroring. The retry re-resolves the replica set from the LIVE ring on every attempt (so a write started mid-reassignment lands on the post-reassignment replica set once it settles), and re-stamps is NOT needed (the original `Stamp` is preserved across retries so apply-if-newer ordering is stable; a fresh attempt may re-stamp safely since the write was never acked). At R=1 multi-backend the same wrapper rides the single-owner `Put` / `putForwarded`: the owner-but-unmounted `errUnitAcquiring` is retried under `WriteTimeout` instead of returned, closing the R=1 gap the same way.

**Retry / timeout policy (precise).**

  - **Budget:** the ENTIRE retry sequence for one logical write is bounded by `WriteTimeout` (default 5s). The first attempt starts the clock; each retry checks remaining budget before sleeping and gives up (surfacing the last retryable error to the client) once the budget is exhausted. There is no separate retry-count cap; the wall-clock bound is the cap, so a fast-acquiring cluster retries a few times and a pathologically-slow one degrades to one timeout-bounded error per write (never a busy-spin, never a deadlock).
  - **Backoff:** start at `RebalanceRetryAfterMs` (default 50ms, the existing handoff retry-after hint), with a small exponential growth (x2) capped well under `WriteTimeout` (cap 500ms), plus jitter (50-100%) so a thundering herd of simultaneously-acquiring writes does not re-collide on the same retry tick. This reuses the same retry-after knob and backoff shape the v0.3 cutover / freeze-window client retries already use; it is not a new tuning surface.
  - **Only retryable outcomes retry.** A retry fires ONLY for the acquiring/quorum-unavailable retryable family (the sentinel-tagged `errUnitAcquiring`, the freeze-window `codes.Unavailable`, and the under-W `codes.Unavailable` that a fan-out returns when acquiring replicas kept it from reaching W). A NON-retryable failure (a genuine peer-down `Unavailable` with no acquiring replica in play, an empty-value rejection, a closed cluster, a decode error) is returned IMMEDIATELY, unretried - the retry must not paper over a real outage by spinning until `WriteTimeout`. Distinguishing "under-W because replicas are acquiring" from "under-W because replicas are DOWN" reuses layer 1's classification: if the fan-out's accumulated errors are all transient-acquiring (none counted toward the failure budget) yet W was not met, the outcome is retryable; if real peer-down failures exhausted the budget, it is not.

**How DURABILITY stays intact (NO ACKED WRITE LOST is preserved).** The retry changes only WHEN a write is acked, never WHETHER an acked write survives. (1) A write is still acked to the client ONLY after W of the unit's R replicas durably applied it; the retry just gives the fan-out more time to collect those W acks across the handoff window. (2) The new owner of a reassigned replica opens the SAME shared-storage per-(unit, replica) database the old owner flushed (durable-before-ack: every acked write is durable before the lease moves), so a write that the retry eventually lands on the freshly-mounted replica sees a database that already contains every previously-acked write - the apply-if-newer compare runs against the true durable state. (3) An un-acked write that times out during the churn is, as before, never reported as success, so the client retries it; no acked write is ever in the lost set. The Phase 2b R>1 lossless gate's oracle (record only acked keys, assert every acked key is readable) is the exact check that this holds, and it is EXTENDED by Phase 2d's new gate below.

**How the SINGLE-WRITER FENCE stays intact (no two writers on one (unit, replica) database).** The retry does NOT relax the epoch fence. A reassigned replica is acquired by `OpenReplicaUnit` at a strictly-higher epoch than the unit's durable lease (the same fence Phase 3 uses for R=1 handoff), which locks out any stale prior holder of that replica position before the new owner serves a single write. The retry only re-dispatches a write that was REFUSED (the acquiring replica explicitly did NOT apply it); it never dispatches the same write to two live holders of one (unit, replica) database, because at any instant exactly one node holds the highest-epoch lease for that position and only that node's mount answers a routed op (every other node either is not the ring owner - refused by the loop guard - or is mid-acquire and itself refuses with the retryable signal). A write that races the flip is refused on the losing side (stale-mount eviction -> `errUnitAcquiring`) and retried onto the winning side. So across the whole retry sequence a given (unit, replica) database has exactly one writer at a time; the retry adds latency, not a second writer.

**THE R>1 WRITE-AVAILABILITY-THROUGH-MEMBERSHIP-CHANGE GATE (the new acceptance test).** A new in-process loss-oracle test (`tests/integration/membership_change_write_availability_test.go`, in-process, sharedfactory + memory backend, NO slatedb tag, NO MinIO) is the acceptance bar for Phase 2d. It: (1) stands up an N-node multi-backend cluster at R=2 on the per-replica shared-backing factory and writes a recorded BASELINE; (2) starts a CONTINUOUS WRITER that keeps acking a tracked keyset through the routed surface, recording a key ONLY once its Put returns nil, and counting attempted-vs-acked; (3) BEGINS A MEMBERSHIP CHANGE (adds nodes / triggers a scale event) WHILE the writer runs, so writes land on units mid-reconcile; (4) asserts BOTH (a) THE ORACLE - every baseline key and every ACKED probe key is readable with its exact value from every node, ZERO acked loss; AND (b) THE ACK RATE - acked / attempted is HIGH (>95%; the bar the staging chaos expects to jump to ~100%). The test is kept honest by demonstrating it FAILS on the pre-fix code: with the retry removed (or `errUnitAcquiring` left counting as a failure), the same membership change drives the ack rate WELL below the threshold (matching the measured ~50-64%), so the gate proves the fix rather than rubber-stamping it. The oracle's record-only-acked discipline is what makes the two assertions independent: durability (a) must hold even at a low ack rate (the old behavior was lossless-but-unavailable), and the fix is the ack-rate (b) climbing to >95% WITHOUT regressing (a).

**IN SCOPE: write availability through an R>1 (and R=1) multi-backend membership change, via a `WriteTimeout`-bounded retry-on-acquiring** (layer 1 makes the acquiring refusal a true fan-out transient; layer 2 retries the whole op when W is momentarily unreachable). The handoff-window refusal becomes bounded LATENCY, not an error. **SUPERSEDED FOR THE BIG-CHURN CASE by Phase 2e (pending ranges) below:** Option A's availability is bounded by mount-time-vs-`WriteTimeout`. Measured on real 3-node staging: incremental scale 3 -> 4 stays ~100% (few units remount, fast), but big-bang 3 -> 12 (about 16 slatedb databases remounting at once, real MinIO mounts exceeding the 5s budget) drops to ~54% as the retry budget exhausts. Phase 2e removes that bound: during a transition the routed replica set is the UNION of the current owners (which still have the data mounted) and the pending owners, so a write/read always reaches a node that physically holds the position, and availability no longer depends on mount time at all. Option A is RETAINED as the safety net for the residual unserved instant Phase 2e leaves (a node crash mid-handoff, the pure-new-mount initial convergence); A and B compose (A is the belt, B shrinks the window A waits on to near zero). Also out of scope (unchanged from Phase 2b): re-replicating a unit's BYTES onto a newly-joined replica position that has no durable copy yet (the copy-free case - a node taking over an EXISTING replica position - is what 2d and 2e handle; standing up a brand-new R-th copy after a permanent node loss is the separate anti-entropy re-replication phase).

### v0.8 Phase 2e: pending ranges (graceful membership transition, R>1)

Phase 2d (Option A) keeps R>1 writes available across a membership change by RETRYING through the acquiring window, bounded by `WriteTimeout`. That makes availability a function of mount-time-vs-timeout: a single slatedb mount from MinIO can exceed the 5s budget under a big-bang scale-out (~16 databases remounting at once), and the measured big-bang 3 -> 12 ack rate falls to ~54%. Phase 2e removes the dependency on mount time entirely by adopting the canonical consistent-hash / gossip fix for a graceful membership transition: PENDING RANGES (Cassandra / ScyllaDB / Riak-Dynamo - shale's family; see `docs/research/graceful-scaledown-prior-art.md`, 24 sources). During a transition the routing layer DUAL-WRITES a position to BOTH its current owners AND its new (pending) owners, and READS from their UNION. A node that is LEAVING (gossiped `Draining`) STAYS a current owner - it keeps the data mounted and keeps serving - and is removed from a position's routed set ONLY AFTER its pending successor has durably MOUNTED that position (proven by the serving marker). Because the union always contains a node that PHYSICALLY HOLDS the position, both reads AND writes stay available throughout, independent of how long the pending owner's mount takes. A 30s mount is a non-event. The detailed design (the current/pending/union computation, sequence, package layout, the R/W crash matrix) is in `docs/design/overlap-handoff.md`; this section is the canonical behavior.

**THIS SUPERSEDES the earlier per-position-forwarding + draining-exclusion design (REMOVED).** An earlier draft of Phase 2e did the OPPOSITE of pending ranges and it is the fragile path the prior-art survey identifies: it EXCLUDED a draining node from the ownership ring the instant it set `Draining` (so the position's owner immediately became the new/pending owner), then relied on the new owner FORWARDING routed ops back to the old owner per position during the mount. That model is proven fragile (write availability held at ~99.7% via forwarding, but post-leave READBACK failed because the new owner was owned-but-unmounted and the read routed to it while the stale mount sat on a third node; the convergence window was unstable). Pending ranges fixes BOTH read and write because the routed UNION always contains a node that physically has the data, so no per-position back-forward, no `acquiringForwardTarget`, no `PredecessorAddr`, and no position-addressed forward wire change are needed. The following machinery is REMOVED (marked dead): the draining-exclusion in `reconcileRingFromMembership`; the per-position forward path (`acquiringForwardTarget` / `forwardPutToPredecessor` / `forwardGetToPredecessor` and the position-addressed forwarded-op proto field); the `Acquiring`-state forward behavior and the `PredecessorAddr` / `Predecessor` fields on `HandoffState`; the retained `priorDesiredReplicas` + `priorAddrs` snapshot used ONLY to identify a single-hop predecessor; the single-hop-scope fallback. The following are REUSED unchanged: the serving marker (`WriteServingMarker` / `ReadServingMarker`), the gossiped `Draining` Meta bit, the per-`(unit, replica)` `mountMap` re-keyed by `ReplicaUnit`, the durable per-replica epoch fence + apply-if-newer / LWW idempotency, `DrainForLeave`, and Option A's retry as the residual-case belt.

**The routing contract: current / pending / union replica sets.** Routing is computed per-op inside `replicasForKey(key)`. For a key's unit `gu = genUnitForKey(key)`:

  - **CURRENT replica set** = `unitReplicas(gu)` resolved over the ring INCLUDING draining members = `ring.LocateKeyN(genUnitBytes(gu), R)` with NO draining-exclusion. These are the nodes that own the position TODAY and have it mounted (the leaving node is here).
  - **PENDING replica set** = `unitReplicas(gu)` resolved over the ring EXCLUDING draining members = `LocateKeyN` over the same id but with every `Draining` member removed from the candidate set. These are the nodes that WILL own the position once the leaving node is gone (the successor that takes over the leaver's exact position is here).
  - **TRANSITION DETECTION** (answers open question (b)): a position is IN TRANSITION when CURRENT != PENDING, which arises from exactly two gossip-observable causes. (1) a LEAVE: some member of CURRENT is `Draining`, so excluding it changes the set (the draining bit is the linearization-free analogue of Cassandra/ScyllaDB's topology epoch). (2) a JOIN: a member recently entered the ring and is a PENDING owner of a position whose CURRENT owner has not yet released - detected as "this node is a pending owner of a position whose serving marker for that `ReplicaUnit` is ABSENT" (no successor has mounted yet). In the join case CURRENT (pre-join owners) and PENDING (post-join owners) differ because the new member shifts the `LocateKeyN` successor chain. Both causes are read off the gossiped ring + the `Draining` bit + the serving marker - no central coordinator, no consensus epoch.
  - **ROUTED replica set** = the set `replicasForKey` actually fans out to. When NOT in transition it is the stable replica set (CURRENT == PENDING, R nodes). When in transition it is the UNION(CURRENT, PENDING), which at R=2 spans up to 3 distinct nodes (the leaver + the two pending owners, or the joiner + the two current owners). The union is a SUPERSET of every node's possibly-disagreeing ownership opinion during gossip lag, so any single-node routing decision still reaches a node that physically holds the position - this is the property that fixes the post-leave readback failure of the superseded forwarding model.

**Dual-write with the ack bar held at the stable R (answers open question (a)).** The fan-out DUAL-WRITES the position to EVERY member of the ROUTED union (writes always go to all routed replicas, exactly as Cassandra sends every write to all replicas regardless of consistency level). The ACK BAR W is held at the STABLE quorum over the configured R - `W = requiredWriteAcks(WriteConsistency, R)` - and is NOT raised to cover the transient extra union member. At R=2 with `WriteConsistency=Quorum`, `W = floor(R/2)+1 = 2`: a write during the transition fans out to up to 3 union members and acks once ANY 2 of those 3 have durably applied it. The extra pending replica is a BONUS write target (it lowers latency-to-durability and pre-warms the successor with live writes during its mount) - it is NOT a higher ack bar. This is deliberately DIFFERENT from Cassandra's `blockFor`, which raises the required-ack count to cover pending endpoints: shale holds the bar at the stable R because raising it to ALL-3 transiently would make any one slow union node halve write availability on an R=2 store, defeating the point. The leaver counts as a normal routed replica whose ack is interchangeable with a pending owner's; because the leaver already has the data, it can satisfy W instantly while the pending owner is still mounting. `WriteConsistency=All` is held at the stable R TOO - `W = requiredWriteAcks(All, R) = R`, the number of STABLE replicas, NOT the transient union size. This is a deliberate departure from Cassandra's CL=ALL (which raises `blockFor` to include pending endpoints and is correspondingly unavailable during a topology change): widening All to the union would block every write on a mid-mount successor for the whole mount window, collapsing scale-down availability. The pending owners inherit the data through the shared object-storage db the moment they mount, so the durability All promises is preserved without requiring their in-flight ack. "All" means all STABLE replicas.

**A FENCED write leg is a TRANSIENT non-ack, not a hard failure.** During the transition the successor opens the position (fencing the leaver's handle the instant it opens, before the successor is serving). A union dual-write that lands on the now-fenced leaver leg fails with the writer-epoch fence (`backend.ErrFenced`). That leg MUST be classified TRANSIENT - neither an ack nor a hard failure - exactly like a mid-mount pending owner's `errUnitAcquiring`: the fenced commit did NOT durably apply on that leg, so not counting it as an ack is correct, and treating it as transient (vs hard) makes the fan-out RETRY the write onto the re-resolved union rather than fast-failing it (the fan-out fast-fails only when too many HARD errors make the stable-R quorum unreachable). The successor that fenced the leaver serves the write once it finishes mounting, so the retry acks; the cost is added latency during the mount window, not a lost or rejected write. The recode happens at EVERY apply-op error site, not just Commit: the writer-epoch fence can surface at WHICHEVER op runs first against a now-stale writer (slatedb's background manifest-epoch poll lands it on the next op), so the apply sequence `Begin->Get->Put->Commit` can fence at any step - the in-memory test double fences at `Begin` (recoded by the Begin path), real slatedb at `Get`/`Put`/`Commit`. Each is recoded to `errUnitAcquiring` and the stale mount evicted, identically. Backends expose the fence backend-agnostically via `backend.ErrFenced` (each wraps its native fence at every op so `errors.Is` resolves it) since the cluster cannot import a specific backend. The recovery is IN-CALL only if the per-write budget (`WriteTimeout`, default 5s, operator-tunable via `--write-timeout` / `SHALE_WRITE_TIMEOUT`) exceeds the successor mount window; a budget shorter than the mount returns a client-retryable `codes.Unavailable` instead. This is the during-leave WRITE-availability seal at R=2/W=quorum: without it a single fenced leg hard-fails the write for the whole successor mount window; it does NOT change the inherent floor that a moving unit with only one live writer cannot meet a 2-ack quorum until the successor mounts (that needs R>=3 or W=1). (The same recode is still owed on the CAS owner-local commit path - `commitRetryable` cannot classify a non-gRPC `backend.ErrFenced` - so CAS writes still hard-fail during a leave until that lands.)

**Union reads.** A read fans out across the ROUTED union per `ReadConsistency` (`Nearest` returns on the first replica that answers; `Quorum`/`All` wait for floor(R/2)+1 / all routed replicas). ANY union member that physically has the position MOUNTED serves it; a union member that is a pending owner still mid-mount returns the transient acquiring code (`errUnitAcquiring` -> `codes.ResourceExhausted` on the replica leg), which the read fan-out SKIPS while another union member (the leaver, which has the data) answers. So a read during a transition always finds the bytes on the still-mounted current owner, even though the ring opinion is mid-rotation. The LWW max-stamp winner selection, read-repair, and tombstone -> `ErrNotFound` are reused verbatim over the union.

**The pending owners acquire in the background (the normal reconcile, unchanged shape).** A node that is a PENDING owner of a position it has not mounted ACQUIREs it via the existing `reconcileReplicaUnits` -> `OpenReplicaUnit` path (mount the per-`(unit, replica)` database from shared storage at a strictly-higher epoch fence). This is the SAME initial-convergence mount machinery Phase 2b uses; pending ranges adds nothing new to the acquire side except that the position is now ALSO a write target via the union DURING the mount, so the successor receives live writes as it warms up. On mount-complete (the `mountMap[ru]` insert) the pending owner writes its SERVING MARKER (`WriteServingMarker(ru, E)`) to shared storage EXACTLY ONCE - the durable, poll-observable "I now physically hold and serve this position" record.

**The serving-marker handoff gate (REUSED unchanged).** The serving marker is the durable, poll-observable signal that a pending owner has mounted. It is keyed by `ReplicaUnit`, carries the writer's open epoch E, and is written EXACTLY ONCE by a node right after it inserts its `mountMap[ru]` entry. `ReadServingMarker(ru) (epoch, ok)` reads it without opening the database. The release rule is unchanged from the prior design: a node releases / drops a position from its routed set only when it reads a marker at an epoch STRICTLY ABOVE (`>`) its own open epoch - positive proof that a live SUCCESSOR is actually serving. The strict `>` rejects a node's own stale gain-marker at exactly its open epoch. A bare durable FENCE-epoch advance (`DurableEpochReplica(ru)`, which bumps at open-START before the mount completes) NEVER triggers release - it is strictly weaker than the serving marker (a successor that fences then crashes mid-mount advanced the fence WITHOUT writing a marker, so the marker is the only positive "live owner serving" confirmation). The marker read is a POINT-IN-TIME liveness observation, not a lease: if the successor crashes in the gap between the leaver reading the marker and completing its removal, the position is unserved until the next reconcile, with NO acked-write loss (durable-before-ack).

**The open epoch is the EXACT epoch the node opened at, captured from `OpenReplicaUnit`'s return value (NOT re-read from the live durable).** Both "its own open epoch" (the leaver's release threshold) and "the writer's open epoch E" (the epoch the serving marker carries) MUST be the precise epoch the factory opened the position at, captured ONCE at open time and held immutable for the life of the mount. They MUST NOT be re-derived from `DurableEpochReplica(ru)`: the durable manifest writer-epoch is a SHARED, monotone counter that EVERY `OpenReplicaUnit` of that position by ANY node bumps by one, so a re-read returns a moving target whose value depends on when it is read. A leaver that captured its release threshold from the live durable would see it CLIMB as its own successor opened (the successor's open bumps the durable above the leaver's epoch), so the successor's marker (at the successor's open epoch == that same bumped durable) could never be strictly above the threshold and the drain would hang to its timeout - the graceful-scale-down availability gap. The fix: `ReplicaBackendFactory.OpenReplicaUnit(ru, intended)` RETURNS the opened epoch `(backend.Backend, Epoch, error)` (it already computes it as `max(intended, durableEpochReplica+1)`); the cluster records that returned epoch per `ReplicaUnit` at the mount (alongside `mountMap[ru]`, cleared on release). EVERY gate that compares a serving marker against "this node's open epoch" reads the recorded value, NOT `DurableEpochReplica`: the `drainCheck` release gate (`beginDrain` captures it into `HandoffState.OpenEpoch`), the `DrainForLeave` completion gate (`allOwnedPositionsHandedOff`, which is the gate whose hang IS the observed real-storage scale-down availability gap - it keeps the leaver blocked-and-serving until each owned position reports a successor marker strictly above the recorded epoch), the stuck-flip recovery marker, AND the serving marker itself are all written/compared at the recorded value. Both sides are then immutable factory-sourced integers, and the fence is STRICTLY MONOTONE (every `OpenReplicaUnit` lands at `max(intended, durable+1)`, strictly above the prior durable), so EVERY later open - the immediate successor, or any further successor in a re-acquire cascade - lands strictly above the leaver's recorded epoch and writes a marker strictly above it (under a cascade the surviving marker is the leaver's epoch +2 or +3; the conclusion holds a fortiori). The successor's marker is therefore permanently strictly above the leaver's threshold (liveness) while a bare durable advance still never releases (safety). `DurableEpochReplica(ru)` remains in use ONLY as the strictly-weaker fence-advance hint described above, never as the gate threshold or the marker epoch.

**ORDERED REMOVAL (the CEP-21 ordering, mapped onto pending ranges).** The transition advances in the canonical order that keeps every replica set covered: (1) ADD the pending owners to the routed set FIRST (the union forms the instant gossip observes the transition; pending owners receive every write while they mount). (2) the pending owners STREAM - here a near-zero-cost MOUNT from shared object storage, not a byte copy - and on mount-complete write their serving marker. (3) the leaver DROPS OUT of a position's routed set (the union collapses onto the PENDING set) ONLY ONCE that position's pending owner has a serving marker present at an epoch strictly above the leaver's open epoch (handoff durably complete). (4) the leaver releases the position (`CloseReplicaUnit(ru)`) after it is out of the routed set. The leaver is fenced out of writing only AFTER its successor is provably serving, so there is NO window where the only durable copy is on a fenced/closed node. `replicasForKey` implements step 3 implicitly: once a position's serving marker is present at an epoch above the leaver's, the routing layer stops counting the leaver as a current owner for that position (it transitions that position from "in transition, union" to "pending set is now the stable set"), and the leaver's `drainCheck` releases its mount.

**Transitional mounts are HELD; reclaim and release gate on PENDING membership (not CURRENT alone).** The reconcile must never tear down a mount that is mid-handoff, or the handoff oscillates and never completes (observed on real object storage as a self-draining leaver fighting its own drain to the `GracefulLeaveDrainTimeout`, with the drain-release gate visibly escalating tick over tick). One invariant governs both halves: **a mounted position that is in the PENDING set is held stable; only a position ABSENT from PENDING is drained (if still current) or released (if not).** It splits into two rules. (1) RECLAIM (aborting a `Draining` position back to `Owned` because the ring flip-flopped it back to this node) fires ONLY for a position that is BACK in the PENDING set - one this node will still own after the draining members leave. A position in CURRENT but NOT in PENDING is being HANDED OFF (this node is itself draining, or a draining successor split moved it out of pending); its drain must run to completion (`drainCheck` releases on the successor's marker), so it is NEVER reclaimed. Without this rule a gracefully-leaving node - which is still a ring member, hence still a CURRENT owner of all its positions - reclaims (un-drains) every position every reconcile tick and then re-drains it, re-running `beginDrain` and re-capturing a fresh (climbing) open epoch each pass, so the strict release gate never catches the successor's marker and the drain hangs to the timeout. (2) the not-current RELEASE (clean-cut dropping a mounted position no longer in CURRENT) does NOT release a position that is in the PENDING set: that is a SUCCESSOR's pending mount (acquired + serving-marked, serving via the union) which must be HELD until the draining member leaves and it becomes the stable owner (entering CURRENT, where the steady-state branch keeps it). Releasing it would tear down the very handoff target the leaver's `drainCheck` is polling, and the successor would churn release -> re-acquire -> re-mark at a climbing epoch. A position absent from BOTH current and pending is genuinely abandoned and takes the plain clean-cut release unchanged. The SAME pending-membership gate governs the two sibling tear-down/grab paths that would otherwise re-open the oscillation: (3) the ACQUIRE (clean-cut new-mount) half (re-)mounts a CURRENT position ONLY if it is also in PENDING. After `drainCheck` RELEASES a drained position its phase is cleared to 0, but the leaver is still a ring member so the position is still CURRENT; without this gate the acquire half RE-GRABS the position it just handed off (re-fencing the successor at a climbed epoch), so the leaver's `ownedPositionCount` never reaches 0 and the leave never completes. (4) `evictStaleMount` (the write-path eviction that drops a mount whose handle failed, so the reconcile re-acquires a fresh one for the auto-recovery wedge) does NOT evict a position in a `Draining` (loser) phase: a fenced write on a draining mount is the EXPECTED successor-fence signal (the successor opened the shared db at a higher epoch), NOT a stale-handle desync. Evicting it would drop the leaver's mount before the successor's marker and let the acquire half re-grab it - the mutual-fencing ping-pong (leaver re-mounts at `durable+1`, re-fences the successor; the successor's next union write evicts + re-acquires + re-marks) that is the write-path driver of the hang under a continuous writer. The fenced write simply fails on the leaver's leg and retries onto the union (the successor's leg already acked it); `drainCheck` completes the drain on the successor's marker. All four rules are one invariant: a mounted position in PENDING is held; positions absent from PENDING are drained/released but never re-grabbed; and a draining mount is never evicted out from under its own drain.

**DrainForLeave completion.** A leaving node's `DrainForLeave(ctx)` (called at the top of `Close()` when `GracefulLeaveDrainTimeout > 0` and `multiReplicated()`): SETS THIS NODE DRAINING (`membership.SetDraining(true)` so the bit gossips and every node recomputes pending = current-minus-this-node, forming the union), then BLOCKS until EVERY position this node owns has a serving successor - i.e. until each owned position's `ReplicaUnit` has a serving marker at an epoch strictly above this node's open epoch and `drainCheck` has released it, so `ownedPositionCount() == 0` - OR `ctx` cancels / the timeout fires. The node STAYS A CURRENT OWNER and keeps serving its mounted positions (receiving union writes + reads) throughout the wait. ONLY AFTER the drain completes does the node do the REAL `membership.Leave()` + `Shutdown()` teardown. **The host run-loop MUST keep the gRPC transport SERVING for the whole drain: it runs `Close()` (which calls `DrainForLeave`) BEFORE it `GracefulStop()`s the gRPC server, never the reverse.** If the transport is stopped first, every union write a peer dual-writes to this draining node is connection-refused for the entire drain window (W=quorum unmet on its moving units, since the leaver's leg - the stable partner that lets a moving unit reach quorum without waiting on the successor mount - is unreachable); that is a host-ordering bug, not a cluster one, but it defeats the whole overlap design. Order: SET DRAINING (form the union) -> SERVE + WAIT (successors mount, write markers, the union collapses position-by-position) -> REAL LEAVE + SHUTDOWN. This REUSES `DrainForLeave`, the `Draining` Meta bit, and the serving marker unchanged; what changes is that the node stays a CURRENT OWNER and is dual-written via the union, instead of being excluded from the ring and forwarded back to.

**The in-memory maps are keyed by `ReplicaUnit`, NOT `GenUnit` (REUSED).** `mountMap` is `map[ReplicaUnit]backend.Backend`, the separate `replicaPos` map is folded into the key, and `handoffPhase map[ReplicaUnit]HandoffState` is a `mountMu`-guarded sibling. This is REQUIRED because a ring change can shuffle this node's index within a unit's replica list, so one node can hold the OLD position (draining, still serving union writes) AND the NEW position (acquiring/ready) of the SAME unit at once. The re-keying ripples through `localBackendForKey` / `localWriteBackendForKey` / `evictStaleMount` / `mountedBackends` and the reconcile diff. The `HandoffState` is SIMPLER than the forwarding design: it is `{Phase HandoffPhase, OpenEpoch Epoch}` with NO `Predecessor` / `PredecessorAddr` fields (no forwarding target to remember - the union routes directly to the leaver), and the phases collapse to `Acquiring` (pending owner mid-mount, served via the union by other members) on the gainer side and `Draining` -> `Releasing` on the loser side. There is no `Acquiring`-state forward behavior: a pending owner mid-mount simply returns `errUnitAcquiring` and the union covers the position via the still-mounted current owner.

**The cut (the exact epoch-fence point, REUSED).** The handoff cut is a single instant on the per-replica durable manifest: the epoch E at which a PENDING owner opened (`OpenReplicaUnit` opens at `max(intended, durableEpochReplica+1)` and writes the bumped epoch). Below E the leaver is the authoritative writer; at or above E the new owner is. The leaver's own writes are fenced the moment it attempts one past E (slatedb `CloseReasonFenced`). Dual-write does NOT violate single-writer: the durable per-`(unit, replica)` databases of the R routed replicas are INDEPENDENT databases (Phase 2b's independence invariant), so dual-writing a position to current + pending owners writes to DISTINCT databases, each with its own single highest-epoch writer; it is NOT two writers on one database. The leaver's database stays the authoritative copy at its replica position until the leaver releases; the pending owner's database is a separate replica copy at its own position, fenced independently. Apply-if-newer / LWW makes a write landing on multiple union members idempotent across the per-replica copies (the same stamped envelope applied to each is a no-op on any copy that already has it).

**DURABILITY INVARIANT: no acked write lost (the argument).** A write acked at W over the stable R is durable on >= W of the routed replicas. (1) FAN-OUT ACK ACCOUNTING: the write is acked to the client ONLY after W routed replicas durably applied it (every routed replica opens `AwaitDurable=true`, so an ack means durable-before-ack on each). Any of those W can be a CURRENT owner OR a PENDING owner - they all hold an independent durable copy of the position at `dbNameReplica(ru)` for their replica index. (2) THE LEAVER IS FENCED ONLY AFTER ITS SUCCESSOR IS SERVING (the marker gate + ordered removal): the leaver stays a routed current owner - mounted, serving, durably holding every write it acked - until a pending owner's serving marker proves the successor physically has the position. So there is NEVER an instant where the only durable copy of an acked write is on a fenced/closed node: at the moment the leaver releases, the successor is already serving (marker present) and the successor's database already contains every acked write durable below E (it mounted the same shared-storage replica OR received the writes live via the union during its mount; either way WAL-recovery-over-the-full-durable-tail on open recovers them - see the seal below). (3) IDEMPOTENCY ACROSS THE UNION (answers open question (c)): a write dual-written to several union members applies the SAME pre-stamped LWW envelope to each member's independent replica database; apply-if-newer (`txApplyIfNewer`: write only if the incoming stamp strictly beats the stored stamp) makes a re-applied or reordered envelope a silent NO-OP on any copy that already has an equal-or-newer stamp. So dual-write cannot double-apply, cannot move any copy backward, and a union write that lands on a copy the next ring opinion no longer routes to is harmless (it is just an extra durable copy). A retry re-dispatches the same stamped envelope to the re-resolved live union, which is idempotent for the same reason.

**Single-writer fence stays intact.** Dual-write writes to INDEPENDENT per-replica databases, one highest-epoch writer each; it never puts two writers on one database. The leaver is the authoritative writer of its replica copy until it releases; a pending owner is the authoritative writer of ITS replica copy from epoch E. A stale write that races the leaver's release lands on its now-fenced handle, fails (`CloseReasonFenced`), is evicted (`evictStaleMount` -> `errUnitAcquiring`), and is retried onto the live union.

**The slate manifest seal (consistent mount under dual-write).** A pending owner must mount a view containing every write acked while it was mounting. With pending ranges this is even cleaner than the forwarding design: a write during the transition is dual-written DIRECTLY to the pending owner's own replica database (not forwarded to the leaver), so once the pending owner is mounted it receives the write into ITS database with no cross-node seal needed. The only seal concern is the writes acked on the leaver (or other current owners) BEFORE the pending owner finished mounting: those are durable in the OTHER replica copies, and the pending owner's copy catches up because (a) DURABLE-BEFORE-ACK on every routed replica, and (b) WAL RECOVERY OVER THE FULL DURABLE TAIL on the pending owner's `OpenReplicaUnit` (it recovers the full durable WAL tail of its own per-replica database, not just the manifest snapshot at fence time), with the manifest-epoch FENCE effective NO LATER THAN the WAL-recovery cutoff (fence-then-recover) so a write that races the mount is either inside the recovered tail or fenced (never an acked-but-invisible gap). **This is the SAME implementation-must-verify assumption as before** (slatedb-go v0.13.1 is an opaque uniffi FFI; the open/recovery path is not in Go source). It MUST be pinned with two tests: (1) a write acked just before a pending owner's fence is readable on that pending owner after the handoff; (2) a write dual-written CONCURRENTLY with a pending owner's mount/recovery (acked below E on its own replica copy) is readable on it after the handoff. If slatedb open does not replay the durable WAL tail OR the fence is not effective at-or-before the recovery cutoff, this is a P0 lost-write and Phase 2e is BLOCKED until closed. The fence-detection chain (a fenced write fails at its durable-write step, strictly before any ack, so no acked-past-fence write exists) holds unchanged.

**THE FOUR CORRECTNESS GUARANTEES (argued explicitly).**

  1. NO LOST WRITES. Covered by the durability-invariant argument above: a write acked at W is durable on >= W independent replica copies; the leaver is removed from the routed set (and only then fenced/closed) ONLY after a pending owner is provably serving, so the union always contains a node that physically holds every acked write. WAL recovery + the fence seal carry the pre-mount writes onto the pending owner's copy. At no instant is an acked write's only durable copy on a fenced/closed node.

  2. NO DOUBLE-APPLY / NO SPLIT-BRAIN. Dual-write targets INDEPENDENT per-replica databases, each with one highest-epoch writer; apply-if-newer / LWW makes the same stamped envelope idempotent across copies and across retries. There is no instant where two writers hold one database open at the highest epoch.

  3. CRASH SAFETY (NOT unconditionally "fully available"; see the R/W matrix in the design doc). (a) A PENDING owner crashes mid-mount: it never wrote a serving marker, so the leaver is never removed from the routed set (ordered removal requires the marker) and keeps serving; the union still covers the position via the leaver. The next reconcile reassigns the pending position. (b) The LEAVER crashes while still a routed current owner (before any successor is serving): the union loses the only mounted copy for the position until a pending owner mounts, degrading THIS position to the Option-A mount-bounded window. Availability during this window depends on R/W: at R>=3/W=majority or R=2/W=1 the other replicas cover W; at R=2/W=2 (write-all) the position is write-unavailable until a pending owner mounts. Every acked write is already durable (durable-before-ack) on its independent replica copies, so a pending owner sees them all once it mounts; no acked write is lost. (c) A pending owner crashes AFTER writing its marker but before the leaver removed it: the marker is durable, so the leaver removes the position on its next poll and the next reconcile reassigns it; no acked write lost. RESIDUAL (marker read is point-in-time): if the successor crashes in the read-to-removal gap, the position is unserved until the next reconcile, benign (no acked-write loss).

  4. SLOW MOUNT IS A NON-EVENT. The entire pending-owner mount window is invisible to availability: the leaver (a current owner) stays in the routed union and serves the position throughout, so the pending owner's mount can take 30s without any routed op failing. The leaver leaves the routed set only at the marker gate, strictly after the successor is serving. This is the property Option A could not give (its availability was bounded by mount-vs-`WriteTimeout`).

**Resolved review findings (the pending-ranges model closes them directly).** The prior adversarial reviews (`docs/design/overlap-handoff-review.md`, `docs/design/overlap-handoff-rereview.md`, kept as history) found holes in the forwarding design; pending ranges closes the same holes WITHOUT the forwarding machinery the re-review's NEW findings then had to patch:

  - P0-1 (routing never reaches the still-serving old owner): closed by KEEPING the old owner in the routed UNION (it stays a current owner), so routing reaches it DIRECTLY. No back-forward needed.
  - P0-2 (per-`GenUnit` maps cannot represent overlapping positions): closed by RE-KEYING `mountMap` / phase map / `HandoffState` by `ReplicaUnit` (REUSED unchanged).
  - P1-1 (release on a bare durable-epoch advance): closed by the SERVING MARKER gate (REUSED unchanged) - removal requires a marker strictly above the leaver's epoch; a bare fence advance never removes.
  - P1-2 / NEW-P1-4 (consistent-mount WAL tail + intra-open fence<=recovery ordering): REUSED as the implementation-must-verify pin, simplified because writes are dual-written to the pending owner's own copy rather than forwarded.
  - P1-3 (release-check lock discipline): REUSED unchanged (I/O outside the lock; phase advance + `mountMap` CAS-delete are one `mountMu` critical section).
  - P1-4 (crash-safety not always fully available): REUSED via the R/W matrix.
  - NEW-P0 / NEW-P1-1 / NEW-P1-2 / NEW-P1-3 (the forwarding-specific holes: position-addressed forward wire change, predecessor identification, single-hop scope, point-in-time marker gap): the FIRST THREE are MOOT under pending ranges (there is no forward, no predecessor to identify, no single-hop scope - the union routes directly), and they document machinery now REMOVED. The point-in-time-marker honesty fix (NEW-P1-3) is REUSED in guarantee 3(c).

**THE PENDING-RANGES AVAILABILITY GATE (the acceptance test).** A new in-process loss-oracle test (`tests/integration/overlap_handoff_test.go`, sharedfactory + memory backend, NO slatedb tag, NO MinIO) is the acceptance bar. It stands up a multi-backend R=2 cluster on a per-replica shared-backing factory whose `OpenReplicaUnit` can be made ARBITRARILY SLOW (a multi-second mount injection modeling a real MinIO mount exceeding `WriteTimeout`), writes a recorded BASELINE, then runs a CONTINUOUS WRITER through a 3 -> N membership change (AND a graceful one-node leave) while the slow mounts are in flight, and asserts BOTH (a) THE ORACLE - every baseline key and every ACKED probe key is readable with its exact value from every node, ZERO acked loss; AND (b) THE ACK RATE - acked / attempted is ~100% EVEN with the slow mount (where Option A alone drops to ~50-54% as the retry budget exhausts on the mount). It is kept honest by a BREAK DEMONSTRATION that disables BOTH the pending-ranges union (forces the clean-cut RELEASE-then-ACQUIRE / draining-exclusion, so the position routes only to the unmounted pending owner) AND the Option-A retry: the same slow-mount change drives the ack rate WELL below the threshold AND (the post-leave-readback failure the forwarding model could not fix) makes baseline keys unreadable from the post-transition routed set, proving the union is what holds BOTH availability and readback. The record-only-acked discipline keeps (a) and (b) independent.

**IN SCOPE: pending ranges for an R>1 (and R=1) multi-backend graceful membership transition** - during a transition the routed replica set is the UNION of current (draining-inclusive) and pending (draining-exclusive) owners; the fan-out DUAL-WRITES the union with the ack bar held at the stable R quorum; reads fan out across the union and any mounted member serves; pending owners acquire in the background via the normal reconcile and write a serving marker on mount-complete; ORDERED REMOVAL drops the leaver from a position's routed set (and only then fences/closes it) once that position's serving marker is present above the leaver's epoch; `DrainForLeave` completes when every owned position has a serving successor. Availability no longer depends on mount time. **OUT OF SCOPE (unchanged):** re-replicating a unit's BYTES onto a brand-new replica position with no durable copy (anti-entropy re-replication is a separate phase); resharding / doubling at R>1 (Phase 4 is R=1); the legacy per-node path and the R=1 lease-handoff (Phase 3) are UNTOUCHED (Phase 2e lives behind `multiReplicated()`). **REMOVED (superseded):** per-position forwarding (`acquiringForwardTarget` / `forwardPutToPredecessor` / `forwardGetToPredecessor`), the position-addressed forwarded-op wire change, the `PredecessorAddr` / `Predecessor` fields, the `priorDesiredReplicas` / `priorAddrs` snapshot, the single-hop-scope fallback, and the draining-exclusion in `reconcileRingFromMembership`. Option A's `WriteTimeout`-bounded retry (Phase 2d) is RETAINED as the safety net for the residual cases (crash mid-handoff, pure-new-mount initial convergence); A and B compose.

#### Graceful leave (scale-down)

Scale-DOWN (a deliberate node REMOVAL) is the case pending ranges is built for. When a node is removed gracefully (a SIGTERM from the operator / orchestrator), it sets itself `Draining`; every node's `replicasForKey` then computes the leaver's positions as CURRENT-but-not-PENDING (current = ring including the leaver, pending = ring excluding it), forms the routed UNION, and DUAL-WRITES the leaver + its pending successors. The leaver STAYS A CURRENT OWNER and keeps serving until each successor mounts and writes a serving marker, at which point ordered removal collapses the union onto the pending set and the leaver releases. The pending-ranges machine is therefore already the right mechanism for a graceful leave; the two pieces this subsection adds are (a) the `Draining` node-state that puts the leaver into the union WITHOUT removing it from ownership, and (b) the leaving node's `Close()` waiting for the drain (the marker gate on every owned position) to finish.

**The gap today (the availability bug this subsection closes), AND why the obvious fix is self-contradictory.** On SIGTERM the run loop calls `Cluster.Close()`. The naive fix - "broadcast `memberlist.Leave()` but keep the transport up so survivors re-own the positions while the leaving node keeps serving" - DOES NOT WORK, and that self-contradiction is the root cause of the measured ~58% in-process / ~70% staging drain availability. The leaving node is being asked to be BOTH gone (so survivors take over its ownership positions) AND alive (so it can keep serving during the drain), and a `memberlist.Leave()`-while-staying-alive cannot represent that. `memberlist.Leave()` broadcasts a "leaving" intent, but the node then KEEPS GOSSIPING AS ALIVE (the transport is up). Survivors receive the leave, briefly drop the node, then keep hearing it alive and RE-ADD it to their membership snapshot. Because ownership is derived from the live membership ring on EVERY node, the survivors that re-added the leaver compute it as STILL OWNING its positions and never start the handoff. `memberlist.Leave()` says "I am gone" while the transport says "I am alive"; membership cannot hold both, so the handoff never starts.

**The fix: a distinct DRAINING node-state - REACHABLE, a CURRENT OWNER, and the source of the PENDING split.** The node needs a state `memberlist.Leave()`-while-alive could not express: it stays a full, alive, addressable member AND a current owner of its positions (so routing keeps dual-writing it and it keeps serving), while its `Draining` bit makes every node compute the PENDING set (ring-minus-this-node) and route the UNION. This is advertised by a gossiped per-member `Draining` bit, NOT by leaving the cluster. The sequence:

  - The leaving node SETS ITSELF DRAINING by flipping a `Draining` flag in its memberlist `Meta` and calling `memberlist.UpdateNode` so the flag gossips out (the `Meta` already carries the node's gRPC dial address; the `Draining` bit rides alongside it). The node stays ALIVE and a current owner - it does NOT call `memberlist.Leave()` here, and it is NOT removed from the ownership ring.
  - Every node (including the draining node itself) DECODES the peer `Meta` on each gossip update and learns the `Draining` bit. `replicasForKey` consumes it: for a position the leaver currently owns, CURRENT (ring including the leaver) != PENDING (ring excluding the leaver), so the position is IN TRANSITION and the routed set becomes UNION(current, pending). The leaver stays in CURRENT (mounted, serving), and the successor that takes the leaver's exact position is in PENDING.
  - Survivors that are PENDING owners of the leaver's positions ACQUIRE them in the background via the normal reconcile, receiving live dual-write traffic via the union as they mount, and write a serving marker on mount-complete. Routing reaches the leaver DIRECTLY (it is a routed union member); there is NO back-forward.
  - The leaving node BLOCKS until every one of its positions has a SERVING successor - i.e. until each owned position's serving marker is present at an epoch strictly above its open epoch and `drainCheck` has released the position (the SAME marker-gate release rule as any transition). Only AFTER the drain completes (or the grace timeout fires) does the node do the REAL `memberlist.Leave()` + `Shutdown()` teardown.

So the order is: SET DRAINING (form the union) -> SERVE + WAIT (successors mount, write markers, the union collapses onto the pending set position-by-position) -> REAL `Leave()` + `Shutdown()`. The drain happens entirely while the node is a normal alive current owner; the actual departure happens only once there is nothing left to drain.

**This SUPERSEDES the ring-freeze stopgap AND the draining-exclusion.** An earlier attempt called `memberlist.Leave()` at the START of the drain (the self-contradiction above) and then tried to paper over the resulting snapshot collapse with a `leaving atomic.Bool` flag; a later draft replaced that with draining-EXCLUSION (drop the draining node from the ownership ring + forward back to it per position). The pending-ranges model removes the need for ALL of it: the node stays ALIVE and a CURRENT OWNER, so its snapshot does not collapse, `reconcileRingFromMembership` works normally and does NOT exclude draining members (the include/exclude split is computed per-op in `replicasForKey`, not baked into the ring), and there is no frozen ring and no per-position back-forward to maintain. The `leaving` flag + its early-return guard, the remove-self-only step, the draining-exclusion in `reconcileRingFromMembership`, and the entire per-position forward path (`acquiringForwardTarget` + the predecessor snapshot) are all REMOVED, replaced by the draining-`Meta`-driven UNION routing in `replicasForKey`.

**Two memberlist verbs, split (Leave vs Shutdown), used ONLY at the END.** memberlist exposes `Leave()` (broadcast the graceful departure so peers record a clean leave and stop re-adding the node) DISTINCT from `Shutdown()` (tear down the local transport). The membership wrapper's `Close()` today does BOTH at once. They are still split: the REAL `Leave()` is deferred to AFTER the drain, not used to START it. Starting the drain with `Leave()` is exactly the self-contradiction above. The drain is started by the `Draining` `Meta` bit (the node stays fully alive, a current owner). Only once the drain is done does the node call the real `Leave()` (now correct: the node really is going away) followed by `Shutdown()` of the transport in the existing teardown.

**The drain entry point.** A `Cluster` method `DrainForLeave(ctx)` (equivalently `GracefulLeave(timeout)`): SET THIS NODE DRAINING via the membership `Meta` flag (so every node computes the pending split and routes the union, while this node stays alive, a current owner, and serving), then BLOCK until no position this node owns still lacks a serving successor (`ownedPositionCount() == 0`: every owned position's `drainCheck` released on its successor's serving marker) OR `ctx` is cancelled / the timeout fires. The reconcile loop, the serving path, and `drainCheck` all stay running during this wait. AFTER the wait returns (drained or timed out), the existing teardown - the real membership `Leave()` then `Shutdown()` - runs. The shutdown path calls `DrainForLeave` at the TOP of `Close()` - gated on config, BEFORE any teardown, while the loops are still running - then proceeds with the existing teardown (now including the real `Leave()`) unchanged. The SIGTERM handler in the run loop just calls `Close()` as it does today, so no run-loop change is required.

**Config gate.** A `Cluster` config field `GracefulLeaveDrainTimeout time.Duration` controls it. `0` = DISABLED = exactly today's behavior (`Close()` does not drain; the gap remains - this is also the break-demo state for the acceptance gate). When `> 0` AND the cluster is multi-backend replicated (`multiReplicated()`), `Close()` calls `DrainForLeave(GracefulLeaveDrainTimeout)` first. The R=1 lease-handoff path and the legacy per-node path are UNTOUCHED (the field is a no-op outside multi-backend replication, consistent with the rest of Phase 2e living behind `multiReplicated()`).

**What drains vs the residual.** A graceful leave maximizes coverage because a position the leaving node owns is, by definition, taken over by a SURVIVING node that becomes a PENDING owner - so the union forms and the leaver drains on the successor's marker. In the common case EVERY position the leaving node serves is covered by the union + the drain wait. The residual (NOT covered, degrading to a gap or to the old behavior):

  - A position where the leaving node simply DROPS OUT of a unit's replica set and some OTHER already-mounted replica covers W with no new node taking the leaver's EXACT position: PENDING already contains R mounted owners without the leaver, so the union adds nothing to acquire and there is nothing to drain - the eager release is correct and the position stays available via the surviving replicas. This is NOT a gap; it is simply outside the drain machinery.
  - A position whose PENDING successor is STUCK (its mount never completes within the grace budget): the drain wait times out, `Close()` proceeds, and that one position is UNSERVED from teardown until the successor finally mounts - exactly today's gap, for that position only, bounded by the operator's grace budget rather than unbounded.

**The invariant.** For a position with a REACHABLE pending successor, a graceful leave has NO unserved window: the leaving node stays a routed current owner and serves the position (dual-written via the union) until the successor is serving (marker present) and `drainCheck` releases it, and only then does the leaving node close. A continuous writer through a graceful one-node leave therefore sees ~100% ack and zero unserved window (vs the gap today). The residual: a position whose successor is STUCK degrades to the old gap AFTER the timeout; a position the surviving replicas already cover is not in the drain (it does not need to be). No-acked-write-lost and single-writer-fence are unchanged (the leaving node never leaves the routed set for a position before its successor is provably serving).

**Operator concern (orchestrator grace period).** The orchestrator's termination grace period MUST exceed `GracefulLeaveDrainTimeout`, or the orchestrator will SIGKILL the process mid-drain and reopen the very gap the drain closes. For a Kubernetes StatefulSet this is `terminationGracePeriodSeconds`, which the operator sets STRICTLY GREATER than `GracefulLeaveDrainTimeout` (with headroom for the leave broadcast to gossip out and for the post-drain teardown to finish). This is an operator-side configuration coupling, surfaced here so it is not forgotten at deploy time; the code only enforces its own timeout, not the orchestrator's.

**Acceptance (the break-demo).** The graceful-leave drain is demonstrated by a continuous writer through a graceful one-node leave asserting ~100% ack and zero unserved window, paired with a BREAK DEMONSTRATION that sets `GracefulLeaveDrainTimeout = 0` (drain disabled): the same leave then shows the gap (acked / attempted drops while the leaving node's just-closed positions are unserved until the survivors mount), proving the drain wait is what holds availability rather than rubber-stamping it. This reuses the slow-`OpenReplicaUnit` injection from the overlap availability gate, so the leaving node's positions take a measurable, mount-time-dominated interval to hand off.

### v0.8 Phase 3: lease-handoff rebalance

Phase 3 makes a multi-backend node ACT on membership changes: when the ring re-assigns units, a unit whose owner changed hands off COPY-FREE. The old owner closes it (flush + release the lease); the new owner opens it at a higher epoch (fencing the old). Bytes never move (they live in shared object storage); only the writer lease moves. This is the data-loss-sensitive phase, built to a NO-ACKED-WRITE-LOST invariant. Multi-backend mode only: the legacy per-node path and its v0.3 Coordinator rebalance are UNTOUCHED.

- **Reconcile on membership change (anti-entropy).** Hook the existing membership path: `bumpRingGen` -> `scheduleEvaluate`. In multi mode (`c.multi`) scheduleEvaluate runs the unit reconcile instead of the v0.3 Coordinator (which is nil / short-circuited in multi mode per Phase 2). The reconcile is desired-vs-mounted: desired = `storageunit.OwnedUnits(self, unitCount, ringOwnerLookup)` against the CURRENT ring; mounted = the mountMap keys (`factory.OpenUnits()`). For each unit desired-but-not-mounted: ACQUIRE (`OpenUnit` at the next epoch). For each mounted-but-not-desired: RELEASE (`CloseUnit`). It is idempotent and self-healing (a node that should own U but lost its mount re-acquires it), safe to run on every membership event. All mountMap mutations are `mountMu`-guarded and the reconcile is serialized (one at a time) so two membership changes cannot interleave mounts.

- **Epoch fencing (the safety core).** Acquire opens at an epoch STRICTLY HIGHER than the unit's current DURABLE lease epoch, which fences the prior owner: its further writes to that unit fail. The cross-node epoch source of truth is the durable lease state (the slatedb manifest writer-epoch for slate; a shared epoch registry for the test factory), NOT in-process state. `OpenUnit(u, epoch)` carries the intended epoch; the backend factory performs the actual fence against the durable manifest.

- **Flush-before-release ordering + the NO-ACKED-WRITE-LOST invariant.** The old owner's `CloseUnit(u)` FLUSHES (durable) then releases. Phase 3 is single-replica with durable-before-ack (R=1 + AwaitDurable=true), so every ACKED write is already durable in object storage before the handoff: the new owner opening the unit sees all acked writes. In-flight (un-acked) writes may be fenced or lost, but the client never got success for those and retries. **NO ACKED WRITE may be lost.** The phase is built to this invariant.

- **Handoff window.** Between old-owner-release and new-owner-acquire the unit is briefly unavailable. An op routed to a unit not currently mounted by its ring-owner returns a RETRYABLE error (`codes.Unavailable` / `FailedPrecondition`, reusing the existing migration-guard / cutover retry shape) so the originator retries and succeeds once the new owner has acquired. Never serve a wrong or stale result; never lose a write.

**OUT OF SCOPE (later phases):** doubling / resharding and the migration tool (Phase 4+); per-unit replication / R>1 in multi mode (still R=1); relaxed durability (still AwaitDurable=true; that combo needs R>=2). The legacy per-node path and its Coordinator are not touched.

### v0.8 Phase 4: doubling resharder

Phase 4 lets an operator GROW capacity by DOUBLING the unit count: N -> 2N at a new cluster generation. Each old unit K bisects into EXACTLY new units K and K+N by one additional hash bit (the math is built: `storageunit.ChildUnit` / `ChildUnits` / `UnitCount.Double` / `UnitForHash`). The bisect is ONLINE and per-unit: the old unit keeps serving while its data is copied, then an atomic per-unit cut-over flips routing. Built to a NO-ACKED-WRITE-LOST invariant on a SINGLE-NODE cluster (the supported, gate-validated surface). Multi-backend mode ONLY; the legacy per-node path is untouched. On a SINGLE-NODE cluster `Reshard()` runs this local bisect directly; on a MULTI-NODE cluster a multi-node reshard is the coordinated cluster-wide-freeze flow below ("v0.8 Multi-node reshard"), which reuses this per-node bisect under a barrier. Assumes STABLE membership for the duration of a reshard.

- **Generation-qualified unit identity (the central change).** Today a unit maps to storage by UnitID alone (`Backing` keys `stores`/`epochs` by bare `UnitID`; `unitIDBytes` feeds the ring a bare id). That COLLIDES across a doubling: gen-g unit K (count N) and gen-(g+1) unit K (count 2N) would share one database. So a unit's STORAGE IDENTITY gains the generation. The implemented shape is a wrapping value type `storageunit.GenUnit{Gen Generation, ID UnitID}`: the storage key / object-store prefix is `(generation, UnitID)`, making gen-g unit K and gen-(g+1) unit K distinct databases that coexist during the bisect. It is threaded through `BackendFactory` (OpenUnit/CloseUnit/CurrentEpoch take a `GenUnit`; OpenUnits returns `[]GenUnit`), the cluster `mountMap` (keyed by `GenUnit`), the ring routing (`genUnitBytes` encodes the generation ahead of the unit id, so the gen-g and gen-(g+1) id of the same K hash to potentially different owners), and the test factories (memfactory / sharedfactory key their stores by `GenUnit`). `Generation` is a monotonic `uint64`; the cluster boots at generation 0.

- **Cluster generation.** The cluster has a CURRENT generation g (N units). Steady state: all units at gen g; routing = `UnitForShardKey(key, N)` at gen g. A reshard transitions g -> g+1 (N -> 2N) and is the ONLY thing that advances the generation.

- **The online per-unit bisect.** An explicit operator trigger - `Cluster.Reshard()` - marches through the N old (gen-g) units, one at a time (serialized by `reshardMu`, so two concurrent Reshard calls cannot interleave). For each old unit K: (1) create the two new gen-(g+1) units K and K+N (fresh databases via `factory.OpenUnit` at the new generation); (2) BACKGROUND-COPY: scan old-unit-K's keys, route each to new-K or new-(K+N) by `ChildUnit` (the new hash bit), write into the new units - old-unit-K KEEPS SERVING reads + writes throughout; (3) CATCH-UP + ATOMIC CUT-OVER: take the per-unit write-pause lock for old-K's key-space ONLY (`pauseUnit`, which Put/Delete/Begin briefly block on), drain the last writes into the new units (a second copy pass under the pause so any write between copy-end and pause-start is captured), then atomically flip routing for old-K's key-space by adding K to the cut-over set, and retire old-unit-K (`CloseUnit` at gen g). After all N bisect, the cluster is uniformly at gen g+1 (2N units); the cluster's live generation advances to g+1 and the cut-over set is cleared (every unit is now resolved by the gen-(g+1) map). On a SINGLE-NODE cluster all 2N units stay on the one node and the reshard is complete. On a MULTI-NODE cluster this node-LOCAL bisect (write-pause + catch-up) is insufficient on its own: a cross-node doubling needs a cluster-wide generation BARRIER so every node routes at one generation and the pause spans the logical key-space, not one node's view. `Reshard` called on a multi-node cluster therefore runs the coordinated cluster-wide-freeze flow below ("v0.8 Multi-node reshard"): it FREEZES writes cluster-wide, runs THIS same per-node bisect on every node under the freeze (the catch-up path inert because the data is static), atomically flips all nodes to gen g+1, resumes, then redistributes the 2N units via the reused Phase 3 lease handoff. The single-node path (this bullet) stays byte-for-byte unchanged.

- **Generation-aware routing during the transition.** The added routing state is small and lives in one `genState` value behind a `RWMutex`: the current generation g, the unit count N at g, the count 2N at g+1, and a per-old-unit "has this bisected/cut-over" set. For a key mid-reshard: compute its OLD unit K = `UnitForHash(hash, N)` at gen g. If K has cut over -> route to its NEW `GenUnit{g+1, UnitForHash(hash, 2N)}`. Else -> `GenUnit{g, K}`. When no reshard is in flight (empty cut-over set) every key resolves to `GenUnit{g, UnitForHash(hash, N)}`, so the steady-state path is unchanged. The resolved `GenUnit` is what gets placed on the ring (for ownership) and looked up in the mountMap (for the local backend). Deterministic; reads/writes land correctly throughout the reshard.

- **NO-ACKED-WRITE-LOST during the bisect (the safety core).** The hazard: a write to old-unit-K arriving DURING the background copy or the catch-up must not be lost. The copy-then-catch-up-then-atomic-cut-over pattern - the same shape as the validated Phase 3 handoff - handles it: writes before cut-over go to old-K and are captured by the catch-up drain; writes after cut-over go to the new units; the brief per-key-space write-pause makes the boundary clean. On a SINGLE-NODE reshard (the supported surface), with R=1 + durable-before-ack, every ACKED write is visible after the reshard - gate-validated lossless under ~252k concurrent acked writes. (A multi-node reshard's node-local write-pause is insufficient alone; it upholds this invariant via the cluster-wide write-freeze barrier below, which removes concurrent writes entirely for the bisect's duration.) The phase is built to this invariant.

**IN SCOPE: single-node growth by doubling under stable membership** (gate-validated lossless under concurrent writes). The MULTI-NODE doubling is the coordinated cluster-wide-freeze flow specified next ("v0.8 Multi-node reshard"), which reuses this per-node bisect under a barrier. **OUT OF SCOPE (later phases):** concurrent membership-change + reshard (assume stable membership during a reshard - a mid-reshard membership change ABORTS); halving / shrink (growth/doubling only); the migration tool (per-node -> per-unit, a separate roadmap item); per-unit replication / R>1 and relaxed durability (still R=1, still AwaitDurable=true). The legacy per-node path is not touched.

### v0.8 Multi-node reshard (cluster-wide freeze)

A multi-node doubling is SAFE via a brief cluster-wide WRITE-FREEZE. Phase 4's single-node bisect leans on a node-LOCAL write-pause, which cannot order writes across nodes: with each node advancing its generation independently, a concurrent write during a cross-node bisect can be routed at the old generation on one node while another has already flipped, and be lost. The freeze removes that hazard at the root: pause ALL writes cluster-wide for the (short) reshard, so there are NO concurrent writes and the per-node bisect is a STATIC copy (the data is not moving, so the catch-up window that is the single-node bisect's hard part disappears). The online/no-freeze and dual-write variants are explicitly OUT of scope (later optimizations).

The node where `Reshard()` is called on a multi-node cluster is the COORDINATOR. It drives a 4-phase barrier over every node (including itself) via direct peer RPC (the `pkg/cluster/remote.go` peerClient pattern + a new `ShaleNode` method per phase under `pkg/rpc/proto`), NOT gossip (gossip is a no-op / eventually-consistent broadcast, wrong for a barrier). The flow targets `targetGen = g+1`:

  1. **FREEZE.** Coordinator RPCs every node `ReshardFreeze(g+1)`. Each node enters a write-freeze: `Put` / `Delete` / `Begin` (and the CAS commit write path) return a RETRYABLE error (`codes.Unavailable`, the same shape as the Phase 3 handoff window) until unfrozen; READS continue, served from the live gen-g units. Each node acks. Coordinator WAITS for ALL acks (the barrier). Any failure or timeout -> ABORT.
  2. **BISECT (local, source drained quiescent).** Each node bisects the gen-g units IT owns - for each owned unit K, copy K's keys into fresh gen-(g+1) units K and K+N (created in the SHARED backing so the later redistribution is copy-free), splitting by `ChildUnit`. The freeze flag is a bool flip, not a synchronous quiesce, so a writer that passed the gate a moment before the flip may still be mid-`Put`. Two layers make the source quiescent: (a) the bisect takes the per-old-unit write-pause WRITE side around the copy (exactly as the single-node bisect does), which BLOCKS until every in-flight writer that ALREADY HOLDS the pause RLock has finished; (b) a writer that passed the freeze gate but had not yet taken the RLock (descheduled in the gap that spans the ring lookup) is refused by a freeze RE-CHECK under that same RLock in the write path - it gets a retryable error, never an ack. Layer (a) alone is insufficient (such a late writer is invisible to the drain and would resolve at gen g, since FLIP runs after BISECT, land in the old unit, ack, and be lost at FLIP); the re-check is what makes "no NEW writer lands a write under the freeze" structurally true, so no catch-up pass is needed. The children are CLEARED before the copy so a retry after an aborted run starts from an empty child (a key deleted between the aborted run and the retry is not resurrected). Each node reports bisect-done; coordinator WAITS for all.
  3. **FLIP (barrier).** Coordinator RPCs all `ReshardFlip(g+1)`. Each node ATOMICALLY advances its `genState` to gen g+1 (routing now resolves the 2N units) and RETIRES its old gen-g units (`CloseUnit`). Each acks; coordinator WAITS for all. No node flips until every node has bisected; the freeze still holds. FLIP applies SEQUENTIALLY; a failure PARTWAY (an earlier node flipped, a later node did not) leaves a documented out-of-scope STRADDLE (see FAIL-SAFE ABORT).
  4. **RESUME + REDISTRIBUTE.** Coordinator RPCs all `ReshardResume`, RETRYING the nodes that have not yet acked until all do (bounded). Each node unfreezes; writes resume at gen g+1. The 2N units then redistribute across nodes by the REUSED Phase 3 lease handoff: the ring re-keys onto the 2N generation-qualified ids; reconcile acquires/releases. Copy-free, because the new units already live in the shared backing. A node the coordinator never reaches with RESUME (it crashed and restarted, or every retry was dropped) is NOT stranded frozen forever: the self-heal reconcile auto-unfreezes a node that has FLIPPED to the target generation but stayed frozen past the RESUME-retry budget, and `ReshardFreeze` tolerates re-freezing such an already-flipped node so a later reshard is not deadlocked.

**SAFETY INVARIANT: NO ACKED WRITE IS LOST.** Acked writes (before the freeze) are in the gen-g units, copied into the gen-(g+1) units during the bisect, and visible after the flip. The freeze flag flip alone does NOT make the source static for an in-flight writer (a writer past the gate before the flip may still be landing a `Put`); the bisect's per-old-unit write-pause WRITE side drains those writers before the copy, so the copy captures every acked write - that drain is load-bearing, not the bool flip. Writes attempted DURING the freeze get a retryable error (never acked) and succeed on client retry after RESUME, at gen g+1. No write is ever routed to a unit being retired (retirement happens at FLIP, under the freeze, after the bisect captured every gen-g write).

**READ AVAILABILITY (strict during FREEZE/BISECT, retryable across FLIP).** Reads are NEVER gated by the freeze flag, so during FREEZE and BISECT every read is served directly from the live gen-g units (strictly always-available). Across the brief FLIP + redistribution window, generations are momentarily STAGGERED across nodes (one node at g+1, a peer still at g): a read forwarded from a peer-at-g lands on a node that resolves at g+1 and may not yet mount (or, post-ring-re-key, no longer own) the resolved gen-(g+1) unit, returning the RETRYABLE acquiring-window error (`codes.Unavailable`) or the ring-refresh loop-guard (`codes.FailedPrecondition`). This is the same retryable window any membership/generation change has; a client that retries (as the gate's read helper does) sees a successful read. So reads are STRICTLY always-served during FREEZE/BISECT and RETRYABLE-available across the FLIP window - never permanently failed, never a wrong/stale value.

**FAIL-SAFE ABORT (bounded).** If ANY node fails to ack a FREEZE / BISECT phase (error or timeout), the coordinator RPCs all `ReshardAbort`: every node UNFREEZES, discards any half-built gen-(g+1) units (harmless - not yet routed), and STAYS at gen g. The cluster never half-reshards. A node that crashed mid-reshard before its FLIP restarts at gen g (it never flipped) - consistent with the survivors. The FLIP phase is the exception: it applies SEQUENTIALLY, so a failure after an earlier node already advanced leaves a STRADDLE (some nodes g, some g+1) that ABORT cannot un-flip (un-flipping a committed generation is itself the inconsistency ABORT prevents). The coordinator surfaces a DISTINCT `ErrReshardStraddle` so the operator knows recovery is MANUAL (re-drive the reshard once every node is reachable), not a clean automatic retry - the same out-of-scope class as a concurrent membership change. A two-phase prepare/commit FLIP that would make FLIP atomic across nodes is a longer-term option, out of scope here. Membership is assumed STABLE for the reshard's duration; the coordinator snapshots the member IDENTITY set and ABORTS before each barrier on ANY add, remove, OR count-preserving swap (X leaves + Y joins), not merely a count change. The whole flow is serialized by `reshardMu`, so two reshards cannot overlap.

**THE MULTI-NODE LOSSLESS-RESHARD GATE (the data-loss oracle).** The multi-node freeze barrier is validated by a dedicated in-process MULTI-NODE integration gate (`tests/integration/lossless_multinode_reshard_gate_test.go`), the cluster-wide-freeze analogue of the single-node Phase 4 gate. It stands up a 2-3 node cluster on the shared-backing factory, writes a recorded dataset spanning every unit (hundreds of keys plus co-located {tag} sets), runs a CONCURRENT probe that keeps acking writes through the full routed surface (retrying the freeze-window retryable error, recording only keys it got an ACK for), then triggers a coordinator-driven `Reshard()`. After the barrier settles it asserts: (1) THE ORACLE - every baseline key AND every acked probe key is readable with its EXACT value from ANY node, zero loss, co-located sets intact; (2) the cluster reached gen g+1 (2N units) correctly partitioned across nodes by the Phase 3 redistribution; (3) reads stayed available DURING the freeze (a read mid-reshard, while a node is frozen, succeeds STRICTLY - served from the live gen-g units, no retry) AND a read issued continuously across the WHOLE reshard (including the staggered FLIP window) is RETRYABLE-available (succeeds with the standard Unavailable-retry helper, never permanently failed), matching the strict-during-FREEZE/retryable-across-FLIP read model; and writes resumed after. It is kept honest by a BREAK DEMONSTRATION that deliberately violates the barrier (one node skips the freeze, so a write slips through at gen g into a unit that is then retired) and shows the oracle FAILS (catches the lost write), plus an ABORT path that forces a phase failure and asserts the cluster stays at gen g, unfrozen, with data intact. If the oracle ever passes while an acked write is silently lost across a multi-node reshard, the gate is broken.

**IN SCOPE:** safe multi-node growth by doubling via the cluster-wide freeze, reusing the Phase 4 per-node bisect (with its write-pause drain + clear-before-copy) + the Phase 3 redistribution + the handoff-window retryable-error shape. The legacy per-node path and the single-node `Reshard` (Phase 4) are UNCHANGED - a single-node cluster skips the freeze protocol entirely. **OUT OF SCOPE:** online (no-freeze) multi-node reshard; dual-write zero-interruption; halving / shrink; concurrent membership-change + reshard (ABORT on any identity change); a partial-FLIP straddle (surfaced as `ErrReshardStraddle`, recovered manually - a two-phase atomic FLIP is the longer-term fix); R>1 / relaxed durability (still R=1).

#### Generation propagation to a joining node

The freeze barrier reshards the nodes that are PRESENT when `Reshard()` runs. A node that JOINS later must arrive at the cluster's LIVE generation, or it routes at the wrong one. This subsection specifies how a joiner learns the generation before it serves any key.

**The hazard (why a fresh joiner is wrong by default).** A node boots its routing state at generation 0: `initGenState` seeds `genState{gen: 0, count: UnitCount}` and the generation advances ONLY via a reshard the node itself participates in (the FLIP handler, or the single-node bisect). After the cluster has resharded to gen g (2^g * N units), a node that joins starts at gen 0 / N units. Routing is generation-qualified end to end - a key resolves to `GenUnit{gen, UnitForHash(h, count)}` and the ring places a unit by `genUnitBytes(GenUnit)` (the generation is hashed AHEAD of the unit id), so the gen-0 id and the live gen-g id of the same key hash to DIFFERENT ring positions. The gen-0 joiner therefore: (a) as an originator, forwards a key to whichever node owns its gen-0 unit, and that node - routing at gen g - disclaims it (`forwarding loop refused: this node does not own the key`); (b) as a ring owner of some gen-0 unit ids, accepts forwarded ops for keys nobody at gen g routes to it. Either way an acked write is lost: the originator never reaches the live owner. This is the `join-after-reshard` data-loss path the chaos harness surfaces (seeds that fire reshard-then-add-node).

**Why reconcile / settle does NOT self-heal it.** The anti-entropy reconcile (`reconcileUnits` -> `desiredGenUnits`) computes the desired unit set against the node's OWN `genSnapshot()`. At gen 0 the joiner "correctly" mounts the gen-0 units the ring assigns it and releases everything else - it is perfectly consistent with its own wrong generation and has no work to do. The stale-freeze self-heal only ever LOWERS a freeze on an already-flipped node; nothing in the steady-state machinery RAISES a node's generation. A generation is advanced by exactly two code paths (FLIP under the barrier, the single-node bisect), neither of which a passive joiner runs. So the joiner stays at gen 0 forever; the loss does not heal. The gap is therefore specifically RESHARD then ADD-A-NODE - the single-node reshard and the fixed-membership multi-node reshard paths are unaffected (their existing lossless gates still hold).

**The fix: query a seed for the live generation at Open, before mounting.** When a node opens in multi-backend mode WITH seeds (a joiner, not the founder), it learns the cluster's live `{generation, unit-count}` by a synchronous peer RPC to a seed BEFORE it derives or mounts any unit, then seeds its `genState` from the answer. This is the same direct-peer-RPC pattern the freeze barrier uses (`ReshardControl`), not gossip: a joiner needs a definite generation before it can serve, and an eventually-consistent broadcast cannot give a "I have the live generation before my first serve" guarantee.

  - **Wire surface.** A small cluster-internal RPC on `ShaleNode` (call it `GenState`) returns the responder's current `{gen, count}` from its `genSnapshot()`. The joiner dials a seed (the seeds it was configured with are an existing live member's bind/forwarding endpoint; the joiner resolves a peer's gRPC address from the membership snapshot the join handshake populated, the same way the forwarding path does) and reads one `{gen, count}` value. Reused by no app-facing caller; never called from outside the cluster.
  - **When in the Open sequence.** In `initMultiBackend`, the order becomes: bring up membership + seed the ring (already done), then - only if this node has seeds - query a seed's generation and `commitGenState(genState{gen: G, count: C, cutOver: empty})` BEFORE `desiredGenUnits()` + the mount loop run. The founder (no seeds) keeps `initGenState`'s gen-0 default. Because the mount derivation and the first routable op both happen strictly AFTER `commitGenState`, the joiner never resolves, routes, or owns a key at gen 0: there is NO gen-0 serving window. (`Open` does not return until this completes, and the gRPC server that exposes the joiner's KV surface is registered by the caller only after `Open` returns, so no external request can reach the joiner before its generation is correct.)
  - **What stable-membership buys us.** A membership change mid-reshard ABORTS the reshard (the coordinator's identity-set re-check). So a node only ever joins when the cluster is at a STABLE generation - between reshards, never mid-FLIP. The seed's `{gen, count}` is therefore a single coherent value (`nextCount` zero, `cutOver` empty); the joiner does not have to reason about a mid-cutover state. The unit-count is carried alongside the generation (rather than re-derived from the joiner's configured `UnitCount` doubled g times) so the contract is explicit and a future non-doubling resize cannot silently desync the two. (This stable-generation guarantee rests on the MULTI-node barrier's identity-set abort; the single-node bisect path, taken when only one node is present, has no membership re-check, so a join racing that path is the same out-of-scope window as a concurrent membership change, narrowed but not eliminated.)

**Race-safety (no gen-0 serving window).** The generation is committed inside `Open`, before mounting and before `Open` returns, so by the time any KV method or forwarded RPC can run, `genSnapshot()` already returns the live generation. There is no window in which the joiner resolves a key at gen 0. The joiner tries every visible non-self peer's `GenState` in turn; only if they ALL fail (each seed unreachable / rejecting) does `Open` FAIL rather than fall back to gen 0, so one momentarily-down peer does not fail a join while another healthy seed exists. Failing closed is correct: a joiner that cannot learn the generation from anyone must not serve at the wrong one. A failed `Open` is surfaced to the caller for a supervised restart (the bind-conflict retry wrapper that already wraps `Open` only retries bind errors, not a gen-query failure).

**Multiple concurrent joins.** Each joiner independently queries a seed and seeds its own `genState`; there is no coordination between joiners and no shared mutable state, so N joiners arriving together each converge to the same live generation (every seed answers the same stable value between reshards). A joiner MAY itself be used as a seed by a second joiner before the first has fully settled its mounts - that is safe, because the first joiner committed its generation inside `Open` (before it became reachable), so it answers the correct `{gen, count}` for any `GenState` query the instant it can receive one.

**Interaction with a reshard that starts AFTER the join.** Once the joiner has joined memberlist, it is in every node's `Members()` snapshot. A reshard the coordinator starts AFTER the join therefore includes the joiner in the participant set (it gets FREEZE / BISECT / FLIP / RESUME like any node) and advances it to g+1 along with everyone else. A reshard already IN FLIGHT when the join lands is the stable-membership case the barrier already handles: the coordinator's identity-set re-check sees the new member and ABORTS, so the join never races a half-applied FLIP - the joiner seeds at the pre-reshard generation g, the cluster stays at g, and a subsequent reshard (now including the joiner) moves everyone to g+1 together. Either ordering is consistent: the joiner is never left at a generation no node else is at.

**Validation (the chaos harness is the gate).** The fix is proven by the multi-node chaos/soak harness: the previously-failing seeds (the reshard-then-join seeds, e.g. 7 and 11) pass with ZERO acked-write loss, while the clean seeds still pass. The harness records a write in its oracle ONLY after the cluster returned success and rides out every retryable signal, so a surviving acked write that becomes unreachable (the `forwarding loop refused` / `does not own the key` symptom of a gen-0 joiner) surfaces as a LOST violation. A targeted integration test pins the same property directly: stand up a multi-node cluster, reshard it, add a node, and assert every acked key is readable from the joiner with its exact value (and that the joiner reports the live generation).

**OUT OF SCOPE (unchanged).** A join that races a reshard mid-FLIP is still handled by ABORT (the join forces the coordinator to abort, same as any mid-reshard membership change); this subsection does not add online join-during-reshard. The legacy per-node path and the single-node reshard are untouched - a single-node cluster and a founder both keep the gen-0 default; only a multi-backend joiner WITH seeds performs the generation query.

### slatedb BackendFactory (deployable multi-backend backing)

Everything above (Phase 2 multi-backend node, Phase 3 lease handoff, Phase 4 doubling reshard, the multi-node freeze) is validated IN-PROCESS by the chaos harness against `internal/sharedfactory`, a SHARED-BACKING test double: one shared store, per-unit handles, writer-epoch fencing simulated through a shared epoch registry. That double models exactly the shape a real object-store-backed factory must have, but it is in-memory and per-test. The DEPLOYABLE piece - the one a real `shaled-slate` node mounts to run the multi-backend model against real object storage - is the `storageunit.BackendFactory` whose backing is ONE shared MinIO/S3 bucket and whose per-unit databases are real slatedb instances inside it. This subsection specifies that factory. It lives in the `backends/slate` module (behind the `slatedb` build tag, alongside the single-instance `slate.Slate`) because it depends on the cgo slatedb binding; the core module never imports it.

It is the REAL version of `sharedfactory`: where the double's `Backing` is an in-memory map, the real factory's backing is the bucket; where the double's per-unit `*memory.Memory` is the durable bytes, the real factory's per-unit bytes are a slatedb database under a unit-derived prefix; where the double fences via a shared epoch registry, the real factory fences via slatedb's own writer-epoch protocol. The same `Backing` / `Handle` split applies: ONE `Backing` per cluster (it owns the bucket connection parameters), one per-node `Handle` (implementing `storageunit.BackendFactory`) off that `Backing`, mirroring the chaos harness wiring (`c.backing.Handle()` per node in `tests/chaos/adapter_inproc.go`). Many nodes' Handles point at the SAME bucket, which is what makes a lease handoff copy-free: a unit's bytes live at a fixed prefix that whichever node currently owns the lease opens.

#### GenUnit -> slatedb DbName mapping (the static data-location map)

A `GenUnit{Gen, ID}` maps deterministically to one slatedb `DbName` (the key-prefix within the shared bucket): `dbName = fmt.Sprintf("u/g%d/u%d", gu.Gen, gu.ID)` (a stable, collision-free encoding of the pair - the generation segment ahead of the unit segment, matching the ring's `genUnitBytes` ordering). The bucket is fixed for the whole cluster; only the DbName varies per unit. So `OpenUnit(gu, epoch)` opens a `slate.Slate` configured with `{Bucket: <shared bucket>, DbName: dbName(gu), ...}` - the existing single-instance backend, one instance per unit. This is the STATIC data-location map from "Two maps, opposite natures": a unit's bytes have a permanent home (`bucket/u/g<gen>/u<id>/...`) independent of which node has the handle open, so growth/handoff moves OWNERSHIP (which node holds the open handle), never bytes.

Because the generation is part of the DbName, gen-g unit K and gen-(g+1) unit K are DISTINCT slatedb databases at DISTINCT prefixes that coexist during a doubling bisect (the old keeps serving while the children fill) - the Phase 4 identity requirement, satisfied structurally by the prefix. A common `Bucket` is reused across the cluster; the per-unit DbName is what isolates one unit's LSM from another's inside it. (The existing per-node deploy already carves sub-prefixes by DbName inside one bucket; this generalizes the same isolation to one-database-per-unit.)

A configurable prefix (a `KeyPrefix` on the `Backing` config, prepended ahead of `u/`) lets one bucket host multiple unrelated shale clusters; default empty. The `u/` segment namespaces unit databases away from any unrelated object the operator might keep in the same bucket and gives `OpenUnits()` a single list-prefix to scan.

#### OpenUnit / CloseUnit / CurrentEpoch onto slatedb

- **OpenUnit(gu, epoch).** Opens (or creates on first touch) the slatedb instance at `dbName(gu)` in the shared bucket via the existing `slate.New` path, with `WriteOptions{AwaitDurable: true}` (the multi-backend invariant - see Durability below) and the operator's `Settings` (memtable sized small so many units fit one node, per the constraints section). Opening that instance IS the fence (see Epoch fencing). Returns the `slate.Slate` as a `backend.Backend`. The `Handle` records `gu -> {slate, openedEpoch}` in its in-process open map. Re-opening a `gu` THIS Handle ALREADY holds is a double-open error at ANY epoch (a unit has at most one live writer per handle): the held-check rejects it, and only `CloseUnit(gu)` clears the slot. (This is STRICTER than the `sharedfactory` double, which permits a strictly-higher same-node re-open. The double's strictly-higher re-open closed + reopened the same backing in-process; real slatedb does NOT support reopening the same db prefix WITHIN one process after it has been opened and fenced forward - it trips an internal "stored epoch is lower than local epoch" assertion in an async Rust task, surfacing as a process-level panic. In a real DEPLOY this never arises: each node is a separate OS process, so a node re-acquiring a unit it previously released opens the prefix in a FRESH process with no stale process-local epoch. The in-process chaos soak, which runs N "nodes" in ONE process, does emit these benign async panics on a handoff-back without losing data - the new owner's fresh open fences the stale instance correctly, so the oracle still sees zero loss. The strictly-higher same-node re-open is therefore both unsafe to realize in one process and unnecessary: the SUT's reconcile only ACQUIREs units it does not already hold - it RELEASEs (`CloseUnit`) before any re-acquire across a membership change - so a same-unit re-open without a prior close is never a real production path. Rejecting it closed keeps the factory correct standalone.) The whole open - the held-check, the durable-manifest fence read, the slatedb open, and the map insert - runs under one critical section per `gu` (a per-`GenUnit` open latch), so a same-unit concurrent open SERIALIZES rather than letting two goroutines both pass the held-check and both open. The `epoch` argument is the cluster's best-effort intended floor; the actual fence is authoritative against the durable manifest (next subsection), so a stale floor cannot under-fence.

  CAVEAT (carried from `slate.New`): the slatedb-go object-store config flows through AWS_* PROCESS env vars, which are global to the process. Every unit in one node's `Handle` shares ONE bucket + ONE set of credentials (that is the whole design - one shared backing), so the env writes are identical across units and do not collide. The `Backing` sets them ONCE at construction; per-unit `OpenUnit` does not re-write them. Two Handles in the same process pointing at DIFFERENT buckets is unsupported (the env writes would collide) and is not a configuration the cluster produces - a node has exactly one `Backing`.

- **CloseUnit(gu).** Flushes + shuts down THIS unit's slatedb instance (`slate.Slate.Close`, which calls `Db.Shutdown` -> flush pending writes durable, then destroys the store handle) and removes it from the Handle's open map, WITHOUT touching any other unit's instance and WITHOUT deleting the bucket bytes. The unit's data stays durable at its prefix for the next owner. Idempotent: closing a unit this Handle does not hold is a no-op returning nil. This is the old owner's release half of a handoff and the resharder's retire-old-unit step. Flush-before-release is load-bearing for NO-ACKED-WRITE-LOST: `Db.Shutdown` forces every acked write down to object storage before the lease moves.

- **CurrentEpoch(gu).** Returns the epoch THIS Handle currently holds `gu` open at (the value recorded by the last OpenUnit), and ok=false if this Handle does not have `gu` open. LOCAL in-process view only, exactly per the `BackendFactory` contract: it is NOT the cross-node source of truth (that is the durable manifest writer-epoch). A new owner acquiring a unit it never held gets ok=false and OpenUnit reads the durable epoch and fences above it. Pure query, no mount/unmount side effect.

#### Epoch fencing (the load-bearing safety property)

The single-writer-per-unit guarantee across a handoff is provided by slatedb's OWN writer-epoch protocol, confirmed present in the slatedb-go v0.13.1 binding and already exercised by the slate backend's `TestMinIO_WriterEpochFencing`: when a second writer opens the same `(bucket, dbName)`, slatedb bumps the manifest's writer-epoch, and the prior writer's next operation FAILS rather than silently committing against a stale epoch. The binding surfaces the fence as `ErrorClosed{Reason: CloseReasonFenced}` (`CloseReason = 2`, "Closed because another writer fenced this instance"). That automatic-fence-on-open IS the lease primitive, so the cluster's `Epoch` maps onto it directly: a lease re-acquire at a higher cluster epoch is just the new owner OPENING the unit's slatedb instance, which fences whatever writer (if any) still holds it. The factory does not have to manually bump anything - opening is the fence.

The durable, cross-node source of truth for a unit's current writer-epoch is the slatedb MANIFEST writer-epoch in object storage, readable WITHOUT opening the db (hence without fencing) via the binding's `Admin` surface: `slatedb.NewAdminBuilder(dbName, store).Build()` then `Admin.ReadManifest(nil)` returns the latest `*VersionedManifest`, whose `WriterEpoch uint64` field is the durable epoch (a nil manifest means the unit has never been created -> durable epoch 0, no prior writer to fence). The factory maps slatedb's `WriterEpoch` to the cluster's `storageunit.Epoch` 1:1.

OpenUnit's fencing contract:

  1. The cluster passes an intended `epoch` (its best-effort `durableEpoch+1` floor - it cannot know another node's durable epoch from its in-process view, so the floor may be stale).
  2. The factory reads the durable manifest writer-epoch for `dbName(gu)` via `Admin.ReadManifest(nil)` and computes the TRUE next epoch as `max(intended, durableWriterEpoch+1)`, so the open ALWAYS lands strictly above the durable epoch even when the cluster's floor is stale (the higher-epoch-fences-lower rule, identical to `sharedfactory.acquire`).
  3. Opening the slatedb instance bumps the manifest to that epoch and fences any writer still open at a lower one. The new owner is now the single live writer; the old owner's next write to that unit fails with `CloseReasonFenced`.
  4. The Handle records the opened epoch so `CurrentEpoch` reports it.

The factory's own epoch ARITHMETIC (step 2: `opened = max(intended, durableWriterEpoch+1)`) is the part the cluster's safety actually rests on, and it is pinned by a dedicated test, NOT left to slatedb's fencing to backstop. The fence handoff test passes the new owner an INTENTIONALLY-STALE intended floor (one that is `<= durableWriterEpoch`), so a regression that neutered the arithmetic to `opened = intended` would open at a non-advancing epoch and FAIL the test - whereas a test that always passes a generous intended (already above durable) would pass even with the arithmetic neutered, because slatedb's own monotonic manifest would still advance. The test reads the durable epoch via `Backing.durableEpoch` before the acquire and asserts the post-acquire durable epoch advanced strictly above the stale floor, so the factory - not slatedb - is what the assertion exercises.

A cold acquire (a unit this Handle never held - the common handoff case) NEVER rejects for being "too low": a new owner must always be able to take the lease, and the durable manifest governs the fence regardless of the cluster's floor. Two GenUnits sharing a UnitID but differing in Generation are INDEPENDENT databases at independent prefixes, so opening gen-(g+1) unit K does not fence or touch gen-g unit K - exactly what the online bisect relies on (the old unit keeps serving while the new ones fill).

**The airtight-fencing property.** Unlike the test double (whose `fenced()`-then-store-write sequence has a documented non-atomic gap), slatedb's fence is enforced inside the engine: once a higher-epoch writer has opened the unit, the lower-epoch writer's `Put`/`Delete`/`Commit` fail at the slatedb layer (the manifest epoch check is part of the durable write path), so two nodes can NEVER both land an acked write to the same `(bucket, dbName)`. The handoff window (between old-owner release and new-owner acquire) is the existing retryable-error window the cluster already serves (`errUnitAcquiring` / `codes.Unavailable`); during it the unit is briefly unavailable, never double-written. This is the production realization of the invariant the chaos harness validated against the double.

#### Durability (AwaitDurable=true per unit)

Each unit's slatedb instance is opened with `WriteOptions{AwaitDurable: true}` (the slatedb-go default, set explicitly here). Every acked write is durable in the bucket (WAL flushed to object storage) BEFORE the ack returns, per unit. Combined with flush-before-release in CloseUnit, this is what makes NO-ACKED-WRITE-LOST hold across a handoff: the new owner opening the unit sees every write the old owner acked, because all of them are durable at the unit's prefix before the lease moved. The multi-backend model is R=1 in this phase, so relaxed durability (`AwaitDurable=false`, which needs R>=2) is NOT offered here; the factory pins `AwaitDurable=true` and does not expose the relaxed knob. (The single-instance `slate.Slate` still exposes `WriteOptions` for the legacy per-node deploy; the multi-backend factory simply always sets it durable.)

#### Copy-free handoff (data stays in the bucket; only the open handle moves)

A handoff of unit U from node A to node B: A's Handle calls `CloseUnit(U)` (flush + shut down A's slatedb instance for U's prefix; bytes stay in the bucket); B's Handle calls `OpenUnit(U, epoch+1)` (opens a slatedb instance against the SAME prefix in the SAME bucket, fencing any A writer). No bytes are copied between nodes - B reads U's existing SST/WAL files straight from object storage. This is the deployable realization of the copy-free property the whole v0.8 model is built around: the Data-location map is static (U's prefix never moves), only the Ownership map (which node's Handle has U open) changes.

#### OpenUnits enumeration (discovering units present in the bucket)

`OpenUnits()` per the `BackendFactory` contract returns the units THIS Handle currently has MOUNTED (the keys of the in-process open map), ascending by `(Generation, UnitID)` - the same LOCAL-mounted-set semantics as `sharedfactory.Handle.OpenUnits`, used by the anti-entropy reconcile to diff desired-vs-mounted. That needs only the in-process map; no bucket I/O.

DISCOVERING units PRESENT in the bucket (the prefixes that EXIST, regardless of who has them mounted) is a separate, bucket-scanning query the reconcile + generation/owner derivation needs and which the test double gets for free (the shared `Backing` map is enumerable). The slatedb-go binding's `ObjectStore` exposes NO list API (`ObjectStoreInterface` is empty), so the factory cannot enumerate prefixes through the slatedb handle. Instead the `Backing` enumerates with the S3 client already in the slate module's dependency set (`minio-go`): a list of the bucket under the `<KeyPrefix>u/` prefix with delimiter `/`, mapping each `g<gen>/u<id>/` common-prefix back to a `GenUnit` by parsing the two segments. The `Backing` (which owns the bucket connection params) exposes a `PresentUnits() ([]GenUnit, error)` for this; it is distinct from the Handle's `OpenUnits()` (mounted set) and is what the cluster uses to learn what exists in the backing (e.g. to derive the generation, or to reconcile against units that exist but no node currently mounts). Keeping the present-in-bucket scan on the `Backing` (not the `BackendFactory` interface) preserves the interface's `OpenUnits() = locally-mounted` contract while still giving the deployable factory the bucket-enumeration the in-memory double provided.

#### Testing (real slatedb + real MinIO, fresh bucket per test)

The factory is proven against REAL slatedb instances in a REAL MinIO bucket, gated behind `slatedb` + `integration` build tags (the same gating as `slate_minio_integration_test.go`): a fresh bucket per test, torn down after. The load-bearing tests:

  - **Copy-free fence handoff.** Open unit U on Handle A (epoch 1), write acked keys, `CloseUnit(U)`. Open U on Handle B at epoch 2 (a different Handle off the SAME Backing/bucket). Assert (a) B reads every key A acked (copy-free: same bytes, no copy), and (b) A re-opened at the stale epoch is fenced - a write through a stale-epoch A handle fails (`CloseReasonFenced`), so the two never both write. This is the production analogue of the chaos harness's lossless-handoff gate, run against real slatedb fencing.
  - **Factory epoch arithmetic (regression-sensitive).** A dedicated test that opens U, releases it (advancing the durable manifest epoch), then re-acquires U on a fresh Handle passing an INTENTIONALLY-STALE intended floor at or below the durable epoch, and asserts (via `Backing.durableEpoch` read before + after) the open landed STRICTLY above the durable epoch. This pins `fenceEpoch`'s `max(intended, durable+1)` arithmetic directly: a neutered `fenceEpoch` that returned the stale `intended` verbatim would FAIL this test, where the plain fence-handoff test (which always passes a generous intended) would not catch the regression because slatedb's own manifest still advances.
  - **Independent generations.** Open gen-g unit K and gen-(g+1) unit K; assert writes to one are invisible to the other and neither fences the other (distinct prefixes -> distinct databases).
  - **Durability across release/re-acquire.** Acked writes survive `CloseUnit` + a fresh `OpenUnit` (the bytes are durable in the bucket).
  - **PresentUnits enumeration.** After opening a few units across generations, `Backing.PresentUnits()` returns exactly those GenUnits (parsed from the bucket prefixes), ascending.

The DEFAULT (no-tag) build and the normal suite are unaffected: this code is entirely behind the `slatedb` tag in the `backends/slate` module, so `go build ./...` / `go test ./...` with CGO_ENABLED=0 never compile or run it.

#### The full chaos harness against the REAL factory (durable-handoff losslessness)

The direct factory tests above prove the factory's PRIMITIVES in isolation (open/close/fence/enumerate). The strongest validation runs the SAME in-process multi-node chaos harness that exercises the whole v0.8 model (lease handoff on membership change, the doubling reshard with its cluster-wide freeze, the join-after-reshard generation path) against the REAL slatedb factory instead of `internal/sharedfactory`, with the unchanged no-acked-write-loss oracle. This is what the legacy `test/real-n2-cluster` could not do: a goroutine "kill" still stops a node, but now that node's units are REAL slatedb databases in MinIO, so a survivor's re-`OpenUnit` (the handoff) reads the dead node's acked bytes back FROM OBJECT STORAGE. A passing soak therefore proves DURABLE-handoff losslessness, not just in-memory-shared-map losslessness.

**The test hook (one small seam, no production-path change).** The harness's in-process adapter (`tests/chaos/adapter_inproc.go`) needs ONE shared backing whose per-node `Handle()`s implement `storageunit.BackendFactory`. Both `sharedfactory.Backing` and the real `slate.Backing` already have exactly that shape (`Handle()` -> a `BackendFactory`), so the seam is a tiny `factoryProvider` interface in the chaos package: `NewHandle() storageunit.BackendFactory`, plus `Reset()` (clear durable state between seeds) and `Close()`. The adapter holds a `factoryProvider` instead of a concrete `*sharedfactory.Backing`, and each `node.handle` is the `storageunit.BackendFactory` interface type. The DEFAULT provider (`run` -> `newSharedFactoryProvider`) wraps `sharedfactory.NewBacking()`, so the existing in-memory soak is unchanged in behavior. A SECOND provider, built only under the `slatedb` tag (`tests/chaos/factory_slate.go`, `//go:build chaos && slatedb`), wraps a real `slate.Backing` against a MinIO bucket; the slate-backed soak runs as its own gated test (`TestChaosSoakSlate`) that calls the shared `runWithProvider` body with that provider. The seam adds no method to the shipped `BackendFactory` interface and no code to any production path - it is confined to the `chaos`-tagged test tree.

**Why a per-seed fresh bucket prefix.** The real factory's durable state is the bucket; a slate-backed soak therefore points each seed at a FRESH `KeyPrefix` so one seed's leftover unit databases (and their durable writer-epochs) never leak into the next seed's run (the in-memory double gets this for free by constructing a new `Backing` per run). The slate provider takes the bucket + connection params from the same env the direct integration tests use (`SLATE_MINIO_ENDPOINT` / `SLATE_MINIO_ACCESS` / `SLATE_MINIO_SECRET`); the test creates one fresh bucket for the whole sweep and removes it on cleanup, while `Reset()` rotates the `KeyPrefix` (`seed<N>/`) between seeds within that bucket.

**Adapting the harness budgets + transients to a slow backend.** Three harness changes (all in the `chaos`-tagged tree, none touching the SUT) make the no-acked-write-loss oracle meaningful against a real object store rather than flapping on backend latency:

  - **Settle-budget scaling.** Every `OpenUnit` / flush is an object-store round-trip (~100ms+), so the convergence + `WaitSettled` budgets that are millisecond-scale for the in-memory double are stretched by a `settleScale` multiplier (default 8x for the slate run, 1x for in-memory). Without this, the final sweep reads a half-mounted cluster and reports durable-but-not-yet-mounted units as FALSE `LOST` verdicts. Scaling the BUDGET (not the workload) is the line between "the cluster genuinely lost a write" and "we asked whether it finished mounting before it could."
  - **Settle the post-kill handoff BEFORE a reshard.** The reshard bisect bisects only MOUNTED old units (`mountedOldUnits`). The `reshard_while_down` combination kills a node then reshards; with the in-memory double the survivors re-mount the dead node's units instantly, but a real re-`OpenUnit` is a round-trip, so the harness must `WaitSettled` (every unit mounted on its ring owner) between the kill and the reshard. Otherwise the bisect skips not-yet-reacquired units and their keys never reach a gen-(g+1) child - a loss the harness would (correctly) flag, but one caused by the harness resharding an unsettled cluster, not by the SUT. The reshard's freeze barrier assumes a settled membership+mount; the harness honors that.
  - **Classifying real-backend transients as retryable.** Two real-backend signals are benign, transient, and must be RETRIED by the client, exactly as the SUT contract intends - not treated as a lost write: (1) the slatedb writer-epoch FENCE (`Closed: Reason=2`, "detected newer DB client") surfacing on an op against a node that held a unit whose lease just moved - the data is durable in the bucket under the NEW lease holder; the op re-routes and succeeds; (2) the SUT's documented mid-reshard SPURIOUS `backend.ErrCrossShard` on a single-key `Transact` whose pin key's unit cuts over between the pin and a later same-shard op (the `guardShard` "TODO(reshard-tx)") - explicitly not data loss, cleared by retry once the cut-over completes. The harness retries both, alongside the `codes.Unavailable` / `FailedPrecondition` cutover signals it already rode out. (Two complementary DIAGNOSTIC knobs, `noReshard` and `reshardOnly`, let an operator bisect a loss to handoff-only vs reshard-only; both isolated the above as harness-classification gaps, never SUT data-loss.)

**Gating + scope.** The slate-backed soak is gated `chaos && slatedb` and needs cgo + the native slatedb lib + a running MinIO, so it never runs under `go test ./...` or even under plain `-tags chaos`; it is an explicit, operator-invoked validation (`-tags "chaos slatedb"`, CGO on, MinIO up). Because every unit is now a real LSM with its own memtable, a slate-backed soak runs at a smaller scale than the in-memory soak (fewer units, fewer doublings, a shorter duration per seed) - enough units that handoff + reshard + join-after-reshard all fire, but bounded so N real slatedb instances fit one process. goleak's leak check is extended (slate build only) to tolerate the minio-go HTTP keep-alive pool + slatedb's async rust-runtime background goroutines, which are not Cluster leaks. The pass condition is identical to the in-memory soak: ZERO oracle violations across the seed sweep and a non-vacuous run (acked writes, chaos events, retryable cutover turbulence). A violation here would be a REAL durable-handoff data-loss, the exact failure the in-memory double can only approximate.

#### Slate ReplicaBackendFactory (deployable R>1 multi-backend)

Everything above describes the R=1 slate factory: one durable slatedb database per `GenUnit`, the lease moving copy-free between nodes. The R>1 (replicated) multi-backend model (Phase 2b) needs the slate `Handle` to also be a `storageunit.ReplicaBackendFactory` - to mount a unit AT A REPLICA POSITION, so the R replicas of one unit are INDEPENDENT durable databases. This is the deployable realization of what `internal/sharedfactory` proves in-process (`OpenReplicaUnit` / `CloseReplicaUnit` over a second pair of `ReplicaUnit`-keyed maps). Without it, `cluster.Open` rejects multi-backend mode at `ReplicationFactor > 1` (the cluster type-asserts `ReplicaBackendFactory` and fails fast with "multi-backend mode at ReplicationFactor N requires a ReplicaBackendFactory"). This subsection specifies the addition. It is PURELY ADDITIVE: the R=1 `OpenUnit` / `CloseUnit` / `CurrentEpoch` / `OpenUnits` paths are byte-for-byte unchanged; single-backend and legacy paths are untouched.

The motivating contract is the durability invariant restated for R>1: **no acked write is lost on a single-node loss.** At R=1 a unit's bytes live at one prefix and the lease moves; losing the lease-holder is survivable only because the bytes are durable in the bucket and another node re-opens the same prefix. At R>1 the system additionally tolerates losing the NODE holding one replica position WHILE that position's bytes are still being served, because each of the R replicas is a complete, independent copy at its own prefix. So the load-bearing requirement on the slate factory is that distinct replica positions of one unit NEVER share bytes: opening replica 1 must not touch replica 0's database. That isolation is structural, achieved by the DbName encoding below.

##### Per-(unit, replica) DbName scheme

A `ReplicaUnit{Unit GenUnit, Replica uint8}` maps deterministically to one slatedb `DbName` by appending a replica segment to the unit's existing R=1 DbName:

```
dbNameReplica(ru) = dbName(ru.Unit) + fmt.Sprintf("/r%d", ru.Replica)
                  = "<KeyPrefix>u/g<gen>/u<id>/r<replica>"
```

This is the object-store realization of `ReplicaUnit.String()` (`"g<gen>/u<id>/r<replica>"`), with the `<KeyPrefix>u/` namespace the R=1 path already uses. The replica position is a CHILD prefix of the unit's prefix, so:

- Each `(gen, unit, replica)` triple maps to a DISTINCT slatedb database at a DISTINCT prefix. Replica 0 of `g1/u5` lives at `u/g1/u5/r0`, replica 1 at `u/g1/u5/r1` - non-overlapping prefixes, hence independent LSM/WAL/manifest object sets. THIS DISTINCTNESS IS THE REPLICA-INDEPENDENCE DURABILITY GUARANTEE: no object key under `u/g1/u5/r0/` is ever read or written by the replica-1 database, so losing the node serving r0 cannot truncate, fence, or corrupt r1's bytes, and vice versa.
- The R=1 DbName (`u/g<gen>/u<id>`) and the R>1 replica DbNames (`u/g<gen>/u<id>/r<replica>`) are disjoint key-spaces: an R=1 unit's slatedb internals live directly under `u/g<gen>/u<id>/` (e.g. `u/g1/u5/manifest/...`), while a replica's live one segment deeper. A cluster is configured R=1 OR R>1 for its whole life (the replication factor is fixed at Open), so the two layouts never coexist in one bucket; the disjointness is a belt-and-suspenders property, not a coexistence requirement.
- `dbNameReplica` is defined as a sibling of `dbName` on `BackingConfig`, reusing `dbName(ru.Unit)` verbatim so the unit prefix can never drift between the R=1 and R>1 encodings. Both `BackingConfig` methods delegate to PURE package-level functions (`dbNameFor(keyPrefix, gu)` / `dbNameReplicaFor(keyPrefix, ru)`) that live in a TAGLESS file (no `slatedb`/cgo dependency), so the durability-critical encoding - per-replica prefix disjointness and R=1 backward-compatibility - is unit-testable WITHOUT the heavy slatedb Rust build. The tagged methods and the pure tests share the one encoding, so the test can never drift from the code it pins.

##### Per-replica fencing (replica 0 and replica 1 fence INDEPENDENTLY)

Epoch fencing is per-`ReplicaUnit`, not per-`GenUnit`. Because each replica position is its own slatedb database with its own durable manifest, the writer-epoch lives in the manifest at `dbNameReplica(ru)`. The factory keys all of its fencing and tracking on `ReplicaUnit`:

- `durableEpochReplica(ru)` reads the durable manifest writer-epoch at `dbNameReplica(ru)` via the same `Admin.ReadManifest(nil)` path the R=1 `durableEpoch` uses, returning `(0, nil)` for a position whose database has never been created. A `DurableEpochReplica(ru)` is exported as the R>1 analogue of `DurableEpoch(gu)` so the regression-sensitive epoch-arithmetic test can pin the per-replica arithmetic directly.
- `fenceEpochReplica(ru, intended)` computes `opened = max(intended, durableEpochReplica(ru)+1)` - identical arithmetic to the R=1 `fenceEpoch`, but reading the PER-REPLICA durable manifest. Opening replica 1 reads r1's manifest only, so r0's epoch and r1's epoch advance on entirely separate manifests: a fence of r0 never bumps r1, and re-acquiring r0 at a higher epoch leaves r1 untouched. This is exactly the `sharedfactory` property where `replicaEpochs[ru]` is keyed by the full `ReplicaUnit`.

##### OpenReplicaUnit / CloseReplicaUnit semantics

The two methods mirror `OpenUnit` / `CloseUnit` structurally, keyed by `ReplicaUnit` instead of `GenUnit`. The `Handle` gains a parallel open map (`openReplica map[ReplicaUnit]*mountedUnit`) and a parallel per-`ReplicaUnit` open latch (`openLatchReplica map[ReplicaUnit]*sync.Mutex`), siblings of the R=1 `open` / `openLatch` maps. The R=1 maps and the R>1 maps are independent: a `Handle` never mixes them (a cluster runs R=1 OR R>1), but keeping them separate keeps the R=1 path byte-for-byte unchanged.

- **OpenReplicaUnit(ru, epoch).** Runs under a per-`ReplicaUnit` latch (a same-position concurrent open SERIALIZES; opens of different positions or different units proceed concurrently). Held-check under the latch: if this Handle already holds `ru` open, reject with a double-open error at ANY epoch (one live writer per replica position per handle; `CloseReplicaUnit(ru)` must clear it first) - the same stricter-than-the-double rule the R=1 `OpenUnit` uses, for the same reason (real slatedb cannot reopen a db prefix in-process after fencing it forward). Otherwise: compute the fence epoch via `fenceEpochReplica(ru, epoch)`, open the slatedb instance at `dbNameReplica(ru)` with `WriteOptions{AwaitDurable: true}` and the backing's `Settings`/`Cache`, record `ru -> {slate, openedEpoch}` in `openReplica`, and return the `slate.Slate` as a `backend.Backend`. Opening the instance IS the fence (slatedb's writer-epoch protocol on r's own manifest), so a stale prior writer of the SAME position is locked out; a writer of a DIFFERENT position is never touched. A cold acquire never rejects for being too low - a new owner must always be able to take the position's lease.
- **CloseReplicaUnit(ru).** Removes `ru` from `openReplica` and `slate.Close`s its instance (`Db.Shutdown` flushes pending writes durable, then destroys the store handle), WITHOUT touching any other replica or unit and WITHOUT deleting the bucket bytes - the position's data stays durable at `dbNameReplica(ru)` for the next owner. Idempotent: closing a position this Handle does not hold is a no-op returning nil. Flush-before-release is load-bearing for the invariant: every acked write to r is durable in the bucket before the position's lease moves.
- **Close() (whole-handle shutdown).** Already best-effort flushes every R=1 unit in `open`; it additionally drains `openReplica`, `slate.Close`-ing each mounted replica. (`OpenUnits()` / `CurrentEpoch` keep their R=1-only `GenUnit` semantics per the base `BackendFactory` contract; the replica positions are an R>1-only concern the cluster tracks via its own `replicaPos` map, and the reconcile's mounted-set diffing for R>1 is a cluster-layer concern, not surfaced through the base interface.)

A compile-time assertion `var _ storageunit.ReplicaBackendFactory = (*Handle)(nil)` lives in the slatedb-tagged file, alongside the existing `var _ storageunit.BackendFactory = (*Handle)(nil)`, so the capability is asserted only when the `slatedb` tag is built.

##### Invariant (replica independence = no acked write lost on single-node loss)

The factory upholds: for any unit `gu` replicated at R positions across R distinct nodes, the loss of ANY ONE node leaves every acked write to `gu` recoverable from the surviving R-1 positions, because (a) each position is an independent durable database at a distinct prefix (the DbName scheme), (b) every acked write is durable in the bucket before the ack (`AwaitDurable: true`), and (c) a surviving node re-opening a lost position reads that position's complete bytes straight from object storage (copy-free, same as the R=1 handoff). Per-replica fencing ensures that re-open does not depend on or disturb any other position. The cluster layer (quorum write/read, read-repair) provides cross-position convergence; the factory's sole job is to make the R positions INDEPENDENT and DURABLE, which the prefix-disjoint DbName encoding plus per-replica `AwaitDurable` fencing deliver.

##### Testing (additive, real slatedb + real MinIO)

The R=1 factory tests are unchanged. The DbName encoding has a PURE (tagless, no cgo) test suite (`dbname_test.go`) that pins it without the slatedb build: it regression-pins the exact R=1 per-unit strings (the on-disk contract a deployed cluster's bytes live at), the exact R>1 per-replica strings, that replica 0 and replica 1 of one unit are DISTINCT, that a swept grid of `(gen, unit, replica)` triples is globally collision-free AND prefix-overlap-free (the object-store realization of replica independence), and that each replica DbName extends its unit's R=1 DbName verbatim. New gated (`slatedb` + `integration`) tests then prove the end-to-end behavior against real slatedb + real MinIO, mirroring the R=1 ones at a replica position:

- **Replica independence.** Open `ru0 = {gu, 0}` and `ru1 = {gu, 1}` on one Backing; write distinct acked keys to each; assert neither sees the other's keys and neither's open fences the other (distinct prefixes -> distinct databases). This is the direct proof of the load-bearing isolation property.
- **Per-replica fence handoff + arithmetic.** Open `ru` on Handle A, write acked keys, `CloseReplicaUnit(ru)`; re-acquire `ru` on Handle B passing an INTENTIONALLY-STALE intended floor; assert (via `DurableEpochReplica` read before/after) the open landed strictly above r's durable epoch, and a write through the stale-epoch A handle is fenced (`CloseReasonFenced`). Pins `fenceEpochReplica`'s arithmetic directly, exactly as the R=1 arithmetic test pins `fenceEpoch`.
- **Durability across release/re-acquire of a position.** Acked writes to `ru` survive `CloseReplicaUnit` + a fresh `OpenReplicaUnit` (bytes durable at the position's prefix).

The strongest validation is the existing chaos soak run at R>1 against the real factory (the `factoryProvider` seam already hands the cluster a `Handle` that now satisfies `ReplicaBackendFactory`), with the unchanged no-acked-write-loss oracle.

### v0.8 Deploy gap: the multi-backend model in the deployable binary

Everything above (lease handoff, doubling reshard, the slatedb `BackendFactory`) is validated IN-PROCESS only: the in-process chaos harness drives a `cluster.Cluster` constructed with `BackendFactory + UnitCount` directly, and the slate factory is proven against real MinIO via the direct factory tests. But the deployable `shaled-slate` binary still runs LEGACY single-`Backend`-per-node mode: it calls `slate.New` for one DB and hands that single `backend.Backend` to `shaled.Run`, which `cluster.Open`s with `Backend:` set and `BackendFactory`/`UnitCount` unset. So an operator cannot deploy the multi-backend model, and cannot trigger a reshard on a running cluster. Three pieces close the gap.

#### Operator config surface for multi-backend mode

`shaled-slate` gains a `--unit-count` flag (env `SHALE_UNIT_COUNT`, default `1`). When `--unit-count` is `1`, the binary keeps the legacy single-`Backend` path byte-for-byte (a single unit is the degenerate multi-backend case, but the legacy path is simpler and is the established default; no behavior change for existing deploys). When `--unit-count > 1` (it must be a power of two, validated by `storageunit.NewUnitCount`), the binary instead constructs a slate `Backing` over the configured shared bucket (`slate.NewBacking(slate.BackingConfig{Bucket, Endpoint, Region, AccessKey, SecretKey, UseSSL, KeyPrefix})` - the SAME `--slate-*` flags already parsed, minus `--slate-db-name`, which multi-backend mode IGNORES because per-unit DbNames are derived from the `GenUnit`), takes a per-node `Handle()` off it, and passes `BackendFactory: handle, UnitCount: count` into `cluster.Open` with `Backend: nil`. The shared bucket is the durable backing every node points at; the `--slate-db-name` per-node isolation of the legacy path is replaced by the factory's per-`GenUnit` DbName mapping, so all nodes share one bucket and a unit's bytes are reachable by whichever node currently leases it (copy-free handoff).

This requires `shaled.Run` / `RunConfig` to carry the multi-backend shape, since today it only accepts a single `Backend`. `RunConfig` gains optional `BackendFactory storageunit.BackendFactory` + `UnitCount storageunit.UnitCount` fields; when set, `Run` builds `cluster.Config` with them instead of `Backend`. The XOR is already enforced downstream by `cluster.validateBackendMode` (set both modes -> error), so `Run` just forwards whichever the caller populated. `CloseBackend` semantics are unchanged (the factory's `Backing`/`Handle` own their slatedb instances; `Cluster.Close` closes mounted units, and the binary's `CloseBackend` releases any `Backing`-level resources). Multi-backend mode supports `ReplicationFactor > 1` because the slate `Handle` is a `ReplicaBackendFactory` (see "Slate ReplicaBackendFactory" above); at R=1 the cluster uses only the base `BackendFactory` path.

#### Operator Reshard RPC

A NEW operator-facing RPC `Reshard` is added to `ShaleNode` (distinct from the existing cluster-INTERNAL `ReshardControl`, which carries individual barrier phases between nodes and is never called from outside the cluster). `ReshardRequest{}` is empty (the only supported reshard is a doubling N -> 2N; no parameters). `ReshardResponse{ from_unit_count, to_unit_count, error }` reports the generation transition or a typed failure. The server handler calls `c.Reshard()` (the existing entrypoint that routes to `reshardCoordinated()` on a multi-node cluster or the single-node path otherwise) and maps its result: a clean return reports the doubled counts; `c.Reshard`'s own guards surface as the `error` string (legacy mode -> "Reshard is only valid in multi-backend mode"; mid-reshard -> `reshardMu` already held serializes a second call; the coordinated flow ABORTS and returns an error if membership is unstable across the freeze barrier). Carrying the failure in the response field (not a gRPC status) matches the `CommitCAS` / `ApplyBatch` / `ReshardControl` wire convention.

**Who can call it + safety.** Any client that can reach a node's gRPC surface (the `shale` CLI gains a `shale reshard` subcommand that dials one node and issues the RPC). Idempotency + safety are inherited from the existing barrier, not re-implemented: `reshardMu` serializes whole reshards (a concurrent second `Reshard` blocks, then runs against the already-doubled generation and doubles again - the operator gets distinct from/to counts, never a corrupt straddle); the coordinated flow re-checks the member IDENTITY set (not just count) before BISECT and before FLIP and ABORTs on any drift, so a reshard issued while membership is unstable refuses cleanly and stays at gen g; the up-front `count.Double()` ceiling check means a reshard either fully proceeds or fully refuses. The RPC adds NO new safety logic - it is a thin trigger over the validated barrier.

#### Validation: the chaosreal harness against the deployable binary

The real-cluster chaos adapter (`tests/chaos/adapter_real.go`, `//go:build chaosreal`) drives a cluster of SEPARATE `shaled-slate` OS processes over real gRPC + real memberlist + a shared MinIO bucket. Today its `Reshard()` seam returns `errReshardUnsupported` because the deployed binary is legacy-mode and there is no operator Reshard RPC. Once `shaled-slate` runs multi-backend mode (launched with `--unit-count > 1`) and the `Reshard` RPC exists, the adapter's `Reshard()` is wired to dial a node (the founder) and issue the operator `Reshard` RPC, and `errReshardUnsupported` is removed. The chaosreal launcher passes `--unit-count` (a new `SHALE_REAL_UNIT_COUNT` env, default a power of two > 1 so the model actually engages) and DROPS the per-node `--slate-db-name` (multi-backend mode shares one bucket; per-unit DbNames are factory-derived). The pass condition is the one the in-process soak proves, now end-to-end across a process boundary and a real reshard: ZERO acked-write loss across a real N=1 -> N=2 doubling, with the unchanged no-acked-write-loss oracle consuming acks from the real gRPC client. This is the validation that the v0.8 model is actually DEPLOYABLE, not merely in-process correct - the gap the chaosreal adapter was built to catch.

#### The deployable run path (exact wiring contract)

The two sections above state WHAT the deploy gap closes; this section pins the EXACT wiring so the implementation builds to a single contract. It does NOT change any `pkg/cluster` multi-backend LOGIC (the Phase 2/2b/3/4 + freeze-barrier machinery is unchanged): it only exposes that already-validated machinery to a deployable binary. The single-backend path (the `memory` / `pebble` / single-`slate` binaries) stays byte-for-byte except that `ReplicationFactor` is now threaded through (today it is dropped, pinning single-backend to the cluster default R=1; see below). The carried-over invariant is unchanged: NO ACKED WRITE IS LOST. This wiring acks a write exactly when `cluster.Cluster` does; it adds no write path.

**New `shaled` standard flags (`pkg/shaled.StdConfig` / `BindStdFlags`).** Two flags are added to the SHARED std set so every `shaled-*` binary accepts them uniformly:

  - `--replication-factor` (env `SHALE_REPLICATION_FACTOR`, default `1`). Parsed as an int; `0` is normalized to `1` by `cluster.normalizeConfig` (so a default `0` and an explicit `1` are identical). It is threaded into `cluster.Config.ReplicationFactor` in BOTH single-backend and multi-backend modes. In single-backend mode it selects the legacy per-node R>1 path (already in `pkg/cluster`); in multi-backend mode the cluster enforces its own rule (R==1 unless the factory is a `ReplicaBackendFactory`, validated by `validateBackendMode`). The slate `Handle` IS a `ReplicaBackendFactory` (see "Slate ReplicaBackendFactory" above), so a `shaled-slate --multi-backend` launched with `--replication-factor > 1` mounts each unit's R replica positions as independent slatedb databases - no validation error.
  - `--unit-count` (env `SHALE_UNIT_COUNT`, default `1`). Parsed as an int and validated by `storageunit.NewUnitCount` (power of two in `[1, 2^30]`); a non-power-of-two errors at flag-validation time (`StdConfig.Validate`), before any backend opens. It is consumed ONLY in multi-backend mode (see the `RunConfig` contract); in single-backend mode it is ignored (default `1`). `StdConfig` stores the validated `storageunit.UnitCount` value so `Run` never re-parses it.

`StdConfig.Validate` (already called by every `shaled-*` main after `fs.Parse`) gains the `--unit-count` power-of-two check and stores the parsed `UnitCount` + `ReplicationFactor`. The std flag set stays backend-agnostic; whether a binary can actually USE `--unit-count > 1` depends on whether its main supplies a `BackendFactory` (only `shaled-slate` does today).

**`RunConfig`: the Backend-vs-BackendFactory contract (exactly one, validated).** `pkg/shaled.RunConfig` today carries only a single `Backend backend.Backend` (+ `CloseBackend func() error`). It gains an ALTERNATIVE multi-backend shape:

  - `BackendFactory storageunit.BackendFactory` - the per-node factory (the slate `Handle`, or any `storageunit.BackendFactory`). Mutually exclusive with `Backend`.
  - `CloseFactory func() error` - the multi-backend analogue of `CloseBackend`: invoked after `Cluster.Close` to release any `Backing`-level resources the factory owns (the slate `Backing`'s shared connection state, an operator-owned block cache, etc.). `Cluster.Close` already closes every MOUNTED unit via the factory; `CloseFactory` is for backing-level teardown the cluster does not own.

`Run` validates the XOR at the top, BEFORE binding the listener: exactly one of `Backend` / `BackendFactory` must be non-nil. Both set -> error (`shaled.Run: set EITHER Backend OR BackendFactory, not both`). Neither set -> error (`shaled.Run: Backend or BackendFactory required`). This is a fail-fast guard in `shaled` mirroring `cluster.validateBackendMode`; the cluster re-validates downstream, but failing in `Run` gives a clearer per-binary error and avoids reserving a listener for a misconfigured node. `BackendLabel` + `Logger` semantics are unchanged.

**How `Run` threads `ReplicationFactor` + `UnitCount` into `cluster.Config`.** `Run` builds ONE `cluster.Config` from `cfg.Std` plus whichever backend field is set:

  - common (both modes): `NodeID`, `BindAddr`, `GRPCAddr` (resolved listener addr), `Seeds` as today, PLUS `ReplicationFactor: cfg.Std.ReplicationFactor` (this is the fix: single-backend `Run` today omits it, pinning the legacy path to the cluster default; now both modes carry the operator's R).
  - single-backend mode (`cfg.Backend != nil`): `Backend: cfg.Backend`. `UnitCount` is left zero (`cluster.validateBackendMode` requires it zero in legacy mode).
  - multi-backend mode (`cfg.BackendFactory != nil`): `BackendFactory: cfg.BackendFactory`, `UnitCount: cfg.Std.UnitCount`, `Backend: nil`.

`ReadConsistency` / `WriteConsistency` / timeouts / rebalance settings stay at the `cluster` defaults (normalized by `cluster.normalizeConfig`); this milestone does not add operator flags for them. The teardown sequence in `Run` is unchanged except it calls `CloseFactory` (multi-backend) or `CloseBackend` (single-backend) after `Cluster.Close`, whichever is set.

The `RunConfig` -> `cluster.Config` mapping is factored into two pure (no-I/O) helpers so the wiring is testable WITHOUT binding a listener or serving gRPC: `clusterConfig(cfg, grpcAddr)` does the field mapping above (R + UnitCount + single-vs-multi mode), and `buildCluster(cfg, grpcAddr)` validates the Backend-vs-BackendFactory XOR then calls `cluster.Open`. `Run` reserves the listener, then calls `cluster.Open(clusterConfig(cfg, resolvedAddr))`; a test calls `buildCluster` directly with an in-process `BackendFactory` + `ReplicationFactor` and exercises `Put`/`Get` on the returned cluster (single-node clusters never dial the broadcast `grpcAddr`, so the test passes any placeholder). This is a structural extraction only; the live `Run` path opens the SAME `cluster.Config`.

**`shaled-slate`: building the slate `Backing` / `Handle` in multi-backend mode.** `backends/slate/cmd/shaled-slate` gains a `--multi-backend` flag (env `SHALE_MULTI_BACKEND`, default `false`):

  - `--multi-backend=false` (default): the EXISTING single-backend path, byte-for-byte. `openSlateBackend(cfg)` opens ONE `slate.New(Config{...DbName...})` and `Run` gets `Backend:` set. `--slate-db-name` is required (as today). `--unit-count` is ignored.
  - `--multi-backend=true`: the binary builds a slate `Backing` from the SAME `--slate-*` connection flags (`slate.NewBacking(slate.BackingConfig{Bucket, Endpoint, Region, AccessKey, SecretKey, UseSSL, KeyPrefix})`), takes a per-node `handle := backing.Handle()` (which implements `storageunit.BackendFactory`), and passes `BackendFactory: handle` + `CloseFactory: backing.Handle close + any backing teardown` into `RunConfig`. `--slate-db-name` is IGNORED in this mode (per-unit DbNames are derived from each `GenUnit` by the factory); it is not required when `--multi-backend=true`. `--unit-count` MUST be `> 1` for the model to engage (a `--multi-backend=true --unit-count=1` is the degenerate single-unit multi-backend case; it is legal but pointless, and the validation does not forbid it). The construction is gated the same way the single backend is: a `--multi-backend` slate `Backing` needs `-tags slatedb` to open real slatedb, so the multi-backend constructor lives in the `slatedb`-tagged sibling (`backend_slatedb.go`); the `!slatedb` stub (`backend_default.go`) returns the existing "rebuild with -tags slatedb" fast-fail for BOTH the single and multi constructors, so a tag-less build still compiles and refuses to run rather than silently misbehaving. A new `slateConfig`-style field carries `MultiBackend bool` + `UnitCount` so the tag-stub and real impl share the shape without `main.go` importing `backends/slate` directly.

  A `--multi-backend=true` requires `--slate-bucket`, `--slate-access-key`, `--slate-secret-key` (same as single mode); it does NOT require `--slate-db-name`. The slate `KeyPrefix` is exposed as a new `--slate-key-prefix` flag (env `SHALE_SLATE_KEY_PREFIX`, default empty) so one bucket can host multiple unrelated clusters; it is read only in multi-backend mode (the single-backend `slate.Config` has no `KeyPrefix`, it uses `DbName` directly).

**`Dockerfile.slatedb` for `shaled-slate`.** A `backends/slate/cmd/shaled-slate/Dockerfile.slatedb` makes the multi-backend slate node containerizable. It mirrors the proven 3-stage hostthis pattern, pinned to the SAME SlateDB version as `backends/slate/go.mod` (`slatedb.io/slatedb-go v0.13.1`, so `ARG SLATEDB_VERSION=v0.13.1`):

  1. **uniffi-build** (`rust:1.91-slim-bookworm`): `git clone --depth 1 --branch ${SLATEDB_VERSION} https://github.com/slatedb/slatedb.git` then `cargo build --release -p slatedb-uniffi`, producing `libslatedb_uniffi.so`.
  2. **go-build** (`golang:1.26-bookworm` + gcc/libc6-dev): copies the `.so` to `/usr/local/lib`, then `CGO_ENABLED=1 CGO_LDFLAGS="-L/usr/local/lib" LD_LIBRARY_PATH=/usr/local/lib go build -tags slatedb -buildvcs=false -ldflags="-s -w" -o /out/shaled-slate ./cmd/shaled-slate`, from the `backends/slate` module root (so the module's own `go.mod` resolves; `GOWORK=off` for the release build per the repo's go.work note). `GOOS`/`GOARCH` derive from `uname -m` exactly as the hostthis Dockerfile does.
  3. **runtime** (`gcr.io/distroless/cc-debian12:nonroot` - cc, not static, for the glibc dynamic loader): copies `libslatedb_uniffi.so` to `/usr/local/lib` + the `shaled-slate` binary, sets `LD_LIBRARY_PATH=/usr/local/lib`, `EXPOSE 7946 7947` (memberlist + gRPC), `ENTRYPOINT ["/shaled-slate"]`. The operator supplies the cluster + slate + multi-backend config via the `SHALE_*` env vars (`SHALE_NODE_ID`, `SHALE_BIND_ADDR`, `SHALE_GRPC_ADDR`, `SHALE_SEEDS`, `SHALE_UNIT_COUNT`, `SHALE_MULTI_BACKEND`, `SHALE_SLATE_*`).

  The Dockerfile is NOT built as part of this milestone (the Rust compile is heavy; the operator builds it with colima). It is committed alongside the wiring so the deployable artifact is reproducible.

**Scope boundary.** This subsection is PURE WIRING: new flags, the `RunConfig` XOR, the `Run` config construction, the `shaled-slate` `--multi-backend` constructor, and the Dockerfile. It changes NO `pkg/cluster` multi-backend behavior, does NOT regress the single-backend path (existing `shaled-*` binaries run identically, now correctly honoring `--replication-factor`), and relies entirely on the already-in-process-validated cluster logic for the NO-ACKED-WRITE-LOST guarantee. Tests are part of the same change: `StdConfig.Validate` rejecting a non-power-of-two `--unit-count`; `Run` erroring on both-set and neither-set; `Run` threading `ReplicationFactor` + `UnitCount` into the `cluster.Config` it builds (a fake/recording `cluster.Open` seam or an in-process memory `BackendFactory` so the test needs NO `-tags slatedb` and NO MinIO); `shaled-slate` flag parsing selecting single-vs-multi. The end-to-end real-process validation is the chaosreal harness (previous subsection), run by the operator under `-tags chaosreal` with a built image.

#### Caveats fixed in this milestone (shipped together)

The slate factory review left two P2 + one P3; all are fixed here, since the factory now ships in the deployable binary:

  - **(P2, FIXED) Fence-epoch test sensitivity.** The factory's fence integration test passed even with a NEUTERED `fenceEpoch` (it proves slatedb fences, but not that the factory computes the RIGHT intended epoch). FIX: a dedicated `TestFactory_FenceEpochArithmetic` pins the factory's epoch ARITHMETIC directly - it re-acquires a released unit passing an intentionally-STALE intended floor (at or below the durable epoch) and asserts, via `Backing.DurableEpoch` read before + after, that the open landed STRICTLY above the durable epoch. A regression that returned the stale `intended` verbatim fails this test instead of hiding behind slatedb's own monotonic manifest.
  - **(P2, FIXED) `Handle.OpenUnit` lock window + unsafe same-node re-open.** `OpenUnit` released its mutex between the held-check and the final map insert, with the real slatedb open in between - a narrow same-unit concurrent-open window where two goroutines could both pass the held-check and both open. FIX: a per-`GenUnit` open latch serializes the WHOLE open (held-check, fence read, slatedb open, map insert) per unit, so a same-unit concurrent open serializes. The fix also tightened the held-unit contract: a re-open of an already-held unit is now rejected at ANY epoch (the prior code's strictly-higher same-node re-open closed + reopened the same slatedb db in-process, which panics a slatedb async task; that path was dead in the SUT, which always `CloseUnit`s before re-acquiring). See OpenUnit's contract above.
  - **(P3, FIXED) Substring fence classification.** `tests/chaos/harness.go isFenceTransient` classified fence errors by SUBSTRING match on `"Reason=2"` (brittle: a slatedb error-string change silently reclassifies a real fence as a real loss). FIX: the slate backend exposes a typed sentinel `slate.IsFenced(err)` (matches `slatedb.ErrorClosed{Reason: CloseReasonFenced}` via `errors.As`), unit-tested against the real typed error so a binding change is caught by a failing test rather than by silent misclassification. The chaos harness's `isFenceTransient` still falls back to substring matching because it only sees the fence error as a gRPC-transported STRING (the typed error does not survive the wire), but the substring set is broadened to the stable structured fragments (`"detected newer DB client"`, `"Closed error"`) so it no longer hinges on the un-`String()`-ed `CloseReason` rendering alone.

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
- [x] **v0.6.x** - replicate the committed OCC write-set to R replicas, closing the R=1 transactional-write durability gap left by v0.6. Sub-tasks:
  - [x] `ApplyBatch` unary RPC (`repeated EnvelopeWrite`; key + already-encoded envelope) on `ShaleNode`; replica handler applies the whole batch in ONE local transaction, apply-only, no re-validation, migration-guard respected, rolls back on any error
  - [x] `CommitCASApply` envelope-aware at R>1: decode-on-validate (tombstone counts as not-found), shared `Stamp{now, owner NodeID}` for the whole commit, encode-on-apply (Delete written as a tombstone-envelope Put), local commit then fan out the same envelopes
  - [x] R=1 path byte-for-byte unchanged (raw values, no envelopes, no fan-out)
  - [x] Fan-out reuses `fanout` + `requiredWriteAcks`, owner's local commit counts as 1 ack, W-ack target, migration-guard transient handling; under-W returns `codes.Unavailable` (write already durable on owner + acked replicas, same best-effort-to-W model as single-key Put)
  - [x] Owner-local validation soundness documented (owner is always a replica + the write target); quorum-read validation noted as the deferred heavier alternative
- [~] **v0.7** (in progress) - LWW on write (apply-if-newer at the replica). Sub-tasks:
  - [ ] Replica-receiving writes at R>1 are apply-if-newer: decode incoming + stored stamp, `Put` only if `incoming.Stamp.Greater(stored.Stamp)` OR no stored value; older incoming write is a silent no-op
  - [ ] Atomic get-compare-put per key inside one backend transaction, serialized under a dedicated per-node `Cluster.applyMu` (memory backend has no write-write conflict detection; distinct from `casCommitMu`)
  - [ ] Covers single-key replicated Put (both receiving branches), `ApplyBatch` write-set fan-out, and read-repair (for free, it rides the single-key replica path); does NOT cover R=1 or the owner's own validated CAS local commit
  - [ ] R=1 path unchanged (raw values, no envelopes, no apply-if-newer, no lock)
  - [ ] `CommitCASApply` releases `casCommitMu` after the owner-local commit, before the fan-out: reordered fan-outs self-resolve via apply-if-newer, restoring "no lock held across the network"
  - [ ] Fixes the read-repair-clobbers-newer-write lost-update bug under `Quorum` / `All`; owner-local validation now sound under every read consistency
- [~] **v0.8** (in progress) - per-shard lease-handoff storage: copy-free rebalance + online doubling resharding. Replaces the per-node single-database model (one slatedb per node, rebalance copies keys) with a FIXED power-of-two count N of self-contained storage-unit databases whose OWNERSHIP is a lease (the ring is re-keyed onto unit ids; handoff is close-on-old-owner / open-at-epoch+1-on-new-owner, zero bytes copied). N is bounded by per-engine memtable memory; growth by doubling makes resharding an online per-unit bisect with no global re-partition. Full design in the "Per-shard lease-handoff storage" section above. Sub-tasks:
  - [x] Storage-unit domain types (`pkg/storageunit`): UnitID + power-of-two UnitCount, the `hash(ShardKey) & (N-1)` unit map, the doubling bisect (ChildUnit/ChildUnits, exhaustively tested), Epoch + BackendFactory mount/lease seam, OwnedUnits derivation. Pure, no I/O.
  - [x] Multi-backend node (Phase 2, STATIC routing): a backend FACTORY + a `unit -> backend` mount map alongside (and mutually exclusive with) the single `Config.Backend`; the ring re-keyed onto unit ids; routing `key -> unit -> owner`; the unit-based owner guard. Topology is fixed at Open (no rebalance / lease handoff yet). See the "v0.8 Phase 2" section above.
  - [x] Lease-handoff rebalance (Phase 3): close-on-old / open-at-epoch+1-on-new, the handoff window, the anti-entropy reconcile (own-but-not-mounted -> mount). See the "v0.8 Phase 3" section above.
  - [x] Doubling resharder (Phase 4, SINGLE-NODE): generation-qualified unit identity (`GenUnit`), `Cluster.Reshard()`, the online per-unit bisect (background copy split by `ChildUnit`, catch-up drain under a per-unit write-pause, atomic cut-over), and the generation / per-unit-cut-over routing state. Gate-validated lossless under ~252k concurrent acked writes. On a single-node cluster this is the whole reshard; on a multi-node cluster `Reshard` delegates to the cluster-wide-freeze barrier below. See the "v0.8 Phase 4" section above.
  - [x] Multi-node reshard: the coordinator-driven cluster-wide WRITE-FREEZE barrier (freeze / bisect / flip / resume) so a doubling coordinates across nodes safely (writes briefly retryable cluster-wide, reads continue, the per-node bisect is a static copy, atomic flip + retire, fail-safe abort on any node failure or membership change), then the 2N units redistribute via the Phase 3 handoff. Wired as a new `ReshardControl` RPC on the `ShaleNode` service (phase enum FREEZE/BISECT/FLIP/RESUME/ABORT + target generation) driven over the peer-RPC pattern; the per-node freeze flag gates Put/Delete/Begin + the CAS commit write path. See "v0.8 Multi-node reshard (cluster-wide freeze)" above.
  - [x] Generation propagation to a joining node: a multi-backend joiner (Open WITH seeds) learns the cluster's live `{generation, unit-count}` by a synchronous peer RPC (`GenState`) to a seed BEFORE it derives or mounts any unit, then seeds its `genState` from the answer - so it never routes / owns / serves a key at gen 0 after the cluster has resharded. Fixes the reshard-then-add-node acked-write-loss path (a gen-0 joiner orphans keys: `forwarding loop refused`). Fail closed if the seed is unreachable (Open fails, no gen-0 fallback). The founder + single-node + legacy paths keep the gen-0 default. See "Generation propagation to a joining node" above.
  - [ ] Write-availability through an R>1 membership change (Phase 2d, retry-on-acquiring): make `errUnitAcquiring` a TRUE fan-out transient (sentinel-tagged so `isTransientReplicaErr` skips it though it stays `codes.Unavailable` client-facing; the cross-node replica leg re-codes to `codes.ResourceExhausted`), and wrap Put / Delete / CommitCASApply (R>1 AND R=1 multi-backend) in a `WriteTimeout`-bounded retry-on-acquiring with the existing `RebalanceRetryAfterMs` backoff + jitter, so a write to a unit mid-reconcile WAITS for the re-mount instead of erroring. Turns the handoff-window refusal into bounded latency; no-acked-write-lost + single-writer-fence unchanged. Acceptance: an in-process loss-oracle gate that writes continuously through a membership change asserts zero acked loss AND acked/attempted > 95% (vs the measured ~50-64% pre-fix), demonstrated to FAIL on the pre-fix code. Option A's availability is bounded by mount-time-vs-`WriteTimeout` (big-bang 3 -> 12 measured ~54%); Phase 2e (overlap handoff) removes that bound and A is retained as its safety net. See "v0.8 Phase 2d" above.
  - [ ] Pending ranges (Phase 2e, graceful membership transition): `replicasForKey` computes a position's CURRENT replica set (ring INCLUDING draining members) and PENDING replica set (ring EXCLUDING draining members); a position is IN TRANSITION when CURRENT != PENDING (a leave: a current member is `Draining`; a join: this node is a pending owner of a position whose serving marker is absent). During a transition the ROUTED set is UNION(current, pending); the fan-out DUAL-WRITES every union member with the ack bar held at the STABLE R quorum (`W = requiredWriteAcks(WriteConsistency, R)`, NOT raised to cover the union - the pending replica is a bonus target, not a higher bar; `All` is held at the stable R too - `All` means all STABLE replicas, NOT the transient union, so a mid-mount successor never blocks the write); reads fan out across the union and any MOUNTED member serves (a mid-mount pending owner returns `errUnitAcquiring` and is skipped). Pending owners ACQUIRE in the background via the normal `reconcileReplicaUnits` -> `OpenReplicaUnit` (receiving union dual-writes as they mount) and write a SERVING MARKER on mount-complete. ORDERED REMOVAL (CEP-21 order): add pending owners to writes first -> they mount + write markers -> drop the leaver from a position's routed set once that position's marker is present at an epoch strictly above the leaver's open epoch (handoff durably complete) -> the leaver releases (`CloseReplicaUnit`), fenced only AFTER its successor serves. Durability: a write acked at W is durable on >= W INDEPENDENT replica copies; dual-write targets independent per-`(unit, replica)` databases (no two-writers-on-one-db); apply-if-newer/LWW makes the same stamped envelope idempotent across union members + retries; the leaver is fenced only after the marker gate, so no acked write's only durable copy is ever on a fenced/closed node. Open implementation-must-verify (UNCHANGED): a pending owner's open must replay the full durable WAL tail AND the manifest-epoch fence must be effective no later than the WAL-recovery cutoff (pin with a write-just-before-fence test AND a write-dual-written-concurrent-with-mount test; slatedb-go is an opaque FFI binding). REMOVED (superseded): per-position forwarding (`acquiringForwardTarget` / `forwardPutToPredecessor` / `forwardGetToPredecessor`), the position-addressed forwarded-op proto field, `PredecessorAddr` / `Predecessor` on `HandoffState`, the `priorDesiredReplicas` / `priorAddrs` snapshot, the single-hop-scope fallback, and the draining-exclusion in `reconcileRingFromMembership`. REUSED: the serving marker (`WriteServingMarker` / `ReadServingMarker`), the gossiped `Draining` Meta bit, `mountMap` re-keyed by `ReplicaUnit`, the durable epoch fence + apply-if-newer, `DrainForLeave`, Option A's retry as the residual-case belt. Acceptance: an in-process loss-oracle gate with an ARBITRARILY SLOW `OpenReplicaUnit` asserts zero acked loss AND acked/attempted ~100% even under a multi-second mount (where Option A alone collapses) AND post-transition readback of every baseline key from the post-leave routed set, demonstrated to FAIL on a forced draining-exclusion (no union) with Option-A retry also off. See "v0.8 Phase 2e" above.
  - [ ] Graceful leave / DRAINING node-state (Phase 2e, scale-down): a leaving node enters a distinct DRAINING state - REACHABLE, a CURRENT OWNER, and the source of the PENDING split - advertised via a gossiped per-member `Draining` bit in the memberlist `Meta` (alongside the gRPC address), set by `membership.SetDraining(true)` which calls `memberlist.UpdateNode` (the node STAYS alive, a current owner, does NOT `Leave()`). Every node decodes the bit; `reconcileRingFromMembership` does NOT exclude draining members from the ring (the include/exclude split is computed per-op in `replicasForKey`, current=ring-incl-draining, pending=ring-excl-draining), so the leaver keeps its positions and is DUAL-WRITTEN via the union while it serves. `DrainForLeave(ctx)` blocks until `ownedPositionCount() == 0` (every owned position has a serving successor, released by `drainCheck` on its marker) or the timeout fires, THEN the real `membership.Leave()` + `Shutdown()` teardown runs. Order: SET DRAINING (form the union) -> SERVE + WAIT (successors mount + write markers, the union collapses onto the pending set) -> REAL LEAVE + SHUTDOWN. FIXES the root-cause design flaw where `memberlist.Leave()`-while-staying-alive was self-contradictory (leave-intent + still-gossiping-alive => survivors RE-ADD the leaver and never start the handoff, ~58% in-process / ~70% staging). SUPERSEDES BOTH the ring-freeze stopgap (`leaving atomic.Bool` + `reconcileRingFromMembership` early-return + `ring.Remove(self)`) AND the draining-exclusion + back-forward draft: the draining node stays alive AND a current owner, so its snapshot never collapses and the union routes to it directly. Gated by `GracefulLeaveDrainTimeout` (`0` = disabled = the break-demo state); behind `multiReplicated()`. Acceptance: a continuous writer through a graceful one-node leave asserts ~100% ack + zero unserved window + post-leave readback (vs the gap with timeout 0), reusing the slow-`OpenReplicaUnit` injection. See "v0.8 Phase 2e: Graceful leave (scale-down)" above.
  - [ ] Migration tool: per-node LSM -> per-unit LSMs (standalone, additive, dry-run on a copy).
  - [ ] Memtable / cache tuning surface so operators size N against RAM.

Each version ships independently; users can adopt v0.1 today (functionally equivalent to using their Backend directly, plus the CLI for daily ergonomics) and grow into v0.2+ when their workload demands it.

---

## Inspirations

- **Olric** - same memberlist + consistent-hash pattern. In-memory only; we generalize to durable backends.
- **Vasto** - sharded RocksDB with jump consistent hash + single-master topology. Dormant; architecture is sound and we borrow heavily. We diverge by using ring-based hashing + gossip membership instead.
- **Cassandra / ScyllaDB** - shard-per-core + gossip + ring topology. Heavyweight services; we want library shape with the same model.
- **DynamoDB** - the access-pattern discipline. DDB forces you to commit to a partition key at table-creation time, which drives the rest of your data model. shale relaxes this to per-key hash tags, but the underlying principle (think about access patterns before key design) is the same.
- **Redis Cluster** - the hash-tag convention (`{tag}`-based key co-location) is borrowed directly.
- **SlateDB** - the default backend. Provides KV semantics on cheap object storage; we provide horizontal scale-out around it.
