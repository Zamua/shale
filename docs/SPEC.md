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

// Mount readiness (multi-backend): per-node mount-state counts + the
// predicate an embedding app wires into its readiness probe. See
// "Mount readiness" under the v0.8 section.
func (c *Cluster) MountReadiness() MountReadiness
func (c *Cluster) Ready(minMountedFraction float64) bool

// Refusal reasons: why an op was refused, matchable UNIFORMLY whether the
// refusal came from a locally-mounted position or was forwarded from a peer,
// and at R=1 or R>1 alike (an R>1 write's fan-out collapse preserves it).
// See "Refusal reasons" under the v0.8 section for the full contract.
//
//	if errors.Is(err, cluster.ErrAcquiring) { /* bounded retry */ }
//
// ErrAcquiring is TRANSIENT and SAFE TO RETRY (the op was not applied; the
// window is bounded by a mount). It is NOT a peer-down signal, though it
// shares codes.Unavailable with one - which is why the code cannot be
// branched on. Because the bound is a MOUNT and not an RPC, size the retry
// budget in SECONDS, not milliseconds; see "Sizing the retry" in the
// "Refusal reasons" section. First slice of the taxonomy; siblings join by
// adding a row.
var ErrAcquiring error

type RefusalReason string

const ReasonAcquiring RefusalReason = "ACQUIRING"

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

**Node metadata: address + a `Draining` bit.** Each node publishes a small `Meta` payload over gossip. It carries the node's gRPC dial address (so peers can route writes/reads to it) AND a boolean `Draining` flag used by the graceful-leave (scale-down) path (see "v0.8 Phase 2e: Graceful leave"). The `Member` value object the membership layer returns from `Members()` / `Snapshot()` exposes both fields (`Addr` and `Draining`), decoded from the peer's `Meta` in the `NotifyJoin` / `NotifyUpdate` callbacks. A `Membership.SetDraining(bool)` method updates the LOCAL node's `Meta` flag and calls `memberlist.UpdateNode` so the change gossips out; the node STAYS a full, alive member - `SetDraining(true)` is NOT `Leave()`. A `Draining` member is REACHABLE (still in the snapshot, address known, serving gRPC) AND, under the pending-ranges model (Phase 2e), it STAYS A CURRENT OWNER in the consistent-hash ring: it keeps the positions it already serves and is NOT dropped from ownership when it sets `Draining`. The `Draining` bit is consumed by the ROUTING layer (`routedReplicasForKey`), which computes a position's CURRENT replica set over the ring INCLUDING draining members and its PENDING replica set over the ring EXCLUDING draining members, and during a transition ROUTES to the UNION (so the draining node keeps receiving writes + serving reads until its successor is provably serving, at which point ordered removal collapses the union onto the pending set). The ring-from-membership reconcile (`reconcileRingFromMembership`) therefore does NOT exclude a draining member - the exclusion is computed per-op inside `routedReplicasForKey`, not baked into the ring. This is the foundation of the draining node-state described in Phase 2e. (NB this REVERSES the earlier draining-exclusion design, where `reconcileRingFromMembership` dropped a draining member from the ring and survivors forwarded back to it; that per-position forwarding model is SUPERSEDED - see Phase 2e.)

The membership layer's event delegate uses non-blocking sends to its event channel so a slow subscriber can't deadlock memberlist's gossip goroutines. When the channel is full, the event is dropped + `Membership.DropCount()` is incremented. To keep the ring consistent with reality despite drops, the cluster runs a periodic reconciler (~5s) that calls `Membership.Snapshot()` and applies any missed adds / removes.

**Periodic re-join (seed anti-entropy) heals post-startup gossip splits.** `Open` contacts the configured seeds exactly once at startup (`memberlist.Join`). memberlist's own PushPull anti-entropy only syncs with members already in the local member list: it never re-dials an address that has been pruned out. A mass rolling restart (every pod gets a NEW address; old addresses go suspect/dead and are reaped while new ones join in disjoint waves) can fragment the gossip ring into two groups that have pruned each other out of their member lists. Once that happens neither group's PushPull can pick a node in the other group, the seed (the only stable bridge) is never re-dialed, and the transient startup split becomes PERMANENT (cross-node gRPC keeps working - this is a gossip split, not a network partition, so the ring never heals on its own). To close this, a Membership configured with seeds runs a background goroutine that periodically re-calls `memberlist.Join(seeds)` (interval = `Config.RejoinInterval`; `0` disables it, used by in-process tests that don't churn addresses). `Join` is idempotent: a no-op when the seed is already a known live member (healthy cluster), a MERGE when split (it re-contacts the seed and PushPull reconciles the two member lists across the bridge). The loop stops cleanly on `Close` and, crucially, does NOT re-join once the node has called `Leave` (a draining / departing node must not re-advertise itself back into the cluster it is leaving). Production wires a non-zero default so a real cluster recovers from a mass restart automatically; the manual workaround (scale the StatefulSet to 0 and back up so pods rejoin a stable seed-anchored cluster one at a time) is no longer required.

**Stale-NodeMeta convergence after a rolling restart (gossiped `Meta` self-heals, address reclaim).** A node's gossiped `Meta` (its gRPC address + `Draining` bit + declared unit count) can go STALE on a peer after a StatefulSet ROLLING restart, and a peer with stale `Meta` for even one member breaks any cluster-wide agreement computed over the membership snapshot (the declarative-reshard unanimity gate is the consumer - see "Declarative reshard"). The root cause is two memberlist mechanics, both confirmed in the v0.5.4 source:

  - **Reset incarnation.** A node's memberlist incarnation is a process-local counter that RESETS to 0 on restart. A restarted pod keeps its stable memberlist node NAME (the `NodeID`) but starts incarnation low. memberlist's `aliveNode` REJECTS an incoming alive message whose incarnation is `<= ` the value a peer already remembers for that name (the staleness guard). So the restarted pod's fresh `Meta` (carrying the NEW declared count after a re-declare) is dropped as "stale" by every peer that still remembers the OLD process's higher incarnation, until that peer independently re-learns the node.
  - **Address-reclaim gate.** A restarted pod also has a NEW pod IP. When `aliveNode` sees an alive message for a known name carrying a DIFFERENT address, it only accepts the address change if the peer's recorded state for that name is `StateLeft` (a graceful `Leave` was observed) OR `StateDead` AND `DeadNodeReclaimTime` has elapsed. With `DeadNodeReclaimTime` unset (memberlist's default 0), a `StateDead` old entry can NEVER be reclaimed by a new IP - the conflicting-address branch `return`s before the incarnation is even considered. On a hard pod kill (no graceful `Leave` reaching that peer in time), the peer marks the old entry `StateDead` via its own failure detector, which does NOT fire on every peer at the same moment - so the new pod's `Meta` reaches SOME peers and is permanently rejected by others. The result is an inconsistent, per-peer-divergent view of one member's `Meta` that does not self-heal, turning any unanimity-style gate into a RACE (it converges only if a peer happens to heal; if none do it can hang for minutes).

  Two coordinated fixes make the `Meta` view self-heal on a bounded interval:

  1. **Periodic local-`Meta` re-broadcast (`MetaRefreshInterval`).** Each node periodically calls `memberlist.UpdateNode`, which re-reads the local `NodeMeta` and broadcasts a fresh alive message with a BUMPED incarnation (`UpdateNode` increments the process-local incarnation each call - the same mechanism `SetDraining` already uses to gossip the draining bit). Repeated bumps monotonically climb the local incarnation, so within a bounded number of ticks the broadcast incarnation overtakes whatever a stale peer remembered for this name, clearing the incarnation-staleness rejection. This is a SEPARATE concern from `RejoinInterval` (which re-bridges a gossip SPLIT by re-`Join`ing seeds); meta-refresh re-publishes THIS node's own metadata so a non-split peer with a stale-but-connected view updates it. It reuses the rejoin loop's lifecycle: it runs only on a non-zero interval, stops on `Close`, and SKIPS once the node is `leaving` (a departing node must not re-advertise itself - identical guard to the rejoin loop). The local node's own cache is already authoritative for its `Meta` (set at `Open` / `SetDraining`), so meta-refresh changes nothing locally; it only re-broadcasts. Default `MetaRefreshInterval` is a small bounded value (~10s) so a stale peer heals within a couple of refresh rounds without meaningfully adding gossip load (UpdateNode on an unchanged `Meta` is a single small broadcast). The `UpdateNode` call passes a BOUNDED (non-zero) broadcast timeout: with a live peer, `UpdateNode` blocks until the queued alive broadcast's `notify` channel fires, and that channel does NOT fire if the broadcast is superseded by the next tick's re-broadcast before it transmits (memberlist's `TransmitLimitedQueue` invalidates the older same-node broadcast) - so a `0` timeout (memberlist's "wait forever", NOT "do not wait") can WEDGE the loop on a tick whose broadcast is superseded, hanging the goroutine permanently. The bounded timeout makes a tick that cannot flush promptly return; the next tick (with a still-higher incarnation) retries. A timed-out tick is best-effort, not an error - the re-broadcast still propagates via normal gossip - so it counts as an attempt.

  2. **Bounded `DeadNodeReclaimTime`.** The incarnation bump alone is INSUFFICIENT when a peer holds the old entry as `StateDead` with a different (old) IP: the conflicting-address branch rejects the new IP before the incarnation matters, and with `DeadNodeReclaimTime=0` that rejection is permanent. Setting a bounded `DeadNodeReclaimTime` (a small value, e.g. ~30s, set on the gossip config in `Open`) lets a peer reclaim a dead node's NAME for the new IP once that much time has elapsed since the old entry went dead - exactly the rolling-restart case (the old pod is gone for good, the new pod legitimately owns the name). It is safe because the name is the stable `NodeID` and a genuine same-name address conflict between two LIVE pods cannot occur under the StatefulSet identity model (one pod per ordinal). Without this, fix 1's re-broadcast would keep being dropped at the address gate for any peer that reaped the old pod as dead rather than observing its graceful leave.

  Together: the address-reclaim window opens after `DeadNodeReclaimTime`, and the periodic re-broadcast (with its climbing incarnation) then lands the new `Meta` on every peer, so every node's snapshot converges to the same per-member `Meta` within a bounded time after the roll - which is what the declarative-reshard unanimity gate needs to fire deterministically rather than racing. A clean SIMULTANEOUS restart already converges (every node re-`Join`s and re-learns peers fresh); these fixes specifically harden the STAGGERED rolling-restart path. References the rolling-restart gossip-convergence issue documented for the declarative-reshard activation.

#### Stable node identity vs memberlist node name

The two convergence fixes above (`MetaRefreshInterval`, `DeadNodeReclaimTime`) treat a restart's symptoms: they widen memberlist's address-reclaim gate and climb the incarnation until a peer eventually accepts the restarted pod's new `Meta`. They do NOT remove the underlying COLLISION, and on a HARD-KILL roll the collision can persist long enough to wedge the declarative-reshard gate. The root cause is that shale used the stable node identity AS the memberlist node name, so a restart looks to memberlist like the SAME node changing its address rather than a NEW node joining. The decouple below makes a restart a clean new-node join and removes the collision entirely; with it in place the two convergence fixes are no longer load-bearing for restarts (they stay, harmless, covering the residual stale-`Meta`-on-a-non-restart-peer case).

**The collision (proven).** A homogeneous StatefulSet pod keeps its STABLE node id across restarts (the pod name, passed as `SHALE_NODE_ID` -> `Config.NodeID`) but gets a NEW pod IP, so a NEW memberlist bind address AND a new gRPC address. shale set the memberlist node `Name` to that stable id. When the old instance does NOT cleanly `Leave` (a rolling restart's graceful leave is frequently MISSED by peers that are themselves cycling - staging logs show peers "Refuting a dead message", i.e. the old entry reached `StateDead`, not `StateLeft`), the old same-name entry never settles to `Dead`/`Left`: it oscillates `Alive`<->`Suspect`. memberlist's `aliveNode` then logs "Conflicting address for `<name>`. Mine:`<oldIP>` Theirs:`<newIP>` Old state:0" and REJECTS the new address BEFORE the incarnation is even compared. The restarted pod's NEW `Meta` (carrying its newly-declared unit count) rides in that rejected alive message, so peers keep the STALE declared count indefinitely, the unanimity gate never holds, and the reshard never fires. Measured in-process: a GRACEFUL restart (old `Leave` -> `StateLeft`, reclaimable) converges in ~3s; a HARD-KILL restart (no `Leave`) stays stuck >50s.

**The fix: the memberlist node name is per-PROCESS, the stable id rides in `Meta`.** Each process gets a UNIQUE memberlist node name, so a restart is a brand-new memberlist node: the old process dies a NORMAL death (a different name, so NO same-name/new-address conflict to reject), and the new one joins cleanly. The name is `Config.NodeID + "#" + <bootEpoch>`, where `bootEpoch` is a per-process monotonic token captured at `Open` (`time.Now().UnixNano()`). The stable node id and the boot epoch travel in the `Meta` payload alongside the existing address, draining bit, and declared count, so peers recover the stable identity from `Meta` rather than from the name.

  - **Wire format (two new optional, order-independent segments).** `encodeMeta` / `decodeMeta` gain two trailing NUL-delimited segments in the SAME forward-compatible style as the existing `U<count>` draining/unit-count segments: a stable-id segment `I<nodeID>` and an epoch segment `E<bootEpoch>` (decimal). They compose with `D` and `U<count>` in any order; the decoder still takes the head (up to the first NUL) as the address and ignores any segment prefix it does not recognize. A node id can contain arbitrary bytes EXCEPT NUL (NUL is the segment separator and never appears in a pod name), so `I<nodeID>` is unambiguous; the epoch is a decimal `uint64`. The address head stays byte-identical to the legacy bare-address form, so an OLD peer (pre-decouple image) still parses addr/draining/count correctly and simply does not see the stable-id/epoch segments. A node that publishes the new segments still publishes the SAME `U<count>` it always did, so an old peer's declared-count view is unchanged.

  - **`nodeToMember`: stable id from `Meta`, fall back to the name.** `Member.ID` is now the stable id decoded from `Meta`, FALLING BACK to the memberlist node `Name` when `Meta` carries no stable-id segment. The fallback is the BACKWARD-COMPAT path for the prod rolling upgrade: a LEGACY peer (old image) still has its memberlist name EQUAL to its stable id, so taking the name yields the correct id for it. `Member` also gains an UNEXPORTED `epoch uint64` field (decoded from the `E` segment, 0 when absent) used ONLY for the dedup tie-break below; it is NOT added to any public field the ring or cluster consumes (`Member.ID`, `Addr`, `Draining`, `DeclaredUnitCount` are unchanged on the public surface).

  - **The cache is keyed by the UNIQUE memberlist name, not the stable id.** The internal membership cache (`map[string]Member`, fed by `NotifyJoin`/`NotifyLeave`/`NotifyUpdate`, see the next paragraph) keys by the memberlist node `Name`, NOT by `Member.ID`. This is load-bearing for the restart LEAVE-HAZARD: during a restart BOTH the old process (name `A`, stable id `S`) and the new process (name `B`, stable id `S`) are briefly present, and the old process's `NotifyLeave(A)` must remove ONLY entry `A` and must NOT drop stable id `S` while `B` is alive. Keying by name makes this correct mechanically: `removeCache` deletes by name `A` and leaves `B` (id `S`) intact. `Members()` / `Snapshot()` then PROJECT the per-name cache down to ONE `Member` per stable id (a dedup keyed on `Member.ID`), and when two names share an id the winner is the HIGHEST `epoch` (the newest process). The local node is seeded into the cache under its own unique name at `Open` (the belt-and-suspenders self seed and the `SetDraining` self-update both key by the local memberlist name, not by `Config.NodeID`), and is included in the projection with its stable id like any other member.

  - **The ring (`pkg/ring`) and `pkg/cluster` are UNCHANGED.** The ring keys on `Member.ID` and must continue to see exactly ONE stable id per logical node - which is precisely what the `Members()` per-id dedup guarantees. `pkg/cluster` keeps comparing `Member.ID` against `Config.NodeID` (still the stable id) everywhere. The only membership-layer behavior change a consumer can observe is that a restart now surfaces as an `EventLeave` for the old name's stable id followed (or preceded) by an `EventJoin` for the same stable id at the new address; the cluster's event loop already handles a same-id address change (it Adds the id with the new addr + evicts the stale client) and its periodic reconcile re-derives the ring from the DEDUPED `Snapshot()`, so a transient per-event ring remove/add for a restarting id self-heals within one reconcile tick exactly as a same-id `NotifyUpdate` does today.

  This is the real fix for the rolling-restart reshard wedge. A graceful restart was already fast; the decouple makes a HARD-KILL restart equally fast (no conflicting-address gate to wait out, so it no longer depends on `DeadNodeReclaimTime` elapsing), which is the faithful model of a SIGKILLed pod whose `Leave` never reached its peers.

`Members()` and `Snapshot()` return from an internal cache that the event delegate maintains: every `NotifyJoin` / `NotifyLeave` / `NotifyUpdate` callback updates the cache (keyed by the unique memberlist node name, see "Stable node identity vs memberlist node name"), and reads consult the cache under a `sync.RWMutex` and PROJECT it to one `Member` per stable id (highest-`epoch` wins) before returning. Reading directly from `memberlist.Members()` instead would race with memberlist's internal `aliveNode` goroutine, which mutates the `*Node` fields exposed by that call without exposing a per-node lock. The event callbacks are serialized against those internal transitions, so cache writes from inside the callbacks are race-free, and the cache stays consistent with the authoritative state memberlist itself publishes via those same events. Even when the channel drops, the cache update happens before the send attempt, so a dropped notification still leaves the cache (and therefore `Snapshot`) authoritative for the reconciler.

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

**Peer-connection resilience at cold-start.** The cluster-internal peer gRPC client is built lazy (it connects on the first RPC, not at construction) and the ClientConn is CACHED for the cluster's lifetime. Single-key Get/Put stay FAIL-FAST so a momentarily-unreachable replica fails over to another replica immediately instead of blocking the hot path. The cross-shard `Aggregate` fan-out, however, has no second replica to fall back to - each peer is scanned exactly once - so it must tolerate a peer whose gRPC server is briefly not-serving at the instant the scan first connects (a heavy cold-start where the process is busy mounting units). Two things provide that tolerance: (1) the peer dial uses a FAST reconnect backoff (≈200ms base, ≤3s ceiling) plus a client keepalive (ping every 30s, `PermitWithoutStream` so an idle cached connection is kept warm), so a cached connection that misses its first connect recovers in seconds rather than the gRPC default's 120s backoff ceiling that would otherwise wedge every later fan-out RPC on that cached client. The gRPC SERVER must permit that keepalive: its `KeepaliveEnforcementPolicy` is set to `MinTime` 10s (below the client's 30s) + `PermitWithoutStream`, because gRPC's DEFAULT server enforcement (`MinTime` 5m, no pings without an active stream) answers the client's 30s idle-connection pings with `GoAway "too_many_pings"` and tears the cross-node connection down - which silently churns the cross-node gen-learning + cross-shard scan during a slow cold-start (a peer whose mount takes minutes keeps the connection idle long enough for the keepalive to fire repeatedly); and (2) before opening a peer's `LocalScan` stream, `Aggregate` explicitly WAITS (bounded by `Config.PeerConnectTimeout`, default 30s) for that peer's connection to reach READY, then opens the stream over the already-ready connection. Without this a cold-starting peer would surface as that peer's `AggregateResult.Err` (a transport "error reading server preface" / connection-refused), even though the peer comes up a moment later. The wait returns AS SOON AS the peer is ready, so a healthy fan-out pays ~nothing; a peer that never comes up inside `PeerConnectTimeout` records its `Err` and the rest of the fan-out is unaffected.

**Fenced-mount tolerance during the scan.** Symmetric to the write path's fence self-heal (a fenced owner-local Put / Commit recodes to the transient acquiring-window error and evicts the stale mount; see "A fenced CAS owner-local commit is a TRANSIENT retry"), the `Aggregate` / `LocalScan` scan path tolerates a MOUNTED unit whose backing handle has been FENCED. The scan reads through this node's (and each peer's) currently-mounted unit handles directly - no fresh open - so it must contend with the same writer-epoch fence the write path does. During a membership change a mounted position can be SUPERSEDED: a higher-epoch owner opens the same per-(unit, replica) database, which fences - and in production slatedb CLOSES - the prior handle, so a read/scan through that now-stale handle fails (the `detected newer DB client` fence, surfacing on `ScanPrefix` or the iterator's `Next`). Two pieces give the scan the same tolerance the write path has: (1) the backend TAGS the fence as `backend.ErrFenced` on EVERY op - the non-transactional `Get` / `ScanPrefix` / iterator `Next` included, not only the transactional ops - so the cluster recognizes it backend-agnostically (`errors.Is(err, backend.ErrFenced)`) rather than only matching the rendered string; and (2) on a fenced scan of a mounted unit the cluster EVICTS the stale mount (the same `evictStaleMount` the write path uses - a no-op on a mid-drain position, whose fence is the EXPECTED successor signal, not a stale-handle desync) and surfaces a TRANSIENT error for that scan rather than failing the whole aggregate with the raw fence. The caller re-runs the scan; the reconcile re-acquires the evicted position (or, if the lease genuinely moved, the superseding owner now covers that unit via its OWN `LocalScan`), so a later pass is fence-free and complete. This is load-bearing for a SCAN-ONLY workload (a cross-shard inventory sweep, the blob-GC sweeper): it never issues the write that would otherwise self-heal the stale mount via the write path, so WITHOUT the scan-path eviction the fence re-surfaces on every retry indefinitely - the eviction is what lets the retry make progress. NOTE on where the transient surfaces: the eviction always happens on the node that physically holds the stale mount (the local node for `localMountedSnapshot`, the PEER for its own `LocalScan`), and a PEER's recoded transient reaches the coordinator THROUGH the scan `fn` (the peer's `LocalScan` stream yields the `codes.Unavailable` mid-iteration, so it lands in that peer's `fn` result), NOT in `AggregateResult.Err` (which stays reserved for a dial / snapshot-open failure). A cross-shard inventory caller (the migrate / sweeper) therefore must treat a transient returned from its scan `fn` as retryable - which it already does. The in-process test factory DEFAULTS to production-shaped close-on-fence semantics (strict read fencing: a fenced handle fails reads too; eager fence: the writer-epoch bumps at open START), matching production slatedb's closed-handle-on-read, with per-test opt-outs (`SetStrictReadFencing(false)` / `SetEagerFence(false)`, justified inline) for tests that specifically need the permissive WRITES-fail / reads-PASS timing; this path is pinned by a real-slatedb integration test (a scan through a fenced handle returns `backend.ErrFenced`) plus an in-process cluster test that mounts a backend whose `ScanPrefix` fences (asserting BOTH scan paths - the local snapshot and the peer `LocalScan` iterator - evict the stale mount and recode the result to the transient acquiring-window error, not the raw fence).

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

`TimestampNanos` is drawn by the **originating node** at the moment `Put` is called, before fan-out, from a **per-node monotone stamp source**: `Next() = max(wall clock, last+1)`, where `last` is the highest nanosecond value the node has ever issued OR observed. The source is ratcheted (`Observe`, an atomic max) by every stamp the node sees: the LWW winner on a replicated read, each stored envelope stamp a CAS validate decodes, and the incoming + stored stamps on a replica-receiving apply. So a node's stamps are strictly increasing and unique per call even across a wall-clock regression (NTP step, VM migration), and a node that has SEEN a stamp never issues a new one at or below it. At the `2^64-1` ceiling the source SATURATES rather than wrapping to zero: observing a maximal stamp (only reachable from a corrupt or hostile envelope; real wall clocks sit ~10x below the ceiling) pins the source at the maximum, forfeiting per-call uniqueness but never monotonicity (a wrap would stamp the node's next write 0, which every replica's apply-if-newer silently rejects). Every replica stores the same Stamp for a given write; replicas do NOT re-stamp on receipt. This keeps the clock single-sourced per write and avoids skew between the primary's stamp and a successor's stamp for the same `Put`. The NodeID tiebreak resolves the rare case where two originators race with identical nanosecond readings. The high-water mark is in-memory only (not persisted).

Clock skew between nodes remains the standard LWW caveat, narrowed by the ratchet: a node with a forward-skewed clock can still shadow honestly-timestamped writes from healthier peers, but once a node has observed a stamp (via a read, a validate, or a replica-receiving apply) it can no longer issue below it. The honest residual: the ratchet is a per-NODE high-water mark over stamps the node has actually SEEN, not a per-key floor, so ANY originator whose high-water mark is below a stored stamp can still stamp a plain `Put` below it (apply-if-newer then no-ops that Put on the replicas, so it is shadowed by the stored value). A fresh process that has observed nothing yet is the common case, but a long-running node under-stamps the same way for a key it never happened to read, validate, or receive an apply for (e.g. an originator that is not one of that key's replicas). A CAS commit carries no such residual, even on a fresh process and even for write-set keys the fn never reads: `CommitCASApply` Observes the stored stamp of every read-set key (during validate) and every blind write-set key (a `tx.Get` + header-only decode inside the commit tx) before drawing the commit stamp. Operators run NTP. A durable per-unit high-water mark (persisting the ratchet across restarts) is possible future work; shale does not synthesize a full hybrid logical clock.

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

### Rebalancing

shale has ONE distributed coordination model, and rebalancing is part of it.

A multi-node cluster is multi-backend: every key belongs to a storage UNIT, and
a unit is a self-contained database in shared durable storage. Ownership of a
unit is a LEASE. When membership changes, a unit whose owner moved is handed
off COPY-FREE: the old owner flushes and releases it, the new owner opens it at
a higher epoch (fencing the old writer), and the bytes never travel through
shale at all. They were already in the shared store; only the lease moved.

That is the whole mechanism. There is no second engine that plans key ranges,
streams them between peers, verifies a checksum and sweeps the source. Such an
engine existed through v0.12 as the fallback for backends that cannot open a
unit and fence a prior writer (`memory`, `pebble`), and carrying it meant shale
selected a coordination model based on which backend it had been handed. That
is the leak this design removed: shale should not know or care whether an
adapter has that capability. An adapter either satisfies
`storageunit.BackendFactory` or it does not; if it does not, it is a
single-node backend.

The full mechanism is specified under "v0.8 Phase 3: lease-handoff rebalance"
(R=1), "v0.8 Phase 2e: pending ranges" (R>1 overlap handoff) and "v0.9
Decentralized reshard" below. What follows here is only the part shared with
the membership layer.

#### Trigger

Rebalancing is driven by membership change, with a settling delay.

When `Membership` reports a join or leave, the node bumps a monotonic ring
generation and schedules a reconcile pass for `T_settle` in the future (default
5s, configurable via `Config.RebalanceSettleDelay`). Any further membership
event inside the window resets the timer. When the timer fires, the node
diffs the units it owns on the current ring against the units it has mounted,
and acquires or releases the difference.

The settling delay collapses bursts (a rolling restart, several nodes joining
within a second, a flapping peer) into one reconcile pass instead of thrashing
the mount map through intermediate ring shapes. It also gives the membership
reconciler time to absorb missed events, so by the time the pass runs, nodes
tend to agree on who is in the cluster.

A node is *rebalance-idle* when no settle-timer reconcile is pending: nothing
is armed-but-unrun and no reconcile callback is mid-flight.
`WaitForRebalanceIdle` blocks until that holds, so a caller that observes idle
immediately after a membership change is guaranteed the debounced pass has
already run and applied its unit moves, not merely that nothing has been
scheduled yet. Single-node mode never schedules a pass, so it is trivially
idle.

#### Rejecting a write to a unit mid-handoff

A Put or Delete that lands on a unit currently between owners is refused with
`codes.ResourceExhausted` plus a retry-after hint (`Config.RebalanceRetryAfterMs`,
default 50ms). That is a TRANSIENT refusal with defined retry semantics, and it
is deliberately distinct from the two other reserved codes:

  - `ResourceExhausted`: in-flight handoff. Retry after the hinted backoff; the
    next attempt may land on a different owner.
  - `FailedPrecondition`: forwarding loop-guard. The receiving node disagrees
    with the originator about ownership; the client must refresh its ring.
  - `Unavailable`: a peer's gRPC channel is gone. This one counts against the
    fan-out's failure budget so a genuinely-down node short-circuits the call.

Keeping handoff refusals out of the failure budget is what lets a fan-out wait
for the other replicas instead of failing the whole call because one replica is
mid-handoff. The same retry-after value is the base of the Layer-2 handoff
retry backoff (see "v0.8 Phase 2d").

#### `Config.Backend`: single-node only

`Config.Backend` is the local-only embedding of one backend: no membership, no
ring, every operation local. ANY backend satisfies it, because the mode imposes
no requirement on the adapter beyond `backend.Backend`. This is the shape a
toolkit should have: the simple case stays simple.

`Config.Backend` together with a `BindAddr` is REJECTED at `Open`. It used to
select the retired second engine. Accepting it now would be worse than an
error and worse than a panic: the cluster would come up, gossip, build a ring
and serve reads and writes, but nothing would move data on a topology change,
so keys would silently become unreachable the moment the ring reassigned them
to a node that had never held their bytes. A three-node deployment would look
healthy while losing reads. `Open` therefore fails with a message naming the
actual requirement: multi-node needs `BackendFactory` + `UnitCount`.

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
  5. **Commit.** `tx.Commit()`. On a commit error, the handler does NOT set `committed` (the deferred `Rollback` runs) and returns `{error}`: same leak-guard discipline a failed commit demands, because a backend's failed `Commit` is not guaranteed to have finalized the transaction. On success, it sets `committed` (suppressing the deferred rollback). At R=1 that is the whole outcome: `{committed:true}`. At R>1 the owner THEN fans the write-set out to the replicas (see "Write-set replication"); `committed` is already `true` before the fan-out runs, so a fan-out that misses W does NOT un-commit the durable owner-local write. A W-shortfall there returns `{committed:true, under_replicated:true}` (the durable write, flagged as replicated to fewer than W acks), NEVER a bare `{error}`: the write is on the owner and MUST NOT be retried away. Because the pods run relaxed (`AwaitDurable=false`, ack from the memtable), the owner's lone copy on the under-W path is made bucket-durable by a SYNCHRONOUS flush of the owner backend before this success returns (see outcome (c) for the full argument); the ONE exception is a flush that itself fails, which is a genuine not-committed durability failure surfaced terminally as `{error}`, not a success. This is the discriminator the retry closure relies on - after the owner-local `Commit` succeeds, the ONLY thing that can fail is the replica fan-out (and, on the under-W branch, the durability flush), and a fan-out shortfall is definitionally "committed but under-replicated", never "not committed". See "The four commit outcomes" under the retry closure.

A `backend.ErrFenced` at ANY of those owner-local op sites (the read-set `Get`, a write-set `Put`/`Delete`, or `Commit`) is NOT returned as a raw `{error}`: it is recoded to the TRANSIENT acquiring-window error and the stale mount evicted, so a CAS commit through a graceful leave is retried rather than hard-failed (see "A fenced CAS owner-local commit is a TRANSIENT retry, not a hard failure" under the retry closure).

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

**Transparent retry through a transient commit failure (cut-over + reshard re-pin).** This is outcome (d) above: two distinct transient signals on the COMMIT can fire while the cluster is mid-reshard, BEFORE any owner-local `Commit` applies, and `Transact` rides BOTH out transparently rather than surfacing them as hard failures. A W-shortfall on the POST-commit replica fan-out (outcome (c)) is NOT one of these - it committed durably and is surfaced as success-under-replication, never re-run. (1) `codes.Unavailable` - a reshard cut-over / acquiring window, or the pin unit's lease mid-handoff: the owner refuses the commit with the retryable code, and `Transact` re-runs `fn` after a backoff (exponential with full jitter, see below) WITHOUT consuming a conflict attempt, bounded by `transactUnavailableTimeout` (default 30s, generous enough to ride out a whole reshard window) AND by a maximum number of retryable re-runs (`transactRetryableMaxRuns`, default 24, see below). (2) `codes.FailedPrecondition` - the reshard cutover re-pin signal: when a forwarded `CommitCAS` lands on the node that JUST lost ownership of `pinKey` across the FLIP/redistribution, the owner refuses WITHOUT applying ("re-pin against the current ring", the same ring-refresh loop-guard reads already retry across the staggered-generation window). Because the commit re-resolves the owner from the LIVE ring on every attempt, a re-run lands on the NEW owner and commits. `Transact` therefore treats `codes.FailedPrecondition` from a commit identically to `codes.Unavailable`: re-run `fn` after backoff, no conflict attempt spent, bounded by the same `transactUnavailableTimeout` and the same re-run cap. Past the deadline either code surfaces as-is (still a retryable status the caller may handle); past the re-run cap `Transact` returns `ErrTransactRetriesExhausted` wrapping the last commit error (see below). The net effect: a `Transact` spanning a reshard commits exactly once at the new generation, transparently, never returning the bare re-pin error to the caller. (A NON-transient `codes.FailedPrecondition` - a genuine cross-shard guard violation - never reaches this path: the cross-shard guard fires inside `fn` as `backend.ErrCrossShard` and aborts immediately, before any commit.)

**The four commit outcomes (and which ones `Transact` re-runs).** Every `CommitCAS` resolves to exactly one of four outcomes, and the retry decision is a pure function of which one:

  - **(a) Conflict** (PRE-commit): a read-check failed, so nothing was applied. Surfaces as `backend.ErrCASConflict`. `Transact` re-runs `fn` from scratch, spending one CONFLICT-budget attempt (`CASMaxAttempts`), with the conflict backoff. UNCHANGED.
  - **(b) Committed and replicated to W**: the owner-local `Commit` succeeded AND the fan-out collected W acks. Surfaces as success (`{committed:true}`). `Transact` returns nil. UNCHANGED.
  - **(c) Committed but under-replicated** (POST-commit, R>1 only): the owner-local `Commit` durably succeeded but the fan-out could not reach W within the write timeout, so the write-set landed on FEWER than W replicas - worst case, on the owner ALONE. Surfaced as `{committed:true, under_replicated:true}`, mapped to nil by the tx-commit-to-error conversion; `Transact` returns nil and does NOT re-run `fn`. **The relaxed-durability closing addition.** shale's intended balance runs the pods RELAXED (slate `AwaitDurable=false`, ack from the memtable, the hostthis default) and buys durability from REPLICATION: an ack to W replicas is W independent in-RAM copies, which survives a single pod loss with NO flush - so the normal W-reached path (b) deliberately does NOT flush, and MUST NOT (flushing every commit would defeat the whole relaxed-durability design). The gap is ONLY the under-W path: when the fan-out cannot reach W the replication safety net did NOT catch the write, so the lone owner copy sits in the memtable (RAM) with no second copy, and acking THAT as durable would be unsound (an owner-pod crash in the sub-second pre-flush window is silent loss - the review proved this against the relaxed prod config). So on the under-W branch ONLY, before returning success, the owner SYNCHRONOUSLY FLUSHES its local backend to the object store (the OPTIONAL `backend.Flusher` capability, the same one the displacement flush uses), converting the lone copy from RAM-only to bucket-durable. `committed:true`-under-replicated therefore genuinely means DURABLE-IN-THE-BUCKET, not memtable-resident. (Under a SAME-OWNER write burst, where under-W becomes the COMMON case, that flush is GROUP-COMMITTED per storage unit so N concurrent under-W writes collapse to O(flush-windows) flushes rather than O(writes) - the same per-write durability guarantee at a fraction of the flush cost; see "Owner-flush coalescing" below.) This is the graceful degradation of the replicate-for-durability model: replicate when we can reach W, flush the single copy when we cannot. The flush is paid ONLY on this rare branch (the replication net missed) - exactly when durability must be bought - so it costs the normal path nothing. A backend already synchronously durable (the `memory`/test backends, `pebble`, or slate at `AwaitDurable=true`) has NO RAM-loss window and does not implement `backend.Flusher`; the under-W path type-asserts, finds none, and skips the flush. If the flush ITSELF errors, that is a genuine durability failure (committed to the memtable, could not be persisted): the commit does NOT report success - the flush error surfaces TERMINALLY as a not-committed error the caller sees, and because a flush I/O error is not a `commitRetryable` code `Transact` does NOT re-run `fn` (no amplification; the write-set is applied at most once). **Replica-count healing** is then automatic: apply-if-newer read-repair on the next quorum `Get` copies the owner's now-durable envelope to a lagging replica. NB the fan-out's own `WriteTimeout`-bounded retry is NOT a forward heal here - it is ALREADY EXHAUSTED at under-W return (its exhaustion is exactly what produced outcome (c)); the two real forward heals are the owner-flush (which bought durability) and read-repair (which restores the replica count). Nothing for the caller to do, nothing to retry.
  - **(d) Not committed, retryable** (PRE-commit): the owner refused the commit BEFORE any owner-local `Commit` - a cut-over / lease mid-handoff refusal (`codes.Unavailable`), a reshard-cutover re-pin (`codes.FailedPrecondition`), or a graceful-leave fence recoded to the acquiring-window `codes.Unavailable`. Nothing was applied. `Transact` re-runs `fn` after a polite backoff WITHOUT spending a conflict attempt, so a reshard is transparent (see below). UNCHANGED.

The correctness crux: (c) and (d) BOTH used to surface as a bare `codes.Unavailable`, indistinguishable, so `Transact` re-ran (c) as if it were (d) - re-issuing a fresh durable commit for a write that already committed, amplifying write load and letting a retried insert observe its own durable write as a false conflict / `ErrSlugTaken`. The fix carries `committed==true` (the sound discriminator: everything after the owner-local `Commit` is post-commit) out of `CommitCASApply` and across the wire, so (c) is now classified success-under-replication AT THE SOURCE and never enters the retry loop. Only genuine (d) - which applied nothing - is re-run.

**The retryable-status loop (for outcome (d)) is polite: exponential backoff with full jitter, plus a re-run cap.** Each consecutive retryable commit failure DOUBLES the backoff, starting at `casBaseBackoff` and capped at 500ms (`transactRetryableBackoffCap`); the sleep before each re-run is full-jitter, i.e. a uniform random duration in `(0, current-backoff]` (jitter spreads a herd of contending callers riding out the same reshard window; the exponential growth backs the whole herd off). The loop is additionally capped at `transactRetryableMaxRuns` retryable re-runs (default 24): past the cap `Transact` gives up and returns `ErrTransactRetriesExhausted` WRAPPING the last commit error, so a caller can distinguish "gave up retrying transient unavailability" from a hard failure by `errors.Is`, while the wrapped retryable gRPC status (`codes.Unavailable` / `codes.FailedPrecondition`) still resolves through the wrap via `status.FromError` for compatibility. `transactUnavailableTimeout` remains the outer wall-clock bound; whichever bound trips first ends the loop. WHY the loop stays polite even though it now only re-runs (d): a herd of callers all stuck behind one reshard window would, on an aggressive flat few-millisecond backoff, re-run `fn` hundreds of times per second for the whole window; each re-run is a full read-set + validate round-trip to the (briefly refusing, then recovering) owner. The exponential-to-500ms backoff plus the ~24-re-run budget rides out the same window while issuing orders of magnitude fewer commit attempts. Note this backoff is now DEFENSE-IN-DEPTH, not the primary remedy for the amplification the v0.10.2 mitigation targeted: outcome (c), the durable-but-under-replicated commit, was the write-amplifying case, and it no longer reaches this loop at all - it is classified as success at the source. The loop's remaining job is purely to make the legitimate (d) reshard-transparency re-runs polite; every (d) re-run applied nothing, so a re-run is a fresh single-apply attempt, never a second durable commit of the same write-set.

**Owner-flush coalescing: the under-W flush is group-committed per storage unit.** Outcome (c)'s synchronous owner-flush is correct but, taken naively, is a per-write flush - and under a SAME-OWNER WRITE BURST that is a flush storm. When many writers hammer ONE storage unit concurrently, the fan-out routinely cannot reach W under the contention, so under-W stops being the rare case and becomes the COMMON one: every one of those durably-committed writes takes the outcome-(c) branch, and one-flush-per-write forces a full backend flush each. But a slate `Flush()` persists the ENTIRE memtable, so N concurrent under-W writes on one unit each forcing their own flush is massively redundant - the second flush already persisted the first write's data. They need exactly ONE flush BETWEEN them, not N. So the owner-flush is GROUP-COMMITTED (textbook group commit) per storage unit: a per-unit coordinator holds a small state (a pending-waiter list plus an in-flight flag); an under-W writer appends itself to its unit's pending list and, if no flush is in flight, spawns the flush-runner, then blocks; the runner SNAPSHOTS-and-CLEARS the pending list, calls `Flush()` ONCE for the whole snapshot, delivers that flush's result to every waiter in it, and loops if new waiters arrived during the flush (a fresh flush for them). A burst of same-unit under-W writes therefore collapses from O(writes) flushes to O(flush-windows) flushes, WITH THE SAME per-write durability guarantee: each write's success still waits for a flush that includes it. The failure mode without coalescing: a high-concurrency same-owner burst turns nearly every commit into its own full-memtable flush - tens of redundant flushes per logical write, since each flush already persisted its predecessors' data - which coalescing collapses back to roughly one flush per flush window.

**The coalescing correctness invariant: a waiter is satisfied ONLY by a flush that STARTED AFTER the waiter enqueued.** This is what makes a SHARED flush durable for every waiter in its batch. Proof: (1) an under-W writer enqueues on the coordinator only AFTER its owner-local `Commit` returned, so its write is already in the owner's memtable before it enqueues; (2) the runner snapshots-and-clears the pending list UNDER THE STATE LOCK and THEN calls `Flush()`, so every waiter in a snapshot enqueued before that flush began, hence its memtable-resident write is captured by the flush; (3) a waiter that enqueues WHILE a flush is in flight is NOT in that flush's snapshot (the snapshot already cleared pending), so it waits for the NEXT flush, which the runner starts AFTER that waiter enqueued - and which therefore persists its write too. The ordering that makes this sound is snapshot-and-clear STRICTLY BEFORE `Flush()`, never after: a waiter is never satisfied by a flush that began before it enqueued, so a shared flush can only ack writes that were memtable-resident when it started. Coalescing is per UNIT: same-unit concurrent under-W writes share a flush (one memtable, one flush persists all their data), while different units flush independently (each mounted unit is an independent durable store, keyed separately in the coordinator, so a flush of one never satisfies a waiter on another). A `Flush()` error propagates to EVERY waiter in that batch - each returns it terminally, exactly as the un-coalesced flush error returned terminally (the terminal-vs-retryable-fence classification is unchanged, applied at the commit call site; the coordinator is transparent to error identity), so a shared flush failure is a durability failure for all its waiters, never a false success for any - and it does not wedge the coordinator: a subsequent under-W write flushes normally. The capability skip is unchanged: a backend without `backend.Flusher` (memory/pebble, strict-durability slate) engages no coordinator at all.

**A fenced CAS owner-local commit is a TRANSIENT retry, not a hard failure (the graceful-leave seal for CAS writes).** The owner-side validate-and-apply (`CommitCASApply`) opens ONE short owner-local `backend.Transaction` and runs `tx.Get` (read-set re-validation) + `tx.Put` (the write-set) + `tx.Commit`. During a graceful leave the successor opens the leaver's position at a higher epoch and FENCES the leaver's writer, so ANY of those owner-local ops can fail with `backend.ErrFenced` (the same writer-epoch fence the plain Put leg sees, surfacing at whichever op runs first against the now-stale writer). That fenced commit did NOT durably apply, so it MUST be recoded to the TRANSIENT acquiring-window error (`errUnitAcquiring`, carrying `codes.Unavailable`) and the stale mount evicted - EXACTLY the `fenceToTransient` recode the plain Put / ApplyBatch apply sites use, at EVERY CAS owner-local op error site (the `tx.Get` inside read-set validation, each `tx.Put`, and the final `tx.Commit`), NOT just Commit. The recode is mechanically possible because `CommitCASApply` resolves the owner-local backend + its `ReplicaUnit` up front (`localBeginForKey` carries the mounted backend and its `ReplicaUnit` on the returned transaction so the apply sites can evict the right mount). `commitRetryable` ALSO returns true for an error that wraps `backend.ErrFenced` (belt-and-suspenders): a multi-`%w`-wrapped `backend.ErrFenced` is `errors.Is`-true but `status.FromError` reports `ok=false`/`Unknown`, so the classifier must branch on the sentinel directly, not only on the gRPC code, or a fenced commit that reached the classifier unrecoded would be mis-read as a hard failure. With the recode in place a fenced CAS commit surfaces as a retryable `codes.Unavailable` and `Transact` RE-RUNS `fn` from scratch: the re-run re-resolves the owner from the LIVE ring (so it lands on the successor once it serves) AND re-reads the read-set against that owner (OCC), so the retried commit is correct and idempotent - the fenced attempt applied nothing, and apply-if-newer / OCC make the re-applied write a clean single apply with no double-apply and no lost update. The FORWARDED CAS leg carries the fence as a retryable status over the wire the same way the Put forward leg does: the CAS RPC handler emits the recoded `codes.Unavailable` (status-preserving, per the handler's existing "a commit error that carries a gRPC status must reach the client AS a gRPC status" path) so a fenced CAS on a REMOTE owner arrives at the client classified retryable, and `commitRetryable` rides it out identically to a local fence. This closes the during-leave gap for CAS / `If-None-Match` writers, mirroring the plain-Put seal above.

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

**Known gap (and how it closes at R>1).** With a fast-ack Backend (slate `AwaitDurable=false`, the hostthis default) and the R=1 commit path, a committed write-set sits in the owner's loss window with no replica copy: R=1 bypasses the replicated path entirely (no fan-out, no flush), so R=1 + `AwaitDurable=false` has NO durability guarantee - that combination is the operator's explicit fast-but-not-durable choice, and shale notes rather than enforces the R>=2 pairing (see "Fast-ack backends" above). At R>1, v0.6.x closes the gap by REPLICATION when the fan-out reaches W (W independent in-RAM copies survive a single pod loss); the residual case - the fan-out short of W, leaving the owner's lone memtable copy - is closed by the under-W owner-flush (outcome (c)): a committed CAS write at R>1 is durable in EVERY case, either on W replicas or flushed to the bucket. Operators running transactions against a fast-ack backend should still pair it with `ReplicationFactor >= 2`.

#### Write-set replication (v0.6.x)

v0.6.x makes a CAS commit replicate its write-set to the shard's R replicas so transactional writes survive a single-replica loss, the "R for durability" property that pairs with a fast-ack backend. It does NOT change the `CommitCAS` protocol: it changes how `CommitCASApply` runs on the owner, and adds one new RPC (`ApplyBatch`) for the owner-to-replica fan-out.

The crux is the **envelope split** already in place for single-key writes (see "Replication (v0.4+) -> Value envelope"): at `ReplicationFactor > 1` the Backend stores ENCODED `Envelope` bytes (stamp + payload), not raw values; at `R == 1` it stores raw values. The v0.6 CAS path did raw `tx.Get` / `tx.Put`, which is correct only at R=1. At R>1 it was wrong two ways: (a) `tx.Get` returns the stored envelope bytes, but a `ReadCheck`'s `expected_value` is the DECODED payload the client saw (the client read path, `getReplicated`, already returns decoded payloads), so a byte-for-byte compare always mismatches; (b) `tx.Put` of a raw value corrupts reads, since `getReplicated` expects an envelope. v0.6.x makes `CommitCASApply` envelope-aware at R>1 while leaving R=1 byte-for-byte unchanged.

**R == 1 (no ring, or `ReplicationFactor` 1): UNCHANGED.** Raw values, no envelopes, no fan-out, exactly the v0.6 path. This is the contract for every existing single-node deploy: a regression here would corrupt or fail every non-replicated transaction. The decode-on-validate / encode-on-apply / fan-out logic is gated entirely behind R>1.

**R > 1: envelope-aware validate-and-apply, then fan out.** Under the same owner-local `backend.Transaction` as v0.6:

  1. **Validate (decode-on-read).** For each `ReadCheck`, `tx.Get` returns the stored envelope; `Decode` it before comparing. A winning **tombstone** envelope (empty payload) counts as not-found: it satisfies `expect_absent`, and conflicts a value-match check (the key the client saw is gone). Otherwise compare the decoded payload to `expected_value` byte-for-byte. Conflict semantics are identical to R=1; only the decode step is inserted.
  2. **Apply (shared-stamp encode-on-write).** Compute ONE shared `Stamp{Next(), owner NodeID}` for the whole commit, drawn from the owner's monotone stamp source (see "LWW comparator") AFTER the validate step has Observed every stored stamp it decoded AND the owner has Observed the stored stamp of every BLIND write-set key (a `WriteOp` key with no matching read-check: one `tx.Get` + header-only stamp decode per blind key against the already-open owner-local tx). For each `WriteOp`, build an `Envelope` (Put -> payload = value; Delete -> empty payload tombstone) with that shared stamp, `Encode` it, and `tx.Put(key, envBytes)` into the local tx. Delete is written as a tombstone-envelope Put, NOT `tx.Delete`, so `getReplicated`'s LWW comparator sees a stamped tombstone (a bare key-removal would lose to a stale stamped value on another replica). Commit the local tx: atomic on the owner.
  3. **Replicate (fan out the SAME envelopes).** After the local commit returns nil, fan out the identical encoded envelopes to the R-1 OTHER replicas via `ApplyBatch` (one call per replica carrying the whole write-set). Reuse `fanout` + `requiredWriteAcks`; wait for W total acks. **The owner's own local commit counts as 1 ack.** So `ApplyBatch` only needs W-1 more replica acks; under `WriteOne` (W=1) the local commit alone satisfies W and no replica ack is required to return success (the fan-out still runs for durability, best-effort). Migration-guard rejections from a mid-handoff replica are transient (don't count toward acks or the failure budget), same as single-key Put. Each replica applies the batch **apply-if-newer** (see "LWW on write"), so a fan-out that arrives out of order relative to a later commit's fan-out self-resolves: the older shared stamp loses the per-key compare and is a no-op.

The owner's local commit plus the replica fan-out mirror `putReplicated`, except the owner side is a transactional validate-and-apply rather than a single Put, and the owner's commit is pre-counted as one of the W acks.

**Lock scope: `casCommitMu` covers validate + local commit only (v0.7+).** The serialization lock is held across steps 1-2 (read-set validation through the owner's local `tx.Commit`) and is **RELEASED before the step-3 fan-out.** Establishing OCC order is the local commit's job: two commits cannot both pass validation against the same observed value and both apply, because the second to acquire the lock re-validates against the first's already-committed write. Once the local commit fixes the order, the fan-out no longer needs the lock: reordered fan-outs arriving at a replica self-resolve via apply-if-newer (the older shared stamp loses on every key). This restores the property CAS was designed for, **no lock held across a network round-trip**: v0.6.x had to hold `casCommitMu` across the whole fan-out precisely because replicas applied verbatim, so an older commit's fan-out could clobber a newer one on a replica; apply-if-newer removes that hazard and lets the lock release at the local commit boundary. The fan-out remains best-effort-to-W and runs after release; an under-W result is surfaced as a committed-under-replication SUCCESS (durable in the object store - the under-W branch synchronously flushes the owner's lone copy before returning - plus any acked replicas), NOT a retryable error (see "The four commit outcomes": once the owner-local commit returns, the write is durable and cannot be un-committed by a fan-out shortfall).

**The shared commit stamp.** Every write in one CAS commit carries the SAME `Stamp`, so the whole write-set replicates and LWW-resolves as a unit: a later single-key Put or a later CAS commit with a greater stamp wins uniformly across all the keys, and a concurrent write with a lesser stamp loses uniformly. A per-op stamp would let LWW split a single transaction's keys across two winners on a laggy replica, breaking the atomicity the OCC commit just established.

**The commit stamp exceeds every stored stamp the commit replaces (observed-stamp ratchet).** The validate step Observes every stored envelope stamp it decodes into the owner's monotone stamp source, and - still before the shared commit stamp is issued - the owner also Observes the stored stamp of every BLIND write-set key (a `WriteOp` key the read-set never validated), via one `tx.Get` + header-only stamp decode per blind key inside the open owner-local tx. So the commit stamp is guaranteed strictly greater than every stamp in the validated read-set AND under the write-set: a committed write-set can never lose the LWW compare to the values it replaced. Without this floor, a stored stamp written by a faster clock would beat a raw-wall-clock commit stamp: the owner validates, commits locally (authoritative), acks - then every replica's apply-if-newer rejects the batch, the next quorum read picks the OLD value, and read-repair pushes it back over the owner's committed copy. That read-repair un-commit of an acked commit is exactly the scenario the ratchet prevents - and a blind write-set key skipping the floor would additionally let apply-if-newer reject SOME of one commit's entries while others land, splitting a single commit's write-set across two LWW winners on the replicas (the atomicity the shared stamp exists to hold).

**Atomicity boundary (same model as single-key Put).** The CAS commit is atomic ON THE OWNER: the owner-local tx either commits the whole write-set or none of it. Replication is best-effort-to-W AFTER that local commit, exactly like `putReplicated`. If fewer than W acks land, `CommitCASApply` returns a committed-under-replication SUCCESS - `{committed:true, under_replicated:true}` carrying the under-W error only for logging/observability - NOT a bare error: the write is ALREADY durable on the owner and on however many replicas did ack, so a caller must never retry it away (doing so re-issues a fresh durable commit and can make a retried insert observe its own committed write as a false conflict). Under a relaxed backend (`AwaitDurable=false`) "durable on the owner" is made true by a SYNCHRONOUS owner-backend flush on this under-W branch (the lone memtable copy is pushed to the object store before success; see outcome (c)) - the one exception being a flush that itself fails, which is surfaced terminally as a not-committed error rather than a false success. This is otherwise the same success-but-under-W shape a single-key Put has: the operator's `WriteConsistency` choice governs the durability guarantee, and an under-W result means the write landed on fewer than W replicas, not that it was rolled back. Replica-count healing is via apply-if-newer read-repair on the next quorum `Get`; the fan-out's own `WriteTimeout`-bounded retry is NOT a forward heal (it is already exhausted at under-W return, which is what produced the under-W outcome). Each replica applies the whole batch atomically through its OWN local `backend.Transaction` (begin, apply all entries, commit), so a replica holds either the entire write-set or none of it. There is no 2PC across replicas: replicas are apply-only.

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
- **Network partition**: nodes on each side see the other as failed. Both sides accept writes. On heal, conflicts resolve via Last-Write-Wins (LWW) using the originator's stamp (drawn from the per-node monotone stamp source) + nodeID tiebreak (see "Replication (v0.4+)" for the full envelope + comparator). R=1 has no replication conflicts; R>1 relies on LWW.
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
  - `shale stats` - per-node counters: key count and lifetime Put/Get/Delete/Scan request counts, plus the node's mount-readiness counts. It reports COUNTERS ONLY: the node keeps no latency histogram, so `stats` carries no percentiles (latency is measured client-side by `shale bench`, which times the ops it issues)
  - `shale ping` - liveness check; exits 0 if the node responds

Subcommands shipped in later versions:

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
- **Unit ownership via the SAME ring.** Unit U is owned by `ring.LocateKey(unitID-bytes(U)).ID`. A bare unit id carries no `{...}` tag, so it hashes whole; units are fed through the one existing ring (no second ring). The cluster resolves that through its own ring-backed `genUnitOwner` (the generation-qualified `LocateKey` over `genUnitBytes`) and uses it for every unit owner question.
- **Mount owned units on Open.** Derive the node's owned units with the cluster's ring-backed `desiredGenUnits()` (at R>1, `desiredReplicaUnits()`, whose pure core is `storageunit.OwnedReplicaUnits`), then `factory.OpenUnit(u, epoch)` each into a `unit -> backend.Backend` mount map the cluster holds. Real epoch fencing is a Phase-3 concern: Phase 2 opens at a fixed/zero epoch (clear TODO, no durable epoch logic invented here).
- **Routing: key -> unit -> owner -> mount-or-forward.** For every op (Put/Get/Delete/ScanPrefix and the CAS/commit path): `shardKey = ring.ShardKey(key)` (the SAME extraction the ring uses, so co-location holds), `unit = storageunit.UnitForShardKey(shardKey, UnitCount)`, `owner = ownerLookup(unit)`. If owner == self, apply against `mountTable.mounts[unit]` (NOT a single `c.backend`); else forward to the owner over the existing gRPC path. The forwarded op carries the key; the receiver re-derives the unit and applies against its own mount map.
- **Unit-based owner guard.** The forwarding loop-guard (`OwnsKey` / the forwarded-but-not-mine refusal) becomes unit-based: a forwarded op whose unit this node has not mounted is REFUSED, not re-forwarded. On `Close`, close every mounted unit (`factory.CloseUnit` each) before the node shuts down.

**IN SCOPE: static topology.** The unit set a node owns is fixed at Open. If membership changes mid-run, Phase 2 may serve stale ownership for the moved units; that is acceptable and documented, exactly as v0.2 served stale routing before v0.3. **OUT OF SCOPE (later phases):** rebalance / lease handoff and mount-unmount on membership change (Phase 3); epoch fencing / writer-epoch handoff (Phase 3, opened at a fixed epoch here); doubling / resharding and the migration tool (Phase 4+). The legacy per-node path is untouched.

### v0.8 Phase 2b: R>1 (replicated) multi-backend, static topology

Phase 2 (and Phases 3, 4) are single-replica (R=1): each unit has exactly ONE durable database and ONE owner; `ApplyBatch` is refused in multi-backend mode with "unsupported in multi-backend mode (single-replica)". Phase 2b extends Phase 2 to R>1 (replicated): each unit has R INDEPENDENT replicas, each a separate database in shared storage, mounted on R different nodes. A node crash is fully recoverable (a surviving replica is a complete independent copy of the unit), and a node leaving cannot orphan a unit's only copy. This mirrors exactly how v0.2 did static routing before v0.3 added rebalance: the unit -> replica-set assignment is FIXED at Open; membership-change handoff at R>1 is OUT of scope (a later phase). It reuses the legacy R>1 write/read/apply machinery verbatim, re-keyed from per-NODE ownership to per-UNIT ownership.

- **The ring yields R replica nodes per unit.** Phase 2 resolved a unit's single owner with `ring.LocateKey(genUnitBytes(gu))` (`genUnitOwner` / `unitOwnerOf`). Phase 2b resolves a unit's REPLICA SET with `ring.LocateKeyN(genUnitBytes(gu), R)`: the primary plus its (R-1) ring successors, the SAME successor-chain machinery the legacy per-node R>1 path uses, but hashing the generation-qualified unit id instead of the raw key's shard key. A new `unitReplicas(gu) []ring.Member` in the cluster layer wraps this lookup; `genUnitOwner` / `unitOwnerOf` stay R=1-exact (Phase 2b never narrows them) and the new replica-set lookup is additive. Co-location is preserved by construction: every key in a `{tag}` set hashes to one unit, and that unit has one replica set, so the set's whole replica placement is identical.

- **`OwnedUnits` generalizes to (unit, replica-position).** Phase 2 derived "which units do I mount" by keeping a unit iff its SINGLE ring owner is self. Phase 2b generalizes this to keep a unit iff self appears ANYWHERE in the unit's R-member replica set, PAIRED with the position self holds. The pure domain stays pure: `pkg/storageunit` gains a `ReplicaLookup` interface (`ReplicasOf(u UnitID) []NodeID`, the ordered replica set) and `OwnedReplicaUnits(self, c, replicas) []OwnedReplica` (enumerate 0..N-1, keep every unit whose replica set contains self, recording self's index as the `OwnedReplica{Unit, Replica}` position). The cluster has a SEPARATE generation-aware derivation `desiredReplicaUnits() []ReplicaUnit` for the R>1 mount (it qualifies each `OwnedReplica` with the live generation and resolves the replica set via `unitReplicas(gu)`, the ring `LocateKeyN` over `genUnitBytes(gu)`); the R=1 `desiredGenUnits` remains the single-owner derivation, resolved through the same ring-backed `genUnitOwner`.

- **Each replica is an INDEPENDENT durable database.** This is the load-bearing factory contract. At R=1 a unit `gu` has ONE durable backing store, keyed by `GenUnit` alone (the lease moves between nodes, the bytes stay put: `sharedfactory.Backing.stores map[GenUnit]*memory.Memory`, and the deployable slate factory's one slatedb prefix per `GenUnit`). At R>1 the R replicas of `gu` are R SEPARATE databases that must each persist independently, so the durable identity is `(GenUnit, replica-position)`, NOT `GenUnit`. The domain models this as a `storageunit.ReplicaUnit{Unit GenUnit, Replica uint8}` value object, and the factory mounts a unit at a replica position via a CAPABILITY interface `storageunit.ReplicaBackendFactory` (`OpenReplicaUnit(ru, epoch)` / `CloseReplicaUnit(ru)`) that EXTENDS `BackendFactory`. R>1 support is opt-in: a factory advertises it by implementing the extension; the R=1 factory contract (one durable store per `GenUnit`, copy-free handoff) is UNCHANGED, so single-replica factories (and the deployable slate factory, until it adopts the extension) are not forced to grow a replica argument they do not use. The cluster validates at Open that the factory implements `ReplicaBackendFactory` when R>1 (a single-replica factory at R>1 is a config error, not a runtime nil-deref). Concretely the test `sharedfactory.Backing` keys a SECOND pair of maps (`replicaStores` / `replicaEpochs`) by `ReplicaUnit` (so replica 0 and replica 1 of one unit are distinct stores with distinct fences that survive each other), and the deployable slate factory would derive R distinct DbName prefixes per `GenUnit`. The node-side mount map stays `mountTable.mounts[GenUnit]backend.Backend` (a node holds AT MOST ONE position per unit, because a replica set is distinct members, so the per-node mount is unambiguous keyed by `GenUnit`); the cluster carries a parallel `replicaPos map[GenUnit]uint8` recording the position this node holds each unit at, so Close releases the right per-replica database. The durable-per-replica identity lives in the factory; the node mount stays `GenUnit`-keyed.

- **Write path: key -> unit -> fan out to the unit's R replicas -> apply-if-newer into each replica's mount.** The originator resolves `replicas = unitReplicas(genUnitForKey(key))` (the unit's R replica nodes) and fans the write out exactly as the legacy `putReplicated` does, but over the UNIT's replica set rather than `ring.LocateKeyN(shardKey(key), R)`. Reuse verbatim: `fanout` (the ack/failure-budget engine), `requiredWriteAcks` (W per `WriteConsistency`), the `Stamp` + `Encode(Envelope{...})` stamping, the `WriteTimeout`-bounded dispatch + surplus drainer. The ONLY new code is a multi-backend dispatcher: where legacy `dispatchReplicaPut` applies locally via `c.applyEnvelopeIfNewer(key, env)` (which writes the single `c.backend`) and remotely via `cli.PutForwarded`, the multi-backend dispatcher applies locally via a new `applyEnvelopeIfNewerToUnit(key, env)` that resolves the key's mounted unit backend (`localWriteBackendForKey`, already used by the R=1 multi write path, which holds the per-unit write-pause read-lock and returns the mounted `backend.Backend`) and runs the SAME never-clobber apply-if-newer compare against THAT backend's transaction (`txApplyIfNewer` + `tx.Put`), and remotely dispatches `cli.PutForwarded` to the replica node, whose RPC handler lands in `LocalReplicaPut`. `LocalReplicaPut`'s multi-backend branch already resolves `localWriteBackendForKey` and writes the mounted unit; Phase 2b changes its multi branch to apply the incoming envelope APPLY-IF-NEWER against that unit (the bytes are an LWW envelope at R>1) instead of the raw `b.Put` it does at R=1. Owner-but-unmounted (handoff window) and stale-mount eviction stay exactly as Phase 3 built them: the retryable `errUnitAcquiring` and `evictStaleMount` are unchanged, so a not-yet-mounted replica returns the transient code the fan-out tolerates. Delete is a tombstone (empty-payload stamped envelope) through the same path, identical to legacy.

- **CAS write-set fan-out re-keyed per unit.** `CommitCASApply` at R>1 already encodes one shared-stamp envelope per write-op and, after the owner-local commit, fans the batch out via `replicateCASBatch`. In multi-backend mode the owner-local commit goes through `localBeginForKey(pinKey, level)` (already the multi path: it opens a tx on the pin key's mounted unit). `replicateCASBatch` is re-keyed: the replica set is `unitReplicas(genUnitForKey(pinKey))` instead of `ring.LocateKeyN(shardKey(pinKey), R)`; the per-replica dispatch (`dispatchApplyBatch`) sends the batch to each replica node's `ApplyBatch` RPC -> `ApplyBatchLocal`. `ApplyBatchLocal`'s multi-backend guard (the current hard refusal) is REPLACED by the real per-unit apply: open one transaction on the batch's mounted unit backend (`localWriteBackendForKey` on the pin key, or per-key the unit each `EnvelopeWrite.Key` resolves to - the cross-shard guard guarantees they co-shard, so one unit covers the batch) and run the existing apply-if-newer loop (`txApplyIfNewer` + `tx.Put`) inside it, under `c.applyMu`, committing the whole batch together. The R=1 multi guard (refuse) is removed only on the R>1 path; an R=1 multi cluster never sends `ApplyBatch` and the refusal can stay for that case, or fold into the `casReplicated()`-style predicate.

- **Read path: ReadConsistency across the unit's replica nodes.** `getReplicated` already reads from N-of-R replicas per `ReadConsistency` (1 / quorum / R), picks the LWW max-stamp winner, and read-repairs laggards. In multi-backend mode the replica set is `unitReplicas(genUnitForKey(key))` instead of `ring.LocateKeyN(shardKey(key), R)`; the per-replica read dispatch (`dispatchReplicaGet`) reads locally via the key's mounted unit backend (`localBackendForKey`, already used by the R=1 multi `Get`) instead of `c.backend.Get`, and remotely via `cli.GetForwarded` (whose handler serves from the replica's mounted unit via `LocalGet`, already multi-aware). The LWW winner selection, the `ReadTimeout`-bounded dispatch, read-repair (which rides the same multi-backend `dispatchReplicaPut`, so a stale repair is a never-clobber no-op against the mounted unit), and tombstone -> `ErrNotFound` are all reused verbatim. A replica node that has not mounted the unit returns the transient acquiring-window error, which the read fan-out skips while another replica satisfies the consistency target.

- **Fenced owner-local ops self-heal intrinsically (the mounted-handle decorator).** A replica that has the unit MOUNTED but whose handle has been FENCED - a higher-epoch owner superseded it during a membership change, and production slatedb CLOSES the stale handle so EVERY op (read OR write) fails with `backend.ErrFenced` - must NOT return the raw fence. The fix is structural, not per-callsite: every backend stored in the mount map is wrapped, at the SINGLE mount seam (`mountTable.mountLocked`), in a fence-self-healing decorator whose owner-local point ops (`Get` / `Put` / `Delete`) recode a fenced result to the TRANSIENT acquiring-window error AND evict the stale mount (`fenceToTransient` / `evictStaleMount`). So the fence-recode-evict is an INTRINSIC property of the mounted handle - a routed read skips that leg while another replica answers, the reconcile re-acquires the evicted position at a fresh epoch, and a NEW owner-local op path cannot reintroduce the bug because it simply calls a method on a handle that already heals. (The streaming scan iterator and the transactional CAS path keep their own mid-stream / mid-transaction fence handling; both resolve THIS decorator from the mount map, so their `fenceToTransient` evicts the right mount. The decorator delegates `ScanPrefix` / `Begin` to the inner backend so those paths are unchanged. `evictStaleMount` keeps its Draining-mount guard, so a fenced op on a mount mid-handoff recodes WITHOUT eviction and the overlap handoff is unaffected.) This replaced an earlier shape that wrapped each owner-local op site by hand - exactly the brittleness that wedged the routed GET forever (#433): the point-GET path returned the raw fence and the mount, still in `mountTable.mounts`, was never re-acquired (the reconcile saw it as healthy), so reads AND writes to the unit stayed dead - load-bearing because hostthis's per-subnet keygate admission READS before it writes, so the read fence blocked the write that would otherwise self-heal. The in-process test factory DEFAULTS to production-shaped close-on-fence semantics (strict read fencing + eager fence at open start), matching production slatedb's closed-handle-on-read; the permissive writes-fail / reads-PASS model survives only as a per-test opt-out (`SetStrictReadFencing(false)` / `SetEagerFence(false)`, justified inline). It is pinned by a test that runs the factory's default close-on-fence semantics and asserts a routed GET to an all-replicas-fenced unit RECOVERS (mounts evicted + re-acquired) instead of wedging, plus a real-slatedb integration check that a `Get` through a fenced handle returns `backend.ErrFenced`.

- **Top-level dispatch.** `Put` / `Get` / `Delete` / `CommitCASApply` gain a multi-backend R>1 branch BEFORE the current `c.multi` R=1 branch: a new predicate `c.multiReplicated()` (the multi analogue of `casReplicated()`: `c.multi && replicationFactor() > 1 && ring populated`) routes to the per-unit replicated paths above. When `c.multi` but R=1, the existing Phase 2/3/4 single-mount paths run unchanged. When not `c.multi`, the legacy per-node paths run unchanged. The three modes are mutually exclusive and each is byte-for-byte preserved outside its own branch.

- **NO ACKED WRITE LOST (how the invariant holds).** Identical structure to the legacy R>1 proof, now per unit. (1) FAN-OUT ACK ACCOUNTING: a write is acked to the client only after W of the unit's R replicas have durably applied it (W per `WriteConsistency`, the owner's local commit counted as one ack, the same `requiredWriteAcks` math). Fewer than W acks returns `codes.Unavailable` and the write is NOT acked (the client retries; Phase 2d makes that retry internal + bounded). A replica mid-acquire returns the sentinel-tagged transient `errUnitAcquiring`, which (per the Phase 2d budget fix) counts toward neither acks nor the failure budget, so a single not-yet-mounted replica cannot fail an otherwise-satisfiable W. (2) NEVER-CLOBBER APPLY: every replica applies APPLY-IF-NEWER (`txApplyIfNewer`: write only if the incoming stamp strictly beats the stored stamp), so a reordered older write or a stale read-repair self-resolves to a no-op and an acked write is never overwritten by a staler one. (3) INDEPENDENCE: the R replicas are independent durable databases, so the loss of one node (hence one replica) leaves W-1 or more complete copies; any acked write reached W replicas, so at least W-1 surviving replicas still hold it and a quorum read finds it. The static-topology boundary (next) means the replica SET never changes mid-run, so no acked write can be stranded on a replica the ring later stops counting.

- **DDD shape.** `pkg/storageunit` stays PURE (no I/O): it gains `ReplicaLookup` (the ordered replica-set lookup the ring is adapted to), `OwnedReplicaUnits` / `OwnedReplica` (the pure core of the R>1 mount derivation), the `ReplicaUnit{Unit GenUnit, Replica uint8}` value object for the durable per-replica identity, and the `ReplicaBackendFactory` capability interface (the R>1 factory seam) - all plain data + pure interfaces, trivially testable without a ring or a factory. The R>1-per-unit WIRING lives entirely in the cluster layer (`multibackend_replicated.go`): `unitReplicas` (ring `LocateKeyN` over the unit id), the per-unit write/read paths (`putReplicatedUnit` / `getReplicatedUnit`, structural mirrors of `putReplicated` / `getReplicated`), their multi-backend dispatchers (`dispatchReplicaPutUnit` / `dispatchReplicaGetUnit` / `applyEnvelopeIfNewerToUnit` / `applyBatchToUnit` / `scheduleReadRepairUnit`), the re-keyed `replicateCASBatch` replica-set resolution, and the `multiReplicated()` predicate. The envelope / apply-if-newer / quorum / fan-out PRIMITIVES are REUSED, not forked: `fanout`, `txApplyIfNewer`, `Encode`/`Decode`, `requiredWriteAcks` / `requiredReadReplicas`, the `collected` LWW-winner shape, the `repairWG` / `repairCtx` lifecycle. (The per-unit write/read paths are thin structural mirrors of the legacy node-keyed ones - the same fan-out engine, the only change being the replica set is the UNIT's and the local apply lands in the mounted unit backend - kept as separate functions so the legacy per-node path stays byte-for-byte untouched.) The factory contract (per-replica durable identity) is an infrastructure-adapter change in `internal/sharedfactory` (and a future deployable slate factory), depending on the domain's `ReplicaUnit` / `ReplicaBackendFactory`, never the reverse.

**THE R>1 MULTI-BACKEND LOSSLESS GATE (the data-loss oracle).** A new in-process integration gate (`tests/integration/lossless_multibackend_r2_gate_test.go`) stands up a 3-node multi-backend cluster at R=2 on a per-replica shared-backing factory, writes a recorded BASELINE dataset spanning every unit (including co-located `{tag}` sets), then runs a CONCURRENT PROBE that keeps acking new keys THROUGH THE FULL ROUTED SURFACE (writers rotate the entry node so writes route both locally and forwarded; a key is recorded ONLY once its Put returns nil), folding those acked keys into the recorded set. It then asserts: (1) THE ORACLE - every baseline key AND every acked probe key is readable with its EXACT value from EVERY node (forwarding to a replica), zero loss; (2) each unit is mounted on EXACTLY R=2 distinct nodes (the replica set), and a write landed on both; (3) co-located sets share one unit hence one replica set; (4) SURVIVE-ONE-LOSS - wiping ONE replica copy of each unit still reads every value (the surviving replica is a complete copy). It is kept honest by a BREAK DEMONSTRATION (`TestR2GateCatchesLostWrite`) that deliberately drops the fan-out to one replica (or wipes one replica's store) AND removes the other replica's copy, then shows the oracle FAILS (catches the lost write) - proving the gate is not a rubber stamp, exactly like the Phase 3 / Phase 4 gates. (Surviving a single replica loss is asserted positively; the break removes BOTH copies so the loss is genuine.) Recording ONLY acked keys is what keeps the oracle honest: a write the cluster never acknowledged is not owed durability, so an unacked-then-lost key cannot mask a genuine loss, and a key the probe DID get an ack for that then fails the readback is a real violation the oracle must catch.

- **Initial-convergence mount (NOT a handoff).** A node's replica set is derived from the FULL ring, which is not known until membership converges (the founder boots alone, peers join over the next moment). So a node mounts its replica copies as the ring fills: the existing reconcile machinery, with an R>1 branch (`reconcileReplicaUnits`) whose desired set is `OwnedReplicaUnits` against the live ring, opening each newly-desired unit at its replica POSITION (an independent durable database) and releasing ones it no longer replicates. This is NOT a lease handoff and NOT re-replication: the R>1 replica copies are independent durable databases, so a node simply OPENS the copies the static ring assigns it (no bytes move between nodes, no copy). Once the cluster has converged the topology is static; a later phase adds true membership-change re-replication. Because the gate writes only AFTER 3-node convergence, every acked write lands on a fully-resolved replica set.

**IN SCOPE: R>1 replicated multi-backend under a STATIC unit -> replica-set topology** fixed once membership converges at Open, reusing the legacy R>1 write/read/apply machinery re-keyed per unit. **OUT OF SCOPE (later phases):** membership-change lease handoff / re-replication at R>1 AFTER convergence (the replica set is static post-convergence; a node join/leave once the cluster is formed re-mounts a reassigned replica's EXISTING shared-storage copy, and Phase 2d below keeps writes AVAILABLE across that re-mount, but standing up a brand-new R-th copy by re-replicating bytes onto a member that has none is still a distinct phase); resharding / doubling at R>1 (Phase 4 is R=1 only; SUPERSEDED by "v0.9 Decentralized reshard" below, which adds R>1 resharding via the CAS arbiter - this scope line records the Phase 2b boundary as it stood, not current capability); anti-entropy re-replication to restore R copies after a permanent node loss. The legacy per-node R>1 path (keyed to node ownership) is UNCHANGED byte-for-byte; the R=1 multi-backend paths (Phases 2/3/4) are UNCHANGED outside their own branches.

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

  1. **Inside the fan-out: make `errUnitAcquiring` actually transient (the budget fix).** The Phase 2b invariant that a mid-acquire replica "counts toward neither budget" is made TRUE by giving the fan-out a way to recognize the acquiring-window code. `errUnitAcquiring` keeps `codes.Unavailable` ON THE WIRE (it is the retryable code the client/forwarding shape already understands, and Phase 3 / the reshard paths depend on that), but it is tagged so `isTransientReplicaErr` can distinguish "this replica is mid-acquire, skip it and wait for the budget to refill" from "this replica is down, count it." The chosen tag is a SENTINEL wrapped error: `errUnitAcquiring` wraps a package-private `errAcquiringSentinel`, and `isTransientReplicaErr` returns true when `errors.Is(err, errAcquiringSentinel)` OR the gRPC status is `codes.ResourceExhausted`. The sentinel survives the local-self dispatch path verbatim (it is a Go error, not re-wrapped). For the REMOTE dispatch path the sentinel does not cross gRPC (only the status code does), so the cross-node acquiring refusal is ALSO emitted by the replica's RPC handler as `codes.ResourceExhausted` for forwarded replica writes (`LocalReplicaPut` / `ApplyBatchLocal`'s mid-acquire branch), the SAME transient code the migration guard already uses, which `isTransientReplicaErr` already skips. (The CLIENT-facing top-level refusal stays `codes.Unavailable` - only the INTERNAL replica-to-replica fan-out leg re-codes to the transient `ResourceExhausted`, exactly as the v0.3 rebalance migration guard does for the same reason.) With this, a single mid-acquire replica no longer consumes the R=2 failure budget: the fan-out waits for the OTHER replicas to satisfy W, and if the acquiring replica finishes its mount before `WriteTimeout` it lands as an ack.
  2. **Around the op: a bounded retry when the WHOLE fan-out cannot reach W.** Layer 1 fixes the common case (one of R replicas acquiring, the rest ack). But during a big churn it is possible for ENOUGH of a unit's replicas to be simultaneously mid-acquire that W acks are not reachable in one fan-out pass (e.g. R=2 with BOTH replicas re-mounting). Layer 2 catches this: when a replicated write returns the retryable acquiring/quorum-unavailable outcome, the Put / Delete / CommitCASApply surface RE-RUNS the fan-out after a short backoff, bounded by the SAME `WriteTimeout` wall-clock budget, so the write waits for the acquires to settle rather than erroring. The retry re-resolves the replica set from the LIVE ring on every attempt (so a write started mid-reassignment lands on the post-reassignment replica set once it settles), and re-stamps is NOT needed (the original `Stamp` is preserved across retries so apply-if-newer ordering is stable; a fresh attempt may re-stamp safely since the write was never acked). At R=1 multi-backend the same wrapper rides the single-owner `Put` / `putForwarded`: the owner-but-unmounted `errUnitAcquiring` is retried under `WriteTimeout` instead of returned, closing the R=1 gap the same way.

**Retry / timeout policy (precise).**

  - **Budget:** the ENTIRE retry sequence for one logical write is bounded by `WriteTimeout` (default 5s). The first attempt starts the clock; each retry checks remaining budget before sleeping and gives up (surfacing the last retryable error to the client) once the budget is exhausted. There is no separate retry-count cap; the wall-clock bound is the cap, so a fast-acquiring cluster retries a few times and a pathologically-slow one degrades to one timeout-bounded error per write (never a busy-spin, never a deadlock).
  - **Backoff:** start at `RebalanceRetryAfterMs` (default 50ms, the existing handoff retry-after hint), with a small exponential growth (x2) capped well under `WriteTimeout` (cap 500ms), plus jitter (50-100%) so a thundering herd of simultaneously-acquiring writes does not re-collide on the same retry tick. This reuses the same retry-after knob and backoff shape the v0.3 cutover / freeze-window client retries already use; it is not a new tuning surface.
  - **Only retryable outcomes retry.** A retry fires ONLY for the acquiring/quorum-unavailable retryable family (the sentinel-tagged `errUnitAcquiring`, the cut-over-window `codes.Unavailable`, and the under-W `codes.Unavailable` that a fan-out returns when acquiring replicas kept it from reaching W). A NON-retryable failure (a genuine peer-down `Unavailable` with no acquiring replica in play, an empty-value rejection, a closed cluster, a decode error) is returned IMMEDIATELY, unretried - the retry must not paper over a real outage by spinning until `WriteTimeout`. Distinguishing "under-W because replicas are acquiring" from "under-W because replicas are DOWN" reuses layer 1's classification: if the fan-out's accumulated errors are all transient-acquiring (none counted toward the failure budget) yet W was not met, the outcome is retryable; if real peer-down failures exhausted the budget, it is not.

**How DURABILITY stays intact (NO ACKED WRITE LOST is preserved).** The retry changes only WHEN a write is acked, never WHETHER an acked write survives. (1) A write is still acked to the client ONLY after W of the unit's R replicas durably applied it; the retry just gives the fan-out more time to collect those W acks across the handoff window. (2) The new owner of a reassigned replica opens the SAME shared-storage per-(unit, replica) database the old owner flushed (durable-before-ack: every acked write is durable before the lease moves), so a write that the retry eventually lands on the freshly-mounted replica sees a database that already contains every previously-acked write - the apply-if-newer compare runs against the true durable state. (3) An un-acked write that times out during the churn is, as before, never reported as success, so the client retries it; no acked write is ever in the lost set. The Phase 2b R>1 lossless gate's oracle (record only acked keys, assert every acked key is readable) is the exact check that this holds, and it is EXTENDED by Phase 2d's new gate below.

**How the SINGLE-WRITER FENCE stays intact (no two writers on one (unit, replica) database).** The retry does NOT relax the epoch fence. A reassigned replica is acquired by `OpenReplicaUnit` at a strictly-higher epoch than the unit's durable lease (the same fence Phase 3 uses for R=1 handoff), which locks out any stale prior holder of that replica position before the new owner serves a single write. The retry only re-dispatches a write that was REFUSED (the acquiring replica explicitly did NOT apply it); it never dispatches the same write to two live holders of one (unit, replica) database, because at any instant exactly one node holds the highest-epoch lease for that position and only that node's mount answers a routed op (every other node either is not the ring owner - refused by the loop guard - or is mid-acquire and itself refuses with the retryable signal). A write that races the flip is refused on the losing side (stale-mount eviction -> `errUnitAcquiring`) and retried onto the winning side. So across the whole retry sequence a given (unit, replica) database has exactly one writer at a time; the retry adds latency, not a second writer.

**THE R>1 WRITE-AVAILABILITY-THROUGH-MEMBERSHIP-CHANGE GATE (the new acceptance test).** A new in-process loss-oracle test (`tests/integration/membership_change_write_availability_test.go`, in-process, sharedfactory + memory backend, NO slatedb tag, NO MinIO) is the acceptance bar for Phase 2d. It: (1) stands up an N-node multi-backend cluster at R=2 on the per-replica shared-backing factory and writes a recorded BASELINE; (2) starts a CONTINUOUS WRITER that keeps acking a tracked keyset through the routed surface, recording a key ONLY once its Put returns nil, and counting attempted-vs-acked; (3) BEGINS A MEMBERSHIP CHANGE (adds nodes / triggers a scale event) WHILE the writer runs, so writes land on units mid-reconcile; (4) asserts BOTH (a) THE ORACLE - every baseline key and every ACKED probe key is readable with its exact value from every node, ZERO acked loss; AND (b) THE ACK RATE - acked / attempted is HIGH (>95%; the bar the staging chaos expects to jump to ~100%). The test is kept honest by demonstrating it FAILS on the pre-fix code: with the retry removed (or `errUnitAcquiring` left counting as a failure), the same membership change drives the ack rate WELL below the threshold (matching the measured ~50-64%), so the gate proves the fix rather than rubber-stamping it. The oracle's record-only-acked discipline is what makes the two assertions independent: durability (a) must hold even at a low ack rate (the old behavior was lossless-but-unavailable), and the fix is the ack-rate (b) climbing to >95% WITHOUT regressing (a).

**IN SCOPE: write availability through an R>1 (and R=1) multi-backend membership change, via a `WriteTimeout`-bounded retry-on-acquiring** (layer 1 makes the acquiring refusal a true fan-out transient; layer 2 retries the whole op when W is momentarily unreachable). The handoff-window refusal becomes bounded LATENCY, not an error. **SUPERSEDED FOR THE BIG-CHURN CASE by Phase 2e (pending ranges) below:** Option A's availability is bounded by mount-time-vs-`WriteTimeout`. Measured on real 3-node staging: incremental scale 3 -> 4 stays ~100% (few units remount, fast), but big-bang 3 -> 12 (about 16 slatedb databases remounting at once, real MinIO mounts exceeding the 5s budget) drops to ~54% as the retry budget exhausts. Phase 2e removes that bound: during a transition the routed replica set is the UNION of the current owners (which still have the data mounted) and the pending owners, so a write/read always reaches a node that physically holds the position, and availability no longer depends on mount time at all. Option A is RETAINED as the safety net for the residual unserved instant Phase 2e leaves (a node crash mid-handoff, the pure-new-mount initial convergence); A and B compose (A is the belt, B shrinks the window A waits on to near zero). Also out of scope (unchanged from Phase 2b): re-replicating a unit's BYTES onto a newly-joined replica position that has no durable copy yet (the copy-free case - a node taking over an EXISTING replica position - is what 2d and 2e handle; standing up a brand-new R-th copy after a permanent node loss is the separate anti-entropy re-replication phase).


### Refusal reasons: the consumer-facing error taxonomy

shale is embedded IN-PROCESS by library consumers (open a `Cluster`, then call `Transact` / `Get` / `ScanPrefix` / `Aggregate`). Routing and peer forwarding both happen INSIDE the cluster, BELOW that seam. A refusal raised by a locally-mounted position and one forwarded from a peer therefore return through the SAME call site, and the consumer has no way to tell them apart. That is a real blocker, not a cosmetic one: a consumer that wants to retry the transient handoff blip but NOT a genuine outage cannot express the difference, because the gRPC status code alone is far too coarse (the acquiring-window refusal and a dead peer are BOTH `codes.Unavailable`) and a message-string match is not a contract anyone should ship. Refusal reasons close that gap.

**The contract.** Each reason has exactly ONE exported sentinel, and matching it is PATH-INDEPENDENT:

```go
if errors.Is(err, cluster.ErrAcquiring) {
    // bounded retry with backoff; the handoff will finish
}
```

`errors.Is(err, cluster.ErrAcquiring)` holds whether (a) the position is owned by THIS node and mid-acquire (in-process dispatch), or (b) the op was FORWARDED to a peer that was mid-acquire (over gRPC). The consumer does not know, and must not need to know, which path produced the error. That path-independence is the whole deliverable.

`ErrAcquiring` documents its retry contract: the refusal is TRANSIENT and SAFE TO RETRY, because it is EXPLICIT (the op was not applied, no partial state was written, no stale result was served) and BOUNDED by the handoff window (a mount, not an outage). It is NOT a peer-down signal: it shares `codes.Unavailable` with genuine unreachability, which is exactly why the code is the wrong thing to branch on.

**SIZING THE RETRY: the bound is a MOUNT, so the budget is in SECONDS.** "Bounded retry with backoff" says nothing about TIMESCALE, and a consumer with no other signal will reach for the sub-second policy that is right for an RPC blip. It is the wrong order of magnitude here. The window is the time to OPEN a per-`(unit, replica)` database from object storage, not a round trip, which puts it in the SECONDS-TO-TENS-OF-SECONDS range. Observed on a real 3-node cluster on object storage during a rolling restart: positions stayed owned-but-unmounted for roughly 17 to 21 seconds. That is an OBSERVATION under those conditions, NOT a guarantee and NOT a constant to hard-code - it moves with backend latency, units per node, and cluster size, so a consumer sizes its total budget against its own backend's mount time and confirms the figure by observation. The failure this prevents is a quiet one: a policy whose TOTAL budget is under a few seconds typically EXHAUSTS rather than absorbs the window, so a refusal shale classified as retryable surfaces to a user as a failure while the retry still looks like it is working. The window length is not discoverable from the API, which is why it is documented here and on the sentinel.

**Where the budget is worth spending: the exposure is asymmetric.** Single-key REQUEST-PATH reads may not reach this retry at all - the read path's own in-budget re-poll absorbs the narrow window on a point op, and across measured rolling deploys such reads did not fire a consumer's retry once. The BACKGROUND CROSS-SHARD FAN-OUT (`Aggregate`) is where it actually bites, for the same reason it is the highest-exposure entry point generally (see the SHAPE-INDEPENDENT section below): a fan-out meets the window whenever ANY node holds ANY owned position unmounted, rather than only when the single addressed position is mid-mount. A background fan-out is also where a wide budget is affordable, since it is not bounded by a request deadline.

**The match is also REPLICATION-INDEPENDENT, and that takes a second mechanism.** The node boundary is not the only place a reason can be lost; the other is a FAN-OUT COLLAPSE. At R>1 a write is not one refusal but R legs, and falling short of W collapses them into a SINGLE freshly-minted status (`classifyWriteAttempt`), discarding every leg value. A contract that held only at R=1 would therefore be a FALSE NEGATIVE on exactly the configuration a replicated consumer runs - their writes unprotected while the gate appears to work - so the collapse has to preserve the reason too. It does: the retryable terminal is built by `reasonTerminal`, which carries BOTH halves (the in-process sentinel AND the wire detail, since the terminal is minted on whichever node the write entered, which for a forwarding consumer is a peer). `Put`, `Delete`, and `Transact`'s CAS commit all reach it.

**The READ path needs the same evidence rule, and for a while it did not have it.** The write terminal preserves the reason across a fan-out collapse; the read terminal used to lose it across a BUDGET EXPIRY. `retryReadThroughHandoff` re-polls while every leg is mid-acquire, but as the `ReadTimeout` budget runs out the final attempt's context expires and its legs report `DeadlineExceeded` - a hard error, so the loop returned THAT in place of the acquiring evidence it had already gathered across earlier attempts. The result was a false negative in the same family as the R>1 collapse: shale knew a unit was moving and handed back an error that said only "slow".

That is not a cosmetic loss, because the consumer CANNOT recover the distinction themselves. Matching `DeadlineExceeded` as retryable collides with a genuinely overloaded read and converts a real outage into a retry storm; not matching it abandons a window that would have healed. The discriminator exists only inside the retry loop, which is the one thing that saw the earlier attempts.

So the read terminal now applies the same evidence rule as the write terminal: `attributeToAcquiring` joins `ErrAcquiring` onto the terminal error when, and only when, (a) this call actually observed an acquiring refusal on an earlier attempt, and (b) the terminal error is specifically a deadline expiry (either the local `context.DeadlineExceeded` or the `codes.DeadlineExceeded` a remote leg returns when its own context expired). `errors.Join` keeps the original deadline error matchable, so nothing is hidden - the reason is ADDED, never substituted. Absent that evidence a deadline stays a plain deadline, which keeps the sentinel honest: `ErrAcquiring` continues to mean "a handoff was observed", not "this was slow".

**The WRITE retry has the same truncation hole one layer up, closed the same way (v0.14.2).** All of a write's retry attempts share one wall-clock budget; when it expires MID-FAN-OUT, the final attempt's accounting snapshot can hold an ack from the mounted replica but no refusal from the mid-acquire one - its refusal had not landed when the context fired. `classifyWriteAttempt` then sees acks short of W with ZERO transient legs, and evidence-based reason attachment correctly finds nothing to attach: the terminal is the plain retryable status, and a consumer gating on `errors.Is(err, ErrAcquiring)` gets a false negative on the LAST attempt precisely because it was the truncated one. `retryWriteThroughHandoff` therefore never lets a reasonless retryable terminal from a truncated final pass REPLACE a reason-carrying terminal an earlier attempt produced: the reason this call already observed is preserved onto the returned error. Evidence-based, as everywhere on this seam - a run whose EVERY attempt was truncated stays reasonless, pinned by test.

The terminal takes its reason from EVIDENCE, never from the branch it was minted in: `legsCarryReason` inspects the actual transient legs, so a shortfall caused only by v0.3 migration guards (a different reason) does not claim to be acquiring. For a leg that arrived from a PEER this depends on `recodeForwardedReplicaErr` carrying the reason detail on the re-coded `ResourceExhausted` - without it a remote mid-acquire replica is indistinguishable from any other transient at the originator, and the terminal could not honestly report it. The re-code's CODE, which is what the fan-out budget reads, is unchanged.

**The MIXED CASE reports as a hard failure and does NOT match.** When W was missed with SOME legs mid-acquire and others GENUINELY DOWN, the terminal carries no reason. `ErrAcquiring` promises a window bounded by a mount, so a bounded retry is guaranteed to observe it end; once a real failure is in the mix that promise is false and the wait is bounded by whatever revives the peer. Matching there would send a consumer's bounded retry against a genuine outage - the precise false positive this taxonomy exists to prevent - and would contradict shale's own judgment, since that branch is the one that sets `retryable: false` and declines to retry internally. Likewise the zero-evidence timeout terminal (the budget was spent before any attempt completed) carries no reason: its message names acquiring, but no leg ever reported, and the sentinel is only ever attached to evidence.

**The match is also SHAPE-INDEPENDENT, and the fan-out shape takes a third mechanism.** `Aggregate` is the highest-exposure entry point, not the lowest: every other call routes ONE key to ONE position, so it meets a handoff window only when that position is mid-mount, while a fan-out meets one when ANY node holds ANY owned position unmounted. It also has an error surface no point op has, and a completeness requirement no point op has.

  - **Two channels, both of which a consumer must check.** A refusal reaches the caller EITHER as `AggregateResult.Err` (shale could not run `fn` for that peer at all: its snapshot was refused, the stream would not open) OR from the iterator `fn` is already holding, in which case it leaves `fn` as whatever `fn` RETURNS - an `any`, not an error - and crosses the fan-out boundary as a VALUE. The second channel is the one a consumer forgets, and the one that fails silently when forgotten: an unmatchable refusal there is consumed as data. Both channels deliver a real error wrapping the sentinel, so ONE `errors.Is` covers both. The correct retry granularity is the WHOLE CALL, never a single peer: the refused peer's slice of the keyspace is absent from every other peer's result too.
  - **A partial result must NEVER be mistakable for a complete one.** This is a strictly higher bar than matchability, and it is what makes the fan-out path data-loss-adjacent rather than an availability question. Fan-out consumers build SETS and act on what is ABSENT from them; a referenced-blob set drives blob GC, so a key missing from the set is an object DELETED. shale's own `referencedObjKeys` -> orphan-sweep is exactly this shape, which is why it already fails the whole scan closed on a single undecodable pointer. A silently-partial scan is not a smaller answer, it is a wrong one that destroys live data, and re-running afterwards cannot undo it.
  - **The gap that produced no error at all.** A cross-shard scan walks the MOUNT MAP, so a position this node OWNS but has not yet mounted is not refused - it is simply ABSENT, and the scan ends cleanly having returned less than it owed. That is not a lost reason; it is a refusal that was never raised, and it is worse than an unmatchable error because a short answer is indistinguishable from a correct one. `scanCoverageErr` closes it: both local-scan entry points (`localScanMounted`, which the peer-facing `LocalScan` handler and the blob sweep reach, and `localMountedSnapshot`, `Aggregate`'s local leg) refuse with `errUnitAcquiring` when `MountReadiness().PendingUnits > 0`, checked BOTH before the scan and at its exhaustion so a position that goes mid-acquire mid-scan converts a clean end-of-iteration into a refusal rather than a truncation. Keying on `MountReadiness` means a node that reports itself not fully mounted and a node that refuses to scan can never disagree. The check is deliberately CONSERVATIVE: a transient reconcile window refuses the scan even where a parent unit still covers the keyspace, because the cost of that error is a retried background fan-out and the cost of the other error is deleted data.
  - **At R>1 a single cleared replica position cannot go silently partial**, since a co-replica still covers those keys; the guard fires per node on its own owned-vs-mounted diff, so the R>1 gate is a contract-preservation test rather than a loss repro.

**How the reason survives gRPC.** The identity is carried in two halves that read ONE shared table (`reasonSentinels`), so a reason can never be encodable but undecodable:

  - **Encode.** Where the refusal is serialized (`errUnitAcquiring`), the status gains a machine-readable `google.rpc.ErrorInfo` detail: a stable `Reason` string under a shale-owned `Domain`. Details from a foreign domain are ignored on decode, so a reason string minted by another service sharing the deployment can never be mistaken for shale's.
  - **Decode.** The peer client applies unary + stream client INTERCEPTORS, so every cluster-internal RPC is covered by ONE decode site rather than each call-site wrapper having to remember. A decoded error re-wraps the exported sentinel; an error carrying NO shale reason detail (notably a genuine peer-down `Unavailable`, which carries no details at all) passes through completely untouched and therefore matches no sentinel.

**What reasons deliberately do NOT change.** The reason is ADDITIVE identity layered on top of the existing signals; every classification boundary Phase 2d established stays exactly where it was:

  - **The client-facing status CODE is unchanged.** An acquiring refusal still travels as `codes.Unavailable`. The existing retry and forwarding shapes key off it, and moving it is out of scope.
  - **The INTERNAL replica-leg recode is unchanged.** `recodeForwardedReplicaErr` still converts a forwarded replica-leg acquiring refusal to `codes.ResourceExhausted`, which is what keeps a cross-node mid-acquire replica out of the fan-out FAILURE budget. The reason detail is a parallel mechanism for the CLIENT-facing refusal, not a replacement for that recode.
  - **The private classifier tag keeps its own identity, and the WRAPPING DIRECTION is what guarantees it.** The package-private `errAcquiringSentinel` now UNWRAPS to the exported `ErrAcquiring`, so every existing `errors.Is` call site against the private tag still matches and `isTransientReplicaErr` / `isAcquiringErr` behave identically. Crucially, the arrow points only that way: a WIRE-DECODED acquiring refusal wraps `ErrAcquiring` WITHOUT wrapping the private tag. The private tag means specifically "a LOCAL in-process mid-acquire replica" to the fan-out ack-vs-failure budget and to the read-leg classes; widening it to wire-arrived errors would silently move a remote acquiring refusal out of the fan-out's accounting and jump a read leg from the capped unreachable re-poll to the full handoff re-poll. Neither is in scope, so decoding attaches the EXPORTED sentinel only.

**FIRST SLICE, not a one-off.** `ReasonAcquiring` is the only reason implemented. The shape is built so siblings (fenced, acquiring, migration-guard, conflict) join by adding a constant, an exported sentinel, and one row in `reasonSentinels` - no redesign, and no change to the encode/decode plumbing, which is why the round-trip test iterates the table rather than naming a single reason.

### v0.8 Phase 2e: pending ranges (graceful membership transition, R>1)

Phase 2d (Option A) keeps R>1 writes available across a membership change by RETRYING through the acquiring window, bounded by `WriteTimeout`. That makes availability a function of mount-time-vs-timeout: a single slatedb mount from MinIO can exceed the 5s budget under a big-bang scale-out (~16 databases remounting at once), and the measured big-bang 3 -> 12 ack rate falls to ~54%. Phase 2e removes the dependency on mount time entirely by adopting the canonical consistent-hash / gossip fix for a graceful membership transition: PENDING RANGES (Cassandra / ScyllaDB / Riak-Dynamo - shale's family; see `docs/research/graceful-scaledown-prior-art.md`, 24 sources). During a transition the routing layer DUAL-WRITES a position to BOTH its current owners AND its new (pending) owners, and READS from their UNION. A node that is LEAVING (gossiped `Draining`) STAYS a current owner - it keeps the data mounted and keeps serving - and is removed from a position's routed set ONLY AFTER its pending successor has durably MOUNTED that position (proven by the serving marker). Because the union always contains a node that PHYSICALLY HOLDS the position, both reads AND writes stay available throughout, independent of how long the pending owner's mount takes. A 30s mount is a non-event. The detailed design (the current/pending/union computation, sequence, package layout, the R/W crash matrix) is in `docs/design/overlap-handoff.md`; this section is the canonical behavior.

**THIS SUPERSEDES the earlier per-position-forwarding + draining-exclusion design (REMOVED).** An earlier draft of Phase 2e did the OPPOSITE of pending ranges and it is the fragile path the prior-art survey identifies: it EXCLUDED a draining node from the ownership ring the instant it set `Draining` (so the position's owner immediately became the new/pending owner), then relied on the new owner FORWARDING routed ops back to the old owner per position during the mount. That model is proven fragile (write availability held at ~99.7% via forwarding, but post-leave READBACK failed because the new owner was owned-but-unmounted and the read routed to it while the stale mount sat on a third node; the convergence window was unstable). Pending ranges fixes BOTH read and write because the routed UNION always contains a node that physically has the data, so no per-position back-forward, no `acquiringForwardTarget`, and no `PredecessorAddr` are needed. (The POSITION-ADDRESSED WIRE FIELD, however, is RETAINED and is load-bearing - see below: what pending ranges drops is the predecessor-forward, not the ability to address a leg by explicit position.) The following machinery is REMOVED (marked dead): the draining-exclusion in `reconcileRingFromMembership`; the per-position forward path (`acquiringForwardTarget` / `forwardPutToPredecessor` / `forwardGetToPredecessor`); the `Acquiring`-state forward behavior and the `PredecessorAddr` / `Predecessor` fields on `HandoffState`; the retained `priorDesiredReplicas` + `priorAddrs` snapshot used ONLY to identify a single-hop predecessor; the single-hop-scope fallback. The following are REUSED unchanged: the serving marker (`WriteServingMarker` / `ReadServingMarker`), the gossiped `Draining` Meta bit, the per-`(unit, replica)` `mountTable.mounts` re-keyed by `ReplicaUnit`, the durable per-replica epoch fence + apply-if-newer / LWW idempotency, `DrainForLeave`, and Option A's retry as the residual-case belt. **RETAINED AND REPURPOSED: the position-addressed proto field (`ReplicaUnitRef ru` on Put / Get / Delete / ScanPrefix).** It is NOT removed - it is live on EVERY union write and read. Its ROLE changed: it no longer addresses a single draining predecessor a successor forwards back to; it addresses each leg of the union fan-out directly, because a union member does not necessarily hold the position at its own ring index (a mid-mount pending owner is not yet there; a displaced current owner no longer is), so the receiver must resolve `mountTable.mounts[ru]` by explicit position with no ring-ownership guard. An unmounted `ru` returns the retryable acquiring error and the originator skips that leg. Sent by `PutAtReplica` / `GetAtReplica` / `DeleteAtReplica` / `ScanPrefixAtReplica`.

**The routing contract: current / pending / union replica sets.** Routing is computed per-op inside `routedReplicasForKey(key)`. For a key's unit `gu = genUnitForKey(key)`:

  - **CURRENT replica set** = `unitReplicas(gu)` resolved over the ring EXCLUDING JOINING members (the `Joining`-bit sibling of the draining exclusion), draining members STILL INCLUDED, subject to the QUORUM FLOOR below. In steady state (no joining member) this is `ring.LocateKeyN(genUnitBytes(gu), R)` over every member - unchanged. These are the nodes that own the position TODAY and have it MOUNTED: on a LEAVE the leaver is here (draining stays a current owner); on a JOIN the DISPLACED, still-mounted old replica is here and the warming newcomer is EXCLUDED, so current is the pre-join placement (the copy that physically holds the bytes) rather than the newcomer's mid-acquire slot.
  - **THE QUORUM FLOOR (current never shrinks below R).** Excluding joining members from current is SAFE only while at least R non-joining holders remain. If excluding them would drop current below R (fewer than R non-joining members in the located chain - the MASS-BOOT case where 2+ of a unit's replicas are freshly-booted at once), current FALLS BACK to the full-ring set (joining members included) for that unit. This reverts that ONE unit to the pre-Joining-bit behavior: unavailable-but-SAFE (a routed op to a warming replica returns the mid-acquire transient, so the write WEDGES until a replica mounts), and critically `stableR = len(current) >= R` always holds for a live unit - so `requiredWriteAcks(WriteConsistency, stableR)` never drops below the normal bar and a write can NEVER ack below R durable copies. Without the floor a mass boot would compute current as empty, `stableR = 0`, `requiredWriteAcks(WriteAll, 0) = 0`, and a write would ACK with ZERO durable applies (a lost acked write). The leave path never needs this floor (a draining node stays a stable MOUNTED current owner, so `stableR = R` by construction); the JOIN path does, because the `Joining` bit fires automatically on every boot-with-deferred-positions (mass restarts included), where 2+ replicas can warm simultaneously.
  - **PENDING replica set** = `unitReplicas(gu)` resolved over a ring GENUINELY REBUILT WITHOUT the draining members (the same exact-reduced-ring construction the CURRENT set uses for the joining exclusion). These are the nodes that WILL own the position once the leaving node is gone (the successor that takes over the leaver's exact position is here). Joining members are NOT excluded from pending (a newcomer is a PENDING owner of the positions it is warming, so it acquires them via the pending-owner path and serving-marks them, exactly as a leave-successor does). **The successor-chain drop-trick approximation is REMOVED** (an earlier implementation computed pending as "locate R+|draining| over the full ring, drop the draining ids, keep the first R" and argued exactness was not required for the future set). That argument was REFUTED in practice: bounded-load consistent hashing is not removal-invariant, so the approximated set can DIVERGE from the true post-leave placement - and pending is the protocol's load-bearing prediction of that placement. Ordered removal drains the leaver against pending's successors and holds displaced owners against pending membership; when pending is wrong, the drain hands a unit to the WRONG successors, the true post-leave owners mount nothing until after the leaver exits, and a FULL-MOVE unit (approximated pending disjoint from the true placement) goes client-visibly unreadable for the whole post-exit acquire window while every physical holder is un-routed (the read hole `TestLeaveJoinOverlap_FullMoveUnit_ReadTransparent` pins). With pending computed exactly, pending == the post-transition placement in BOTH directions (a join's pending is the full ring; a leave's pending is the genuinely-rebuilt survivor ring; an overlapping join+leave composes), so the drain-time acquires + marker gates cover the post-exit placement BY CONSTRUCTION and the exit itself moves no ownership. The exact rebuild is only computed while draining members exist (a rare, brief transition), mirroring the joining-side reduced-ring cost profile.
  - **THE UNIFIED SPLIT (leave and join are one mechanism).** current = ring EXCLUDING joining (floored); pending = ring EXCLUDING draining. The EXISTING pending-ranges machinery then covers both directions with no new controller: a DISPLACED owner (a position in current-not-pending) rides the DRAIN half (`beginDrain` / `drainCheck`, stays serving until the successor serving-marks); a NEWCOMER (a position in pending-not-current) rides the pending-owner ACQUIRE half (`acquireReplicaUnitOverlap` + serving marker, a background mount, NOT the clean-cut synchronous acquire); routing returns current UNION pending. A LEAVE is the case where joining is empty (current = full ring, the leaver a stable current owner); a JOIN is the case where draining is empty (pending = full ring, the newcomer a pending owner). A simultaneous join+leave composes for free (current excludes joining, pending excludes draining, the union covers both).
  - **THE `Joining` BIT + ITS LIFECYCLE.** `Joining` is a gossiped per-member Meta bit mirroring `Draining` (its own NUL-delimited "J" segment, forward-compatible, read into `Member.Joining`, set/cleared via `membership.SetJoining`). A node SETS it on boot when `mountReplicaUnits` BOOT-DEFERS one or more owned positions (a peer's serving marker is present, so the node did not open the position and avoid fencing the live peer - "owned but not yet mounted"). A cold start (no markers anywhere) defers nothing and never sets the bit, so first-cluster convergence is unchanged. The node CLEARS it once it has fully warmed - every position in its PENDING (real-ownership) set is in `mountTable.mounts`. The reconcile loop maintains the clear (each tick: if this node is joining and all its pending positions are mounted, `SetJoining(false)`). While set, the newcomer is excluded from `current` (so a displaced peer stays the stable holder and the newcomer acquires via the pending-owner overlap path); once cleared it is an ordinary current owner and routing collapses to the steady state. **Boot-time timing of the bit and the warm-up (v0.14.2), each behind a safety gate; absent the gate condition everything falls back to the settle debounce byte-for-byte:** (a) `startJoining` pre-raises the bit at membership-open for every seeded R>1 node (before any mount), so peers exclude a warming node from CURRENT from its first gossip; a boot that then completes with ZERO deferred positions CLEARS the bit AT BOOT COMPLETION rather than a reconcile tick later - it is fully warmed by construction, and the stale bit otherwise makes a peer booting into it within the settle window read it as not-established. (b) A boot that DEFERRED positions arms its warm-up reconcile IMMEDIATELY (`scheduleReconcileIn(0)`) iff an ESTABLISHED (non-Joining) peer is visible (`peerJoiningCounts`, decision logged): in a mass boot every node advertises Joining from membership-open, so the gate preserves the debounce, which is load-bearing there (bypassing it lost acked writes 8 of 8 runs in the mass-boot gate). (c) On a membership JOIN, an established node promptly acquires positions that are desired, unmounted, and have NO SERVING MARKER (`promptAcquireFreshPositions`): a join mints positions nobody has ever served, uncontested by definition. Two independent gates - self-established only, and markerless only (in a mass RESTART every position is marked, so the pass is a no-op there by construction). The acquire is the background bounded path, dedup'd via `startAcquire`.
  - **TRANSITION DETECTION** (answers open question (b)): a position is IN TRANSITION when CURRENT != PENDING, which arises from exactly two gossip-observable causes. (1) a LEAVE: some member of CURRENT is `Draining`, so excluding it from PENDING changes the set (the draining bit is the linearization-free analogue of Cassandra/ScyllaDB's topology epoch). (2) a JOIN: some member is `Joining`, so excluding it from CURRENT changes the set - the still-mounted displaced owner is in CURRENT and the warming newcomer is in PENDING. In the join case CURRENT (pre-join owners, newcomer excluded) and PENDING (post-join owners, newcomer included) differ because the new member shifts the `LocateKeyN` successor chain. Both causes are read off the gossiped ring + the `Draining` / `Joining` bits + the serving marker - no central coordinator, no consensus epoch. (Under the quorum floor a join whose exclusion would drop current below R is NOT in transition for that unit: current falls back to the full ring == pending, the safe wedge.)
  - **ROUTED replica set** = the set `routedReplicasForKey` actually fans out to. When NOT in transition it is the stable replica set (CURRENT == PENDING, R nodes). When in transition it is the UNION(CURRENT, PENDING), which at R=2 spans up to 3 distinct nodes (the leaver + the two pending owners, or the joiner + the two current owners). The union is a SUPERSET of every node's possibly-disagreeing ownership opinion during gossip lag, so any single-node routing decision still reaches a node that physically holds the position - this is the property that fixes the post-leave readback failure of the superseded forwarding model.

**Dual-write with the ack bar held at the stable R (answers open question (a)).** The fan-out DUAL-WRITES the position to EVERY member of the ROUTED union (writes always go to all routed replicas, exactly as Cassandra sends every write to all replicas regardless of consistency level). The ACK BAR W is held at the STABLE quorum over the configured R - `W = requiredWriteAcks(WriteConsistency, R)` - and is NOT raised to cover the transient extra union member. At R=2 with `WriteConsistency=Quorum`, `W = floor(R/2)+1 = 2`: a write during the transition fans out to up to 3 union members and acks once ANY 2 of those 3 have durably applied it. The extra pending replica is a BONUS write target (it lowers latency-to-durability and pre-warms the successor with live writes during its mount) - it is NOT a higher ack bar. This is deliberately DIFFERENT from Cassandra's `blockFor`, which raises the required-ack count to cover pending endpoints: shale holds the bar at the stable R because raising it to ALL-3 transiently would make any one slow union node halve write availability on an R=2 store, defeating the point. The leaver counts as a normal routed replica whose ack is interchangeable with a pending owner's; because the leaver already has the data, it can satisfy W instantly while the pending owner is still mounting. `WriteConsistency=All` is held at the stable R TOO - `W = requiredWriteAcks(All, R) = R`, the number of STABLE replicas, NOT the transient union size. This is a deliberate departure from Cassandra's CL=ALL (which raises `blockFor` to include pending endpoints and is correspondingly unavailable during a topology change): widening All to the union would block every write on a mid-mount successor for the whole mount window, collapsing scale-down availability. The pending owners inherit the data through the shared object-storage db the moment they mount, so the durability All promises is preserved without requiring their in-flight ack. "All" means all STABLE replicas.

**CAS during a transition: ONE designated owner + the union write-set fan-out.** The CAS validate-and-apply is owner-local and single-writer by design, so it cannot "ride the union" the way a plain put does (two nodes validating the same pin unit concurrently would be a lost-update split brain). Instead the transition support has three parts, none of which weakens the single-owner model:

  - **Owner designation.** The owner-side validate-and-apply for a pin key runs on exactly ONE designated node: the FULL-ring head of the pin unit, UNLESS that head is `Joining`, in which case the head of the CURRENT set (the displaced, still-mounted owner). This is a deterministic pure function of (ring members, joining set); the client dispatch (`commitCAS`) and the server-side ownership gate (the CommitCAS `FailedPrecondition` re-pin refusal) evaluate the SAME function, so per converged view at most one node accepts commits and a mis-routed commit is refused and re-pinned, never applied. Under the QUORUM FLOOR (mass boot) the current set falls back to the full ring, so the designation falls back to the full-ring head (a warming newcomer whose owner-local begin refuses mid-acquire): the safe wedge, unchanged. SINGLE-OWNER SAFETY ACROSS VIEW SKEW (two peers transiently disagreeing about the `Joining` bit): the designated owner always serves the pin unit at the POSITION-0 durable database - the current head serves its current-index-0 mount, and the newcomer (which becomes the full-ring head, else no designation change happens at all) acquires pending-index 0, the SAME durable database - and one database has ONE fencing chain, so the newcomer's open fences the displaced head before the newcomer can serve. Two designated owners can never both commit un-fenced.
  - **Write-set fan-out over the union.** After the owner-local commit, `replicateCASBatch` fans the encoded write-set out over the SAME routed union `routedReplicasForKey` computes for a plain put (current + pending members), with the ack bar `requiredWriteAcks(WriteConsistency, stableR)` over `stableR = len(current)` - NOT over the raw full-ring set. During a join the displaced still-mounted owner is a union member whose leg can ack, so a moving unit's CAS commit reaches W via the displaced owner + the un-displaced co-replica while the newcomer is still mounting; the warming newcomer is a bonus apply target, exactly as it is for a put.
  - **Mounted-position resolve for the owner-local begin and the batch apply.** The owner-local transaction (`localBeginForKey` -> `localWriteBackendForKey`) and the replica-side batch apply (`applyBatchToUnit`) resolve the node's position for the unit by its ring index, FALLING BACK to the lowest MOUNTED position of that unit when the index resolve misses. The displaced current owner's position is no longer its full-ring index (the newcomer shifted the chain), but its mount is still keyed by the position it actually holds; the fallback lets it open/apply against that mount. Steady state never takes the fallback (the index resolve hits); every write through it is fence-guarded + apply-if-newer, so a stale fallback mount degrades to the transient recode, never a wrong ack.

**A FENCED write leg is a TRANSIENT non-ack, not a hard failure.** During the transition the successor opens the position (fencing the leaver's handle the instant it opens, before the successor is serving). A union dual-write that lands on the now-fenced leaver leg fails with the writer-epoch fence (`backend.ErrFenced`). That leg MUST be classified TRANSIENT - neither an ack nor a hard failure - exactly like a mid-mount pending owner's `errUnitAcquiring`: the fenced commit did NOT durably apply on that leg, so not counting it as an ack is correct, and treating it as transient (vs hard) makes the fan-out RETRY the write onto the re-resolved union rather than fast-failing it (the fan-out fast-fails only when too many HARD errors make the stable-R quorum unreachable). The successor that fenced the leaver serves the write once it finishes mounting, so the retry acks; the cost is added latency during the mount window, not a lost or rejected write. The recode happens at EVERY apply-op error site, not just Commit: the writer-epoch fence can surface at WHICHEVER op runs first against a now-stale writer (slatedb's background manifest-epoch poll lands it on the next op), so the apply sequence `Begin->Get->Put->Commit` can fence at any step - the in-memory test double fences at `Begin` (recoded by the Begin path), real slatedb at `Get`/`Put`/`Commit`. Each is recoded to `errUnitAcquiring` and the stale mount evicted, identically. Backends expose the fence backend-agnostically via `backend.ErrFenced` (each wraps its native fence at every op so `errors.Is` resolves it) since the cluster cannot import a specific backend. The recovery is IN-CALL only if the per-write budget (`WriteTimeout`, default 5s, operator-tunable via `--write-timeout` / `SHALE_WRITE_TIMEOUT`) exceeds the successor mount window; a budget shorter than the mount returns a client-retryable `codes.Unavailable` instead. This is the during-leave WRITE-availability seal at R=2/W=quorum: without it a single fenced leg hard-fails the write for the whole successor mount window; it does NOT change the inherent floor that a moving unit with only one live writer cannot meet a 2-ack quorum until the successor mounts (that needs R>=3 or W=1). The SAME recode covers the CAS owner-local commit path (see "A fenced CAS owner-local commit is a TRANSIENT retry, not a hard failure"), so a CAS write through a graceful leave is available too.

**Union reads.** A read fans out across the ROUTED union per `ReadConsistency`; the gather collects every leg's result and the consistency level sets how many USABLE answers it keeps for winner selection (`Nearest` 1, `Quorum` floor(R/2)+1, `All` every stable replica) - it is a keep-count, not an early return. ANY union member that physically has the position MOUNTED serves it; a union member that is a pending owner still mid-mount returns the transient acquiring code (`errUnitAcquiring` -> `codes.ResourceExhausted` on the replica leg - the recode applies to EVERY forwarded replica leg, reads included, because the acquiring sentinel does not survive gRPC and a bare `codes.Unavailable` would be miscounted as a down peer at the originator), which the read fan-out SKIPS while another union member (the leaver, which has the data) answers. So a read during a transition always finds the bytes on the still-mounted current owner, even though the ring opinion is mid-rotation. The LWW max-stamp winner selection, read-repair, and tombstone -> `ErrNotFound` are reused verbatim over the union. Two guards keep the union read honest at the edges of a transition: (1) MOUNTED-POSITION FALLBACK (the read sibling of the CAS "Mounted-position resolve" fallback above): a read leg addressed at a position `ru` the member has not mounted serves from the LOWEST mounted position of the SAME unit instead, because a ring change can shuffle a member's index within a unit's replica set (the member physically holds the unit's bytes at its OLD index while the routed set already addresses it at the NEW one). With the pending set computed exactly, the surviving triggers for that window are gossip/view skew between members mid-transition, a CRASH (non-graceful) leave that skips the drain protocol entirely, and the reconcile lag between a ring change and the same-unit re-mount completing. The fallback is READ-ONLY: a replica copy at another index of the same unit is just another replica of the same data, and no write can have acked past it while every routed position of the unit is unmounted; write legs never fall back (a write applied only to a soon-released old-index copy could be lost). (2) ALL-LEGS-TRANSIENT is RETRIED WITHIN THE READ BUDGET, then `Unavailable`, NOT not-found: when every union leg reports the transient acquiring window (mid-acquire refusal, fenced-recode, an UNREACHABLE member - a refused dial or transport failure against a just-departed member's address is "this copy is not here anymore" during a transition, so a read leg's `codes.Unavailable` is skipped within the sweep rather than counted as a hard failure (the write fan-out's down-peer accounting is unchanged), BUT unreachability alone is weaker evidence of a transition than the handoff classes, so it earns a TIME-BOUNDED re-poll (the UNREACHABLE GRACE, default 2s, always bounded by the read budget), not the full budget: a just-departed member's address LINGERS in the routed union until the membership update propagates and the reader's ring rebuilds, so during that ring-lag window unreachable-only sweeps are EXPECTED - a sweep-count cap shorter than the lag surfaces spurious dial errors mid-rollout (the observed read-canary 500s), while the grace re-polls through the lag and serves the moment the refreshed union has a live leg. Only when unreachable-only evidence persists past the grace is the FIRST dial error surfaced VERBATIM (its text is the diagnostic); any handoff-class evidence (acquiring, fenced, closed) resets the run and keeps the full-budget re-poll. A genuinely all-down replica set therefore surfaces its dial error at ~the grace (well under the budget) instead of stalling the read to the full ReadTimeout - or a CLOSED-MID-RELEASE mount - a leg that resolves a local handle in the same instant ordered removal's release closes it reads `backend: closed`, and a leaving node's own shutdown surfaces the same to its peers' forwarded legs; on a union leg that means "this copy just moved on", so it is classified transient exactly like the acquiring refusal, in both its in-process form and its wire form) and none answers, the read does NOT surface an error while budget remains - it RE-POLLS the union (re-resolving the routed set from the live ring each attempt, jittered exponential backoff off the same retry-after base the write path uses, capped at the same bound) until a leg serves or the `ReadTimeout` wall clock (default 5s) expires. This is the read-side mirror of the write path's `WriteTimeout`-bounded retry-through-handoff (Phase 2d Option A): a sub-second transient window - canonically the fence-at-open-START blip where a successor's open fences every still-mounted copy for the duration of the open - becomes bounded ADDED LATENCY instead of a client-visible error. The retry fires ONLY for the pure all-transient outcome: any hard leg failure (a decode error, an unexpected server-side failure) is surfaced immediately and unretried, so the retry never papers over a real outage by spinning; a not-found with no transient legs returns immediately; and the unreachable-only cap above bounds the outage case. Only when the budget expires with every attempt still all-transient does the read surface the retryable acquiring error - never `ErrNotFound` - so a key that exists is never reported absent just because its unit is mid-handoff everywhere. The closed-mount and unreachable-member reclassifications apply ONLY to union LEGS: a client op entering a genuinely closed cluster (`Cluster.Close`) still returns `backend.ErrClosed` immediately from the entry not-ready check, unretried. ENTRY-DURING-DRAIN is part of this contract: a gracefully-leaving node keeps serving ENTRY (client) traffic for the whole drain - `DrainForLeave` runs at the top of `Close` BEFORE the closed flip, and the not-ready check flips only at the actual close - so the only window a pod fast-fails `backend.ErrClosed` at entry is the post-drain teardown tail, which the ORCHESTRATOR must cover by keeping client routing away from the pod for the full leave duration (routing drain >= drain timeout + teardown), not by shale serving past its close.

**Union scans (`ScanPrefix` at R>1).** A single-shard scan is a READ and follows the same union contract, with read-one placement semantics (a scan has always been served by a single replica): `ScanPrefix` resolves the ROUTED union for the prefix's unit and walks it CURRENT-FIRST (one leg per member - the mounted-position fallback covers a member's index shuffle), serving the whole scan from the FIRST member that physically has the unit mounted. In steady state the first leg is the ring primary, so the behavior is exactly the pre-transition single-owner scan. A leg that is mid-acquire (the transient acquiring recode), unreachable (a just-killed leaver), fenced, or CLOSED mid-release (the `backend: closed` a leg reads when the handle it resolved is being released under it, in-process or across the wire) is SKIPPED and the next union leg serves; a walk in which NO leg can serve and every reachable leg was TRANSIENT is RE-POLLED within the `ReadTimeout` budget exactly as the union read's all-legs-transient retry (guard 2 above - same backoff, same budget, same fire-only-on-pure-transient rule), so the fence-window blip is bounded scan latency, not an error; only when the walk yields a hard error, or the budget expires still all-transient, does the scan fail (the first hard error, else the retryable acquiring error). The remote leg is POSITION-ADDRESSED on the wire (the `ScanPrefixRequest` carries the same `ReplicaUnitRef` the Get/Put/Delete forwards carry) and served with NO ring-ownership guard, exactly like `GetAtReplica` -> `LocalReplicaGetAt`: the receiver resolves its own mount (with the fallback) and answers from what it physically holds, so gossip-lag ring disagreement between originator and receiver cannot loop-guard-refuse a scan mid-transition. The remote leg's first message is PRIMED at open so a mid-acquire receiver is classified (and skipped) before the iterator is returned, not surfaced as a mid-iteration error. THE WALK IS DEADLINE-BOUNDED: every leg's open + prime runs under the SAME `ReadTimeout` deadline the re-poll uses, so a wedged-but-connected peer (handler stuck, transport alive) or a black-holed address can stall the walk only to the budget, never hang it. Once a leg is CHOSEN (its prime succeeded) its stream is DETACHED from that deadline for the drain: the budget bounds leg SELECTION, not stream consumption - a large scan legitimately outlives the read budget, exactly as the pre-union forwarded scan carried no stream deadline. Without this contract a scan is the single-owner-routed odd one out: it routes to the full-ring primary (which a JOIN hands to the still-warming newcomer and a LEAVE's ordered removal releases from the leaver position-by-position while the leaver is still the ring primary), turning every membership transition into a client-visible `Unavailable` window for scans even while Get/Put stay transparent.

**The pending owners acquire in the background (the normal reconcile, unchanged shape).** A node that is a PENDING owner of a position it has not mounted ACQUIREs it via the existing `reconcileReplicaUnits` -> `OpenReplicaUnit` path (mount the per-`(unit, replica)` database from shared storage at a strictly-higher epoch fence). This is the SAME initial-convergence mount machinery Phase 2b uses; pending ranges adds nothing new to the acquire side except that the position is now ALSO a write target via the union DURING the mount, so the successor receives live writes as it warms up. On mount-complete (the `mountTable.mounts[ru]` insert) the pending owner writes its SERVING MARKER (`WriteServingMarker(ru, E)`) to shared storage EXACTLY ONCE - the durable, poll-observable "I now physically hold and serve this position" record. **The publish belongs to the mount-table TRANSITION that installs the mount (`mountServing`), not to any mount path.** Boot, the clean-cut acquire, the overlap flip and the stuck-flip rearm all reach that one transition, so "mounted and serving locally but never marked" is not expressible and a future mount path inherits the publish rather than having to know about it - the omission it prevents has NO local symptom (the mount serves normally; only a predecessor elsewhere stays Draining forever). The publish carries the epoch the factory RETURNED from the open and runs with the mount installed and the table lock RELEASED: marker I/O must never run under the mount lock. The background acquires are BOUNDED node-wide by `Config.OpenConcurrency` (default 1 - strictly sequential, the proven-safe mode): each overlap-acquire goroutine takes a node-wide open permit around its `OpenReplicaUnit`, the SAME knob that sizes the boot mount pool, so every real-data open on a node respects one bound. Rationale: concurrent real-data FFI opens are a documented read-corruption trigger in the shipped storage binding, so unbounded parallel acquires during a join/leave would be exactly that trigger; and the bound costs no availability - a queued position simply stays `PhaseAcquiring` while the union keeps covering it via the still-mounted current owner. Raising the bound is a deliberate operator experiment via the knob, never an implicit behavior. **The bound must never tick-quantize the cycle**: queued acquires block on the permit itself, so when one open finishes the next queued open starts IMMEDIATELY (event-driven chaining, never a reconcile-tick wait); the serving marker is written inline in the acquire goroutine right after the mount flip (no tick between mount and marker); and a FAILED open re-drives itself in the acquire goroutine on a short jittered backoff (releasing the permit between attempts so a failing position never starves the queue), with the periodic reconcile as the bounded backstop once the backoff is exhausted. **The re-drive branches on the acquire's own ERROR RETURN**, never on a re-read of the shared `mountTable.acquireErr` diagnostic map: that map is keyed per position and written AND cleared by every mount site on the node (a successful mount clears the entry at the mount choke point; boot records a NON-error `boot-deferred:` note under the same key), so reading it back as the retry condition would let an unrelated path's write decide this loop's control flow - a concurrent mount of the position could end the retry of a still-failing open, and a boot-defer record could make a SUCCESSFUL open look failed. The outcome travels as a return value, private to the call; `mountTable.acquireErr` stays a WRITE-ONLY observability mirror (see "Mount readiness"). The per-position handoff cycle therefore costs ~the open itself plus at most a tick or two of edge-triggering across the WHOLE batch, and never accumulates ticks per position. **A slow or HUNG open must not starve the queue (the permit watchdog).** The storage binding's open (and its close/shutdown sibling the self-heal path may call) is an un-cancellable FFI call; without a watchdog, ONE open wedged against a degraded store holds the bound=1 permit indefinitely and every queued position starves - a total pipeline stall. After `Config.OpenPermitTimeout` (default 60s, ~2x a worst-case clean open) the acquire goroutine RELEASES THE PERMIT ONLY: queued positions proceed, while the stuck position's own open keeps running to completion (still the ONLY open for that position - `mountTable.inFlight` dedupes re-drives for as long as the goroutine lives, so the watchdog can never create a double-open of the same position; when the open eventually returns it takes the normal serving-mark or failure-re-drive path). The watchdog deliberately trades a bounded overlap risk for unstarving the queue: releasing the permit lets another position's open run while the stuck one is still in flight, which is the concurrent-real-data-open shape the OpenConcurrency=1 default exists to avoid - accepted here because (a) that failure mode manifests as a DETECTED, RETRIED open error (the binding refuses the open; the transient-open retry and the failure re-drive absorb it), never silently served corruption; (b) any zombie-vs-retry double-open of one position is serialized by the manifest epoch fence (the last opener lands strictly above and fences the earlier); and (c) the watchdog only fires in an already-degraded state where the alternative is indefinite full-queue starvation. A stuck position therefore degrades to ONE stuck position, not a stuck node.

**The serving-marker handoff gate (REUSED unchanged).** The serving marker is the durable, poll-observable signal that a pending owner has mounted. It is keyed by `ReplicaUnit`, carries the writer's open epoch E, and is written EXACTLY ONCE by a node right after it inserts its `mountTable.mounts[ru]` entry. `ReadServingMarker(ru) (epoch, ok)` reads it without opening the database. The release rule is unchanged from the prior design: a node releases / drops a position from its routed set only when it reads a marker at an epoch STRICTLY ABOVE (`>`) its own open epoch - positive proof that a live SUCCESSOR is actually serving. The strict `>` rejects a node's own stale gain-marker at exactly its open epoch. A bare durable FENCE-epoch advance (`DurableEpochReplica(ru)`, which bumps at open-START before the mount completes) NEVER triggers release - it is strictly weaker than the serving marker (a successor that fences then crashes mid-mount advanced the fence WITHOUT writing a marker, so the marker is the only positive "live owner serving" confirmation). The marker read is a POINT-IN-TIME liveness observation, not a lease: if the successor crashes in the gap between the leaver reading the marker and completing its removal, the position is unserved until the next reconcile, with NO acked-write loss (durable-before-ack).

**The open epoch is the EXACT epoch the node opened at, captured from `OpenReplicaUnit`'s return value (NOT re-read from the live durable).** Both "its own open epoch" (the leaver's release threshold) and "the writer's open epoch E" (the epoch the serving marker carries) MUST be the precise epoch the factory opened the position at, captured ONCE at open time and held immutable for the life of the mount. They MUST NOT be re-derived from `DurableEpochReplica(ru)`: the durable manifest writer-epoch is a SHARED, monotone counter that EVERY `OpenReplicaUnit` of that position by ANY node bumps by one, so a re-read returns a moving target whose value depends on when it is read. A leaver that captured its release threshold from the live durable would see it CLIMB as its own successor opened (the successor's open bumps the durable above the leaver's epoch), so the successor's marker (at the successor's open epoch == that same bumped durable) could never be strictly above the threshold and the drain would hang to its timeout - the graceful-scale-down availability gap. The fix: `ReplicaBackendFactory.OpenReplicaUnit(ru, intended)` RETURNS the opened epoch `(backend.Backend, Epoch, error)` (it already computes it as `max(intended, durableEpochReplica+1)`); the cluster records that returned epoch per `ReplicaUnit` at the mount (alongside `mountTable.mounts[ru]`, cleared on release). EVERY gate that compares a serving marker against "this node's open epoch" reads the recorded value, NOT `DurableEpochReplica`: the `drainCheck` release gate (`beginDrain` captures it into `HandoffState.OpenEpoch`), the `DrainForLeave` completion gate (`allOwnedPositionsHandedOff`, which is the gate whose hang IS the observed real-storage scale-down availability gap - it keeps the leaver blocked-and-serving until each owned position reports a successor marker strictly above the recorded epoch), the stuck-flip recovery marker, AND the serving marker itself are all written/compared at the recorded value. Both sides are then immutable factory-sourced integers, and the fence is STRICTLY MONOTONE (every `OpenReplicaUnit` lands at `max(intended, durable+1)`, strictly above the prior durable), so EVERY later open - the immediate successor, or any further successor in a re-acquire cascade - lands strictly above the leaver's recorded epoch and writes a marker strictly above it (under a cascade the surviving marker is the leaver's epoch +2 or +3; the conclusion holds a fortiori). The successor's marker is therefore permanently strictly above the leaver's threshold (liveness) while a bare durable advance still never releases (safety). `DurableEpochReplica(ru)` remains in use ONLY as the strictly-weaker fence-advance hint described above, never as the gate threshold or the marker epoch.

**ORDERED REMOVAL (the CEP-21 ordering, mapped onto pending ranges).** The transition advances in the canonical order that keeps every replica set covered: (1) ADD the pending owners to the routed set FIRST (the union forms the instant gossip observes the transition; pending owners receive every write while they mount). (2) the pending owners STREAM - here a near-zero-cost MOUNT from shared object storage, not a byte copy - and on mount-complete write their serving marker. (3) the leaver DROPS OUT of a position's routed set (the union collapses onto the PENDING set) ONLY ONCE that position's pending owner has a serving marker present at an epoch strictly above the leaver's open epoch (handoff durably complete). (4) the leaver releases the position (`CloseReplicaUnit(ru)`) after it is out of the routed set. The leaver is fenced out of writing only AFTER its successor is provably serving, so there is NO window where the only durable copy is on a fenced/closed node. `routedReplicasForKey` implements step 3 implicitly: once a position's serving marker is present at an epoch above the leaver's, the routing layer stops counting the leaver as a current owner for that position (it transitions that position from "in transition, union" to "pending set is now the stable set"), and the leaver's `drainCheck` releases its mount.

**Displacement flush (WAL-tail minimization at the Draining edge).** The moment a node observes it is the DISPLACED owner of a position (the `Owned -> Draining` edge, `beginDrain` - the same edge in BOTH transition directions: a leave drains the leaver's positions, a join drains the still-mounted owner a warming newcomer displaces), it fires a ONE-SHOT, BEST-EFFORT flush of that position's backend: force the in-memory write state (the memtable) durable to the backing store NOW, without closing the backend, so the successor's fencing `OpenReplicaUnit` recovers a MINIMAL WAL tail. Rationale (measured, `backends/slate/openbench_slatedb_test.go`): the fencing open's cost has a fixed protocol floor plus a data-dependent term that scales with the count of unflushed WAL objects (roughly four object-store round trips per WAL object); an owner carrying an unflushed tail multiplies the successor's open cost several-fold, while a memtable flush (tens of milliseconds, from a still-live owner) collapses it back to the clean-open floor. The release-time flush that already exists (`CloseReplicaUnit`'s flush-on-shutdown) cannot help here: under ordered removal the successor opens BEFORE the leaver closes, so only a flush at the DRAIN edge lands in time. Mechanics: the capability is the OPTIONAL `backend.Flusher` interface (`Flush() error`); a backend that does not implement it is skipped silently (the displacement flush is an optimization, never a requirement). The flush runs in a background goroutine (never under a cluster lock), fires EXACTLY ONCE per `Owned -> Draining` edge (`beginDrain` arms it only when it sets the phase; re-entrant drain ticks return before it; a reclaim + re-drain is a NEW edge and flushes again), and NEVER creates an error path: every failure is discarded, because the only failure modes are benign (the successor already opened and fenced this owner - the pre-flush behavior; or the backend is closing). Safety: a flush only makes the displaced owner's DURABLE state fresher (memtable contents that were already acked land in L0 instead of waiting in the WAL); it moves no fence, changes no epoch, and cannot lose or reorder an acked write, so every fencing/durability invariant above is untouched. If the successor's open wins the race, its recovery simply replays the WAL tail exactly as it would have without the flush. The same edge also arms a FAST DRAIN POLL: while any position is Draining, the displaced owner re-runs its release checks on a short cadence (an at-most-one background poller that exits when no Draining position remains, lifetime-capped so a STUCK drain degrades back to the tick cadence instead of polling the store forever) instead of only on the periodic reconcile tick, so the release (close + cleanup) follows the successor's serving marker within ~half a second. POLL-ONLY is preserved - this is a local cadence change, there is still no push RPC; the leaver's own `DrainForLeave` already fast-polls, this covers the JOIN direction's displaced owner.

**Transitional mounts are HELD; reclaim and release gate on PENDING membership (not CURRENT alone).** The reconcile must never tear down a mount that is mid-handoff, or the handoff oscillates and never completes (observed on real object storage as a self-draining leaver fighting its own drain to the `GracefulLeaveDrainTimeout`, with the drain-release gate visibly escalating tick over tick). One invariant governs both halves: **a mounted position that is in the PENDING set is held stable; only a position ABSENT from PENDING is drained (if still current) or released (if not).** It splits into two rules. (1) RECLAIM (aborting a `Draining` position back to `Owned` because the ring flip-flopped it back to this node) fires ONLY for a position that is BACK in the PENDING set - one this node will still own after the draining members leave. A position in CURRENT but NOT in PENDING is being HANDED OFF (this node is itself draining, or a draining successor split moved it out of pending); its drain must run to completion (`drainCheck` releases on the successor's marker), so it is NEVER reclaimed. Without this rule a gracefully-leaving node - which is still a ring member, hence still a CURRENT owner of all its positions - reclaims (un-drains) every position every reconcile tick and then re-drains it, re-running `beginDrain` and re-capturing a fresh (climbing) open epoch each pass, so the strict release gate never catches the successor's marker and the drain hangs to the timeout. (2) the not-current RELEASE (clean-cut dropping a mounted position no longer in CURRENT) does NOT release a position that is in the PENDING set: that is a SUCCESSOR's pending mount (acquired + serving-marked, serving via the union) which must be HELD until the draining member leaves and it becomes the stable owner (entering CURRENT, where the steady-state branch keeps it). Releasing it would tear down the very handoff target the leaver's `drainCheck` is polling, and the successor would churn release -> re-acquire -> re-mark at a climbing epoch. A position absent from BOTH current and pending is genuinely abandoned and takes the plain clean-cut release - UNLESS the INTRA-NODE POSITION-MOVE HOLD below applies. The SAME pending-membership gate governs the two sibling tear-down/grab paths that would otherwise re-open the oscillation: (3) the ACQUIRE half - since v0.14.2 the BACKGROUND BOUNDED acquire (`beginAcquire` -> `acquireReplicaUnitOverlap`: a `PhaseAcquiring` window under the open-permit pool, visible in DebugState; the synchronous clean-cut mount survives only in the break-demo) - (re-)mounts a CURRENT position ONLY if it is also in PENDING. After `drainCheck` RELEASES a drained position its phase is cleared to 0, but the leaver is still a ring member so the position is still CURRENT; without this gate the acquire half RE-GRABS the position it just handed off (re-fencing the successor at a climbed epoch), so the leaver's `ownedPositionCount` never reaches 0 and the leave never completes. (4) `evictStaleMount` (the write-path eviction that drops a mount whose handle failed, so the reconcile re-acquires a fresh one for the auto-recovery wedge) does NOT evict a position in a `Draining` (loser) phase: a fenced write on a draining mount is the EXPECTED successor-fence signal (the successor opened the shared db at a higher epoch), NOT a stale-handle desync. Evicting it would drop the leaver's mount before the successor's marker and let the acquire half re-grab it - the mutual-fencing ping-pong (leaver re-mounts at `durable+1`, re-fences the successor; the successor's next union write evicts + re-acquires + re-marks) that is the write-path driver of the hang under a continuous writer. The fenced write simply fails on the leaver's leg and retries onto the union (the successor's leg already acked it); `drainCheck` completes the drain on the successor's marker. (5) THE INTRA-NODE POSITION-MOVE HOLD: a mounted position absent from BOTH current and pending is NOT released while this node still desires the SAME UNIT at a DIFFERENT, not-yet-mounted position. A ring change (gossip/view skew mid-transition, a crash leave that skipped the drain protocol, a transition-bit flap) can shuffle this node's index within a unit's replica set with NO transition bit in flight; releasing the old-index copy in the same reconcile pass that starts the (slow) acquire of the new index destroys the node's only readable copy of the unit for the whole mount window - the post-leave read-availability hole. The old copy is HELD (still mounted, serving reads via the mounted-position fallback of "Union reads") until the same-unit acquire completes; the NEXT reconcile pass then sees no unmounted same-unit desire and releases it via the plain clean-cut branch. The hold is timer-free and self-releasing on this node's own mount progress; it holds only a handle (the durable copy lives in shared storage either way), and a successor of the OLD index that opens it simply fences the held handle, which recodes to the transient and evicts as usual. (6) THE MARKER-GATED ABANDONED RELEASE: a mounted position absent from BOTH current and pending, at a LIVE-generation position index the unit's live placement still uses, is NOT released until the position's serving marker sits STRICTLY ABOVE this node's open epoch - positive proof a successor has re-opened this exact per-position database and is serving it (the same strict-`>` release rule `drainCheck` applies to an explicit drain). This is defense-in-depth behind the exact pending set: transition-bit flaps can clear a drain phase and re-classify a still-load-bearing copy as abandoned, and destroying the last local copy before ANY successor serves the position converts a bounded acquire window into a data-unavailability window. The gate is best-effort shared-storage I/O on a rare branch: a marker-read ERROR holds (fail toward availability; the next pass re-checks), a marker at-or-below our epoch holds, a marker strictly above releases. THE HOLD FIRES ONLY WHEN A MARKER EXISTS: a position with NO marker AT ALL releases clean-cut - no live owner has ever served it, so no serving mount is in flight whose marker would release the hold; holding it would wedge forever against a signal that is not coming (the hold's premise - "the successor's serving mount publishes the releasing marker" - only holds once at least one marked serving mount has happened, which is exactly the flap-interleaving class the rule defends). The hold cannot outlive a real transition - the position index is in the unit's live placement, so its new owner's acquire writes the releasing marker - and it does not apply to an out-of-range index, a retired generation, OR WHILE A RESHARD IS IN FLIGHT (generation machinery owns retirement then; every future acquire happens at the NEW generation, so no old-generation marker is coming - holding a live-gen position mid-split wedged the stuck node's flip and broke split convergence, the v0.10.0 regression). THE HONEST BOUND: on an IDLE unit the hold persists - the successor's acquire is itself triggered by the reconcile, but if the successor never acquires (or its marker write fails), the held copy sits mounted until FIRST ACCESS: a read/write leg touching the held handle after a successor fences it recodes + evicts, and the next reconcile pass then releases or re-acquires normally. That is fail-toward-availability by construction: an idle unit holding a spare handle costs a mount slot, never correctness, and self-heals on traffic. All six rules are one invariant: a mounted position in PENDING is held; positions absent from PENDING are drained/released but never re-grabbed; a draining mount is never evicted out from under its own drain; a copy of a unit this node is still (re-)mounting elsewhere is never torn down first; and no last copy is destroyed before its successor provably serves.

**DrainForLeave completion.** A leaving node's `DrainForLeave(ctx)` (called at the top of `Close()` when `GracefulLeaveDrainTimeout > 0` and `multiReplicated()`): SETS THIS NODE DRAINING (`membership.SetDraining(true)` so the bit gossips and every node recomputes pending = current-minus-this-node, forming the union), then BLOCKS until EVERY position this node owns has a serving successor - i.e. until each owned position's `ReplicaUnit` has a serving marker at an epoch strictly above this node's open epoch and `drainCheck` has released it, so `ownedPositionCount() == 0` - OR `ctx` cancels / the timeout fires. The node STAYS A CURRENT OWNER and keeps serving its mounted positions (receiving union writes + reads) throughout the wait. ONLY AFTER the drain completes does the node do the REAL `membership.Leave()` + `Shutdown()` teardown. **The host run-loop MUST keep the gRPC transport SERVING for the whole drain: it runs `Close()` (which calls `DrainForLeave`) BEFORE it `GracefulStop()`s the gRPC server, never the reverse.** If the transport is stopped first, every union write a peer dual-writes to this draining node is connection-refused for the entire drain window (W=quorum unmet on its moving units, since the leaver's leg - the stable partner that lets a moving unit reach quorum without waiting on the successor mount - is unreachable); that is a host-ordering bug, not a cluster one, but it defeats the whole overlap design. Order: SET DRAINING (form the union) -> SERVE + WAIT (successors mount, write markers, the union collapses position-by-position) -> REAL LEAVE + SHUTDOWN. This REUSES `DrainForLeave`, the `Draining` Meta bit, and the serving marker unchanged; what changes is that the node stays a CURRENT OWNER and is dual-written via the union, instead of being excluded from the ring and forwarded back to.

**The in-memory maps are keyed by `ReplicaUnit`, NOT `GenUnit` (REUSED).** `mountTable.mounts` is `map[ReplicaUnit]backend.Backend`, the separate `replicaPos` map is folded into the key, and `mountTable.phases map[ReplicaUnit]HandoffState` is a `mountTable`-lock-guarded sibling. This is REQUIRED because a ring change can shuffle this node's index within a unit's replica list, so one node can hold the OLD position (draining, still serving union writes) AND the NEW position (acquiring/ready) of the SAME unit at once. The re-keying ripples through `localBackendForKey` / `localWriteBackendForKey` / `evictStaleMount` / `mountedBackends` and the reconcile diff. The `HandoffState` is SIMPLER than the forwarding design: it is `{Phase HandoffPhase, OpenEpoch Epoch}` with NO `Predecessor` / `PredecessorAddr` fields (no forwarding target to remember - the union routes directly to the leaver), and the phases collapse to `Acquiring` (pending owner mid-mount, served via the union by other members) on the gainer side and `Draining` -> `Releasing` on the loser side. There is no `Acquiring`-state forward behavior: a pending owner mid-mount simply returns `errUnitAcquiring` and the union covers the position via the still-mounted current owner.

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
  - P0-2 (per-`GenUnit` maps cannot represent overlapping positions): closed by RE-KEYING `mountTable.mounts` / phase map / `HandoffState` by `ReplicaUnit` (REUSED unchanged).
  - P1-1 (release on a bare durable-epoch advance): closed by the SERVING MARKER gate (REUSED unchanged) - removal requires a marker strictly above the leaver's epoch; a bare fence advance never removes.
  - P1-2 / NEW-P1-4 (consistent-mount WAL tail + intra-open fence<=recovery ordering): REUSED as the implementation-must-verify pin, simplified because writes are dual-written to the pending owner's own copy rather than forwarded.
  - P1-3 (release-check lock discipline): REUSED unchanged (I/O outside the lock; phase advance + `mountTable.mounts` CAS-delete are one `mountTable`-lock critical section).
  - P1-4 (crash-safety not always fully available): REUSED via the R/W matrix.
  - NEW-P0 / NEW-P1-1 / NEW-P1-2 / NEW-P1-3 (the forwarding-specific holes: position-addressed forward wire change, predecessor identification, single-hop scope, point-in-time marker gap): the FIRST THREE are MOOT under pending ranges (there is no forward, no predecessor to identify, no single-hop scope - the union routes directly), and they document machinery now REMOVED. The point-in-time-marker honesty fix (NEW-P1-3) is REUSED in guarantee 3(c).

**THE PENDING-RANGES AVAILABILITY GATE (the acceptance test).** A new in-process loss-oracle test (`tests/integration/overlap_handoff_test.go`, sharedfactory + memory backend, NO slatedb tag, NO MinIO) is the acceptance bar. It stands up a multi-backend R=2 cluster on a per-replica shared-backing factory whose `OpenReplicaUnit` can be made ARBITRARILY SLOW (a multi-second mount injection modeling a real MinIO mount exceeding `WriteTimeout`), writes a recorded BASELINE, then runs a CONTINUOUS WRITER through a 3 -> N membership change (AND a graceful one-node leave) while the slow mounts are in flight, and asserts BOTH (a) THE ORACLE - every baseline key and every ACKED probe key is readable with its exact value from every node, ZERO acked loss; AND (b) THE ACK RATE - acked / attempted is ~100% EVEN with the slow mount (where Option A alone drops to ~50-54% as the retry budget exhausts on the mount). It is kept honest by a BREAK DEMONSTRATION that disables BOTH the pending-ranges union (forces the clean-cut RELEASE-then-ACQUIRE / draining-exclusion, so the position routes only to the unmounted pending owner) AND the Option-A retry: the same slow-mount change drives the ack rate WELL below the threshold AND (the post-leave-readback failure the forwarding model could not fix) makes baseline keys unreadable from the post-transition routed set, proving the union is what holds BOTH availability and readback. The record-only-acked discipline keeps (a) and (b) independent.

**IN SCOPE: pending ranges for an R>1 (and R=1) multi-backend graceful membership transition** - during a transition the routed replica set is the UNION of current (draining-inclusive) and pending (draining-exclusive) owners; the fan-out DUAL-WRITES the union with the ack bar held at the stable R quorum; reads fan out across the union and any mounted member serves; pending owners acquire in the background via the normal reconcile and write a serving marker on mount-complete; ORDERED REMOVAL drops the leaver from a position's routed set (and only then fences/closes it) once that position's serving marker is present above the leaver's epoch; `DrainForLeave` completes when every owned position has a serving successor. Availability no longer depends on mount time. **OUT OF SCOPE (unchanged):** re-replicating a unit's BYTES onto a brand-new replica position with no durable copy (anti-entropy re-replication is a separate phase); resharding / doubling at R>1 (Phase 4 is R=1; SUPERSEDED by "v0.9 Decentralized reshard" below - R>1 resharding exists via the CAS arbiter); the legacy per-node path and the R=1 lease-handoff (Phase 3) are UNTOUCHED (Phase 2e lives behind `multiReplicated()`). **REMOVED (superseded):** per-position forwarding (`acquiringForwardTarget` / `forwardPutToPredecessor` / `forwardGetToPredecessor`), the position-addressed forwarded-op wire change, the `PredecessorAddr` / `Predecessor` fields, the `priorDesiredReplicas` / `priorAddrs` snapshot, the single-hop-scope fallback, and the draining-exclusion in `reconcileRingFromMembership`. Option A's `WriteTimeout`-bounded retry (Phase 2d) is RETAINED as the safety net for the residual cases (crash mid-handoff, pure-new-mount initial convergence); A and B compose.

#### Graceful leave (scale-down)

Scale-DOWN (a deliberate node REMOVAL) is the case pending ranges is built for. When a node is removed gracefully (a SIGTERM from the operator / orchestrator), it sets itself `Draining`; every node's `routedReplicasForKey` then computes the leaver's positions as CURRENT-but-not-PENDING (current = ring including the leaver, pending = ring excluding it), forms the routed UNION, and DUAL-WRITES the leaver + its pending successors. The leaver STAYS A CURRENT OWNER and keeps serving until each successor mounts and writes a serving marker, at which point ordered removal collapses the union onto the pending set and the leaver releases. The pending-ranges machine is therefore already the right mechanism for a graceful leave; the two pieces this subsection adds are (a) the `Draining` node-state that puts the leaver into the union WITHOUT removing it from ownership, and (b) the leaving node's `Close()` waiting for the drain (the marker gate on every owned position) to finish.

**The gap today (the availability bug this subsection closes), AND why the obvious fix is self-contradictory.** On SIGTERM the run loop calls `Cluster.Close()`. The naive fix - "broadcast `memberlist.Leave()` but keep the transport up so survivors re-own the positions while the leaving node keeps serving" - DOES NOT WORK, and that self-contradiction is the root cause of the measured ~58% in-process / ~70% staging drain availability. The leaving node is being asked to be BOTH gone (so survivors take over its ownership positions) AND alive (so it can keep serving during the drain), and a `memberlist.Leave()`-while-staying-alive cannot represent that. `memberlist.Leave()` broadcasts a "leaving" intent, but the node then KEEPS GOSSIPING AS ALIVE (the transport is up). Survivors receive the leave, briefly drop the node, then keep hearing it alive and RE-ADD it to their membership snapshot. Because ownership is derived from the live membership ring on EVERY node, the survivors that re-added the leaver compute it as STILL OWNING its positions and never start the handoff. `memberlist.Leave()` says "I am gone" while the transport says "I am alive"; membership cannot hold both, so the handoff never starts.

**The fix: a distinct DRAINING node-state - REACHABLE, a CURRENT OWNER, and the source of the PENDING split.** The node needs a state `memberlist.Leave()`-while-alive could not express: it stays a full, alive, addressable member AND a current owner of its positions (so routing keeps dual-writing it and it keeps serving), while its `Draining` bit makes every node compute the PENDING set (ring-minus-this-node) and route the UNION. This is advertised by a gossiped per-member `Draining` bit, NOT by leaving the cluster. The sequence:

  - The leaving node SETS ITSELF DRAINING by flipping a `Draining` flag in its memberlist `Meta` and calling `memberlist.UpdateNode` so the flag gossips out (the `Meta` already carries the node's gRPC dial address; the `Draining` bit rides alongside it). The node stays ALIVE and a current owner - it does NOT call `memberlist.Leave()` here, and it is NOT removed from the ownership ring.
  - Every node (including the draining node itself) DECODES the peer `Meta` on each gossip update and learns the `Draining` bit. `routedReplicasForKey` consumes it: for a position the leaver currently owns, CURRENT (ring including the leaver) != PENDING (ring excluding the leaver), so the position is IN TRANSITION and the routed set becomes UNION(current, pending). The leaver stays in CURRENT (mounted, serving), and the successor that takes the leaver's exact position is in PENDING.
  - Survivors that are PENDING owners of the leaver's positions ACQUIRE them in the background via the normal reconcile, receiving live dual-write traffic via the union as they mount, and write a serving marker on mount-complete. Routing reaches the leaver DIRECTLY (it is a routed union member); there is NO back-forward.
  - The leaving node BLOCKS until every one of its positions has a SERVING successor - i.e. until each owned position's serving marker is present at an epoch strictly above its open epoch and `drainCheck` has released the position (the SAME marker-gate release rule as any transition). Only AFTER the drain completes (or the grace timeout fires) does the node do the REAL `memberlist.Leave()` + `Shutdown()` teardown.

So the order is: SET DRAINING (form the union) -> SERVE + WAIT (successors mount, write markers, the union collapses onto the pending set position-by-position) -> REAL `Leave()` + `Shutdown()`. The drain happens entirely while the node is a normal alive current owner; the actual departure happens only once there is nothing left to drain.

**This SUPERSEDES the ring-freeze stopgap AND the draining-exclusion.** An earlier attempt called `memberlist.Leave()` at the START of the drain (the self-contradiction above) and then tried to paper over the resulting snapshot collapse with a `leaving atomic.Bool` flag; a later draft replaced that with draining-EXCLUSION (drop the draining node from the ownership ring + forward back to it per position). The pending-ranges model removes the need for ALL of it: the node stays ALIVE and a CURRENT OWNER, so its snapshot does not collapse, `reconcileRingFromMembership` works normally and does NOT exclude draining members (the include/exclude split is computed per-op in `routedReplicasForKey`, not baked into the ring), and there is no frozen ring and no per-position back-forward to maintain. The `leaving` flag + its early-return guard, the remove-self-only step, the draining-exclusion in `reconcileRingFromMembership`, and the entire per-position forward path (`acquiringForwardTarget` + the predecessor snapshot) are all REMOVED, replaced by the draining-`Meta`-driven UNION routing in `routedReplicasForKey`.

**Two memberlist verbs, split (Leave vs Shutdown), used ONLY at the END.** memberlist exposes `Leave()` (broadcast the graceful departure so peers record a clean leave and stop re-adding the node) DISTINCT from `Shutdown()` (tear down the local transport). The membership wrapper's `Close()` today does BOTH at once. They are still split: the REAL `Leave()` is deferred to AFTER the drain, not used to START it. Starting the drain with `Leave()` is exactly the self-contradiction above. The drain is started by the `Draining` `Meta` bit (the node stays fully alive, a current owner). Only once the drain is done does the node call the real `Leave()` (now correct: the node really is going away) followed by `Shutdown()` of the transport in the existing teardown.

**The drain entry point.** A `Cluster` method `DrainForLeave(ctx)` (equivalently `GracefulLeave(timeout)`): SET THIS NODE DRAINING via the membership `Meta` flag (so every node computes the pending split and routes the union, while this node stays alive, a current owner, and serving), then BLOCK until no position this node owns still lacks a serving successor (`ownedPositionCount() == 0`: every owned position's `drainCheck` released on its successor's serving marker) OR `ctx` is cancelled / the timeout fires. The reconcile loop, the serving path, and `drainCheck` all stay running during this wait. AFTER the wait returns (drained or timed out), the existing teardown - the real membership `Leave()` then `Shutdown()` - runs. The shutdown path calls `DrainForLeave` at the TOP of `Close()` - gated on config, BEFORE any teardown, while the loops are still running - then proceeds with the existing teardown (now including the real `Leave()`) unchanged. The SIGTERM handler in the run loop just calls `Close()` as it does today, so no run-loop change is required.

**Config gate.** A `Cluster` config field `GracefulLeaveDrainTimeout time.Duration` controls it. `0` = DISABLED = exactly today's behavior (`Close()` does not drain; the gap remains - this is also the break-demo state for the acceptance gate). When `> 0` AND the cluster is multi-backend replicated (`multiReplicated()`), `Close()` calls `DrainForLeave(GracefulLeaveDrainTimeout)` first. The R=1 lease-handoff path and the legacy per-node path are UNTOUCHED (the field is a no-op outside multi-backend replication, consistent with the rest of Phase 2e living behind `multiReplicated()`).

**What drains vs the residual.** A graceful leave maximizes coverage because a position the leaving node owns is, by definition, taken over by a SURVIVING node that becomes a PENDING owner - so the union forms and the leaver drains on the successor's marker. In the common case EVERY position the leaving node serves is covered by the union + the drain wait. The residual (NOT covered, degrading to a gap or to the old behavior):

  - A position where the leaving node simply DROPS OUT of a unit's replica set and some OTHER already-mounted replica covers W with no new node taking the leaver's EXACT position: PENDING already contains R mounted owners without the leaver, so the union adds nothing to acquire and there is nothing to drain - the eager release is correct and the position stays available via the surviving replicas. This is NOT a gap; it is simply outside the drain machinery.
  - A position whose PENDING successor is STUCK (its mount never completes within the grace budget): the drain wait times out, `Close()` proceeds, and that one position is UNSERVED from teardown until the successor finally mounts - exactly today's gap, for that position only, bounded by the operator's grace budget rather than unbounded.

**The invariant.** For a position with a REACHABLE pending successor, a graceful leave has NO unserved window: the leaving node stays a routed current owner and serves the position (dual-written via the union) until the successor is serving (marker present) and `drainCheck` releases it, and only then does the leaving node close. A continuous writer through a graceful one-node leave therefore sees ~100% ack and zero unserved window (vs the gap today). The residual: a position whose successor is STUCK degrades to the old gap AFTER the timeout; a position the surviving replicas already cover is not in the drain (it does not need to be). No-acked-write-lost and single-writer-fence are unchanged (the leaving node never leaves the routed set for a position before its successor is provably serving).

**Operator concern (orchestrator grace period).** The orchestrator's termination grace period MUST exceed `GracefulLeaveDrainTimeout`, or the orchestrator will SIGKILL the process mid-drain and reopen the very gap the drain closes. For a Kubernetes StatefulSet this is `terminationGracePeriodSeconds`, which the operator sets STRICTLY GREATER than `GracefulLeaveDrainTimeout` (with headroom for the leave broadcast to gossip out and for the post-drain teardown to finish). This is an operator-side configuration coupling, surfaced here so it is not forgotten at deploy time; the code only enforces its own timeout, not the orchestrator's.

**Acceptance (the break-demo).** The graceful-leave drain is demonstrated by a continuous writer through a graceful one-node leave asserting ~100% ack and zero unserved window, paired with a BREAK DEMONSTRATION that sets `GracefulLeaveDrainTimeout = 0` (drain disabled): the same leave then shows the gap (acked / attempted drops while the leaving node's just-closed positions are unserved until the survivors mount), proving the drain wait is what holds availability rather than rubber-stamping it. This reuses the slow-`OpenReplicaUnit` injection from the overlap availability gate, so the leaving node's positions take a measurable, mount-time-dominated interval to hand off.

### v0.8 Phase 2f: degraded boot (a node survives an un-openable replica)

Phase 2b's Open-time mount (`mountReplicaUnits`) opens every replica position this node owns SEQUENTIALLY and ABORTS the whole `Open` on the FIRST open error (`closeMountedUnits()` + `return err`). That makes a single un-openable replica backing store FATAL TO THE WHOLE NODE: `Open` fails, the process exits, and under a Kubernetes StatefulSet / Recreate it crash-loops forever. The blast radius is grotesquely out of proportion to the fault: a node holds a replica of MANY units, all healthy, plus its share of every other unit's traffic, yet ONE corrupt replica takes the entire node - and with it the node's serving capacity AND the redundancy of every unit it replicates - permanently offline. This is the opposite of what R>1 is for: a single damaged replica is exactly the fault R>1 exists to absorb.

**How a single replica becomes permanently un-openable.** The backing store of one `(unit, replica)` database in shared storage is damaged in a way the open path cannot get past: e.g. a referenced SSTable object is truncated to a near-zero length (slatedb raises `empty SSTable` when an object it must read is `<= 10` bytes), or the open stalls on degraded object-store I/O past `SLATE_DB_OPEN_TIMEOUT` (`buildDb`'s `runWithTimeout` returns an error, not a hang). Crucially this is INVISIBLE while the owning node stays up: slatedb's in-memory block cache serves the already-open database from RAM and never re-reads the damaged object; only a cache-cold REOPEN (a restart, or a peer acquiring the position) surfaces the damage. So the fault is latent and tends to fire at the worst time - on a restart - turning a routine roll into a brick.

**The fix: boot DEGRADED.** `mountReplicaUnits` SKIPS a replica position it cannot open - it records the error in `mountTable.acquireErr`, logs it, and CONTINUES to the next position - instead of aborting `Open`. This is byte-for-byte the skip-record-retry contract the reconcile-time `acquireReplicaUnit` ALREADY follows for a desired-but-unmountable position; degraded boot simply makes the Open-time mount obey the SAME contract the steady-state mount already does. `Open` then SUCCEEDS with the healthy positions mounted, the node becomes Ready, and it serves. A skipped position is DESIRED-BUT-UNMOUNTED: it shows in `/debug/shale/state` (the existing wedge flag) carrying its `mountTable.acquireErr`, and the periodic reconcile retries the open every tick - so a TRANSIENT open failure (an object-store blip at boot) SELF-HEALS the moment the store recovers, while a PERMANENTLY-corrupt backing store stays skipped (and observable) until repaired.

**Fencing safety: never OPEN a position a live peer is already serving (the load-bearing refinement).** The skip must trigger BEFORE the open is attempted, not after it fails - because the open ITSELF fences the peer. slatedb bumps the durable writer-epoch INSIDE `DbBuilder.Build()` (atomically, as part of opening), so even an open that ultimately ERRORS has ALREADY fenced any peer holding that position: the peer's live db then fails every op with "detected newer DB client" (`backend.ErrFenced`). A naive "attempt the open, skip on error" therefore does the WORST thing on a re-bootstrapping node - a seed pod that restarts with empty/unreachable seeds computes a 1-node ring (it desires EVERY position), attempts to open all of them, and fences EVERY peer that was serving, converting a one-node restart into a cluster-wide write outage even though each open then fails and gets skipped. So BEFORE opening a position, `mountReplicaUnits` reads its durable SERVING MARKER (`ReadServingMarker(ru)` - a tiny sibling object, NOT a slatedb open, so the read is fence-free): if a marker is present (some owner reached Ready serving this position), the boot mount SKIPS the position WITHOUT opening it (recorded desired-but-unmounted, same as an open-failure skip), so it never fences the live peer. The node boots owning only the positions NO ONE is serving - a genuine cold start has no markers, so it still mounts everything; for the rest it defers to the steady-state reconcile, which (once the ring CONVERGES) performs the proper ownership handoff: the overlap path for a draining predecessor, or a clean acquire of a position whose marker is stale because its owner is genuinely gone. The skip is BOOT-ONLY: the reconcile-time acquire does NOT consult the marker (it is the legitimate ownership-change path that SHOULD take over), so a true sole-survivor still re-acquires a stale-marked position once converged. This is the distinction between "I am the assigned owner taking over" (reconcile, fences correctly) and "I just restarted and do not yet know who owns what" (boot, must not fence anyone already serving). The marker read is one extra fence-free object GET per desired position at boot - negligible, and it only gates the boot mount.

**Hang safety (a slow replica cannot wedge the sequential mount).** The open is already bounded: `buildDb` runs `DbBuilder.Build` under `runWithTimeout(SLATE_DB_OPEN_TIMEOUT)` and returns an ERROR when a stalled or pathologically-slow open exceeds the bound (the un-cancellable uniffi goroutine is left to be reaped on a later close; it is not a process exit). So a position whose open HANGS - e.g. a slatedb read looping on a truncated object - is converted to a skip after the timeout, exactly like an immediate open error. The boot-fence's PRIOR durable-epoch read (`durableEpoch` / `durableEpochReplica`'s `Admin.ReadManifest`, run BEFORE the DB open to compute the fence floor) is bounded by the SAME `runWithTimeout(openTimeout())` (via `readDurableEpochBounded`, store built inside the goroutine so a timeout never Destroys a store the abandoned admin call still uses). Without that bound a slow/hung manifest read on a BLOATED unit (many un-GC'd objects on a tight object store) wedged the boot path FOREVER even though the DB open itself was bounded - the cold-start mount-hang. With BOTH phases bounded, a pathologically-slow position is skipped after the timeout (degraded, reconcile retries), never a hang. The sequential mount loop therefore can be slowed by a bad position (by at most two timeouts: the fence read + the open) but never WEDGED by one.

**What a skipped position costs, precisely - and why it is SAFE.**

  - **Reads to the degraded unit stay available** as long as >= 1 of its replicas is mounted somewhere. The local unmounted position returns the transient `errUnitAcquiring` from `dispatchReplicaGetUnit`, which the read fan-out SKIPS while another replica satisfies `ReadConsistency`. At R=2 with one position skipped, the unit is served READ-ONLY by its one surviving replica.
  - **Writes to the degraded unit BLOCK (bounded), never lose, never under-replicate-and-ack.** A write needs `W = requiredWriteAcks` acks; the local unmounted position returns `errUnitAcquiring`, which (Phase 2d) counts toward NEITHER the ack nor the failure budget. At R=2 `WriteQuorum`, `W = 2`: with only one mountable replica, W is unreachable, so a write to the degraded unit RETRIES under `WriteTimeout` and then surfaces the retryable unavailable outcome. This is the CORRECT conservative behavior, not a regression: silently dropping to `W = 1` for a degraded unit would let an acked write live on a SINGLE replica, violating NO ACKED WRITE LOST the instant that replica also failed. So writes to a degraded unit are unavailable until the position is restored - bounded latency then a clean unavailable error, never a lost or single-copy-acked write. Writes to every OTHER (healthy) unit on the node are fully available.
  - **No durable bytes are touched.** Skipping a mount neither loses nor mutates the position's database; it only declines to SERVE that one position locally. The damage stays exactly as it was, available for repair.

So degraded boot shrinks the blast radius of one corrupt replica from "the whole node + the redundancy of every unit it replicates, offline indefinitely" to "ONE unit, read-only and write-blocked, until that one replica is repaired" - while every healthy unit on the node keeps full read+write availability.

**Durability is untouched; the lossless gate still holds.** Degraded boot changes only AVAILABILITY (which positions a node serves locally), never durability. A skipped position is indistinguishable, to the data-loss oracle, from a position not yet mounted during the initial-convergence window the Phase 2b gate already exercises: no acked write can depend on a skipped position being mounted, because an ack required W replicas and a write to a degraded unit that cannot reach W is never acked. The single-writer fence is likewise untouched (a position that is not mounted has no local writer to fence).

**Acceptance (the break-demo).** A new in-process gate stands up an R=2 multi-backend cluster on a factory that can be told to FAIL the open of ONE specific `(unit, replica)` position (a poisoned-backing injection), then boots a node owning that position. It asserts: (1) `Open` SUCCEEDS and the node becomes Ready (does NOT crash); (2) every HEALTHY unit on that node serves reads AND writes; (3) reads to the DEGRADED unit succeed (served by the surviving replica); (4) the degraded position is reported desired-but-unmounted with its open error in the debug state, AND a degraded-boot line is logged (the log falls back to stderr when no sink is wired, so a degraded boot is never silent even in a default deployment - it must not merely trade a crash-loop for a silently half-functional node); (5) once the injection is cleared, the periodic reconcile mounts the position and the unit returns to full R=2 (reads + writes). It is kept honest by a BREAK DEMONSTRATION: with the degraded-boot skip removed (the pre-fix `return err`), the same poisoned position makes `Open` FAIL (the node bricks), proving the skip is what holds the node up rather than rubber-stamping it.

**OUT OF SCOPE here (the self-heal phase).** Degraded boot keeps the NODE and the CLUSTER serving and isolates the damage to one unit's redundancy + write-availability; it does NOT repair the corrupt replica. RESTORING the degraded unit to R copies - re-replicating a fresh copy of the position from a surviving replica so writes regain W and the unit regains its redundancy - is anti-entropy re-replication, the distinct phase Phase 2b already lists as out of scope. The repair is destructive (the corrupt backing store must be replaced, not opened) and its trigger policy (operator-initiated vs automatic, and the guard that the source replica is healthy before the corrupt one is discarded) is a deliberate design decision deferred to that phase; degraded boot is the prerequisite that makes the node survivable enough to host the repair.

### v0.8 Phase 2g: bounded-concurrency boot mount (fast cold start)

**The problem.** Cold-start latency of `mountReplicaUnits` is the node's `desiredReplicaUnits` count times the per-open latency. A slatedb open is an object-store round trip (durable manifest probe + WAL replay) that, against a slow or contended store, can take seconds; a node owning MANY positions therefore has a mount whose wall-clock is the SUM of those latencies. Opening one position at a time makes that sum the node's time-to-Ready, which can overrun the readiness window - and because every node cold-starts at once during a mass restart (a rollout, a node drain, a control-plane bounce), a sum-of-latencies mount turns an otherwise-safe all-pods restart into a slow, probe-overrunning cascade. The mass restart is not the hazard; the sum-of-latencies cold start is.

**The mechanism: a bounded worker pool (OPT-IN; default OFF).** The boot mount CAN open the node's owned positions through a bounded worker pool rather than one at a time, collapsing the wall-clock from `sum(latency)` to roughly `ceil(n / concurrency)` opens. The pool size is `Config.OpenConcurrency`; zero is normalized to `defaultOpenConcurrency`. A pool of size 1 is exactly the sequential mount.

**STATUS: concurrency > 1 is DISABLED BY DEFAULT (`defaultOpenConcurrency` = 1) because it is UNSAFE with the slatedb-go binding we ship.** Opening MULTIPLE units CONCURRENTLY, when those units have REAL durable data to read (WAL replay / SST reads), corrupts the reads and slatedb reports `empty SSTable` (a too-small SST/WAL object). This is INDEPENDENT of object-store CPU: it reproduced on prod (2026-06-19) against an UNCAPPED, un-throttled store, on 15 of 16 real-data units, at concurrency 4, even with a per-open retry - while a SEQUENTIAL mount of the SAME units succeeds every time. The trigger is the concurrent FFI opens themselves (a concurrency hazard in slatedb-go's uniffi `DbBuilder.Build` across distinct DBs, or in the distributed store's handling of the concurrent read burst), NOT CPU saturation (an earlier theory that mistook a throttle CORRELATION for the cause). EMPTY/fresh units do NOT reproduce it (no real objects to read), which is why a fresh-data staging cluster passed twice while prod failed - the lesson: concurrency-of-real-data changes MUST be validated against PROD-SHAPED DATA, not an empty staging cluster. The worker pool + the transient retry stay in the code, gated behind `Config.OpenConcurrency > 1`, for if/when slatedb-go is made concurrency-safe; until then the mount is sequential and a deployment MUST NOT raise OpenConcurrency without re-validating on real data. See `infra/postmortems/2026-06-18-hostthis-concurrent-mount-empty-sstable.md`.

**The transient-open retry (kept, harmless at any concurrency).** The slate backend RETRIES a RETURNED transient open error (`empty SSTable`) a bounded number of times with backoff before giving up to degraded boot. It only retries a RETURNED error, never a timed-out (still-running) open - retrying a stalled open would race the live un-cancellable open against the writer-epoch fence. At the sequential default it costs nothing (sequential opens of real data do not truncate); it is a cheap safety net for any genuinely-transient store blip and is the right primitive to keep for a future concurrency-safe binding. A genuinely corrupt object still exhausts the retries and falls through to degraded boot, unchanged.

**Why concurrency is safe here (no new fencing, no new races).** Each `(unit, replica)` position is an INDEPENDENT durable database (its own dbName), and the single-writer fence is PER-dbName, so opening DISTINCT positions concurrently never fences one another - the factory's per-position latch still serializes any two opens of the SAME position, while distinct positions proceed in parallel. Every per-position rule of degraded boot is UNCHANGED and simply runs inside a worker: the fence-free `ReadServingMarker` check still gates each open (so a concurrent boot still never fences a live peer), an open error or `SLATE_DB_OPEN_TIMEOUT` still SKIPS that one position (degraded, desired-but-unmounted, reconcile retries), and a success still mounts it. The shared bookkeeping is concurrency-safe: the mount map stays `mountTable`-lock-guarded, `mountTable.acquireErr` / `mountTable.openEpochs` are already `sync.Map`, and the mounted / skipped / deferred tallies are atomic. The set of positions that end up mounted vs skipped vs deferred is IDENTICAL to the serial mount; only the wall-clock differs. A pool of size 1 degenerates to exactly the prior sequential mount, so the change is a strict generalization.

**Acceptance.** An in-process gate boots a node owning many positions on a factory whose open BLOCKS on a barrier, and asserts that opens are IN FLIGHT CONCURRENTLY up to the configured limit (not one-at-a-time) and never beyond it, while a parallel assertion re-runs the degraded-boot break-demo under concurrency > 1 to confirm a single poisoned position is still skipped (node still Ready, healthy units still serve) exactly as in the serial case.

### Mount readiness (programmatic mount-state surface + Ready predicate)

Degraded boot (Phase 2f) deliberately lets `Open` SUCCEED with positions unmounted: the node comes up, serves its healthy positions, and the reconcile retries the rest. That is the right per-node availability call - and it creates a NEW hazard one level up: an embedding application that equates "process is up" with "node is serving its share" reports itself healthy while ZERO of its desired positions are mounted (every open failed, or every position was boot-deferred). Under a surge rollout, an orchestrator that trusts that health signal replaces every old pod with new pods that mount NOTHING, and the rollout "completes" into a cluster that cannot serve - a single mis-set backend credential is enough. The facts needed to stop this were already collected per node - `/debug/shale/state` dumps desired/pending/mounted/handoff-phase per position plus the last swallowed acquire error - but only as a human-readable debug string behind `SHALE_DEBUG_ADDR`. This section promotes those facts to a first-class programmatic surface on `Cluster` so the embedding application can gate its own readiness on actual mount state.

**The surface (pkg/cluster).**

```go
// MountReadiness is a point-in-time summary of this node's mount state,
// counted over the DESIRED set (the replica positions this node owns).
type MountReadiness struct {
    DesiredUnits     int    // replica positions this node owns (full ring)
    MountedUnits     int    // desired positions currently mounted (serving locally)
    PendingUnits     int    // desired positions not yet mounted (== Desired - Mounted)
    FailedOpenUnits  int    // pending positions whose last acquire attempt recorded an error
    LastAcquireError string // one representative recorded acquire error; "" when none
}

func (c *Cluster) MountReadiness() MountReadiness
func (r MountReadiness) Ready(minMountedFraction float64) bool
func (c *Cluster) Ready(minMountedFraction float64) bool // c.MountReadiness().Ready(f)
```

**Count semantics.** Every count is taken over the node's CURRENT desired set - `desiredReplicaUnits()`, the full-ring ownership set, the SAME set the debug dump's desired-but-unmounted wedge flag keys on (a warming joiner's owned-but-unmounted positions correctly count as pending, exactly as they correctly show the wedge flag).

  - `MountedUnits` counts desired positions present in the mount map. A mounted position the node NO LONGER desires (a loser mid-drain still serving for its successor) counts NOWHERE: readiness is about what the node OWES, not what it happens to still hold.
  - `PendingUnits` is exactly `DesiredUnits - MountedUnits`: desired positions not yet mounted, whether the acquire is quietly in flight, boot-deferred, or failing. The invariant `Mounted + Pending == Desired` always holds.
  - `FailedOpenUnits` is the subset of pending positions whose most recent acquire attempt recorded an error in `mountTable.acquireErr` (a WRITE-ONLY diagnostic map: the mount paths write it, only the reporting surfaces - this readout and `/debug/shale/state` - read it, and no control flow branches on it): an open that returned an error (degraded boot, reconcile-time acquire failure), or a boot-deferral that declined to open because a peer holds the serving marker (recorded for the same reason - the position is unmounted and the record says why). A successful mount DELETES the record, so the count never carries a stale error for a mounted position; records for positions no longer desired are ignored.
  - `LastAcquireError` is the recorded error of the first failed position in position order (deterministic across calls at unchanged state); `""` when `FailedOpenUnits == 0`. One representative message for a probe-period diagnostic, not a join of all of them - the full per-position detail stays on `/debug/shale/state`.

**The predicate.** `Ready(minMountedFraction)` returns `MountedUnits >= ceil(f * DesiredUnits)` where `f` is `minMountedFraction` clamped to `[0, 1]`. Edges, pinned deliberately:

  - `DesiredUnits == 0` -> READY at every fraction. A node with no assigned positions - mid-join before ownership lands, a non-storage role, or legacy single-backend mode (no per-unit mounts; the one backend either opened at `Open` or `Open` failed) - has nothing to mount, and "vacuously ready" is the answer that does not wedge its rollout.
  - `f <= 0` clamps to 0 (floor of 0 positions: always ready - no floor requested); `f >= 1` clamps to 1 (every desired position must be mounted); NaN clamps to 1 (the conservative end - garbage input must not accidentally disable the gate).
  - A node still inside its initial reconcile window is NOT special-cased: the predicate reports mount state AS IT IS, even one tick after boot. Boot grace is the consumer's concern (its probe's initial delay / failure threshold), not shale's - a special case here would re-open exactly the blind spot this surface closes.

**Intended consumer pattern (generic).** An embedding application wires its READINESS probe to the predicate - the readiness endpoint returns healthy iff `cluster.Ready(minFraction)` (fraction chosen by the operator: `1.0` strict, lower tolerates a degraded minority) - so an orchestrator gates rollout progress on actual mount state and a config error that makes every open fail stalls the FIRST replacement pod instead of replacing the whole fleet. LIVENESS stays separate: mount state must NOT restart the process (a restart cannot repair an unmountable backing store, and the process staying up is what lets the reconcile keep retrying while the healthy positions keep serving).

**On the wire.** The same counts ride the existing `Stats` RPC as additive fields (`desired_units`, `mounted_units`, `pending_units`, `failed_open_units`, `last_acquire_error`), printed by `shale stats`, so an operator can read any node's mount state remotely without wiring `SHALE_DEBUG_ADDR`. The RPC reports; it does not gate: the readiness decision stays in-process in the embedding application (a probe must not depend on a second network hop).

**Cost.** One desired-set enumeration plus one mount-map pass under the read lock: O(desired positions) per call, no formatting, no allocation beyond the desired-set slice the enumeration already builds - cheap enough to sit behind a probe polled every few seconds.

### v0.8 Phase 3: lease-handoff rebalance

Phase 3 makes a multi-backend node ACT on membership changes: when the ring re-assigns units, a unit whose owner changed hands off COPY-FREE. The old owner closes it (flush + release the lease); the new owner opens it at a higher epoch (fencing the old). Bytes never move (they live in shared object storage); only the writer lease moves. This is the data-loss-sensitive phase, built to a NO-ACKED-WRITE-LOST invariant. Multi-backend mode only: the legacy per-node path and its v0.3 Coordinator rebalance are UNTOUCHED.

- **Reconcile on membership change (anti-entropy).** Hook the existing membership path: `bumpRingGen` -> `scheduleEvaluate`. In multi mode (`c.multi`) scheduleEvaluate runs the unit reconcile instead of the v0.3 Coordinator (which is nil / short-circuited in multi mode per Phase 2). The reconcile is desired-vs-mounted: desired = the cluster's ring-backed `desiredGenUnits()` against the CURRENT ring; mounted = the mountTable.mounts keys (`factory.OpenUnits()`). For each unit desired-but-not-mounted: ACQUIRE (`OpenUnit` at the next epoch). For each mounted-but-not-desired: RELEASE (`CloseUnit`). It is idempotent and self-healing (a node that should own U but lost its mount re-acquires it), safe to run on every membership event. All mountTable.mounts mutations are `mountTable`-lock-guarded and the reconcile is serialized (one at a time) so two membership changes cannot interleave mounts.

- **Epoch fencing (the safety core).** Acquire opens at an epoch STRICTLY HIGHER than the unit's current DURABLE lease epoch, which fences the prior owner: its further writes to that unit fail. The cross-node epoch source of truth is the durable lease state (the slatedb manifest writer-epoch for slate; a shared epoch registry for the test factory), NOT in-process state. `OpenUnit(u, epoch)` carries the intended epoch; the backend factory performs the actual fence against the durable manifest.

- **Flush-before-release ordering + the NO-ACKED-WRITE-LOST invariant.** The old owner's `CloseUnit(u)` FLUSHES (durable) then releases. Phase 3 is single-replica with durable-before-ack (R=1 + AwaitDurable=true), so every ACKED write is already durable in object storage before the handoff: the new owner opening the unit sees all acked writes. In-flight (un-acked) writes may be fenced or lost, but the client never got success for those and retries. **NO ACKED WRITE may be lost.** The phase is built to this invariant.

- **Handoff window.** Between old-owner-release and new-owner-acquire the unit is briefly unavailable. An op routed to a unit not currently mounted by its ring-owner returns a RETRYABLE error (`codes.Unavailable` / `FailedPrecondition`, reusing the existing migration-guard / cutover retry shape) so the originator retries and succeeds once the new owner has acquired. Never serve a wrong or stale result; never lose a write.

**OUT OF SCOPE (later phases):** doubling / resharding and the migration tool (Phase 4+); per-unit replication / R>1 in multi mode (still R=1); relaxed durability (still AwaitDurable=true; that combo needs R>=2). The legacy per-node path and its Coordinator are not touched.

### v0.8 Phase 4: doubling resharder

Phase 4 lets an operator GROW capacity by DOUBLING the unit count: N -> 2N at a new cluster generation. Each old unit K bisects into EXACTLY new units K and K+N by one additional hash bit (the math is built: `storageunit.ChildUnit` / `ChildUnits` / `UnitCount.Double` / `UnitForHash`). The bisect is ONLINE and per-unit: the old unit keeps serving while its data is copied, then an atomic per-unit cut-over flips routing. Built to a NO-ACKED-WRITE-LOST invariant on a SINGLE-NODE cluster (the supported, gate-validated surface). Multi-backend mode ONLY; the legacy per-node path is untouched. On a SINGLE-NODE cluster `Reshard()` runs this local bisect directly; on a MULTI-NODE cluster `Reshard()` delegates to the arbiter-driven flow below ("Multi-node reshard"), whose R=1 drive reuses this per-unit bisect under parent-anchored placement. Assumes STABLE membership for the duration of a reshard.

- **Generation-qualified unit identity (the central change).** Today a unit maps to storage by UnitID alone (`Backing` keys `stores`/`epochs` by bare `UnitID`; `unitIDBytes` feeds the ring a bare id). That COLLIDES across a doubling: gen-g unit K (count N) and gen-(g+1) unit K (count 2N) would share one database. So a unit's STORAGE IDENTITY gains the generation. The implemented shape is a wrapping value type `storageunit.GenUnit{Gen Generation, ID UnitID}`: the storage key / object-store prefix is `(generation, UnitID)`, making gen-g unit K and gen-(g+1) unit K distinct databases that coexist during the bisect. It is threaded through `BackendFactory` (OpenUnit/CloseUnit/CurrentEpoch take a `GenUnit`; OpenUnits returns `[]GenUnit`), the cluster `mountTable.mounts` (keyed by `GenUnit`), the ring routing (`genUnitBytes` encodes the generation ahead of the unit id, so the gen-g and gen-(g+1) id of the same K hash to potentially different owners), and the test factories (memfactory / sharedfactory key their stores by `GenUnit`). `Generation` is a monotonic `uint64`; the cluster boots at generation 0.

- **Cluster generation.** The cluster has a CURRENT generation g (N units). Steady state: all units at gen g; routing = `UnitForShardKey(key, N)` at gen g. A reshard transitions g -> g+1 (N -> 2N) and is the ONLY thing that advances the generation.

- **The online per-unit bisect.** An explicit operator trigger - `Cluster.Reshard()` - marches through the N old (gen-g) units, one at a time (serialized by `reshardMu`, so two concurrent Reshard calls cannot interleave). For each old unit K: (1) create the two new gen-(g+1) units K and K+N (fresh databases via `factory.OpenUnit` at the new generation); (2) BACKGROUND-COPY: scan old-unit-K's keys, route each to new-K or new-(K+N) by the direct 2N map `UnitForHash(h, 2N)` (equivalently, the one new hash bit - the equivalence `ChildUnit(h, N) == UnitForHash(h, 2N)` is the doubling property, pinned by test), write into the new units - old-unit-K KEEPS SERVING reads + writes throughout; (3) CATCH-UP + ATOMIC CUT-OVER: take the per-unit write-pause lock for old-K's key-space ONLY (`pauseUnit`, which Put/Delete/Begin briefly block on), drain the last writes into the new units (a second copy pass under the pause so any write between copy-end and pause-start is captured), then atomically flip routing for old-K's key-space by adding K to the cut-over set, and retire old-unit-K (`CloseUnit` at gen g). After all N bisect, the cluster is uniformly at gen g+1 (2N units); the cluster's live generation advances to g+1 and the cut-over set is cleared (every unit is now resolved by the gen-(g+1) map). On a SINGLE-NODE cluster all 2N units stay on the one node and the reshard is complete. On a MULTI-NODE cluster this node-LOCAL bisect (write-pause + catch-up) is insufficient on its own: a cross-node doubling needs cross-node ordering so a concurrent write cannot route at the old generation on one node while another has flipped. `Reshard` called on a multi-node cluster therefore delegates to the arbiter-driven flow below ("Multi-node reshard"): at R=1, parent-anchored placement funnels each splitting unit's writes to its one gen-g owner, which runs THIS same per-unit bisect (background copy + pause-held clear+copy+flip); durable cut-over markers order cross-node finalize; then the 2N units redistribute via the reused Phase 3 lease handoff. The single-node path (this bullet) stays byte-for-byte unchanged.

- **Generation-aware routing during the transition.** The added routing state is small and lives in one `genState` value behind a `RWMutex`: the current generation g, the unit count N at g, the count 2N at g+1, and a per-old-unit "has this bisected/cut-over" set. For a key mid-reshard: compute its OLD unit K = `UnitForHash(hash, N)` at gen g. If K has cut over -> route to its NEW `GenUnit{g+1, UnitForHash(hash, 2N)}`. Else -> `GenUnit{g, K}`. When no reshard is in flight (empty cut-over set) every key resolves to `GenUnit{g, UnitForHash(hash, N)}`, so the steady-state path is unchanged. The resolved `GenUnit` is what gets placed on the ring (for ownership) and looked up in the mountTable.mounts (for the local backend). Deterministic; reads/writes land correctly throughout the reshard.

- **NO-ACKED-WRITE-LOST during the bisect (the safety core).** The hazard: a write to old-unit-K arriving DURING the background copy or the catch-up must not be lost. The copy-then-catch-up-then-atomic-cut-over pattern - the same shape as the validated Phase 3 handoff - handles it: writes before cut-over go to old-K and are captured by the catch-up drain; writes after cut-over go to the new units; the brief per-key-space write-pause makes the boundary clean. On a SINGLE-NODE reshard (the supported surface), with R=1 + durable-before-ack, every ACKED write is visible after the reshard - gate-validated lossless under ~252k concurrent acked writes. (A multi-node reshard's node-local write-pause is insufficient alone; it upholds this invariant via parent-anchored placement - see "Multi-node reshard" below, which funnels a splitting unit's writes to one node so this same boundary applies for the bisect's duration.) The phase is built to this invariant.

**THE CONCURRENT PROBE MUST PROVABLY OVERLAP THE RESHARD (gate soundness).** Every reshard gate whose oracle depends on writes acked DURING the reshard (the Phase 4 single-node gate `tests/integration/lossless_reshard_gate_test.go`, its break demonstration, and the multi-node gate below) starts its concurrent probe under a FIRST-ACK BARRIER, not a sleep: each probe writer closes a per-writer ready channel after its first acked write, and the reshard does not begin until every writer has done so, on a generous deadline. The overlap is therefore a STRUCTURAL property, not a bet on the reshard outlasting goroutine scheduling. This matters because an in-process bisect can complete in a few hundred MICROSECONDS - faster than the runtime wakes a freshly spawned writer on an idle machine - so a sleep-gated probe can issue ZERO writes, leaving the reshard racing nothing and the oracle pinning nothing. That failure is INVERTED (it fires when the box is quiet, so CI stays green) and it makes the oracle UNSOUND rather than merely flaky: a gate whose probe never ran will rubber-stamp a genuine lost-acked-write regression. A gate that cannot establish the barrier FAILS loudly rather than proceeding with an unproven probe.

**IN SCOPE: single-node growth by doubling under stable membership** (gate-validated lossless under concurrent writes). The MULTI-NODE doubling is the arbiter-driven flow specified next ("Multi-node reshard"), which reuses this per-node bisect per unit - there is no barrier and no abort: the durable target, once agreed, is converged to. **OUT OF SCOPE (later phases):** concurrent membership-change + reshard (STABLE MEMBERSHIP DURING A RESHARD IS AN ASSUMPTION, not an enforced guard: the retired freeze barrier used to abort on membership drift, and an arbiter-side membership re-check is an open follow-up); halving / shrink (growth/doubling only; an R=1 merge target refuses); the migration tool (per-node -> per-unit, a separate roadmap item); relaxed durability (still AwaitDurable=true).

### Multi-node reshard (arbiter-driven; the R=1 parent-anchored drive)

A multi-node doubling is coordinated by ONE mechanism: the decentralized CAS ARBITER (the v0.9 agreement layer below), for ANY replication factor. `Cluster.Reshard()` on ANY cluster with a `Config.ConditionalStore` (the arbiter wired) DELEGATES to it - single-node included, because delegation is what advances the durable `__cluster/init` marker and the arbiter record, so a restart resumes the post-reshard generation (the inline bisect path survives only for storeless single-node clusters, where no durable record exists to go stale): it CAS-retargets the arbiter's agreed count to 2N (`Arbiter.Retarget`), then synchronously pumps this node's reconcile - which drives its own share of the split AND observes peers' durable cut-over markers - until the LOCAL generation advances, bounded. A multi-node `Reshard()` with no `Config.ConditionalStore` refuses with the typed `ErrReshardNeedsConditionalStore` (the arbiter's agreement object lives in the shared conditional store; a deployable multi-node cluster already requires CAS-capable storage for slatedb manifest fencing, so the refusal excludes only configurations that could not deploy anyway). A retarget that was accepted but has not converged locally within the wait budget returns `ErrReshardInProgress`: the reshard KEEPS RUNNING on every node's reconcile cadence, and a repeated `Reshard()` is idempotent (retargeting an agreed target is a no-op; the call re-waits on the same convergence). The synchronous operator surface is therefore preserved over an eventually-convergent driver, and there is no coordinator, no cluster-wide freeze, and no abort: the durable target, once agreed, is converged to.

At R>1 the drive is the envelope dual-write protocol specified under v0.9 below, unchanged. At R=1 the copy/dual-write machinery CANNOT run - R=1 units store RAW bytes (no LWW envelopes) and the R=1 write path is single-leg (no dual-write router) - so the R=1 multi-node drive (`multibackend_reshard_r1.go`) reuses the PROVEN single-node bisect mechanics per unit, made cross-node-safe by one new invariant:

**PARENT-ANCHORED PLACEMENT (the R=1 cross-node ordering invariant).** While a split is in flight on a node (`genState.nextCount != 0` at R=1), OWNERSHIP resolution for a key ignores the cut-over set and resolves the PARENT `GenUnit{g, K}`; only the owner's LOCAL db resolution honors `cutOver`. Every write for K's key-space therefore funnels to ONE node - K's gen-g owner - for the whole in-flight window, whatever subset of cut-over markers each forwarding node has observed, and that owner's per-unit cut-over is EXACTLY the single-node bisect's proven boundary: background copy, then take K's write-pause WRITE side, clear + re-copy the two children (an exact image of the drained parent, deletes included - no resurrection), publish K's durable cut-over marker and - only if the publish succeeded - flip `cutOver[K]` while still holding the pause (MARKER BEFORE FLIP: the pause write side is still held, so a failed publish simply skips the flip and the next tick re-runs the clear+copy; flipping first would let writers ack into the children with no durable record of the flip, and a crash there would re-bisect on restart and revert them). On restart or re-entry, the drive treats the DURABLE marker as the flip authority: markers are observed into the in-memory view before any copy is driven, and a unit is bisected only when its marker is PROVABLY absent (a marker read error skips the unit - unreadable must not be treated as absent). A writer that resolved the parent before the flip holds the pause READ side across its apply, so the drain captures it; a writer that arrives after the flip resolves the gen-(g+1) child, mounted CO-LOCATED on the same owner (the in-flight desired set anchors every owned parent AND both its children to the parent's owner; parents stay mounted - flipped or not - until finalize). The durable markers order only cross-node FINALIZE: each node retires its parents and advances its generation once it observes EVERY unit's marker. The R=1 finalize deliberately runs NO final copy - the pause-held clear+copy at cut-over already made the children exact, no write can reach a parent after its flip, and re-copying raw parent bytes would clobber post-flip child writes and resurrect deletes (there is no apply-if-newer shield at R=1).

**SAFETY INVARIANT: NO ACKED WRITE IS LOST.** Before a unit's flip, every write for its key-space lands on the parent under the pause read side and the pause-held clear+copy captures it. At the flip, the children exactly match the drained parent. After the flip, writes land in the co-located children directly. Cross-node, parent-anchoring means no node ever routes a splitting unit's write anywhere but its one owner, so the staggered order in which nodes observe markers cannot land a write in a store being retired. After a node finalizes while a peer is still in flight, a stale forward hits the existing retryable acquiring-window / ring-refresh loop-guard errors (never a wrong ack) and succeeds on retry once the peer observes the markers and finalizes - the same "retryable across the flip window" model every generation change has. Post-finalize the children redistribute to their gen-(g+1) ring homes by the normal copy-free lease handoff; a fenced co-located mount whose ownership moved away is evicted and its orphaned factory hold closed by the reconcile's held-but-not-owned sweep.

**READ AVAILABILITY (retryable across the whole reshard).** Reads route parent-anchored like writes, served by the owner from whichever side of its flip the key resolves to. Across the staggered finalize window a read may briefly hit the retryable acquiring-window error or the ring-refresh loop-guard; a reader with the standard retry always eventually reads the correct value - never a permanent failure, never a wrong/stale value.

**THE MULTI-NODE LOSSLESS-RESHARD GATE (the data-loss oracle).** `tests/integration/lossless_multinode_reshard_gate_test.go` stands up a 3-node R=1 cluster on the shared-backing factory plus one shared `MemConditionalStore`, writes a recorded dataset spanning every unit (hundreds of keys plus co-located {tag} sets), runs a CONCURRENT probe that keeps acking writes through the full routed surface (held behind the FIRST-ACK BARRIER until every writer provably landed an ack), a continuous reader asserting retryable-availability, then triggers a delegated `Reshard()`. It asserts: (1) THE ORACLE - every baseline + acked probe key readable with its EXACT value from ANY node, zero loss, co-located sets intact; (2) gen g+1 reached with the 2N units partitioned correctly across nodes; (3) reads retryable-available throughout. It is kept honest by a BREAK DEMONSTRATION (`Config.TestingForceUncleanReshard`: flip + publish + retire WITHOUT copying) that asserts the oracle CATCHES the loss.

**IN SCOPE:** multi-node growth by doubling via the arbiter, any R, under stable membership; the delegated synchronous `Reshard()` surface; the typed no-store refusal. **OUT OF SCOPE / OPEN:** an R=1 MERGE (refused - never entered, never advanced toward - because a raw-bytes cross-node merge copy has no envelope ordering; halve at R=1 is a follow-up); a membership guard / abort (the retired freeze barrier snapshotted the member identity set and aborted on drift; the arbiter path has no abort and no bounded-completion deadline - the standing "orphan-target GC + per-reshard abort deadline" open item now covers R=1 too); a `Transact` whose buffering spans a mid-reshard cut-over of its pin unit (the pin-time-generation TODO in castx.go; the commit path itself is covered by the pause-held `pausedTx`); the mid-reshard full-cluster-crash resume limitation below.

(HISTORY. The v0.8 multi-node reshard was a coordinator-driven cluster-wide WRITE-FREEZE barrier: FREEZE -> static BISECT -> FLIP -> RESUME over a `ReshardControl` peer RPC, with write refusal on every frozen node and abort on any membership drift. It was retired when `MemConditionalStore` gave every backend CAS: the arbiter model replaces the freeze's global write-pause with parent-anchored placement plus the per-unit write-pause, keeps writes available outside each unit's brief cut-over, and removes the barrier's straddle/stranded-freeze failure modes along with its wire surface.)

#### Generation propagation to a joining node

A reshard advances the generation on the nodes PRESENT while it runs. A node that JOINS later must arrive at the cluster's LIVE generation, or it routes at the wrong one. This subsection specifies how a joiner learns the generation before it serves any key.

**The hazard (why a fresh joiner is wrong by default).** A node boots its routing state at generation 0: `initGenState` seeds `genState{gen: 0, count: UnitCount}` and the generation advances ONLY via a reshard the node itself participates in (the arbiter finalize, or the single-node bisect). After the cluster has resharded to gen g (2^g * N units), a node that joins starts at gen 0 / N units. Routing is generation-qualified end to end - a key resolves to `GenUnit{gen, UnitForHash(h, count)}` and the ring places a unit by `genUnitBytes(GenUnit)` (the generation is hashed AHEAD of the unit id), so the gen-0 id and the live gen-g id of the same key hash to DIFFERENT ring positions. The gen-0 joiner therefore: (a) as an originator, forwards a key to whichever node owns its gen-0 unit, and that node - routing at gen g - disclaims it (`forwarding loop refused: this node does not own the key`); (b) as a ring owner of some gen-0 unit ids, accepts forwarded ops for keys nobody at gen g routes to it. Either way an acked write is lost: the originator never reaches the live owner. Reconcile / settle do NOT self-heal it: the steady-state machinery never RAISES a node's generation.

**The fix: learn the live generation at Open, before mounting - deferring while a reshard is in flight.** Two paths, by configuration:

  - **ConditionalStore wired (the homogeneous deployment, and every cluster that can multi-node reshard):** the joiner adopts the `__cluster/init` durable marker's `{gen, count}` (`bootstrapViaMarker`). Before adopting, it checks the durable reshard state and DEFERS (bounded by `GenLearnBudget`) while a reshard is provably in flight - the arbiter's agreed count differs from the marker's count, or from its own target - re-reading the marker each pass so it adopts the post-reshard value. The defer is best-effort by design: the marker advances when the FIRST node finalizes (a lagging peer may still be draining - the residual window the retryable forward path covers), and on budget expiry the joiner PROCEEDS with the freshest marker value rather than failing `Open`, because a FULL-cluster restart mid-reshard would otherwise deadlock with every node deferring on a reshard no live node can finish (that case keeps the documented resume-at-marker semantics).
  - **No ConditionalStore (legacy seed-RPC clusters):** the joiner queries each visible seed's `GenState` RPC for `{gen, count, reshard_in_flight}` BEFORE it derives or mounts any unit, and commits the answer. A seed reporting `reshard_in_flight` (its `nextCount` is set) is NOT an answer: the joiner defers and re-sweeps (within `GenLearnBudget`) until a seed is steady, then seeds `genState` from the stable value. Only if every seed stays unreachable / rejecting / in-flight for the whole budget does `Open` FAIL rather than fall back to gen 0 (fail closed, supervised restart). The patience mechanics (re-sweep with backoff, the cold-starting-seed rationale, `genQueryTimeout`) are unchanged.

**Race-safety (no gen-0 serving window).** The generation is committed inside `Open`, before mounting and before `Open` returns, so by the time any KV method or forwarded RPC can run, `genSnapshot()` already returns the live generation. The caller registers the joiner's gRPC server only after `Open` returns, so no external request reaches it before its generation is correct.

**Multiple concurrent joins.** Each joiner independently learns the generation and seeds its own `genState`; N joiners arriving together each converge to the same live value. A joiner MAY itself be used as a seed by a second joiner before the first has settled its mounts - safe, because the first joiner committed its generation inside `Open` before becoming reachable.

**Interaction with a reshard.** A reshard that starts AFTER the join includes the joiner (it is in the ring; its reconcile drives its share and it advances with everyone else). A join that lands WHILE a reshard is in flight is deferred by the gates above, so a joiner only seeds from a stable generation. What the defer does NOT cover: the joiner's ring membership itself shifts gen-g placement the moment it joins gossip, so a join racing an active reshard can still move a mid-split parent between nodes - the standing concurrent-membership-change-during-reshard scope gap (the retired barrier ABORTED on membership drift; the arbiter path has no abort). Operationally a reshard is a short window; do not scale while one is in flight.

**Validation.** The targeted integration gate (`tests/integration/join_after_reshard_gen_test.go`) stands up a multi-node cluster sharing one conditional store, reshards it via the delegated flow, adds a node, and asserts the joiner reports the live generation, every acked pre-join key is readable FROM the joiner with its exact value, and a write routed through the joiner round-trips - plus the chaos harness's reshard-then-join seeds.

### v0.9 Decentralized reshard (declarative split/merge, R>1)

v0.9 introduced the DECLARATIVE, DECENTRALIZED reshard: declare the desired unit count (the way N pods are declared) and the cluster reconciles toward it ONLINE, growing (split, N -> 2N) or shrinking (merge, 2N -> N), with NO cluster-wide freeze and NO elected coordinator. The full design is `docs/decentralized-reshard-design.md`. It is now the ONLY multi-node reshard mechanism, for ANY R: the R>1 drive is the envelope dual-write protocol in this section; the R=1 drive is the parent-anchored reuse of the single-node bisect specified in "Multi-node reshard" above (split only at R=1); the single-node path keeps the inline online bisect. The driver runs from the reconcile tick (`observeReshard`, gated on a wired Arbiter, i.e. `Config.ConditionalStore` on a multi-backend cluster), and the IMPERATIVE `Cluster.Reshard()` entry on a multi-node cluster DELEGATES to it (Retarget + bounded converge; typed refusal without a store). The mode + reshard decision matrices derived from the code live in `pkg/cluster/doc.go`. Power-of-two double/halve only (clean bisection by one hash bit, no all-to-all reshuffle). `count = 0` is invalid and rejected.

**The agreement layer (built).** The cross-node ordering authority is NOT node-local state and NOT a coordinator: it is a single durable object in shared storage advanced by a conditional-write race - the same MinIO/S3 If-None-Match / If-Match (create-if-absent / compare-and-set) primitive slatedb already requires for manifest fencing. The capability is the `storageunit.ConditionalStore` interface (`PutIfAbsent` / `CompareAndSet` / `Get`-with-version); the slate module supplies `MinioConditionalStore`, tests use `MemConditionalStore`. Over it sits the `pkg/reshard.Arbiter`: it seeds, reads, retargets, and advances a `State{epoch, count, target, plan}` object. `Retarget` is the declarative "I want N shards" op (CAS the agreed `target`); `Advance` steps `count` ONE generation toward `target` (`Double` to split, `Halve` to merge), CAS-advancing `epoch -> epoch+1`, so exactly one racing writer wins and the rest adopt the winner. The agreed `target` lives IN the durable object (not as a per-node value) so a rolling config change cannot make one node split while another merges it back - every node plans the same direction. A node fails closed on an unknown (newer) schema version of the object. This is Redis's monotonic `configEpoch` made durable: determinism plus a CAS race replace the elected coordinator.

A cluster opts in by setting `Config.ConditionalStore` on a multi-backend node (any R); `Open` then constructs and seeds the Arbiter (idempotent across the cluster: one node's create-if-absent wins, every other node and any later joiner adopts the already-seeded State). When unset, or legacy, the Arbiter is not constructed, the cluster stays on the single-node reshard path only, and a multi-node `Reshard()` refuses.

**The online split protocol (built + gate-validated).** Declaring a doubled count (`Retarget`) drives a fully online `N -> 2N` split with no coordinator and no cluster-wide freeze, on every R>1 reconcile tick (`observeReshard`):

- **Agreement -> genState.** When the agreed count is ahead, a node enters the split (sets `genState.nextCount = 2N`); when it is behind by more than one generation it still steps ONE generation at a time (the non-contiguous-epoch invariant). When settled at the agreed count with the target still ahead, a node races `Advance` to step the agreed epoch one generation (the CAS lets exactly one node perform it).
- **Cut-over-aware desired set.** While a split is in flight, `desiredReplicaUnits` (and its draining-excluded sibling, in lockstep) also desire the gen-(g+1) CHILDREN, CO-LOCATED at their gen-g parent's replica slot, so the overlap reconcile mounts each child alongside its parent and the copy + dual-write stay LOCAL. After finalize the children redistribute to their gen-(g+1) ring homes by the normal zero-copy lease handoff (the slot's durable bytes have a fixed prefix; only which node holds the slot changes).
- **Two-generation dual-write router.** During the split a write DUAL-WRITES to the key's parent `GenUnit{g,K}` and the one child `GenUnit{g+1, child}` it hashes into, co-located at the same R slots. The ack bar is counted over the AUTHORITATIVE legs only: the PARENT until this node observes K's durable cut-over marker, the CHILD after. The other generation's legs are supplementary (best-effort), so every acked write has a durable home on the authoritative set throughout, and a mid-mount child returns the transient acquiring error the fan-out skips.
- **Envelope-aware copy + caught-up.** Each node copies its parent slot into the co-located child slots APPLY-IF-NEWER (never a bare Put: a stale copied value loses to a newer dual-write and vice versa, idempotent + order-independent), re-scanning until a CLEAN pass (applied nothing). It then publishes that slot's durable caught-up marker; once every replica slot of the unit is caught up, one node publishes the unit's durable cut-over marker (create-only CAS, one winner).
- **Per-unit flip + finalize.** Every node polls the durable cut-over markers and, on observing one, sets local `cutOver[K]` (routing + the ack bar flip to the children) - the single cluster-ordered cut point, observed identically by every node, replacing the freeze's global ordering with a per-unit durable one. Once EVERY old unit has cut over, each owned parent is retired under a per-unit WRITE-QUIESCE: finalize takes that unit's write-pause WRITE side around its final clean copy + retire, while every parent-leg apply takes the read side (`resolveAndApplyReplicaPut`). So the final copy and the retire are ATOMIC with respect to parent-leg writes: a lagging-node write racing the retire blocks, then resolves the now-retired mount as not-present and returns the transient acquiring error (re-routed to the child, never a lost ack). After every owned parent retires, the live generation advances; the overlap reconcile then redistributes the children to their ring homes.

**Why no acked write is lost.** The ack bar is over the authoritative generation throughout; the cut-over marker is published only after every replica slot is caught up (the children provably hold everything the parents held when the marker appeared); after the marker, writes ack over the children. A parent is retired only after a final clean copy taken UNDER its write-pause, so no write can land on the parent between the final scan and the retire (the per-unit analogue of the single-node bisect's pause, replacing the cluster-wide freeze with a per-unit one). There is no instant when the only durable home of an acked write is a unit being closed, and the single cluster-ordered cut point is the durable marker. The gate exercises this with WriteOne (a single-slot ack) through a widened finalize window.

**Durable generation marker (homogeneous deployment).** When a node opts into the homogeneous bootstrap (`Config.ConditionalStore` set - the single-StatefulSet deployment that retires the seed pod, `docs/design/homogeneous-bootstrap.md`), `finalizeReshard` ALSO advances the `__cluster/init` durable marker's `{generation, unit-count}` to the new generation. The STEADY-STATE guarantee: once a reshard has COMPLETED cluster-wide (every node has drained + retired its parents, the cluster is steady at gen+1), the marker is at gen+1, so a FULL-cluster restart (no live peer to query) resumes the finalized generation losslessly (all data is in the children) instead of re-forming gen 0 over gen-N data. The marker tracks the FINALIZED generation (the local `genState.gen`, advanced only at `finalizeReshard` under `allCutOver`), NOT the reshard arbiter's epoch: the arbiter advances its epoch to the next TARGET step BEFORE any node finalizes (it is the agreement that drives the split/merge), so the arbiter runs ahead of durability and a restart resuming it could mount children that are not yet caught up. The advance happens BEFORE any parent is retired (closing the narrow window where a restart between "parents retired" and "marker advanced" would resume a stale, retired-parent generation); it is a monotone, idempotent `CompareAndSet` (concurrent finalizers produce exactly one advance, a re-run never regresses), and a failed write DEFERS the retire and is retried by the next finalize tick. A cluster with no `__cluster/init` marker (legacy seed-RPC founding) is unaffected: the advance only updates an existing marker. A merge advances identically (the halved count). Scope: any cluster with a `ConditionalStore` (the R=1 arbiter-driven finalize advances the marker identically).

**KNOWN LIMITATION: a full-cluster crash DURING an active reshard is NOT lossless** (and the marker advance does not claim to make it so). `allCutOver` is LOCAL to a node (the cut-over markers IT has observed), and `finalizeReshard` runs per-node, so the FIRST node to reach `allCutOver` advances the SHARED marker to gen+1 while a lagging peer that has not yet flipped is still acking writes on its PARENT legs and has not yet drained those stragglers into the children (its strict drain runs in ITS own `finalizeReshard`, possibly later). Mid-reshard, acked writes are therefore split across gen-g parents (lagging units) and gen-(g+1) children (flipped units); a full-cluster crash + single-generation resume (which `bootstrapViaMarker` does) cannot hold both, so it loses the writes homed in the other generation (resuming gen+1 loses undrained parent stragglers; resuming gen-g would lose already-flipped child writes). This is fundamental to a generation-collapsed resume and is the same class as the not-yet-built `Transact`/join-during-reshard items below. The marker advance STRICTLY NARROWS the pre-existing behavior (before it, a mid-reshard full restart reset everyone to gen 0, losing everything past gen 0) and fully fixes the COMPLETED-reshard resume; making a mid-reshard full crash lossless needs resume-time reshard re-drain (re-mount the still-durable gen-g parents from object storage and re-run the strict drain into the children before serving), tracked as a follow-up. Operationally: do not full-restart a homogeneous cluster during an active reshard until that lands; a reshard is a short, operator-initiated window.

**THE LOSSLESS DECENTRALIZED-SPLIT GATE.** `tests/integration/lossless_decentralized_split_gate_test.go` stands up a 3-node R=2 cluster, declares a doubled count, and runs a continuous writer (recording only ACKED keys) through the full online `4 -> 8` split while child mounts are STAGGERED across nodes (so they observe each cut-over marker and flip in different orders, the cross-node flip window the freeze used to eliminate). It asserts ZERO acked loss readable from EVERY node, a high ack rate, and every node at gen 1. It is kept honest by a BREAK-DEMO (`Config.TestingForceUncleanReshard`: flip + retire parents WITHOUT copying their data) that asserts the oracle CATCHES the resulting loss.

**The online MERGE protocol (`2N -> N`, built + gate-validated).** Declaring a halved count drives a fully online merge, the same driver as the split with the direction-specific pieces:

- **Controller + routing.** `reshardGenStep` steps one generation toward a LOWER agreed count (`Halve`), and the routing substrate is direction-agnostic: `resolveGenUnit` resolves a cut-over key at `nextCount` (which yields the SURVIVOR for a merge, the CHILD for a split), and `ParentUnit(c, 2N) = c % N` maps both parents `K` and `K+N` to survivor `K`.
- **Desired set + placement.** The survivor mounts at its OWN gen-(g+1) ring home (NOT co-located - two parents on different nodes collapse into one survivor, so the copy is cross-node by nature). No post-merge redistribution: the survivor is already at its final home.
- **Two-source router.** A write under either parent dual-writes to its PARENT and the survivor it hashes into (`UnitForHash(h, nextCount)` at gen+1), ack over the authoritative legs (parent before the flip, survivor after). The survivor legs use the survivor's gen-(g+1) ring set (cross-node), not the parent's slots.
- **Two-source copy + dual-parent caught-up.** Each parent slot is copied into the survivor by forwarding every key to the survivor's replicas APPLY-IF-NEWER with a WRITE-QUORUM ack per key (the cross-node merge copy). Because two parents feed one survivor, the cut-over markers for BOTH parents are published together, gated on BOTH parents' slots being caught up - so the survivor holds both sources AS OF THE COPY SCAN before either parent flips. (Unlike the split, the cross-node copy does not loop to a strictly-clean re-scan, so a write that lands on a parent after the scan passed its key and whose best-effort supplementary survivor-leg drops is a flip-to-finalize read STALENESS, not a loss: it is on the parent, ack'd over the parent, and captured by the finalize strict re-copy under the pause before the parent retires.)
- **Finalize.** Both parents retire under the SAME per-unit write-quiesce as the split (the strict final copy - here a quorum forward into the survivor - runs under the parent's write-pause WRITE side, so no acked write lands on a retiring parent).

**THE LOSSLESS MERGE GATE.** `lossless_decentralized_split_gate_test.go` also drives a live `8 -> 4` merge under continuous writes + staggered timing: ZERO acked loss from every node, every node at gen 1 (N units), kept honest by a break-demo (flip + retire parents without copying into the survivor) and a WriteOne-across-a-widened-finalize-window variant that exercises the quiesce on the cross-node path.

**Scope (built).** Online SPLIT (`N -> 2N`) and MERGE (`2N -> N`) under stable membership, both directions gate-validated lossless. **Not yet built (subsequent phases):** `Transact` whose BUFFERING spans a mid-reshard cut-over of its pin unit (the commit path is covered; a joiner mid-reshard now DEFERS via the `GenState` `reshard_in_flight` field / the marker-path gate, see "Generation propagation to a joining node"); orphan-target GC + a per-reshard abort deadline; union reads across the in-flight window (a flip-time STALENESS, not a loss); backgrounding the copy for slow object stores (today it is synchronous under the reconcile lock - correct, and proven by the in-memory gates); composition with a concurrent membership change on the same unit; HLC stamps if clock skew proves real at the merge boundary.

#### Declarative reshard: the unit count is config, declared and gossiped

The v0.9 reshard above is DRIVEN by the arbiter's agreed `target`, but nothing yet SETS that target from operator config after founding. This subsection makes the unit count a DECLARATIVE config value: the operator declares "I want N units" the same way a Deployment declares `replicas`, and the cluster reconciles online. There is NO imperative trigger (no RPC, no `shale reshard` CLI command): the operator edits `SHALE_UNIT_COUNT` in the deployment and applies it, and the running cluster splits or merges to match. This is the operator surface for the homogeneous deployment.

**The config value is the same `SHALE_UNIT_COUNT` that seeds the arbiter.** At founding, `SHALE_UNIT_COUNT` seeds the arbiter's `{count, target}` (both = the declared value). After founding, the SAME value is the node's standing DECLARED unit count. The operator changes the cluster's shard count by changing that value in the single, shared deployment template and re-applying - exactly as they would change `replicas`. (The shard count and the replica/pod count are independent axes: scaling pods is a membership change the ring + rebalance already handle; changing `SHALE_UNIT_COUNT` is a reshard. Because the value lives in one shared pod template, every pod normally declares the SAME count, and a change is the only time they transiently differ.)

**Why the declared value cannot drive the target directly (the rolling-deploy flap).** The arbiter `target` lives in ONE durable object so the cluster plans one direction; if each node independently CAS'd the target to ITS OWN declared value, a rolling deploy - during which old pods declare the OLD count and new pods declare the NEW count for ~a roll's duration - would have nodes fighting the target back and forth (split, then merge, then split). The original design rejected per-node-desired for exactly this reason and left the target operator-set. The fix is not to abandon per-node config but to gate the retarget on AGREEMENT.

**Gossip the declared count; retarget only on unanimity + steadiness (coordinator-free, flap-proof).** Each node advertises its standing declared unit count in its membership metadata (the memberlist `NodeMeta` payload that already carries the address + draining bit gains a `U<count>` trailing segment; unknown segments are ignored on decode, so it is backward + forward compatible). On every R>1 reconcile tick, `observeDeclaredReshardTarget` runs alongside `observeReshard` and retargets the arbiter to a declared count `D` ONLY when ALL of:

  - the arbiter is STEADY (`count == target`, no generation step pending) AND no split/merge is in flight locally (`genState.nextCount == 0`); and
  - EVERY live member (including self) advertises a KNOWN declared count and they are ALL EQUAL to the same `D` (UNANIMITY); a member with an unknown/absent declared count - an older image that does not gossip it - breaks unanimity and defers the retarget (fail-safe); and
  - `D != target` (else it is already declared; no-op).

  When all hold, the node CAS-retargets the arbiter to `D` (the arbiter's CAS makes exactly one racing node win; the rest observe the new target and no-op). `observeReshard` then converges `count` toward `D` one generation per tick, online, lossless, in either direction - a larger `D` splits, a smaller `D` merges - using the already-built split/merge driver.

**Why unanimity is flap-proof (the load-bearing property).** During a rolling deploy the live set is a MIX of old-declared and new-declared nodes, so unanimity does not hold and NO node retargets - the cluster stays at its current count until the roll completes. The moment the last node rolls, the live set is unanimous on the new count and exactly one retarget fires. Crucially this also defeats the subtle race where a reshard finishes FASTER than the roll: a lingering old-declared pod keeps the live set non-unanimous, so even at steady state no node can drag the target back to the old count - the target moves only when EVERY live member agrees, and a not-yet-rolled pod is a VETO, never a vote for the stale value. The agreement is over LIVE members (a crashed/removed pod stops vetoing), so a node permanently stuck at a different declared count fails safe (the cluster simply never auto-reshards, and the disagreement is observable) rather than flapping. No elected coordinator and no extra durable state beyond the arbiter and the gossiped count: the gossip carries the DESIRED, the arbiter carries the AGREED, and unanimity-over-live-members is the bridge.

**Opt-in (the homogeneous deployment), so imperative drivers are not fought.** Driving the target from the gossiped declared count is gated by `Config.DeclarativeReshard`, set by the deployable runtime whenever an arbiter is wired in multi-backend mode (the homogeneous deployment). It defaults OFF, because the SAME arbiter is ALSO the seam the decentralized driver's own tests use: the lossless split/merge gate drives a reshard by calling `Arbiter.Retarget` directly (standing in for the declarative layer) while the nodes are founded at the OLD count, so if `observeDeclaredReshardTarget` also ran there it would reconcile the gate's target back to the founded count. With the flag, the gate (and every existing path) leaves the target externally driven; only the homogeneous deployment reconciles it from the gossip. The declared count is gossiped regardless (it is just informational when the flag is off). The declared count is the node's configured `SHALE_UNIT_COUNT` (env), captured at membership Open BEFORE any durable-marker count adoption - so a node that RESUMES at an older live count (the marker) while its env declares a new count correctly advertises the NEW (env) count, which is exactly the re-declare-then-roll signal.

**Scope.** Both directions (split + merge) via the existing gate-validated driver; power-of-two declared counts only (a non-power-of-two `SHALE_UNIT_COUNT` is rejected at flag validation, as today). The known mid-reshard full-crash limitation above is unchanged (a reshard is a short declare-and-wait window). A cluster with no arbiter (R=1, legacy, or `ConditionalStore` unset), or with `DeclarativeReshard` off, ignores the declared-count gossip entirely and never auto-reshards.

### slatedb BackendFactory (deployable multi-backend backing)

Everything above (Phase 2 multi-backend node, Phase 3 lease handoff, Phase 4 doubling reshard, the multi-node reshard) is validated IN-PROCESS by the chaos harness against `internal/sharedfactory`, a SHARED-BACKING test double: one shared store, per-unit handles, writer-epoch fencing simulated through a shared epoch registry. That double models exactly the shape a real object-store-backed factory must have, but it is in-memory and per-test. The DEPLOYABLE piece - the one a real `shaled-slate` node mounts to run the multi-backend model against real object storage - is the `storageunit.BackendFactory` whose backing is ONE shared MinIO/S3 bucket and whose per-unit databases are real slatedb instances inside it. This subsection specifies that factory. It lives in the `backends/slate` module (behind the `slatedb` build tag, alongside the single-instance `slate.Slate`) because it depends on the cgo slatedb binding; the core module never imports it.

It is the REAL version of `sharedfactory`: where the double's `Backing` is an in-memory map, the real factory's backing is the bucket; where the double's per-unit `*memory.Memory` is the durable bytes, the real factory's per-unit bytes are a slatedb database under a unit-derived prefix; where the double fences via a shared epoch registry, the real factory fences via slatedb's own writer-epoch protocol. The same `Backing` / `Handle` split applies: ONE `Backing` per cluster (it owns the bucket connection parameters), one per-node `Handle` (implementing `storageunit.BackendFactory`) off that `Backing`, mirroring the chaos harness wiring (`c.backing.Handle()` per node in `tests/chaos/adapter_inproc.go`). Many nodes' Handles point at the SAME bucket, which is what makes a lease handoff copy-free: a unit's bytes live at a fixed prefix that whichever node currently owns the lease opens.

#### GenUnit -> slatedb DbName mapping (the static data-location map)

A `GenUnit{Gen, ID}` maps deterministically to one slatedb `DbName` (the key-prefix within the shared bucket): `dbName = fmt.Sprintf("u/g%d/u%d", gu.Gen, gu.ID)` (a stable, collision-free encoding of the pair - the generation segment ahead of the unit segment, matching the ring's `genUnitBytes` ordering). The bucket is fixed for the whole cluster; only the DbName varies per unit. So `OpenUnit(gu, epoch)` opens a `slate.Slate` configured with `{Bucket: <shared bucket>, DbName: dbName(gu), ...}` - the existing single-instance backend, one instance per unit. This is the STATIC data-location map from "Two maps, opposite natures": a unit's bytes have a permanent home (`bucket/u/g<gen>/u<id>/...`) independent of which node has the handle open, so growth/handoff moves OWNERSHIP (which node holds the open handle), never bytes.

Because the generation is part of the DbName, gen-g unit K and gen-(g+1) unit K are DISTINCT slatedb databases at DISTINCT prefixes that coexist during a doubling bisect (the old keeps serving while the children fill) - the Phase 4 identity requirement, satisfied structurally by the prefix. A common `Bucket` is reused across the cluster; the per-unit DbName is what isolates one unit's LSM from another's inside it. (The existing per-node deploy already carves sub-prefixes by DbName inside one bucket; this generalizes the same isolation to one-database-per-unit.)

A configurable prefix (a `KeyPrefix` on the `Backing` config, prepended ahead of `u/`) lets one bucket host multiple unrelated shale clusters; default empty. The `u/` segment namespaces unit databases away from any unrelated object the operator might keep in the same bucket and gives `OpenUnits()` a single list-prefix to scan.

#### OpenUnit / CloseUnit / CurrentEpoch onto slatedb

- **OpenUnit(gu, epoch).** Opens (or creates on first touch) the slatedb instance at `dbName(gu)` in the shared bucket via the existing `slate.New` path, with `WriteOptions{AwaitDurable: true}` (the multi-backend invariant - see Durability below) and the operator's `Settings` (memtable sized small so many units fit one node, per the constraints section). Opening that instance IS the fence (see Epoch fencing). Returns the `slate.Slate` as a `backend.Backend`. The `Handle` records `gu -> {slate, openedEpoch}` in its in-process open map. Re-opening a `gu` THIS Handle ALREADY holds is a double-open error at ANY epoch (a unit has at most one live writer per handle): the held-check rejects it, and only `CloseUnit(gu)` clears the slot. (This is STRICTER than the `sharedfactory` double, which permits a strictly-higher same-node re-open. The double's strictly-higher re-open closed + reopened the same backing in-process; real slatedb does NOT support reopening the same db prefix WITHIN one process after it has been opened and fenced forward - it trips an internal "stored epoch is lower than local epoch" assertion in an async Rust task, surfacing as a process-level panic. In a real DEPLOY this never arises: each node is a separate OS process, so a node re-acquiring a unit it previously released opens the prefix in a FRESH process with no stale process-local epoch. The in-process chaos soak, which runs N "nodes" in ONE process, does emit these benign async panics on a handoff-back without losing data - the new owner's fresh open fences the stale instance correctly, so the oracle still sees zero loss. The strictly-higher same-node re-open is therefore both unsafe to realize in one process and unnecessary: the SUT's reconcile only ACQUIREs units it does not already hold - it RELEASEs (`CloseUnit`) before any re-acquire across a membership change - so a same-unit re-open without a prior close is never a real production path. Rejecting it closed keeps the factory correct standalone.) The whole open - the held-check, the durable-manifest fence read, the slatedb open, and the map insert - runs under one critical section per `gu` (a per-`GenUnit` open latch), so a same-unit concurrent open SERIALIZES rather than letting two goroutines both pass the held-check and both open. The `epoch` argument is the cluster's best-effort intended floor; the actual fence is authoritative against the durable manifest (next subsection), so a stale floor cannot under-fence.

  CAVEAT (carried from `slate.New`): the slatedb-go object-store config flows through AWS_* PROCESS env vars, which are global to the process. Every unit in one node's `Handle` shares ONE bucket + ONE set of credentials (that is the whole design - one shared backing), so the env writes are identical across units and do not collide. The `Backing` sets them ONCE at construction; per-unit `OpenUnit` does not re-write them. What is ENFORCED fail-fast is the ENV TUPLE: the backend keeps a per-process registry of the object-store env config it first applied (endpoint, region, credentials, SSL mode - the exact tuple the env writes carry), and a later construction (`slate.New` or `NewBacking`) whose config CONFLICTS with the registered tuple fails at construction time with a config-conflict error naming the differing fields (never the secret values), instead of silently clobbering the process env and authenticating requests with the wrong credentials. An identical repeat (e.g. many Handles over one Backing) registers cleanly. The BUCKET is deliberately NOT part of the tuple: it travels in the `s3://` URL, not the env, so two Backings in one process differing only by bucket neither collide in env nor trip the guard. That shape remains UNSUPPORTED by the cluster design (a node has exactly one `Backing`), but it is documented-unsupported, not enforced. The registry is write-once per process (the env vars are never unset, so there is no un-apply on Close); changing the object-store config requires a process restart.

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

The direct factory tests above prove the factory's PRIMITIVES in isolation (open/close/fence/enumerate). The strongest validation runs the SAME in-process multi-node chaos harness that exercises the whole v0.8 model (lease handoff on membership change, the doubling reshard, the join-after-reshard generation path) against the REAL slatedb factory instead of `internal/sharedfactory`, with the unchanged no-acked-write-loss oracle. This is what the legacy `test/real-n2-cluster` could not do: a goroutine "kill" still stops a node, but now that node's units are REAL slatedb databases in MinIO, so a survivor's re-`OpenUnit` (the handoff) reads the dead node's acked bytes back FROM OBJECT STORAGE. A passing soak therefore proves DURABLE-handoff losslessness, not just in-memory-shared-map losslessness.

**The test hook (one small seam, no production-path change).** The harness's in-process adapter (`tests/chaos/adapter_inproc.go`) needs ONE shared backing whose per-node `Handle()`s implement `storageunit.BackendFactory`. Both `sharedfactory.Backing` and the real `slate.Backing` already have exactly that shape (`Handle()` -> a `BackendFactory`), so the seam is a tiny `factoryProvider` interface in the chaos package: `NewHandle() storageunit.BackendFactory`, plus `Reset()` (clear durable state between seeds) and `Close()`. The adapter holds a `factoryProvider` instead of a concrete `*sharedfactory.Backing`, and each `node.handle` is the `storageunit.BackendFactory` interface type. The DEFAULT provider (`run` -> `newSharedFactoryProvider`) wraps `sharedfactory.NewBacking()`, so the existing in-memory soak is unchanged in behavior. A SECOND provider, built only under the `slatedb` tag (`tests/chaos/factory_slate.go`, `//go:build chaos && slatedb`), wraps a real `slate.Backing` against a MinIO bucket; the slate-backed soak runs as its own gated test (`TestChaosSoakSlate`) that calls the shared `runWithProvider` body with that provider. The seam adds no method to the shipped `BackendFactory` interface and no code to any production path - it is confined to the `chaos`-tagged test tree.

**Why a per-seed fresh bucket prefix.** The real factory's durable state is the bucket; a slate-backed soak therefore points each seed at a FRESH `KeyPrefix` so one seed's leftover unit databases (and their durable writer-epochs) never leak into the next seed's run (the in-memory double gets this for free by constructing a new `Backing` per run). The slate provider takes the bucket + connection params from the same env the direct integration tests use (`SLATE_MINIO_ENDPOINT` / `SLATE_MINIO_ACCESS` / `SLATE_MINIO_SECRET`); the test creates one fresh bucket for the whole sweep and removes it on cleanup, while `Reset()` rotates the `KeyPrefix` (`seed<N>/`) between seeds within that bucket.

**Adapting the harness budgets + transients to a slow backend.** Three harness changes (all in the `chaos`-tagged tree, none touching the SUT) make the no-acked-write-loss oracle meaningful against a real object store rather than flapping on backend latency:

  - **Settle-budget scaling.** Every `OpenUnit` / flush is an object-store round-trip (~100ms+), so the convergence + `WaitSettled` budgets that are millisecond-scale for the in-memory double are stretched by a `settleScale` multiplier (default 8x for the slate run, 1x for in-memory). Without this, the final sweep reads a half-mounted cluster and reports durable-but-not-yet-mounted units as FALSE `LOST` verdicts. Scaling the BUDGET (not the workload) is the line between "the cluster genuinely lost a write" and "we asked whether it finished mounting before it could."
  - **Settle the post-kill handoff BEFORE a reshard.** The reshard bisect bisects only MOUNTED old units (`mountedOldUnits`). The `reshard_while_down` combination kills a node then reshards; with the in-memory double the survivors re-mount the dead node's units instantly, but a real re-`OpenUnit` is a round-trip, so the harness must `WaitSettled` (every unit mounted on its ring owner) between the kill and the reshard. Otherwise the bisect skips not-yet-reacquired units and their keys never reach a gen-(g+1) child - a loss the harness would (correctly) flag, but one caused by the harness resharding an unsettled cluster, not by the SUT. The reshard assumes a settled membership+mount; the harness honors that.
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

#### Operator Reshard RPC (PROPOSED)

A PROPOSED operator-facing RPC `Reshard` on `ShaleNode` (not yet in the proto). `ReshardRequest{}` is empty (the only supported reshard is a doubling N -> 2N; no parameters). `ReshardResponse{ from_unit_count, to_unit_count, error }` reports the generation transition or a typed failure. The server handler calls `c.Reshard()` (the existing entrypoint: the inline bisect on a single-node cluster, the DELEGATED arbiter flow - `Retarget` + bounded converge - on a multi-node one) and maps its result: a clean return reports the doubled counts; `c.Reshard`'s own guards surface as the `error` string (legacy mode -> "Reshard is only valid in multi-backend mode"; no `ConditionalStore` on a multi-node cluster -> the `ErrReshardNeedsConditionalStore` refusal; not-yet-converged within the wait budget -> `ErrReshardInProgress`, the reshard continuing in the background). Carrying the failure in the response field (not a gRPC status) matches the `CommitCAS` / `ApplyBatch` wire convention.

**Who can call it + safety.** Any client that can reach a node's gRPC surface (the `shale` CLI would gain a `shale reshard` subcommand that dials one node and issues the RPC). Idempotency + safety are inherited from the underlying flows, not re-implemented: the single-node bisect is serialized by `reshardMu` and either fully proceeds or fully refuses (the up-front `count.Double()` ceiling check); the delegated flow is idempotent by construction (retargeting an agreed target is a no-op; a repeated call re-waits on the same convergence). The RPC adds NO new safety logic - it is a thin trigger over the validated entrypoints.

#### Validation: the chaosreal harness against the deployable binary

The real-cluster chaos adapter (`tests/chaos/adapter_real.go`, `//go:build chaosreal`) drives a cluster of SEPARATE `shaled-slate` OS processes over real gRPC + real memberlist + a shared MinIO bucket. Today its `Reshard()` seam returns `errReshardUnsupported` because the deployed binary is legacy-mode and there is no operator Reshard RPC. Once `shaled-slate` runs multi-backend mode (launched with `--unit-count > 1`) and the `Reshard` RPC exists, the adapter's `Reshard()` is wired to dial a node (the founder) and issue the operator `Reshard` RPC, and `errReshardUnsupported` is removed. The chaosreal launcher passes `--unit-count` (a new `SHALE_REAL_UNIT_COUNT` env, default a power of two > 1 so the model actually engages) and DROPS the per-node `--slate-db-name` (multi-backend mode shares one bucket; per-unit DbNames are factory-derived). The pass condition is the one the in-process soak proves, now end-to-end across a process boundary and a real reshard: ZERO acked-write loss across a real N=1 -> N=2 doubling, with the unchanged no-acked-write-loss oracle consuming acks from the real gRPC client. This is the validation that the v0.8 model is actually DEPLOYABLE, not merely in-process correct - the gap the chaosreal adapter was built to catch.

#### The deployable run path (exact wiring contract)

The two sections above state WHAT the deploy gap closes; this section pins the EXACT wiring so the implementation builds to a single contract. It does NOT change any `pkg/cluster` multi-backend LOGIC (the Phase 2/2b/3/4 + reshard machinery is unchanged): it only exposes that already-validated machinery to a deployable binary. The single-backend path (the `memory` / `pebble` / single-`slate` binaries) stays byte-for-byte except that `ReplicationFactor` is now threaded through (today it is dropped, pinning single-backend to the cluster default R=1; see below). The carried-over invariant is unchanged: NO ACKED WRITE IS LOST. This wiring acks a write exactly when `cluster.Cluster` does; it adds no write path.

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

## v0.13: one coordination engine, one storage port

shale is a coordination layer and toolkit. One proper distributed
implementation exists, over slate. A user could in theory build a distributed
implementation over sqlite; we have not, and it would be a bit odd. What shale
must NOT do is carry a separate implementation for some backends and a further
one for others, selected by what it was handed. That is a leaky abstraction:
shale should not know or care whether an adapter has certain limitations.

### The diagnosis

The `storageunit.BackendFactory` port is NOT the leak. It states an honest
requirement (the new owner opens the unit at a higher epoch and fences the
prior writer) and an adapter either satisfies it or does not. A contract some
adapters cannot meet is a normal, non-leaky thing.

The LEAK was the FALLBACK. Because `memory` and `pebble` cannot satisfy the
fencing requirement, shale carried an entire SECOND coordination engine: plan
key ranges, stream them over gRPC, verify a checksum, sweep the source. It then
selected between the two engines based on which backend it was handed.
Deleting the fallback un-leaks the abstraction; nothing else had to change for
that statement to become true.

The same diagnosis applied to R=1 versus R>1. Requiring a SECOND interface
(`ReplicaBackendFactory`) was a second place shale asked what an adapter could
do: `validateBackendMode` type-asserted it at `Open` and refused R>1 for a
factory that did not implement it, and the cluster carried a `replicaFactory`
capability view alongside `factory`. Collapsing the two ports removes that
question.

### One storage port

`storageunit.BackendFactory` is now the ONLY storage interface shale declares.
`ReplicaBackendFactory` is gone, the capability assertion at `Open` is gone, and
there is no type assertion anywhere in shale that asks an adapter which subset
of the contract it supports. An adapter either satisfies the contract or it does
not, and that is a property of the adapter rather than a branch in the
coordination layer.

Every method is keyed by `storageunit.MountRef`, and R=1 is replica 0.

#### The mount identity carries THREE components, not two

This is the load-bearing decision, because the literal reading of the collapse
is DATA-LOSS-UNSAFE.

An adapter that derives a storage location from the mount identity does not
derive the same location for "unit U" and "unit U, replica 0": the replica
position is a CHILD segment, so the two are different strings. The slate
encodings are the worked example:

    sole    -> "<keyPrefix>u/g<gen>/u<id>"
    replica -> "<keyPrefix>u/g<gen>/u<id>/r<replica>"

An existing R=1 multi-backend deployment's bytes live at the FORMER. Resolving
an R=1 open through the replica-0 encoding would mount an EMPTY database and
report the unit fresh: silent total data loss presented as a healthy cluster.

Before the collapse the selector was carried IMPLICITLY, by WHICH METHOD the
cluster called (`OpenUnit` versus `OpenReplicaUnit`), with the cluster-side twin
being `replicaFactory != nil`. One port has one method, so that carrier is gone
and the bit has to become explicit. `MountRef` therefore carries:

  - the generation-qualified unit,
  - the replica position,
  - the LAYOUT SELECTOR (`Replicated()`).

It is built only through `SoleMount(gu)` or `ReplicaMount(ru)`; the fields are
unexported, so there is no way to construct a ref whose layout is unset or to
flip it on an existing one. `SoleMount(gu)` and `ReplicaMount(NewReplicaUnit(gu,
0))` are DISTINCT values and DISTINCT map keys, exactly as their two locations
are distinct strings, which also lets an adapter track both families of mounts
in one map without aliasing.

The un-leaking still holds: the layout is a property of the thing being opened,
not a question shale asks the adapter about itself.

Rejected alternatives: configuring the adapter with R at construction (one
slate `Backing`/`Handle` serves both surfaces, and this moves a routing decision
into adapter config, the leak wearing a different hat); and migrating R=1 data
onto the replica-0 prefix (a data-movement event, not a refactor).

#### The change is INTERFACE-ONLY

No location derivation changed for either layout. The slate adapter already
funnelled both surfaces through one internal `unitRef` plus `dbNameForRef`, "the
SINGLE place the on-disk encoding split is decided"; that shape was LIFTED into
`pkg/storageunit` as the exported `MountRef` and `unitRef` became an alias for
it. The tagless pins in `backends/slate/dbname_test.go` still compile and pass
UNCHANGED, including `TestDbNameForRef_R1AndReplica0DoNotAlias` (the explicit
data-loss guard) and `TestUnitRef_R1AndReplica0AreDistinctMapKeys`. Having to
EDIT any of them would have been the signal that the change had moved bytes.

The serving-marker key moved from being addressed by `ReplicaUnit` to being
addressed by the mount, deriving as `dbNameForRef(kp, ref) + "/serving"`. For a
replica ref that is byte-for-byte the previous `dbNameReplicaFor(kp, ru) +
"/serving"`, which is pinned by a new tagless test; for a sole ref it is a
different key, which is the point (deriving both through the replica encoding
would alias a sole mount's marker onto replica 0's).

#### The port

    OpenUnit(m MountRef, epoch Epoch) (backend.Backend, Epoch, error)
    CloseUnit(m MountRef) error
    CurrentEpoch(m MountRef) (Epoch, bool)
    OpenUnits() []MountRef
    DurableEpoch(m MountRef) (Epoch, error)
    WriteServingMarker(m MountRef, epoch Epoch) error
    ReadServingMarker(m MountRef) (Epoch, bool, error)

Two signature changes beyond the key type. `OpenUnit` now RETURNS the exact
epoch it opened at, which was previously only on the R>1 method and is what a
caller must use as its open epoch rather than re-reading the durable epoch (a
shared counter any node's later open bumps). `OpenUnits` returns mount refs, so
it can enumerate replica mounts; the previous `[]GenUnit` signature could not
name them, so an R>1 handle reported an empty set.

#### The cluster side

`c.replicaFactory` is gone; there is one `c.factory`. The capability predicate
`replicaFactory != nil` became `c.replicaLayout()`, defined as `c.multi &&
c.replicationFactor() > 1` -- the same expression `initReplicatedFactory` used,
so the layout is unchanged for every configuration.

`replicaLayout()` is NOT `multiReplicated()` and the two must not be
substituted. `multiReplicated()` additionally requires a populated ring, so on a
SINGLE-NODE R>1 cluster they disagree: such a cluster mounts by replica position
while serving through the single-owner path. That divergence is pre-existing and
deliberate, but it now also decides ADDRESSING, so using `multiReplicated()` for
it would make a single-node R>1 cluster resolve mounts its own earlier boots
wrote elsewhere. `c.mountRefFor(ru)` is the one place a tracked `ReplicaUnit`
becomes a `MountRef`.

#### Consequence: no configuration is refused for lacking a capability

`Open` no longer rejects R>1 for a factory that cannot address replicas,
because there is no longer a way to ask. A factory that compiles against the
port has stated it meets the contract. This is the intended trade: the
alternative is shale branching on adapter capability, which is the thing being
removed.

### What was retired (the fallback engine)

`Config.Backend` WITH a `BindAddr` (legacy multi-node) and everything reachable
only from it:

  - `pkg/rebalance`, the range state machine, plan, execute, reconcile and
    sweep.
  - The cluster-side adapter: the Coordinator lifecycle, the settle-timer
    Evaluate, the ring-snapshot diffing, and the gRPC-backed
    `MigrateDestination`.
  - The per-NODE replicated dispatchers (`putReplicated`, `getReplicated`,
    their per-replica dispatch and the async read-repair scheduler). The
    fan-out and quorum PRIMITIVES they were built on (`fanout`,
    `requiredWriteAcks`, `requiredReadReplicas`, the transient-error
    classifiers) are SHARED with the unit path and stayed.
  - The `MigrateRange` and `ProposeRebalance` RPCs, their request/response
    messages, and the `shale rebalance` subcommand.

### What survives, and why

  - **Single-node mode, untouched.** `Config.Backend` with an EMPTY `BindAddr`
    is local-only embedding and keeps working with ANY backend. `Open` returns
    for single-node before membership is built, so nothing removed here was
    reachable from it.
  - **The debounce.** `Config.RebalanceSettleDelay` governs the multi-backend
    unit reconcile, not just the retired Evaluate.
  - **The retry-after hint.** `Config.RebalanceRetryAfterMs` is both the client
    hint and the base of the Layer-2 handoff retry backoff.
  - **`WaitForRebalanceIdle`.** Now purely the debounce-quiescence predicate,
    which is what multi-backend always relied on.
  - **The R>1 CAS envelope format.** `casReplicated()` is live in
    `CommitCASApply` and is TRUE for multi-backend R>1, so the envelope
    encode/decode path is not dead and was kept.

`Config.RebalanceGraceDuration` and `Config.RebalanceHandoffTimeout` fed only
the retired Coordinator and were REMOVED from the public `Config`. shale is
pre-v1; there is no migration path and no deprecation window.

### Known gap: re-replicating an under-replicated unit

A unit written while the cluster was too small to hold R replicas stays
under-replicated after the cluster grows. Concretely: a solo founder at R=2
writes into ONE replica position, because there is one node; when a second node
joins it acquires the (empty) second-position databases and the ring is
satisfied, but nothing copies position 0's bytes into position 1. The pair
settles holding a PARTITION of the keys rather than each holding all of them.

This is a consequence of the model, not a regression: the lease-handoff engine
never copies bytes, which is exactly what makes it copy-free. The retired
per-node engine papered over it by copying keys during its reconcile pass.
Closing it properly needs an anti-entropy / re-replication pass that fills a
newly-owned replica position from a peer that already holds one, which is a
design question of its own.

`tests/integration/rf2_membership_change_test.go` and
`rf2_shrink_retention_test.go` carry skipped tests asserting the property, kept
because it is one a replicated store should eventually hold. Note the SHRINK
direction is fine and covered: a leave never drops a key below R
(`TestRF2_NoKeyDropsBelowReplicationFactorOnLeave` passes), because the
surviving replica databases already exist.

### The binaries

`shaled` and `shaled-pebble` are single-node demonstration and test daemons.
`--bind-addr` is blanked in `shaled.Run` when a single `Backend` is configured
and the operator did not explicitly supply one, so a bare invocation works out
of the box; an EXPLICIT `--bind-addr` is honored and then refused by `Open`
with a message naming the multi-backend requirement. A non-empty `--seeds` with
an empty `--bind-addr` is a startup error, so an operator cannot silently get N
independent single nodes where they asked for a cluster. The default is NOT
changed in `BindStdFlags`, because that constructor is shared with
`shaled-slate`, whose multi-backend mode is a legitimate multi-node
configuration.

## The coordination port (pkg/coord)

The cluster no longer talks to gossip, the ring, or membership directly: it
asks a `coord.Coordinator` port, and `pkg/coord/gossip` is the one shipping
adapter (memberlist + the consistent-hash ring + the event/reconcile loops
that mirror one into the other). Everything the "Cluster model" section above
says about memberlist mechanics - seed rejoin, meta refresh, incarnation
staleness, drop-tolerant event channels - remains exactly true, but it is now
a description of the GOSSIP ADAPTER's interior, invisible through the port.
`pkg/coord` itself imports only `errors` and `pkg/storageunit`.

**The boundary.** The port answers WHO and WHERE questions: who is a member
right now (`View`, and the per-op forms below), where does a unit sit
(`Locate`), what transitional stance does each member advertise (roles:
`Joining`, `Draining`), and a coalescing hint that any of it may have changed
(`Changed`). The storage layer keeps every protocol built ON those answers:
boot/join orchestration, the settle debounce, serving markers, the drain gate,
overlap warm-up. A different coordinator (the planned CAS/lease adapter backed
by conditional writes) replaces how the answers are produced, never what the
storage layer does with them.

**Contract points that carry the safety weight:**

- **Hints are lossy; roles are not.** `Changed` may coalesce or drop; every
  consumer re-reads the view and reconciles on a cadence. Role visibility is
  NOT allowed to ride the hint channel: a role flip must be visible to the
  query methods the moment the coordination mechanism delivers it, hint or no
  hint. (This is why the adapter may not serve roles from a change-invalidated
  cache: a dropped role-flip event would freeze a stale answer in place.)
- **Per-operation queries are specified per-op-cheap.** `Populated()` (the
  replicated-vs-single-owner routing predicate) and `TransitionSets()` (the
  joining/draining split behind current/pending routing) run on EVERY
  Put/Get/Delete. Both are contract-bound to answer without constructing a
  view snapshot, and steady-state `TransitionSets` returns nil maps
  (allocation-free fast path). `predicate_bench_test.go` pins both against
  the snapshot-built shape they replaced.
- **`Locate` is deterministic.** Two nodes holding the same view compute the
  same replica set in the same order; routing correctness rests on it.
- **`Placement.Exclude` is a POST-TRANSITION promise, in protocol terms.**
  Locate with an Exclude set must return the placement that WILL HOLD once
  those nodes are no longer members - the same answer the coordinator itself
  will give after their departure - never the current placement with the
  excluded nodes filtered out of the result. The hashing adapter meets it by
  genuinely rebuilding a reduced ring (bounded-load consistent hashing is not
  removal-invariant, so filter-after-locate is a DIFFERENT and wrong
  placement that can strand a handoff on nodes that will never own the unit);
  a table coordinator meets it by reading its coordinated post-departure
  assignment. Phrasing the contract as the outcome rather than the mechanism
  ("as if these nodes were not members") is what lets both adapters implement
  it honestly.
- **`Start` owns bootstrap truth.** "Did anyone else already exist" is
  answered by the coordinator (it is a fact about how the cluster was
  discovered), and `Params.InitialRoles` lets a node advertise `Joining`
  atomically with its own first announcement - closing the race where a peer
  learns "the newcomer exists" before "the newcomer is warming" and
  clean-cuts a position the newcomer displaces.

**MIGRATING TO v0.15.0 (a BREAKING construction change).** `Config.BindAddr`
and `Config.Seeds` were REMOVED from `cluster.Config` when coordination moved
behind this port: they are gossip transport details, and the port exists so the
cluster does not name them. A consumer that set either field no longer
compiles, and the fix is one construction line - hand them to the adapter
instead:

```go
// before v0.15.0
cluster.Open(cluster.Config{ /* ... */ BindAddr: bind, Seeds: seeds })

// v0.15.0 onward
cluster.Open(cluster.Config{ /* ... */ Coordinator: gossip.New(gossip.Config{
    BindAddr: bind,
    Seeds:    seeds,
})})
```

`Coordinator: nil` means SINGLE-NODE, exactly as an empty `BindAddr` did before,
so a single-node consumer drops the fields and passes nothing. Behavior is
otherwise unchanged: the gossip adapter runs the same memberlist, the same
seed bootstrap, the same ring. A consumer that never set `BindAddr` / `Seeds`
(single-node, or one that already constructed a coordinator) upgrades with no
source change at all.

## The CAS coordinator (pkg/coord/cas)

The port's second adapter: `pkg/coord/cas` produces the same answers from ONE
shared membership document in a `storageunit.ConditionalStore` (`PutIfAbsent` /
`CompareAndSet` / `Get` - the same conditional-write seam the reshard arbiter
runs on, implemented by slate's MinIO adapter and the in-process
`MemConditionalStore`) instead of from a SWIM mesh. Both adapters are
first-class and permanent: gossip serves every store (it needs no conditional
writes); CAS serves deployments that already pay for a conditional store (a k8s
deployment on MinIO/S3) and want one truth channel with no UDP mesh to
operate. Nothing above the port moves when the adapter is swapped: serving
markers, the drain protocol and `DrainForLeave`, overlap warm-up, boot-defer /
generation learning, the settle debounce are all built on the port's ANSWERS
and behave identically under either adapter.

**The membership document.** All coordination state is one JSON document at a
well-known key (`__coord/members`): a member list of rows `{id, addr, roles,
declaredUnitCount, leaseGen, inc}`. The store's opaque version token (an ETag
on the real store) is the document's CAS handle. `inc` is the row's
INCARNATION nonce: minted fresh every time a row is CREATED (bootstrap, or
the renewal re-add after a GC) and carried unchanged by in-place edits. It
closes an ABA hole in expiry: a member GC'd and rejoining between two of an
observer's polls restarts `leaseGen` at 1, which can equal the counter the
observer last tracked - without the nonce that observer reads "still not
advancing", keeps the member expired, and its GC reaps every fresh row the
rejoiner writes. Observers compare nonces for INEQUALITY only (never order
them), preserving the no-clocks property. Every mutation - join, role
change, lease renewal, expired-member GC, graceful leave - is a
read-modify-write `CompareAndSet`, retried on `ErrPrecondition` with bounded
backoff: one racer wins, the loser re-reads the winner's document and
re-applies its edit, so concurrent editors interleave without ever losing each
other's rows.

**The lease model: counters, not clocks.** Each node renews its own lease by
CAS-incrementing its OWN row's `leaseGen` on a fixed cadence (production
default ~2s). Liveness is a monotone counter advancing in the document. No
member ever writes a wall-clock timestamp and no observer ever compares one.

**Failure detection is observer-side lease expiry.** Every node polls the
document on its own cadence (production default ~1s; the poll and the renewal
are the adapter's only two I/O loops, and both stop at Close). An observer
tracks each member's `(inc, leaseGen)` across its own poll ticks; a member
whose pair has not changed for K consecutive observed polls (default 5) is
EXPIRED: dropped from the view THIS OBSERVER serves. A node never expires
ITSELF: its renewal advances its own counter, and a node that cannot write
the store cannot poll it either, so self-exemption also keeps the port's
"View always contains Self once started" shape. The judgment compares two
rates the observer itself witnesses (its polls vs the member's renewals),
never two clocks, so clock skew between nodes cannot false-expire anyone -
and a stalled observer cannot either, because its poll ticks stall with it and
the count restarts from the current `leaseGen` when it resumes. The tuning
constraint is `K * pollInterval` comfortably above the renewal interval (the
defaults give 5s of observed silence against a 2s renewal, a 2.5x margin);
the port makes even a mis-tuned expiry an availability cost, never a
correctness one. Any live member may GC an expired member's row out of the
document via CAS - idempotent, and racing GCs resolve by CAS (one wins; the
rest re-read a document that is already correct).

**Duplicate identity is displacement, and the old process steps down.** Each
node records the incarnation nonce it minted for its own row; every renewal
verifies the row still carries it. A row wearing a different `inc` means a
replacement process with the same stable ID took the identity over (the k8s
StatefulSet overlap: the old pod still running behind a partitioned kubelet
while its replacement bootstraps). Blindly renewing would merge the two
processes into one lease - and if the replacement then dies non-gracefully,
the zombie keeps the row's counter advancing forever, so no observer ever
expires a row whose address points at a dead pod. The old process STEPS
DOWN instead: it marks itself displaced (surfaced through the adapter's
`Health()` accessor), stops renewing, polling and GC'ing, keeps serving
reads from its last published snapshot, and never mutates the document
again - Close skips the graceful row removal too, because the row now
belongs to the replacement. Terminal state; the supervisor retires the
displaced process.

**The tradeoff vs SWIM, stated honestly.** Detection latency is
`K * pollInterval` (default ~5s) plus up to one renewal interval of slack -
deliberately more conservative than SWIM's probe + suspicion pipeline, which
reaps a dead peer in a couple of seconds. In exchange: no gossip port, no
seed list, no mesh to operate, and a liveness that means exactly "this node
can write the shared store" - for a store-backed deployment the liveness that
matters, because a node that cannot reach the store cannot serve anyway. The
document is also a serialization point: renewals are CAS writes to one
object, so the document's write rate grows as N / renewalInterval and
contention retries grow with N. Right for the small fleets this adapter
targets; wrong for a hundred-node mesh, which is gossip's territory.

**One snapshot, one basis: the dual-basis class is structurally absent.** The
poll rebuilds a `pkg/ring` from the live (non-expired) member set whenever
the observed membership changes and publishes `{view, ring}` as ONE immutable
pair behind an atomic pointer swap. Every per-op query (`View`, `Populated`,
`TransitionSets`, `PlacementMembers`, `Locate`) reads the currently published
pair: no locks, no I/O, per the port. Because view and ring are one artifact
derived from one source, `PlacementMembers()` equals `View()`'s member set
ALWAYS - there is no event channel to drop and no reconcile cadence to wait
out, so the gossip adapter's documented window (ring trails the snapshot,
heals on reconcile) cannot exist here. `PlacementMembers` stays on the port
because the guards that read it must work under either adapter; under CAS it
simply never deviates.

**Placement is the same math.** Same `pkg/ring`, same `GenUnitBytes` hash
input, same bounded-load construction. `Placement.Exclude` is honored by the
same genuinely-rebuilt reduced ring the gossip adapter uses, meeting the
exactness contract (the placement that WILL HOLD after departure, never
filter-after-locate). Given the same live member set, the two adapters answer
Locate identically.

**Bootstrap is discovery, and it is exact.** `Start` attempts `PutIfAbsent`
of a document containing only self. Success IS founding (`BootstrapFounded`);
`ErrPrecondition` means an incumbent document exists, so Start CAS-appends
self's row and reports `BootstrapJoined`. A document that exists but holds
ZERO rows also founds: everyone left gracefully, so there is no incumbent to
learn a generation from, and "did anyone else already exist" is answered no
(reporting joined there would leave the starter waiting forever on a departed
incumbent). `Params.InitialRoles` ride in that
first row, atomic with the node's first appearance - the same atomicity the
gossip adapter gets from the first Meta payload, closing the same
member-before-warming race (and a founder still drops a requested
`RoleJoining`, per the port). `SoloStart` collapses to store reachability:
the conditional write makes founded-vs-joined a definitive answer rather than
a could-not-reach-peers guess, so there is no tentative-then-correct dance
(the gossip solo-found retraction has no CAS counterpart) and no silent-fork
risk for the flag to guard. An unreachable STORE fails Start - the store is
this adapter's one required peer.

**The store outlives the cluster: run CAS with the marker path.** A
full-cluster crash (SIGKILL, power loss) removes no rows and leaves no live
observer to expire them, so every restarting node finds the prior
generation's rows and bootstrap reports `BootstrapJoined` even though every
"incumbent" is dead - the zero-rows founding rationale above applies
verbatim to all-rows-dead, but the adapter cannot tell a dead incumbent
from a slow one at Start. The CAS coordinator is therefore DESIGNED to run
with `cluster.Config.ConditionalStore` wired to the SAME store (the shape
the integration fixtures wire, and the production shape): on that path a
joiner learns the generation from the durable bootstrap marker instead of
dialing a live incumbent, which makes all-nodes-crash recovery BOUNDED -
the zombie rows pollute the view for at most `ExpiryPolls * pollInterval`,
because bootstrap seeds every incumbent row's expiry baseline at Start
itself (observation begins at Start, not at the first poll), then expiry
drops them and GC reaps their rows. WITHOUT the marker path
(`ConditionalStore` nil), a joiner must learn the generation from a live
peer that only begins serving after its own Open returns: after a
full-cluster crash all N nodes mutually wait, each fails Open on the
generation-learn budget, and recovery degrades to the supervisor-restart
cycle escaping only if some node's bootstrap read lands in an
all-rows-removed window. Deploy the CAS adapter with the marker path.

**Role visibility rides the poll, never the hint.** `SetRole` CAS-updates
self's row (bounded retries) and returns; the flip becomes visible to the
query methods when a poll next refreshes the snapshot - this node's own
within one poll interval, each peer's within theirs. That satisfies the
port's liveness clause the same way View does: the poll IS this mechanism's
delivery, and the change hint plays no part in it, so a dropped hint can
never freeze a stale role answer. Roles are exactly as live as membership
itself - one snapshot - and `TransitionSets` can never disagree with `View`.

**`Changed()` is the same lossy hint.** The poll fires the depth-1 coalescing
channel when the PUBLISHED SNAPSHOT moved - membership, addresses, roles,
declared counts, including an expiry (which changes the view without any
document write) - never on document-version movement alone. The distinction
is load-bearing: lease renewals advance the document version on virtually
every poll while leaving the view unchanged, because renewals are the
mechanism's HEARTBEAT, not a view change. A version-keyed hint would
therefore fire at poll cadence, and a consumer that debounces view changes
with extend-on-burst semantics (the cluster's settle timer) would have its
debounced reconcile re-armed forever and never run it - reconcile
starvation, pinned as a regression by the adapter's
`TestChanged_FiresOnObservedChangeAndCoalesces`. Coalesced and droppable by
contract; consumers re-read and reconcile on a cadence, exactly as under
gossip.

**Close is the graceful fast path; expiry is the crash path.** Close stops
both loops, then best-effort CAS-removes self's row. A crash, a partition or
a failed removal falls through to observer-side expiry - the same
graceful-leave-vs-reaping split gossip has with Leave vs SWIM suspicion.

**Generation agreement stays cluster-side.** The reshard arbiter already
drives generation + cut-over agreement over the same `ConditionalStore`,
above the port. The CAS coordinator does not absorb it: `Generation()`
returns 0 and `ProposeGeneration` refuses with `ErrGenerationUnsupported`,
exactly like gossip. (One document and one truth channel make this adapter
the natural future home, but moving the arbiter is a separate change with its
own migration story.)

**Choosing an adapter.** Operational fit, not capability:

- **CAS (`pkg/coord/cas`)** - needs a `ConditionalStore`. One truth channel:
  membership, roles and liveness live in the store the data lives in, so
  "coordinator says alive" and "can actually serve" cannot silently diverge.
  No SWIM mesh, no seed lists, no extra open port: the k8s-friendly shape
  (hostthis migrates to it). Slower failure detection (`K * poll`, ~5s
  default) and a per-document CAS rate that grows with N.
- **gossip (`pkg/coord/gossip`)** - works over ANY store; keeps every backend
  without conditional writes fully supported. Faster failure detection (SWIM
  probe + suspicion, seconds) and no central serialization point under
  membership churn. Carries the documented dual-basis window and the
  operational surface of a mesh: a bind port, seed lists, UDP reachability.

**One contract, two adapters.** The port-contract tests that grew up inside
the gossip suite are extracted to a shared harness, `internal/coordcontract`:
`RunContract(t, adapter)` pins Populated tracking View, TransitionSets
matching View's roles (nil-map steady state, retraction back to nil,
caller-owned maps), the PlacementMembers basis agreement, Locate determinism
across instances, primary-stable-across-N, clamping and degenerate n,
exclusion as a genuinely reduced placement, and exclude-everyone-returns-nil.
Every adapter runs the harness and keeps its adapter-specific suite on top
(gossip: ring-drop heal, reconcile idempotence, bootstrap discovery; cas:
lease expiry, document GC, bootstrap races).

## Roadmap

- [ ] **v0.1** - single-node Cluster wrapping one Backend; memory backend impl; SlateDB backend impl; gRPC service (used by CLI + ready for v0.2 inter-node); `shaled` standalone binary; `shale` CLI with put/get/delete/scan/topology/stats/ping. API lockup.
- [ ] **v0.2** - multi-node + memberlist + hash ring + gRPC forwarding. Static topology (no rebalance). `shale topology` now shows real membership + ring.
- [ ] **v0.3** - rebalancing on join/leave. Atomic ownership swap. (Retired in v0.13: the per-node key-copy engine this shipped was replaced by the v0.8 unit lease handoff, and the `shale rebalance` subcommand went with it.)
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
  - [x] Storage-unit domain types (`pkg/storageunit`): UnitID + power-of-two UnitCount, the `hash(ShardKey) & (N-1)` unit map, the doubling bisect (ChildUnit/ChildUnits, exhaustively tested), Epoch + BackendFactory mount/lease seam, the owned-unit derivation. Pure, no I/O.
  - [x] Multi-backend node (Phase 2, STATIC routing): a backend FACTORY + a `unit -> backend` mount map alongside (and mutually exclusive with) the single `Config.Backend`; the ring re-keyed onto unit ids; routing `key -> unit -> owner`; the unit-based owner guard. Topology is fixed at Open (no rebalance / lease handoff yet). See the "v0.8 Phase 2" section above.
  - [x] Lease-handoff rebalance (Phase 3): close-on-old / open-at-epoch+1-on-new, the handoff window, the anti-entropy reconcile (own-but-not-mounted -> mount). See the "v0.8 Phase 3" section above.
  - [x] Doubling resharder (Phase 4, SINGLE-NODE): generation-qualified unit identity (`GenUnit`), `Cluster.Reshard()`, the online per-unit bisect (background copy split by the direct 2N map `UnitForHash`, with `ChildUnit` as the test-pinned statement of the equivalence, catch-up drain under a per-unit write-pause, atomic cut-over), and the generation / per-unit-cut-over routing state. Gate-validated lossless under ~252k concurrent acked writes. On a single-node cluster this is the whole reshard; on a multi-node cluster `Reshard` now delegates to the arbiter-driven flow (see the R=1-migration item below). See the "v0.8 Phase 4" section above.
  - [x] Multi-node reshard: the coordinator-driven cluster-wide WRITE-FREEZE barrier (freeze / bisect / flip / resume) so a doubling coordinates across nodes safely (writes briefly retryable cluster-wide, reads continue, the per-node bisect is a static copy, atomic flip + retire, fail-safe abort on any node failure or membership change), then the 2N units redistribute via the Phase 3 handoff. Wired as a `ReshardControl` RPC on the `ShaleNode` service (phase enum FREEZE/BISECT/FLIP/RESUME/ABORT + target generation) driven over the peer-RPC pattern; the per-node freeze flag gated Put/Delete/Begin + the CAS commit write path. SUPERSEDED: the barrier (and its `ReshardControl` wire surface, freeze gates, membership abort, and stale-freeze self-heal) was retired when the R=1 multi-node reshard migrated onto the CAS arbiter - see the next item and "Multi-node reshard" above.
  - [x] R=1 multi-node reshard migrated onto the decentralized CAS arbiter (freeze barrier RETIRED): the arbiter is constructed for ANY multi-backend cluster with a `ConditionalStore`; a new R=1 parent-anchored drive reuses the single-node bisect per unit (placement resolves the PARENT for a splitting unit's whole in-flight window, so the owner's pause-held clear+copy+flip is the one write-path boundary; durable cut-over markers gate cross-node finalize; no final copy at R=1 by design); `Cluster.Reshard()` on a multi-node cluster DELEGATES (Retarget + bounded converge) with typed `ErrReshardNeedsConditionalStore` / `ErrReshardInProgress` refusals; a joiner defers its generation adoption while a reshard is in flight (the `GenState` `reshard_in_flight` field + the marker-path defer). R=1 merge targets are refused (split only). See "Multi-node reshard (arbiter-driven)".
  - [x] Generation propagation to a joining node: a multi-backend joiner (Open WITH seeds) learns the cluster's live `{generation, unit-count}` by a synchronous peer RPC (`GenState`) to a seed BEFORE it derives or mounts any unit, then seeds its `genState` from the answer - so it never routes / owns / serves a key at gen 0 after the cluster has resharded. Fixes the reshard-then-add-node acked-write-loss path (a gen-0 joiner orphans keys: `forwarding loop refused`). The query is PATIENT - it re-sweeps visible seeds with backoff for a bounded budget (`GenLearnBudget`, default 180s) so a seed that is still cold-starting (port bound, gRPC not yet serving until its own mount finishes) is WAITED FOR instead of crash-looping the joiner; fails closed only if every seed stays unreachable for the whole budget (Open fails, no gen-0 fallback). The founder + single-node + legacy paths keep the gen-0 default. See "Generation propagation to a joining node" above.
  - [ ] Write-availability through an R>1 membership change (Phase 2d, retry-on-acquiring): make `errUnitAcquiring` a TRUE fan-out transient (sentinel-tagged so `isTransientReplicaErr` skips it though it stays `codes.Unavailable` client-facing; the cross-node replica leg re-codes to `codes.ResourceExhausted`), and wrap Put / Delete / CommitCASApply (R>1 AND R=1 multi-backend) in a `WriteTimeout`-bounded retry-on-acquiring with the existing `RebalanceRetryAfterMs` backoff + jitter, so a write to a unit mid-reconcile WAITS for the re-mount instead of erroring. Turns the handoff-window refusal into bounded latency; no-acked-write-lost + single-writer-fence unchanged. Acceptance: an in-process loss-oracle gate that writes continuously through a membership change asserts zero acked loss AND acked/attempted > 95% (vs the measured ~50-64% pre-fix), demonstrated to FAIL on the pre-fix code. Option A's availability is bounded by mount-time-vs-`WriteTimeout` (big-bang 3 -> 12 measured ~54%); Phase 2e (overlap handoff) removes that bound and A is retained as its safety net. See "v0.8 Phase 2d" above.
  - [ ] Pending ranges (Phase 2e, graceful membership transition): `routedReplicasForKey` computes a position's CURRENT replica set (ring INCLUDING draining members) and PENDING replica set (ring EXCLUDING draining members); a position is IN TRANSITION when CURRENT != PENDING (a leave: a current member is `Draining`; a join: this node is a pending owner of a position whose serving marker is absent). During a transition the ROUTED set is UNION(current, pending); the fan-out DUAL-WRITES every union member with the ack bar held at the STABLE R quorum (`W = requiredWriteAcks(WriteConsistency, R)`, NOT raised to cover the union - the pending replica is a bonus target, not a higher bar; `All` is held at the stable R too - `All` means all STABLE replicas, NOT the transient union, so a mid-mount successor never blocks the write); reads fan out across the union and any MOUNTED member serves (a mid-mount pending owner returns `errUnitAcquiring` and is skipped). Pending owners ACQUIRE in the background via the normal `reconcileReplicaUnits` -> `OpenReplicaUnit` (receiving union dual-writes as they mount) and write a SERVING MARKER on mount-complete. ORDERED REMOVAL (CEP-21 order): add pending owners to writes first -> they mount + write markers -> drop the leaver from a position's routed set once that position's marker is present at an epoch strictly above the leaver's open epoch (handoff durably complete) -> the leaver releases (`CloseReplicaUnit`), fenced only AFTER its successor serves. Durability: a write acked at W is durable on >= W INDEPENDENT replica copies; dual-write targets independent per-`(unit, replica)` databases (no two-writers-on-one-db); apply-if-newer/LWW makes the same stamped envelope idempotent across union members + retries; the leaver is fenced only after the marker gate, so no acked write's only durable copy is ever on a fenced/closed node. Open implementation-must-verify (UNCHANGED): a pending owner's open must replay the full durable WAL tail AND the manifest-epoch fence must be effective no later than the WAL-recovery cutoff (pin with a write-just-before-fence test AND a write-dual-written-concurrent-with-mount test; slatedb-go is an opaque FFI binding). REMOVED (superseded): per-position forwarding (`acquiringForwardTarget` / `forwardPutToPredecessor` / `forwardGetToPredecessor`), `PredecessorAddr` / `Predecessor` on `HandoffState`, the `priorDesiredReplicas` / `priorAddrs` snapshot, the single-hop-scope fallback, and the draining-exclusion in `reconcileRingFromMembership`. REUSED: the serving marker (`WriteServingMarker` / `ReadServingMarker`), the gossiped `Draining` Meta bit, `mountTable.mounts` re-keyed by `ReplicaUnit`, the durable epoch fence + apply-if-newer, `DrainForLeave`, Option A's retry as the residual-case belt. RETAINED (NOT removed - the earlier draft's list was wrong here): the position-addressed proto field `ReplicaUnitRef ru`, now carried on every union write + read leg so the receiver resolves `mountTable.mounts[ru]` by explicit position rather than by its own ring index. Acceptance: an in-process loss-oracle gate with an ARBITRARILY SLOW `OpenReplicaUnit` asserts zero acked loss AND acked/attempted ~100% even under a multi-second mount (where Option A alone collapses) AND post-transition readback of every baseline key from the post-leave routed set, demonstrated to FAIL on a forced draining-exclusion (no union) with Option-A retry also off. See "v0.8 Phase 2e" above.
  - [ ] Graceful leave / DRAINING node-state (Phase 2e, scale-down): a leaving node enters a distinct DRAINING state - REACHABLE, a CURRENT OWNER, and the source of the PENDING split - advertised via a gossiped per-member `Draining` bit in the memberlist `Meta` (alongside the gRPC address), set by `membership.SetDraining(true)` which calls `memberlist.UpdateNode` (the node STAYS alive, a current owner, does NOT `Leave()`). Every node decodes the bit; `reconcileRingFromMembership` does NOT exclude draining members from the ring (the include/exclude split is computed per-op in `routedReplicasForKey`, current=ring-incl-draining, pending=ring-excl-draining), so the leaver keeps its positions and is DUAL-WRITTEN via the union while it serves. `DrainForLeave(ctx)` blocks until `ownedPositionCount() == 0` (every owned position has a serving successor, released by `drainCheck` on its marker) or the timeout fires, THEN the real `membership.Leave()` + `Shutdown()` teardown runs. Order: SET DRAINING (form the union) -> SERVE + WAIT (successors mount + write markers, the union collapses onto the pending set) -> REAL LEAVE + SHUTDOWN. FIXES the root-cause design flaw where `memberlist.Leave()`-while-staying-alive was self-contradictory (leave-intent + still-gossiping-alive => survivors RE-ADD the leaver and never start the handoff, ~58% in-process / ~70% staging). SUPERSEDES BOTH the ring-freeze stopgap (`leaving atomic.Bool` + `reconcileRingFromMembership` early-return + `ring.Remove(self)`) AND the draining-exclusion + back-forward draft: the draining node stays alive AND a current owner, so its snapshot never collapses and the union routes to it directly. Gated by `GracefulLeaveDrainTimeout` (`0` = disabled = the break-demo state); behind `multiReplicated()`. Acceptance: a continuous writer through a graceful one-node leave asserts ~100% ack + zero unserved window + post-leave readback (vs the gap with timeout 0), reusing the slow-`OpenReplicaUnit` injection. See "v0.8 Phase 2e: Graceful leave (scale-down)" above.
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
